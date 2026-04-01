import streamlit as st
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go

st.set_page_config(page_title="Bursa Insider Dealing Overlay", layout="wide")

st.title("📊 Bursa Malaysia Insider Dealing Overlay")
st.markdown("Visualize director and major shareholder dealings on top of historical stock prices.")

# --- Sidebar (Global) ---
with st.sidebar:
    st.header("⚙️ Settings")
    company_code = st.text_input("Company Code (e.g., 0151)", value="0151")
    period = st.selectbox("Stock Price Period", ["1mo", "3mo", "6mo", "1y", "2y", "5y", "max"], index=3)
    pages_to_scrape = st.number_input("Pages to Scrape", min_value=1, max_value=100, value=20)
    
    with st.expander("🛠 Advanced / Cloudflare Bypass"):
        st.markdown(
            "If Cloudflare blocks this Streamlit app (e.g., 0 results or 403 errors), "
            "you **must** use a proxy. Residential proxies work best."
        )
        proxy_url = st.text_input("Proxy URL (e.g. http://user:pass@host:port)", value="")
        st.caption("Tip: Try [WebShare](https://www.webshare.io/) or [Bright Data](https://brightdata.com/) for cheap rotating proxies.")

    st.divider()
    st.info("🌐 Fetching data directly on the server")
    scrape_btn = st.button("🚀 Scrape & Analyze", use_container_width=True)

    dealings_df = pd.DataFrame()
    scrape_stats = {}
    scrape_triggered = False

    if scrape_btn:
        try:
            import scrape_bursa
            with st.spinner(f"Scraping {pages_to_scrape} pages of announcements..."):
                p = proxy_url.strip() if proxy_url.strip() else None
                result = scrape_bursa.scrape(company_code=company_code, pages=int(pages_to_scrape), proxy=p)
                if isinstance(result, tuple):
                    dealings_df, scrape_stats = result
                else:
                    dealings_df = result
                    scrape_stats = {}
            scrape_triggered = True
        except Exception as e:
            st.error(f"Scraping error: {e}")

# --- Helper Functions ---
def get_stock_data(ticker_code, period):
    ticker = f"{ticker_code}.KL"
    try:
        df = yf.download(ticker, period=period, interval="1d", auto_adjust=False, progress=False)
        if df.empty:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.loc[:, ~df.columns.duplicated()]

        if "Close" not in df.columns:
            st.error(f"No 'Close' column found. Available: {list(df.columns)}")
            return None

        keep = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
        df = df[keep].copy()

        if hasattr(df.index, "tz") and df.index.tz is not None:
            df.index = df.index.tz_localize(None)

        return df
    except Exception as e:
        st.error(f"Error fetching stock data: {e}")
        return None

# --- Main App Logic with Tabs ---
tab_analysis, tab_debug = st.tabs(["📊 Analysis", "🕵️ Proxy & IP Debugger"])

with tab_analysis:
    if scrape_triggered:
        # Show diagnostics in the analysis tab
        with st.expander("🔧 Scrape Diagnostics", expanded=True):
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Pages Fetched", scrape_stats.get("pages_fetched", 0))
            col2.metric("API Rows Seen", scrape_stats.get("total_rows_seen", 0))
            col3.metric("Links Matched", scrape_stats.get("links_found", 0))
            col4.metric("Raw Results", scrape_stats.get("raw_results", 0))
            col5.metric("After Filter", scrape_stats.get("after_filter", 0))
            
            if scrape_stats.get("errors"):
                st.error("🚨 **Scrape Errors / Blocks Detected:**")
                for err in scrape_stats["errors"]:
                    st.warning(err)
                st.info("💡 **Tip:** Go to the **🕵️ Proxy Debugger** tab to test your connection.")

        # 1. Fetch Stock Data
        with st.spinner(f"Fetching stock data for {company_code}.KL..."):
            stock_df = get_stock_data(company_code, period)
            
        if stock_df is None:
            st.error(f"Could not find stock data for ticker '{company_code}.KL'. Please check the code.")
        else:
            if dealings_df.empty:
                st.warning("No insider dealings (Acquisitions/Disposals) found in the selected range.")
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=stock_df.index, y=stock_df['Close'], name="Close Price", line=dict(color="#2962FF", width=2)))
                fig.update_layout(title=f"Stock Price: {company_code}.KL", xaxis_title="Date", yaxis_title="Price (RM)", template="plotly_white")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.success(f"✅ Successfully loaded {len(dealings_df)} dealings!")

                # --- 3. Parse dates ---
                dealings_df['Parsed Date'] = pd.to_datetime(dealings_df['Date of Transaction'], dayfirst=True, errors='coerce')
                if hasattr(dealings_df['Parsed Date'].dtype, 'tz') and dealings_df['Parsed Date'].dt.tz is not None:
                    dealings_df['Parsed Date'] = dealings_df['Parsed Date'].dt.tz_localize(None)
                dealings_df = dealings_df.dropna(subset=['Parsed Date'])

                # --- 4. Show raw data for inspection ---
                with st.expander(f"🔍 Raw scraped data ({len(dealings_df)} rows)"):
                    st.dataframe(dealings_df[['Parsed Date', 'Name', 'Designation', 'Transaction Type', 'Price (RM)', 'No. of Shares']], use_container_width=True)

                # --- 5. Filter to stock period ---
                min_date = stock_df.index.min().replace(tzinfo=None)
                max_date = stock_df.index.max().replace(tzinfo=None)
                dealings_df['Parsed Date'] = pd.to_datetime(dealings_df['Parsed Date']).dt.tz_localize(None)
                in_range = (dealings_df['Parsed Date'] >= min_date) & (dealings_df['Parsed Date'] <= max_date)
                filtered_df = dealings_df[in_range].copy()
                out_of_range_count = len(dealings_df) - len(filtered_df)

                if filtered_df.empty:
                    st.warning(f"⚠️ {len(dealings_df)} dealings scraped, but none fall within the chart period. Try '5y' or 'max'.")
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=stock_df.index, y=stock_df['Close'], name="Daily Close Price (RM)", line=dict(color="rgba(41,98,255,0.6)", width=2)))
                    fig.update_layout(title=f"Stock Price: {company_code}.KL", template="plotly_white", height=500)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    dealings_df = filtered_df
                    acq = dealings_df[dealings_df['Transaction Type'].str.lower().str.contains("acqui|bought|purchase", na=False)].copy()
                    dis = dealings_df[dealings_df['Transaction Type'].str.lower().str.contains("dispos|sold|sale", na=False)].copy()

                    date_to_close = stock_df['Close'].to_dict()
                    def fill_missing_price(row):
                        if pd.isna(row['Price (RM)']):
                            date_val = row['Parsed Date']
                            if date_val in date_to_close: return date_to_close[date_val]
                            idx = stock_df.index.get_indexer([date_val], method='pad')
                            if idx[0] >= 0: return stock_df['Close'].iloc[idx[0]]
                        return row['Price (RM)']

                    acq['Price (RM)'] = acq.apply(fill_missing_price, axis=1)
                    dis['Price (RM)'] = dis.apply(fill_missing_price, axis=1)
                    acq, dis = acq.dropna(subset=['Price (RM)']), dis.dropna(subset=['Price (RM)'])

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=stock_df.index, y=stock_df['Close'], name="Daily Close Price (RM)", line=dict(color="rgba(41, 98, 255, 0.6)", width=2)))
                    if not acq.empty:
                        fig.add_trace(go.Scatter(x=acq['Parsed Date'], y=acq['Price (RM)'], mode='markers', name='Acquisition 🟢', marker=dict(color='#2E7D32', size=13, symbol='circle', line=dict(width=2, color='white')),
                            customdata=acq[['Name', 'Designation', 'No. of Shares', 'Price (RM)', 'Transaction Type']].values, hovertemplate="<b>%{customdata[0]}</b><br>Shares: %{customdata[2]:,.0f}<br>Price: RM %{customdata[3]:.3f}<extra></extra>"))
                    if not dis.empty:
                        fig.add_trace(go.Scatter(x=dis['Parsed Date'], y=dis['Price (RM)'], mode='markers', name='Disposal 🔴', marker=dict(color='#C62828', size=13, symbol='circle', line=dict(width=2, color='white')),
                            customdata=dis[['Name', 'Designation', 'No. of Shares', 'Price (RM)', 'Transaction Type']].values, hovertemplate="<b>%{customdata[0]}</b><br>Shares: %{customdata[2]:,.0f}<br>Price: RM %{customdata[3]:.3f}<extra></extra>"))

                    fig.update_layout(title=f"Stock Price vs. Insider Dealings: {company_code}.KL", template="plotly_white", height=600)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    with st.expander("📋 View Full Dealings Table"):
                        show_cols = ['Parsed Date', 'Name', 'Designation', 'No. of Shares', 'Price (RM)', 'Transaction Type', 'URL']
                        st.dataframe(dealings_df[show_cols], use_container_width=True)

    else:
        st.info("👈 Enter a company code and click 'Scrape & Analyze' in the sidebar to begin.")

with tab_debug:
    st.header("🕵️ Connection Diagnostics")
    st.markdown(
        "Use this tool to see exactly what Cloudflare sees when you make a request "
        "from this environment (Streamlit Cloud vs. Local)."
    )
    
    if st.button("🔍 Run Connection Test"):
        import scrape_bursa
        p = proxy_url.strip() if proxy_url.strip() else None
        with st.spinner("Testing connectivity..."):
            diag = scrape_bursa.check_connection(proxy=p)
            
        if diag.get("error"):
            st.error(f"Test Failed: {diag['error']}")
        else:
            c1, c2 = st.columns(2)
            c1.metric("Detected Public IP", diag["ip"])
            status = diag["bursa_status"]
            if status == 200:
                c2.success(f"Bursa Status: {status} (SUCCESS)")
            else:
                c2.error(f"Bursa Status: {status} (BLOCKED)")
                
            st.divider()
            st.subheader("Response Snippet from Bursa")
            st.code(diag["bursa_snippet"])
            
            if status == 403:
                st.warning(
                    "⚠️ **Confirming Block:** You are getting a 403 Forbidden. "
                    "This environment's IP is flagged by Cloudflare. You **must** "
                    "use a Proxy URL in the sidebar to bypass this on Streamlit Cloud."
                )
            elif status == 200:
                st.success("✅ **Success:** Bursa is currently accessible from this IP.")
