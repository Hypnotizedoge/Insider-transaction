import streamlit as st
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go

st.set_page_config(page_title="Bursa Insider Dealing Overlay", layout="wide")

st.title("📊 Bursa Malaysia Insider Dealing Overlay")
st.markdown("Visualize director and major shareholder dealings on top of historical stock prices.")

# --- Sidebar ---
with st.sidebar:
    st.header("⚙️ Settings")
    company_code = st.text_input("Company Code (e.g., 0151)", value="0151")
    period = st.selectbox("Stock Price Period", ["1mo", "3mo", "6mo", "1y", "2y", "5y", "max"], index=3)

    st.subheader("Data Source")
    mode = st.radio("Choose mode", [
        "🌐 Live Scrape (Backend/Streamlit)",
        "📂 Auto-Load Saved CSV", 
        "📤 Upload Manual CSV"
    ], index=0)

    dealings_df = pd.DataFrame()
    scrape_triggered = False

    if mode == "📂 Auto-Load Saved CSV":
        st.info(
            f"Looking for saved data for **{company_code}**...\n\n"
            "To update this data, run `python scrape_bursa.py --company " + company_code + "` "
            "locally and commit the resulting CSV file to your repository.",
            icon="💡"
        )
        analyze_btn = st.button("📊 Load & Analyze", use_container_width=True)
        if analyze_btn:
            import os
            expected_file = f"{company_code}_bursa_dealings.csv"
            # Fallback to the old name if the new one doesn't exist
            if not os.path.exists(expected_file) and os.path.exists("bursa_dealings.csv"):
                expected_file = "bursa_dealings.csv"

            if os.path.exists(expected_file):
                dealings_df = pd.read_csv(expected_file)
                scrape_triggered = True
                st.toast(f"Loaded {expected_file}", icon="✅")
            else:
                st.error(f"Could not find `{expected_file}` in the app directory. Please run the scraper locally first and ensure the file is in the same folder as app.py.")

    elif mode == "📤 Upload Manual CSV":
        st.info(
            "Run `scrape_bursa.py` on your local machine to generate `bursa_dealings.csv`, "
            "then upload it here.",
            icon="💡"
        )
        uploaded_file = st.file_uploader("Upload bursa_dealings.csv", type=["csv"])
        analyze_btn = st.button("📊 Analyze Uploaded Data", use_container_width=True)
        if analyze_btn and uploaded_file is not None:
            dealings_df = pd.read_csv(uploaded_file)
            scrape_triggered = True
        elif analyze_btn and uploaded_file is None:
            st.warning("Please upload a CSV file first.")

    else:  # Live Scrape (Backend/Streamlit)
        pages_to_scrape = st.number_input("Pages to Scrape", min_value=1, max_value=100, value=10)
        st.info("🌐 Fetching data directly on the server")
        scrape_btn = st.button("🚀 Scrape & Analyze", use_container_width=True)

        if scrape_btn:
            try:
                import scrape_bursa
                with st.spinner(f"Scraping {pages_to_scrape} pages of announcements..."):
                    result = scrape_bursa.scrape(company_code=company_code, pages=int(pages_to_scrape))
                    if isinstance(result, tuple):
                        dealings_df, scrape_stats = result
                    else:
                        dealings_df = result
                        scrape_stats = {}

                # Show diagnostics
                with st.expander("🔧 Scrape Diagnostics", expanded=True):
                    col1, col2, col3, col4, col5 = st.columns(5)
                    col1.metric("Pages Fetched", scrape_stats.get("pages_fetched", 0))
                    col2.metric("API Rows Seen", scrape_stats.get("total_rows_seen", 0))
                    col3.metric("Links Matched", scrape_stats.get("links_found", 0))
                    col4.metric("Raw Results", scrape_stats.get("raw_results", 0))
                    col5.metric("After Filter", scrape_stats.get("after_filter", 0))
                    if scrape_stats.get("errors"):
                        st.error("Errors: " + " | ".join(scrape_stats["errors"]))
                    if scrape_stats.get("sample_titles"):
                        st.caption("**Sample titles seen in API:**")
                        for t in scrape_stats["sample_titles"]:
                            st.caption(f"• {t}")
                    elif scrape_stats.get("pages_fetched", 0) == 0:
                        st.error("⛔ API fetch failed. If you pushed the curl_cffi update, Streamlit Cloud might still be blocked by Cloudflare (datacenters IP block).")
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

        # yfinance >= 0.2 returns MultiIndex columns: (field, ticker)
        # Flatten to single level using the field name (first level)
        if isinstance(df.columns, pd.MultiIndex):
            # Drop the ticker level — keep field names only
            df.columns = df.columns.get_level_values(0)

        # Deduplicate columns (can happen if MultiIndex had repeated field names)
        df = df.loc[:, ~df.columns.duplicated()]

        # Ensure we have a 'Close' column
        if "Close" not in df.columns:
            st.error(f"No 'Close' column found. Available: {list(df.columns)}")
            return None

        # Keep only the columns we need
        keep = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
        df = df[keep].copy()

        # Remove timezone from index so date comparisons work
        if hasattr(df.index, "tz") and df.index.tz is not None:
            df.index = df.index.tz_localize(None)

        return df
    except Exception as e:
        st.error(f"Error fetching stock data: {e}")
        return None


# --- Main App Logic ---
if scrape_triggered:
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
            # scrape_bursa may already return datetime; to_datetime handles both string and datetime
            dealings_df['Parsed Date'] = pd.to_datetime(
                dealings_df['Date of Transaction'], dayfirst=True, errors='coerce'
            )
            # Strip any timezone so comparison with stock index works
            if hasattr(dealings_df['Parsed Date'].dtype, 'tz') and dealings_df['Parsed Date'].dt.tz is not None:
                dealings_df['Parsed Date'] = dealings_df['Parsed Date'].dt.tz_localize(None)

            dealings_df = dealings_df.dropna(subset=['Parsed Date'])

            # --- 4. Show raw data for inspection ---
            with st.expander(f"🔍 Raw scraped data ({len(dealings_df)} rows, before date filter)"):
                st.dataframe(dealings_df[['Parsed Date', 'Name', 'Designation', 'Transaction Type', 'Price (RM)', 'No. of Shares']], use_container_width=True)

            # --- 5. Filter to stock period ---
            min_date = stock_df.index.min()
            max_date = stock_df.index.max()
            filtered_df = dealings_df[
                (dealings_df['Parsed Date'] >= min_date) &
                (dealings_df['Parsed Date'] <= max_date)
            ]

            if filtered_df.empty:
                st.warning(
                    f"⚠️ {len(dealings_df)} dealings scraped, but none fall within the chart period "
                    f"({min_date.date()} → {max_date.date()}). "
                    "Try selecting a longer period (e.g. **5y** or **max**) in the sidebar."
                )
                # Still show price chart without markers
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=stock_df.index, y=stock_df['Close'],
                    name="Daily Close Price (RM)", line=dict(color="rgba(41,98,255,0.6)", width=2)))
                fig.update_layout(title=f"Stock Price: {company_code}.KL", template="plotly_white", height=500)
                st.plotly_chart(fig, use_container_width=True)
            else:
                dealings_df = filtered_df

                # --- 6. Split acquisitions vs disposals ---
                # Use na=False so NaN Transaction Type rows are excluded cleanly
                acq = dealings_df[
                    dealings_df['Transaction Type'].str.lower().str.contains(
                        "acqui|bought|purchase", na=False
                    )
                ].copy()
                dis = dealings_df[
                    dealings_df['Transaction Type'].str.lower().str.contains(
                        "dispos|sold|sale", na=False
                    )
                ].copy()

                # Drop rows with no price — they'd be invisible on chart anyway
                acq = acq.dropna(subset=['Price (RM)'])
                dis = dis.dropna(subset=['Price (RM)'])

                st.info(f"📍 Showing **{len(acq)} acquisitions** (🟢) and **{len(dis)} disposals** (🔴) on chart. "
                        f"Period: {min_date.date()} → {max_date.date()}")

                # --- 7. Build chart ---
                fig = go.Figure()

                # Stock price line
                fig.add_trace(go.Scatter(
                    x=stock_df.index,
                    y=stock_df['Close'],
                    name="Daily Close Price (RM)",
                    line=dict(color="rgba(41, 98, 255, 0.6)", width=2),
                    hovertemplate="Date: %{x|%d %b %Y}<br>Close: RM %{y:.3f}<extra></extra>"
                ))

                # Acquisitions — green dots at transaction price
                if not acq.empty:
                    fig.add_trace(go.Scatter(
                        x=acq['Parsed Date'],
                        y=acq['Price (RM)'],
                        mode='markers',
                        name='Acquisition 🟢',
                        marker=dict(color='#2E7D32', size=13, symbol='circle',
                                    line=dict(width=2, color='white')),
                        customdata=acq[['Name', 'Designation', 'No. of Shares', 'Price (RM)', 'Transaction Type']].values,
                        hovertemplate=(
                            "<b>%{customdata[0]}</b><br>"
                            "Designation: %{customdata[1]}<br>"
                            "Shares: %{customdata[2]:,.0f}<br>"
                            "Transaction Price: RM %{customdata[3]:.3f}<br>"
                            "Type: %{customdata[4]}<br>"
                            "Date: %{x|%d %b %Y}<extra></extra>"
                        )
                    ))

                # Disposals — red dots at transaction price
                if not dis.empty:
                    fig.add_trace(go.Scatter(
                        x=dis['Parsed Date'],
                        y=dis['Price (RM)'],
                        mode='markers',
                        name='Disposal 🔴',
                        marker=dict(color='#C62828', size=13, symbol='circle',
                                    line=dict(width=2, color='white')),
                        customdata=dis[['Name', 'Designation', 'No. of Shares', 'Price (RM)', 'Transaction Type']].values,
                        hovertemplate=(
                            "<b>%{customdata[0]}</b><br>"
                            "Designation: %{customdata[1]}<br>"
                            "Shares: %{customdata[2]:,.0f}<br>"
                            "Transaction Price: RM %{customdata[3]:.3f}<br>"
                            "Type: %{customdata[4]}<br>"
                            "Date: %{x|%d %b %Y}<extra></extra>"
                        )
                    ))

                fig.update_layout(
                    title=dict(text=f"Stock Price vs. Insider Dealings: {company_code}.KL", font=dict(size=22)),
                    xaxis=dict(title="Date", showgrid=True, gridcolor="lightgray"),
                    yaxis=dict(title="Price (RM)", showgrid=True, gridcolor="lightgray"),
                    template="plotly_white",
                    hovermode="closest",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    height=700
                )
                st.plotly_chart(fig, use_container_width=True)

                # --- 8. Data table ---
                with st.expander("📋 View Full Dealings Table"):
                    show_cols = ['Parsed Date', 'Name', 'Designation', 'Description',
                                 'No. of Shares', 'Price (RM)', 'Transaction Type', 'URL']
                    st.dataframe(dealings_df[show_cols], use_container_width=True)


else:
    if mode == "📂 Auto-Load Saved CSV":
        st.info(f"👈 Click 'Load & Analyze' in the sidebar to auto-load saved data for {company_code}.KL.")
    elif mode == "📤 Upload Manual CSV":
        st.info("👈 Upload a CSV file and click 'Analyze Uploaded Data' in the sidebar to begin.")
    else:
        st.info("👈 Enter a company code and click 'Scrape & Analyze' in the sidebar to begin.")
