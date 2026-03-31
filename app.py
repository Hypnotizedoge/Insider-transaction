import streamlit as st
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
from datetime import datetime, timedelta
import scrape_bursa
import time

st.set_page_config(page_title="Bursa Insider Dealing Overlay", layout="wide")

st.title("📊 Bursa Malaysia Insider Dealing Overlay")
st.markdown("Visualize director and major shareholder dealings on top of historical stock prices.")

# --- Sidebar Inputs ---
with st.sidebar:
    st.header("Settings")
    company_code = st.text_input("Company Code (e.g., 0151)", value="0151")
    pages_to_scrape = st.number_input("Pages to Scrape", min_value=1, max_value=100, value=10)
    
    st.divider()
    period = st.selectbox("Stock Price Period", ["1mo", "3mo", "6mo", "1y", "2y", "5y", "max"], index=3)
    
    scrape_btn = st.button("🚀 Scrape & Analyze", use_container_width=True)

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
if scrape_btn:
    # 1. Fetch Stock Data
    with st.spinner(f"Fetching stock data for {company_code}.KL..."):
        stock_df = get_stock_data(company_code, period)
        
    if stock_df is None:
        st.error(f"Could not find stock data for ticker '{company_code}.KL'. Please check the code.")
    else:
        # 2. Scrape Bursa Dealings using the specified backend
        with st.spinner(f"Scraping {pages_to_scrape} pages of announcements..."):
            try:
                result = scrape_bursa.scrape(company_code=company_code, pages=int(pages_to_scrape))
                # scrape() returns (df, stats) — handle both tuple and bare DataFrame defensively
                if isinstance(result, tuple):
                    dealings_df, scrape_stats = result
                else:
                    dealings_df = result
                    scrape_stats = {}
            except Exception as scrape_err:
                st.error(f"Scraping failed with error: {scrape_err}")
                st.stop()
        
        # Always show diagnostic stats
        with st.expander("🔧 Scrape Diagnostics", expanded=True):
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Pages Fetched", scrape_stats.get("pages_fetched", 0))
            col2.metric("Links Found", scrape_stats.get("links_found", 0))
            col3.metric("Raw Results", scrape_stats.get("raw_results", 0))
            col4.metric("After Filter", scrape_stats.get("after_filter", 0))
            if scrape_stats.get("errors"):
                st.error("Errors encountered: " + " | ".join(scrape_stats["errors"]))
        
        if dealings_df.empty:
            st.warning("No insider dealings (Acquisitions/Disposals) found in the selected range.")
            
            # Show the price chart anyway
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=stock_df.index, y=stock_df['Close'], name="Close Price", line=dict(color="#2962FF", width=2)))
            fig.update_layout(title=f"Stock Price: {company_code}.KL", xaxis_title="Date", yaxis_title="Price (RM)", template="plotly_white")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.success(f"Successfully extracted {len(dealings_df)} dealings!")
            
            # 3. Process Dealings Data
            # Note: scrape_bursa.py already converts 'Date of Transaction' to datetime
            dealings_df['Parsed Date'] = pd.to_datetime(dealings_df['Date of Transaction'], dayfirst=True, errors='coerce')
            dealings_df = dealings_df.dropna(subset=['Parsed Date'])
            
            # Debug: show raw scraped data before date filtering
            with st.expander(f"🔍 Debug: Raw scraped data ({len(dealings_df)} rows before date filter)"):
                st.dataframe(dealings_df, use_container_width=True)

            # Filter dealings to match stock period range (index is already tz-naive)
            min_date = stock_df.index.min()
            max_date = stock_df.index.max()
            
            filtered_df = dealings_df[(dealings_df['Parsed Date'] >= min_date) & (dealings_df['Parsed Date'] <= max_date)]
            
            if filtered_df.empty and not dealings_df.empty:
                st.warning(f"⚠️ Scraped {len(dealings_df)} dealings, but none fall within the selected stock period ({min_date.date()} → {max_date.date()}). Try a longer period (e.g. '5y' or 'max').")
                dealings_df = filtered_df
            else:
                dealings_df = filtered_df
            
            # 4. Create Plotly Visualization
            fig = go.Figure()

            # Stock Price Line (Y=Price, X=Date)
            fig.add_trace(go.Scatter(
                x=stock_df.index,
                y=stock_df['Close'],
                name="Daily Close Price (RM)",
                line=dict(color="rgba(41, 98, 255, 0.6)", width=2),
                hovertemplate="Date: %{x|%d %b %Y}<br>Close: RM %{y:.3f}<extra></extra>"
            ))

            # Split into Acquired and Disposed
            acq = dealings_df[dealings_df['Transaction Type'].str.lower().str.contains("acquire|acquisition|bought")]
            dis = dealings_df[dealings_df['Transaction Type'].str.lower().str.contains("dispose|disposal|sold")]

            # Acquisitions (Green Dots) - X=Date, Y=Price
            if not acq.empty:
                fig.add_trace(go.Scatter(
                    x=acq['Parsed Date'],
                    y=acq['Price (RM)'],
                    mode='markers',
                    name='Acquisition',
                    marker=dict(color='#2E7D32', size=12, symbol='circle', line=dict(width=2, color='white')),
                    customdata=acq[['Name', 'Designation', 'No. of Shares', 'Price (RM)', 'Transaction Type']],
                    hovertemplate="<b>%{customdata[0]}</b><br>" +
                                "Designation: %{customdata[1]}<br>" +
                                "Shares: %{customdata[2]:,.0f}<br>" +
                                "Transaction Price: RM %{customdata[3]:.3f}<br>" +
                                "Type: %{customdata[4]}<br>" +
                                "Date: %{x|%d %b %Y}<extra></extra>"
                ))

            # Disposals (Red Dots) - X=Date, Y=Price
            if not dis.empty:
                fig.add_trace(go.Scatter(
                    x=dis['Parsed Date'],
                    y=dis['Price (RM)'],
                    mode='markers',
                    name='Disposal',
                    marker=dict(color='#C62828', size=12, symbol='circle', line=dict(width=2, color='white')),
                    customdata=dis[['Name', 'Designation', 'No. of Shares', 'Price (RM)', 'Transaction Type']],
                    hovertemplate="<b>%{customdata[0]}</b><br>" +
                                "Designation: %{customdata[1]}<br>" +
                                "Shares: %{customdata[2]:,.0f}<br>" +
                                "Transaction Price: RM %{customdata[3]:.3f}<br>" +
                                "Type: %{customdata[4]}<br>" +
                                "Date: %{x|%d %b %Y}<extra></extra>"
                ))

            fig.update_layout(
                title=dict(
                    text=f"Stock Price vs. Insider Dealings: {company_code}.KL",
                    font=dict(size=24)
                ),
                xaxis=dict(
                    title="Date (Horizontal Axis)",
                    showgrid=True,
                    gridcolor='lightgray'
                ),
                yaxis=dict(
                    title="Price (RM) (Vertical Axis)",
                    showgrid=True,
                    gridcolor='lightgray'
                ),
                template="plotly_white",
                hovermode='closest',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                height=700
            )

            st.plotly_chart(fig, use_container_width=True)

            # 5. Data Table
            with st.expander("View Raw Dealings Data"):
                st.dataframe(dealings_df[['Parsed Date', 'Name', 'Designation', 'Description', 'No. of Shares', 'Price (RM)', 'Transaction Type', 'URL']], use_container_width=True)

else:
    st.info("👈 Enter a company code and click 'Scrape & Analyze' in the sidebar to begin.")
