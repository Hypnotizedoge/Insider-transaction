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
        df = yf.download(ticker, period=period, interval="1d")
        if df.empty:
            return None
        # Handle MultiIndex if necessary
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
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
            # Using the 'scrape' function from the user's scrape_bursa.py
            try:
                dealings_df = scrape_bursa.scrape(company_code=company_code, pages=int(pages_to_scrape))
            except Exception as scrape_err:
                st.error(f"Scraping failed with error: {scrape_err}")
                st.stop()
        
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
            dealings_df['Parsed Date'] = dealings_df['Date of Transaction']
            dealings_df = dealings_df.dropna(subset=['Parsed Date'])
            
            # Filter dealings to match stock period range
            min_date = stock_df.index.min()
            max_date = stock_df.index.max()
            dealings_df = dealings_df[(dealings_df['Parsed Date'] >= min_date) & (dealings_df['Parsed Date'] <= max_date)]
            
            # 4. Create Plotly Visualization
            fig = go.Figure()

            # Stock Price Line (Y=Price, X=Date)
            fig.add_trace(go.Scatter(
                x=stock_df.index, 
                y=stock_df['Close'], 
                name="Stock Price (Close)",
                line=dict(color="rgba(41, 98, 255, 0.5)", width=2),
                hoverinfo='x+y'
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
