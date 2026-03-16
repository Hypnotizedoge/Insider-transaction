import streamlit as st
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta

# --- Config ---
st.set_page_config(page_title="Insider Trading Tracker", layout="wide")

# --- Helper Functions ---
@st.cache_data(ttl=3600)
def fetch_price_data(ticker, period="1y"):
    """Fetches historical OHLCV data using yfinance."""
    try:
        data = yf.download(ticker, period=period, progress=False)
        if data.empty:
            return None
        # yfinance download sometimes returns MultiIndex columns. Flatten them if needed.
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.droplevel(1)
        data.reset_index(inplace=True)
        return data
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None

def generate_mock_insider_data(price_df):
    """Generates mock insider transactions based on the date range of the price data."""
    if price_df is None or price_df.empty:
        return pd.DataFrame()
        
    start_date = price_df['Date'].min()
    end_date = price_df['Date'].max()
    date_range = (end_date - start_date).days
    
    # Generate 5-15 random transactions
    num_transactions = np.random.randint(5, 16)
    
    dates = []
    categories = []
    types = []
    amounts = []
    prices = []
    names = ["BROADY GEORGE K", "AHMAD BIN ALI", "TAN SRI LIM", "DATUK SERI WONG", "LEE CHONG WEI"]
    participants = []
    
    for _ in range(num_transactions):
        random_days = np.random.randint(0, date_range)
        tx_date = start_date + timedelta(days=random_days)
        
        # Only pick trading days (exist in price_df)
        # Find the closest date in price_df
        closest_row = price_df.iloc[(price_df['Date'] - tx_date).abs().argsort()[:1]]
        actual_tx_date = closest_row['Date'].values[0]
        actual_price = closest_row['Close'].values[0]
        
        dates.append(actual_tx_date)
        
        tx_type = np.random.choice(["Buy", "Sell"], p=[0.7, 0.3]) # More buys than sells usually
        types.append(tx_type)
        
        category = np.random.choice(["Changes in Shareholding", "Dealings in Listed Securities"], p=[0.7, 0.3])
        categories.append(category)
        
        amounts.append(np.random.randint(10, 500) * 1000) # 10k to 500k shares
        
        # Add slight randomization to price (representing intraday price or slight variation)
        price_variation = np.random.uniform(0.98, 1.02)
        prices.append(round(actual_price * price_variation, 3))
        
        participants.append(np.random.choice(names))
        
    mock_data = pd.DataFrame({
        'Date': pd.to_datetime(dates),
        'Category': categories,
        'Type': types,
        'Participant': participants,
        'Amount': amounts,
        'Price': prices
    })
    
    return mock_data.sort_values(by='Date').reset_index(drop=True)


def create_chart(price_df, insider_df, ticker_name):
    """Creates a Bloomberg-style chart with Plotly."""
    
    # Create subplots: 1 row, 1 col, but setup for secondary y-axis if we wanted volume
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.03, subplot_titles=(f'{ticker_name} Price & Insider Action', 'Volume'),
                        row_width=[0.2, 0.7])

    # 1. Candlestick Chart (Row 1)
    fig.add_trace(go.Candlestick(
        x=price_df['Date'],
        open=price_df['Open'],
        high=price_df['High'],
        low=price_df['Low'],
        close=price_df['Close'],
        name='Price',
        increasing_line_color='white', decreasing_line_color='white', # Outlines
        increasing_fillcolor='black', decreasing_fillcolor='black'    # Filled body
    ), row=1, col=1)
    
    # 2. Historical Close Line Chart overlay (Optional, but looks nice. Let's stick to pure candlestick for now or add a MA)
    
    # 3. Volume Bar Chart (Row 2)
    colors = ['green' if row['Close'] >= row['Open'] else 'red' for idx, row in price_df.iterrows()]
    fig.add_trace(go.Bar(
        x=price_df['Date'],
        y=price_df['Volume'],
        name='Volume',
        marker_color=colors
    ), row=2, col=1)

    # 4. Insider Transaction Markers — 4 separate series for legibility
    SERIES_CONFIG = [
        # (category_label, tx_type, display_name, symbol, color)
        ("Changes in Shareholding",       "Buy",  "SH — Buy",        "triangle-up",   "#00ff00"),  # bright green
        ("Changes in Shareholding",       "Sell", "SH — Sell",       "triangle-down", "#ff4444"),  # red
        ("Dealings in Listed Securities", "Buy",  "Dealings — Buy",  "triangle-up",   "#00ccff"),  # cyan
        ("Dealings in Listed Securities", "Sell", "Dealings — Sell", "triangle-down", "#ff9900"),  # orange
    ]

    if not insider_df.empty:
        for cat, tx_type, series_name, symbol, color in SERIES_CONFIG:
            subset = insider_df[
                (insider_df['Category'] == cat) & (insider_df['Type'] == tx_type)
            ]
            if subset.empty:
                continue

            hover_texts = [
                f"<b>{row['Participant']}</b><br>"
                f"<b>Category:</b> {row['Category']}<br>"
                f"<b>Transaction:</b> {row['Type'].upper()}<br>"
                f"<b>Date:</b> {pd.Timestamp(row['Date']).strftime('%d-%b-%y')}<br>"
                f"<b>Shares:</b> {int(row['Amount']):,}<br>"
                f"<b>Price:</b> RM {row['Price']:.3f}"
                for _, row in subset.iterrows()
            ]

            fig.add_trace(go.Scatter(
                x=subset['Date'],
                y=subset['Price'],
                mode='markers',
                marker=dict(
                    symbol=symbol,
                    size=16,
                    color=color,
                    line=dict(width=2, color='#111111')
                ),
                name=series_name,
                hoverinfo='text',
                hovertext=hover_texts
            ), row=1, col=1)

    # Bloomberg Style Layout Tweaks
    fig.update_layout(
        template="plotly_dark", # Pitch black background
        plot_bgcolor='#000000',
        paper_bgcolor='#000000',
        height=700,
        margin=dict(l=50, r=50, t=50, b=50),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        xaxis_rangeslider_visible=False,
    )
    
    # Grid lines and formatting
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#333333', row=1, col=1)
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#333333', row=1, col=1)
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#333333', row=2, col=1)
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#333333', row=2, col=1)

    return fig

# --- Main App ---
st.title("📈 Bursa Malaysia Insider Trading Flow Tracker")
st.markdown("*Note: Currently utilizing mock insider data for demonstration purposes due to scraping protections on Bursa Malaysia.*")

# UI Controls
col1, col2 = st.columns([1, 1])

with col1:
    ticker_input = st.text_input("Enter Bursa Ticker (e.g., 1155.KL for Maybank, 1023.KL for CIMB)", "1155.KL")
    
with col2:
    period_options = {
        "1 Month": "1mo",
        "3 Months": "3mo",
        "6 Months": "6mo",
        "1 Year": "1y",
        "2 Years": "2y",
        "5 Years": "5y"
    }
    selected_period_label = st.selectbox("Select Time Period", list(period_options.keys()), index=3) # Default 1y
    period_val = period_options[selected_period_label]

st.divider()

if ticker_input:
    with st.spinner(f"Fetching data for {ticker_input}..."):
        # Fetch Data
        price_data = fetch_price_data(ticker_input, period_val)
        
        if price_data is not None and not price_data.empty:
            # Generate Mock Data
            insider_data = generate_mock_insider_data(price_data)
            
            # Plot
            fig = create_chart(price_data, insider_data, ticker_input)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show Data Table
            if not insider_data.empty:
                st.subheader(f"Insider Transactions ({selected_period_label})")

                # Rename columns for display clarity
                display_df = insider_data.rename(columns={
                    'Participant': 'Name / Company',
                    'Amount': 'Shares',
                    'Price': 'Price (RM)'
                })

                def style_type(val):
                    if val == 'Buy':
                        return 'color: #00ff00; font-weight: bold'
                    elif val == 'Sell':
                        return 'color: #ff4444; font-weight: bold'
                    return ''

                def style_category(val):
                    if val == 'Dealings in Listed Securities':
                        return 'color: #00ccff'
                    elif val == 'Changes in Shareholding':
                        return 'color: #ff9900'
                    return ''

                styled = (
                    display_df.style
                    .format({"Shares": "{:,}", "Price (RM)": "{:.3f}"})
                    .applymap(style_type, subset=['Type'])
                    .applymap(style_category, subset=['Category'])
                )

                st.dataframe(styled, use_container_width=True)

                # Summary metrics
                col_a, col_b, col_c, col_d = st.columns(4)
                col_a.metric("Total Transactions", len(insider_data))
                col_b.metric("Buys", len(insider_data[insider_data['Type'] == 'Buy']))
                col_c.metric("Sells", len(insider_data[insider_data['Type'] == 'Sell']))
                col_d.metric("Total Shares Traded", f"{insider_data['Amount'].sum():,}")
        else:
            st.warning(f"No price data found for {ticker_input} over the selected period. Please check the ticker symbol (ensure it ends with .KL).")
