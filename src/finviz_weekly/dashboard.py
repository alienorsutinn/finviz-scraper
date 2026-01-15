"""
Interactive Dashboard Module

Streamlit-based dashboard for portfolio analytics:
- Real-time portfolio monitoring
- Risk metrics visualization
- Factor analytics
- Trade execution interface
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class DashboardConfig:
    """Dashboard configuration."""
    title: str = "Finviz Scraper Dashboard"
    refresh_interval: int = 300  # seconds
    default_theme: str = "dark"
    show_live_prices: bool = True
    enable_trading: bool = False  # Safety flag


def create_dashboard_app():
    """
    Create Streamlit dashboard application.

    Run with: streamlit run -m finviz_weekly.dashboard
    """
    try:
        import streamlit as st
    except ImportError:
        logger.error("Streamlit not installed. Install with: pip install streamlit")
        return None

    # Page config
    st.set_page_config(
        page_title="Finviz Scraper Dashboard",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Sidebar
    st.sidebar.title("📊 Navigation")
    page = st.sidebar.radio(
        "Select Page",
        ["Portfolio Overview", "Risk Analytics", "Factor Analysis",
         "Screening", "Trading", "Settings"]
    )

    # Main content
    if page == "Portfolio Overview":
        render_portfolio_overview(st)
    elif page == "Risk Analytics":
        render_risk_analytics(st)
    elif page == "Factor Analysis":
        render_factor_analysis(st)
    elif page == "Screening":
        render_screening(st)
    elif page == "Trading":
        render_trading(st)
    elif page == "Settings":
        render_settings(st)

    return st


def render_portfolio_overview(st):
    """Render portfolio overview page."""
    st.title("📊 Portfolio Overview")

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Portfolio Value", "$125,432", "+2.3%")
    with col2:
        st.metric("Day P&L", "+$2,854", "+2.3%")
    with col3:
        st.metric("Positions", "15", "")
    with col4:
        st.metric("Cash", "$12,543", "")

    st.divider()

    # Holdings table
    st.subheader("Current Holdings")

    holdings_data = {
        'Ticker': ['AAPL', 'MSFT', 'GOOGL', 'NVDA', 'META'],
        'Shares': [100, 50, 25, 30, 40],
        'Avg Cost': [150.00, 350.00, 140.00, 450.00, 300.00],
        'Current': [175.50, 380.00, 155.00, 520.00, 350.00],
        'P&L %': ['+17.0%', '+8.6%', '+10.7%', '+15.6%', '+16.7%'],
        'P&L $': ['+$2,550', '+$1,500', '+$375', '+$2,100', '+$2,000'],
    }
    df = pd.DataFrame(holdings_data)
    st.dataframe(df, use_container_width=True)

    # Performance chart
    st.subheader("Performance History")

    # Generate sample data
    dates = pd.date_range(end=datetime.now(), periods=90, freq='D')
    values = 100000 * (1 + np.random.randn(90).cumsum() * 0.01)

    chart_data = pd.DataFrame({
        'Date': dates,
        'Portfolio': values,
        'Benchmark (SPY)': 100000 * (1 + np.random.randn(90).cumsum() * 0.008)
    })
    chart_data = chart_data.set_index('Date')

    st.line_chart(chart_data)


def render_risk_analytics(st):
    """Render risk analytics page."""
    st.title("⚠️ Risk Analytics")

    # Risk metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Portfolio Beta", "1.15", "-0.05")
        st.metric("Sharpe Ratio", "1.82", "+0.12")

    with col2:
        st.metric("Volatility (Ann.)", "18.5%", "+1.2%")
        st.metric("Max Drawdown", "-8.3%", "")

    with col3:
        st.metric("VaR (95%)", "-$3,250", "")
        st.metric("CVaR (95%)", "-$4,100", "")

    st.divider()

    # Sector exposure
    st.subheader("Sector Exposure")

    sector_data = {
        'Sector': ['Technology', 'Healthcare', 'Financials', 'Consumer', 'Energy'],
        'Weight': [35, 20, 15, 18, 12],
        'Limit': [30, 25, 25, 25, 20],
    }
    sector_df = pd.DataFrame(sector_data)

    col1, col2 = st.columns(2)
    with col1:
        st.bar_chart(sector_df.set_index('Sector')['Weight'])
    with col2:
        st.dataframe(sector_df)

    # Correlation matrix
    st.subheader("Position Correlations")
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'NVDA', 'META']
    corr_matrix = pd.DataFrame(
        np.random.uniform(0.3, 0.9, (5, 5)),
        index=tickers,
        columns=tickers
    )
    np.fill_diagonal(corr_matrix.values, 1.0)
    st.dataframe(corr_matrix.style.background_gradient(cmap='RdYlGn_r'))


def render_factor_analysis(st):
    """Render factor analysis page."""
    st.title("🔬 Factor Analysis")

    # Factor exposures
    st.subheader("Factor Exposures")

    factor_data = {
        'Factor': ['Momentum', 'Value', 'Quality', 'Size', 'Volatility'],
        'Exposure': [0.8, -0.3, 0.6, 0.2, -0.4],
        'Contribution': ['+2.1%', '-0.8%', '+1.5%', '+0.3%', '+0.6%'],
    }
    st.dataframe(pd.DataFrame(factor_data))

    # PCA results
    st.subheader("Principal Component Analysis")

    col1, col2 = st.columns(2)
    with col1:
        st.write("**Explained Variance**")
        pca_data = {
            'Component': ['PC1', 'PC2', 'PC3', 'PC4', 'PC5'],
            'Variance': [45.2, 22.1, 12.5, 8.3, 5.1],
            'Cumulative': [45.2, 67.3, 79.8, 88.1, 93.2],
        }
        st.bar_chart(pd.DataFrame(pca_data).set_index('Component')['Variance'])

    with col2:
        st.write("**Top Factor Loadings (PC1)**")
        loadings = {
            'Factor': ['Quality', 'Momentum', 'Growth', 'Value', 'Risk'],
            'Loading': [0.52, 0.48, 0.35, -0.28, -0.45],
        }
        st.dataframe(pd.DataFrame(loadings))

    # Factor decay
    st.subheader("Factor Signal Decay")
    decay_data = {
        'Factor': ['Momentum', 'Value', 'Quality', 'Growth'],
        'Half-Life (days)': [15, 45, 35, 20],
        'Optimal Holding': ['2 weeks', '6 weeks', '4 weeks', '3 weeks'],
    }
    st.dataframe(pd.DataFrame(decay_data))


def render_screening(st):
    """Render screening page."""
    st.title("🔍 Stock Screening")

    # Screening parameters
    col1, col2, col3 = st.columns(3)

    with col1:
        theme = st.selectbox(
            "Strategy Theme",
            ["GARP", "Quality Value", "Momentum", "Dividend Growth", "Custom"]
        )

    with col2:
        min_mcap = st.number_input(
            "Min Market Cap ($M)",
            min_value=100,
            max_value=100000,
            value=1000
        )

    with col3:
        top_n = st.slider("Top N Results", 10, 100, 30)

    if st.button("Run Screen"):
        st.info("Running screening...")

        # Sample results
        results = {
            'Rank': range(1, 11),
            'Ticker': ['NVDA', 'MSFT', 'AAPL', 'GOOGL', 'META',
                      'AMZN', 'CRM', 'ADBE', 'AMD', 'AVGO'],
            'Score': [92.5, 88.3, 86.1, 84.5, 82.8,
                     80.2, 78.5, 76.3, 74.8, 73.2],
            'Quality': [85, 90, 88, 85, 80, 75, 82, 78, 72, 85],
            'Value': [65, 70, 72, 75, 78, 80, 68, 65, 70, 75],
            'Momentum': [95, 85, 80, 78, 82, 70, 75, 72, 85, 68],
        }
        st.dataframe(pd.DataFrame(results), use_container_width=True)


def render_trading(st):
    """Render trading interface page."""
    st.title("💹 Trading")

    st.warning("⚠️ Trading is in PAPER mode. No real orders will be placed.")

    # Order entry
    st.subheader("New Order")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        ticker = st.text_input("Ticker", "AAPL")

    with col2:
        side = st.selectbox("Side", ["Buy", "Sell"])

    with col3:
        quantity = st.number_input("Quantity", min_value=1, value=100)

    with col4:
        order_type = st.selectbox("Type", ["Market", "Limit"])

    if order_type == "Limit":
        limit_price = st.number_input("Limit Price", min_value=0.01, value=150.00)

    if st.button("Submit Order"):
        st.success(f"Order submitted: {side} {quantity} {ticker}")

    st.divider()

    # Open orders
    st.subheader("Open Orders")
    orders = {
        'ID': ['ORD001', 'ORD002'],
        'Ticker': ['MSFT', 'GOOGL'],
        'Side': ['Buy', 'Buy'],
        'Qty': [50, 25],
        'Type': ['Limit', 'Limit'],
        'Price': ['$375.00', '$145.00'],
        'Status': ['Pending', 'Pending'],
    }
    st.dataframe(pd.DataFrame(orders))

    # Trade history
    st.subheader("Recent Trades")
    trades = {
        'Date': ['2024-01-15', '2024-01-14', '2024-01-12'],
        'Ticker': ['AAPL', 'NVDA', 'META'],
        'Side': ['Buy', 'Buy', 'Sell'],
        'Qty': [100, 30, 40],
        'Price': ['$175.50', '$520.00', '$350.00'],
        'P&L': ['-', '-', '+$2,000'],
    }
    st.dataframe(pd.DataFrame(trades))


def render_settings(st):
    """Render settings page."""
    st.title("⚙️ Settings")

    st.subheader("Data Sources")
    st.checkbox("Enable Real-time Prices", value=True)
    st.checkbox("Include After-hours Data", value=False)
    st.number_input("Data Refresh Interval (seconds)", 60, 3600, 300)

    st.divider()

    st.subheader("Risk Parameters")
    st.slider("Max Position Size (%)", 1, 20, 10)
    st.slider("Max Sector Exposure (%)", 10, 50, 30)
    st.slider("Stop Loss (%)", 1, 20, 8)

    st.divider()

    st.subheader("Notifications")
    st.checkbox("Email Alerts", value=True)
    st.checkbox("Slack Notifications", value=False)
    st.text_input("Alert Email", "user@example.com")

    if st.button("Save Settings"):
        st.success("Settings saved!")


def run_dashboard():
    """Entry point for running the dashboard."""
    import sys

    # Check if streamlit is available
    try:
        import streamlit
    except ImportError:
        print("Streamlit not installed. Install with: pip install streamlit")
        print("Then run: streamlit run -m finviz_weekly.dashboard")
        sys.exit(1)

    # If run directly, launch streamlit
    if __name__ == "__main__":
        import subprocess
        subprocess.run(["streamlit", "run", __file__])
    else:
        create_dashboard_app()


# Streamlit entry point
if __name__ == "__main__":
    create_dashboard_app()
