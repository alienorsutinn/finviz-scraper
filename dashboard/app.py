"""
Streamlit dashboard for Finviz Weekly screening results.

Run with: streamlit run dashboard/app.py
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

st.set_page_config(
    page_title="Finviz Weekly Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title and description
st.title("📊 Finviz Weekly Screening Dashboard")
st.markdown("Interactive analysis of stock screening results with enhanced data insights")

# Sidebar - Data loading
st.sidebar.header("Data Selection")

data_dir = Path("data/latest")
scored_path = data_dir / "finviz_scored.parquet"

if not scored_path.exists():
    st.error(f"Data not found at {scored_path}. Run screening first!")
    st.stop()

# Load data
@st.cache_data
def load_data():
    df = pd.read_parquet(scored_path)
    return df

df = load_data()
st.sidebar.success(f"Loaded {len(df)} stocks")

# Check for enhanced data
has_insider = "insider_net_value" in df.columns
has_earnings = "earnings_avg_alpha" in df.columns
has_financials = "net_margin" in df.columns and "roe" in df.columns

st.sidebar.markdown("**Enhanced Data Available:**")
st.sidebar.markdown(f"- Insider: {'✅' if has_insider else '❌'}")
st.sidebar.markdown(f"- Earnings: {'✅' if has_earnings else '❌'}")
st.sidebar.markdown(f"- Financials: {'✅' if has_financials else '❌'}")

# Sidebar filters
st.sidebar.header("Filters")

# Sector filter
sectors = sorted(df["sector"].dropna().unique()) if "sector" in df.columns else []
selected_sectors = st.sidebar.multiselect("Sectors", sectors, default=[])

# Market cap filter
if "market_cap" in df.columns:
    df["_market_cap_numeric"] = df["market_cap"].apply(
        lambda x: float(str(x).replace("B", "e9").replace("M", "e6").replace("K", "e3")) if pd.notna(x) else 0
    )
    min_mcap = st.sidebar.slider(
        "Min Market Cap (B)",
        0.0, 100.0, 1.0, 0.5,
        format="$%.1fB"
    )
    df_filtered = df[df["_market_cap_numeric"] >= min_mcap * 1e9]
else:
    df_filtered = df

# Apply sector filter
if selected_sectors:
    df_filtered = df_filtered[df_filtered["sector"].isin(selected_sectors)]

st.sidebar.markdown(f"**{len(df_filtered)} stocks after filters**")

# Main content tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📋 Overview",
    "🏢 Insider Trading",
    "📈 Earnings Quality",
    "💪 Financial Health",
    "🎯 Screening Results"
])

# Tab 1: Overview
with tab1:
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Stocks", len(df_filtered))
    with col2:
        avg_score = df_filtered["score_master"].mean() if "score_master" in df_filtered.columns else 0
        st.metric("Avg Master Score", f"{avg_score:.1f}")
    with col3:
        if has_insider:
            net_buyers = (df_filtered["insider_net_value"] > 0).sum()
            st.metric("Net Insider Buyers", net_buyers)
        else:
            st.metric("Enhanced Data", "Not Available")
    with col4:
        if "zone_label" in df_filtered.columns:
            aggressive = (df_filtered["zone_label"].str.upper() == "AGGRESSIVE").sum()
            st.metric("Aggressive Zone", aggressive)

    st.subheader("Sector Distribution")
    if "sector" in df_filtered.columns:
        sector_counts = df_filtered["sector"].value_counts().head(10)
        fig = px.bar(
            x=sector_counts.index,
            y=sector_counts.values,
            labels={"x": "Sector", "y": "Count"},
            title="Top 10 Sectors"
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Score Distribution")
    if "score_master" in df_filtered.columns:
        fig = px.histogram(
            df_filtered,
            x="score_master",
            nbins=50,
            title="Master Score Distribution",
            labels={"score_master": "Master Score"}
        )
        st.plotly_chart(fig, use_container_width=True)

# Tab 2: Insider Trading
with tab2:
    if has_insider:
        st.subheader("🏢 Insider Trading Analysis")

        insider_df = df_filtered[
            df_filtered["insider_net_value"].notna() &
            (df_filtered["insider_net_value"] != 0)
        ].copy()

        if not insider_df.empty:
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("### Top 10 Insider Buying")
                top_buying = insider_df.nlargest(10, "insider_net_value")[
                    ["ticker", "company", "sector", "insider_net_value", "insider_total_buys", "insider_total_sells"]
                ]
                st.dataframe(top_buying, use_container_width=True)

                # Insider buying chart
                fig = px.bar(
                    top_buying,
                    x="ticker",
                    y="insider_net_value",
                    title="Top Insider Buying (Net Value)",
                    labels={"insider_net_value": "Net Value ($)"}
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("### Top 10 Insider Selling")
                top_selling = insider_df.nsmallest(10, "insider_net_value")[
                    ["ticker", "company", "sector", "insider_net_value", "insider_total_buys", "insider_total_sells"]
                ]
                st.dataframe(top_selling, use_container_width=True)

                # Insider selling chart
                fig = px.bar(
                    top_selling,
                    x="ticker",
                    y="insider_net_value",
                    title="Top Insider Selling (Net Value)",
                    labels={"insider_net_value": "Net Value ($)"},
                    color_discrete_sequence=["red"]
                )
                st.plotly_chart(fig, use_container_width=True)

            # Insider activity heatmap by sector
            if "sector" in insider_df.columns:
                st.subheader("Insider Activity by Sector")
                sector_insider = insider_df.groupby("sector").agg({
                    "insider_net_value": "sum",
                    "insider_total_buys": "sum",
                    "insider_total_sells": "sum"
                }).reset_index()

                fig = px.bar(
                    sector_insider,
                    x="sector",
                    y=["insider_total_buys", "insider_total_sells"],
                    title="Insider Transactions by Sector",
                    barmode="group"
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No insider activity in filtered data")
    else:
        st.warning("Insider data not available. Run with `--include-insider` flag.")

# Tab 3: Earnings Quality
with tab3:
    if has_earnings:
        st.subheader("📈 Earnings Quality Analysis")

        earnings_df = df_filtered[
            df_filtered["earnings_avg_alpha"].notna() &
            (df_filtered["earnings_total_events"].fillna(0) >= 2)
        ].copy()

        if not earnings_df.empty:
            st.markdown("### Top Consistent Earnings Beaters")
            top_earnings = earnings_df.nlargest(20, "earnings_avg_alpha")[
                ["ticker", "company", "sector", "earnings_avg_alpha", "earnings_win_rate", "earnings_total_events"]
            ]
            top_earnings["earnings_avg_alpha"] = (top_earnings["earnings_avg_alpha"] * 100).round(2)
            top_earnings["earnings_win_rate"] = (top_earnings["earnings_win_rate"] * 100).round(0)
            st.dataframe(top_earnings, use_container_width=True)

            # Earnings alpha distribution
            fig = px.scatter(
                earnings_df,
                x="earnings_avg_alpha",
                y="earnings_win_rate",
                color="sector" if "sector" in earnings_df.columns else None,
                size="earnings_total_events",
                hover_data=["ticker", "company"],
                title="Earnings Alpha vs Win Rate",
                labels={
                    "earnings_avg_alpha": "Avg Alpha vs SPY",
                    "earnings_win_rate": "Win Rate",
                    "earnings_total_events": "Events"
                }
            )
            st.plotly_chart(fig, use_container_width=True)

            # Earnings quality by sector
            if "sector" in earnings_df.columns:
                st.subheader("Earnings Quality by Sector")
                sector_earnings = earnings_df.groupby("sector").agg({
                    "earnings_avg_alpha": "mean",
                    "earnings_win_rate": "mean"
                }).reset_index()
                sector_earnings["earnings_avg_alpha"] *= 100

                fig = px.bar(
                    sector_earnings,
                    x="sector",
                    y="earnings_avg_alpha",
                    title="Average Earnings Alpha by Sector (%)",
                    labels={"earnings_avg_alpha": "Avg Alpha (%)"}
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No earnings data in filtered results")
    else:
        st.warning("Earnings data not available. Run with `--include-earnings` flag.")

# Tab 4: Financial Health
with tab4:
    if has_financials:
        st.subheader("💪 Financial Health Analysis")

        financial_df = df_filtered[
            df_filtered["net_margin"].notna() &
            df_filtered["roe"].notna() &
            df_filtered["current_ratio"].notna()
        ].copy()

        if not financial_df.empty:
            # Calculate composite score
            financial_df["health_score"] = (
                financial_df["net_margin"].clip(0, 0.3) / 0.3 * 33.3 +
                financial_df["roe"].clip(0, 0.3) / 0.3 * 33.3 +
                financial_df["current_ratio"].clip(0, 3) / 3 * 33.3
            )

            st.markdown("### Top Financial Health")
            top_health = financial_df.nlargest(20, "health_score")[
                ["ticker", "company", "sector", "net_margin", "roe", "current_ratio", "debt_to_equity"]
            ]
            top_health["net_margin"] = (top_health["net_margin"] * 100).round(1)
            top_health["roe"] = (top_health["roe"] * 100).round(1)
            st.dataframe(top_health, use_container_width=True)

            # 3D scatter: Margin vs ROE vs Current Ratio
            fig = px.scatter_3d(
                financial_df,
                x="net_margin",
                y="roe",
                z="current_ratio",
                color="sector" if "sector" in financial_df.columns else None,
                hover_data=["ticker", "company"],
                title="Financial Health 3D View",
                labels={
                    "net_margin": "Net Margin",
                    "roe": "ROE",
                    "current_ratio": "Current Ratio"
                }
            )
            st.plotly_chart(fig, use_container_width=True)

            # Financial metrics distribution
            col1, col2 = st.columns(2)
            with col1:
                fig = px.histogram(
                    financial_df,
                    x="net_margin",
                    nbins=50,
                    title="Net Margin Distribution"
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.histogram(
                    financial_df,
                    x="roe",
                    nbins=50,
                    title="ROE Distribution"
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No financial data in filtered results")
    else:
        st.warning("Financial data not available. Run with `--include-financials` flag.")

# Tab 5: Screening Results
with tab5:
    st.subheader("🎯 Top Screening Results")

    # Load top lists
    top_files = list(data_dir.glob("top50_*.csv"))

    if top_files:
        # Group by theme family
        traditional_themes = []
        enhanced_themes = []

        for f in top_files:
            theme_name = f.stem.replace("top50_", "")
            if theme_name in ["insider_momentum", "earnings_surprise", "quality_growth_enhanced", "enhanced_master"]:
                enhanced_themes.append(theme_name)
            else:
                traditional_themes.append(theme_name)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### Traditional Themes")
            selected_trad = st.selectbox("Select theme", traditional_themes) if traditional_themes else None

            if selected_trad:
                theme_df = pd.read_csv(data_dir / f"top50_{selected_trad}.csv")
                st.dataframe(theme_df.head(20), use_container_width=True)

        with col2:
            st.markdown("### Enhanced Themes")
            if enhanced_themes:
                selected_enh = st.selectbox("Select enhanced theme", enhanced_themes)
                theme_df = pd.read_csv(data_dir / f"top50_{selected_enh}.csv")
                st.dataframe(theme_df.head(20), use_container_width=True)
            else:
                st.info("No enhanced themes available. Run screening with enhanced data.")

        # Conviction analysis
        st.subheader("High Conviction Stocks")
        conviction_path = data_dir / "conviction_2plus.csv"
        if conviction_path.exists():
            conviction_df = pd.read_csv(conviction_path)
            st.markdown(f"**{len(conviction_df)} stocks appearing in 2+ theme families**")
            st.dataframe(conviction_df.head(30), use_container_width=True)
    else:
        st.warning("No screening results found. Run `screen` command first.")

# Footer
st.markdown("---")
st.markdown("**Finviz Weekly** | Enhanced Stock Screening & Analysis")
