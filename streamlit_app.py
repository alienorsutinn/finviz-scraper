import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st


DATA_DIR = Path("data")
LATEST_DIR = DATA_DIR / "latest"
HISTORY_PATH = DATA_DIR / "history" / "finviz_fundamentals_history.parquet"


@st.cache_data(show_spinner=False)
def load_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


@st.cache_data(show_spinner=False)
def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def load_weights() -> dict | None:
    p = LATEST_DIR / "learned_weights.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def safe_colmap(df: pd.DataFrame) -> dict[str, str]:
    return {c.lower(): c for c in df.columns}


def get_col(df: pd.DataFrame, name_lower: str) -> str | None:
    return safe_colmap(df).get(name_lower)


def section_header(title: str, subtitle: str | None = None):
    st.markdown(f"## {title}")
    if subtitle:
        st.caption(subtitle)


def main():
    st.set_page_config(page_title="Finviz Weekly Dashboard", layout="wide")

    st.title("Finviz Weekly — Screens & Candidates")
    st.caption("Reads from data/latest + data/history. Use this to browse ranked lists and drill into tickers.")

    # Load core datasets
    scored_path = LATEST_DIR / "finviz_scored.parquet"
    latest_path = LATEST_DIR / "finviz_fundamentals.parquet"

    if not scored_path.exists():
        st.error(f"Missing {scored_path}. Run: python -m finviz_weekly screen --out data ...")
        st.stop()

    scored = load_parquet(scored_path)

    weights = load_weights()
    mode = (weights or {}).get("mode", "unknown") if weights else "missing"

    # Sidebar controls
    st.sidebar.header("Filters")
    ticker_search = st.sidebar.text_input("Ticker contains", value="").strip().upper()

    sector_col = get_col(scored, "sector")
    industry_col = get_col(scored, "industry")

    sector_vals = sorted([x for x in scored[sector_col].dropna().unique().tolist()]) if sector_col else []
    industry_vals = sorted([x for x in scored[industry_col].dropna().unique().tolist()]) if industry_col else []

    sector_pick = st.sidebar.multiselect("Sector", sector_vals, default=[])
    industry_pick = st.sidebar.multiselect("Industry", industry_vals, default=[])

    show_only_ok = st.sidebar.checkbox("Only __status == ok (if present)", value=True)

    # Apply filters
    df = scored.copy()
    if show_only_ok and "__status" in df.columns:
        df = df[df["__status"] == "ok"].copy()

    if ticker_search:
        df = df[df["ticker"].astype(str).str.upper().str.contains(ticker_search)].copy()

    if sector_pick and sector_col:
        df = df[df[sector_col].isin(sector_pick)].copy()

    if industry_pick and industry_col:
        df = df[df[industry_col].isin(industry_pick)].copy()

    # Overview row
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Rows (filtered)", f"{len(df):,}")
    c2.metric("Rows (total)", f"{len(scored):,}")
    c3.metric("Weights mode", mode)
    # best-effort as_of_date
    as_of = None
    if "as_of_date" in scored.columns:
        as_of = str(scored["as_of_date"].iloc[0])
    c4.metric("as_of_date", as_of or "unknown")

    # Show weights
    with st.expander("Weights (learned/fallback)"):
        if not weights:
            st.write("No learned_weights.json found.")
        else:
            st.json(weights)

    # Tabs
    tab_overview, tab_screens, tab_candidates, tab_ticker = st.tabs(
        ["Overview", "Screens", "Candidates", "Ticker Drilldown"]
    )

    with tab_overview:
        section_header("Scored Snapshot", "This is data/latest/finviz_scored.parquet (filtered by sidebar).")
        # sensible columns first
        preferred = [
            "ticker", "company", "sector", "industry", "market_cap", "price",
            "score_quality", "score_value", "score_risk", "score_growth", "score_momentum", "score_oversold",
            "score_quality_value", "score_oversold_quality", "score_compounders", "score_learned"
        ]
        cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
        st.dataframe(df[cols], use_container_width=True, height=520)

    with tab_screens:
        section_header("Top Lists", "These are exported CSVs in data/latest/ (if present).")
        files = [
            ("Quality + Value", "top60_quality_value.csv"),
            ("Oversold + Quality", "top60_oversold_quality.csv"),
            ("Compounders", "top60_compounders.csv"),
            ("Learned Composite", "top60_learned.csv"),
        ]
        for label, fname in files:
            p = LATEST_DIR / fname
            if p.exists():
                st.subheader(label)
                top = load_csv(p)
                st.dataframe(top, use_container_width=True, height=260)
            else:
                st.info(f"Missing {fname} (run screen with --top 60).")

    with tab_candidates:
        section_header("Candidates", "Union lists + your split lists.")
        cand_files = [
            ("Union candidates", "candidates.txt"),
            ("Operating candidates", "candidates_operating.txt"),
            ("BDC candidates", "candidates_bdc.txt"),
            ("Asset managers candidates", "candidates_asset_managers.txt"),
        ]
        for label, fname in cand_files:
            p = LATEST_DIR / fname
            st.subheader(label)
            if p.exists():
                ticks = [t.strip() for t in p.read_text().splitlines() if t.strip()]
                st.write(f"{len(ticks)} tickers")
                st.code(", ".join(ticks[:200]) + (" ..." if len(ticks) > 200 else ""))
                st.download_button(
                    label=f"Download {fname}",
                    data="\n".join(ticks) + "\n",
                    file_name=fname,
                    mime="text/plain",
                )
            else:
                st.info(f"Missing {fname}")

    with tab_ticker:
        section_header("Ticker Drilldown", "Pick a ticker to view its snapshot row + history (if available).")

        tickers = sorted(df["ticker"].astype(str).str.upper().unique().tolist())
        pick = st.selectbox("Ticker", options=tickers, index=0 if tickers else None)

        if pick:
            row = scored[scored["ticker"].astype(str).str.upper() == pick].head(1)
            if row.empty:
                st.warning("No row found in scored snapshot.")
            else:
                st.subheader(f"{pick} — Snapshot")
                st.dataframe(row.T, use_container_width=True)

            # history charts
            if HISTORY_PATH.exists():
                hist = load_parquet(HISTORY_PATH)
                if "ticker" in hist.columns:
                    h = hist[hist["ticker"].astype(str).str.upper() == pick].copy()
                    if "as_of_date" in h.columns:
                        h["as_of_date"] = pd.to_datetime(h["as_of_date"], errors="coerce")
                        h = h.dropna(subset=["as_of_date"]).sort_values("as_of_date")

                        st.subheader(f"{pick} — History")
                        st.caption("Will become useful once you have multiple snapshot dates.")

                        # price chart if present
                        price_col = get_col(h, "price")
                        if price_col:
                            chart = (
                                alt.Chart(h)
                                .mark_line()
                                .encode(
                                    x="as_of_date:T",
                                    y=alt.Y(f"{price_col}:Q", title="Price"),
                                    tooltip=["as_of_date:T", alt.Tooltip(f"{price_col}:Q", title="Price")],
                                )
                                .properties(height=240)
                            )
                            st.altair_chart(chart, use_container_width=True)

                        # score columns chart if present (from scoring on history, not stored by default)
                        score_cols = [c for c in ["score_quality","score_value","score_risk","score_growth"] if c in h.columns]
                        if score_cols:
                            long = h[["as_of_date"] + score_cols].melt("as_of_date", var_name="score", value_name="value")
                            chart2 = (
                                alt.Chart(long)
                                .mark_line()
                                .encode(
                                    x="as_of_date:T",
                                    y="value:Q",
                                    color="score:N",
                                    tooltip=["as_of_date:T", "score:N", "value:Q"],
                                )
                                .properties(height=240)
                            )
                            st.altair_chart(chart2, use_container_width=True)
                        else:
                            st.info("History does not yet include score_* columns (that’s OK).")
                    else:
                        st.info("History exists but has no as_of_date.")
                else:
                    st.info("History exists but has no ticker column.")
            else:
                st.info("No history parquet found yet (data/history/...).")

    st.caption("Tip: run daily/weekly to accumulate history; learning becomes real once you have >1 as_of_date.")


if __name__ == "__main__":
    main()
