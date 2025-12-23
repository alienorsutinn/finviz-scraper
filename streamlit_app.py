from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st


# ----------------------------
# Helpers
# ----------------------------
def _safe_read_parquet(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception as e:
        st.error(f"Failed to read parquet: {path}\n\n{e}")
        return None


def _safe_read_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception as e:
        st.error(f"Failed to read CSV: {path}\n\n{e}")
        return None


def _list_run_dates(runs_dir: Path) -> List[str]:
    if not runs_dir.exists():
        return []
    dates = []
    for p in runs_dir.iterdir():
        if p.is_dir() and p.name[:4].isdigit():
            dates.append(p.name)
    return sorted(dates, reverse=True)


def _resolve_data_dir(base_out: Path, selection: str) -> Path:
    if selection == "latest":
        return base_out / "latest"
    return base_out / "runs" / selection


def _discover_top_lists(dir_path: Path) -> List[Path]:
    return sorted(dir_path.glob("top*_*.csv"))


def _label_for_top_csv(path: Path) -> str:
    # e.g. top60_operating_garp.csv -> operating_garp
    name = path.stem
    if name.startswith("top"):
        parts = name.split("_", 1)
        if len(parts) == 2:
            return parts[1]
    return name


def _format_money(x: object) -> str:
    try:
        v = float(x)
    except Exception:
        return str(x)
    if v >= 1e12:
        return f"{v/1e12:.2f}T"
    if v >= 1e9:
        return f"{v/1e9:.2f}B"
    if v >= 1e6:
        return f"{v/1e6:.2f}M"
    if v >= 1e3:
        return f"{v/1e3:.2f}K"
    return f"{v:.2f}"


def _maybe_add_dividend_yield(df: pd.DataFrame) -> pd.DataFrame:
    # Finviz sometimes lacks dividend yield, but you have dividend_ttm and price.
    if "dividend_yield" in df.columns:
        return df
    if "dividend_ttm" in df.columns and "price" in df.columns:
        out = df.copy()
        with pd.option_context("mode.use_inf_as_na", True):
            out["dividend_yield"] = pd.to_numeric(out["dividend_ttm"], errors="coerce") / pd.to_numeric(
                out["price"], errors="coerce"
            )
        return out
    return df


def _key_fields(df: pd.DataFrame) -> List[str]:
    wanted = [
        "ticker",
        "company",
        "sector",
        "industry",
        "country",
        "market_cap",
        "price",
        "p_e",
        "eps_(ttm)",
        "profit_margin",
        "oper._margin",
        "gross_margin",
        "debt_eq",
        "lt_debt_eq",
        "beta",
        "volatility_m",
        "avg_volume",
        "rel_volume",
        "short_float",
        "short_ratio",
        "dividend_ttm",
        "dividend_yield",
        "dividend_gr._3_5y",
        "score_value",
        "score_quality",
        "score_risk",
        "score_learned",
    ]
    return [c for c in wanted if c in df.columns]


def _ticker_links(ticker: str) -> Dict[str, str]:
    t = ticker.upper().strip()
    return {
        "Finviz": f"https://finviz.com/quote.ashx?t={t}",
        "TradingView": f"https://www.tradingview.com/symbols/{t}/",
        "Yahoo Finance": f"https://finance.yahoo.com/quote/{t}/",
    }


# ----------------------------
# Streamlit App
# ----------------------------
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="data", help="Base data folder (default: data)")
    args, _ = parser.parse_known_args()

    base_out = Path(args.out).resolve()
    runs_dir = base_out / "runs"

    st.set_page_config(page_title="Finviz Weekly Dashboard", layout="wide")
    st.title("Finviz Weekly — Dashboard")

    run_dates = _list_run_dates(runs_dir)
    choices = ["latest"] + run_dates

    with st.sidebar:
        st.header("Data source")
        selection = st.selectbox("Select run", choices, index=0)
        data_dir = _resolve_data_dir(base_out, selection)

        st.caption(f"Using: `{data_dir}`")

        refresh = st.button("Reload data")

    # reload trigger
    if refresh:
        st.cache_data.clear()

    @st.cache_data(show_spinner=False)
    def load_scored(dir_path: Path) -> Optional[pd.DataFrame]:
        return _safe_read_parquet(dir_path / "finviz_scored.parquet")

    @st.cache_data(show_spinner=False)
    def load_conviction(dir_path: Path) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        c2 = _safe_read_csv(dir_path / "conviction_2plus.csv")
        c3 = _safe_read_csv(dir_path / "conviction_3plus.csv")
        return c2, c3

    @st.cache_data(show_spinner=False)
    def load_report_md(dir_path: Path) -> Optional[str]:
        p = dir_path / "report.md"
        if not p.exists():
            return None
        try:
            return p.read_text(encoding="utf-8")
        except Exception:
            return p.read_text(errors="ignore")

    scored = load_scored(data_dir)
    if scored is None or scored.empty:
        st.warning("No `finviz_scored.parquet` found for this run. Run `python -m finviz_weekly screen ...` first.")
        return

    scored = _maybe_add_dividend_yield(scored)

    c2, c3 = load_conviction(data_dir)
    report_md = load_report_md(data_dir)

    # Top-level KPIs
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Rows", f"{len(scored):,}")
    with col2:
        st.metric("Unique tickers", f"{scored['ticker'].nunique():,}" if "ticker" in scored.columns else "—")
    with col3:
        st.metric("Sectors", f"{scored['sector'].nunique():,}" if "sector" in scored.columns else "—")
    with col4:
        st.metric("Top lists", f"{len(_discover_top_lists(data_dir)):,}")

    tabs = st.tabs(["Overview", "Screens", "Conviction", "Ticker Card"])

    # ----------------------------
    # Overview
    # ----------------------------
    with tabs[0]:
        left, right = st.columns([1, 1])
        with left:
            st.subheader("Sector mix")
            if "sector" in scored.columns:
                sector_counts = scored["sector"].fillna("Unknown").value_counts().head(20)
                st.bar_chart(sector_counts)
            else:
                st.info("No `sector` column found.")

        with right:
            st.subheader("Asset type mix")
            if "asset_type" in scored.columns:
                at_counts = scored["asset_type"].fillna("Unknown").value_counts()
                st.bar_chart(at_counts)
            else:
                st.info("No `asset_type` column found.")

        st.subheader("Report")
        if report_md:
            st.markdown(report_md)
        else:
            st.info("No `report.md` found for this run (you can still use Screens/Conviction).")

    # ----------------------------
    # Screens
    # ----------------------------
    with tabs[1]:
        top_csvs = _discover_top_lists(data_dir)
        if not top_csvs:
            st.warning("No `top*_*.csv` files found in this run directory.")
        else:
            labels = [_label_for_top_csv(p) for p in top_csvs]
            label = st.selectbox("Select screen", labels, index=0)
            chosen_path = top_csvs[labels.index(label)]
            screen_df = _safe_read_csv(chosen_path)

            st.caption(f"File: `{chosen_path.name}`")

            if screen_df is not None and not screen_df.empty:
                # nicer formatting
                view = screen_df.copy()
                if "market_cap" in view.columns:
                    view["market_cap"] = view["market_cap"].apply(_format_money)
                st.dataframe(view, use_container_width=True, height=600)

                if "ticker" in view.columns:
                    tickers = "\n".join([str(t).strip().upper() for t in view["ticker"].tolist() if str(t).strip()])
                    st.download_button(
                        "Download tickers.txt",
                        data=(tickers + "\n"),
                        file_name=f"{label}_tickers.txt",
                        mime="text/plain",
                    )

    # ----------------------------
    # Conviction
    # ----------------------------
    with tabs[2]:
        if c2 is None or c2.empty:
            st.warning("No conviction file found (conviction_2plus.csv).")
        else:
            st.subheader("Consensus shortlist (appears across multiple screens)")
            min_count = st.slider("Minimum list count", min_value=2, max_value=20, value=3)
            dfc = c2.copy()

            if "count" in dfc.columns:
                dfc = dfc[dfc["count"] >= min_count]

            # Join in sector/industry/asset_type from scored for filtering
            if "ticker" in dfc.columns and "ticker" in scored.columns:
                meta_cols = [c for c in ["ticker", "sector", "industry", "asset_type", "market_cap", "price"] if c in scored.columns]
                meta = scored[meta_cols].drop_duplicates("ticker")
                dfc = dfc.merge(meta, on="ticker", how="left")

            cols = st.columns(3)
            with cols[0]:
                sectors = sorted([s for s in dfc["sector"].dropna().unique()]) if "sector" in dfc.columns else []
                sector_filter = st.multiselect("Sector filter", sectors, default=[])
            with cols[1]:
                asset_types = sorted([a for a in dfc["asset_type"].dropna().unique()]) if "asset_type" in dfc.columns else []
                asset_filter = st.multiselect("Asset type filter", asset_types, default=[])
            with cols[2]:
                max_per_sector = st.number_input("Max per sector (0 = no cap)", min_value=0, max_value=100, value=0)

            if sector_filter and "sector" in dfc.columns:
                dfc = dfc[dfc["sector"].isin(sector_filter)]
            if asset_filter and "asset_type" in dfc.columns:
                dfc = dfc[dfc["asset_type"].isin(asset_filter)]

            # Apply sector cap by highest count then (optionally) score_learned
            sort_cols = []
            if "count" in dfc.columns:
                sort_cols.append("count")
            if "score_learned" in dfc.columns:
                sort_cols.append("score_learned")
            if sort_cols:
                dfc = dfc.sort_values(sort_cols, ascending=False)

            if max_per_sector and "sector" in dfc.columns:
                dfc = (
                    dfc.groupby("sector", dropna=False, as_index=False)
                    .head(int(max_per_sector))
                    .reset_index(drop=True)
                )

            st.dataframe(dfc, use_container_width=True, height=650)

            if "ticker" in dfc.columns:
                tickers = "\n".join([str(t).strip().upper() for t in dfc["ticker"].tolist() if str(t).strip()])
                st.download_button(
                    "Download conviction tickers.txt",
                    data=(tickers + "\n"),
                    file_name=f"conviction_min{min_count}.txt",
                    mime="text/plain",
                )

    # ----------------------------
    # Ticker Card
    # ----------------------------
    with tabs[3]:
        if "ticker" not in scored.columns:
            st.warning("No ticker column found in scored data.")
        else:
            tickers = sorted(scored["ticker"].dropna().astype(str).str.upper().unique().tolist())
            t = st.selectbox("Select ticker", tickers, index=0)
            row = scored[scored["ticker"].astype(str).str.upper() == t].head(1)
            if row.empty:
                st.warning("Ticker not found in this run.")
            else:
                r = row.iloc[0].to_dict()
                st.subheader(f"{t} — {r.get('company', '')}")

                # Links
                links = _ticker_links(t)
                link_cols = st.columns(len(links))
                for i, (name, url) in enumerate(links.items()):
                    with link_cols[i]:
                        st.link_button(name, url)

                # Key metrics
                m1, m2, m3, m4 = st.columns(4)
                with m1:
                    st.metric("Price", f"{r.get('price', '—')}")
                with m2:
                    st.metric("Market Cap", _format_money(r.get("market_cap", "—")))
                with m3:
                    st.metric("P/E", f"{r.get('p_e', '—')}")
                with m4:
                    dy = r.get("dividend_yield", None)
                    st.metric("Dividend Yield", f"{(float(dy)*100):.2f}%" if dy == dy else "—")  # dy==dy checks NaN

                # Membership (from conviction file if available)
                membership = None
                if c2 is not None and "ticker" in c2.columns:
                    m = c2[c2["ticker"].astype(str).str.upper() == t]
                    if not m.empty:
                        membership = m.iloc[0].to_dict()

                left, right = st.columns([1, 1])
                with left:
                    st.markdown("**Profile**")
                    prof = {k: r.get(k) for k in ["sector", "industry", "country", "asset_type"] if k in scored.columns}
                    st.json(prof)

                    st.markdown("**Factors / scores**")
                    score_keys = [k for k in ["score_value", "score_quality", "score_risk", "score_learned"] if k in scored.columns]
                    st.json({k: r.get(k) for k in score_keys})

                with right:
                    st.markdown("**Key fields**")
                    keys = _key_fields(scored)
                    card = {k: r.get(k) for k in keys}
                    # format a couple of big ones
                    if "market_cap" in card:
                        card["market_cap"] = _format_money(card["market_cap"])
                    st.dataframe(pd.DataFrame([card]), use_container_width=True)

                    if membership:
                        st.markdown("**Appears in**")
                        st.write(f"Count: **{membership.get('count', '—')}**")
                        st.code(str(membership.get("lists", "")))


if __name__ == "__main__":
    main()
