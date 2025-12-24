from __future__ import annotations

import math
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd


LATEST_DIR = Path("data/latest")
IN_SCORED = LATEST_DIR / "finviz_scored.parquet"          # base scored file (ticker/price/sector/industry etc)
IN_FUND = LATEST_DIR / "finviz_fundamentals.parquet"      # raw finviz fundamentals/ratios (if present)

OUT_PARQUET = LATEST_DIR / "finviz_scored.parquet"
OUT_CSV_GZ = LATEST_DIR / "finviz_scored.csv.gz"
OUT_BUY = LATEST_DIR / "buy_candidates.csv"


def _pick_col(df: pd.DataFrame, names: Iterable[str]) -> Optional[str]:
    for n in names:
        if n in df.columns:
            return n
    # try case-insensitive exact matches
    lower_map = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower_map:
            return lower_map[n.lower()]
    return None


def _parse_num(s: pd.Series) -> pd.Series:
    """Parse Finviz-style numbers: '-', '12.3%', '1.2B', '450M', '30K' into floats."""
    if s is None:
        return s
    if pd.api.types.is_numeric_dtype(s):
        return s.astype(float)

    x = s.astype(str).str.strip()
    x = x.replace({"": np.nan, "-": np.nan, "None": np.nan, "nan": np.nan})

    # percent
    is_pct = x.str.endswith("%", na=False)
    x_pct = x.where(~is_pct, x.str.rstrip("%"))

    # suffix multipliers
    mult = pd.Series(1.0, index=x_pct.index)
    for suf, m in [("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)]:
        mask = x_pct.str.endswith(suf, na=False)
        mult = mult.where(~mask, m)
        x_pct = x_pct.where(~mask, x_pct.str.rstrip(suf))

    # remove commas
    x_pct = x_pct.str.replace(",", "", regex=False)

    out = pd.to_numeric(x_pct, errors="coerce") * mult
    # convert percent to numeric percent (keep as 12.3 not 0.123) — we treat as “percentage points”
    out = out.where(~is_pct, out)
    return out.astype(float)


def _robust_z(x: pd.Series) -> pd.Series:
    """Robust z-score using MAD; returns 0 where insufficient info."""
    x = x.astype(float)
    med = x.median(skipna=True)
    mad = (x - med).abs().median(skipna=True)
    if not np.isfinite(mad) or mad == 0:
        return pd.Series(0.0, index=x.index)
    return (x - med) / (1.4826 * mad)


def _ensure_cols(df: pd.DataFrame, mapping: dict[str, list[str]]) -> dict[str, str]:
    found = {}
    for key, cands in mapping.items():
        col = _pick_col(df, cands)
        if col:
            found[key] = col
    return found


def main() -> None:
    if not IN_SCORED.exists():
        raise SystemExit(f"Missing {IN_SCORED}. Run your scoring step first.")

    base = pd.read_parquet(IN_SCORED)
    base.columns = [c.strip() for c in base.columns]

    # Bring in raw finviz fundamentals if available (recommended)
    if IN_FUND.exists():
        fund = pd.read_parquet(IN_FUND)
        fund.columns = [c.strip() for c in fund.columns]
        # normalize ticker name
        tcol_f = _pick_col(fund, ["ticker", "Ticker", "symbol", "Symbol"])
        if not tcol_f:
            raise SystemExit(f"Could not find ticker column in {IN_FUND}")
        fund = fund.rename(columns={tcol_f: "ticker"})
        fund["ticker"] = fund["ticker"].astype(str).str.upper().str.strip()
        # merge (base wins for price/sector/industry if already present)
        base_tcol = _pick_col(base, ["ticker", "Ticker", "symbol", "Symbol"])
        if not base_tcol:
            raise SystemExit(f"Could not find ticker column in {IN_SCORED}")
        base = base.rename(columns={base_tcol: "ticker"})
        base["ticker"] = base["ticker"].astype(str).str.upper().str.strip()
        df = base.merge(fund, on="ticker", how="left", suffixes=("", "_fund"))
    else:
        df = base.copy()
        if "ticker" not in df.columns:
            tcol = _pick_col(df, ["Ticker", "symbol", "Symbol"])
            if tcol:
                df = df.rename(columns={tcol: "ticker"})
        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()

    # Identify core columns (robust)
    colmap = _ensure_cols(
        df,
        {
            "price": ["price", "Price"],
            "sector": ["sector", "Sector"],
            "industry": ["industry", "Industry"],
            "pe": ["P/E", "P/E (ttm)", "PE", "pe", "pe_ttm", "Price/Earnings"],
            "pb": ["P/B", "P/BV", "PB", "pb", "Price/Book"],
            "ps": ["P/S", "PS", "ps", "Price/Sales", "P/Sales"],
            "eps_ttm": ["EPS (ttm)", "EPS ttm", "eps", "epsTTM", "EPS"],
            "roe": ["ROE", "roe", "ROE %"],
            "debt_eq": ["Debt/Eq", "Debt/Equity", "debt_eq", "Debt/Eq."],
            "eps_g5": ["EPS next 5Y", "EPS next 5y", "EPS growth next 5 years", "EPS Next 5Y"],
            "sales_g5": ["Sales past 5Y", "Sales past 5y", "Sales growth past 5 years", "Sales Past 5Y"],
        },
    )

    if "price" not in colmap:
        raise SystemExit("Could not find a price column. Expected something like 'price' or 'Price'.")

    # Parse numeric fields
    for k in ["pe", "pb", "ps", "eps_ttm", "roe", "debt_eq", "eps_g5", "sales_g5"]:
        if k in colmap:
            df[k] = _parse_num(df[colmap[k]])
        else:
            df[k] = np.nan

    df["price"] = _parse_num(df[colmap["price"]])

    # group keys
    df["sector"] = df[colmap["sector"]] if "sector" in colmap else "Unknown"
    df["industry"] = df[colmap["industry"]] if "industry" in colmap else "Unknown"
    df["sector"] = df["sector"].astype(str).replace({"nan": "Unknown"}).fillna("Unknown")
    df["industry"] = df["industry"].astype(str).replace({"nan": "Unknown"}).fillna("Unknown")

    # peer medians: industry (if enough), else sector, else global
    def _peer_median(series: pd.Series, key: pd.Series, min_n: int = 25) -> pd.Series:
        med = series.groupby(key).transform("median")
        cnt = series.groupby(key).transform("count")
        return med.where(cnt >= min_n, np.nan)

    pe_med_ind = _peer_median(df["pe"].where(df["pe"] > 0), df["industry"])
    pb_med_ind = _peer_median(df["pb"].where(df["pb"] > 0), df["industry"])
    ps_med_ind = _peer_median(df["ps"].where(df["ps"] > 0), df["industry"])

    pe_med_sec = df["pe"].where(df["pe"] > 0).groupby(df["sector"]).transform("median")
    pb_med_sec = df["pb"].where(df["pb"] > 0).groupby(df["sector"]).transform("median")
    ps_med_sec = df["ps"].where(df["ps"] > 0).groupby(df["sector"]).transform("median")

    pe_peer = pe_med_ind.fillna(pe_med_sec).fillna(df["pe"].median(skipna=True))
    pb_peer = pb_med_ind.fillna(pb_med_sec).fillna(df["pb"].median(skipna=True))
    ps_peer = ps_med_ind.fillna(ps_med_sec).fillna(df["ps"].median(skipna=True))

    # modest adjustments (robust z within sector)
    z_g = _robust_z(df["eps_g5"].groupby(df["sector"]).transform(lambda x: x)).fillna(0.0)
    z_roe = _robust_z(df["roe"].groupby(df["sector"]).transform(lambda x: x)).fillna(0.0)
    z_de = _robust_z(df["debt_eq"].groupby(df["sector"]).transform(lambda x: x)).fillna(0.0)

    # keep it modest
    adj_pe = np.exp(0.12 * z_g + 0.08 * z_roe - 0.08 * z_de)
    adj_pb = np.exp(0.10 * z_roe - 0.08 * z_de)
    adj_ps = np.exp(0.10 * _robust_z(df["sales_g5"]).fillna(0.0) + 0.05 * z_roe)

    adj_pe = np.clip(adj_pe, 0.70, 1.40)
    adj_pb = np.clip(adj_pb, 0.70, 1.40)
    adj_ps = np.clip(adj_ps, 0.70, 1.40)

    pe_target = pe_peer * adj_pe
    pb_target = pb_peer * adj_pb
    ps_target = ps_peer * adj_ps

    # anchors
    eps = df["eps_ttm"]
    eps = eps.where(np.isfinite(eps) & (eps != 0), df["price"] / df["pe"].where(df["pe"] > 0))
    bvps = df["price"] / df["pb"].where(df["pb"] > 0)
    sps = df["price"] / df["ps"].where(df["ps"] > 0)

    wfv_pe = eps * pe_target
    wfv_pb = bvps * pb_target
    wfv_ps = sps * ps_target

    # weights (sector-aware, renormalize for missing anchors)
    is_fin = df["sector"].astype(str).str.lower().str.contains("financial")
    w_pe = np.where(is_fin, 0.35, 0.60)
    w_pb = np.where(is_fin, 0.55, 0.20)
    w_ps = np.where(is_fin, 0.10, 0.20)

    W = pd.DataFrame({"pe": w_pe, "pb": w_pb, "ps": w_ps})
    V = pd.DataFrame({"pe": wfv_pe, "pb": wfv_pb, "ps": wfv_ps})

    valid = V.notna() & np.isfinite(V)
    W = W.where(valid, 0.0)
    wsum = W.sum(axis=1).replace(0.0, np.nan)

    wfv_raw = (V.fillna(0.0) * W).sum(axis=1) / wsum

    # percentile-based clipping of ratio (no hard 4x clamp)
    ratio_raw = (wfv_raw / df["price"]).replace([np.inf, -np.inf], np.nan)

    # sector percentiles (fallback global)
    q_lo = ratio_raw.groupby(df["sector"]).transform(lambda x: x.quantile(0.10))
    q_hi = ratio_raw.groupby(df["sector"]).transform(lambda x: x.quantile(0.90))

    q_lo = q_lo.fillna(ratio_raw.quantile(0.10))
    q_hi = q_hi.fillna(ratio_raw.quantile(0.90))

    ratio_clip = ratio_raw.clip(lower=q_lo, upper=q_hi)
    wfv = ratio_clip * df["price"]

    df["wfv"] = wfv
    df["wfv_method"] = np.where(
        valid["pe"] & ~valid["pb"] & ~valid["ps"], "peer_pe",
        np.where(valid["pb"] & ~valid["pe"] & ~valid["ps"], "peer_pb",
        np.where(valid["ps"] & ~valid["pe"] & ~valid["pb"], "peer_ps", "peer_blend"))
    )

    df["wfv_clamped"] = (ratio_raw.notna()) & (ratio_raw != ratio_clip)

    df["price_to_wfv"] = (df["price"] / df["wfv"]).replace([np.inf, -np.inf], np.nan)
    df["upside_pct"] = (df["wfv"] / df["price"] - 1.0).replace([np.inf, -np.inf], np.nan)

    # zones = margin-of-safety buckets (keep thresholds until we can backtest returns)
    def zone(r: float) -> str:
        if not np.isfinite(r):
            return "WATCH"
        if r <= 0.75:
            return "STRONG_BUY"
        if r <= 0.90:
            return "BUY"
        if r <= 1.10:
            return "WATCH"
        if r <= 1.25:
            return "TRIM"
        return "AVOID"

    df["zone_label"] = df["price_to_wfv"].apply(zone)

    # Print summary
    non_null = int(df["wfv"].notna().sum())
    total = int(len(df))
    print(f"wfv non-null: {non_null} / {total}")
    print("zone counts:\n", df["zone_label"].value_counts(dropna=False))
    print("clamped (percentile):", int(df["wfv_clamped"].sum()))

    # Write outputs
    df.to_parquet(OUT_PARQUET, index=False)
    df.to_csv(OUT_CSV_GZ, index=False, compression="gzip")

    buy = df[df["zone_label"].isin(["BUY", "STRONG_BUY"])].copy()
    buy = buy.sort_values(["zone_label", "price_to_wfv"], ascending=[True, True])
    buy.to_csv(OUT_BUY, index=False)

    print("wrote:", OUT_PARQUET)
    print("wrote:", OUT_CSV_GZ)
    print("wrote:", OUT_BUY)


if __name__ == "__main__":
    main()
