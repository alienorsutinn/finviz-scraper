from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from .screen import _build_colmap
from .valuation import add_multiples_valuation

ZONE_KEEP = {"STRONG_BUY", "BUY", "WATCH"}

def load_scored(out: str) -> pd.DataFrame:
    latest = Path(out) / "latest"
    p_parq = latest / "finviz_scored.parquet"
    p_csv = latest / "finviz_scored.csv.gz"
    if p_parq.exists():
        return pd.read_parquet(p_parq)
    if p_csv.exists():
        return pd.read_csv(p_csv, compression="gzip", low_memory=False)
    raise FileNotFoundError(f"Missing scored snapshot in {latest}")

def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    if "ticker" not in df.columns:
        for c in ["Ticker", "symbol", "Symbol", "raw__ticker", "raw__symbol"]:
            if c in df.columns:
                df = df.rename(columns={c: "ticker"})
                break
    need = ["zone", "wfv_riskoff", "price_to_wfv_riskoff", "size_units", "risk_score"]
    if any(c not in df.columns for c in need):
        df = add_multiples_valuation(df, colmap=_build_colmap(df))
    return df

def export(out: str, top: int, min_price: float, min_mcap: float, min_dollar_vol: float) -> Path:
    df = ensure_cols(load_scored(out))

    # try common column names for liquidity
    price_col = "price" if "price" in df.columns else None
    mcap_col = "market_cap" if "market_cap" in df.columns else ("Market Cap" if "Market Cap" in df.columns else None)
    vol_col = "volume" if "volume" in df.columns else ("Volume" if "Volume" in df.columns else None)

    if price_col:
        df = df[df[price_col].fillna(0) >= min_price]

    if mcap_col:
        df = df[df[mcap_col].fillna(0) >= min_mcap]

    # dollar volume proxy: price * volume
    if price_col and vol_col:
        df["dollar_vol"] = (pd.to_numeric(df[price_col], errors="coerce") * pd.to_numeric(df[vol_col], errors="coerce"))
        df = df[df["dollar_vol"].fillna(0) >= min_dollar_vol]
    else:
        df["dollar_vol"] = pd.NA

    # keep only actionable zones
    df = df[df["zone"].astype(str).isin(ZONE_KEEP)].copy()

    zone_rank = {"STRONG_BUY": 0, "BUY": 1, "WATCH": 2}
    df["zone_rank"] = df["zone"].astype(str).map(zone_rank).fillna(99).astype(int)

    # choose “best” within zone by cheapness + sizing
    cols = ["ticker","zone","price_to_wfv_riskoff","wfv_riskoff","wfv","size_units","risk_score","dollar_vol"]
    cols += [c for c in ["composite_score","sector","industry","price"] if c in df.columns]
    cols = [c for c in cols if c in df.columns]

    out_df = (
        df.sort_values(
            by=["zone_rank", "price_to_wfv_riskoff", "size_units", "risk_score"],
            ascending=[True, True, False, True],
            kind="mergesort",
        )
        .head(int(top))
        .loc[:, cols]
        .reset_index(drop=True)
    )

    out_path = Path(out) / "latest" / "today_actions.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False)
    print(f"Wrote: {out_path} (rows={len(out_df)})")
    return out_path

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data")
    ap.add_argument("--top", type=int, default=40)
    ap.add_argument("--min-price", type=float, default=5.0)
    ap.add_argument("--min-mcap", type=float, default=1_000_000_000.0)
    ap.add_argument("--min-dollar-vol", type=float, default=5_000_000.0)
    args = ap.parse_args()
    export(args.out, args.top, args.min_price, args.min_mcap, args.min_dollar_vol)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
