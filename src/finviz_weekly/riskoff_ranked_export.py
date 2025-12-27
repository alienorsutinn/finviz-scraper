from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from .screen import _build_colmap
from .valuation import add_multiples_valuation


REQUIRED = ["ticker", "wfv", "wfv_riskoff", "price_to_wfv_riskoff", "zone", "size_units", "risk_score"]


def _load_latest_scored(out: str) -> pd.DataFrame:
    latest = Path(out) / "latest"
    p_parq = latest / "finviz_scored.parquet"
    p_csv = latest / "finviz_scored.csv.gz"

    if p_parq.exists():
        return pd.read_parquet(p_parq)
    if p_csv.exists():
        return pd.read_csv(p_csv, compression="gzip", low_memory=False)

    raise FileNotFoundError(f"Missing scored snapshot in {latest}. Expected finviz_scored.parquet or finviz_scored.csv.gz")


def _ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    # normalise ticker column
    if "ticker" not in df.columns:
        for c in ["Ticker", "symbol", "Symbol", "raw__ticker", "raw__symbol"]:
            if c in df.columns:
                df = df.rename(columns={c: "ticker"})
                break

    # If riskoff fields missing, recompute using same colmap logic as screen.py
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        colmap = _build_colmap(df)
        df = add_multiples_valuation(df, colmap=colmap)

    # final check
    missing2 = [c for c in REQUIRED if c not in df.columns]
    if missing2:
        raise ValueError(f"Still missing required columns after valuation: {missing2}")

    return df


def export_riskoff_ranked(out: str = "data", top: int = 80) -> Path:
    df = _load_latest_scored(out)
    df = _ensure_required_columns(df)

    zone_rank = {
        "STRONG_BUY": 0,
        "BUY": 1,
        "WATCH": 2,
        "HOLD": 3,
        "TRIM": 4,
        "AVOID": 5,
        "NO_DATA": 6,
    }
    df["zone_rank"] = df["zone"].astype(str).map(zone_rank).fillna(6).astype(int)

    ranked = (
        df.sort_values(
            by=["zone_rank", "price_to_wfv_riskoff", "size_units", "risk_score"],
            ascending=[True, True, False, True],
            kind="mergesort",
        )
        .head(int(top))
        .loc[:, REQUIRED]
        .reset_index(drop=True)
    )

    out_path = Path(out) / "latest" / "riskoff_ranked.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ranked.to_csv(out_path, index=False)
    print(f"Wrote: {out_path} (rows={len(ranked)})")
    return out_path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data")
    ap.add_argument("--top", type=int, default=80)
    args = ap.parse_args()
    export_riskoff_ranked(out=args.out, top=args.top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
