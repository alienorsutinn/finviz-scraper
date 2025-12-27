from __future__ import annotations

from pathlib import Path
import pandas as pd


ZONE_RANK = {
    "STRONG_BUY": 0,
    "BUY": 1,
    "WATCH": 2,
    "HOLD": 3,
    "TRIM": 4,
    "AVOID": 5,
    "NO_DATA": 9,
}


def export_riskoff_ranked(latest_scored_path: str | Path, out_csv_path: str | Path, top: int = 80) -> None:
    df = pd.read_parquet(latest_scored_path)

    # Find ticker column robustly
    ticker_cols = [c for c in df.columns if c.lower() in ("ticker", "symbol") or "ticker" in c.lower() or "symbol" in c.lower()]
    ticker_col = ticker_cols[0] if ticker_cols else None

    # Require riskoff outputs
    req = ["wfv_riskoff", "price_to_wfv_riskoff", "zone", "size_units"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in scored parquet: {missing}")

    out = df[df["wfv_riskoff"].notna()].copy()
    out["zone_rank"] = out["zone"].map(ZONE_RANK).fillna(9).astype(int)
    out = out.sort_values(["zone_rank", "price_to_wfv_riskoff"], ascending=[True, True])

    cols = []
    if ticker_col:
        cols.append(ticker_col)
    # include helpful context if present
    for c in ["Company", "Sector", "Industry", "Price", "wfv", "wfv_riskoff", "price_to_wfv_riskoff", "zone", "size_units", "risk_score", "flags", "CompositeScore"]:
        if c in out.columns and c not in cols:
            cols.append(c)

    out.head(top)[cols].to_csv(out_csv_path, index=False)
    print(f"Wrote: {out_csv_path} (rows={min(top, len(out))})")


if __name__ == "__main__":
    # Default paths (matches repo structure)
    export_riskoff_ranked(
        latest_scored_path="data/latest/finviz_scored.parquet",
        out_csv_path="data/latest/riskoff_ranked.csv",
        top=80,
    )
