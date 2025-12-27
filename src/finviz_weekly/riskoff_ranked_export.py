from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

LOGGER = logging.getLogger(__name__)


def _read_latest_scored(out_dir: Path) -> pd.DataFrame:
    latest_dir = out_dir / "latest"
    parquet_path = latest_dir / "finviz_scored.parquet"
    csv_gz_path = latest_dir / "finviz_scored.csv.gz"
    csv_path = latest_dir / "finviz_scored.csv"

    # Prefer parquet if it exists AND can be read
    if parquet_path.exists():
        try:
            return pd.read_parquet(parquet_path)
        except Exception as e:
            LOGGER.warning(
                "Failed to read %s (%s). Falling back to CSV.", parquet_path, e
            )

    # Fall back to CSV artifacts (these are always written)
    if csv_gz_path.exists():
        return pd.read_csv(csv_gz_path, compression="gzip")
    if csv_path.exists():
        return pd.read_csv(csv_path)

    raise FileNotFoundError(
        f"Could not find latest scored snapshot. Tried: "
        f"{parquet_path}, {csv_gz_path}, {csv_path}"
    )


def export_riskoff_ranked(out: str = "data", top: int = 80) -> Path:
    out_dir = Path(out)
    df = _read_latest_scored(out_dir)

    required = {"ticker", "wfv", "wfv_riskoff", "price_to_wfv_riskoff", "zone", "size_units", "risk_score"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in scored snapshot: {missing}")

    # Keep only actionable zones (optional: tune this)
    allowed_zones = {"STRONG_BUY", "BUY", "WATCH", "HOLD", "TRIM", "AVOID"}
    df = df[df["zone"].isin(allowed_zones)].copy()

    # Rank: best zone first, cheapest (price_to_wfv_riskoff) first, then lower risk_score
    zone_rank = {
        "STRONG_BUY": 0,
        "BUY": 1,
        "WATCH": 2,
        "HOLD": 3,
        "TRIM": 4,
        "AVOID": 5,
    }
    df["zone_rank"] = df["zone"].map(zone_rank).fillna(99).astype(int)

    df = df.sort_values(
        by=["zone_rank", "price_to_wfv_riskoff", "risk_score"],
        ascending=[True, True, True],
        kind="mergesort",
    )

    cols = ["ticker", "wfv", "wfv_riskoff", "price_to_wfv_riskoff", "zone", "size_units", "risk_score"]
    out_path = out_dir / "latest" / "riskoff_ranked.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df.head(int(top))[cols].to_csv(out_path, index=False)
    print(f"Wrote: {out_path} (rows={min(len(df), int(top))})")
    return out_path


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO)

    p = argparse.ArgumentParser(description="Export RiskOff-ranked CSV from latest scored snapshot.")
    p.add_argument("--out", default="data", help="Output directory (default: data)")
    p.add_argument("--top", type=int, default=80, help="Top N rows (default: 80)")
    args = p.parse_args(argv)

    export_riskoff_ranked(out=args.out, top=args.top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
