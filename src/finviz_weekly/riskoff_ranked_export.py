from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd

from .valuation import add_multiples_valuation


def _read_latest_scored(out: str) -> pd.DataFrame:
    """Read latest scored snapshot (prefer parquet; fallback to csv.gz)."""
    base = Path(out)
    latest = base / "latest"

    pq = latest / "finviz_scored.parquet"
    gz = latest / "finviz_scored.csv.gz"

    if pq.exists():
        return pd.read_parquet(pq)

    if gz.exists():
        # low_memory=False to avoid mixed-type chunk inference issues
        return pd.read_csv(gz, compression="gzip", low_memory=False)

    raise FileNotFoundError(
        f"Could not find scored snapshot at {pq} or {gz}. "
        f"Run: python -m finviz_weekly screen --out {out} --top 80"
    )


def _ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the RiskOff columns exist. If missing, compute them by re-running
    the valuation augmentation on the already-scored snapshot.
    """
    # Standardize ticker column for downstream
    if "ticker" not in df.columns:
        for c in ("Ticker", "raw__ticker", "symbol", "Symbol"):
            if c in df.columns:
                df = df.rename(columns={c: "ticker"})
                break

    # Some runs may name risk differently; normalize
    if "risk_score" not in df.columns:
        for c in ("risk", "risk_score_0_100", "riskScore", "risk_score_norm"):
            if c in df.columns:
                df = df.rename(columns={c: "risk_score"})
                break

    # Compute missing valuation / zones if needed
    need = {"wfv_riskoff", "price_to_wfv_riskoff", "zone", "size_units"}
    if not need.issubset(set(df.columns)):
        # add_multiples_valuation will also compute wfv if needed
        # and will add zone/size_units when it can.
        df = add_multiples_valuation(df)

    # As a final safety: if risk_score is still missing, default neutral
    if "risk_score" not in df.columns:
        df["risk_score"] = 60.0

    # Coerce the critical numeric columns
    for c in ("wfv", "wfv_riskoff", "price_to_wfv_riskoff", "size_units", "risk_score"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # If zone missing even after valuation, set to NO_DATA
    if "zone" not in df.columns:
        df["zone"] = "NO_DATA"

    return df


def export_riskoff_ranked(out: str = "data", top: int = 80) -> Path:
    """
    Export a ranked CSV of top risk-off opportunities to data/latest/riskoff_ranked.csv.
    Ranking = cheapest by price_to_wfv_riskoff within BUY-ish zones.
    """
    df = _read_latest_scored(out)
    df = _ensure_required_columns(df)

    # Filter to the actionable zones (tweak if you want)
    keep_zones = {"STRONG_BUY", "BUY", "WATCH"}
    d = df[df["zone"].astype(str).isin(keep_zones)].copy()

    # Drop junk rows
    d = d.dropna(subset=["ticker", "wfv_riskoff", "price_to_wfv_riskoff"])
    d = d[d["wfv_riskoff"] > 0]

    # Rank
    d = d.sort_values(["price_to_wfv_riskoff", "size_units"], ascending=[True, False])

    cols = ["ticker", "wfv", "wfv_riskoff", "price_to_wfv_riskoff", "zone", "size_units", "risk_score"]
    cols = [c for c in cols if c in d.columns]

    out_path = Path(out) / "latest" / "riskoff_ranked.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    d.head(int(top))[cols].to_csv(out_path, index=False)
    print(f"Wrote: {out_path} (rows={min(len(d), int(top))})")
    return out_path


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data", help="Base output folder (default: data)")
    ap.add_argument("--top", type=int, default=80, help="Rows to export (default: 80)")
    args = ap.parse_args(argv)
    export_riskoff_ranked(out=args.out, top=args.top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
