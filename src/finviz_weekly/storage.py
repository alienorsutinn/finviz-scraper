"""Storage helpers for saving scraped data."""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd

LOGGER = logging.getLogger(__name__)


RUNS_DIR = "runs"
LATEST_DIR = "latest"
HISTORY_DIR = "history"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_run_outputs(df: pd.DataFrame, out_root: str, as_of: date, formats: Iterable[str]) -> Path:
    """Save run outputs to dated directory."""

    root = Path(out_root)
    run_dir = ensure_dir(root / RUNS_DIR / as_of.isoformat())
    if "parquet" in formats:
        parquet_path = run_dir / "finviz_fundamentals.parquet"
        df.to_parquet(parquet_path, index=False, compression="snappy")
    if "csv" in formats:
        csv_path = run_dir / "finviz_fundamentals.csv"
        df.to_csv(csv_path, index=False)
    meta = {
        "as_of": as_of.isoformat(),
        "rows": len(df),
        "columns": list(df.columns),
    }
    with (run_dir / "meta.json").open("w", encoding="utf-8") as fh:
        json.dump(meta, fh, indent=2)
    LOGGER.info("Saved run outputs to %s", run_dir)
    return run_dir


def update_latest(df: pd.DataFrame, out_root: str) -> Path:
    latest_dir = ensure_dir(Path(out_root) / LATEST_DIR)
    path = latest_dir / "finviz_fundamentals.parquet"
    df.to_parquet(path, index=False, compression="snappy")
    LOGGER.info("Updated latest snapshot at %s", path)
    return path


def append_history(df: pd.DataFrame, out_root: str, as_of: date) -> Path:
    history_dir = ensure_dir(Path(out_root) / HISTORY_DIR)
    history_path = history_dir / "finviz_fundamentals_history.parquet"
    df_with_date = df.copy()
    df_with_date["as_of_date"] = as_of.isoformat()
    if history_path.exists():
        existing = pd.read_parquet(history_path)
        combined = pd.concat([existing, df_with_date], ignore_index=True)
        combined = combined.drop_duplicates(subset=["ticker", "as_of_date"])
    else:
        combined = df_with_date
    combined.to_parquet(history_path, index=False, compression="snappy")
    LOGGER.info("Appended history to %s", history_path)
    return history_path
