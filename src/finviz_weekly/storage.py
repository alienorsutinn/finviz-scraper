"""Storage helpers for saving scraped data (crash-safe with checkpoints)."""
from __future__ import annotations

import json
import logging
import os
from datetime import date
from pathlib import Path
from typing import Iterable, Tuple, List, Dict, Set

import pandas as pd

LOGGER = logging.getLogger(__name__)

RUNS_DIR = "runs"
LATEST_DIR = "latest"
HISTORY_DIR = "history"

CHECKPOINT_NAME = "finviz_fundamentals.checkpoint.ndjson"
META_NAME = "meta.json"

FINAL_PARQUET = "finviz_fundamentals.parquet"
FINAL_CSV = "finviz_fundamentals.csv"

PARTIAL_PARQUET = "finviz_fundamentals.partial.parquet"
PARTIAL_CSV = "finviz_fundamentals.partial.csv"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_run_dir(out_root: str, as_of: date) -> Path:
    root = Path(out_root)
    return ensure_dir(root / RUNS_DIR / as_of.isoformat())


def checkpoint_path(run_dir: Path) -> Path:
    return run_dir / CHECKPOINT_NAME


def meta_path(run_dir: Path) -> Path:
    return run_dir / META_NAME


def prepare_run_dir(out_root: str, as_of: date, *, resume: bool) -> Path:
    """Create run dir; if resume=False, clear existing checkpoint/partials for today."""
    run_dir = get_run_dir(out_root, as_of)
    if not resume:
        for p in [checkpoint_path(run_dir), run_dir / PARTIAL_PARQUET, run_dir / PARTIAL_CSV]:
            try:
                if p.exists():
                    p.unlink()
            except OSError:
                pass
    return run_dir


def append_checkpoint(run_dir: Path, record: Dict[str, object]) -> None:
    """
    Append a single record to the checkpoint NDJSON file.
    Durable: flush + fsync so a crash/power loss keeps what we wrote.
    """
    path = checkpoint_path(run_dir)
    line = json.dumps(record, ensure_ascii=False)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def load_checkpoint(run_dir: Path) -> Tuple[List[Dict[str, object]], Set[str]]:
    """Load checkpoint NDJSON; returns (records, tickers_seen)."""
    path = checkpoint_path(run_dir)
    if not path.exists():
        return [], set()

    records: List[Dict[str, object]] = []
    seen: Set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            rec = json.loads(raw)
            records.append(rec)
            t = rec.get("Ticker") or rec.get("ticker")
            if isinstance(t, str) and t.strip():
                seen.add(t.strip().upper())
        except json.JSONDecodeError:
            # ignore partial/corrupted lines
            continue
    return records, seen


def write_meta(run_dir: Path, meta: Dict[str, object]) -> None:
    """Atomic meta write."""
    path = meta_path(run_dir)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _write_df(df: pd.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    if path.suffix == ".parquet":
        df.to_parquet(path, index=False, compression="snappy")
    elif path.suffix == ".csv":
        df.to_csv(path, index=False)
    else:
        raise ValueError(f"Unsupported output: {path}")


def save_partial_outputs(df: pd.DataFrame, run_dir: Path, formats: Iterable[str]) -> None:
    if "parquet" in formats:
        _write_df(df, run_dir / PARTIAL_PARQUET)
    if "csv" in formats:
        _write_df(df, run_dir / PARTIAL_CSV)


def save_final_outputs(df: pd.DataFrame, run_dir: Path, formats: Iterable[str]) -> None:
    if "parquet" in formats:
        _write_df(df, run_dir / FINAL_PARQUET)
    if "csv" in formats:
        _write_df(df, run_dir / FINAL_CSV)


def update_latest(
    df: pd.DataFrame,
    out_root: str,
    as_of: date,
    *,
    only_ok: bool = True,
    include_as_of_date: bool = True,
) -> Path:
    """Write the latest snapshot.

    Notes:
      - `only_ok=True` keeps downstream scoring clean (no errored rows).
      - `include_as_of_date=True` keeps schema aligned with history.
    """
    latest_dir = ensure_dir(Path(out_root) / LATEST_DIR)
    path = latest_dir / FINAL_PARQUET

    out_df = df.copy()
    if only_ok and "__status" in out_df.columns:
        out_df = out_df[out_df["__status"] == "ok"].copy()

    if include_as_of_date:
        out_df["as_of_date"] = as_of.isoformat()

    out_df.to_parquet(path, index=False, compression="snappy")
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
