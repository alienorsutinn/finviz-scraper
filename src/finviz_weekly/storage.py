from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterable, Set
import pandas as pd

LOGGER = logging.getLogger(__name__)


def prepare_run_dir(out_dir: str, as_of) -> Path:
    run_dir = Path(out_dir) / "runs" / as_of.isoformat()
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def write_meta(run_dir: Path, meta: dict) -> None:
    p = run_dir / "meta.json"
    p.write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")


def _checkpoint_path(run_dir: Path) -> Path:
    return run_dir / "checkpoint.txt"


def read_checkpoint(run_dir: Path) -> Set[str]:
    p = _checkpoint_path(run_dir)
    if not p.exists():
        return set()
    return {line.strip().upper() for line in p.read_text(encoding="utf-8").splitlines() if line.strip()}


def write_checkpoint(run_dir: Path, done: Iterable[str]) -> None:
    p = _checkpoint_path(run_dir)
    txt = "\n".join(sorted({t.strip().upper() for t in done if t}))
    p.write_text(txt + ("\n" if txt else ""), encoding="utf-8")


def update_latest(df: pd.DataFrame, out_dir: str, *, only_ok: bool, include_as_of_date: bool) -> None:
    if df is None or df.empty:
        LOGGER.warning("Skipping latest snapshot update: empty dataframe")
        return

    latest_dir = Path(out_dir) / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)

    out_path = latest_dir / "finviz_fundamentals.parquet"

    to_write = df.copy()
    if only_ok and "ok" in to_write.columns:
        to_write = to_write[to_write["ok"] == True]  # noqa: E712

    if to_write.empty:
        LOGGER.warning("Skipping latest snapshot update: empty after only_ok filter")
        return

    # include_as_of_date means the pipeline already injected it; we don't force it here.
    to_write.to_parquet(out_path, index=False)
    LOGGER.info("Updated latest snapshot at %s", out_path)


def append_history(df: pd.DataFrame, out_dir: str, as_of) -> None:
    if df is None or df.empty:
        LOGGER.warning("Skipping history append: empty dataframe")
        return

    hist_dir = Path(out_dir) / "history"
    hist_dir.mkdir(parents=True, exist_ok=True)

    out_path = hist_dir / "finviz_fundamentals_history.parquet"

    to_write = df.copy()
    if "as_of_date" not in to_write.columns:
        to_write.insert(0, "as_of_date", as_of.isoformat())

    if out_path.exists():
        old = pd.read_parquet(out_path)
        combined = pd.concat([old, to_write], ignore_index=True)
    else:
        combined = to_write

    combined.to_parquet(out_path, index=False)
    LOGGER.info("Appended history to %s", out_path)
