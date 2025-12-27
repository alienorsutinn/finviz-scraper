from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

LOGGER = logging.getLogger(__name__)


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def append_checkpoint(run_dir: Path, rows: list[dict]) -> None:
    _ensure_dir(run_dir)
    p = run_dir / "checkpoint.jsonl"
    with p.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def load_checkpoint(run_dir: Path) -> pd.DataFrame:
    p = run_dir / "checkpoint.jsonl"
    if not p.exists():
        return pd.DataFrame()
    rows = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return pd.DataFrame(rows)


# Back-compat aliases (some of your earlier patches referenced these)
def write_checkpoint(run_dir: Path, rows: list[dict]) -> None:
    append_checkpoint(run_dir, rows)


def read_checkpoint(run_dir: Path) -> pd.DataFrame:
    return load_checkpoint(run_dir)


def update_latest(
    df: pd.DataFrame,
    out_dir: str,
    *,
    only_ok: bool = False,
    include_as_of_date: bool = True,
) -> None:
    out = Path(out_dir)
    latest_dir = out / "latest"
    _ensure_dir(latest_dir)

    p = latest_dir / "finviz_fundamentals.parquet"

    if df is None or df.empty:
        LOGGER.warning("Refusing to overwrite latest snapshot with empty dataframe: %s", p)
        return

    out_df = df.copy()
    if only_ok and "ok" in out_df.columns:
        out_df = out_df[out_df["ok"] == True]  # noqa: E712

    # include_as_of_date handled upstream by pipeline (df already has it if needed)
    out_df.to_parquet(p, index=False)
    LOGGER.info("Updated latest snapshot at %s", p)


def append_history(df: pd.DataFrame, out_dir: str, as_of) -> None:
    out = Path(out_dir)
    hist_dir = out / "history"
    _ensure_dir(hist_dir)

    p = hist_dir / "finviz_fundamentals_history.parquet"

    if df is None or df.empty:
        LOGGER.warning("Skipping history append: empty dataframe")
        return

    df.to_parquet(p, index=False) if not p.exists() else pd.concat(
        [pd.read_parquet(p), df], ignore_index=True
    ).to_parquet(p, index=False)
    LOGGER.info("Appended history to %s", p)


def write_meta(run_dir: Path, meta: Dict[str, Any]) -> None:
    _ensure_dir(run_dir)
    p = run_dir / "meta.json"
    p.write_text(json.dumps(meta, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def read_meta(run_dir: Path) -> Optional[Dict[str, Any]]:
    p = run_dir / "meta.json"
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))
