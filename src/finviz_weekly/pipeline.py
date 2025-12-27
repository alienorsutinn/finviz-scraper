from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from .config import AppConfig
from .fundamentals import scrape_fundamentals
from .http import create_session
from .screener import get_industries, get_tickers_all, get_tickers_for_industry
from .storage import (
    append_checkpoint,
    append_history,
    load_checkpoint,
    prepare_run_dir,
    save_final_outputs,
    save_partial_outputs,
    update_latest,
    write_meta,
)
from .utils import normalize_records

LOGGER = logging.getLogger(__name__)


def _sleep_jitter(cfg: AppConfig) -> None:
    rl = cfg.run.rate_limits
    time.sleep(random.uniform(float(rl.page_sleep_min), float(rl.page_sleep_max)))


def _load_cached_universe(out_dir: str) -> List[str]:
    # Try the common "latest" artifacts and pull tickers if present.
    candidates = [
        Path(out_dir) / "latest" / "finviz_scored.parquet",
        Path(out_dir) / "latest" / "finviz_fundamentals.parquet",
        Path(out_dir) / "latest" / "finviz_scored_peerwfv.parquet",
    ]
    for p in candidates:
        try:
            if p.exists():
                df = pd.read_parquet(p)
                if "ticker" in df.columns:
                    ticks = [str(x).upper() for x in df["ticker"].dropna().tolist()]
                    ticks = sorted(set(ticks))
                    if ticks:
                        LOGGER.warning("Using cached universe from %s (%d tickers)", p.as_posix(), len(ticks))
                        return ticks
        except Exception:
            continue
    return []


def build_universe(session: requests.Session, cfg: AppConfig) -> List[str]:
    # Attempt 1: all-screener (fastest when it works)
    try:
        ticks = get_tickers_all(session, cfg.http, ticker_limit=cfg.run.ticker_limit)
        if ticks:
            LOGGER.info("Universe size: %d tickers (all-screener)", len(ticks))
            return ticks
    except Exception as e:
        LOGGER.warning("All-screener universe failed (%s). Falling back to per-industry union.", e)

    # Attempt 2: per-industry union (more robust)
    try:
        industries = get_industries(session, cfg.http)
        if cfg.run.industry_limit:
            industries = industries[: int(cfg.run.industry_limit)]

        all_set: set[str] = set()
        for ind in industries:
            try:
                ticks_i = get_tickers_for_industry(session, cfg.http, ind, ticker_limit=None)
                all_set.update(ticks_i)
                if cfg.run.ticker_limit and len(all_set) >= int(cfg.run.ticker_limit):
                    break
                _sleep_jitter(cfg)
            except Exception as e:
                LOGGER.warning("Industry '%s' failed (%s). Continuing.", ind, e)

        ticks = sorted(all_set)
        if cfg.run.ticker_limit:
            ticks = ticks[: int(cfg.run.ticker_limit)]

        LOGGER.info("Universe size: %d tickers (per-industry union)", len(ticks))
        if ticks:
            return ticks
    except Exception as e:
        LOGGER.warning("Per-industry universe failed (%s).", e)

    # Attempt 3: cached universe fallback
    cached = _load_cached_universe(cfg.run.out_dir)
    if cached:
        if cfg.run.ticker_limit:
            return cached[: int(cfg.run.ticker_limit)]
        return cached

    LOGGER.warning("Universe size: 0 tickers")
    return []


async def _scrape_one(
    sem: asyncio.Semaphore,
    session: requests.Session,
    cfg: AppConfig,
    ticker: str,
) -> Optional[Dict[str, Any]]:
    async with sem:
        # Respect rate_per_sec by sleeping per-task
        # (not perfect, but good enough + avoids insta-ban)
        time.sleep(max(0.0, 1.0 / float(cfg.run.rate_limits.rate_per_sec)))
        try:
            rec = await asyncio.to_thread(scrape_fundamentals, session, cfg.http, ticker)
            return rec
        except Exception as e:
            LOGGER.warning("Scrape failed ticker=%s err=%s", ticker, e)
            return None


async def run_pipeline(session: requests.Session, cfg: AppConfig, run_dir: Path, as_of: date) -> pd.DataFrame:
    # decide tickers
    tickers: List[str]
    if cfg.run.mode == "tickers":
        tickers = list(cfg.run.tickers)
        if cfg.run.tickers_file:
            p = Path(cfg.run.tickers_file)
            if p.exists():
                txt = p.read_text(encoding="utf-8").strip()
                if txt:
                    tickers = [t.strip().upper() for t in txt.replace("\n", ",").split(",") if t.strip()]
    else:
        tickers = build_universe(session, cfg)

    if not tickers:
        raise RuntimeError("Universe is empty (Finviz blocked or parser broke). Aborting to avoid writing empty outputs.")

    # resume via checkpoint
    records, done = load_checkpoint(run_dir) if cfg.run.resume else ([], set())
    remaining = [t for t in tickers if t not in done]

    LOGGER.info("Resume=%s; tickers_total=%d; already_done=%d; remaining=%d",
                cfg.run.resume, len(tickers), len(done), len(remaining))

    sem = asyncio.Semaphore(max(1, int(cfg.run.rate_limits.concurrency)))

    out_records: List[Dict[str, Any]] = list(records)
    buffer: List[Dict[str, Any]] = []

    tasks = []
    for t in remaining:
        tasks.append(asyncio.create_task(_scrape_one(sem, session, cfg, t)))

    checkpoint_every = max(1, int(cfg.run.rate_limits.checkpoint_every))

    completed = 0
    for fut in asyncio.as_completed(tasks):
        rec = await fut
        completed += 1
        if rec:
            buffer.append(rec)

        if completed % checkpoint_every == 0:
            if buffer:
                append_checkpoint(run_dir, buffer)
                out_records.extend(buffer)
                buffer.clear()
            write_meta(run_dir, {"as_of": as_of.isoformat(), "tickers_total": len(tickers), "tickers_done": len(done) + completed})

    # flush
    if buffer:
        append_checkpoint(run_dir, buffer)
        out_records.extend(buffer)
        buffer.clear()

    df = normalize_records(out_records) if out_records else pd.DataFrame()
    return df


def execute(cfg: AppConfig) -> pd.DataFrame:
    as_of = date.today()
    run_dir = prepare_run_dir(cfg.run.out_dir, as_of)

    session = create_session(cfg.http)

    try:
        df = asyncio.run(run_pipeline(session, cfg, run_dir, as_of))
    except KeyboardInterrupt:
        LOGGER.warning("Interrupted. Salvaging from checkpoint and writing partial snapshot...")
        records, _ = load_checkpoint(run_dir)
        df = normalize_records(records) if records else pd.DataFrame()
        save_partial_outputs(df, run_dir, cfg.run.formats)
        write_meta(run_dir, {"as_of": as_of.isoformat(), "interrupted": True, "rows": int(len(df))})
        LOGGER.warning("Partial saved to %s", run_dir)
        return df

    # Final outputs
    save_final_outputs(df, run_dir, cfg.run.formats)
    update_latest(df, cfg.run.out_dir, as_of, only_ok=cfg.run.latest_only_ok, include_as_of_date=cfg.run.latest_include_as_of_date)
    append_history(df, cfg.run.out_dir, as_of)

    LOGGER.info("Run completed, data in %s", run_dir)
    return df
