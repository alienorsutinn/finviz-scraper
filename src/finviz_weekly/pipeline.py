"""Pipeline orchestration (crash-safe, resumable)."""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd

from .config import AppConfig
from .fundamentals import scrape_fundamentals
from .parse import parse_human_number, parse_missing, parse_percent, parse_range
from .screener import get_industries, get_tickers_for_industry
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

LOGGER = logging.getLogger(__name__)


class AsyncRateLimiter:
    """Global rate limiter across all tasks."""

    def __init__(self, rate_per_sec: float) -> None:
        self.rate_per_sec = rate_per_sec
        self._lock = asyncio.Lock()
        self._last_call: Optional[float] = None

    async def wait(self) -> None:
        async with self._lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            min_interval = 1 / self.rate_per_sec if self.rate_per_sec > 0 else 0
            if self._last_call is None:
                self._last_call = now
                return
            elapsed = now - self._last_call
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)
            self._last_call = asyncio.get_running_loop().time()


def build_universe(session, config: AppConfig) -> List[str]:
    industries = get_industries(session, config.http)
    if config.run.industry_limit:
        industries = industries[: config.run.industry_limit]

    tickers: List[str] = []
    for code in industries:
        tickers.extend(
            get_tickers_for_industry(
                session,
                config.http,
                code,
                ticker_limit=config.run.ticker_limit,
                page_sleep_range=(
                    config.run.rate_limits.page_sleep_min,
                    config.run.rate_limits.page_sleep_max,
                ),
            )
        )
        if config.run.ticker_limit and len(tickers) >= config.run.ticker_limit:
            tickers = tickers[: config.run.ticker_limit]
            break

    # Global dedupe (tickers can appear in multiple Finviz buckets)


    tickers = list(dict.fromkeys([str(t).strip().upper() for t in tickers if str(t).strip()]))


    LOGGER.info("Universe size: %d tickers", len(tickers))
    return tickers


def _parse_value(raw: str):
    raw_clean = parse_missing(raw)
    if raw_clean is pd.NA:
        return pd.NA
    if isinstance(raw_clean, str) and raw_clean.endswith("%"):
        pct = parse_percent(raw_clean)
        if pct is not pd.NA:
            return pct
    if isinstance(raw_clean, str) and "-" in raw_clean:
        rng = parse_range(raw_clean)
        if rng is not pd.NA:
            return rng
    num = parse_human_number(raw_clean)
    if num is not pd.NA:
        return num
    return raw_clean


def _snake_case(key: str) -> str:
    return key.strip().lower().replace(" ", "_").replace("/", "_")


def normalize_records(records: Iterable[Dict[str, object]]) -> pd.DataFrame:
    rows = []
    for rec in records:
        row: Dict[str, object] = {}
        for key, value in rec.items():
            snake = _snake_case(str(key))
            raw_col = f"raw__{snake}"
            row[raw_col] = value
            if isinstance(value, str):
                row[snake] = _parse_value(value)
            else:
                row[snake] = value
        rows.append(row)

    df = pd.DataFrame(rows)

    # ensure ticker column exists + first
    if "ticker" not in df.columns and "Ticker" in df.columns:
        df["ticker"] = df["Ticker"]
    if "ticker" in df.columns:
        s = df.pop("ticker")
        df.insert(0, "ticker", s.astype(str).str.upper())

    return df


async def run_pipeline(session, config: AppConfig, run_dir: Path, as_of: date) -> pd.DataFrame:
    # Determine ticker list
    if config.run.mode == "universe":
        tickers = build_universe(session, config)
    else:
        tickers = list(config.run.tickers)

    if config.run.ticker_limit:
        tickers = tickers[: config.run.ticker_limit]

    # Resume support (skip tickers already checkpointed)
    checkpoint_records: List[Dict[str, object]] = []
    done: set[str] = set()
    if config.run.resume:
        checkpoint_records, done = load_checkpoint(run_dir)
        if done:
            LOGGER.info("Resume enabled: %d tickers already checkpointed for %s", len(done), as_of.isoformat())

    remaining = [t for t in tickers if t.upper() not in done]
    LOGGER.info("Scraping %d tickers (remaining), %d already saved", len(remaining), len(done))

    records: List[Dict[str, object]] = list(checkpoint_records)

    limiter = AsyncRateLimiter(config.run.rate_limits.rate_per_sec)
    sem = asyncio.Semaphore(max(1, int(config.run.rate_limits.concurrency)))
    checkpoint_every = max(1, int(config.run.rate_limits.checkpoint_every or 1))

    async def scrape_one(ticker: str) -> Dict[str, object]:
        async with sem:
            try:
                await limiter.wait()
                data = await asyncio.to_thread(scrape_fundamentals, ticker)
                if not isinstance(data, dict):
                    raise ValueError(f"Unexpected fundamentals payload: {data}")
                data.setdefault("Ticker", ticker)
                data["__status"] = "ok"
                return data
            except Exception as exc:
                # Mark as done to avoid infinite retries on resume
                return {"Ticker": ticker, "__status": "error", "__error": repr(exc)}

    tasks = [asyncio.create_task(scrape_one(t)) for t in remaining]

    completed = 0
    total_new = len(tasks)

    def _counts(recs: List[Dict[str, object]]) -> tuple[int, int]:
        ok = sum(1 for r in recs if r.get("__status") == "ok")
        err = sum(1 for r in recs if r.get("__status") == "error")
        return ok, err

    for fut in asyncio.as_completed(tasks):
        rec = await fut
        records.append(rec)

        # DURABLE checkpoint after every ticker
        append_checkpoint(run_dir, rec)
        completed += 1

        if completed % checkpoint_every == 0 or completed == total_new:
            df_partial = normalize_records(records)
            save_partial_outputs(df_partial, run_dir, config.run.formats)

            ok, err = _counts(records)
            write_meta(
                run_dir,
                {
                    "as_of": as_of.isoformat(),
                    "total_target": len(tickers),
                    "already_checkpointed": len(done),
                    "completed_this_run": completed,
                    "ok": ok,
                    "error": err,
                },
            )
            LOGGER.info("Checkpoint: %d/%d new tickers done (ok=%d, err=%d)", completed, total_new, ok, err)

    return normalize_records(records)


def execute(session, config: AppConfig) -> pd.DataFrame:
    as_of = date.today()
    run_dir = prepare_run_dir(config.run.out_dir, as_of, resume=config.run.resume)

    try:
        df = asyncio.run(run_pipeline(session, config, run_dir, as_of))
    except KeyboardInterrupt:
        # salvage from checkpoint and write a partial snapshot
        LOGGER.warning("Interrupted. Salvaging from checkpoint and writing partial snapshot...")
        records, _ = load_checkpoint(run_dir)
        df = normalize_records(records) if records else pd.DataFrame()
        save_partial_outputs(df, run_dir, config.run.formats)
        write_meta(run_dir, {"as_of": as_of.isoformat(), "interrupted": True, "rows": int(len(df))})
        LOGGER.warning("Partial saved to %s", run_dir)
        return df

    # final outputs
    save_final_outputs(df, run_dir, config.run.formats)
    update_latest(df, config.run.out_dir, as_of, only_ok=config.run.latest_only_ok, include_as_of_date=config.run.latest_include_as_of_date)
    append_history(df, config.run.out_dir, as_of)

    LOGGER.info("Run completed, data in %s", run_dir)
    return df
