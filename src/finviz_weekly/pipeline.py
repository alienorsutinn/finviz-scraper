"""Pipeline orchestration."""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Dict, Iterable, List, Optional

import pandas as pd

from .config import AppConfig
from .fundamentals import scrape_fundamentals
from .parse import parse_human_number, parse_missing, parse_percent, parse_range
from .screener import get_industries, get_tickers_for_industry
from .storage import append_history, save_run_outputs, update_latest

LOGGER = logging.getLogger(__name__)


class AsyncRateLimiter:
    """Global rate limiter using an asyncio lock."""

    def __init__(self, rate_per_sec: float) -> None:
        self.rate_per_sec = rate_per_sec
        self._lock = asyncio.Lock()
        self._last_call: Optional[float] = None

    async def wait(self) -> None:
        async with self._lock:
            now = asyncio.get_event_loop().time()
            min_interval = 1 / self.rate_per_sec if self.rate_per_sec > 0 else 0
            if self._last_call is None:
                self._last_call = now
                return
            elapsed = now - self._last_call
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)
            self._last_call = asyncio.get_event_loop().time()


async def _scrape_ticker(ticker: str, limiter: AsyncRateLimiter) -> Optional[Dict[str, str]]:
    try:
        await limiter.wait()
        return await asyncio.to_thread(scrape_fundamentals, ticker)
    except Exception as exc:  # pragma: no cover - network dependent
        LOGGER.error("Failed to scrape %s: %s", ticker, exc)
        return None


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
                page_sleep_range=(config.run.rate_limits.page_sleep_min, config.run.rate_limits.page_sleep_max),
            )
        )
        if config.run.ticker_limit and len(tickers) >= config.run.ticker_limit:
            tickers = tickers[: config.run.ticker_limit]
            break
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


def normalize_records(records: Iterable[Dict[str, str]]) -> pd.DataFrame:
    rows = []
    for rec in records:
        row: Dict[str, object] = {}
        for key, value in rec.items():
            snake = _snake_case(key)
            raw_col = f"raw__{snake}"
            row[raw_col] = value
            parsed = _parse_value(value)
            row[snake] = parsed
        rows.append(row)
    return pd.DataFrame(rows)


async def run_pipeline(session, config: AppConfig) -> pd.DataFrame:
    tickers: List[str]
    if config.run.mode == "universe":
        tickers = build_universe(session, config)
    else:
        tickers = config.run.tickers
    if config.run.ticker_limit:
        tickers = tickers[: config.run.ticker_limit]
    limiter = AsyncRateLimiter(config.run.rate_limits.rate_per_sec)
    tasks = [_scrape_ticker(t, limiter) for t in tickers]
    results = await asyncio.gather(*tasks)
    cleaned = [r for r in results if r]
    df = normalize_records(cleaned)
    if "ticker" not in df.columns:
        df.insert(0, "ticker", tickers[: len(df)])
    else:
        ticker_series = df.pop("ticker")
        df.insert(0, "ticker", ticker_series)
    return df


def execute(session, config: AppConfig) -> pd.DataFrame:
    as_of = date.today()
    df = asyncio.run(run_pipeline(session, config))
    run_dir = save_run_outputs(df, config.run.out_dir, as_of, config.run.formats)
    update_latest(df, config.run.out_dir)
    append_history(df, config.run.out_dir, as_of)
    LOGGER.info("Run completed, data in %s", run_dir)
    return df
