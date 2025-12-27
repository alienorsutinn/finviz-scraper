from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class HttpConfig:
    proxy: Optional[str] = None
    timeout_sec: int = 30
    max_retries: int = 4
    backoff_sec: float = 1.0
    user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )


@dataclass(frozen=True)
class RunConfig:
    mode: str = "universe"               # "universe" | "tickers"
    out_dir: str = "data"
    tickers: Optional[str] = None        # comma-separated when mode="tickers"
    tickers_file: Optional[str] = None
    ticker_limit: int = 4000

    resume: bool = True
    checkpoint_every: int = 25

    concurrency: int = 1                 # keep 1 until everything is stable
    rate_per_sec: float = 1.0
    page_sleep_min: float = 0.10
    page_sleep_max: float = 0.40

    latest_only_ok: bool = True
    include_as_of_date_latest: bool = True


@dataclass(frozen=True)
class AppConfig:
    http: HttpConfig
    run: RunConfig


def env_config(
    *,
    mode: str,
    out_dir: str,
    tickers: Optional[str],
    tickers_file: Optional[str],
    ticker_limit: int,
    resume: bool,
    checkpoint_every: int,
    concurrency: int,
    rate_per_sec: float,
    page_sleep_min: float,
    page_sleep_max: float,
    latest_only_ok: bool,
    include_as_of_date_latest: bool,
    proxy: Optional[str] = None,
) -> AppConfig:
    http = HttpConfig(proxy=proxy)
    run = RunConfig(
        mode=mode,
        out_dir=out_dir,
        tickers=tickers,
        tickers_file=tickers_file,
        ticker_limit=ticker_limit,
        resume=resume,
        checkpoint_every=checkpoint_every,
        concurrency=concurrency,
        rate_per_sec=rate_per_sec,
        page_sleep_min=page_sleep_min,
        page_sleep_max=page_sleep_max,
        latest_only_ok=latest_only_ok,
        include_as_of_date_latest=include_as_of_date_latest,
    )
    return AppConfig(http=http, run=run)
