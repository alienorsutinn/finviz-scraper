from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional, Sequence, Tuple


@dataclass(frozen=True)
class RateLimits:
    rate_per_sec: float = 1.0
    page_sleep_min: float = 0.8
    page_sleep_max: float = 1.8
    concurrency: int = 6
    checkpoint_every: int = 1


@dataclass(frozen=True)
class RunConfig:
    mode: str = "universe"  # "universe" | "tickers"
    tickers: Tuple[str, ...] = ()
    tickers_file: Optional[str] = None

    industry_limit: Optional[int] = None
    ticker_limit: Optional[int] = None

    out_dir: str = "data"
    formats: Tuple[str, ...] = ("parquet",)

    resume: bool = True
    rate_limits: RateLimits = field(default_factory=RateLimits)

    latest_only_ok: bool = True
    latest_include_as_of_date: bool = False


@dataclass(frozen=True)
class HttpConfig:
    proxy: Optional[str] = None
    timeout_sec: float = 30.0
    max_retries: int = 3
    backoff_sec: float = 1.5
    user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )


@dataclass(frozen=True)
class AppConfig:
    run: RunConfig
    http: HttpConfig


def env_config(
    *,
    mode: str,
    tickers: Sequence[str],
    tickers_file: Optional[str],
    industry_limit: Optional[int],
    ticker_limit: Optional[int],
    out_dir: str,
    formats: Sequence[str],
    resume: bool,
    rate_per_sec: float,
    page_sleep_min: float,
    page_sleep_max: float,
    concurrency: int,
    checkpoint_every: int,
    proxy: Optional[str],
    latest_only_ok: bool,
    latest_include_as_of_date: bool,
) -> AppConfig:
    run = RunConfig(
        mode=mode,
        tickers=tuple(tickers),
        tickers_file=tickers_file,
        industry_limit=industry_limit,
        ticker_limit=ticker_limit,
        out_dir=out_dir,
        formats=tuple(formats),
        resume=resume,
        rate_limits=RateLimits(
            rate_per_sec=rate_per_sec,
            page_sleep_min=page_sleep_min,
            page_sleep_max=page_sleep_max,
            concurrency=concurrency,
            checkpoint_every=checkpoint_every,
        ),
        latest_only_ok=latest_only_ok,
        latest_include_as_of_date=latest_include_as_of_date,
    )

    # If not passed, allow FINVIZ_PROXY or HTTPS_PROXY/HTTP_PROXY
    p = proxy or os.environ.get("FINVIZ_PROXY") or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")

    http = HttpConfig(proxy=p)
    return AppConfig(run=run, http=http)
