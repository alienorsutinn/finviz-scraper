"""Configuration utilities for finviz weekly scraper."""
from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Iterable, Optional


@dataclass
class RateLimits:
    """Rate limit configuration."""

    rate_per_sec: float = 0.5
    page_sleep_min: float = 0.8
    page_sleep_max: float = 1.8


@dataclass
class RunConfig:
    """Runtime configuration."""

    mode: str
    tickers: list[str]
    industry_limit: Optional[int]
    ticker_limit: Optional[int]
    out_dir: str
    formats: Iterable[str]
    log_level: str
    rate_limits: RateLimits


@dataclass
class HttpConfig:
    """HTTP configuration and proxy support."""

    proxy: Optional[str]
    timeout_connect: int = 5
    timeout_read: int = 20
    max_retries: int = 5


@dataclass
class AppConfig:
    """Application configuration assembled from environment variables."""

    http: HttpConfig
    run: RunConfig


USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.129 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
]


def env_config(
    mode: str,
    tickers: Optional[list[str]] = None,
    industry_limit: Optional[int] = None,
    ticker_limit: Optional[int] = None,
    out_dir: str = "data",
    formats: Optional[Iterable[str]] = None,
    log_level: str = "INFO",
    rate_per_sec: float = 0.5,
    page_sleep_min: float = 0.8,
    page_sleep_max: float = 1.8,
) -> AppConfig:
    """Construct configuration from provided values and environment variables."""

    proxy = os.getenv("FINVIZ_PROXY")
    http = HttpConfig(proxy=proxy)
    rate_limits = RateLimits(
        rate_per_sec=rate_per_sec, page_sleep_min=page_sleep_min, page_sleep_max=page_sleep_max
    )
    run = RunConfig(
        mode=mode,
        tickers=tickers or [],
        industry_limit=industry_limit,
        ticker_limit=ticker_limit,
        out_dir=out_dir,
        formats=formats or ["parquet", "csv"],
        log_level=log_level,
        rate_limits=rate_limits,
    )
    return AppConfig(http=http, run=run)
