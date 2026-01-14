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
    concurrency: int = 6
    checkpoint_every: int = 10

    def __post_init__(self):
        """Validate rate limit configuration."""
        if self.rate_per_sec <= 0 or self.rate_per_sec > 10:
            raise ValueError(
                f"rate_per_sec must be in (0, 10], got {self.rate_per_sec}. "
                "Higher values may trigger rate limiting from Finviz."
            )

        if self.page_sleep_min < 0:
            raise ValueError(f"page_sleep_min must be >= 0, got {self.page_sleep_min}")

        if self.page_sleep_max < self.page_sleep_min:
            raise ValueError(
                f"page_sleep_max ({self.page_sleep_max}) must be >= page_sleep_min ({self.page_sleep_min})"
            )

        if self.concurrency < 1 or self.concurrency > 50:
            raise ValueError(f"concurrency must be in [1, 50], got {self.concurrency}")

        if self.checkpoint_every < 1:
            raise ValueError(f"checkpoint_every must be >= 1, got {self.checkpoint_every}")


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
    resume: bool = True

    # Latest snapshot options
    latest_only_ok: bool = True
    latest_include_as_of_date: bool = True

    # Enhanced scraping options
    include_insider: bool = False
    include_earnings: bool = False
    include_financials: bool = False
    enhanced_scoring: bool = False

    def __post_init__(self):
        """Validate run configuration."""
        from pathlib import Path

        valid_modes = ["tickers", "universe", "all-screener"]
        if self.mode not in valid_modes:
            raise ValueError(f"mode must be one of {valid_modes}, got '{self.mode}'")

        if self.ticker_limit is not None and self.ticker_limit < 1:
            raise ValueError(f"ticker_limit must be >= 1 or None, got {self.ticker_limit}")

        if self.industry_limit is not None and self.industry_limit < 1:
            raise ValueError(f"industry_limit must be >= 1 or None, got {self.industry_limit}")

        # Validate output directory is writable
        out_path = Path(self.out_dir)
        try:
            out_path.mkdir(parents=True, exist_ok=True)
            if not os.access(out_path, os.W_OK):
                raise PermissionError(f"Output directory not writable: {out_path}")
        except OSError as e:
            raise PermissionError(f"Cannot create/access output directory {out_path}: {e}") from e

        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level.upper() not in valid_log_levels:
            raise ValueError(f"log_level must be one of {valid_log_levels}, got '{self.log_level}'")


@dataclass
class HttpConfig:
    """HTTP configuration and proxy support."""
    proxy: Optional[str]
    timeout_connect: int = 5
    timeout_read: int = 20
    max_retries: int = 5

    def __post_init__(self):
        """Validate HTTP configuration."""
        if self.timeout_connect < 1:
            raise ValueError(f"timeout_connect must be >= 1, got {self.timeout_connect}")

        if self.timeout_read < 1:
            raise ValueError(f"timeout_read must be >= 1, got {self.timeout_read}")

        if self.max_retries < 0 or self.max_retries > 10:
            raise ValueError(f"max_retries must be in [0, 10], got {self.max_retries}")


@dataclass
class AppConfig:
    """Application configuration."""
    http: HttpConfig
    run: RunConfig


# Used by http layer (safe defaults even if not referenced)
USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
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
    resume: bool = True,
    concurrency: int = 6,
    checkpoint_every: int = 10,
    latest_only_ok: bool = True,
    latest_include_as_of_date: bool = True,
    include_insider: bool = False,
    include_earnings: bool = False,
    include_financials: bool = False,
    enhanced_scoring: bool = False,
) -> AppConfig:
    """Construct configuration from provided values and environment variables."""
    proxy = os.getenv("FINVIZ_PROXY")
    http = HttpConfig(proxy=proxy)

    rate_limits = RateLimits(
        rate_per_sec=rate_per_sec,
        page_sleep_min=page_sleep_min,
        page_sleep_max=page_sleep_max,
        concurrency=concurrency,
        checkpoint_every=checkpoint_every,
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
        resume=resume,
        latest_only_ok=latest_only_ok,
        latest_include_as_of_date=latest_include_as_of_date,
        include_insider=include_insider,
        include_earnings=include_earnings,
        include_financials=include_financials,
        enhanced_scoring=enhanced_scoring,
    )

    return AppConfig(http=http, run=run)
