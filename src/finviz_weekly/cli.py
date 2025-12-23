"""Command line interface for finviz_weekly."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import List

from .config import env_config
from .http import create_session
from .pipeline import execute


LOGGER = logging.getLogger(__name__)


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finviz weekly scraper")
    parser.add_argument("run", nargs="?")
    parser.add_argument("--mode", choices=["universe", "tickers"], required=True)
    parser.add_argument("--tickers", help="Comma separated list of tickers")
    parser.add_argument("--tickers-file", help="Path to file with tickers")
    parser.add_argument("--industry-limit", type=int)
    parser.add_argument("--ticker-limit", type=int)
    parser.add_argument("--rate-per-sec", type=float, default=0.5)
    parser.add_argument("--page-sleep-min", type=float, default=0.8)
    parser.add_argument("--page-sleep-max", type=float, default=1.8)
    parser.add_argument("--out", default="data")
    parser.add_argument("--formats", default="parquet,csv")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def _load_tickers(args: argparse.Namespace) -> list[str]:
    tickers: list[str] = []
    if args.tickers:
        tickers.extend([t.strip().upper() for t in args.tickers.split(",") if t.strip()])
    if args.tickers_file:
        path = Path(args.tickers_file)
        if path.exists():
            contents = path.read_text().splitlines()
            tickers.extend([c.strip().upper() for c in contents if c.strip()])
    return tickers


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    tickers = _load_tickers(args)
    config = env_config(
        mode=args.mode,
        tickers=tickers,
        industry_limit=args.industry_limit,
        ticker_limit=args.ticker_limit,
        out_dir=args.out,
        formats=[f.strip() for f in args.formats.split(",") if f.strip()],
        log_level=args.log_level,
        rate_per_sec=args.rate_per_sec,
        page_sleep_min=args.page_sleep_min,
        page_sleep_max=args.page_sleep_max,
    )
    session = create_session(config.http)
    execute(session, config)


if __name__ == "__main__":
    main()
