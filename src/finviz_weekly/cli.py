from __future__ import annotations

import argparse
import logging
from typing import Optional

from .config import env_config
from .pipeline import execute


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="finviz_weekly")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="Scrape Finviz fundamentals")
    r.add_argument("--mode", choices=["universe", "tickers"], default="universe")
    r.add_argument("--out", dest="out_dir", default="data")
    r.add_argument("--tickers", default=None, help="Comma-separated tickers (mode=tickers)")
    r.add_argument("--tickers-file", default=None, help="One ticker per line (mode=tickers)")
    r.add_argument("--ticker-limit", type=int, default=4000)

    r.add_argument("--resume", dest="resume", action="store_true", default=True)
    r.add_argument("--no-resume", dest="resume", action="store_false")

    r.add_argument("--checkpoint-every", type=int, default=25)

    r.add_argument("--concurrency", type=int, default=1)
    r.add_argument("--rate-per-sec", type=float, default=1.0)
    r.add_argument("--page-sleep-min", type=float, default=0.10)
    r.add_argument("--page-sleep-max", type=float, default=0.40)

    r.add_argument("--latest-only-ok", dest="latest_only_ok", action="store_true", default=True)
    r.add_argument("--no-latest-only-ok", dest="latest_only_ok", action="store_false")

    r.add_argument("--latest-include-as-of-date", dest="latest_include_as_of_date", action="store_true", default=True)
    r.add_argument("--no-latest-include-as-of-date", dest="latest_include_as_of_date", action="store_false")

    r.add_argument("--proxy", default=None, help="HTTP(S) proxy URL (if Finviz blocked)")

    return p


def main(argv: Optional[list[str]] = None) -> None:
    logging.basicConfig(level=logging.INFO)
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "run":
        cfg = env_config(
            mode=args.mode,
            out_dir=args.out_dir,
            tickers=args.tickers,
            tickers_file=args.tickers_file,
            ticker_limit=args.ticker_limit,
            resume=args.resume,
            checkpoint_every=args.checkpoint_every,
            concurrency=args.concurrency,
            rate_per_sec=args.rate_per_sec,
            page_sleep_min=args.page_sleep_min,
            page_sleep_max=args.page_sleep_max,
            latest_only_ok=bool(args.latest_only_ok),
            include_as_of_date_latest=bool(args.latest_include_as_of_date),
            proxy=args.proxy,
        )
        execute(cfg)
