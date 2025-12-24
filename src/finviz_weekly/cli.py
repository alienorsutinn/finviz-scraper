"""Command line interface for finviz_weekly."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import List

from .config import env_config
from .http import create_session
from .pipeline import execute
from .screen import run_screening
from .learn import train_weights
from .report import write_report_from_latest
from .debate import run_debate


LOGGER = logging.getLogger(__name__)


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finviz weekly scraper")

    parser.add_argument(
        "command",
        nargs="?",
        default="run",
        choices=["run", "screen", "train", "report", "debate"],
        help="Command to execute (default: run).",
    )

    # run args
    parser.add_argument("--mode", choices=["universe", "tickers"], required=False)
    parser.add_argument("--tickers", help="Comma separated list of tickers")
    parser.add_argument("--tickers-file", help="Path to file with tickers")
    parser.add_argument("--industry-limit", type=int)
    parser.add_argument("--ticker-limit", type=int)

    parser.add_argument("--rate-per-sec", type=float, default=0.5)
    parser.add_argument("--page-sleep-min", type=float, default=0.8)
    parser.add_argument("--page-sleep-max", type=float, default=1.8)
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--checkpoint-every", type=int, default=10)
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resume from today's partial checkpoint (default: enabled).",
    )
    parser.add_argument("--out", default="data")
    parser.add_argument("--formats", default="parquet,csv")
    parser.add_argument("--log-level", default="INFO")

    # latest snapshot options (run)
    parser.add_argument(
        "--latest-only-ok",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Only write ok rows to data/latest (default: enabled).",
    )
    parser.add_argument(
        "--latest-include-as-of-date",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include as_of_date in data/latest (default: enabled).",
    )

    # screening args (screen)
    parser.add_argument("--top", type=int, default=50, help="Top-N tickers per screen.")
    parser.add_argument("--min-market-cap", type=float, default=300_000_000)
    parser.add_argument("--min-price", type=float, default=1.0)
    parser.add_argument("--candidates-max", type=int, default=100)

    # debate args
    parser.add_argument("--input", choices=["candidates", "conviction2", "tickers"], default="candidates")
    parser.add_argument("--max-tickers", type=int, default=30)
<<<<<<< ours
    parser.add_argument("--research", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--recency-days", type=int, default=30)
    parser.add_argument("--max-queries-per-ticker", type=int, default=20)
    parser.add_argument("--max-results-per-query", type=int, default=3)
    parser.add_argument("--evidence-max", type=int, default=25)
    parser.add_argument("--cache-days", type=int, default=14)
=======
    parser.add_argument("--research", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--recency-days", type=int, default=30)
    parser.add_argument("--max-queries-per-ticker", type=int, default=12)
    parser.add_argument("--max-results-per-query", type=int, default=3)
    parser.add_argument("--evidence-max", type=int, default=20)
    parser.add_argument("--cache-days", type=int, default=14)
    parser.add_argument("--timeout-seconds", type=int, default=15)
    parser.add_argument("--as-of")
    parser.add_argument("--provider", choices=["openai", "mock"], default="openai")
    parser.add_argument("--model", help="LLM model name (default env OPENAI_MODEL or gpt-5-mini)")
    parser.add_argument("--verbose", action=argparse.BooleanOptionalAction, default=False)
>>>>>>> theirs

    # training args (train)
    parser.add_argument("--min-rows-per-group", type=int, default=250)
    parser.add_argument("--group-col", type=str, default="sector")

    return parser.parse_args(argv)


def _load_tickers(args: argparse.Namespace) -> list[str]:
    tickers: list[str] = []
    if args.tickers:
        tickers.extend([t.strip().upper() for t in args.tickers.split(",") if t.strip()])
    if args.tickers_file:
        path = Path(args.tickers_file)
        if path.exists():
            tickers.extend([t.strip().upper() for t in path.read_text().splitlines() if t.strip()])
    return tickers


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    if args.command == "screen":
        run_screening(
            out_dir=args.out,
            top_n=int(args.top),
            min_market_cap=float(args.min_market_cap),
            min_price=float(args.min_price),
            candidates_max=int(args.candidates_max),
        )
        return

    if args.command == "train":
        train_weights(
            out_dir=args.out,
            min_rows_per_group=int(args.min_rows_per_group),
            group_col=str(args.group_col),
        )
        return

    if args.command == "report":
        write_report_from_latest(out_dir=args.out)
        return

    if args.command == "debate":
        run_debate(
            out_dir=args.out,
            input_mode=args.input,
            tickers=args.tickers,
            max_tickers=args.max_tickers,
            research=bool(args.research),
            recency_days=args.recency_days,
            max_queries_per_ticker=args.max_queries_per_ticker,
            max_results_per_query=args.max_results_per_query,
            evidence_max=args.evidence_max,
            cache_days=args.cache_days,
<<<<<<< ours
=======
            timeout_seconds=args.timeout_seconds,
            as_of=args.as_of,
            provider=args.provider,
            model=args.model,
>>>>>>> theirs
        )
        return

    # run
    if not args.mode:
        raise SystemExit("--mode is required for the 'run' command")

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
        resume=args.resume,
        concurrency=args.concurrency,
        checkpoint_every=args.checkpoint_every,
        latest_only_ok=bool(args.latest_only_ok),
        latest_include_as_of_date=bool(args.latest_include_as_of_date),
    )

    session = create_session(config.http)
    execute(session, config)


if __name__ == "__main__":
    main()
