from __future__ import annotations

import logging
import random
import time
from datetime import date
from pathlib import Path
from typing import List

import pandas as pd

from .config import AppConfig
from .fundamentals import scrape_fundamentals
from .http import create_session
from .screener import get_tickers_all
from .storage import (
    append_history,
    prepare_run_dir,
    read_checkpoint,
    update_latest,
    write_checkpoint,
    write_meta,
)

LOGGER = logging.getLogger(__name__)

def _load_cached_universe(out_dir: str):
    from pathlib import Path
    import pandas as pd
    p = Path(out_dir) / 'latest' / 'finviz_scored.parquet'
    if not p.exists():
        return None
    df = pd.read_parquet(p)
    if 'ticker' not in df.columns:
        return None
    tickers = [t for t in df['ticker'].dropna().astype(str).str.upper().tolist() if t]
    return tickers



def _load_cached_universe(out_dir: str) -> List[str]:
    candidates = [
        Path(out_dir) / "latest" / "finviz_scored.parquet",
        Path(out_dir) / "history" / "finviz_fundamentals_history.parquet",
        Path(out_dir) / "latest" / "finviz_fundamentals.parquet",
    ]
    for p in candidates:
        if not p.exists():
            continue
        try:
            df = pd.read_parquet(p)
            if "ticker" in df.columns and len(df) > 0:
                tickers = df["ticker"].astype(str).str.upper().dropna().unique().tolist()
                tickers = [t for t in tickers if t and t != "NAN"]
                if tickers:
                    LOGGER.warning("Using cached universe from %s (%s tickers)", p, len(tickers))
                    return tickers
        except Exception as e:
            LOGGER.warning("Failed reading cached universe from %s (%s)", p, e)
            continue
    return []


def build_universe(cfg: AppConfig) -> List[str]:
    session = create_session(cfg.http)

    # Try live Finviz screener
    try:
        tickers = get_tickers_all(
            session,
            cfg.http,
            ticker_limit=cfg.run.ticker_limit,
            rate_per_sec=cfg.run.rate_per_sec,
        )
        tickers = list(dict.fromkeys([t.upper() for t in tickers]))  # stable dedupe
        if tickers:
            return tickers
        LOGGER.warning("Live Finviz universe returned 0 tickers.")
    except Exception as e:
        LOGGER.warning("Live Finviz universe failed (%s).", e)

    # Fallback to cached tickers
    cached = _load_cached_universe(cfg.run.out_dir)
    if cached:
        return cached[: cfg.run.ticker_limit]

    return []


def execute(cfg: AppConfig):
    as_of = date.today()
    run_dir = prepare_run_dir(cfg.run.out_dir, as_of)

    # Decide tickers list
    if cfg.run.mode == "tickers":
        if cfg.run.tickers:
            tickers = [t.strip().upper() for t in cfg.run.tickers.split(",") if t.strip()]
        elif cfg.run.tickers_file:
            txt = Path(cfg.run.tickers_file).read_text(encoding="utf-8")
            tickers = [t.strip().upper() for t in txt.splitlines() if t.strip()]
        else:
            raise RuntimeError("mode=tickers requires --tickers or --tickers-file")
    else:
        tickers = None
        if getattr(cfg.run, 'prefer_cached_universe', True):
            tickers = _load_cached_universe(cfg.run.out_dir)
        if not tickers:
            try:
                tickers = build_universe(session, config)
            except Exception as e:
                LOGGER.warning("Live Finviz universe failed (%s)", e)
                if not tickers:
                    tickers = _load_cached_universe(cfg.run.out_dir)
        if not tickers:
            raise RuntimeError("Universe is empty. Live Finviz blocked and no cached finviz_scored.parquet available.")

    tickers = tickers[: cfg.run.ticker_limit]

    if not tickers:
        raise RuntimeError(
            "Universe is empty (Finviz blocked and no cache available). "
            "Aborting to avoid writing empty outputs."
        )

    # Resume
    done = read_checkpoint(run_dir) if cfg.run.resume else set()
    if done:
        LOGGER.info("Resume enabled: %s tickers already checkpointed for %s", len(done), as_of.isoformat())

    remaining = [t for t in tickers if t not in done]
    LOGGER.info("Universe size: %s tickers", len(tickers))
    LOGGER.info("Scraping %s tickers (remaining), %s already saved", len(remaining), len(done))

    # scrape
    session = create_session(cfg.http)
    rows = []
    completed = set(done)

    for i, t in enumerate(remaining, start=1):
        # small jitter to reduce blocking risk
        if cfg.run.page_sleep_max > 0:
            time.sleep(random.uniform(cfg.run.page_sleep_min, cfg.run.page_sleep_max))

        try:
            row = scrape_fundamentals(t, session=session)
            if row:
                row["ticker"] = t
                rows.append(row)
        except Exception as e:
            LOGGER.warning("Scrape failed for %s (%s)", t, e)

        completed.add(t)

        if (i % cfg.run.checkpoint_every) == 0:
            write_checkpoint(run_dir, completed)
            LOGGER.info("Checkpoint: %s/%s", len(completed), len(tickers))

    # Final write
    write_checkpoint(run_dir, completed)

    df = pd.DataFrame(rows)
    if not df.empty and cfg.run.include_as_of_date_latest:
        df.insert(0, "as_of_date", as_of.isoformat())

    # Always write a run artifact (even if small), but NEVER update latest/history if empty
    (run_dir / "fundamentals.parquet").write_bytes(b"")  # placeholder for atomic intent
    if not df.empty:
        df.to_parquet(run_dir / "fundamentals.parquet", index=False)
    else:
        # keep placeholder; meta will show 0
        LOGGER.warning("Run produced 0 rows (Finviz likely blocked). Not updating latest/history.")

    if not df.empty:
        update_latest(
            df,
            cfg.run.out_dir,
            only_ok=cfg.run.latest_only_ok,
            include_as_of_date=cfg.run.include_as_of_date_latest,
        )
        append_history(df, cfg.run.out_dir, as_of)

    write_meta(run_dir, {"as_of": as_of.isoformat(), "tickers_total": len(tickers), "tickers_done": len(completed)})
    LOGGER.info("Run completed, data in %s", run_dir)
    return df
