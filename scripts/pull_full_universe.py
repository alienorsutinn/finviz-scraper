#!/usr/bin/env python3
"""
Pull a *full* US-listed ticker universe (NYSE/Nasdaq/AMEX) and scrape Finviz fundamentals.

Why this exists:
- Your "train" step needs history (multiple snapshot dates) to learn weights.
- Today you only have 1 snapshot date, so learning will fall back (expected).
- This script lets you pull ALL tickers now, then you run it daily to build history.

Outputs (default --out data):
- data/latest/universe.csv
- data/latest/finviz_fundamentals.parquet
- data/history/finviz_fundamentals_history.parquet   (append with as_of_date)
- data/history/pull_full_universe_checkpoint.json    (resume support)
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests


NASDAQLISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHERLISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"


@dataclass
class SymDirConfig:
    include_etf: bool = True
    include_test_issues: bool = False
    cache: bool = True


def _safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _http_get_text(url: str, timeout_s: float = 30.0) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        )
    }
    r = requests.get(url, headers=headers, timeout=timeout_s)
    r.raise_for_status()
    return r.text


def _read_cached_or_fetch(url: str, cache_path: Path, cache: bool = True) -> str:
    if cache and cache_path.exists():
        return cache_path.read_text(encoding="utf-8", errors="ignore")
    txt = _http_get_text(url)
    if cache:
        cache_path.write_text(txt, encoding="utf-8")
    return txt


def _parse_pipe_table(txt: str) -> Tuple[List[str], List[List[str]]]:
    """
    Nasdaq Trader symbol directory files are pipe-delimited with a header row and a footer line like:
      "File Creation Time: ..."
    """
    lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
    # Drop footer
    lines = [ln for ln in lines if not ln.lower().startswith("file creation time")]
    header = lines[0].split("|")
    rows = [ln.split("|") for ln in lines[1:]]
    # Some lines may have trailing pipes; pad/truncate to header length.
    out_rows: List[List[str]] = []
    for r in rows:
        if len(r) < len(header):
            r = r + [""] * (len(header) - len(r))
        out_rows.append(r[: len(header)])
    return header, out_rows


def _clean_symbol(sym: str) -> Optional[str]:
    sym = sym.strip()
    if not sym:
        return None
    # Filter out obvious non-tickers in these files
    if sym.upper() in {"SYMBOL", "ACT SYMBOL"}:
        return None
    # Remove weird whitespace
    sym = re.sub(r"\s+", "", sym)
    # Some symbols contain special markers we don't want
    if "^" in sym or "~" in sym:
        return None
    return sym


def load_us_listed_universe(
    out_dir: Path,
    cfg: SymDirConfig,
    as_of_date: dt.date,
) -> List[str]:
    """
    Returns a deduped, sorted list of US-listed symbols from Nasdaq Trader.
    Includes NYSE/Nasdaq/AMEX listings. OTC is NOT included here.
    """
    hist_dir = out_dir / "history"
    _safe_mkdir(hist_dir)

    stamp = as_of_date.strftime("%Y%m%d")
    nasdaq_cache = hist_dir / f"symdir_nasdaqlisted_{stamp}.txt"
    other_cache = hist_dir / f"symdir_otherlisted_{stamp}.txt"

    nasdaq_txt = _read_cached_or_fetch(NASDAQLISTED_URL, nasdaq_cache, cache=cfg.cache)
    other_txt = _read_cached_or_fetch(OTHERLISTED_URL, other_cache, cache=cfg.cache)

    n_header, n_rows = _parse_pipe_table(nasdaq_txt)
    o_header, o_rows = _parse_pipe_table(other_txt)

    # Index headers
    n_idx = {c: i for i, c in enumerate(n_header)}
    o_idx = {c: i for i, c in enumerate(o_header)}

    # nasdaqlisted.txt: Symbol | Security Name | Market Category | Test Issue | Financial Status | Round Lot Size | ETF | NextShares
    # otherlisted.txt: ACT Symbol | Security Name | Exchange | CQS Symbol | ETF | Round Lot Size | Test Issue | NASDAQ Symbol
    tickers: List[str] = []

    for r in n_rows:
        sym = _clean_symbol(r[n_idx.get("Symbol", 0)])
        if not sym:
            continue
        test_issue = (r[n_idx.get("Test Issue", 0)] or "").strip().upper()
        etf_flag = (r[n_idx.get("ETF", 0)] or "").strip().upper()
        if not cfg.include_test_issues and test_issue == "Y":
            continue
        if not cfg.include_etf and etf_flag == "Y":
            continue
        tickers.append(sym)

    for r in o_rows:
        sym = _clean_symbol(r[o_idx.get("ACT Symbol", 0)])
        if not sym:
            continue
        test_issue = (r[o_idx.get("Test Issue", 0)] or "").strip().upper()
        etf_flag = (r[o_idx.get("ETF", 0)] or "").strip().upper()
        if not cfg.include_test_issues and test_issue == "Y":
            continue
        if not cfg.include_etf and etf_flag == "Y":
            continue
        tickers.append(sym)

    # Dedup + sort
    tickers = sorted(set(tickers))

    # Very rare: drop completely broken entries
    tickers = [t for t in tickers if len(t) <= 12]
    return tickers


def _load_checkpoint(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"done": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"done": []}


def _save_checkpoint(path: Path, done: List[str]) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"done": sorted(set(done))}, indent=2), encoding="utf-8")
    tmp.replace(path)


def _try_import_finviz_scraper() -> Any:
    """
    Your repo already has finviz scraping code; we try to import it.
    Expected function (common in your project): finviz_weekly.fundamentals.scrape_fundamentals(ticker, session, http_cfg)
    """
    try:
        from finviz_weekly.fundamentals import scrape_fundamentals  # type: ignore
        return scrape_fundamentals
    except Exception as e:
        raise RuntimeError(
            "Could not import finviz_weekly.fundamentals.scrape_fundamentals. "
            "Make sure you're running from the repo root with the venv activated."
        ) from e


def _try_import_http_cfg() -> Tuple[Any, Any]:
    """
    We try to reuse your existing session/config if present.
    Fallback to a plain requests.Session.
    """
    try:
        from finviz_weekly.config import HttpConfig  # type: ignore
        from finviz_weekly.http import create_session  # type: ignore

        http_cfg = HttpConfig()
        session = create_session(http_cfg)
        return session, http_cfg
    except Exception:
        # Minimal fallback
        sess = requests.Session()
        return sess, None


def _normalize_ticker_for_finviz(t: str) -> str:
    """
    Finviz often uses dot-class tickers (BRK.B). Keep dots as-is.
    Some data sources use '-' (BRK-B). If your universe contains '-', convert to '.'.
    """
    if "-" in t and "." not in t:
        # This is a heuristic; works for most class shares.
        return t.replace("-", ".")
    return t


def scrape_all_fundamentals(
    tickers: List[str],
    out_dir: Path,
    as_of_date: dt.date,
    workers: int,
    sleep_ms: int,
    checkpoint_every: int,
    resume: bool,
) -> pd.DataFrame:
    """
    Scrape fundamentals for all tickers.
    Uses threads conservatively; Finviz will throttle if you go crazy.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    hist_dir = out_dir / "history"
    _safe_mkdir(hist_dir)
    ckpt_path = hist_dir / "pull_full_universe_checkpoint.json"

    scrape_fundamentals = _try_import_finviz_scraper()

    done: List[str] = []
    if resume:
        done = _load_checkpoint(ckpt_path).get("done", [])
        done = [d for d in done if isinstance(d, str)]

    done_set = set(done)

    # Filter tickers to do
    todo = [t for t in tickers if t not in done_set]

    print(f"[info] as_of_date={as_of_date} tickers_total={len(tickers)} todo={len(todo)} done={len(done_set)}")

    # Thread-local sessions (avoid sharing one Session across threads)
    import threading

    local = threading.local()

    def get_client() -> Tuple[requests.Session, Any]:
        if getattr(local, "session", None) is None:
            session, http_cfg = _try_import_http_cfg()
            local.session = session
            local.http_cfg = http_cfg
        return local.session, local.http_cfg

    def fetch_one(ticker: str) -> Dict[str, Any]:
        t0 = time.time()
        ticker_finviz = _normalize_ticker_for_finviz(ticker)
        session, http_cfg = get_client()
        try:
            if http_cfg is None:
                # Some implementations expect (ticker, session, http_cfg)
                rec = scrape_fundamentals(ticker_finviz, session, None)
            else:
                rec = scrape_fundamentals(ticker_finviz, session, http_cfg)
            if not isinstance(rec, dict):
                rec = {"_raw": str(rec)}
            rec["ticker"] = ticker
            rec["as_of_date"] = str(as_of_date)
            rec["__status"] = "ok"
            rec["__latency_s"] = round(time.time() - t0, 3)
            return rec
        except Exception as e:
            return {
                "ticker": ticker,
                "as_of_date": str(as_of_date),
                "__status": "error",
                "__error": repr(e),
                "__latency_s": round(time.time() - t0, 3),
            }
        finally:
            # Gentle pacing
            if sleep_ms > 0:
                time.sleep((sleep_ms + random.randint(0, 100)) / 1000.0)

    rows: List[Dict[str, Any]] = []
    failures = 0
    completed = 0

    # IMPORTANT: keep workers modest (Finviz will throttle/ban if too aggressive)
    workers = max(1, int(workers))

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_one, t): t for t in todo}
        for fut in as_completed(futs):
            rec = fut.result()
            rows.append(rec)
            completed += 1
            if rec.get("__status") != "ok":
                failures += 1

            # checkpoint
            if rec.get("ticker"):
                done_set.add(rec["ticker"])

            if checkpoint_every > 0 and completed % checkpoint_every == 0:
                _save_checkpoint(ckpt_path, list(done_set))
                print(
                    f"[progress] completed={completed}/{len(todo)} "
                    f"ok={completed - failures} fail={failures} "
                    f"ckpt_written={ckpt_path}"
                )

    # Final checkpoint
    _save_checkpoint(ckpt_path, list(done_set))
    print(f"[done] ok={len([r for r in rows if r.get('__status') == 'ok'])} fail={failures}")

    df = pd.DataFrame(rows)
    return df


def _append_history(history_path: Path, df: pd.DataFrame) -> None:
    if history_path.exists():
        try:
            old = pd.read_parquet(history_path)
            out = pd.concat([old, df], ignore_index=True)
        except Exception:
            out = df.copy()
    else:
        out = df.copy()

    # De-dupe by (ticker, as_of_date) keeping last
    if "ticker" in out.columns and "as_of_date" in out.columns:
        out = out.drop_duplicates(subset=["ticker", "as_of_date"], keep="last")

    _safe_mkdir(history_path.parent)
    out.to_parquet(history_path, index=False)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data", help="Output directory (default: data)")
    p.add_argument("--include-etf", action="store_true", help="Include ETFs (default: False unless set)")
    p.add_argument("--exclude-etf", action="store_true", help="Exclude ETFs explicitly")
    p.add_argument("--workers", type=int, default=3, help="Thread workers for finviz scraping (default: 3)")
    p.add_argument("--sleep-ms", type=int, default=150, help="Sleep between requests per worker (default: 150ms)")
    p.add_argument("--checkpoint-every", type=int, default=50, help="Write resume checkpoint every N tickers")
    p.add_argument("--no-resume", action="store_true", help="Do not resume from checkpoint; start fresh")
    args = p.parse_args()

    out_dir = Path(args.out)
    latest_dir = out_dir / "latest"
    hist_dir = out_dir / "history"
    _safe_mkdir(latest_dir)
    _safe_mkdir(hist_dir)

    as_of = dt.date.today()
