from __future__ import annotations
import datetime as dt

import io
import logging
import time
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class PriceFetchConfig:
    lookback_years: int = 10
    max_workers: int = 10
    timeout_sec: int = 10
    retries: int = 2
    auto_adjust: bool = True
    backoff_sec: float = 0.5
    ping_timeout_sec: int = 3



def _env_price_source() -> str:
    """Return the requested price source via FINVIZ_PRICE_SOURCE (yahoo|stooq|auto)."""
    v = (os.getenv("FINVIZ_PRICE_SOURCE") or "").strip().lower()
    if not v:
        return "auto"
    if v in {"yahoo", "yfinance", "yf"}:
        return "yahoo"
    if v in {"stooq"}:
        return "stooq"
    if v in {"auto"}:
        return "auto"
    log.warning("Unknown FINVIZ_PRICE_SOURCE=%r; using auto.", v)
    return "auto"


def _stooq_symbol(ticker: str) -> str:
    t = ticker.strip().lower()
    return f"{t}.us"


def _ping_url(url: str, timeout_sec: int) -> bool:
    try:
        r = requests.get(url, timeout=timeout_sec)
        return r.status_code < 500
    except Exception:
        return False


def _stooq_is_reachable(cfg: PriceFetchConfig) -> bool:
    # Ping the *data* endpoint (homepage can be reachable while the CSV endpoint is blocked).
    test = "https://stooq.com/q/d/l/?s=aapl.us&i=d"
    return _ping_url(test, cfg.ping_timeout_sec) or _ping_url(test.replace("https://", "http://"), cfg.ping_timeout_sec)


def _fetch_stooq_daily(ticker: str, cfg: PriceFetchConfig) -> pd.DataFrame:
    sym = _stooq_symbol(ticker)

    # Try http first then https (some networks block TLS here)
    urls = [
        f"http://stooq.com/q/d/l/?s={sym}&i=d",
        f"https://stooq.com/q/d/l/?s={sym}&i=d",
    ]

    last_err: Optional[Exception] = None
    for attempt in range(cfg.retries):
        for url in urls:
            try:
                r = requests.get(url, timeout=cfg.timeout_sec)
                r.raise_for_status()

                df = pd.read_csv(io.StringIO(r.text))
                if df.empty or "Date" not in df.columns:
                    return pd.DataFrame()

                df = df.rename(
                    columns={
                        "Date": "date",
                        "Close": "close",
                        "Open": "open",
                        "High": "high",
                        "Low": "low",
                        "Volume": "volume",
                    }
                )
                df["ticker"] = ticker.upper()
                df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
                df = df.dropna(subset=["date"])
                df = df.sort_values(["ticker", "date"])

                keep = ["ticker", "date", "close", "open", "high", "low", "volume"]
                for c in keep:
                    if c not in df.columns:
                        df[c] = pd.NA
                return df[keep]

            except Exception as e:
                last_err = e

        time.sleep(cfg.backoff_sec * (attempt + 1))

    if last_err:
        raise last_err
    return pd.DataFrame()



def _fetch_yahoo_daily(ticker: str, cfg: PriceFetchConfig) -> pd.DataFrame:
    """Fetch daily prices from Yahoo Finance via yfinance (robust to MultiIndex output)."""
    try:
        import yfinance as yf  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "yfinance is required for Yahoo price fetch. Install with: pip install -U yfinance"
        ) from e

    start = (dt.date.today() - dt.timedelta(days=365 * cfg.lookback_years)).strftime("%Y-%m-%d")
    end = dt.date.today().strftime("%Y-%m-%d")

    data = yf.download(
        ticker.upper(),
        start=start,
        end=end,
        interval="1d",
        progress=False,
        auto_adjust=getattr(cfg, "auto_adjust", False),
        threads=False,  # we're already parallelising at a higher level
    )
    if data is None or data.empty:
        return pd.DataFrame(columns=["ticker", "date", "close"])

    # yfinance can return MultiIndex columns even for a single ticker (e.g. ('Close','AAPL')).
    if isinstance(data.columns, pd.MultiIndex):
        try:
            lvl_last = list(dict.fromkeys(data.columns.get_level_values(-1)))
            if len(lvl_last) == 1:
                data.columns = data.columns.get_level_values(0)
            else:
                data = data.xs(ticker.upper(), axis=1, level=-1, drop_level=True)
        except Exception:
            data.columns = ["_".join([str(x) for x in col if x is not None]) for col in data.columns]

    data = data.reset_index()

    colmap = {str(c).strip().lower(): c for c in data.columns}
    close_col = None
    for k in ["close", "adj close", "adjclose", "adj_close"]:
        if k in colmap:
            close_col = colmap[k]
            break
    if close_col is None:
        return pd.DataFrame(columns=["ticker", "date", "close"])

    date_col = colmap.get("date") or colmap.get("datetime") or colmap.get("index")
    if date_col is None:
        for c in data.columns:
            if str(c).strip().lower() == "date":
                date_col = c
                break
    if date_col is None:
        return pd.DataFrame(columns=["ticker", "date", "close"])

    out = data[[date_col, close_col]].copy()
    out = out.rename(columns={date_col: "date", close_col: "close"})
    out["ticker"] = ticker.upper()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["date", "close"])
    return out[["ticker", "date", "close"]]


    # Yahoo returns timezone-aware DatetimeIndex; we convert to date.
    # Use a long period so we can compute 21d/63d/126d forward returns reliably.
    period = f"{cfg.lookback_years}y"
    data = yf.download(ticker, period=period, interval="1d", auto_adjust=getattr(cfg, "auto_adjust", False), progress=False)

    if data is None or len(data) == 0:
        return pd.DataFrame()

    data = data.reset_index()
    # Column names vary; normalize
    colmap = {c.lower(): c for c in data.columns}
    date_col = "Date" if "Date" in data.columns else ("Datetime" if "Datetime" in data.columns else None)
    if date_col is None:
        # fallback: first column
        date_col = data.columns[0]

    close_col = colmap.get("close", None)
    open_col = colmap.get("open", None)
    high_col = colmap.get("high", None)
    low_col = colmap.get("low", None)
    vol_col = colmap.get("volume", None)

    df = pd.DataFrame(
        {
            "ticker": str(ticker).upper(),
            "date": pd.to_datetime(data[date_col], errors="coerce").dt.date,
            "close": data[close_col] if close_col else pd.NA,
            "open": data[open_col] if open_col else pd.NA,
            "high": data[high_col] if high_col else pd.NA,
            "low": data[low_col] if low_col else pd.NA,
            "volume": data[vol_col] if vol_col else pd.NA,
        }
    )
    df = df.dropna(subset=["date", "close"])
    df = df.sort_values(["ticker", "date"])
    return df


def read_prices(prices_path: Path) -> pd.DataFrame:
    if prices_path.exists():
        return pd.read_parquet(prices_path)
    return pd.DataFrame(columns=["ticker", "date", "close", "open", "high", "low", "volume"])


def write_prices(prices_path: Path, df: pd.DataFrame) -> None:
    prices_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(prices_path, index=False)


def upsert_prices(prices_path: Path, new_df: pd.DataFrame) -> pd.DataFrame:
    existing = read_prices(prices_path)
    if existing.empty:
        merged = new_df.copy()
    else:
        merged = pd.concat([existing, new_df], ignore_index=True)

    merged["ticker"] = merged["ticker"].astype(str).str.upper()
    merged["date"] = pd.to_datetime(merged["date"], errors="coerce").dt.date
    merged = merged.dropna(subset=["ticker", "date"])
    merged = merged.sort_values(["ticker", "date"])
    merged = merged.drop_duplicates(subset=["ticker", "date"], keep="last")
    write_prices(prices_path, merged)
    return merged


def ensure_price_history(
    out_dir: Path,
    tickers: Iterable[str],
    *,
    cfg: PriceFetchConfig | None = None,
) -> Path:
    """
    Ensures data/history/prices.parquet exists and contains price rows for tickers.
    Tries Stooq first (http/https). If Stooq unreachable, falls back to Yahoo via yfinance.
    """
    cfg = cfg or PriceFetchConfig()
    prices_path = out_dir / "history" / "prices.parquet"

    tickers = sorted({t.strip().upper() for t in tickers if t and str(t).strip()})
    if not tickers:
        raise ValueError("No tickers provided to ensure_price_history()")

    existing = read_prices(prices_path)
    have = set(existing["ticker"].astype(str).str.upper().unique()) if not existing.empty and "ticker" in existing.columns else set()
    missing = [t for t in tickers if t not in have]

    if not missing:
        log.info("Prices already present for %d tickers (%s)", len(tickers), prices_path)
        return prices_path

    src = _env_price_source()
    if src == "stooq":
        use_stooq = True
    elif src == "yahoo":
        use_stooq = False
    else:
        use_stooq = _stooq_is_reachable(cfg)
    if not use_stooq:
        log.warning("Stooq not reachable from this network. Falling back to Yahoo (yfinance).")

    log.info("Fetching prices for %d missing tickers (%s) ...", len(missing), "stooq" if use_stooq else "yahoo")

    fetched_parts: list[pd.DataFrame] = []

    ex = ThreadPoolExecutor(max_workers=cfg.max_workers)
    try:
        futs = {}
        for t in missing:
            if use_stooq:
                futs[ex.submit(_fetch_stooq_daily, t, cfg)] = t
            else:
                futs[ex.submit(_fetch_yahoo_daily, t, cfg)] = t

        for fut in as_completed(futs):
            t = futs[fut]
            try:
                df = fut.result()
                if df is None or df.empty:
                    continue
                fetched_parts.append(df)
            except Exception as e:
                # If stooq is reachable but individual tickers fail, just warn.
                log.warning("Price fetch failed for %s: %s", t, e)

    except KeyboardInterrupt:
        log.warning("Interrupted by user. Cancelling outstanding price fetches...")
        ex.shutdown(wait=False, cancel_futures=True)
        raise
    finally:
        # normal shutdown
        try:
            ex.shutdown(wait=True, cancel_futures=True)
        except TypeError:
            # older python fallback
            ex.shutdown(wait=True)

    if not fetched_parts:
        log.warning("No new prices fetched. Leaving prices store as-is (%s).", prices_path)
        # still ensure the folder exists
        prices_path.parent.mkdir(parents=True, exist_ok=True)
        if not prices_path.exists():
            write_prices(prices_path, existing if existing is not None else pd.DataFrame())
        return prices_path

    new_df = pd.concat(fetched_parts, ignore_index=True)
    upsert_prices(prices_path, new_df)
    log.info("Wrote/updated prices store: %s (new_rows=%d)", prices_path, len(new_df))
    return prices_path
