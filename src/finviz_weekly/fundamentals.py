"""Fundamentals scraping.

This module intentionally accepts multiple call signatures because the pipeline
may pass (session, http_config, ticker) OR keyword args like session=..., http_config=...
FinvizFinance performs its own HTTP internally, so we ignore session/http_config.
"""

from __future__ import annotations

import time
import logging
from typing import Any, Dict, Optional

from finvizfinance.quote import finvizfinance


LOGGER = logging.getLogger(__name__)


def _pick_ticker(args: tuple[Any, ...], ticker: Optional[str]) -> str:
    # Prefer explicit ticker kwarg
    if isinstance(ticker, str) and ticker.strip():
        return ticker.strip().upper()

    # Otherwise: last positional string wins (covers (session, http, "AAPL") and ("AAPL",))
    for a in reversed(args):
        if isinstance(a, str) and a.strip():
            return a.strip().upper()

    raise TypeError("scrape_fundamentals requires a ticker (positional or ticker=...)")


def scrape_fundamentals(*args: Any, ticker: Optional[str] = None, session: Any = None, http_config: Any = None, **kwargs: Any) -> Dict[str, Any]:
    """Scrape Finviz fundamentals for a single ticker.

    Compatible call styles:
      - scrape_fundamentals("AAPL")
      - scrape_fundamentals(session, http_config, "AAPL")
      - scrape_fundamentals(ticker="AAPL", session=session, http_config=http_config)

    Returns:
      dict of Finviz fields (plus 'ticker').
    """
    t = _pick_ticker(args, ticker)

    last_exc: Optional[Exception] = None
    for attempt in range(1, 4):
        try:
            q = finvizfinance(t)
            d = q.ticker_fundament()  # dict of fields
            if not isinstance(d, dict) or not d:
                raise RuntimeError(f"Empty fundamentals dict for {t}")

            d = dict(d)  # copy
            d["ticker"] = t
            return d
        except Exception as e:
            last_exc = e
            LOGGER.warning("scrape_fundamentals failed for %s (attempt %d/3): %s", t, attempt, e)
            if attempt < 3:
                time.sleep(0.8 * attempt)
                continue
            raise RuntimeError(f"scrape_fundamentals failed for {t}: {last_exc}") from last_exc
