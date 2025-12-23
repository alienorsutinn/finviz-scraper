"""Fetch fundamentals using finvizfinance."""
from __future__ import annotations

import logging
from typing import Dict

from finvizfinance.quote import finvizfinance

LOGGER = logging.getLogger(__name__)


def scrape_fundamentals(ticker: str) -> Dict[str, str]:
    """Scrape fundamentals for a ticker using finvizfinance, adding ticker key."""

    try:
        quote = finvizfinance(ticker)
        data = quote.TickerFundament()
    except Exception:  # pragma: no cover - fallback path
        LOGGER.warning("TickerFundament failed for %s, trying ticker_fundament", ticker)
        quote = finvizfinance(ticker)
        data = quote.ticker_fundament()
    if not isinstance(data, dict):
        raise ValueError(f"Unexpected data for {ticker}: {data}")
    data["Ticker"] = ticker
    return data
