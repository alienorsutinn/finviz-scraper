"""Screener scraping utilities."""
from __future__ import annotations

import logging
import random
import time
from typing import List

from bs4 import BeautifulSoup

from .config import HttpConfig
from .http import request_with_retries
from .parse import is_valid_ticker

LOGGER = logging.getLogger(__name__)
BASE_URL = "https://finviz.com"


def get_industries(session, http_config: HttpConfig) -> List[str]:
    """Fetch the list of industry codes from Finviz screener."""

    url = f"{BASE_URL}/screener.ashx?v=111"
    response = request_with_retries(session, url, http_config)
    soup = BeautifulSoup(response.text, "html.parser")
    select = soup.find("select", {"id": "fs_ind"})
    industries: List[str] = []
    if not select:
        LOGGER.warning("Industry select not found")
        return industries
    for option in select.find_all("option"):
        value = (option.get("value") or "").strip()
        if value in {"", "Any", "stocksonly", "exchangetradedfund"}:
            continue
        industries.append(value)
    LOGGER.info("Found %d industries", len(industries))
    return industries


def _extract_tickers_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    tickers = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "quote.ashx?t=" in href:
            text = link.text.strip().upper()
            if is_valid_ticker(text):
                tickers.append(text)
    return tickers


def get_tickers_for_industry(
    session,
    http_config: HttpConfig,
    industry_code: str,
    *,
    ticker_limit: int | None = None,
    page_sleep_range: tuple[float, float] = (0.8, 1.8),
) -> List[str]:
    """Paginate through screener results to collect tickers for an industry."""

    tickers: List[str] = []
    start = 1
    min_sleep, max_sleep = page_sleep_range
    while True:
        url = f"{BASE_URL}/screener.ashx?v=111&f=ind_{industry_code}&r={start}"
        response = request_with_retries(session, url, http_config)
        page_tickers = _extract_tickers_from_html(response.text)
        LOGGER.debug("Industry %s offset %s found %d tickers", industry_code, start, len(page_tickers))
        if not page_tickers:
            break
        for t in page_tickers:
            if t not in tickers:
                tickers.append(t)
            if ticker_limit and len(tickers) >= ticker_limit:
                return tickers
        start += 20
        sleep_for = random.uniform(min_sleep, max_sleep)
        time.sleep(sleep_for)
    LOGGER.info("Collected %d tickers for %s", len(tickers), industry_code)
    return tickers


__all__ = ["get_industries", "get_tickers_for_industry", "_extract_tickers_from_html"]
