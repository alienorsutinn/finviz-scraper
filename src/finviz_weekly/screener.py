from __future__ import annotations

import logging
import re
from typing import List, Optional, Set

from bs4 import BeautifulSoup
import requests

from .config import HttpConfig
from .http import request_with_retries

LOGGER = logging.getLogger(__name__)

BASE = "https://finviz.com"


def _abs_url(href: str) -> str:
    if href.startswith("http"):
        return href
    if not href.startswith("/"):
        href = "/" + href
    return BASE + href


def _extract_tickers_from_html(html: str) -> List[str]:
    # Finviz tickers appear in screener rows as links to quote.ashx?t=XXXX
    soup = BeautifulSoup(html, "lxml")
    tickers: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "quote.ashx?t=" in href:
            # href could be relative
            m = re.search(r"quote\.ashx\?t=([A-Z0-9\-\.]+)", href)
            if m:
                t = m.group(1).strip().upper()
                if t and t not in tickers:
                    tickers.append(t)
    return tickers


def get_industries(session: requests.Session, cfg: HttpConfig) -> List[str]:
    # Attempt 1: old "industry select" on screener page
    try:
        url = f"{BASE}/screener.ashx?v=111"
        html = request_with_retries(session, url, cfg)
        soup = BeautifulSoup(html, "lxml")
        sel = soup.select_one("select#fs_ind")
        if sel:
            out: List[str] = []
            for opt in sel.find_all("option"):
                v = (opt.get("value") or "").strip()
                # Finviz uses ind_foo in screener filters
                if v.startswith("ind_"):
                    out.append(v.replace("ind_", "", 1))
            out = sorted(set(out))
            if out:
                return out
        LOGGER.warning("Industry select not found; falling back to groups.ashx parser")
    except Exception as e:
        LOGGER.warning("Industry select scrape failed (%s); falling back to groups.ashx parser", e)

    # Attempt 2: robust fallback from groups page
    url = f"{BASE}/groups.ashx?g=industry&v=110&o=name"
    html = request_with_retries(session, url, cfg)
    soup = BeautifulSoup(html, "lxml")

    codes: Set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "screener.ashx" in href and "f=ind_" in href:
            try:
                part = href.split("f=ind_", 1)[1]
                code = part.split("&", 1)[0].strip()
                if code:
                    codes.add(code)
            except Exception:
                continue

    out = sorted(codes)
    if not out:
        LOGGER.warning("Could not parse industries from groups.ashx; Finviz may be blocking HTML.")
    return out


def get_tickers_for_industry(
    session: requests.Session,
    cfg: HttpConfig,
    industry_code: str,
    *,
    ticker_limit: Optional[int] = None,
) -> List[str]:
    tickers: List[str] = []
    r = 1
    page_size_guess = 20  # safe; Finviz varies

    while True:
        url = f"{BASE}/screener.ashx?v=111&f=ind_{industry_code}&r={r}"
        html = request_with_retries(session, url, cfg)
        page = _extract_tickers_from_html(html)

        if not page:
            break

        for t in page:
            if t not in tickers:
                tickers.append(t)
                if ticker_limit and len(tickers) >= ticker_limit:
                    return tickers

        # If we got fewer than a page worth, likely last page
        if len(page) < page_size_guess:
            break

        r += len(page)

    return tickers


def get_tickers_all(
    session: requests.Session,
    cfg: HttpConfig,
    *,
    ticker_limit: Optional[int] = None,
) -> List[str]:
    # "All screener" is optional and sometimes blocked; kept for completeness.
    tickers: List[str] = []
    r = 1
    page_size_guess = 20

    while True:
        url = f"{BASE}/screener.ashx?v=111&r={r}"
        html = request_with_retries(session, url, cfg)
        page = _extract_tickers_from_html(html)

        if not page:
            break

        for t in page:
            if t not in tickers:
                tickers.append(t)
                if ticker_limit and len(tickers) >= ticker_limit:
                    return tickers

        if len(page) < page_size_guess:
            break

        r += len(page)

    return tickers
