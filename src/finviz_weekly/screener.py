from __future__ import annotations

import logging
import re
import time
from typing import List, Optional

from bs4 import BeautifulSoup

from .config import HttpConfig
from .http import request_with_retries

LOGGER = logging.getLogger(__name__)

_FINVIZ_SCREENER = "https://finviz.com/screener.ashx"


def _extract_tickers(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.select("a.screener-link-primary"):
        t = (a.get_text() or "").strip().upper()
        if t and re.fullmatch(r"[A-Z.\-]{1,12}", t):
            out.append(t)
    return out


def get_tickers_all(
    session,
    http_cfg: HttpConfig,
    *,
    ticker_limit: int,
    rate_per_sec: float = 1.0,
) -> List[str]:
    """
    Pull tickers from Finviz screener pages by paging `r=1,21,41,...`.
    If Finviz blocks (empty HTML / bot wall), this will raise or return [].
    """
    tickers: List[str] = []
    seen = set()

    r = 1
    step = 20
    delay = 1.0 / max(rate_per_sec, 0.01)

    while len(tickers) < ticker_limit:
        html = request_with_retries(session, _FINVIZ_SCREENER, http_cfg, params={"v": "111", "r": str(r)})
        page = _extract_tickers(html)

        new = 0
        for t in page:
            if t not in seen:
                seen.add(t)
                tickers.append(t)
                new += 1
                if len(tickers) >= ticker_limit:
                    break

        if new == 0:
            break

        r += step
        time.sleep(delay)

    return tickers
