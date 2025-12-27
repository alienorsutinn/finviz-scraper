from __future__ import annotations

import time

DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8',
    'Connection': 'keep-alive',
    'Referer': 'https://finviz.com/',
}

import random
import logging
from typing import Optional

import requests

from .config import HttpConfig

LOGGER = logging.getLogger(__name__)


def create_session(config: Optional[HttpConfig] = None) -> requests.Session:
    cfg = config or HttpConfig()
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": cfg.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    if cfg.proxy:
        s.proxies.update({"http": cfg.proxy, "https": cfg.proxy})
    return s


def request_with_retries(session, url: str, cfg):
    """GET a URL with retries. Raises on persistent failures.

    Returns response.text (never silently returns empty string on error).
    """
    last_exc = None
    for attempt in range(1, getattr(cfg, "max_retries", 3) + 1):
        try:
            timeout = getattr(cfg, "timeout_sec", 20)
            r = session.get(url, headers=DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
            status = getattr(r, "status_code", None)
            text = getattr(r, "text", "") or ""

            # Debug visibility (critical for diagnosing Finviz blocks / CAPTCHA)
            if status != 200:
                raise RuntimeError(f"HTTP {status} for {url} (len={len(text)})")

            if len(text) < 200:
                # Finviz pages are never that small unless blocked/empty
                raise RuntimeError(f"Suspiciously small HTML for {url} (len={len(text)})")

            return text

        except Exception as e:
            last_exc = e
            # simple backoff
            sleep_s = getattr(cfg, "sleep_sec", 1.0) * attempt
            try:
                import time as _time
                _time.sleep(sleep_s)
            except Exception:
                pass

    raise RuntimeError(f"request_with_retries failed after retries: {last_exc}")

    assert last_exc is not None
    raise last_exc
