from __future__ import annotations

import time
from typing import Any, Optional

import requests

from .config import HttpConfig


_DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}


def create_session(cfg: HttpConfig) -> requests.Session:
    s = requests.Session()
    headers = dict(_DEFAULT_HEADERS)
    headers["User-Agent"] = cfg.user_agent
    s.headers.update(headers)

    # Only set proxies if explicitly provided
    if cfg.proxy:
        s.proxies.update({"http": cfg.proxy, "https": cfg.proxy})

    return s


def request_with_retries(
    session: requests.Session,
    url: str,
    cfg: HttpConfig,
    *,
    params: Optional[dict[str, Any]] = None,
) -> str:
    last_exc: Optional[Exception] = None

    for attempt in range(1, cfg.max_retries + 1):
        try:
            r = session.get(url, params=params, timeout=cfg.timeout_sec, allow_redirects=True)
            status = r.status_code
            if status != 200:
                raise RuntimeError(f"HTTP {status} for {url} (final_url={r.url})")

            text = r.text or ""
            if len(text) < 200:
                # include useful headers for debugging blocks/proxies
                ct = r.headers.get("content-type")
                server = r.headers.get("server")
                cl = r.headers.get("content-length")
                raise RuntimeError(
                    f"Suspiciously small HTML for {url} (len={len(text)}, final_url={r.url}, "
                    f"content-type={ct}, content-length={cl}, server={server})"
                )

            return text

        except Exception as e:
            last_exc = e
            if attempt < cfg.max_retries:
                time.sleep(cfg.backoff_sec * attempt)
            continue

    raise RuntimeError(f"request_with_retries failed after retries: {last_exc}")
