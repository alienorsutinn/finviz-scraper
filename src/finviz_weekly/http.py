from __future__ import annotations

import time
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


def request_with_retries(
    session: requests.Session,
    url: str,
    config: Optional[HttpConfig] = None,
    *,
    http_config: Optional[HttpConfig] = None,  # backwards compat for old callers
    timeout: Optional[float] = None,
) -> str:
    cfg = config or http_config or HttpConfig()
    to = float(timeout if timeout is not None else cfg.timeout_sec)

    last_exc: Exception | None = None
    for attempt in range(1, int(cfg.max_retries) + 1):
        try:
            r = session.get(url, timeout=to)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_exc = e
            sleep_s = cfg.backoff_sec * (attempt ** 1.2) + random.random() * 0.25
            LOGGER.warning("HTTP failed (attempt %s/%s) url=%s err=%s; sleeping %.2fs",
                           attempt, cfg.max_retries, url, e, sleep_s)
            time.sleep(sleep_s)

    assert last_exc is not None
    raise last_exc
