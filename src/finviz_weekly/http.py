"""HTTP utilities with retries and polite headers."""
from __future__ import annotations

import logging
import random
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import HttpConfig, USER_AGENTS

LOGGER = logging.getLogger(__name__)


def _retry_strategy(config: HttpConfig) -> Retry:
    return Retry(
        total=config.max_retries,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        raise_on_status=False,
    )


def create_session(config: HttpConfig) -> requests.Session:
    """Create a configured requests session."""

    session = requests.Session()
    adapter = HTTPAdapter(max_retries=_retry_strategy(config))
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    if config.proxy:
        session.proxies.update({"http": config.proxy, "https": config.proxy})
    return session


def request_with_retries(
    session: requests.Session, url: str, config: HttpConfig, *, timeout: Optional[tuple[int, int]] = None
) -> requests.Response:
    """Perform a GET request with headers and retries."""

    headers = {"User-Agent": random.choice(USER_AGENTS)}
    retries = 0
    max_retries = config.max_retries
    timeout = timeout or (config.timeout_connect, config.timeout_read)
    while True:
        try:
            LOGGER.debug("Requesting %s (attempt %s)", url, retries + 1)
            resp = session.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:  # includes Timeout, HTTPError
            retries += 1
            if retries >= max_retries:
                LOGGER.error("Request failed after %s retries: %s", retries, exc)
                raise
            sleep_for = 2 ** (retries - 1) * 0.5
            LOGGER.warning("Request error (%s). Retrying in %.2fs", exc, sleep_for)
            time.sleep(sleep_for)
