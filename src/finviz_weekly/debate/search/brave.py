from __future__ import annotations

import json
import time
from typing import List, Optional
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

from .base import SearchProvider, SearchResult
from ..util import load_json_if_fresh, sha_text, write_json, ensure_dir
from pathlib import Path
import logging

LOGGER = logging.getLogger(__name__)


class BraveSearchProvider(SearchProvider):
    def __init__(self, *, api_key: str, cache_dir: Path, cache_days: int = 14, user_agent: str = "finviz-weekly/1.0"):
        self.api_key = api_key
        self.cache_dir = cache_dir
        self.cache_days = cache_days
        self.user_agent = user_agent
        ensure_dir(self.cache_dir)

    def _request(self, url: str) -> Optional[dict]:
        req = Request(url, headers={"X-Subscription-Token": self.api_key, "User-Agent": self.user_agent})
        backoff = 1.0
        for _ in range(3):
            try:
                with urlopen(req, timeout=10) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except Exception as exc:
                LOGGER.warning("Brave search request failed: %s", exc)
                time.sleep(backoff)
                backoff *= 2
        return None

    def search(self, query: str, *, recency_days: int, max_results: int) -> List[SearchResult]:
        qhash = sha_text(query)
        cache_path = self.cache_dir / f"{qhash}.json"
        cached = load_json_if_fresh(cache_path, max_age_days=self.cache_days)
        if cached:
            data = cached
        else:
            url = f"https://api.search.brave.com/res/v1/web/search?q={quote_plus(query)}&count={max_results}&freshness={recency_days}d"
            data = self._request(url) or {}
            write_json(cache_path, data)

        web = (data or {}).get("web") or {}
        results = []
        for item in web.get("results") or []:
            results.append(
                SearchResult(
                    title=item.get("title") or "",
                    url=item.get("url") or "",
                    snippet=item.get("description") or "",
                    published=item.get("date") or "unknown",
                )
            )
        return results[:max_results]
