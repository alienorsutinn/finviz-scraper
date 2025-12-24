from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

from .base import SearchProvider, SearchResult
from ..util import ensure_dir, load_json_if_fresh, sha_text, write_json, backoff_sleep

LOGGER = logging.getLogger(__name__)


class GoogleCSEProvider(SearchProvider):
    name = "google"

    def __init__(self, *, api_key: str, cx: str, cache_dir: Path, cache_days: int = 14, user_agent: str = "finviz-weekly/1.0"):
        self.api_key = api_key
        self.cx = cx
        self.cache_dir = cache_dir
        self.cache_days = cache_days
        self.user_agent = user_agent
        ensure_dir(self.cache_dir)

    def _request(self, url: str, timeout: int) -> Optional[dict]:
        req = Request(url, headers={"User-Agent": self.user_agent})
        for attempt in range(3):
            try:
                with urlopen(req, timeout=timeout) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except Exception as exc:
                LOGGER.warning("Google CSE request failed: %s", exc)
                backoff_sleep(attempt)
        return None

    def search(self, query: str, *, recency_days: int, max_results: int, timeout: int) -> List[SearchResult]:
        qhash = sha_text(query)
        cache_path = self.cache_dir / f"{qhash}.json"
        cached = load_json_if_fresh(cache_path, max_age_days=self.cache_days)
        if cached:
            data = cached
        else:
            url = f"https://www.googleapis.com/customsearch/v1?key={self.api_key}&cx={self.cx}&q={quote_plus(query)}&num={max_results}"
            data = self._request(url, timeout=timeout) or {}
            write_json(cache_path, data)

        items = (data or {}).get("items") or []
        results: List[SearchResult] = []
        for item in items:
            link = item.get("link") or ""
            title = item.get("title") or ""
            snippet = item.get("snippet") or ""
            published = item.get("pagemap", {}).get("metatags", [{}])[0].get("article:published_time") or "unknown"
            results.append(SearchResult(title=title, url=link, snippet=snippet, published=published))
        return results[:max_results]
