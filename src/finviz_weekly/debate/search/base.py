from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class SearchResult:
    title: str
    url: str
    snippet: str
    published: str | None = None


class SearchProvider:
    def search(self, query: str, *, recency_days: int, max_results: int) -> List[SearchResult]:
        raise NotImplementedError
