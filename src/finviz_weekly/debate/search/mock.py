from __future__ import annotations

from typing import List

from .base import SearchProvider, SearchResult


class MockSearchProvider(SearchProvider):
    """Deterministic provider for tests; returns simple canned results."""

    def search(self, query: str, *, recency_days: int, max_results: int) -> List[SearchResult]:
        results = []
        for i in range(min(max_results, 3)):
            results.append(
                SearchResult(
                    title=f"Mock result {i+1} for {query}",
                    url=f"https://example.com/{query.replace(' ', '_')}/{i}",
                    snippet="Mock snippet highlighting key points.",
                    published="unknown",
                )
            )
        return results
