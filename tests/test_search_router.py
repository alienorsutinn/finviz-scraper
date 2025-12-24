from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from finviz_weekly.debate.search.base import SearchProvider, SearchResult  # type: ignore
from finviz_weekly.debate.search.router import SearchRouter, ProviderQuota  # type: ignore


class DummyProvider(SearchProvider):
    def __init__(self, name: str, results: int):
        self.name = name
        self.results = results
        self.calls = 0

    def search(self, query: str, *, recency_days: int, max_results: int, timeout: int):
        self.calls += 1
        if self.results == 0:
            return []
        return [SearchResult(title="t", url=f"https://{self.name}.com", snippet="s")]


def test_router_quota(tmp_path: Path):
    p1 = DummyProvider("brave", 1)
    p2 = DummyProvider("google", 1)
    quotas = {
        "brave": ProviderQuota(limit=1, path=tmp_path / "brave.json"),
        "google": ProviderQuota(limit=2, path=tmp_path / "google.json"),
    }
    router = SearchRouter([p1, p2], quotas)

    res1 = router.search("q", recency_days=30, max_results=3, timeout=5)
    assert res1 and p1.calls == 1

    res2 = router.search("q2", recency_days=30, max_results=3, timeout=5)
    assert res2 and p2.calls == 1  # brave exhausted, fallback to google
