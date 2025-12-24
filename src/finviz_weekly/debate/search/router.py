from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from .base import SearchProvider, SearchResult
from ..util import ensure_dir

LOGGER = logging.getLogger(__name__)


@dataclass
class ProviderQuota:
    limit: int
    path: Path

    def load(self) -> int:
        if not self.path.exists():
            return 0
        try:
            return int(json.loads(self.path.read_text()).get("used", 0))
        except Exception:
            return 0

    def save(self, used: int) -> None:
        ensure_dir(self.path.parent)
        self.path.write_text(json.dumps({"used": used}, indent=2), encoding="utf-8")


class SearchRouter:
    def __init__(self, providers: List[SearchProvider], quotas: dict[str, ProviderQuota]):
        self.providers = providers
        self.quotas = quotas
        self.usage_run = {p.name: 0 for p in providers}

    def _consume(self, provider: SearchProvider) -> bool:
        quota = self.quotas.get(provider.name)
        if not quota:
            return True
        used = quota.load()
        if used >= quota.limit:
            return False
        quota.save(used + 1)
        self.usage_run[provider.name] = self.usage_run.get(provider.name, 0) + 1
        return True

    def search(self, query: str, *, recency_days: int, max_results: int, timeout: int) -> List[SearchResult]:
        for provider in self.providers:
            if not self._consume(provider):
                continue
            try:
                res = provider.search(query, recency_days=recency_days, max_results=max_results, timeout=timeout)
                if res:
                    return res
            except Exception as exc:
                LOGGER.warning("Provider %s failed: %s", provider.name, exc)
                continue
        return []

    def usage_summary(self) -> dict:
        return self.usage_run
