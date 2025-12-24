from __future__ import annotations

import json
from html.parser import HTMLParser
from pathlib import Path
from typing import Optional
from urllib.request import Request, urlopen

from ..util import ensure_dir, load_json_if_fresh, sha_text, write_json, clean_excerpt
import logging

LOGGER = logging.getLogger(__name__)


class _HTMLTextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.chunks = []

    def handle_data(self, data):
        if data:
            self.chunks.append(data.strip())

    def get_text(self):
        return " ".join([c for c in self.chunks if c])


def extract_text(html: str) -> str:
    parser = _HTMLTextExtractor()
    parser.feed(html)
    return parser.get_text()


def fetch_and_extract(url: str, *, cache_dir: Path, cache_days: int, user_agent: str = "finviz-weekly/1.0") -> dict:
    ensure_dir(cache_dir)
    uhash = sha_text(url)
    cache_path = cache_dir / f"{uhash}.json"
    cached = load_json_if_fresh(cache_path, max_age_days=cache_days)
    if cached:
        return cached

    text = ""
    try:
        req = Request(url, headers={"User-Agent": user_agent})
        with urlopen(req, timeout=10) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
            text = extract_text(html)
            text = clean_excerpt(text, limit=12000)
    except Exception as exc:
        LOGGER.warning("Failed to fetch %s: %s", url, exc)

    payload = {"url": url, "text": text}
    write_json(cache_path, payload)
    return payload
