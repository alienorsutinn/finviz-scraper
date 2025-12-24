from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def today_iso() -> str:
    return date.today().isoformat()


def sha_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_json_if_fresh(path: Path, *, max_age_days: int) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
        if age > timedelta(days=max_age_days):
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def write_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


@dataclass
class EvidenceCard:
    id: str
    tag: str
    claim: str
    source: Dict[str, Any]
    excerpt: str
    relevance: int


def clean_excerpt(text: str, limit: int = 160) -> str:
    t = " ".join(str(text).split())
    if len(t) <= limit:
        return t
    return t[: limit - 3] + "..."
