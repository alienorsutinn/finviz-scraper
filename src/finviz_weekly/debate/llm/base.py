from __future__ import annotations

from typing import Any, Dict


class LLMClient:
    def generate_json(self, prompt: str, schema_hint: str | None = None) -> Dict[str, Any]:
        raise NotImplementedError
