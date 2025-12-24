from __future__ import annotations

from typing import Any, Dict


class LLMClient:
<<<<<<< ours
    def generate_json(self, prompt: str, schema_hint: str | None = None) -> Dict[str, Any]:
=======
    def generate_json(self, prompt: str, schema_hint: str | None = None, *, max_tokens: int = 512, temperature: float = 0.3) -> Dict[str, Any]:
>>>>>>> theirs
        raise NotImplementedError
