from __future__ import annotations

import json
import os
from typing import Any, Dict

from .base import LLMClient


class OpenAIClient(LLMClient):
    """Thin wrapper; assumes openai package is installed."""

    def __init__(self, model: str | None = None):
        try:
            import openai  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("openai package not installed; install openai to use OpenAIClient") from exc

        # Validate API key exists
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(
                "❌ OPENAI_API_KEY environment variable required.\n"
                "Get one at: https://platform.openai.com/api-keys\n"
                "Then run: export OPENAI_API_KEY='sk-...'"
            )

        self._client = openai.OpenAI(api_key=api_key)
        self.model = model or os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

    def generate_json(self, prompt: str, schema_hint: str | None = None, *, max_tokens: int = 512, temperature: float = 0.3) -> Dict[str, Any]:  # pragma: no cover - network
        completion = self._client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "Respond with valid JSON only."},
                {"role": "user", "content": prompt},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        content = completion.choices[0].message.content or "{}"
        try:
            return json.loads(content)
        except Exception:
            return {}
