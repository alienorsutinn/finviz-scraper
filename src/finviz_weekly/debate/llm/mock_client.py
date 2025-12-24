from __future__ import annotations

import random
from typing import Any, Dict

from .base import LLMClient


class MockLLMClient(LLMClient):
    """Deterministic-ish mock that returns structured defaults for tests."""

    def __init__(self, seed: int = 42):
        random.seed(seed)

    def generate_json(self, prompt: str, schema_hint: str | None = None) -> Dict[str, Any]:
        # Very lightweight mapper to return predictable structures
        return {
            "thesis": ["Solid outlook", "Manageable risks"],
            "risks": ["Competition", "Macro headwinds"],
            "evidence_ids": ["ev_001"],
            "confidence": 65,
            "questions": [
                {"from": "judge", "to": "bear_analyst", "question": "What is your top risk?"},
                {"from": "macro_specialist", "to": "valuation_analyst", "question": "How does macro affect valuation?"},
            ],
            "answers": [{"from": "bear_analyst", "to": "judge", "answer": "Risks remain but manageable", "evidence_ids": ["ev_001"]}],
            "decision": "WATCH",
            "conviction": 60,
            "valuation": {"bear": 0.8, "base": 1.0, "bull": 1.2, "p_bear": 0.2, "p_base": 0.5, "p_bull": 0.3},
            "top_reasons": ["Quant strong", "Valuation reasonable"],
            "top_risks": ["Execution", "Competition"],
            "catalysts": {"near": ["Earnings"], "long": ["Market share gains"]},
            "what_change": ["Missed earnings"],
            "followups": ["Check filings"],
            "evidence": [],
        }
