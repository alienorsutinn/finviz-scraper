from __future__ import annotations

import random
from typing import Any, Dict

from .base import LLMClient


class MockLLMClient(LLMClient):
    """Deterministic-ish mock that returns structured defaults for tests."""

    def __init__(self, seed: int = 42):
        random.seed(seed)

<<<<<<< ours
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
=======
    def generate_json(self, prompt: str, schema_hint: str | None = None, *, max_tokens: int = 512, temperature: float = 0.3) -> Dict[str, Any]:
        if schema_hint == "queries":
            return {"queries": ["company earnings outlook", "competitive landscape"], "why": ["baseline"], "must_find": ["guidance"]}  # type: ignore
        if schema_hint == "evidence":
            return {
                "evidence": [
                    {
                        "id": "ev_001",
                        "tag": "macro",
                        "claim": "Macro backdrop stable.",
                        "source": {"title": "Mock Source", "domain": "example.com", "url": "https://example.com", "published": "unknown"},
                        "excerpt": "Macro environment steady.",
                        "relevance": 70,
                    }
                ]
            }
        if schema_hint == "opening":
            return {
                "thesis": ["Mock thesis"],
                "risks": ["Mock risk"],
                "catalysts_near": ["Earnings"],
                "catalysts_long": ["Share gains"],
                "confidence": 60,
                "citations": {"packet": ["price"], "evidence": ["ev_001"]},
            }
        if schema_hint == "cross_questions":
            return {
                "questions": [
                    {"from": "bear_analyst", "to": "industry_specialist", "question": "What is competition risk?"},
                    {"from": "macro_specialist", "to": "valuation_analyst", "question": "Rate sensitivity?"},
                    {"from": "quality_accounting", "to": "industry_specialist", "question": "Any accounting flags?"},
                    {"from": "valuation_analyst", "to": "bear_analyst", "question": "Valuation downside?"},
                    {"from": "judge", "to": "macro_specialist", "question": "Key catalyst?"},
                ]
            }
        if schema_hint == "cross_answers":
            return {
                "answers": [
                    {"from": "industry_specialist", "to": "bear_analyst", "answer": "Competition manageable", "citations": {"packet": [], "evidence": ["ev_001"]}},
                ]
            }
        if schema_hint == "rebuttal":
            return {
                "what_changed": ["Addressed competition"],
                "updated_confidence": 62,
                "updated_view": ["Still constructive"],
                "citations": {"packet": [], "evidence": ["ev_001"]},
            }
        if schema_hint == "judge":
            return {
                "decision": "WATCH",
                "conviction": 55,
                "fair_values": {"bear": 8.0, "base": 10.0, "bull": 13.0},
                "probabilities": {"bear": 0.2, "base": 0.5, "bull": 0.3},
                "top_reasons": ["Quant decent"],
                "top_risks": ["Competition"],
                "catalysts_near": ["Earnings"],
                "catalysts_long": ["Share gains"],
                "what_would_change_my_mind": ["Deteriorating margins"],
                "required_followups": ["Check filings"],
                "debate_quality": 70,
                "uncited_claims_found": [],
                "citations_used": {"packet": ["price"], "evidence": ["ev_001"]},
            }
        return {}
>>>>>>> theirs
