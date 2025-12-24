from __future__ import annotations

from typing import Dict, Any
import math


ZONE_POINTS = {
    "AGGRESSIVE": 90,
    "ADD": 80,
    "STARTER": 70,
    "WATCH": 55,
    "AVOID": 35,
}


def valuation_score(zone_label: str | None, upside_pct: Any | None) -> float:
    base = ZONE_POINTS.get(str(zone_label).upper(), 50) if zone_label else 50
    try:
        up = float(upside_pct)
    except Exception:
        up = 0.0
    up_adj = max(-20.0, min(20.0, up / 5.0))
    return max(0.0, min(100.0, base + up_adj))


def blend_scores(packet: Dict[str, Any], judge: Dict[str, Any], debate_quality: float) -> Dict[str, float]:
    quant = None
    if packet.get("score_learned") is not None and not math.isnan(packet.get("score_learned")):
        quant = float(packet.get("score_learned")) * 100
    elif packet.get("score_master") is not None:
        quant = float(packet.get("score_master")) * 100
    else:
        quant = 50.0
    val = valuation_score(packet.get("zone_label"), packet.get("upside_pct"))
    llm_score = float(judge.get("conviction", 0) or 0)
    dq = float(debate_quality or 0)
    final = 0.40 * quant + 0.25 * val + 0.25 * llm_score + 0.10 * dq
    return {
        "QuantScore": quant,
        "ValuationScore": val,
        "LLMScore": llm_score,
        "DebateQualityScore": dq,
        "FinalScore": final,
    }
