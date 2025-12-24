from __future__ import annotations

from typing import Dict, Any
import math


ZONE_POINTS = {
    "AGGRESSIVE": 85,
    "ADD": 75,
    "STARTER": 65,
    "WATCH": 50,
    "AVOID": 30,
}


def valuation_score(zone_label: str | None, upside_pct: Any | None) -> float:
    base = ZONE_POINTS.get(str(zone_label).upper(), 50) if zone_label else 50
    try:
        up = float(upside_pct)
    except Exception:
        up = 0.0
    up_adj = max(-15.0, min(15.0, (max(-50.0, min(80.0, up)) / 80.0) * 15.0))
    return max(0.0, min(100.0, base + up_adj))


def blend_scores(packet: Dict[str, Any], judge: Dict[str, Any], debate_quality: float) -> Dict[str, float]:
    quant = None
    if packet.get("score_learned") is not None:
        try:
            quant = float(packet.get("score_learned")) * 100
        except Exception:
            quant = None
    if quant is None and packet.get("score_master") is not None:
        try:
            quant = float(packet.get("score_master")) * 100
        except Exception:
            quant = None

    val = valuation_score(packet.get("zone_label"), packet.get("upside_pct"))
    llm_score = float(judge.get("conviction", 0) or 0)
    dq = float(judge.get("debate_quality", debate_quality) or 0)

    weights = []
    comps = []
    if quant is not None:
        weights.append(0.40)
        comps.append(("QuantScore", quant))
    weights.append(0.25)
    comps.append(("ValuationScore", val))
    weights.append(0.25)
    comps.append(("LLMScore", llm_score))
    weights.append(0.10)
    comps.append(("DebateQualityScore", dq))
    total_w = sum(weights)
    final = sum(w / total_w * comp for (_, comp), w in zip(comps, weights))

    out = {k: v for k, v in comps}
    out["FinalScore"] = final
    return out
