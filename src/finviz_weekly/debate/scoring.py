from __future__ import annotations

from typing import Dict, Any
import math


ZONE_POINTS = {
<<<<<<< ours
    "AGGRESSIVE": 90,
    "ADD": 80,
    "STARTER": 70,
    "WATCH": 55,
    "AVOID": 35,
=======
    "AGGRESSIVE": 85,
    "ADD": 75,
    "STARTER": 65,
    "WATCH": 50,
    "AVOID": 30,
>>>>>>> theirs
}


def valuation_score(zone_label: str | None, upside_pct: Any | None) -> float:
    base = ZONE_POINTS.get(str(zone_label).upper(), 50) if zone_label else 50
    try:
        up = float(upside_pct)
    except Exception:
        up = 0.0
<<<<<<< ours
    up_adj = max(-20.0, min(20.0, up / 5.0))
=======
    up_adj = max(-15.0, min(15.0, (max(-50.0, min(80.0, up)) / 80.0) * 15.0))
>>>>>>> theirs
    return max(0.0, min(100.0, base + up_adj))


def blend_scores(packet: Dict[str, Any], judge: Dict[str, Any], debate_quality: float) -> Dict[str, float]:
    quant = None
<<<<<<< ours
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
=======
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
>>>>>>> theirs
