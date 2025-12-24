from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
import pandas as pd

from .util import ensure_dir


def write_results(run_dir: Path, results: List[Dict[str, Any]], search_usage: dict[str, int]) -> None:
    ensure_dir(run_dir)
    rows = []
    for r in results:
        judge = r.get("judge", {})
        scores = r.get("scores", {})
        rows.append(
            {
                "ticker": r.get("ticker"),
                "company": r.get("packet", {}).get("company"),
                "sector": r.get("packet", {}).get("sector"),
                "industry": r.get("packet", {}).get("industry"),
                "price": r.get("packet", {}).get("price"),
                "wfv": r.get("packet", {}).get("wfv"),
                "zone_label": r.get("packet", {}).get("zone_label"),
                "upside_pct": r.get("packet", {}).get("upside_pct"),
                "score_master": r.get("packet", {}).get("score_master"),
                "score_learned": r.get("packet", {}).get("score_learned"),
                "decision": judge.get("decision"),
                "conviction": judge.get("conviction"),
                "final_score": scores.get("FinalScore"),
                "bear_fv": (judge.get("fair_values") or {}).get("bear"),
                "base_fv": (judge.get("fair_values") or {}).get("base"),
                "bull_fv": (judge.get("fair_values") or {}).get("bull"),
                "p_bear": (judge.get("probabilities") or {}).get("bear"),
                "p_base": (judge.get("probabilities") or {}).get("base"),
                "p_bull": (judge.get("probabilities") or {}).get("bull"),
                "top_reasons": "; ".join(judge.get("top_reasons", [])) if judge.get("top_reasons") else "",
                "top_risks": "; ".join(judge.get("top_risks", [])) if judge.get("top_risks") else "",
                "what_changes_mind": "; ".join(judge.get("what_would_change_my_mind", [])) if judge.get("what_would_change_my_mind") else "",
                "quant_score": scores.get("QuantScore"),
                "valuation_score": scores.get("ValuationScore"),
                "llm_score": scores.get("LLMScore"),
                "debate_quality_score": scores.get("DebateQualityScore"),
                "confidence_variance": r.get("confidence_variance", 0.0),
                "evidence_count": len(r.get("evidence", [])),
            }
        )
    df = pd.DataFrame(rows)
    df.to_csv(run_dir / "debate_results.csv", index=False)

    # report
    buys = df[df["decision"] == "BUY"].sort_values("final_score", ascending=False)
    watch = df[df["decision"] == "WATCH"].sort_values("final_score", ascending=False)
    avoid = df[df["decision"] == "AVOID"].sort_values("final_score", ascending=False)
    md = ["# Debate results\n"]
    md.append(f"- brave_queries_used_this_run: {search_usage.get('brave', 0)}")
    md.append(f"- google_queries_used_this_run: {search_usage.get('google', 0)}\n")
    md.append(f"- BUY: {len(buys)} | WATCH: {len(watch)} | AVOID: {len(avoid)}\n")
    for name, frame in [("Top BUY", buys), ("WATCH", watch), ("AVOID", avoid)]:
        md.append(f"\n## {name}\n")
        if frame.empty:
            md.append("_(none)_\n")
        else:
            md.append(frame[["ticker", "company", "final_score", "decision", "conviction"]].head(20).to_string(index=False))
            md.append("\n")
    if not df.empty:
        md.append("\n## Biggest disagreements (confidence variance)\n")
        md.append(df.sort_values("confidence_variance", ascending=False).head(10).to_string(index=False))
        md.append("\n## Evidence-rich\n")
        md.append(df.sort_values("evidence_count", ascending=False).head(10).to_string(index=False))
    (run_dir / "debate_report.md").write_text("\n".join(md), encoding="utf-8")
