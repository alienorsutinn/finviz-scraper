from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
import pandas as pd

from .util import ensure_dir


def write_results(run_dir: Path, results: List[Dict[str, Any]]) -> None:
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
                "bear_fv": judge.get("bear_fv"),
                "base_fv": judge.get("base_fv"),
                "bull_fv": judge.get("bull_fv"),
                "p_bear": judge.get("p_bear"),
                "p_base": judge.get("p_base"),
                "p_bull": judge.get("p_bull"),
                "top_reasons": "; ".join(judge.get("top_reasons", [])) if judge.get("top_reasons") else "",
                "top_risks": "; ".join(judge.get("top_risks", [])) if judge.get("top_risks") else "",
                "what_changes_mind": "; ".join(judge.get("what_change", [])) if judge.get("what_change") else "",
            }
        )
    df = pd.DataFrame(rows)
    df.to_csv(run_dir / "debate_results.csv", index=False)

    # report
    buys = df[df["decision"] == "BUY"].sort_values("final_score", ascending=False)
    watch = df[df["decision"] == "WATCH"].sort_values("final_score", ascending=False)
    avoid = df[df["decision"] == "AVOID"].sort_values("final_score", ascending=False)
    md = ["# Debate results\n"]
    for name, frame in [("Top BUY", buys), ("WATCH", watch), ("AVOID", avoid)]:
        md.append(f"\n## {name}\n")
        if frame.empty:
            md.append("_(none)_\n")
        else:
            md.append(frame.head(20).to_string(index=False))
            md.append("\n")
    # simple derived stats
    if not df.empty:
        md.append("\n## Most evidence-rich\n")
        md.append(df.sort_values("conviction", ascending=False).head(10).to_string(index=False))
    (run_dir / "debate_report.md").write_text("\n".join(md), encoding="utf-8")
