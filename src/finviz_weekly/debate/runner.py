from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .util import ensure_dir, write_json, today_iso
from .packet import build_packet
from .search.mock import MockSearchProvider
from .search.brave import BraveSearchProvider
from .search.fetch import fetch_and_extract
from .llm.mock_client import MockLLMClient
from .llm.openai_client import OpenAIClient
from .prompts import (
    RESEARCH_PLAN_PROMPT,
    EVIDENCE_SYNTH_PROMPT,
    OPENING_PROMPT,
    CROSS_QUESTIONS_PROMPT,
    CROSS_ANSWERS_PROMPT,
    REBUTTAL_PROMPT,
    JUDGE_PROMPT,
)
from .scoring import blend_scores
from .aggregate import write_results


def _load_scored(out_dir: str) -> pd.DataFrame:
    base = Path(out_dir) / "latest"
    parquet = base / "finviz_scored.parquet"
    if parquet.exists():
        return pd.read_parquet(parquet)
    csv = base / "finviz_scored.csv.gz"
    if csv.exists():
        return pd.read_csv(csv)
    raise FileNotFoundError(f"Missing scored snapshot in {base}")


def _load_tickers(out_dir: str, mode: str, tickers_arg: Optional[str]) -> List[str]:
    base = Path(out_dir) / "latest"
    if mode == "candidates":
        ticks = (base / "candidates.txt").read_text().splitlines()
        return [t.strip().upper() for t in ticks if t.strip()]
    if mode == "conviction2":
        df = pd.read_csv(base / "conviction_2plus.csv")
        return [str(t).strip().upper() for t in df["ticker"].tolist() if str(t).strip()]
    if mode == "tickers":
        if not tickers_arg:
            return []
        return [t.strip().upper() for t in tickers_arg.split(",") if t.strip()]
    raise ValueError(f"Unknown input mode {mode}")


def _init_search_provider(research: bool, cache_dir: Path, cache_days: int, recency_days: int, api_key: Optional[str]) :
    if research and api_key:
        return BraveSearchProvider(api_key=api_key, cache_dir=cache_dir / "brave", cache_days=cache_days)
    return MockSearchProvider()


def _init_llm(research: bool) :
    if research and os.environ.get("OPENAI_API_KEY"):
        try:
            return OpenAIClient()
        except Exception:
            return MockLLMClient()
    return MockLLMClient()


def _synthesize_evidence(llm, notes: List[Dict], max_cards: int) -> List[Dict]:
    prompt = EVIDENCE_SYNTH_PROMPT + "\n" + json.dumps(notes)[:3000]
    data = llm.generate_json(prompt, schema_hint="evidence")
    evs = data.get("evidence", []) if isinstance(data, dict) else []
    cards = []
    for i, ev in enumerate(evs[:max_cards]):
        cid = f"ev_{i+1:03d}"
        ev["id"] = ev.get("id", cid)
        cards.append(ev)
    return cards


def _run_rounds(llm, packet: Dict, evidence: List[Dict]) -> Dict[str, Dict]:
    agents = ["macro_specialist", "industry_specialist", "quality_accounting", "valuation_analyst", "bear_analyst"]
    openings = {}
    for ag in agents:
        openings[ag] = llm.generate_json(OPENING_PROMPT.format(agent=ag), schema_hint="opening")

    cross_q = llm.generate_json(CROSS_QUESTIONS_PROMPT, schema_hint="cross_questions").get("questions", [])
    cross_a = {}
    for ag in agents:
        cross_a[ag] = llm.generate_json(CROSS_ANSWERS_PROMPT.format(agent=ag), schema_hint="cross_answers")

    rebuttals = {}
    for ag in agents:
        rebuttals[ag] = llm.generate_json(REBUTTAL_PROMPT.format(agent=ag), schema_hint="rebuttal")

    judge = llm.generate_json(JUDGE_PROMPT, schema_hint="judge")
    # ensure required keys
    judge.setdefault("decision", "WATCH")
    judge.setdefault("conviction", 60)
    judge.setdefault("bear_fv", packet.get("price"))
    judge.setdefault("base_fv", packet.get("price"))
    judge.setdefault("bull_fv", packet.get("price"))
    judge.setdefault("p_bear", 0.2)
    judge.setdefault("p_base", 0.5)
    judge.setdefault("p_bull", 0.3)
    return {
        "openings": openings,
        "cross_questions": cross_q,
        "cross_answers": cross_a,
        "rebuttals": rebuttals,
        "judge": judge,
    }


def run_debate(
    *,
    out_dir: str = "data",
    input_mode: str = "candidates",
    tickers: Optional[str] = None,
    max_tickers: int = 30,
    research: bool = False,
    recency_days: int = 30,
    max_queries_per_ticker: int = 20,
    max_results_per_query: int = 3,
    evidence_max: int = 25,
    cache_days: int = 14,
) -> None:
    scored = _load_scored(out_dir)
    tickers_list = _load_tickers(out_dir, input_mode, tickers)[:max_tickers]
    if not tickers_list:
        return

    as_of = today_iso()
    run_dir = Path(out_dir) / "debate" / as_of
    ensure_dir(run_dir)

    search_provider = _init_search_provider(research, Path(out_dir) / "cache", cache_days, recency_days, os.environ.get("BRAVE_API_KEY"))
    llm = _init_llm(research)

    results = []
    for t in tickers_list:
        packet = build_packet(scored, t)

        # research plan
        plan = llm.generate_json(RESEARCH_PLAN_PROMPT, schema_hint="queries")
        queries = plan.get("queries", []) if isinstance(plan, dict) else []
        queries = queries[:max_queries_per_ticker]

        notes = []
        if research and queries:
            seen_urls = set()
            for q in queries:
                res = search_provider.search(q, recency_days=recency_days, max_results=max_results_per_query)
                for r in res:
                    if r.url in seen_urls:
                        continue
                    seen_urls.add(r.url)
                    fetched = fetch_and_extract(r.url, cache_dir=Path(out_dir) / "cache" / "pages", cache_days=cache_days)
                    notes.append(
                        {
                            "title": r.title,
                            "url": r.url,
                            "snippet": r.snippet,
                            "published": r.published or "unknown",
                            "text": fetched.get("text", ""),
                        }
                    )
        else:
            # offline mode: synthesize placeholder evidence input
            notes.append({"title": f"Offline note for {t}", "url": "https://example.com", "snippet": "Mock data", "published": "unknown", "text": "Mock evidence"})

        evidence_cards = _synthesize_evidence(llm, notes, evidence_max)
        evidence_path = run_dir / f"{t}_evidence.json"
        write_json(evidence_path, {"ticker": t, "evidence": evidence_cards})

        rounds = _run_rounds(llm, packet, evidence_cards)
        judge = rounds["judge"]
        # Debate quality heuristic: number of cited evidence
        cited = 0
        for op in rounds["openings"].values():
            cited += len(op.get("evidence_ids", [])) if isinstance(op, dict) else 0
        debate_quality = min(100.0, 60.0 + 2.0 * cited)
        scores = blend_scores(packet, judge, debate_quality)

        ticker_payload = {
            "ticker": t,
            "packet": packet,
            "evidence": evidence_cards,
            "rounds": rounds,
            "judge": judge,
            "scores": scores,
        }
        write_json(run_dir / f"{t}.json", ticker_payload)
        results.append(ticker_payload)

    write_results(run_dir, results)
