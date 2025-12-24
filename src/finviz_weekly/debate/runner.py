from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

<<<<<<< ours
from .util import ensure_dir, write_json, today_iso
from .packet import build_packet
from .search.mock import MockSearchProvider
from .search.brave import BraveSearchProvider
=======
from .util import ensure_dir, write_json, today_iso, clean_excerpt
from .packet import build_packet
from .search import (
    MockSearchProvider,
    BraveSearchProvider,
    GoogleCSEProvider,
    SearchRouter,
)
from .search.router import ProviderQuota
>>>>>>> theirs
from .search.fetch import fetch_and_extract
from .llm.mock_client import MockLLMClient
from .llm.openai_client import OpenAIClient
from .prompts import (
<<<<<<< ours
    RESEARCH_PLAN_PROMPT,
    EVIDENCE_SYNTH_PROMPT,
    OPENING_PROMPT,
    CROSS_QUESTIONS_PROMPT,
    CROSS_ANSWERS_PROMPT,
    REBUTTAL_PROMPT,
    JUDGE_PROMPT,
=======
    SYSTEM_JSON_ONLY,
    research_plan_prompt,
    evidence_prompt,
    openings_prompt,
    CROSS_QUESTIONS_PROMPT,
    cross_answers_prompt,
    rebuttal_prompt,
    judge_prompt,
>>>>>>> theirs
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


<<<<<<< ours
def _init_search_provider(research: bool, cache_dir: Path, cache_days: int, recency_days: int, api_key: Optional[str]) :
    if research and api_key:
        return BraveSearchProvider(api_key=api_key, cache_dir=cache_dir / "brave", cache_days=cache_days)
    return MockSearchProvider()


def _init_llm(research: bool) :
    if research and os.environ.get("OPENAI_API_KEY"):
        try:
            return OpenAIClient()
=======
def _init_router(research: bool, cache_dir: Path, cache_days: int, brave_key: Optional[str], google_key: Optional[str], google_cx: Optional[str]) -> SearchRouter:
    providers = []
    quotas = {}
    quota_dir = cache_dir / "quota"
    month_key = Path(quota_dir / f"brave_{today_iso()[:7]}.json")
    day_key = Path(quota_dir / f"google_{today_iso()}.json")
    if research and brave_key:
        providers.append(BraveSearchProvider(api_key=brave_key, cache_dir=cache_dir / "search" / "brave", cache_days=cache_days))
        quotas["brave"] = ProviderQuota(limit=2000, path=month_key)
    if research and google_key and google_cx:
        providers.append(GoogleCSEProvider(api_key=google_key, cx=google_cx, cache_dir=cache_dir / "search" / "google", cache_days=cache_days))
        quotas["google"] = ProviderQuota(limit=100, path=day_key)
    if not providers:
        providers.append(MockSearchProvider())
    return SearchRouter(providers, quotas)


def _init_llm(provider: str, model: Optional[str]) :
    if provider == "mock":
        return MockLLMClient()
    if os.environ.get("OPENAI_API_KEY"):
        try:
            return OpenAIClient(model=model)
>>>>>>> theirs
        except Exception:
            return MockLLMClient()
    return MockLLMClient()


<<<<<<< ours
def _synthesize_evidence(llm, notes: List[Dict], max_cards: int) -> List[Dict]:
    prompt = EVIDENCE_SYNTH_PROMPT + "\n" + json.dumps(notes)[:3000]
    data = llm.generate_json(prompt, schema_hint="evidence")
=======
def _synthesize_evidence(llm, notes: List[Dict], max_cards: int, ticker: str) -> List[Dict]:
    prompt = evidence_prompt(json.dumps(notes), ticker)
    data = llm.generate_json(prompt, schema_hint="evidence", max_tokens=600)
>>>>>>> theirs
    evs = data.get("evidence", []) if isinstance(data, dict) else []
    cards = []
    for i, ev in enumerate(evs[:max_cards]):
        cid = f"ev_{i+1:03d}"
        ev["id"] = ev.get("id", cid)
<<<<<<< ours
=======
        # enforce required keys
        ev["excerpt"] = " ".join(str(ev.get("excerpt", "")) .split()[:25])
        ev["tag"] = ev.get("tag") or "other"
        if "source" not in ev:
            ev["source"] = {"title": "", "domain": "", "url": "", "published": "unknown"}
>>>>>>> theirs
        cards.append(ev)
    return cards


def _run_rounds(llm, packet: Dict, evidence: List[Dict]) -> Dict[str, Dict]:
    agents = ["macro_specialist", "industry_specialist", "quality_accounting", "valuation_analyst", "bear_analyst"]
<<<<<<< ours
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
=======
    evidence_ids = [ev.get("id") for ev in evidence if ev.get("id")]
    packet_json = json.dumps(packet)

    openings = {}
    for ag in agents:
        openings[ag] = llm.generate_json(openings_prompt(ag, packet_json, evidence_ids), schema_hint="opening", max_tokens=700)

    cross_q = llm.generate_json(CROSS_QUESTIONS_PROMPT, schema_hint="cross_questions", max_tokens=400).get("questions", [])
    cross_a = {}
    for ag in agents:
        cross_a[ag] = llm.generate_json(cross_answers_prompt(ag), schema_hint="cross_answers", max_tokens=400)

    rebuttals = {}
    for ag in agents:
        rebuttals[ag] = llm.generate_json(rebuttal_prompt(ag), schema_hint="rebuttal", max_tokens=400)

    judge = llm.generate_json(judge_prompt(packet_json, evidence_ids), schema_hint="judge", max_tokens=700)
    judge.setdefault("decision", "WATCH")
    judge.setdefault("conviction", 50)
    judge.setdefault("fair_values", {"bear": packet.get("price"), "base": packet.get("price"), "bull": packet.get("price")})
    judge.setdefault("probabilities", {"bear": 0.2, "base": 0.5, "bull": 0.3})
    judge.setdefault("debate_quality", judge.get("debate_quality", 60))
    judge.setdefault("citations_used", {"packet": [], "evidence": []})
>>>>>>> theirs
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
<<<<<<< ours
    research: bool = False,
    recency_days: int = 30,
    max_queries_per_ticker: int = 20,
    max_results_per_query: int = 3,
    evidence_max: int = 25,
    cache_days: int = 14,
=======
    research: bool = True,
    recency_days: int = 30,
    max_queries_per_ticker: int = 12,
    max_results_per_query: int = 3,
    evidence_max: int = 20,
    cache_days: int = 14,
    timeout_seconds: int = 15,
    as_of: Optional[str] = None,
    provider: str = "openai",
    model: Optional[str] = None,
>>>>>>> theirs
) -> None:
    scored = _load_scored(out_dir)
    tickers_list = _load_tickers(out_dir, input_mode, tickers)[:max_tickers]
    if not tickers_list:
        return

<<<<<<< ours
    as_of = today_iso()
    run_dir = Path(out_dir) / "debate" / as_of
    ensure_dir(run_dir)

    search_provider = _init_search_provider(research, Path(out_dir) / "cache", cache_days, recency_days, os.environ.get("BRAVE_API_KEY"))
    llm = _init_llm(research)
=======
    if as_of:
        as_of_final = as_of
    elif "as_of_date" in scored.columns:
        try:
            as_of_final = str(scored["as_of_date"].iloc[0])
        except Exception:
            as_of_final = today_iso()
    else:
        as_of_final = today_iso()
    run_dir = Path(out_dir) / "debate" / as_of_final
    ensure_dir(run_dir)

    cache_base = Path(out_dir) / "cache"
    router = _init_router(research, cache_base, cache_days, os.environ.get("BRAVE_API_KEY"), os.environ.get("GOOGLE_CSE_API_KEY"), os.environ.get("GOOGLE_CSE_CX"))
    llm = _init_llm(provider, model)
>>>>>>> theirs

    results = []
    for t in tickers_list:
        packet = build_packet(scored, t)

        # research plan
<<<<<<< ours
        plan = llm.generate_json(RESEARCH_PLAN_PROMPT, schema_hint="queries")
        queries = plan.get("queries", []) if isinstance(plan, dict) else []
=======
        queries = []
        for ag in ["macro_specialist", "industry_specialist", "quality_accounting", "valuation_analyst", "bear_analyst"]:
            plan = llm.generate_json(research_plan_prompt(ag, t, packet.get("company")), schema_hint="queries", max_tokens=200)
            queries.extend(plan.get("queries", []) if isinstance(plan, dict) else [])
>>>>>>> theirs
        queries = queries[:max_queries_per_ticker]

        notes = []
        if research and queries:
            seen_urls = set()
            for q in queries:
<<<<<<< ours
                res = search_provider.search(q, recency_days=recency_days, max_results=max_results_per_query)
=======
                res = router.search(q, recency_days=recency_days, max_results=max_results_per_query, timeout=timeout_seconds)
>>>>>>> theirs
                for r in res:
                    if r.url in seen_urls:
                        continue
                    seen_urls.add(r.url)
<<<<<<< ours
                    fetched = fetch_and_extract(r.url, cache_dir=Path(out_dir) / "cache" / "pages", cache_days=cache_days)
=======
                    fetched = fetch_and_extract(r.url, cache_dir=cache_base / "pages", cache_days=cache_days)
>>>>>>> theirs
                    notes.append(
                        {
                            "title": r.title,
                            "url": r.url,
                            "snippet": r.snippet,
                            "published": r.published or "unknown",
<<<<<<< ours
                            "text": fetched.get("text", ""),
                        }
                    )
        else:
            # offline mode: synthesize placeholder evidence input
            notes.append({"title": f"Offline note for {t}", "url": "https://example.com", "snippet": "Mock data", "published": "unknown", "text": "Mock evidence"})

        evidence_cards = _synthesize_evidence(llm, notes, evidence_max)
=======
                            "text": clean_excerpt(fetched.get("text", ""), limit=1200),
                        }
                    )
        else:
            notes.append({"title": f"Offline note for {t}", "url": "https://example.com", "snippet": "Mock data", "published": "unknown", "text": "Mock evidence"})

        evidence_cards = _synthesize_evidence(llm, notes, evidence_max, t)
>>>>>>> theirs
        evidence_path = run_dir / f"{t}_evidence.json"
        write_json(evidence_path, {"ticker": t, "evidence": evidence_cards})

        rounds = _run_rounds(llm, packet, evidence_cards)
        judge = rounds["judge"]
<<<<<<< ours
        # Debate quality heuristic: number of cited evidence
        cited = 0
        for op in rounds["openings"].values():
            cited += len(op.get("evidence_ids", [])) if isinstance(op, dict) else 0
        debate_quality = min(100.0, 60.0 + 2.0 * cited)
        scores = blend_scores(packet, judge, debate_quality)
=======
        scores = blend_scores(packet, judge, judge.get("debate_quality", 0))
        confs = []
        for op in rounds.get("openings", {}).values():
            try:
                confs.append(float(op.get("confidence", 0)))
            except Exception:
                pass
        conf_var = 0.0
        if confs:
            mean_c = sum(confs) / len(confs)
            conf_var = sum((c - mean_c) ** 2 for c in confs) / len(confs)
>>>>>>> theirs

        ticker_payload = {
            "ticker": t,
            "packet": packet,
            "evidence": evidence_cards,
            "rounds": rounds,
            "judge": judge,
            "scores": scores,
<<<<<<< ours
=======
            "confidence_variance": conf_var,
>>>>>>> theirs
        }
        write_json(run_dir / f"{t}.json", ticker_payload)
        results.append(ticker_payload)

<<<<<<< ours
    write_results(run_dir, results)
=======
    write_results(run_dir, results, router.usage_summary())
>>>>>>> theirs
