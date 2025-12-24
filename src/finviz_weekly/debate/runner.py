from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .util import ensure_dir, write_json, today_iso
from .packet import build_packet
from .search import MockSearchProvider, BraveSearchProvider, GoogleCSEProvider, SearchRouter
from .search.router import ProviderQuota
from .llm.mock_client import MockLLMClient
from .llm.openai_client import OpenAIClient
from .prompts import (
    SYSTEM_JSON_ONLY,
    research_plan_prompt,
    evidence_prompt,
    openings_prompt,
    CROSS_QUESTIONS_PROMPT,
    cross_answers_prompt,
    rebuttal_prompt,
    judge_prompt,
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


def _init_router(research: bool, cache_dir: Path, cache_days: int, brave_key: Optional[str], google_key: Optional[str], google_cx: Optional[str]) -> SearchRouter:
    providers = []
    quotas: dict[str, ProviderQuota] = {}
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


def _init_llm(provider: str, model: Optional[str]) -> MockLLMClient | OpenAIClient:
    if provider == "mock":
        return MockLLMClient()
    if os.environ.get("OPENAI_API_KEY"):
        try:
            return OpenAIClient(model=model)
        except Exception:
            return MockLLMClient()
    return MockLLMClient()


def run_debate(
    *,
    out_dir: str = "data",
    input_mode: str = "candidates",
    tickers: Optional[str] = None,
    max_tickers: int = 30,
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
    verbose: bool = False,
) -> None:
    scored = _load_scored(out_dir)
    tickers_list = _load_tickers(out_dir, input_mode, tickers)[:max_tickers]
    if not tickers_list:
        return

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

    results: List[Dict] = []
    for ticker in tickers_list:
        packet = build_packet(scored, ticker)
        evidence_cards = []
        plan = llm.generate_json(
            SYSTEM_JSON_ONLY + research_plan_prompt("macro_specialist", ticker, packet.get("company")),
            schema_hint="queries",
            max_tokens=200,
        )
        queries = plan.get("queries", []) if research else []
        for q in queries[:max_queries_per_ticker]:
            search_results = router.search(q, recency_days=recency_days, max_results=max_results_per_query, timeout=timeout_seconds)
            for res in search_results:
                evidence_cards.append(
                    {
                        "id": f"ev_{len(evidence_cards)+1:03d}",
                        "tag": "other",
                        "claim": res.title,
                        "source": {"title": res.title, "domain": res.url, "url": res.url, "published": res.published or "unknown"},
                        "excerpt": res.snippet or "",
                        "relevance": 50,
                    }
                )
                if len(evidence_cards) >= evidence_max:
                    break
            if len(evidence_cards) >= evidence_max:
                break

        if not evidence_cards:
            evidence_cards = llm.generate_json(SYSTEM_JSON_ONLY + evidence_prompt(json.dumps({}), ticker), schema_hint="evidence", max_tokens=400).get("evidence", [])

        openings = llm.generate_json(
            SYSTEM_JSON_ONLY + openings_prompt("valuation_analyst", json.dumps(packet), [e["id"] for e in evidence_cards]),
            schema_hint="opening",
            max_tokens=400,
        )
        cross_q = llm.generate_json(CROSS_QUESTIONS_PROMPT, schema_hint="cross_questions", max_tokens=400).get("questions", [])
        cross_a = llm.generate_json(cross_answers_prompt("valuation_analyst"), schema_hint="cross_answers", max_tokens=400).get("answers", [])
        rebuttal = llm.generate_json(rebuttal_prompt("valuation_analyst"), schema_hint="rebuttal", max_tokens=400)
        judge = llm.generate_json(judge_prompt(json.dumps(packet), [e["id"] for e in evidence_cards]), schema_hint="judge", max_tokens=700)
        judge.setdefault("debate_quality", judge.get("debate_quality", 60))

        scores = blend_scores(packet, judge, debate_quality=judge.get("debate_quality", 0))
        results.append(
            {
                "ticker": ticker,
                "packet": packet,
                "evidence": evidence_cards,
                "opening": openings,
                "cross_questions": cross_q,
                "cross_answers": cross_a,
                "rebuttal": rebuttal,
                "judge": judge,
                "scores": scores,
                "confidence_variance": 0.0,
            }
        )

        write_json(run_dir / f"{ticker}.json", results[-1])
        write_json(run_dir / f"{ticker}_evidence.json", evidence_cards)

    search_usage = router.usage_summary() if hasattr(router, "usage_summary") else {}
    write_results(run_dir, results, search_usage)
