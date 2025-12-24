from __future__ import annotations

<<<<<<< ours
RESEARCH_PLAN_PROMPT = """You are an investment analyst planning web research. Propose concise search queries for the ticker and company. Return JSON: {"queries":[...]}."""

EVIDENCE_SYNTH_PROMPT = """You are an equity research assistant. Given extracted notes, produce concise evidence cards.
Return JSON: {"evidence":[{"id":"ev_001","tag":"macro|competition|product|regulatory|balance_sheet|guidance|other","claim":"one sentence","source":{"title":...,"domain":...,"url":...,"published":...},"excerpt":"<=25 words","relevance":0-100},...]}.
Use only the supplied notes; cite URLs."""

OPENING_PROMPT = """You are {agent}. Using the evidence IDs and packet fields, give thesis, risks, evidence_ids, confidence 0-100. JSON keys: thesis, risks, evidence_ids, confidence."""

CROSS_QUESTIONS_PROMPT = """You are the chair judge. Draft up to 5 cross-exam questions targeting specified agents. JSON: {"questions":[{"from":"judge","to":"bear_analyst","question":"..."}]}."""

CROSS_ANSWERS_PROMPT = """You are {agent}. Answer assigned questions briefly citing evidence_ids or packet fields. JSON: {{"answers":[{{"from":...,"to":...,"answer":...,"evidence_ids":[...]}}]}}."""

REBUTTAL_PROMPT = """You are {agent}. Update stance after cross-exam. JSON: {{"thesis":[...], "risks":[...], "evidence_ids":[...], "confidence":int, "changes":[...]}}."""

JUDGE_PROMPT = """You are the judge. Synthesize decision with citations. JSON keys:
decision (BUY|WATCH|AVOID), conviction (0-100), bull_fv, base_fv, bear_fv, p_bull, p_base, p_bear,
final_score (0-100), top_reasons, top_risks, catalysts {"near": [...], "long": [...]}, what_change (list), followups (list)."""
=======
SYSTEM_JSON_ONLY = "You are an investment debate system. Respond with VALID JSON only. Do not include explanations."


def research_plan_prompt(agent: str, ticker: str, company: str | None) -> str:
    return (
        f"You are {agent}. Propose web search queries for {ticker} ({company or 'company'}). "
        'Return JSON: {"queries":[...],"why":[...],"must_find":[...]}'
    )


def evidence_prompt(notes_json: str, ticker: str) -> str:
    return (
        "You are an equity research assistant. Using the supplied snippets, create evidence cards. "
        "Return JSON: {\"evidence\":[{\"id\":\"ev_001\",\"tag\":\"guidance|lawsuit|partnership|macro|competition|balance_sheet|product|regulatory|other\","
        "\"claim\":\"one sentence\",\"source\":{\"title\":...,\"domain\":...,\"url\":...,\"published\":...},"
        "\"excerpt\":\"<=25 words\",\"relevance\":0-100}]}. "
        f"Ticker: {ticker}. Snippets: {notes_json[:2500]}"
    )


def openings_prompt(agent: str, packet_json: str, evidence_ids: list[str]) -> str:
    return (
        f"You are {agent}. Provide concise thesis/risks/catalysts with citations. "
        'Return JSON: {"thesis":[...],"risks":[...],"catalysts_near":[...],"catalysts_long":[...],'
        '"confidence":0-100,"citations":{"packet":[...],"evidence":[...]}}. '
        "Packet fields available: "
        f"{packet_json[:2000]}."
        f" Evidence ids: {', '.join(evidence_ids)}."
    )


CROSS_QUESTIONS_PROMPT = (
    "You are the chair judge. Draft up to 5 cross-exam questions targeting specific agents:\n"
    "Q1 bear_analyst -> industry_specialist\n"
    "Q2 macro_specialist -> valuation_analyst\n"
    "Q3 quality_accounting -> industry_specialist\n"
    "Q4 valuation_analyst -> bear_analyst\n"
    "Q5 judge -> any (tie-breaker)\n"
    'Return JSON: {"questions":[{"from":..., "to":..., "question":...},...]}.'
)


def cross_answers_prompt(agent: str) -> str:
    return (
        f"You are {agent}. Answer your assigned questions briefly with citations. "
        'Return JSON: {"answers":[{"from":...,"to":...,"answer":...,"citations":{"packet":[...],"evidence":[...]}}]}'
    )


def rebuttal_prompt(agent: str) -> str:
    return (
        f"You are {agent}. Provide rebuttal: what_changed, updated_confidence (0-100), updated_view, citations. "
        'Return JSON: {"what_changed":[...],"updated_confidence":int,"updated_view":[...],"citations":{"packet":[...],"evidence":[...]}}'
    )


def judge_prompt(packet_json: str, evidence_ids: list[str]) -> str:
    return (
        "You are the chair judge. Enforce citation discipline (packet or evidence IDs). "
        "Penalize uncited claims. Provide final decision JSON:\n"
        '{'
        '"decision":"BUY|WATCH|AVOID",'
        '"conviction":0-100,'
        '"fair_values":{"bear":...,"base":...,"bull":...},'
        '"probabilities":{"bear":p1,"base":p2,"bull":p3},'
        '"top_reasons":[...],'
        '"top_risks":[...],'
        '"catalysts_near":[...],'
        '"catalysts_long":[...],'
        '"what_would_change_my_mind":[...],'
        '"required_followups":[...],'
        '"debate_quality":0-100,'
        '"uncited_claims_found":[...],'
        '"citations_used":{"packet":[...],"evidence":[...]}'
        '}. '
        f"Packet fields: {packet_json[:1200]}. Evidence ids: {', '.join(evidence_ids)}."
    )
>>>>>>> theirs
