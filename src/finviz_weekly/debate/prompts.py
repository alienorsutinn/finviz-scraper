from __future__ import annotations

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
