from __future__ import annotations

from typing import Any, Dict, List
import pandas as pd

FALLBACK_METRICS = [
    "ticker",
    "company",
    "sector",
    "industry",
    "market_cap",
    "price",
    "wfv",
    "fair_bear",
    "fair_risk",
    "fair_base",
    "fair_bull",
    "price_to_wfv",
    "upside_pct",
    "zone_label",
    "valuation_anchors",
    "score_master",
    "score_learned",
]


def build_packet(scored: pd.DataFrame, ticker: str) -> Dict[str, Any]:
    df = scored[scored["ticker"].astype(str).str.upper() == ticker.upper()]
    if df.empty:
        return {"ticker": ticker, "missing": True}
    row = df.iloc[0].to_dict()
    packet: Dict[str, Any] = {}
    for k in FALLBACK_METRICS:
        packet[k] = row.get(k)

    # Include factor/theme scores if present
    score_cols = [c for c in df.columns if c.startswith("score_")]
    for c in score_cols:
        packet[c] = row.get(c)

    # Common quality metrics
    for c in ["p_e", "forward_p_e", "gross_margin", "oper_margin", "profit_margin", "roe", "debt_eq", "eps_growth_past_5y", "sales_growth_past_5y"]:
        if c in row:
            packet[c] = row.get(c)

    missing = [k for k in ["price", "wfv", "score_master"] if packet.get(k) is None]
    packet["data_quality"] = {"missing": missing}
    return packet
