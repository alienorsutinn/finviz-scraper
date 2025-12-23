"""Multiples-based valuation derived from a single Finviz snapshot.

This module deliberately stays within the information Finviz provides.

Core trick
---------
If you have Price and a ratio, you can infer a per-share fundamental:
- P/E  => EPS  ~= Price / (P/E)
- P/B  => BVPS ~= Price / (P/B)
- P/FCF=> FCF/share ~= Price / (P/FCF)

Then you can re-rate that per-share fundamental using a peer median multiple
(computed within sector/industry/size buckets) to get a "fair" price.

From there:
- Build scenario fair values (bear/risk/base/bull) using multipliers
- Compute WFV (weighted fair value)
- Label zones (AGGRESSIVE / ADD / STARTER / WATCH / AVOID)

This gives you a *valuation layer* even before you have historical snapshots.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import math
import pandas as pd


@dataclass(frozen=True)
class ScenarioConfig:
    multipliers: dict[str, float]
    probabilities: dict[str, float]
    aggressive_band: float = 0.80
    add_band_hi: float = 0.93
    starter_band_hi: float = 1.00
    watch_band_hi: float = 1.15


DEFAULT_SCENARIOS = ScenarioConfig(
    multipliers={"bear": 0.70, "risk": 0.85, "base": 1.00, "bull": 1.20},
    probabilities={"bear": 0.25, "risk": 0.25, "base": 0.40, "bull": 0.10},
)


def _to_float(x) -> float:
    """Best-effort numeric parse for Finviz-style strings."""
    if x is None:
        return float("nan")
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "" or s in {"-", "N/A", "na", "nan", "None"}:
        return float("nan")

    # percentages
    if s.endswith("%"):  # e.g. "12.3%"
        try:
            return float(s[:-1]) / 100.0
        except Exception:
            return float("nan")

    # market cap style: 12.3B / 850M / 120K
    mult = 1.0
    if s[-1] in {"B", "M", "K"}:
        unit = s[-1]
        s = s[:-1]
        mult = {"B": 1e9, "M": 1e6, "K": 1e3}[unit]

    # commas
    s = s.replace(",", "")
    try:
        return float(s) * mult
    except Exception:
        return float("nan")


def _bucket_market_cap(mcap: float) -> str:
    if not math.isfinite(mcap) or mcap <= 0:
        return "unknown"
    if mcap < 3e8:
        return "micro"
    if mcap < 2e9:
        return "small"
    if mcap < 1e10:
        return "mid"
    if mcap < 2e11:
        return "large"
    return "mega"


def _find_col(colmap: dict[str, str], candidates: Iterable[str]) -> str | None:
    """Return original column name for any normalized candidate key."""
    for c in candidates:
        key = c.strip().lower()
        if key in colmap:
            return colmap[key]
    return None


def _zone_from_ratio(price_to_wfv: float, cfg: ScenarioConfig) -> str:
    if not math.isfinite(price_to_wfv) or price_to_wfv <= 0:
        return "WATCH"
    if price_to_wfv <= cfg.aggressive_band:
        return "AGGRESSIVE"
    if price_to_wfv <= cfg.add_band_hi:
        return "ADD"
    if price_to_wfv <= cfg.starter_band_hi:
        return "STARTER"
    if price_to_wfv <= cfg.watch_band_hi:
        return "WATCH"
    return "AVOID"


def add_multiples_valuation(
    df: pd.DataFrame,
    *,
    colmap: dict[str, str],
    cfg: ScenarioConfig = DEFAULT_SCENARIOS,
    group_cols: tuple[str, ...] = ("sector", "industry"),
) -> pd.DataFrame:
    """Add WFV + scenario fair values + zones using peer-anchored multiples.

    Parameters
    ----------
    df:
        Snapshot dataframe.
    colmap:
        Mapping from normalized name -> original column name (from screen._build_colmap).
    cfg:
        Scenario configuration.
    group_cols:
        Columns (normalized names) to use for peer grouping.

    Returns
    -------
    DataFrame with appended columns:
    - fair_bear, fair_risk, fair_base, fair_bull
    - wfv
    - price_to_wfv, upside_pct
    - zone_label
    - valuation_anchors
    """

    out = df.copy()

    price_col = _find_col(colmap, ["price", "Price"])
    pe_col = _find_col(colmap, ["p/e", "p/e ttm", "pe", "pe ttm"])
    pb_col = _find_col(colmap, ["p/b", "p/b ttm", "pb"])
    pfcf_col = _find_col(colmap, ["p/fcf", "p/ fcf", "p/cf", "p/cash flow"])
    mcap_col = _find_col(colmap, ["market cap", "market_cap", "mkt cap", "mcap"])

    if price_col is None:
        # can't do anything meaningful
        return out

    # numeric versions
    price = out[price_col].map(_to_float)
    pe = out[pe_col].map(_to_float) if pe_col and pe_col in out.columns else pd.Series(float("nan"), index=out.index)
    pb = out[pb_col].map(_to_float) if pb_col and pb_col in out.columns else pd.Series(float("nan"), index=out.index)
    pfcf = out[pfcf_col].map(_to_float) if pfcf_col and pfcf_col in out.columns else pd.Series(float("nan"), index=out.index)

    if mcap_col and mcap_col in out.columns:
        mcap = out[mcap_col].map(_to_float)
    else:
        mcap = pd.Series(float("nan"), index=out.index)

    cap_bucket = mcap.map(_bucket_market_cap)

    # grouping key
    group_parts: list[pd.Series] = []
    for g in group_cols:
        gcol = _find_col(colmap, [g])
        if gcol and gcol in out.columns:
            group_parts.append(out[gcol].astype(str).fillna("unknown"))
    group_parts.append(cap_bucket)

    if group_parts:
        group_key = group_parts[0]
        for p in group_parts[1:]:
            group_key = group_key.astype(str) + "|" + p.astype(str)
    else:
        group_key = cap_bucket

    out["peer_group"] = group_key

    # peer medians of multiples
    def _median_pos(s: pd.Series) -> float:
        s = pd.to_numeric(s, errors="coerce")
        s = s[(s > 0) & (s.replace([math.inf, -math.inf], math.nan).notna())]
        if len(s) == 0:
            return float("nan")
        return float(s.median())

    peer_pe = out.assign(_pe=pe).groupby("peer_group")["_pe"].transform(_median_pos)
    peer_pb = out.assign(_pb=pb).groupby("peer_group")["_pb"].transform(_median_pos)
    peer_pfcf = out.assign(_pfcf=pfcf).groupby("peer_group")["_pfcf"].transform(_median_pos)

    # implied per-share fundamentals
    eps = price / pe.replace(0, math.nan)
    bvps = price / pb.replace(0, math.nan)
    fcfps = price / pfcf.replace(0, math.nan)

    fair_pe = eps * peer_pe
    fair_pb = bvps * peer_pb
    fair_pfcf = fcfps * peer_pfcf

    fair_cols = [fair_pe, fair_pb, fair_pfcf]
    fair_stack = pd.concat(fair_cols, axis=1)
    fair_stack.columns = ["fair_pe", "fair_pb", "fair_pfcf"]

    base_fair = fair_stack.mean(axis=1, skipna=True)
    anchors = fair_stack.notna().sum(axis=1)

    out["valuation_anchors"] = anchors
    out["fair_base"] = base_fair

    for scen, mult in cfg.multipliers.items():
        out[f"fair_{scen}"] = base_fair * mult

    # Weighted fair value
    wfv = pd.Series(0.0, index=out.index)
    wfv[:] = float("nan")
    # Only compute where we have a base fair
    ok = base_fair.notna() & (anchors > 0)
    if ok.any():
        exp = pd.Series(0.0, index=out.index)
        for scen, prob in cfg.probabilities.items():
            exp = exp + prob * out[f"fair_{scen}"]
        wfv.loc[ok] = exp.loc[ok]

    out["wfv"] = wfv

    price_to_wfv = price / wfv.replace(0, math.nan)
    out["price_to_wfv"] = price_to_wfv
    out["upside_pct"] = (wfv / price.replace(0, math.nan)) - 1.0

    out["zone_label"] = price_to_wfv.map(lambda r: _zone_from_ratio(float(r) if pd.notna(r) else float("nan"), cfg))

    return out
