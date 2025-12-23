"""Screening + scoring on top of data/latest/finviz_fundamentals.parquet.

Outputs (latest + dated run folder):
- finviz_scored.parquet
- top{N}_*.csv for each theme (global + operating/assetmgr/bdc segments)
- candidates*.txt (balanced union + segmented lists)
- conviction_2plus.csv / conviction_3plus.csv / conviction.txt
- report.md (one-page summary)
"""
from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .valuation import add_multiples_valuation

from .report import write_report

LOGGER = logging.getLogger(__name__)

LATEST_DIR = "latest"
RUNS_DIR = "runs"
LEARNED_WEIGHTS_DEFAULT = "learned_weights.json"

KNOWN_BDCS = {
    "ARCC","MAIN","BXSL","OBDC","GBDC","BBDC","BCSF","KBDC","TRIN","FDUS","GAIN","GSBD","SLRC","HTGC","MSDL","MSIF","TCPC","PFLT","NMFC","CSWC"
}


@dataclass
class ScreenResult:
    name: str
    ranked: pd.DataFrame


def _build_colmap(df: pd.DataFrame) -> Dict[str, str]:
    return {c.lower(): c for c in df.columns}


def _find_col(colmap: Dict[str, str], candidates: List[str]) -> Optional[str]:
    for k in candidates:
        c = colmap.get(k.lower())
        if c:
            return c
    return None


def _parse_num(x: object) -> float:
    if x is None:
        return float("nan")
    s = str(x).strip()
    if s in ("", "-", "nan", "None"):
        return float("nan")
    s = s.replace("%", "").replace(",", "")
    mult = 1.0
    if s.endswith("B"):
        mult = 1e9
        s = s[:-1]
    elif s.endswith("M"):
        mult = 1e6
        s = s[:-1]
    elif s.endswith("K"):
        mult = 1e3
        s = s[:-1]
    try:
        return float(s) * mult
    except Exception:
        return float("nan")


def _to_num(s: pd.Series) -> pd.Series:
    return s.map(_parse_num)


def _pct_score(s: pd.Series, *, higher_better: bool) -> pd.Series:
    v = _to_num(s)
    r = v.rank(pct=True)
    return r if higher_better else (1.0 - r)


def _mean_or_zero(parts: List[pd.Series], *, index: pd.Index) -> pd.Series:
    if not parts:
        return pd.Series([0.0] * len(index), index=index)
    dfp = pd.concat(parts, axis=1)
    return dfp.mean(axis=1).fillna(0.0)


def score_snapshot(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, ScreenResult]]:
    if df.empty:
        return df, {}

    colmap = _build_colmap(df)

    def get_series(names: List[str]) -> Optional[pd.Series]:
        c = _find_col(colmap, names)
        return df[c] if c else None

    # --- factor scores (robust to missing columns) ---
    value_parts: List[pd.Series] = []
    for nm in ["forward_p_e", "p_e", "p_s", "p_b", "p_fcf", "ev_ebitda"]:
        s = get_series([nm])
        if s is not None:
            value_parts.append(_pct_score(s, higher_better=False))
    value_score = _mean_or_zero(value_parts, index=df.index)

    quality_parts: List[pd.Series] = []
    for nm in ["roi", "roe", "roa", "gross_margin", "oper_margin", "profit_margin"]:
        s = get_series([nm])
        if s is not None:
            quality_parts.append(_pct_score(s, higher_better=True))
    quality_score = _mean_or_zero(quality_parts, index=df.index)

    risk_parts: List[pd.Series] = []
    for nm in ["current_ratio", "quick_ratio"]:
        s = get_series([nm])
        if s is not None:
            risk_parts.append(_pct_score(s, higher_better=True))
    for nm in ["debt_eq", "lt_debt_eq", "beta"]:
        s = get_series([nm])
        if s is not None:
            risk_parts.append(_pct_score(s, higher_better=False))
    risk_score = _mean_or_zero(risk_parts, index=df.index)

    growth_parts: List[pd.Series] = []
    for nm in ["sales_growth_past_5y", "eps_growth_past_5y", "sales_growth_qoq", "eps_growth_qoq"]:
        s = get_series([nm])
        if s is not None:
            growth_parts.append(_pct_score(s, higher_better=True))
    growth_score = _mean_or_zero(growth_parts, index=df.index)

    momentum_parts: List[pd.Series] = []
    for nm in ["perf_quarter", "perf_half_y", "perf_year"]:
        s = get_series([nm])
        if s is not None:
            momentum_parts.append(_pct_score(s, higher_better=True))
    momentum_score = _mean_or_zero(momentum_parts, index=df.index)

    oversold_parts: List[pd.Series] = []
    for nm in ["rsi_14", "perf_month", "perf_week"]:
        s = get_series([nm])
        if s is not None:
            oversold_parts.append(_pct_score(s, higher_better=False))
    oversold_score = _mean_or_zero(oversold_parts, index=df.index)

    out = df.copy()
    out["score_value"] = value_score
    out["score_quality"] = quality_score
    out["score_risk"] = risk_score
    out["score_growth"] = growth_score
    out["score_momentum"] = momentum_score
    out["score_oversold"] = oversold_score

    # --- existing composites ---
    out["score_quality_value"] = 0.45 * out["score_quality"] + 0.45 * out["score_value"] + 0.10 * out["score_risk"]
    out["score_oversold_quality"] = 0.45 * out["score_quality"] + 0.35 * out["score_oversold"] + 0.20 * out["score_risk"]
    out["score_compounders"] = 0.50 * out["score_quality"] + 0.25 * out["score_growth"] + 0.15 * out["score_value"] + 0.10 * out["score_momentum"]

    # --- new themes (do NOT require extra columns) ---
    out["score_hq_low_leverage"] = 0.60 * out["score_quality"] + 0.40 * out["score_risk"]
    out["score_turnaround_value"] = 0.45 * out["score_oversold"] + 0.35 * out["score_value"] + 0.20 * out["score_quality"]
    out["score_garp"] = 0.35 * out["score_quality"] + 0.35 * out["score_growth"] + 0.25 * out["score_value"] + 0.05 * out["score_risk"]

    # demonstrating “shareholder yield” only if dividend exists
    div_col = _find_col(colmap, ["dividend_yield", "dividend", "dividend_%", "dividend%"])
    if div_col:
        div_score = _pct_score(out[div_col], higher_better=True).fillna(0.0)
        out["score_shareholder_yield"] = 0.55 * div_score + 0.25 * out["score_value"] + 0.20 * out["score_risk"]
    # “short squeeze / crowded” only if short float exists
    sf_col = _find_col(colmap, ["short_float", "short_float_%", "shortfloat", "short_interest"])
    if sf_col:
        sf_score = _pct_score(out[sf_col], higher_better=True).fillna(0.0)
        out["score_short_squeeze"] = 0.40 * sf_score + 0.30 * out["score_oversold"] + 0.30 * out["score_momentum"]

    # Multiples-based valuation + WFV/zones (works even with only 1 snapshot date)
    out = add_multiples_valuation(out, colmap=colmap)

    # --- screens dict ---
    screens: Dict[str, ScreenResult] = {}
    theme_defs = [
        ("quality_value", "score_quality_value"),
        ("oversold_quality", "score_oversold_quality"),
        ("compounders", "score_compounders"),
        ("hq_low_leverage", "score_hq_low_leverage"),
        ("turnaround_value", "score_turnaround_value"),
        ("garp", "score_garp"),
    ]
    if "score_shareholder_yield" in out.columns:
        theme_defs.append(("shareholder_yield", "score_shareholder_yield"))
    if "score_short_squeeze" in out.columns:
        theme_defs.append(("short_squeeze", "score_short_squeeze"))

    for name, score_col in theme_defs:
        ranked = out.sort_values(score_col, ascending=False, kind="mergesort").reset_index(drop=True)
        ranked["rank"] = ranked.index + 1
        screens[name] = ScreenResult(name=name, ranked=ranked)

    return out, screens


def _infer_as_of(df: pd.DataFrame) -> date:
    if "as_of_date" in df.columns:
        try:
            return date.fromisoformat(str(df["as_of_date"].iloc[0]))
        except Exception:
            pass
    return date.today()


def _read_latest(out_dir: str) -> pd.DataFrame:
    path = Path(out_dir) / LATEST_DIR / "finviz_fundamentals.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Latest snapshot not found: {path}")
    return pd.read_parquet(path)


def _apply_basic_filters(df: pd.DataFrame, *, min_market_cap: float, min_price: float) -> pd.DataFrame:
    out = df.copy()
    colmap = _build_colmap(out)

    mc_col = _find_col(colmap, ["market_cap"])
    if mc_col:
        out["_mc"] = _to_num(out[mc_col])
        out = out[out["_mc"] >= float(min_market_cap)].copy()

    px_col = _find_col(colmap, ["price"])
    if px_col:
        out["_px"] = pd.to_numeric(out[px_col], errors="coerce")
        out = out[out["_px"] >= float(min_price)].copy()

    return out.drop(columns=[c for c in ["_mc", "_px"] if c in out.columns], errors="ignore")


def _tag_asset_type(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ticker"] = out["ticker"].astype(str).str.upper()

    sector = out["sector"].astype(str) if "sector" in out.columns else pd.Series([""] * len(out), index=out.index)
    industry = out["industry"].astype(str) if "industry" in out.columns else pd.Series([""] * len(out), index=out.index)

    is_bdc = (
        out["ticker"].isin(KNOWN_BDCS)
        | industry.str.contains("business development", case=False, na=False)
        | industry.str.contains(r"\bBDC\b", case=False, na=False)
    )

    is_asset_mgr = (
        sector.str.contains("financial", case=False, na=False)
        & (
            industry.str.contains("asset management", case=False, na=False)
            | industry.str.contains("capital markets", case=False, na=False)
            | industry.str.contains("financial data", case=False, na=False)
            | industry.str.contains("broker", case=False, na=False)
        )
        & (~is_bdc)
    )

    out["asset_type"] = "operating"
    out.loc[is_asset_mgr, "asset_type"] = "asset_manager"
    out.loc[is_bdc, "asset_type"] = "bdc"
    return out


def _load_learned_weights(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _apply_learned_score(df: pd.DataFrame, learned: dict) -> pd.DataFrame:
    feats = learned.get("features") or []
    global_w = (learned.get("global") or {}).get("weights") or {}
    group_col = learned.get("group_col") or None
    groups = learned.get("groups") or {}

    out = df.copy()
    out["score_learned"] = 0.0

    def score_row(row, wmap):
        s = 0.0
        for f in feats:
            if f in out.columns:
                try:
                    s += float(wmap.get(f, 0.0)) * float(row.get(f, 0.0))
                except Exception:
                    pass
        return s

    if group_col and group_col in out.columns and groups:
        scores = []
        for _, r in out.iterrows():
            g = str(r.get(group_col, ""))
            wmap = (groups.get(g) or {}).get("weights") or global_w
            scores.append(score_row(r, wmap))
        out["score_learned"] = scores
    else:
        wmap = global_w
        out["score_learned"] = out.apply(lambda r: score_row(r, wmap), axis=1)

    return out


def _top(df: pd.DataFrame, score_col: str, n: int) -> pd.DataFrame:
    if df.empty:
        return df
    return df.sort_values(score_col, ascending=False, kind="mergesort").head(int(n)).copy()


def run_screening(
    *,
    out_dir: str = "data",
    top_n: int = 50,
    min_market_cap: float = 300_000_000,
    min_price: float = 1.0,
    candidates_max: int = 100,
) -> None:
    latest = _read_latest(out_dir)
    latest = _apply_basic_filters(latest, min_market_cap=min_market_cap, min_price=min_price)
    if latest.empty:
        LOGGER.warning("No rows after filters; nothing to screen.")
        return

    as_of = _infer_as_of(latest)
    run_dir = Path(out_dir) / RUNS_DIR / as_of.isoformat()
    latest_dir = Path(out_dir) / LATEST_DIR
    run_dir.mkdir(parents=True, exist_ok=True)
    latest_dir.mkdir(parents=True, exist_ok=True)

    scored, screens = score_snapshot(latest)
    scored = _tag_asset_type(scored)

    scored.to_parquet(run_dir / "finviz_scored.parquet", index=False)
    scored.to_parquet(latest_dir / "finviz_scored.parquet", index=False)

    lists: Dict[str, pd.DataFrame] = {}
    unions_operating: List[str] = []
    unions_asset: List[str] = []
    unions_bdc: List[str] = []

    def export_list(name: str, df: pd.DataFrame, score_col: str):
        top = _top(df, score_col, top_n)
        keep = [c for c in [
            "ticker", "company", "sector", "industry", "asset_type",
            "market_cap", "price",
            # valuation outputs
            "wfv", "fair_bear", "fair_risk", "fair_base", "fair_bull",
            "price_to_wfv", "upside_pct", "zone_label", "valuation_anchors",
            score_col,
        ] if c in top.columns]

        top_view = top[keep] if keep else top
        csv_name = f"top{int(top_n)}_{name}.csv"
        top_view.to_csv(run_dir / csv_name, index=False)
        top_view.to_csv(latest_dir / csv_name, index=False)
        lists[name] = top_view
        if "ticker" in top.columns:
            return [str(t).strip().upper() for t in top["ticker"].tolist() if str(t).strip()]
        return []

    # Export all screens returned by score_snapshot
    for key, res in screens.items():
        score_col = None
        for c in res.ranked.columns:
            if c.startswith("score_") and c.endswith(key) and c in res.ranked.columns:
                score_col = c
        # fallback: we know rank is computed by sorting on a score column, so just use the known mapping:
        mapping = {
            "quality_value": "score_quality_value",
            "oversold_quality": "score_oversold_quality",
            "compounders": "score_compounders",
            "hq_low_leverage": "score_hq_low_leverage",
            "turnaround_value": "score_turnaround_value",
            "garp": "score_garp",
            "shareholder_yield": "score_shareholder_yield",
            "short_squeeze": "score_short_squeeze",
        }
        score_col = mapping.get(key)
        if not score_col or score_col not in scored.columns:
            continue

        export_list(key, scored, score_col)

        op = scored[scored["asset_type"] == "operating"].copy()
        am = scored[scored["asset_type"] == "asset_manager"].copy()
        bd = scored[scored["asset_type"] == "bdc"].copy()

        unions_operating.extend(export_list(f"operating_{key}", op, score_col))
        unions_asset.extend(export_list(f"assetmgr_{key}", am, score_col))
        unions_bdc.extend(export_list(f"bdc_{key}", bd, score_col))

    # Learned composite (if weights exist)
    learned = _load_learned_weights(Path(out_dir) / LATEST_DIR / LEARNED_WEIGHTS_DEFAULT)
    if learned:
        scored2 = _apply_learned_score(scored, learned)
        scored2 = _tag_asset_type(scored2)

        export_list("learned", scored2, "score_learned")
        unions_operating.extend(export_list("operating_learned", scored2[scored2["asset_type"]=="operating"].copy(), "score_learned"))
        unions_asset.extend(export_list("assetmgr_learned", scored2[scored2["asset_type"]=="asset_manager"].copy(), "score_learned"))
        unions_bdc.extend(export_list("bdc_learned", scored2[scored2["asset_type"]=="bdc"].copy(), "score_learned"))

    def dedupe(xs: List[str]) -> List[str]:
        return list(dict.fromkeys([x for x in xs if x]))

    unions_operating = dedupe(unions_operating)
    unions_asset = dedupe(unions_asset)
    unions_bdc = dedupe(unions_bdc)

    # Balanced candidates caps
    operating_cap = max(1, int(0.70 * candidates_max))
    asset_cap = max(0, int(0.20 * candidates_max))
    bdc_cap = max(0, candidates_max - operating_cap - asset_cap)

    op = unions_operating[:operating_cap]
    am = unions_asset[:asset_cap]
    bd = unions_bdc[:bdc_cap]

    union = dedupe(op + am + bd)
    if len(union) < candidates_max:
        remainder = []
        remainder.extend(unions_operating[operating_cap:])
        remainder.extend(unions_asset[asset_cap:])
        remainder.extend(unions_bdc[bdc_cap:])
        union = dedupe(union + remainder)[: int(candidates_max)]

    def write_txt(path: Path, ticks: List[str]):
        path.write_text("\n".join(ticks) + ("\n" if ticks else ""), encoding="utf-8")

    write_txt(run_dir / "candidates_operating.txt", op)
    write_txt(latest_dir / "candidates_operating.txt", op)
    write_txt(run_dir / "candidates_asset_managers.txt", am)
    write_txt(latest_dir / "candidates_asset_managers.txt", am)
    write_txt(run_dir / "candidates_bdc.txt", bd)
    write_txt(latest_dir / "candidates_bdc.txt", bd)
    write_txt(run_dir / "candidates.txt", union)
    write_txt(latest_dir / "candidates.txt", union)

    # Conviction lists: tickers appearing in 2+ / 3+ lists (across ALL exported top lists)
    list_membership = defaultdict(list)
    for list_name, df_list in lists.items():
        if "ticker" not in df_list.columns:
            continue
        for t in df_list["ticker"].astype(str).str.upper().tolist():
            if t:
                list_membership[t].append(list_name)

    rows = []
    for t, lsts in list_membership.items():
        rows.append({"ticker": t, "count": len(set(lsts)), "lists": ", ".join(sorted(set(lsts)))})
    conv = pd.DataFrame(rows).sort_values(["count", "ticker"], ascending=[False, True])

    conv2 = conv[conv["count"] >= 2].copy()
    conv3 = conv[conv["count"] >= 3].copy()

    conv2.to_csv(run_dir / "conviction_2plus.csv", index=False)
    conv2.to_csv(latest_dir / "conviction_2plus.csv", index=False)
    conv3.to_csv(run_dir / "conviction_3plus.csv", index=False)
    conv3.to_csv(latest_dir / "conviction_3plus.csv", index=False)

    write_txt(run_dir / "conviction.txt", conv2["ticker"].head(200).tolist())
    write_txt(latest_dir / "conviction.txt", conv2["ticker"].head(200).tolist())

    # Report
    write_report(
        out_dir=out_dir,
        as_of=as_of.isoformat(),
        scored=scored,
        lists=lists,
        candidates_by_group={
            "operating": op,
            "asset_manager": am,
            "bdc": bd,
            "union": union,
        },
    )

    LOGGER.info(
        "Screening done for %s. Outputs in %s and %s (candidates=%d, conviction_2plus=%d)",
        as_of.isoformat(),
        run_dir,
        latest_dir,
        len(union),
        int(len(conv2)),
    )
