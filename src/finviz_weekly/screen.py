"""Screening + scoring on top of data/latest/finviz_fundamentals.parquet.

Outputs:
- data/latest/finviz_scored.parquet
- data/latest/top{N}_quality_value.csv
- data/latest/top{N}_oversold_quality.csv
- data/latest/top{N}_compounders.csv
- data/latest/candidates.txt
"""
from __future__ import annotations

import logging
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .storage import LATEST_DIR, RUNS_DIR, ensure_dir

LOGGER = logging.getLogger(__name__)

LEARNED_WEIGHTS_DEFAULT = "learned_weights.json"

def _load_learned_weights(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

def _apply_learned_score(df, learned):
    # learned format written by learn.py
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
        # group-specific weights where available, else global
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


def _canon(name: str) -> str:
    s = str(name).lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _build_colmap(df: pd.DataFrame) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for c in df.columns:
        out.setdefault(_canon(c), c)
    return out


def _get(df: pd.DataFrame, colmap: Dict[str, str], canonical: str) -> Optional[pd.Series]:
    real = colmap.get(canonical)
    return df[real] if real else None


def _to_numeric(s: pd.Series) -> pd.Series:
    def _coerce(x):
        if isinstance(x, tuple):
            return None
        return x
    return pd.to_numeric(s.map(_coerce), errors="coerce")


def _winsorize(s: pd.Series, low_q: float = 0.01, high_q: float = 0.99) -> pd.Series:
    s = s.copy()
    non_na = s.dropna()
    if len(non_na) < 30:
        return s
    lo = non_na.quantile(low_q)
    hi = non_na.quantile(high_q)
    return s.clip(lower=lo, upper=hi)


def _pct_score(s: pd.Series, *, higher_better: bool = True) -> pd.Series:
    x = _winsorize(_to_numeric(s))
    r = x.rank(pct=True, method="average")
    if not higher_better:
        r = 1.0 - r
    return r.fillna(0.0)


def _mean_or_zero(parts: List[pd.Series], *, index: pd.Index) -> pd.Series:
    if not parts:
        return pd.Series(0.0, index=index)
    tmp = pd.concat(parts, axis=1)
    return tmp.mean(axis=1).reindex(index).fillna(0.0)


@dataclass(frozen=True)
class ScreenResult:
    name: str
    ranked: pd.DataFrame


def score_snapshot(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, ScreenResult]]:
    if df.empty:
        return df, {}

    colmap = _build_colmap(df)

    def col(name: str) -> Optional[pd.Series]:
        return _get(df, colmap, name)

    # Value (lower is better)
    value_parts: List[pd.Series] = []
    for nm in ["forward_p_e", "p_e", "p_s", "p_b", "p_fcf", "ev_ebitda"]:
        s = col(nm)
        if s is not None:
            value_parts.append(_pct_score(s, higher_better=False))
    value_score = _mean_or_zero(value_parts, index=df.index)

    # Quality (higher is better)
    quality_parts: List[pd.Series] = []
    for nm in ["roe", "roa", "roi", "roic", "gross_margin", "oper_margin", "profit_margin"]:
        s = col(nm)
        if s is not None:
            quality_parts.append(_pct_score(s, higher_better=True))
    quality_score = _mean_or_zero(quality_parts, index=df.index)

    # Risk (mixed)
    risk_parts: List[pd.Series] = []
    for nm in ["debt_eq", "lt_debt_eq", "beta", "volatility_w", "volatility_m"]:
        s = col(nm)
        if s is not None:
            risk_parts.append(_pct_score(s, higher_better=False))
    for nm in ["current_ratio", "quick_ratio", "market_cap"]:
        s = col(nm)
        if s is not None:
            risk_parts.append(_pct_score(s, higher_better=True))
    risk_score = _mean_or_zero(risk_parts, index=df.index)

    # Growth proxies
    growth_parts: List[pd.Series] = []
    for nm in ["eps_next_5y", "sales_past_5y", "eps_this_y", "eps_next_y", "sales_q_q", "eps_q_q"]:
        s = col(nm)
        if s is not None:
            growth_parts.append(_pct_score(s, higher_better=True))
    growth_score = _mean_or_zero(growth_parts, index=df.index)

    # Momentum (optional)
    momentum_parts: List[pd.Series] = []
    for nm in ["perf_month", "perf_quarter", "perf_year", "perf_ytd", "sma50", "sma200"]:
        s = col(nm)
        if s is not None:
            momentum_parts.append(_pct_score(s, higher_better=True))
    momentum_score = _mean_or_zero(momentum_parts, index=df.index)

    # Oversold (lower is better)
    oversold_parts: List[pd.Series] = []
    for nm in ["rsi_14", "perf_month", "perf_week"]:
        s = col(nm)
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

    out["score_quality_value"] = 0.45 * out["score_quality"] + 0.45 * out["score_value"] + 0.10 * out["score_risk"]
    out["score_oversold_quality"] = 0.45 * out["score_quality"] + 0.35 * out["score_oversold"] + 0.20 * out["score_risk"]
    out["score_compounders"] = 0.50 * out["score_quality"] + 0.25 * out["score_growth"] + 0.15 * out["score_value"] + 0.10 * out["score_momentum"]

    screens: Dict[str, ScreenResult] = {}
    for name, score_col in [
        ("quality_value", "score_quality_value"),
        ("oversold_quality", "score_oversold_quality"),
        ("compounders", "score_compounders"),
    ]:
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
        raise FileNotFoundError(f"Latest snapshot not found at {path}. Run the scraper first.")
    return pd.read_parquet(path)


def _apply_basic_filters(df: pd.DataFrame, *, min_market_cap: float, min_price: float) -> pd.DataFrame:
    if df.empty:
        return df
    colmap = _build_colmap(df)
    out = df.copy()

    if "__status" in out.columns:
        out = out[out["__status"] == "ok"].copy()

    mc = _get(out, colmap, "market_cap")
    if mc is not None:
        out = out[_to_numeric(mc) >= float(min_market_cap)].copy()

    price = _get(out, colmap, "price")
    if price is not None:
        out = out[_to_numeric(price) >= float(min_price)].copy()

    return out


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
    run_dir = ensure_dir(Path(out_dir) / RUNS_DIR / as_of.isoformat())
    latest_dir = ensure_dir(Path(out_dir) / LATEST_DIR)

    scored, screens = score_snapshot(latest)

    scored.to_parquet(run_dir / "finviz_scored.parquet", index=False, compression="snappy")
    scored.to_parquet(latest_dir / "finviz_scored.parquet", index=False, compression="snappy")

    unions: List[str] = []

    for key, res in screens.items():
        score_col = {
            "quality_value": "score_quality_value",
            "oversold_quality": "score_oversold_quality",
            "compounders": "score_compounders",
        }[key]

        top = res.ranked.head(int(top_n)).copy()
        keep = [c for c in ["ticker","company","sector","industry","market_cap","price",
                            "score_value","score_quality","score_risk",score_col,"rank"] if c in top.columns]
        top_view = top[keep] if keep else top

        csv_name = f"top{int(top_n)}_{key}.csv"
        top_view.to_csv(run_dir / csv_name, index=False)
        top_view.to_csv(latest_dir / csv_name, index=False)

        if "ticker" in top.columns:
            unions.extend([str(t).strip().upper() for t in top["ticker"].tolist() if str(t).strip()])

    candidates = list(dict.fromkeys(unions))[: int(candidates_max)]
    (run_dir / "candidates.txt").write_text("\n".join(candidates) + ("\n" if candidates else ""))
    (latest_dir / "candidates.txt").write_text("\n".join(candidates) + ("\n" if candidates else ""))
    # --- Learned composite ranking (if learned_weights.json exists) ---
    learned = _load_learned_weights(Path(out_dir) / LATEST_DIR / LEARNED_WEIGHTS_DEFAULT)
    if learned:
        scored2 = _apply_learned_score(scored, learned)
        top = scored2.sort_values("score_learned", ascending=False, kind="mergesort").head(int(top_n)).copy()

        keep = [c for c in ["ticker","company","sector","industry","market_cap","price",
                            "score_value","score_quality","score_risk","score_learned"] if c in top.columns]
        top_view = top[keep] if keep else top
        csv_name = f"top{int(top_n)}_learned.csv"
        top_view.to_csv(run_dir / csv_name, index=False)
        top_view.to_csv(latest_dir / csv_name, index=False)

        if "ticker" in top.columns:
            unions.extend([str(t).strip().upper() for t in top["ticker"].tolist() if str(t).strip()])


    LOGGER.info("Screening done for %s. Outputs in %s and %s", as_of.isoformat(), run_dir, latest_dir)
