from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .prices import ensure_price_history, PriceFetchConfig

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class TrainConfig:
    horizon_trading_days: int = 21
    min_rows_global: int = 200
    min_rows_per_group: int = 250
    price_lookback_years: int = 10


FACTOR_COLS = [
    "score_quality",
    "score_value",
    "score_risk",
    "score_growth",
    "score_oversold",
    "score_momentum",
]


FALLBACK_WEIGHTS = {
    "score_quality": 0.30,
    "score_value": 0.30,
    "score_risk": 0.20,
    "score_growth": 0.10,
    "score_oversold": 0.10,
    "score_momentum": 0.00,
}


def _winsorize(s: pd.Series, p_lo: float = 0.05, p_hi: float = 0.95) -> pd.Series:
    if s.dropna().empty:
        return s
    lo = s.quantile(p_lo)
    hi = s.quantile(p_hi)
    return s.clip(lower=lo, upper=hi)


def _spearman_ic(x: pd.Series, y: pd.Series) -> float:
    df = pd.DataFrame({"x": x, "y": y}).dropna()
    if len(df) < 30:
        return float("nan")
    return float(df["x"].corr(df["y"], method="spearman"))


def _normalize_positive_weights(ic_map: Dict[str, float]) -> Dict[str, float]:
    vals = {}
    for k, v in ic_map.items():
        if v is None or np.isnan(v):
            vals[k] = 0.0
        else:
            vals[k] = max(0.0, float(v))
    s = sum(vals.values())
    if s <= 0:
        return dict(FALLBACK_WEIGHTS)
    return {k: v / s for k, v in vals.items()}


def _load_latest_scored(out_dir: Path) -> pd.DataFrame:
    p = out_dir / "latest" / "finviz_scored.parquet"
    if not p.exists():
        raise FileNotFoundError(
            f"Missing {p}. Run `python -m finviz_weekly screen --out {out_dir}` first."
        )
    df = pd.read_parquet(p)

    # Keep only rows we can use
    if "ticker" not in df.columns:
        raise ValueError("finviz_scored.parquet missing 'ticker' column")

    df["ticker"] = df["ticker"].astype(str).str.upper()

    # as_of_date is optional; if missing, assume single-date run
    if "as_of_date" in df.columns:
        df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce").dt.date
    else:
        # Use today's date as label; forward returns still computed from prices by date.
        df["as_of_date"] = pd.Timestamp.today().date()

    # Ensure factor cols exist
    for c in FACTOR_COLS:
        if c not in df.columns:
            df[c] = np.nan

    # Basic sanity: drop NaN-heavy rows
    df = df.dropna(subset=["ticker", "as_of_date"])
    return df


def _compute_forward_returns_from_prices(
    scored_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    *,
    horizon_trading_days: int,
) -> pd.DataFrame:
    """
    For each (ticker, as_of_date) row, compute forward return using trading-day index.
    Uses first trading day ON/AFTER as_of_date as entry.
    """
    if prices_df.empty:
        return scored_df.assign(forward_return=np.nan)

    prices_df = prices_df.copy()
    prices_df["ticker"] = prices_df["ticker"].astype(str).str.upper()
    prices_df["date"] = pd.to_datetime(prices_df["date"], errors="coerce").dt.date
    prices_df = prices_df.dropna(subset=["ticker", "date", "close"])
    prices_df = prices_df.sort_values(["ticker", "date"])

    # Build per-ticker arrays for fast lookup
    by_ticker = {}
    for t, g in prices_df.groupby("ticker", sort=False):
        dates = np.array(g["date"].tolist(), dtype=object)
        close = np.array(g["close"].astype(float).tolist(), dtype=float)
        by_ticker[t] = (dates, close)

    fwd = []
    for row in scored_df[["ticker", "as_of_date"]].itertuples(index=False):
        t = row.ticker
        d0 = row.as_of_date
        if t not in by_ticker or d0 is None:
            fwd.append(np.nan)
            continue

        dates, close = by_ticker[t]
        # find first index with date >= d0
        i0 = None
        for i in range(len(dates)):
            if dates[i] >= d0:
                i0 = i
                break
        if i0 is None:
            fwd.append(np.nan)
            continue

        i1 = i0 + horizon_trading_days
        if i1 >= len(close):
            fwd.append(np.nan)
            continue

        p0 = close[i0]
        p1 = close[i1]
        if not np.isfinite(p0) or not np.isfinite(p1) or p0 <= 0:
            fwd.append(np.nan)
            continue
        fwd.append((p1 / p0) - 1.0)

    out = scored_df.copy()
    out["forward_return"] = fwd
    return out


def train_weights(
    out_dir: str | Path,
    *,
    group_col: str = "sector",
    min_rows_per_group: int = 250,
) -> Path:
    """
    Trains factor weights using forward returns computed from price history.
    Writes:
      - data/latest/learned_weights.json
      - data/latest/ic_diagnostics.csv
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    latest_dir = out_path / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)

    cfg = TrainConfig(min_rows_per_group=min_rows_per_group)

    scored = _load_latest_scored(out_path)

    # If you have multiple dates later, this will train across all (ticker,date) rows.
    unique_dates = int(scored["as_of_date"].nunique())
    log.info("Scored rows=%d unique_dates=%d", len(scored), unique_dates)

    tickers = scored["ticker"].astype(str).str.upper().unique().tolist()

    ensure_price_history(
        out_path,
        tickers,
        cfg=PriceFetchConfig(lookback_years=cfg.price_lookback_years),
    )
    prices = pd.read_parquet(out_path / "history" / "prices.parquet")

    if prices is None or prices.empty:
        log.warning("Prices store is empty; cannot compute forward returns. Falling back.")
        weights_path = latest_dir / "learned_weights.json"
        diag_path = latest_dir / "ic_diagnostics.csv"
        payload = {
            "version": 1,
            "mode": "fallback",
            "horizon_trading_days": cfg.horizon_trading_days,
            "n_unique_dates": unique_dates,
            "n_forward_rows": 0,
            "as_of": "",
            "group_col": group_col,
            "features": list(FACTOR_COLS),
            "global": {"weights": dict(FALLBACK_WEIGHTS), "ic": {}},
            "groups": {},
            "by_group": {},
        }
        weights_path.write_text(json.dumps(payload, indent=2))
        pd.DataFrame([{"scope": "global", "factor": k, "ic": float("nan")} for k in FACTOR_COLS]).to_csv(diag_path, index=False)
        return weights_path


    joined = _compute_forward_returns_from_prices(
        scored,
        prices,
        horizon_trading_days=cfg.horizon_trading_days,
    )
    joined["forward_return"] = _winsorize(joined["forward_return"])

    n_forward = int(joined["forward_return"].notna().sum())
    log.info("Forward-return rows=%d (horizon=%d trading days)", n_forward, cfg.horizon_trading_days)
    as_of_label = ""
    if "as_of_date" in joined.columns:
        try:
            as_of_label = str(max(joined["as_of_date"]))
        except Exception:
            as_of_label = ""

    weights_path = latest_dir / "learned_weights.json"
    diag_path = latest_dir / "ic_diagnostics.csv"

    if n_forward < cfg.min_rows_global:
        log.warning(
            "Insufficient forward-return rows to learn weights (%d < %d). Writing fallback weights to %s",
            n_forward,
            cfg.min_rows_global,
            weights_path,
        )
        payload = {
            "version": 1,
            "mode": "fallback",
            "horizon_trading_days": cfg.horizon_trading_days,
            "n_unique_dates": unique_dates,
            "n_forward_rows": n_forward,
            "as_of": as_of_label,
            "group_col": group_col,
            "features": list(FACTOR_COLS),
            "global": {"weights": dict(FALLBACK_WEIGHTS), "ic": {}},
            "groups": {},
            "by_group": {},
        }
        weights_path.write_text(json.dumps(payload, indent=2))
        pd.DataFrame([{"scope": "global", "factor": k, "ic": np.nan} for k in FACTOR_COLS]).to_csv(
            diag_path, index=False
        )
        return weights_path

    # Global ICs
    global_ic = {}
    for c in FACTOR_COLS:
        global_ic[c] = _spearman_ic(joined[c], joined["forward_return"]) if c in joined.columns else np.nan
    global_weights = _normalize_positive_weights(global_ic)

    # Group ICs (optional)
    by_group = {}
    diag_rows = []
    for f, ic in global_ic.items():
        diag_rows.append({"scope": "global", "group": "", "factor": f, "ic": ic})

    if group_col in joined.columns:
        for gval, gdf in joined.groupby(group_col, dropna=True):
            gdf = gdf.copy()
            gdf = gdf.dropna(subset=["forward_return"])
            if len(gdf) < cfg.min_rows_per_group:
                continue
            ic_map = {}
            for c in FACTOR_COLS:
                ic_map[c] = _spearman_ic(gdf[c], gdf["forward_return"]) if c in gdf.columns else np.nan
                diag_rows.append({"scope": "group", "group": str(gval), "factor": c, "ic": ic_map[c]})
            by_group[str(gval)] = {"weights": _normalize_positive_weights(ic_map), "ic": ic_map}
    else:
        log.warning("group_col=%s not found in scored data; skipping group weights", group_col)

    payload = {
        "version": 1,
        "mode": "learned",
        "as_of": as_of_label,
        "group_col": group_col,
        "features": list(FACTOR_COLS),
        "horizon_trading_days": cfg.horizon_trading_days,
        "n_unique_dates": unique_dates,
        "n_forward_rows": n_forward,
        "global": {"weights": global_weights, "ic": global_ic},
        "groups": by_group,
        "by_group": by_group,
    }
    weights_path.write_text(json.dumps(payload, indent=2))
    pd.DataFrame(diag_rows).to_csv(diag_path, index=False)

    # Summary logs
    global_ic_mean = np.nanmean([v for v in global_ic.values()]) if global_ic else float("nan")
    log.info("Trained weights. Mean(global IC)=%.4f. Wrote %s", global_ic_mean, weights_path)
    log.info("IC diagnostics -> %s", diag_path)
    return weights_path
