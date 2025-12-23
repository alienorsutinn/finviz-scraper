"""Learn composite weights from historical Finviz snapshots.

If there is insufficient history to compute forward returns, we write a
fallback weights file (hand-tuned priors) so screening still works.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

from .screen import score_snapshot

LOGGER = logging.getLogger(__name__)

FEATURES = [
    "score_value",
    "score_quality",
    "score_risk",
    "score_growth",
    "score_momentum",
    "score_oversold",
]

# Hand-tuned "Fundamentals Index" priors (stable, value/quality biased)
FALLBACK_WEIGHTS = {
    "score_quality": 0.30,
    "score_value": 0.30,
    "score_risk": 0.20,
    "score_growth": 0.10,
    "score_oversold": 0.10,
    "score_momentum": 0.00,
}


def _softmax(x: np.ndarray) -> np.ndarray:
    x = x - np.max(x)
    e = np.exp(x)
    return e / np.sum(e)


def _spearman_corr(a: np.ndarray, b: np.ndarray) -> float:
    if len(a) < 5 or len(b) < 5:
        return 0.0
    ra = pd.Series(a).rank().to_numpy()
    rb = pd.Series(b).rank().to_numpy()
    c = np.corrcoef(ra, rb)[0, 1]
    return float(c) if np.isfinite(c) else 0.0


def _fit_weights(X: np.ndarray, y: np.ndarray, *, seed: int = 42) -> Tuple[np.ndarray, float]:
    """Fit non-negative weights (via softmax) to maximize Spearman IC."""
    # Try scipy if available; else random search.
    try:
        from scipy.optimize import minimize  # type: ignore

        def obj(raw_w: np.ndarray) -> float:
            w = _softmax(raw_w)
            s = X @ w
            return -_spearman_corr(s, y)

        init = np.zeros(X.shape[1], dtype=float)
        res = minimize(obj, init, method="Powell", options={"maxiter": 200})
        w = _softmax(res.x)
        ic = _spearman_corr(X @ w, y)
        return w, ic
    except Exception:
        rng = np.random.default_rng(seed)
        best_w = None
        best_ic = -1e9
        for _ in range(3000):
            raw = rng.normal(size=X.shape[1])
            w = _softmax(raw)
            ic = _spearman_corr(X @ w, y)
            if ic > best_ic:
                best_ic = ic
                best_w = w
        return np.asarray(best_w), float(best_ic)


def _compute_forward_returns(hist: pd.DataFrame) -> pd.DataFrame:
    """Compute forward return using next available snapshot per ticker."""
    if "as_of_date" not in hist.columns:
        raise ValueError("history dataframe missing as_of_date")

    df = hist.copy()
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
    df = df.dropna(subset=["ticker", "as_of_date"])

    cols = {c.lower(): c for c in df.columns}
    price_col = cols.get("price")
    if not price_col:
        raise ValueError("history dataframe missing Price column")

    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df = df.dropna(subset=[price_col])

    df = df.sort_values(["ticker", "as_of_date"])
    df["next_price"] = df.groupby("ticker")[price_col].shift(-1)
    df["next_date"] = df.groupby("ticker")["as_of_date"].shift(-1)

    df["fwd_return"] = (df["next_price"] / df[price_col]) - 1.0
    df["horizon_days"] = (df["next_date"] - df["as_of_date"]).dt.days

    df = df.dropna(subset=["fwd_return", "horizon_days"])
    df = df[(df["horizon_days"] >= 7) & (df["horizon_days"] <= 180)].copy()
    return df


def _write_weights(out_path: Path, payload: Dict[str, object]) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path


def train_weights(
    *,
    out_dir: str = "data",
    min_rows_per_group: int = 250,
    group_col: str = "sector",
) -> Path:
    """Train global and per-group weights; write JSON to data/latest/learned_weights.json."""
    hist_path = Path(out_dir) / "history" / "finviz_fundamentals_history.parquet"
    if not hist_path.exists():
        raise FileNotFoundError(f"Missing history parquet at {hist_path}")

    hist = pd.read_parquet(hist_path)
    if "__status" in hist.columns:
        hist = hist[hist["__status"] == "ok"].copy()

    # sanity: dates
    if "as_of_date" not in hist.columns:
        raise ValueError("history parquet missing as_of_date")
    n_dates = pd.Series(hist["as_of_date"].astype(str)).nunique()
    LOGGER.info("History rows=%d unique_dates=%d", len(hist), int(n_dates))

    # score raw history
    hist_scored, _ = score_snapshot(hist)

    # forward returns
    df = _compute_forward_returns(hist_scored)
    LOGGER.info("Forward-return rows=%d", len(df))

    out_path = Path(out_dir) / "latest" / "learned_weights.json"

    # If insufficient data, write fallback priors
    if n_dates < 2 or len(df) < 500:
        payload = {
            "mode": "fallback",
            "reason": "insufficient history for forward-return learning",
            "features": FEATURES,
            "global": {"weights": FALLBACK_WEIGHTS, "ic": 0.0},
            "group_col": group_col,
            "groups": {},
            "n_unique_dates": int(n_dates),
            "n_forward_rows": int(len(df)),
        }
        LOGGER.warning("Insufficient data to learn weights; writing fallback weights to %s", out_path)
        return _write_weights(out_path, payload)

    # Train global
    df = df.dropna(subset=FEATURES + ["fwd_return"]).copy()
    X = df[FEATURES].astype(float).to_numpy()
    y = df["fwd_return"].astype(float).to_numpy()
    w_global, ic_global = _fit_weights(X, y)

    result: Dict[str, object] = {
        "mode": "learned",
        "features": FEATURES,
        "global": {"weights": {f: float(w) for f, w in zip(FEATURES, w_global)}, "ic": float(ic_global)},
        "group_col": group_col,
        "groups": {},
        "n_unique_dates": int(n_dates),
        "n_forward_rows": int(len(df)),
    }

    # Optional per-group
    if group_col in df.columns and n_dates >= 4:
        for g, sub in df.groupby(group_col):
            if len(sub) < min_rows_per_group:
                continue
            sub = sub.dropna(subset=FEATURES + ["fwd_return"])
            if len(sub) < min_rows_per_group:
                continue
            Xg = sub[FEATURES].astype(float).to_numpy()
            yg = sub["fwd_return"].astype(float).to_numpy()
            wg, icg = _fit_weights(Xg, yg, seed=7)
            result["groups"][str(g)] = {
                "weights": {f: float(w) for f, w in zip(FEATURES, wg)},
                "ic": float(icg),
                "n": int(len(sub)),
            }

    LOGGER.info("Trained weights. Global IC=%.4f. Wrote %s", ic_global, out_path)
    return _write_weights(out_path, result)
