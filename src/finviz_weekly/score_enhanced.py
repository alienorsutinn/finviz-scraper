"""Enhanced scoring with new data sources."""
from __future__ import annotations

import logging
import pandas as pd
import numpy as np

LOGGER = logging.getLogger(__name__)


def calculate_insider_score(df: pd.DataFrame) -> pd.Series:
    """
    Calculate insider score (0-10) based on net insider activity.

    Positive net value = buying signal (score > 5)
    Negative net value = selling signal (score < 5)

    Args:
        df: DataFrame with 'insider_net_value' column

    Returns:
        Series with insider scores (0-10 scale)
    """
    scores = pd.Series(5.0, index=df.index)  # neutral default

    if "insider_net_value" in df.columns:
        mask = df["insider_net_value"].notna()
        if mask.any():
            # Normalize to 0-10 scale
            max_val = df.loc[mask, "insider_net_value"].abs().max()
            if max_val > 0:
                # Scale from -1 to +1, then shift to 0-10
                normalized = df.loc[mask, "insider_net_value"] / max_val
                scores[mask] = 5 + 5 * normalized.clip(-1, 1)

    return scores


def calculate_earnings_score(df: pd.DataFrame) -> pd.Series:
    """
    Calculate earnings quality score (0-10) based on earnings surprise history.

    Positive alpha = consistent outperformance (score > 5)
    Negative alpha = consistent underperformance (score < 5)

    Args:
        df: DataFrame with 'earnings_avg_alpha' column

    Returns:
        Series with earnings scores (0-10 scale)
    """
    scores = pd.Series(5.0, index=df.index)  # neutral default

    if "earnings_avg_alpha" in df.columns:
        mask = df["earnings_avg_alpha"].notna()
        if mask.any():
            # Alpha > 3% = excellent (10), < -3% = poor (0)
            # Scale from -3 to +3 percentage points
            normalized = df.loc[mask, "earnings_avg_alpha"].clip(-3, 3) / 3
            scores[mask] = 5 + 5 * normalized

    return scores


def calculate_financial_health_score(df: pd.DataFrame) -> pd.Series:
    """
    Calculate financial health score (0-10) based on key ratios.

    Combines profitability (net margin, ROE) and safety (liquidity).

    Args:
        df: DataFrame with financial ratio columns

    Returns:
        Series with financial health scores (0-10 scale)
    """
    scores = pd.Series(5.0, index=df.index)  # neutral default

    has_ratios = (
        "net_margin" in df.columns
        and "roe" in df.columns
        and "current_ratio" in df.columns
    )

    if has_ratios:
        mask = (
            df["net_margin"].notna()
            & df["roe"].notna()
            & df["current_ratio"].notna()
        )

        if mask.any():
            # Profitability scores (each 0-3.33 points)
            # Net margin: 0-30% is good
            margin_score = df.loc[mask, "net_margin"].clip(0, 0.3) / 0.3 * 3.33

            # ROE: 0-30% is good
            roe_score = df.loc[mask, "roe"].clip(0, 0.3) / 0.3 * 3.33

            # Liquidity score (0-3.33 points)
            # Current ratio: 0-3 is range (>2 is good)
            liquidity_score = df.loc[mask, "current_ratio"].clip(0, 3) / 3 * 3.33

            # Total score (0-10)
            scores[mask] = margin_score + roe_score + liquidity_score

    return scores


def calculate_enhanced_total_score(
    df: pd.DataFrame,
    weights: dict = None,
) -> pd.Series:
    """
    Calculate enhanced total score incorporating new data sources.

    Args:
        df: DataFrame with both traditional and enhanced data
        weights: Optional custom weights for each factor
                 Default: traditional=0.60, insider=0.15, earnings=0.15, financial=0.10

    Returns:
        Enhanced total score (0-100 scale)
    """
    if weights is None:
        weights = {
            "traditional": 0.60,  # Existing factors (quality, value, etc.)
            "insider": 0.15,
            "earnings": 0.15,
            "financial": 0.10,
        }

    # Validate weights sum to 1.0
    weight_sum = sum(weights.values())
    if not np.isclose(weight_sum, 1.0):
        LOGGER.warning(
            "Weights sum to %.3f, not 1.0. Normalizing weights.", weight_sum
        )
        weights = {k: v / weight_sum for k, v in weights.items()}

    # Start with traditional score (if exists)
    if "total_score" in df.columns:
        # Traditional score is already 0-100, scale by weight
        base_score = df["total_score"] * weights["traditional"]
    else:
        # No traditional score, use neutral 50
        LOGGER.warning("No 'total_score' column found, using neutral score")
        base_score = pd.Series(50.0, index=df.index) * weights["traditional"]

    # Calculate new factor scores (0-10 scale) and scale to 0-100 then apply weights
    insider_score = calculate_insider_score(df) * 10 * weights["insider"]
    earnings_score = calculate_earnings_score(df) * 10 * weights["earnings"]
    financial_score = calculate_financial_health_score(df) * 10 * weights["financial"]

    # Combine all scores
    enhanced_score = base_score + insider_score + earnings_score + financial_score

    # Ensure score is in valid range
    enhanced_score = enhanced_score.clip(0, 100)

    LOGGER.info(
        "Enhanced scoring complete. Mean score: %.2f, Median: %.2f",
        enhanced_score.mean(),
        enhanced_score.median(),
    )

    return enhanced_score


__all__ = [
    "calculate_insider_score",
    "calculate_earnings_score",
    "calculate_financial_health_score",
    "calculate_enhanced_total_score",
]
