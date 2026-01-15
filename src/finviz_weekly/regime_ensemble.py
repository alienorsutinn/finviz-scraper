"""
Regime-Aware Ensemble Weighting.

Adjusts model weights based on detected market regime:
- Bull market: Emphasize momentum, growth factors
- Bear market: Emphasize quality, value, defensive factors
- High volatility: Reduce position sizes, emphasize quality
- Low volatility: Normal positioning

Uses the regime detection module to determine current conditions.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)


@dataclass
class RegimeWeights:
    """Weight configuration for a specific regime."""

    regime_name: str

    # Horizon weights (how much to weight each horizon)
    horizon_weights: Dict[int, float] = field(default_factory=dict)

    # Factor category weights
    momentum_weight: float = 1.0
    value_weight: float = 1.0
    quality_weight: float = 1.0
    growth_weight: float = 1.0

    # Position sizing multiplier
    position_size_mult: float = 1.0

    # Sector tilts (positive = overweight, negative = underweight)
    sector_tilts: Dict[str, float] = field(default_factory=dict)


@dataclass
class RegimeEnsembleConfig:
    """Configuration for regime-aware ensemble."""

    # Regime definitions with their weights
    bull_weights: RegimeWeights = None
    bear_weights: RegimeWeights = None
    high_vol_weights: RegimeWeights = None
    low_vol_weights: RegimeWeights = None

    # Transition smoothing
    smoothing_days: int = 5  # Days to smooth regime transitions
    use_probability_weighting: bool = True  # Weight by regime probability

    # Default horizon weights (used as baseline)
    default_horizon_weights: Dict[int, float] = field(default_factory=dict)

    def __post_init__(self):
        # Default weights if not specified
        if self.default_horizon_weights is None or not self.default_horizon_weights:
            self.default_horizon_weights = {5: 0.2, 21: 0.5, 63: 0.3}

        if self.bull_weights is None:
            self.bull_weights = RegimeWeights(
                regime_name="bull",
                horizon_weights={5: 0.3, 21: 0.4, 63: 0.3},  # More short-term
                momentum_weight=1.3,  # Increase momentum
                value_weight=0.8,
                quality_weight=0.9,
                growth_weight=1.2,  # Increase growth
                position_size_mult=1.0,
                sector_tilts={
                    "Technology": 0.1,
                    "Consumer Cyclical": 0.1,
                    "Financial": 0.05,
                    "Utilities": -0.1,
                    "Consumer Defensive": -0.1,
                },
            )

        if self.bear_weights is None:
            self.bear_weights = RegimeWeights(
                regime_name="bear",
                horizon_weights={5: 0.15, 21: 0.35, 63: 0.5},  # More long-term
                momentum_weight=0.7,  # Reduce momentum
                value_weight=1.2,  # Increase value
                quality_weight=1.3,  # Increase quality
                growth_weight=0.8,
                position_size_mult=0.7,  # Reduce positions
                sector_tilts={
                    "Technology": -0.1,
                    "Consumer Cyclical": -0.15,
                    "Healthcare": 0.1,
                    "Utilities": 0.15,
                    "Consumer Defensive": 0.15,
                },
            )

        if self.high_vol_weights is None:
            self.high_vol_weights = RegimeWeights(
                regime_name="high_volatility",
                horizon_weights={5: 0.1, 21: 0.4, 63: 0.5},
                momentum_weight=0.6,  # Reduce momentum in vol
                value_weight=1.1,
                quality_weight=1.4,  # High quality in vol
                growth_weight=0.7,
                position_size_mult=0.5,  # Significantly reduce positions
                sector_tilts={
                    "Healthcare": 0.1,
                    "Utilities": 0.15,
                    "Consumer Defensive": 0.1,
                    "Technology": -0.1,
                },
            )

        if self.low_vol_weights is None:
            self.low_vol_weights = RegimeWeights(
                regime_name="low_volatility",
                horizon_weights={5: 0.25, 21: 0.45, 63: 0.3},
                momentum_weight=1.1,
                value_weight=1.0,
                quality_weight=1.0,
                growth_weight=1.1,
                position_size_mult=1.1,  # Slightly increase positions
                sector_tilts={},
            )


@dataclass
class RegimeState:
    """Current regime state with probabilities."""

    timestamp: str
    trend_regime: str  # "bull", "bear", "neutral"
    volatility_regime: str  # "high", "normal", "low"

    # Regime probabilities (for smooth transitions)
    bull_probability: float = 0.0
    bear_probability: float = 0.0
    high_vol_probability: float = 0.0

    # Market metrics
    spy_return_20d: float = 0.0
    spy_return_50d: float = 0.0
    vix_level: float = 0.0
    vix_percentile: float = 0.0


class RegimeAwareEnsemble:
    """
    Ensemble model that adjusts weights based on market regime.

    Combines predictions from multiple models/horizons with
    regime-dependent weighting to improve performance across
    different market conditions.
    """

    def __init__(self, config: Optional[RegimeEnsembleConfig] = None):
        """
        Initialize regime-aware ensemble.

        Args:
            config: Ensemble configuration
        """
        self.config = config or RegimeEnsembleConfig()
        self.regime_history: List[RegimeState] = []
        self.current_regime: Optional[RegimeState] = None

        LOGGER.info("Regime-aware ensemble initialized")

    def detect_regime(
        self,
        spy_prices: pd.Series,
        vix_prices: Optional[pd.Series] = None,
    ) -> RegimeState:
        """
        Detect current market regime.

        Args:
            spy_prices: SPY price series (index=date)
            vix_prices: Optional VIX price series

        Returns:
            RegimeState with current regime classification
        """
        # Calculate returns
        returns = spy_prices.pct_change()

        # 20-day and 50-day returns
        ret_20d = (spy_prices.iloc[-1] / spy_prices.iloc[-20] - 1) * 100 if len(spy_prices) >= 20 else 0
        ret_50d = (spy_prices.iloc[-1] / spy_prices.iloc[-50] - 1) * 100 if len(spy_prices) >= 50 else 0

        # Moving average trend
        sma_20 = spy_prices.rolling(20).mean().iloc[-1] if len(spy_prices) >= 20 else spy_prices.iloc[-1]
        sma_50 = spy_prices.rolling(50).mean().iloc[-1] if len(spy_prices) >= 50 else spy_prices.iloc[-1]
        sma_200 = spy_prices.rolling(200).mean().iloc[-1] if len(spy_prices) >= 200 else spy_prices.iloc[-1]

        current_price = spy_prices.iloc[-1]

        # Trend regime detection
        if current_price > sma_20 > sma_50 and ret_20d > 0:
            trend_regime = "bull"
            bull_prob = min(1.0, 0.5 + ret_20d / 20)  # Higher return = higher prob
            bear_prob = max(0.0, 0.5 - ret_20d / 20)
        elif current_price < sma_20 < sma_50 and ret_20d < 0:
            trend_regime = "bear"
            bear_prob = min(1.0, 0.5 - ret_20d / 20)
            bull_prob = max(0.0, 0.5 + ret_20d / 20)
        else:
            trend_regime = "neutral"
            bull_prob = 0.5
            bear_prob = 0.5

        # Volatility regime detection
        vix_level = 20.0  # Default
        vix_percentile = 50.0
        high_vol_prob = 0.0

        if vix_prices is not None and len(vix_prices) > 0:
            vix_level = vix_prices.iloc[-1]

            # Calculate VIX percentile over last year
            if len(vix_prices) >= 252:
                vix_percentile = (vix_prices.iloc[-252:] < vix_level).mean() * 100
            else:
                vix_percentile = (vix_prices < vix_level).mean() * 100

        # Alternatively, use realized volatility
        realized_vol = returns.rolling(20).std().iloc[-1] * np.sqrt(252) * 100 if len(returns) >= 20 else 15

        if vix_level > 25 or realized_vol > 25:
            volatility_regime = "high"
            high_vol_prob = min(1.0, (vix_level - 20) / 20)
        elif vix_level < 15 or realized_vol < 12:
            volatility_regime = "low"
            high_vol_prob = 0.0
        else:
            volatility_regime = "normal"
            high_vol_prob = 0.3

        state = RegimeState(
            timestamp=datetime.now().isoformat(),
            trend_regime=trend_regime,
            volatility_regime=volatility_regime,
            bull_probability=bull_prob,
            bear_probability=bear_prob,
            high_vol_probability=high_vol_prob,
            spy_return_20d=ret_20d,
            spy_return_50d=ret_50d,
            vix_level=vix_level,
            vix_percentile=vix_percentile,
        )

        self.current_regime = state
        self.regime_history.append(state)

        # Keep only recent history
        if len(self.regime_history) > 252:
            self.regime_history = self.regime_history[-252:]

        return state

    def get_regime_weights(
        self,
        regime: Optional[RegimeState] = None,
    ) -> RegimeWeights:
        """
        Get weights for current regime.

        Args:
            regime: Regime state (uses current if None)

        Returns:
            RegimeWeights for the regime
        """
        regime = regime or self.current_regime

        if regime is None:
            # Return default neutral weights
            return RegimeWeights(
                regime_name="default",
                horizon_weights=self.config.default_horizon_weights,
            )

        if self.config.use_probability_weighting:
            # Blend weights based on probabilities
            return self._blend_weights(regime)
        else:
            # Use discrete regime
            if regime.volatility_regime == "high":
                return self.config.high_vol_weights
            elif regime.trend_regime == "bull":
                return self.config.bull_weights
            elif regime.trend_regime == "bear":
                return self.config.bear_weights
            else:
                return self.config.low_vol_weights

    def _blend_weights(self, regime: RegimeState) -> RegimeWeights:
        """Blend weights based on regime probabilities."""

        # Get base weights
        bull_w = self.config.bull_weights
        bear_w = self.config.bear_weights
        high_vol_w = self.config.high_vol_weights

        # Probability weighting
        p_bull = regime.bull_probability * (1 - regime.high_vol_probability)
        p_bear = regime.bear_probability * (1 - regime.high_vol_probability)
        p_high_vol = regime.high_vol_probability

        # Normalize
        total = p_bull + p_bear + p_high_vol
        if total > 0:
            p_bull /= total
            p_bear /= total
            p_high_vol /= total
        else:
            p_bull = p_bear = p_high_vol = 1/3

        # Blend horizon weights
        blended_horizon = {}
        for horizon in self.config.default_horizon_weights.keys():
            blended_horizon[horizon] = (
                p_bull * bull_w.horizon_weights.get(horizon, 0.33) +
                p_bear * bear_w.horizon_weights.get(horizon, 0.33) +
                p_high_vol * high_vol_w.horizon_weights.get(horizon, 0.33)
            )

        # Blend factor weights
        momentum = p_bull * bull_w.momentum_weight + p_bear * bear_w.momentum_weight + p_high_vol * high_vol_w.momentum_weight
        value = p_bull * bull_w.value_weight + p_bear * bear_w.value_weight + p_high_vol * high_vol_w.value_weight
        quality = p_bull * bull_w.quality_weight + p_bear * bear_w.quality_weight + p_high_vol * high_vol_w.quality_weight
        growth = p_bull * bull_w.growth_weight + p_bear * bear_w.growth_weight + p_high_vol * high_vol_w.growth_weight

        # Blend position size
        position_mult = (
            p_bull * bull_w.position_size_mult +
            p_bear * bear_w.position_size_mult +
            p_high_vol * high_vol_w.position_size_mult
        )

        # Blend sector tilts
        all_sectors = set()
        for w in [bull_w, bear_w, high_vol_w]:
            all_sectors.update(w.sector_tilts.keys())

        blended_sectors = {}
        for sector in all_sectors:
            blended_sectors[sector] = (
                p_bull * bull_w.sector_tilts.get(sector, 0) +
                p_bear * bear_w.sector_tilts.get(sector, 0) +
                p_high_vol * high_vol_w.sector_tilts.get(sector, 0)
            )

        return RegimeWeights(
            regime_name=f"blended_{regime.trend_regime}_{regime.volatility_regime}",
            horizon_weights=blended_horizon,
            momentum_weight=momentum,
            value_weight=value,
            quality_weight=quality,
            growth_weight=growth,
            position_size_mult=position_mult,
            sector_tilts=blended_sectors,
        )

    def combine_predictions(
        self,
        horizon_predictions: Dict[int, np.ndarray],
        weights: Optional[RegimeWeights] = None,
    ) -> np.ndarray:
        """
        Combine horizon predictions using regime weights.

        Args:
            horizon_predictions: Dict of horizon -> predictions array
            weights: Regime weights (uses current if None)

        Returns:
            Combined prediction array
        """
        weights = weights or self.get_regime_weights()

        if not horizon_predictions:
            return np.array([])

        # Get array length from first prediction
        first_pred = next(iter(horizon_predictions.values()))
        combined = np.zeros(len(first_pred))
        total_weight = 0

        for horizon, pred in horizon_predictions.items():
            weight = weights.horizon_weights.get(horizon, 0)
            combined += pred * weight
            total_weight += weight

        if total_weight > 0:
            combined /= total_weight

        return combined

    def adjust_factor_scores(
        self,
        scores: pd.DataFrame,
        weights: Optional[RegimeWeights] = None,
    ) -> pd.DataFrame:
        """
        Adjust factor scores based on regime weights.

        Args:
            scores: DataFrame with factor score columns
            weights: Regime weights (uses current if None)

        Returns:
            DataFrame with adjusted scores
        """
        weights = weights or self.get_regime_weights()
        result = scores.copy()

        # Identify and adjust factor columns
        factor_adjustments = {
            "momentum": weights.momentum_weight,
            "value": weights.value_weight,
            "quality": weights.quality_weight,
            "growth": weights.growth_weight,
        }

        for factor_type, adjustment in factor_adjustments.items():
            # Find columns containing this factor type
            factor_cols = [c for c in result.columns if factor_type in c.lower()]
            for col in factor_cols:
                result[col] = result[col] * adjustment

        return result

    def apply_sector_tilts(
        self,
        data: pd.DataFrame,
        sector_col: str = "sector",
        score_col: str = "score",
        weights: Optional[RegimeWeights] = None,
    ) -> pd.DataFrame:
        """
        Apply sector tilts to scores.

        Args:
            data: DataFrame with sector and score columns
            sector_col: Name of sector column
            score_col: Name of score column
            weights: Regime weights (uses current if None)

        Returns:
            DataFrame with sector-adjusted scores
        """
        weights = weights or self.get_regime_weights()
        result = data.copy()

        if sector_col not in result.columns or score_col not in result.columns:
            return result

        # Apply tilts
        for sector, tilt in weights.sector_tilts.items():
            mask = result[sector_col] == sector
            # Tilt is additive to percentile-based scores
            result.loc[mask, score_col] = result.loc[mask, score_col] * (1 + tilt)

        return result

    def get_position_size_multiplier(
        self,
        weights: Optional[RegimeWeights] = None,
    ) -> float:
        """
        Get position size multiplier for current regime.

        Args:
            weights: Regime weights (uses current if None)

        Returns:
            Position size multiplier (0.5 to 1.1 typically)
        """
        weights = weights or self.get_regime_weights()
        return weights.position_size_mult

    def get_regime_summary(self) -> Dict[str, Any]:
        """Get summary of current regime state."""
        if self.current_regime is None:
            return {"status": "no_regime_detected"}

        regime = self.current_regime
        weights = self.get_regime_weights()

        return {
            "timestamp": regime.timestamp,
            "trend_regime": regime.trend_regime,
            "volatility_regime": regime.volatility_regime,
            "bull_probability": round(regime.bull_probability, 2),
            "bear_probability": round(regime.bear_probability, 2),
            "high_vol_probability": round(regime.high_vol_probability, 2),
            "spy_return_20d": round(regime.spy_return_20d, 2),
            "vix_level": round(regime.vix_level, 2),
            "position_size_mult": round(weights.position_size_mult, 2),
            "recommended_weights": weights.horizon_weights,
            "factor_weights": {
                "momentum": round(weights.momentum_weight, 2),
                "value": round(weights.value_weight, 2),
                "quality": round(weights.quality_weight, 2),
                "growth": round(weights.growth_weight, 2),
            },
        }


def create_regime_aware_scorer(
    base_scores: pd.DataFrame,
    spy_prices: pd.Series,
    vix_prices: Optional[pd.Series] = None,
    config: Optional[RegimeEnsembleConfig] = None,
) -> pd.DataFrame:
    """
    Convenience function to create regime-adjusted scores.

    Args:
        base_scores: DataFrame with base factor scores
        spy_prices: SPY price series
        vix_prices: Optional VIX series
        config: Optional ensemble configuration

    Returns:
        DataFrame with regime-adjusted scores
    """
    ensemble = RegimeAwareEnsemble(config)

    # Detect regime
    regime = ensemble.detect_regime(spy_prices, vix_prices)

    LOGGER.info(f"Detected regime: {regime.trend_regime} / {regime.volatility_regime}")

    # Adjust scores
    adjusted = ensemble.adjust_factor_scores(base_scores)

    # Apply sector tilts if sector column exists
    if "sector" in adjusted.columns and "score" in adjusted.columns:
        adjusted = ensemble.apply_sector_tilts(adjusted)

    return adjusted
