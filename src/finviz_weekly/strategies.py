"""
Pre-built Strategy Library

Provides ready-to-use trading strategies:
- Momentum strategies
- Value strategies
- Quality strategies
- Multi-factor strategies
- Mean reversion strategies
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)


class StrategyType(Enum):
    """Types of trading strategies."""
    MOMENTUM = "momentum"
    VALUE = "value"
    QUALITY = "quality"
    GROWTH = "growth"
    MULTI_FACTOR = "multi_factor"
    MEAN_REVERSION = "mean_reversion"
    TREND_FOLLOWING = "trend_following"
    STATISTICAL_ARB = "statistical_arbitrage"


class RebalanceFrequency(Enum):
    """Rebalancing frequencies."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"


@dataclass
class StrategyConfig:
    """Base configuration for strategies."""
    name: str
    strategy_type: StrategyType
    rebalance_frequency: RebalanceFrequency = RebalanceFrequency.MONTHLY

    # Universe filters
    min_market_cap: float = 1e9  # $1B minimum
    min_price: float = 5.0
    min_volume: float = 500000
    max_stocks: int = 50

    # Position sizing
    equal_weight: bool = True
    max_position_weight: float = 0.10

    # Risk management
    stop_loss_pct: Optional[float] = None
    take_profit_pct: Optional[float] = None
    max_sector_weight: float = 0.30


@dataclass
class Signal:
    """Trading signal."""
    ticker: str
    direction: int  # 1 = long, -1 = short, 0 = neutral
    strength: float  # 0-100
    reason: str
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'ticker': self.ticker,
            'direction': self.direction,
            'strength': self.strength,
            'reason': self.reason,
            'timestamp': self.timestamp.isoformat(),
        }


@dataclass
class StrategyResult:
    """Result from strategy execution."""
    strategy_name: str
    signals: List[Signal]
    selected_tickers: List[str]
    weights: Dict[str, float]
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dataframe(self) -> pd.DataFrame:
        """Convert to DataFrame."""
        return pd.DataFrame([s.to_dict() for s in self.signals])


class BaseStrategy(ABC):
    """Abstract base class for trading strategies."""

    def __init__(self, config: StrategyConfig):
        self.config = config

    @abstractmethod
    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        """Generate trading signals from data."""
        pass

    @abstractmethod
    def select_stocks(self, signals: List[Signal]) -> List[str]:
        """Select stocks based on signals."""
        pass

    def calculate_weights(self, tickers: List[str]) -> Dict[str, float]:
        """Calculate portfolio weights."""
        if not tickers:
            return {}

        if self.config.equal_weight:
            weight = min(1.0 / len(tickers), self.config.max_position_weight)
            return {t: weight for t in tickers}

        # Default to equal weight
        return {t: 1.0 / len(tickers) for t in tickers}

    def run(self, data: pd.DataFrame) -> StrategyResult:
        """Run the complete strategy."""
        # Apply universe filters
        filtered_data = self._apply_filters(data)

        # Generate signals
        signals = self.generate_signals(filtered_data)

        # Select stocks
        selected = self.select_stocks(signals)

        # Calculate weights
        weights = self.calculate_weights(selected)

        return StrategyResult(
            strategy_name=self.config.name,
            signals=signals,
            selected_tickers=selected,
            weights=weights,
        )

    def _apply_filters(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply universe filters."""
        df = data.copy()

        if 'market_cap' in df.columns:
            df = df[df['market_cap'] >= self.config.min_market_cap]

        if 'price' in df.columns:
            df = df[df['price'] >= self.config.min_price]

        if 'volume' in df.columns:
            df = df[df['volume'] >= self.config.min_volume]

        return df


# =============================================================================
# Momentum Strategies
# =============================================================================

class MomentumStrategy(BaseStrategy):
    """
    Classic price momentum strategy.

    Buys stocks with strong recent performance.
    """

    def __init__(
        self,
        lookback_days: int = 252,
        skip_days: int = 21,
        **kwargs,
    ):
        config = StrategyConfig(
            name="Momentum",
            strategy_type=StrategyType.MOMENTUM,
            **kwargs,
        )
        super().__init__(config)
        self.lookback_days = lookback_days
        self.skip_days = skip_days

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        signals = []

        # Check for momentum columns
        momentum_col = None
        for col in ['perf_year', 'perf_half_y', 'momentum_score', 'return_12m']:
            if col in data.columns:
                momentum_col = col
                break

        if momentum_col is None:
            LOGGER.warning("No momentum column found")
            return signals

        for _, row in data.iterrows():
            ticker = row.get('ticker', '')
            momentum = row.get(momentum_col, 0)

            if pd.isna(momentum):
                continue

            # Normalize to 0-100 scale
            strength = min(100, max(0, 50 + momentum * 100))

            direction = 1 if momentum > 0 else 0

            signals.append(Signal(
                ticker=ticker,
                direction=direction,
                strength=strength,
                reason=f"12m momentum: {momentum:.1%}",
            ))

        return signals

    def select_stocks(self, signals: List[Signal]) -> List[str]:
        # Sort by strength, take top N
        sorted_signals = sorted(signals, key=lambda s: s.strength, reverse=True)
        return [s.ticker for s in sorted_signals[:self.config.max_stocks] if s.direction > 0]


class DualMomentumStrategy(BaseStrategy):
    """
    Dual momentum strategy (absolute + relative).

    Combines absolute momentum (trend) with relative momentum (cross-sectional).
    """

    def __init__(self, **kwargs):
        config = StrategyConfig(
            name="Dual Momentum",
            strategy_type=StrategyType.MOMENTUM,
            **kwargs,
        )
        super().__init__(config)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        signals = []

        for _, row in data.iterrows():
            ticker = row.get('ticker', '')

            # Absolute momentum (vs cash/bonds)
            perf_year = row.get('perf_year', 0) or 0
            abs_momentum = perf_year > 0.02  # Beat cash rate

            # Relative momentum (vs universe median)
            rel_momentum = row.get('momentum_score', 50) or 50

            if abs_momentum and rel_momentum > 60:
                direction = 1
                strength = rel_momentum
                reason = f"Dual momentum: abs={perf_year:.1%}, rel={rel_momentum:.0f}"
            else:
                direction = 0
                strength = rel_momentum
                reason = "Failed dual momentum test"

            signals.append(Signal(
                ticker=ticker,
                direction=direction,
                strength=strength,
                reason=reason,
            ))

        return signals

    def select_stocks(self, signals: List[Signal]) -> List[str]:
        sorted_signals = sorted(signals, key=lambda s: s.strength, reverse=True)
        return [s.ticker for s in sorted_signals[:self.config.max_stocks] if s.direction > 0]


# =============================================================================
# Value Strategies
# =============================================================================

class ValueStrategy(BaseStrategy):
    """
    Classic value investing strategy.

    Buys stocks trading at discount to intrinsic value.
    """

    def __init__(
        self,
        pe_threshold: float = 20,
        pb_threshold: float = 3,
        **kwargs,
    ):
        config = StrategyConfig(
            name="Value",
            strategy_type=StrategyType.VALUE,
            **kwargs,
        )
        super().__init__(config)
        self.pe_threshold = pe_threshold
        self.pb_threshold = pb_threshold

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        signals = []

        for _, row in data.iterrows():
            ticker = row.get('ticker', '')

            pe = row.get('pe', float('inf')) or float('inf')
            pb = row.get('pb', float('inf')) or float('inf')
            forward_pe = row.get('forward_pe', pe) or pe

            # Value score (lower is better for these metrics)
            value_score = 0
            reasons = []

            if 0 < pe < self.pe_threshold:
                value_score += 30
                reasons.append(f"PE={pe:.1f}")

            if 0 < pb < self.pb_threshold:
                value_score += 30
                reasons.append(f"PB={pb:.1f}")

            if 0 < forward_pe < pe:
                value_score += 20
                reasons.append(f"FwdPE={forward_pe:.1f}")

            # Use value_score column if available
            if 'value_score' in row:
                value_score = row['value_score'] or 0

            direction = 1 if value_score > 50 else 0

            signals.append(Signal(
                ticker=ticker,
                direction=direction,
                strength=value_score,
                reason=", ".join(reasons) if reasons else "Low value score",
            ))

        return signals

    def select_stocks(self, signals: List[Signal]) -> List[str]:
        sorted_signals = sorted(signals, key=lambda s: s.strength, reverse=True)
        return [s.ticker for s in sorted_signals[:self.config.max_stocks] if s.direction > 0]


class DeepValueStrategy(BaseStrategy):
    """
    Deep value strategy focusing on distressed/turnaround situations.
    """

    def __init__(self, **kwargs):
        config = StrategyConfig(
            name="Deep Value",
            strategy_type=StrategyType.VALUE,
            min_market_cap=500e6,  # Allow smaller caps
            **kwargs,
        )
        super().__init__(config)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        signals = []

        for _, row in data.iterrows():
            ticker = row.get('ticker', '')

            # Deep value criteria
            pe = row.get('pe', float('inf')) or float('inf')
            pb = row.get('pb', float('inf')) or float('inf')
            ps = row.get('ps', float('inf')) or float('inf')
            perf_year = row.get('perf_year', 0) or 0

            score = 0
            reasons = []

            # Very low valuations
            if 0 < pe < 10:
                score += 25
                reasons.append(f"Very low PE: {pe:.1f}")

            if 0 < pb < 1:
                score += 25
                reasons.append(f"Below book: PB={pb:.2f}")

            if 0 < ps < 0.5:
                score += 25
                reasons.append(f"Very low PS: {ps:.2f}")

            # Contrarian - beaten down stocks
            if perf_year < -0.30:
                score += 15
                reasons.append(f"Down {perf_year:.0%} YTD")

            # Turnaround potential
            if row.get('insider_score', 0) and row['insider_score'] > 70:
                score += 10
                reasons.append("Insider buying")

            direction = 1 if score >= 50 else 0

            signals.append(Signal(
                ticker=ticker,
                direction=direction,
                strength=score,
                reason="; ".join(reasons) if reasons else "Not deep value",
            ))

        return signals

    def select_stocks(self, signals: List[Signal]) -> List[str]:
        sorted_signals = sorted(signals, key=lambda s: s.strength, reverse=True)
        return [s.ticker for s in sorted_signals[:self.config.max_stocks] if s.direction > 0]


# =============================================================================
# Quality Strategies
# =============================================================================

class QualityStrategy(BaseStrategy):
    """
    Quality investing strategy.

    Buys high-quality companies with strong fundamentals.
    """

    def __init__(
        self,
        min_roe: float = 15,
        min_margin: float = 10,
        **kwargs,
    ):
        config = StrategyConfig(
            name="Quality",
            strategy_type=StrategyType.QUALITY,
            **kwargs,
        )
        super().__init__(config)
        self.min_roe = min_roe
        self.min_margin = min_margin

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        signals = []

        for _, row in data.iterrows():
            ticker = row.get('ticker', '')

            roe = row.get('roe', 0) or 0
            roa = row.get('roa', 0) or 0
            profit_margin = row.get('profit_margin', 0) or 0
            current_ratio = row.get('current_ratio', 0) or 0
            debt_equity = row.get('debt_equity', float('inf')) or float('inf')

            score = 0
            reasons = []

            # Profitability
            if roe > self.min_roe:
                score += 25
                reasons.append(f"ROE={roe:.1f}%")

            if roa > 10:
                score += 15
                reasons.append(f"ROA={roa:.1f}%")

            if profit_margin > self.min_margin:
                score += 20
                reasons.append(f"Margin={profit_margin:.1f}%")

            # Financial health
            if current_ratio > 1.5:
                score += 15
                reasons.append(f"Current={current_ratio:.1f}")

            if debt_equity < 0.5:
                score += 15
                reasons.append(f"D/E={debt_equity:.2f}")

            # Use quality_score if available
            if 'quality_score' in row and row['quality_score']:
                score = row['quality_score']

            direction = 1 if score > 50 else 0

            signals.append(Signal(
                ticker=ticker,
                direction=direction,
                strength=score,
                reason=", ".join(reasons) if reasons else "Low quality score",
            ))

        return signals

    def select_stocks(self, signals: List[Signal]) -> List[str]:
        sorted_signals = sorted(signals, key=lambda s: s.strength, reverse=True)
        return [s.ticker for s in sorted_signals[:self.config.max_stocks] if s.direction > 0]


# =============================================================================
# Multi-Factor Strategies
# =============================================================================

class MultiFactorStrategy(BaseStrategy):
    """
    Multi-factor strategy combining multiple factors.
    """

    def __init__(
        self,
        factor_weights: Optional[Dict[str, float]] = None,
        **kwargs,
    ):
        config = StrategyConfig(
            name="Multi-Factor",
            strategy_type=StrategyType.MULTI_FACTOR,
            **kwargs,
        )
        super().__init__(config)

        self.factor_weights = factor_weights or {
            'momentum': 0.25,
            'value': 0.25,
            'quality': 0.25,
            'growth': 0.25,
        }

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        signals = []

        for _, row in data.iterrows():
            ticker = row.get('ticker', '')

            # Collect factor scores
            factors = {}

            if 'momentum_score' in row:
                factors['momentum'] = row['momentum_score'] or 50

            if 'value_score' in row:
                factors['value'] = row['value_score'] or 50

            if 'quality_score' in row:
                factors['quality'] = row['quality_score'] or 50

            if 'growth_score' in row:
                factors['growth'] = row['growth_score'] or 50

            # Calculate composite score
            if factors:
                total_weight = sum(
                    self.factor_weights.get(f, 0) for f in factors
                )
                if total_weight > 0:
                    composite = sum(
                        factors[f] * self.factor_weights.get(f, 0)
                        for f in factors
                    ) / total_weight
                else:
                    composite = 50
            else:
                composite = row.get('total_score', 50) or 50

            direction = 1 if composite > 60 else 0

            signals.append(Signal(
                ticker=ticker,
                direction=direction,
                strength=composite,
                reason=f"Composite: {composite:.0f}",
            ))

        return signals

    def select_stocks(self, signals: List[Signal]) -> List[str]:
        sorted_signals = sorted(signals, key=lambda s: s.strength, reverse=True)
        return [s.ticker for s in sorted_signals[:self.config.max_stocks] if s.direction > 0]


class QMJStrategy(BaseStrategy):
    """
    Quality Minus Junk (QMJ) strategy.

    Based on AQR's QMJ factor research.
    """

    def __init__(self, **kwargs):
        config = StrategyConfig(
            name="Quality Minus Junk",
            strategy_type=StrategyType.MULTI_FACTOR,
            **kwargs,
        )
        super().__init__(config)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        signals = []

        for _, row in data.iterrows():
            ticker = row.get('ticker', '')

            # QMJ components
            # Profitability
            roe = row.get('roe', 0) or 0
            roa = row.get('roa', 0) or 0
            gross_margin = row.get('gross_margin', 0) or 0

            # Growth
            eps_growth = row.get('eps_growth_next_y', 0) or 0

            # Safety
            beta = row.get('beta', 1) or 1
            debt_equity = row.get('debt_equity', 1) or 1

            # Calculate QMJ score
            profitability_score = min(100, (roe + roa + gross_margin) / 3 * 2)
            growth_score = min(100, 50 + eps_growth * 100)
            safety_score = min(100, 50 + (1 - beta) * 25 + (1 - min(debt_equity, 2) / 2) * 25)

            qmj_score = (profitability_score + growth_score + safety_score) / 3

            direction = 1 if qmj_score > 55 else 0

            signals.append(Signal(
                ticker=ticker,
                direction=direction,
                strength=qmj_score,
                reason=f"QMJ: P={profitability_score:.0f}, G={growth_score:.0f}, S={safety_score:.0f}",
            ))

        return signals

    def select_stocks(self, signals: List[Signal]) -> List[str]:
        sorted_signals = sorted(signals, key=lambda s: s.strength, reverse=True)
        return [s.ticker for s in sorted_signals[:self.config.max_stocks] if s.direction > 0]


# =============================================================================
# Mean Reversion Strategies
# =============================================================================

class MeanReversionStrategy(BaseStrategy):
    """
    Mean reversion strategy.

    Buys oversold stocks expecting reversion to mean.
    """

    def __init__(
        self,
        rsi_oversold: float = 30,
        rsi_overbought: float = 70,
        **kwargs,
    ):
        config = StrategyConfig(
            name="Mean Reversion",
            strategy_type=StrategyType.MEAN_REVERSION,
            **kwargs,
        )
        super().__init__(config)
        self.rsi_oversold = rsi_oversold
        self.rsi_overbought = rsi_overbought

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        signals = []

        for _, row in data.iterrows():
            ticker = row.get('ticker', '')

            rsi = row.get('rsi', 50) or 50
            sma20 = row.get('sma20', 0) or 0
            sma50 = row.get('sma50', 0) or 0
            price = row.get('price', 0) or 0

            score = 50
            reasons = []
            direction = 0

            # RSI signals
            if rsi < self.rsi_oversold:
                score += 25
                direction = 1
                reasons.append(f"RSI oversold: {rsi:.0f}")
            elif rsi > self.rsi_overbought:
                score -= 25
                direction = -1
                reasons.append(f"RSI overbought: {rsi:.0f}")

            # Price vs moving averages
            if price > 0 and sma20 > 0:
                pct_below_sma20 = (sma20 - price) / sma20
                if pct_below_sma20 > 0.10:
                    score += 15
                    direction = 1
                    reasons.append(f"{pct_below_sma20:.0%} below SMA20")

            signals.append(Signal(
                ticker=ticker,
                direction=max(0, direction),  # Long only
                strength=score,
                reason=", ".join(reasons) if reasons else "No mean reversion signal",
            ))

        return signals

    def select_stocks(self, signals: List[Signal]) -> List[str]:
        sorted_signals = sorted(signals, key=lambda s: s.strength, reverse=True)
        return [s.ticker for s in sorted_signals[:self.config.max_stocks] if s.direction > 0]


# =============================================================================
# Strategy Factory
# =============================================================================

STRATEGY_REGISTRY: Dict[str, type] = {
    'momentum': MomentumStrategy,
    'dual_momentum': DualMomentumStrategy,
    'value': ValueStrategy,
    'deep_value': DeepValueStrategy,
    'quality': QualityStrategy,
    'multi_factor': MultiFactorStrategy,
    'qmj': QMJStrategy,
    'mean_reversion': MeanReversionStrategy,
}


def get_strategy(name: str, **kwargs) -> BaseStrategy:
    """
    Get strategy by name.

    Args:
        name: Strategy name
        **kwargs: Strategy configuration

    Returns:
        Strategy instance
    """
    strategy_class = STRATEGY_REGISTRY.get(name.lower())
    if strategy_class is None:
        available = ", ".join(STRATEGY_REGISTRY.keys())
        raise ValueError(f"Unknown strategy: {name}. Available: {available}")

    return strategy_class(**kwargs)


def list_strategies() -> List[Dict[str, str]]:
    """List all available strategies."""
    return [
        {
            'name': name,
            'type': cls({} if name == 'momentum' else {}).config.strategy_type.value
            if hasattr(cls, '__init__') else 'unknown',
        }
        for name, cls in STRATEGY_REGISTRY.items()
    ]


def run_all_strategies(data: pd.DataFrame) -> Dict[str, StrategyResult]:
    """Run all strategies on data."""
    results = {}

    for name in STRATEGY_REGISTRY:
        try:
            strategy = get_strategy(name)
            results[name] = strategy.run(data)
        except Exception as e:
            LOGGER.warning(f"Strategy {name} failed: {e}")

    return results
