"""
Automated Portfolio Rebalancing Engine

Features:
- Multiple rebalancing strategies
- Transaction cost optimization
- Tax-aware rebalancing
- Drift monitoring
- Trade generation
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)


class RebalanceStrategy(Enum):
    """Rebalancing strategies."""
    CALENDAR = "calendar"  # Fixed schedule
    THRESHOLD = "threshold"  # Drift-based
    HYBRID = "hybrid"  # Calendar + threshold
    TAX_AWARE = "tax_aware"  # Minimize tax impact


class RebalanceFrequency(Enum):
    """Rebalancing frequencies for calendar strategy."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUALLY = "annually"


@dataclass
class RebalanceConfig:
    """Configuration for rebalancing."""
    strategy: RebalanceStrategy = RebalanceStrategy.THRESHOLD

    # Calendar settings
    frequency: RebalanceFrequency = RebalanceFrequency.MONTHLY

    # Threshold settings
    absolute_threshold: float = 0.05  # 5% absolute drift
    relative_threshold: float = 0.25  # 25% relative drift

    # Cost settings
    transaction_cost_bps: float = 10.0
    min_trade_value: float = 100.0
    tax_rate_short_term: float = 0.35
    tax_rate_long_term: float = 0.15

    # Constraints
    max_turnover: float = 0.25  # Max 25% turnover per rebalance
    max_single_trade_pct: float = 0.10  # Max 10% of portfolio per trade

    # Cash buffer
    cash_buffer_pct: float = 0.02  # Keep 2% in cash


@dataclass
class PortfolioPosition:
    """Current portfolio position."""
    ticker: str
    shares: float
    current_price: float
    cost_basis: float
    purchase_date: datetime
    sector: Optional[str] = None

    @property
    def market_value(self) -> float:
        return self.shares * self.current_price

    @property
    def unrealized_gain(self) -> float:
        return (self.current_price - self.cost_basis) * self.shares

    @property
    def unrealized_gain_pct(self) -> float:
        if self.cost_basis > 0:
            return (self.current_price - self.cost_basis) / self.cost_basis
        return 0.0

    @property
    def holding_period_days(self) -> int:
        return (datetime.now() - self.purchase_date).days

    @property
    def is_long_term(self) -> bool:
        return self.holding_period_days > 365


@dataclass
class Trade:
    """Proposed trade."""
    ticker: str
    action: str  # 'buy' or 'sell'
    shares: float
    estimated_price: float
    estimated_value: float
    reason: str

    # Tax impact (for sells)
    tax_impact: float = 0.0
    is_long_term: bool = False

    # Priority for execution
    priority: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'ticker': self.ticker,
            'action': self.action,
            'shares': self.shares,
            'estimated_price': self.estimated_price,
            'estimated_value': self.estimated_value,
            'reason': self.reason,
            'tax_impact': self.tax_impact,
        }


@dataclass
class RebalanceResult:
    """Result from rebalancing analysis."""
    needs_rebalance: bool
    trigger_reason: str
    trades: List[Trade]

    # Metrics
    total_turnover: float = 0.0
    estimated_transaction_cost: float = 0.0
    estimated_tax_impact: float = 0.0

    # Portfolio drift
    max_absolute_drift: float = 0.0
    max_relative_drift: float = 0.0
    drift_by_asset: Dict[str, float] = field(default_factory=dict)

    # Summary
    buys_count: int = 0
    sells_count: int = 0
    buys_value: float = 0.0
    sells_value: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'needs_rebalance': self.needs_rebalance,
            'trigger_reason': self.trigger_reason,
            'trades': [t.to_dict() for t in self.trades],
            'total_turnover': self.total_turnover,
            'estimated_transaction_cost': self.estimated_transaction_cost,
            'estimated_tax_impact': self.estimated_tax_impact,
            'max_absolute_drift': self.max_absolute_drift,
            'max_relative_drift': self.max_relative_drift,
            'buys_count': self.buys_count,
            'sells_count': self.sells_count,
        }


class DriftCalculator:
    """Calculate portfolio drift from targets."""

    def __init__(
        self,
        positions: List[PortfolioPosition],
        target_weights: Dict[str, float],
    ):
        self.positions = positions
        self.target_weights = target_weights

        # Calculate total portfolio value
        self.total_value = sum(p.market_value for p in positions)

        # Current weights
        self.current_weights = {
            p.ticker: p.market_value / self.total_value if self.total_value > 0 else 0
            for p in positions
        }

    def calculate_drift(self) -> Dict[str, Dict[str, float]]:
        """
        Calculate drift for all assets.

        Returns dict of ticker -> {absolute_drift, relative_drift, target, current}
        """
        all_tickers = set(self.current_weights.keys()) | set(self.target_weights.keys())
        drift = {}

        for ticker in all_tickers:
            current = self.current_weights.get(ticker, 0)
            target = self.target_weights.get(ticker, 0)

            absolute_drift = current - target
            relative_drift = (
                absolute_drift / target if target > 0
                else (1.0 if current > 0 else 0)
            )

            drift[ticker] = {
                'current_weight': current,
                'target_weight': target,
                'absolute_drift': absolute_drift,
                'relative_drift': relative_drift,
            }

        return drift

    def get_max_drift(self) -> Tuple[float, float]:
        """Get maximum absolute and relative drift."""
        drift = self.calculate_drift()

        max_absolute = max(
            abs(d['absolute_drift']) for d in drift.values()
        )
        max_relative = max(
            abs(d['relative_drift']) for d in drift.values()
        )

        return max_absolute, max_relative


class Rebalancer:
    """Main rebalancing engine."""

    def __init__(self, config: Optional[RebalanceConfig] = None):
        self.config = config or RebalanceConfig()
        self.last_rebalance_date: Optional[datetime] = None

    def analyze(
        self,
        positions: List[PortfolioPosition],
        target_weights: Dict[str, float],
        cash_available: float = 0.0,
    ) -> RebalanceResult:
        """
        Analyze portfolio and determine if rebalancing is needed.

        Args:
            positions: Current portfolio positions
            target_weights: Target allocation weights
            cash_available: Cash available for purchases

        Returns:
            RebalanceResult
        """
        # Calculate drift
        drift_calc = DriftCalculator(positions, target_weights)
        drift = drift_calc.calculate_drift()
        max_abs, max_rel = drift_calc.get_max_drift()

        # Check if rebalancing is needed
        needs_rebalance, trigger_reason = self._check_trigger(max_abs, max_rel)

        if not needs_rebalance:
            return RebalanceResult(
                needs_rebalance=False,
                trigger_reason=trigger_reason,
                trades=[],
                max_absolute_drift=max_abs,
                max_relative_drift=max_rel,
                drift_by_asset={t: d['absolute_drift'] for t, d in drift.items()},
            )

        # Generate trades
        trades = self._generate_trades(
            positions,
            target_weights,
            drift,
            drift_calc.total_value,
            cash_available,
        )

        # Calculate metrics
        buys = [t for t in trades if t.action == 'buy']
        sells = [t for t in trades if t.action == 'sell']

        buys_value = sum(t.estimated_value for t in buys)
        sells_value = sum(t.estimated_value for t in sells)

        total_turnover = (buys_value + sells_value) / (2 * drift_calc.total_value) \
            if drift_calc.total_value > 0 else 0

        transaction_cost = (buys_value + sells_value) * (self.config.transaction_cost_bps / 10000)
        tax_impact = sum(t.tax_impact for t in sells)

        return RebalanceResult(
            needs_rebalance=True,
            trigger_reason=trigger_reason,
            trades=trades,
            total_turnover=total_turnover,
            estimated_transaction_cost=transaction_cost,
            estimated_tax_impact=tax_impact,
            max_absolute_drift=max_abs,
            max_relative_drift=max_rel,
            drift_by_asset={t: d['absolute_drift'] for t, d in drift.items()},
            buys_count=len(buys),
            sells_count=len(sells),
            buys_value=buys_value,
            sells_value=sells_value,
        )

    def _check_trigger(
        self,
        max_abs_drift: float,
        max_rel_drift: float,
    ) -> Tuple[bool, str]:
        """Check if rebalancing should be triggered."""
        strategy = self.config.strategy

        if strategy == RebalanceStrategy.THRESHOLD:
            if max_abs_drift >= self.config.absolute_threshold:
                return True, f"Absolute drift {max_abs_drift:.1%} exceeds threshold"
            if max_rel_drift >= self.config.relative_threshold:
                return True, f"Relative drift {max_rel_drift:.1%} exceeds threshold"
            return False, "Drift within acceptable range"

        elif strategy == RebalanceStrategy.CALENDAR:
            if self.last_rebalance_date is None:
                return True, "Initial rebalance"

            days_since = (datetime.now() - self.last_rebalance_date).days
            freq_days = {
                RebalanceFrequency.DAILY: 1,
                RebalanceFrequency.WEEKLY: 7,
                RebalanceFrequency.MONTHLY: 30,
                RebalanceFrequency.QUARTERLY: 91,
                RebalanceFrequency.ANNUALLY: 365,
            }
            threshold_days = freq_days.get(self.config.frequency, 30)

            if days_since >= threshold_days:
                return True, f"Calendar trigger: {days_since} days since last rebalance"
            return False, f"Calendar not triggered: {days_since}/{threshold_days} days"

        elif strategy == RebalanceStrategy.HYBRID:
            # Check both calendar and threshold
            if max_abs_drift >= self.config.absolute_threshold:
                return True, f"Threshold trigger: {max_abs_drift:.1%} drift"
            if self.last_rebalance_date:
                days_since = (datetime.now() - self.last_rebalance_date).days
                if days_since >= 30:  # Monthly minimum
                    return True, f"Calendar trigger: {days_since} days"
            return False, "No trigger condition met"

        return False, "Unknown strategy"

    def _generate_trades(
        self,
        positions: List[PortfolioPosition],
        target_weights: Dict[str, float],
        drift: Dict[str, Dict[str, float]],
        portfolio_value: float,
        cash_available: float,
    ) -> List[Trade]:
        """Generate trades to rebalance portfolio."""
        trades = []
        positions_map = {p.ticker: p for p in positions}

        # Sort by drift magnitude (largest first)
        sorted_drift = sorted(
            drift.items(),
            key=lambda x: abs(x[1]['absolute_drift']),
            reverse=True,
        )

        for ticker, d in sorted_drift:
            target_value = target_weights.get(ticker, 0) * portfolio_value
            current_value = d['current_weight'] * portfolio_value
            diff = target_value - current_value

            # Skip small trades
            if abs(diff) < self.config.min_trade_value:
                continue

            position = positions_map.get(ticker)

            if diff > 0:
                # Need to buy
                trade = Trade(
                    ticker=ticker,
                    action='buy',
                    shares=0,  # Will be calculated based on price
                    estimated_price=position.current_price if position else 0,
                    estimated_value=diff,
                    reason=f"Underweight by {abs(d['absolute_drift']):.1%}",
                    priority=1 if d['absolute_drift'] < -self.config.absolute_threshold else 2,
                )
                trades.append(trade)

            elif diff < 0 and position:
                # Need to sell
                shares_to_sell = min(
                    abs(diff) / position.current_price,
                    position.shares,
                )

                # Calculate tax impact
                tax_impact = self._calculate_tax_impact(position, shares_to_sell)

                trade = Trade(
                    ticker=ticker,
                    action='sell',
                    shares=shares_to_sell,
                    estimated_price=position.current_price,
                    estimated_value=abs(diff),
                    reason=f"Overweight by {abs(d['absolute_drift']):.1%}",
                    tax_impact=tax_impact,
                    is_long_term=position.is_long_term,
                    priority=1 if d['absolute_drift'] > self.config.absolute_threshold else 2,
                )
                trades.append(trade)

        # Sort by priority
        trades.sort(key=lambda t: t.priority)

        # Apply turnover constraint
        trades = self._apply_turnover_constraint(trades, portfolio_value)

        return trades

    def _calculate_tax_impact(
        self,
        position: PortfolioPosition,
        shares_to_sell: float,
    ) -> float:
        """Calculate tax impact of selling shares."""
        if position.unrealized_gain <= 0:
            return 0.0  # No tax on losses

        gain_per_share = position.current_price - position.cost_basis
        total_gain = gain_per_share * shares_to_sell

        if total_gain <= 0:
            return 0.0

        tax_rate = (
            self.config.tax_rate_long_term
            if position.is_long_term
            else self.config.tax_rate_short_term
        )

        return total_gain * tax_rate

    def _apply_turnover_constraint(
        self,
        trades: List[Trade],
        portfolio_value: float,
    ) -> List[Trade]:
        """Limit trades to max turnover constraint."""
        if portfolio_value <= 0:
            return trades

        max_turnover_value = portfolio_value * self.config.max_turnover
        cumulative_value = 0.0
        constrained_trades = []

        for trade in trades:
            if cumulative_value + trade.estimated_value <= max_turnover_value:
                constrained_trades.append(trade)
                cumulative_value += trade.estimated_value
            else:
                # Partial trade
                remaining = max_turnover_value - cumulative_value
                if remaining > self.config.min_trade_value:
                    partial_trade = Trade(
                        ticker=trade.ticker,
                        action=trade.action,
                        shares=trade.shares * (remaining / trade.estimated_value),
                        estimated_price=trade.estimated_price,
                        estimated_value=remaining,
                        reason=trade.reason + " (partial)",
                        tax_impact=trade.tax_impact * (remaining / trade.estimated_value),
                        is_long_term=trade.is_long_term,
                        priority=trade.priority,
                    )
                    constrained_trades.append(partial_trade)
                break

        return constrained_trades

    def execute_rebalance(
        self,
        result: RebalanceResult,
    ) -> None:
        """
        Mark rebalance as executed.

        Note: Actual order execution should be done via broker integration.
        """
        if result.needs_rebalance and result.trades:
            self.last_rebalance_date = datetime.now()
            LOGGER.info(
                f"Rebalance executed: {result.buys_count} buys, {result.sells_count} sells, "
                f"turnover: {result.total_turnover:.1%}"
            )


class TaxLossHarvester:
    """Tax loss harvesting module."""

    def __init__(
        self,
        wash_sale_days: int = 30,
        min_loss_threshold: float = 100.0,
    ):
        self.wash_sale_days = wash_sale_days
        self.min_loss_threshold = min_loss_threshold
        self.recent_sales: Dict[str, datetime] = {}

    def find_harvest_opportunities(
        self,
        positions: List[PortfolioPosition],
        replacement_map: Optional[Dict[str, str]] = None,
    ) -> List[Trade]:
        """
        Find tax loss harvesting opportunities.

        Args:
            positions: Current positions
            replacement_map: Map of ticker to replacement ticker

        Returns:
            List of harvest trades
        """
        trades = []

        for position in positions:
            if position.unrealized_gain >= 0:
                continue

            # Check wash sale rule
            if position.ticker in self.recent_sales:
                days_since_sale = (
                    datetime.now() - self.recent_sales[position.ticker]
                ).days
                if days_since_sale < self.wash_sale_days:
                    continue

            # Check minimum loss threshold
            loss = abs(position.unrealized_gain)
            if loss < self.min_loss_threshold:
                continue

            trade = Trade(
                ticker=position.ticker,
                action='sell',
                shares=position.shares,
                estimated_price=position.current_price,
                estimated_value=position.market_value,
                reason=f"Tax loss harvest: ${loss:,.0f} loss",
                tax_impact=-loss * 0.35,  # Tax benefit
            )
            trades.append(trade)

            # Add replacement buy if available
            if replacement_map and position.ticker in replacement_map:
                replacement = replacement_map[position.ticker]
                buy_trade = Trade(
                    ticker=replacement,
                    action='buy',
                    shares=0,
                    estimated_price=0,
                    estimated_value=position.market_value,
                    reason=f"Replacement for {position.ticker}",
                )
                trades.append(buy_trade)

        return trades


# Convenience functions

def calculate_portfolio_drift(
    current_weights: Dict[str, float],
    target_weights: Dict[str, float],
) -> pd.DataFrame:
    """
    Calculate portfolio drift.

    Returns DataFrame with drift metrics.
    """
    all_tickers = set(current_weights.keys()) | set(target_weights.keys())

    rows = []
    for ticker in all_tickers:
        current = current_weights.get(ticker, 0)
        target = target_weights.get(ticker, 0)
        absolute = current - target
        relative = absolute / target if target > 0 else (1.0 if current > 0 else 0)

        rows.append({
            'ticker': ticker,
            'current': current,
            'target': target,
            'absolute_drift': absolute,
            'relative_drift': relative,
        })

    return pd.DataFrame(rows).sort_values('absolute_drift', key=abs, ascending=False)


def generate_rebalance_trades(
    current_weights: Dict[str, float],
    target_weights: Dict[str, float],
    portfolio_value: float,
    prices: Dict[str, float],
) -> List[Dict[str, Any]]:
    """
    Generate simple rebalance trades.

    Returns list of trade dicts.
    """
    drift_df = calculate_portfolio_drift(current_weights, target_weights)
    trades = []

    for _, row in drift_df.iterrows():
        ticker = row['ticker']
        diff = row['target'] - row['current']
        trade_value = diff * portfolio_value

        if abs(trade_value) < 100:  # Min trade threshold
            continue

        price = prices.get(ticker, 0)
        if price <= 0:
            continue

        shares = abs(trade_value) / price

        trades.append({
            'ticker': ticker,
            'action': 'buy' if diff > 0 else 'sell',
            'shares': shares,
            'value': abs(trade_value),
            'price': price,
        })

    return trades
