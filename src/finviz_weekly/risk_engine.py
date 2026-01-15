"""
Risk Management Engine

Comprehensive risk management for portfolio construction:
- Position sizing with various methods
- Position limits (per-stock, sector, correlation)
- Stop-loss automation (fixed, trailing, volatility-adjusted)
- Risk attribution and decomposition
- Order execution simulation with slippage
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class PositionSizingMethod(Enum):
    """Position sizing methods."""
    EQUAL_WEIGHT = "equal_weight"
    RISK_PARITY = "risk_parity"
    VOLATILITY_TARGET = "volatility_target"
    KELLY = "kelly"
    CONVICTION = "conviction"  # Based on signal strength


class StopLossType(Enum):
    """Stop-loss types."""
    FIXED_PCT = "fixed_pct"
    TRAILING_PCT = "trailing_pct"
    ATR_BASED = "atr_based"
    VOLATILITY_ADJUSTED = "volatility_adjusted"
    TIME_BASED = "time_based"


@dataclass
class PositionLimits:
    """Position limit configuration."""
    max_position_pct: float = 0.10  # Max 10% per stock
    max_sector_pct: float = 0.30  # Max 30% per sector
    max_correlated_pct: float = 0.40  # Max 40% in correlated positions
    correlation_threshold: float = 0.7  # Threshold for "correlated"
    min_position_pct: float = 0.01  # Min 1% (avoid tiny positions)
    max_positions: int = 30  # Maximum number of positions
    min_positions: int = 10  # Minimum diversification


@dataclass
class StopLossConfig:
    """Stop-loss configuration."""
    stop_type: StopLossType = StopLossType.TRAILING_PCT
    stop_pct: float = 0.08  # 8% stop loss
    trailing_activation: float = 0.05  # Activate trailing after 5% gain
    atr_multiplier: float = 2.0  # Stop at 2x ATR
    max_holding_days: int = 63  # Time-based exit (3 months)
    profit_target_pct: Optional[float] = 0.20  # Take profit at 20%


@dataclass
class Position:
    """Active position."""
    ticker: str
    entry_date: str
    entry_price: float
    shares: int
    current_price: float
    stop_price: float
    highest_price: float  # For trailing stop
    sector: str
    signal_strength: float  # Original conviction score

    @property
    def market_value(self) -> float:
        return self.shares * self.current_price

    @property
    def cost_basis(self) -> float:
        return self.shares * self.entry_price

    @property
    def pnl(self) -> float:
        return self.market_value - self.cost_basis

    @property
    def pnl_pct(self) -> float:
        if self.cost_basis > 0:
            return self.pnl / self.cost_basis
        return 0.0

    @property
    def days_held(self) -> int:
        entry = datetime.strptime(self.entry_date, "%Y-%m-%d")
        return (datetime.now() - entry).days


@dataclass
class RiskMetrics:
    """Portfolio risk metrics."""
    total_value: float
    cash: float
    invested: float
    num_positions: int

    # Risk measures
    portfolio_volatility: float  # Annualized
    portfolio_beta: float
    var_95: float  # Value at Risk (95%)
    cvar_95: float  # Conditional VaR
    max_drawdown: float

    # Concentration
    largest_position_pct: float
    largest_sector_pct: float
    herfindahl_index: float  # Concentration measure

    # Attribution
    sector_exposures: Dict[str, float]
    factor_exposures: Dict[str, float]


@dataclass
class ExecutionResult:
    """Simulated execution result."""
    ticker: str
    side: str  # 'buy' or 'sell'
    requested_shares: int
    executed_shares: int
    requested_price: float
    executed_price: float
    slippage_pct: float
    commission: float
    market_impact: float
    total_cost: float


class PositionSizer:
    """Calculate position sizes using various methods."""

    def __init__(self, method: PositionSizingMethod = PositionSizingMethod.VOLATILITY_TARGET,
                 target_volatility: float = 0.15,
                 kelly_fraction: float = 0.25):
        self.method = method
        self.target_volatility = target_volatility
        self.kelly_fraction = kelly_fraction

    def calculate_sizes(self,
                       portfolio_value: float,
                       candidates: pd.DataFrame,
                       volatilities: Dict[str, float],
                       expected_returns: Optional[Dict[str, float]] = None,
                       correlations: Optional[pd.DataFrame] = None) -> Dict[str, float]:
        """
        Calculate position sizes for candidates.

        Args:
            portfolio_value: Total portfolio value
            candidates: DataFrame with candidate tickers and scores
            volatilities: Annualized volatility per ticker
            expected_returns: Expected returns per ticker (for Kelly)
            correlations: Correlation matrix

        Returns:
            Dict mapping ticker to position size (dollar amount)
        """
        tickers = list(candidates['ticker'].values) if 'ticker' in candidates.columns else list(candidates.index)

        if self.method == PositionSizingMethod.EQUAL_WEIGHT:
            return self._equal_weight(portfolio_value, tickers)

        elif self.method == PositionSizingMethod.RISK_PARITY:
            return self._risk_parity(portfolio_value, tickers, volatilities)

        elif self.method == PositionSizingMethod.VOLATILITY_TARGET:
            return self._volatility_target(portfolio_value, tickers, volatilities)

        elif self.method == PositionSizingMethod.KELLY:
            return self._kelly(portfolio_value, tickers, volatilities,
                             expected_returns or {})

        elif self.method == PositionSizingMethod.CONVICTION:
            scores = dict(zip(candidates['ticker'], candidates.get('score', [50]*len(candidates))))
            return self._conviction_weighted(portfolio_value, tickers, scores)

        return self._equal_weight(portfolio_value, tickers)

    def _equal_weight(self, portfolio_value: float, tickers: List[str]) -> Dict[str, float]:
        """Equal weight across all positions."""
        if not tickers:
            return {}
        weight = portfolio_value / len(tickers)
        return {t: weight for t in tickers}

    def _risk_parity(self, portfolio_value: float, tickers: List[str],
                    volatilities: Dict[str, float]) -> Dict[str, float]:
        """Equal risk contribution from each position."""
        if not tickers:
            return {}

        # Inverse volatility weighting
        inv_vols = {}
        for t in tickers:
            vol = volatilities.get(t, 0.30)  # Default 30% vol
            inv_vols[t] = 1.0 / max(vol, 0.01)

        total_inv_vol = sum(inv_vols.values())
        if total_inv_vol == 0:
            return self._equal_weight(portfolio_value, tickers)

        return {t: portfolio_value * inv_vols[t] / total_inv_vol for t in tickers}

    def _volatility_target(self, portfolio_value: float, tickers: List[str],
                          volatilities: Dict[str, float]) -> Dict[str, float]:
        """Size positions to target specific volatility contribution."""
        if not tickers:
            return {}

        # Target volatility per position
        vol_per_position = self.target_volatility / np.sqrt(len(tickers))

        sizes = {}
        for t in tickers:
            vol = volatilities.get(t, 0.30)
            # Position size = (target vol / stock vol) * equal weight
            if vol > 0:
                weight = min(vol_per_position / vol, 0.15)  # Cap at 15%
                sizes[t] = portfolio_value * weight
            else:
                sizes[t] = portfolio_value / len(tickers)

        # Normalize to sum to portfolio value
        total = sum(sizes.values())
        if total > 0:
            sizes = {t: s * portfolio_value / total for t, s in sizes.items()}

        return sizes

    def _kelly(self, portfolio_value: float, tickers: List[str],
              volatilities: Dict[str, float],
              expected_returns: Dict[str, float]) -> Dict[str, float]:
        """Kelly criterion position sizing."""
        if not tickers:
            return {}

        sizes = {}
        for t in tickers:
            vol = volatilities.get(t, 0.30)
            exp_ret = expected_returns.get(t, 0.10)

            # Kelly fraction = expected_return / variance
            if vol > 0:
                kelly = exp_ret / (vol ** 2)
                # Apply fractional Kelly and cap
                weight = min(kelly * self.kelly_fraction, 0.15)
                weight = max(weight, 0)
                sizes[t] = portfolio_value * weight
            else:
                sizes[t] = portfolio_value / len(tickers)

        # Normalize
        total = sum(sizes.values())
        if total > 0 and total != portfolio_value:
            sizes = {t: s * portfolio_value / total for t, s in sizes.items()}

        return sizes

    def _conviction_weighted(self, portfolio_value: float, tickers: List[str],
                            scores: Dict[str, float]) -> Dict[str, float]:
        """Weight by conviction/signal strength."""
        if not tickers:
            return {}

        # Normalize scores to weights
        total_score = sum(scores.get(t, 50) for t in tickers)
        if total_score == 0:
            return self._equal_weight(portfolio_value, tickers)

        return {t: portfolio_value * scores.get(t, 50) / total_score for t in tickers}


class StopLossManager:
    """Manage stop-loss orders for positions."""

    def __init__(self, config: Optional[StopLossConfig] = None):
        self.config = config or StopLossConfig()

    def calculate_stop_price(self, position: Position,
                            atr: Optional[float] = None,
                            volatility: Optional[float] = None) -> float:
        """Calculate stop price based on configuration."""
        if self.config.stop_type == StopLossType.FIXED_PCT:
            return position.entry_price * (1 - self.config.stop_pct)

        elif self.config.stop_type == StopLossType.TRAILING_PCT:
            # Use highest price for trailing stop
            if position.pnl_pct >= self.config.trailing_activation:
                return position.highest_price * (1 - self.config.stop_pct)
            else:
                return position.entry_price * (1 - self.config.stop_pct)

        elif self.config.stop_type == StopLossType.ATR_BASED and atr:
            return position.current_price - (atr * self.config.atr_multiplier)

        elif self.config.stop_type == StopLossType.VOLATILITY_ADJUSTED and volatility:
            # Stop at 2 standard deviations
            stop_distance = position.entry_price * volatility * 2 / np.sqrt(252)
            return position.entry_price - stop_distance

        # Default to fixed percentage
        return position.entry_price * (1 - self.config.stop_pct)

    def check_exit_signals(self, position: Position,
                          atr: Optional[float] = None) -> Tuple[bool, str]:
        """
        Check if position should be exited.

        Returns:
            (should_exit, reason)
        """
        # Check stop loss
        if position.current_price <= position.stop_price:
            return True, "stop_loss"

        # Check profit target
        if self.config.profit_target_pct:
            if position.pnl_pct >= self.config.profit_target_pct:
                return True, "profit_target"

        # Check time-based exit
        if position.days_held >= self.config.max_holding_days:
            return True, "time_exit"

        return False, ""

    def update_trailing_stop(self, position: Position) -> float:
        """Update trailing stop if price has increased."""
        if self.config.stop_type != StopLossType.TRAILING_PCT:
            return position.stop_price

        # Only trail after activation threshold
        if position.pnl_pct < self.config.trailing_activation:
            return position.stop_price

        # Update highest price
        new_high = max(position.highest_price, position.current_price)
        new_stop = new_high * (1 - self.config.stop_pct)

        # Stop only moves up, never down
        return max(position.stop_price, new_stop)


class RiskEngine:
    """Main risk management engine."""

    def __init__(self,
                 limits: Optional[PositionLimits] = None,
                 stop_config: Optional[StopLossConfig] = None):
        self.limits = limits or PositionLimits()
        self.stop_manager = StopLossManager(stop_config)
        self.positions: Dict[str, Position] = {}

    def check_position_limits(self,
                             proposed_position: Dict[str, float],
                             current_positions: Dict[str, Position],
                             sector_mapping: Dict[str, str],
                             correlations: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """
        Check if proposed positions violate limits.

        Returns:
            Dict with violations and adjusted positions
        """
        violations = []
        adjusted = proposed_position.copy()
        total_value = sum(proposed_position.values())

        if total_value == 0:
            return {'violations': [], 'adjusted': {}, 'passed': True}

        # Check max position size
        for ticker, value in proposed_position.items():
            pct = value / total_value
            if pct > self.limits.max_position_pct:
                violations.append(f"{ticker}: {pct:.1%} exceeds max {self.limits.max_position_pct:.1%}")
                adjusted[ticker] = total_value * self.limits.max_position_pct

        # Check sector limits
        sector_exposure = {}
        for ticker, value in adjusted.items():
            sector = sector_mapping.get(ticker, "Unknown")
            sector_exposure[sector] = sector_exposure.get(sector, 0) + value

        for sector, exposure in sector_exposure.items():
            pct = exposure / total_value
            if pct > self.limits.max_sector_pct:
                violations.append(f"Sector {sector}: {pct:.1%} exceeds max {self.limits.max_sector_pct:.1%}")
                # Scale down positions in this sector
                scale = self.limits.max_sector_pct / pct
                for ticker in adjusted:
                    if sector_mapping.get(ticker) == sector:
                        adjusted[ticker] *= scale

        # Check number of positions
        if len(adjusted) > self.limits.max_positions:
            violations.append(f"Too many positions: {len(adjusted)} > {self.limits.max_positions}")
            # Keep top positions by value
            sorted_positions = sorted(adjusted.items(), key=lambda x: -x[1])
            adjusted = dict(sorted_positions[:self.limits.max_positions])

        # Check correlation limits
        if correlations is not None:
            correlated_exposure = self._calculate_correlated_exposure(
                adjusted, correlations, total_value
            )
            if correlated_exposure > self.limits.max_correlated_pct:
                violations.append(f"Correlated exposure: {correlated_exposure:.1%} exceeds max")

        return {
            'violations': violations,
            'adjusted': adjusted,
            'passed': len(violations) == 0
        }

    def _calculate_correlated_exposure(self,
                                       positions: Dict[str, float],
                                       correlations: pd.DataFrame,
                                       total_value: float) -> float:
        """Calculate exposure to highly correlated positions."""
        tickers = list(positions.keys())
        correlated_value = 0

        for i, t1 in enumerate(tickers):
            for t2 in tickers[i+1:]:
                if t1 in correlations.index and t2 in correlations.columns:
                    corr = abs(correlations.loc[t1, t2])
                    if corr >= self.limits.correlation_threshold:
                        correlated_value += min(positions[t1], positions[t2])

        return correlated_value / total_value if total_value > 0 else 0

    def calculate_risk_metrics(self,
                              positions: Dict[str, Position],
                              returns_data: pd.DataFrame,
                              benchmark_returns: Optional[pd.Series] = None) -> RiskMetrics:
        """Calculate comprehensive risk metrics for portfolio."""
        if not positions:
            return RiskMetrics(
                total_value=0, cash=0, invested=0, num_positions=0,
                portfolio_volatility=0, portfolio_beta=1.0,
                var_95=0, cvar_95=0, max_drawdown=0,
                largest_position_pct=0, largest_sector_pct=0,
                herfindahl_index=0, sector_exposures={}, factor_exposures={}
            )

        total_value = sum(p.market_value for p in positions.values())
        weights = {t: p.market_value / total_value for t, p in positions.items()}

        # Portfolio volatility
        tickers = list(positions.keys())
        if all(t in returns_data.columns for t in tickers):
            port_returns = sum(
                returns_data[t] * weights[t] for t in tickers if t in returns_data.columns
            )
            portfolio_volatility = port_returns.std() * np.sqrt(252)
        else:
            portfolio_volatility = 0.20  # Default

        # Beta calculation
        if benchmark_returns is not None and len(port_returns) > 0:
            cov = port_returns.cov(benchmark_returns)
            var = benchmark_returns.var()
            portfolio_beta = cov / var if var > 0 else 1.0
        else:
            portfolio_beta = 1.0

        # VaR and CVaR
        if len(port_returns) > 0:
            var_95 = np.percentile(port_returns, 5) * total_value
            cvar_95 = port_returns[port_returns <= np.percentile(port_returns, 5)].mean() * total_value
        else:
            var_95 = total_value * 0.05
            cvar_95 = total_value * 0.08

        # Concentration metrics
        position_pcts = list(weights.values())
        largest_position_pct = max(position_pcts) if position_pcts else 0
        herfindahl_index = sum(p ** 2 for p in position_pcts)

        # Sector exposure
        sector_exposure = {}
        for ticker, pos in positions.items():
            sector = pos.sector
            sector_exposure[sector] = sector_exposure.get(sector, 0) + weights[ticker]

        largest_sector_pct = max(sector_exposure.values()) if sector_exposure else 0

        return RiskMetrics(
            total_value=total_value,
            cash=0,  # Would need cash tracking
            invested=total_value,
            num_positions=len(positions),
            portfolio_volatility=portfolio_volatility,
            portfolio_beta=portfolio_beta,
            var_95=abs(var_95),
            cvar_95=abs(cvar_95),
            max_drawdown=0,  # Would need equity curve
            largest_position_pct=largest_position_pct,
            largest_sector_pct=largest_sector_pct,
            herfindahl_index=herfindahl_index,
            sector_exposures=sector_exposure,
            factor_exposures={}
        )


class ExecutionSimulator:
    """Simulate order execution with realistic costs."""

    def __init__(self,
                 commission_per_share: float = 0.005,
                 min_commission: float = 1.0,
                 spread_pct: float = 0.001,
                 impact_coefficient: float = 0.1):
        """
        Initialize simulator.

        Args:
            commission_per_share: Commission per share ($)
            min_commission: Minimum commission per order
            spread_pct: Bid-ask spread as percentage
            impact_coefficient: Market impact coefficient
        """
        self.commission_per_share = commission_per_share
        self.min_commission = min_commission
        self.spread_pct = spread_pct
        self.impact_coefficient = impact_coefficient

    def simulate_execution(self,
                          ticker: str,
                          side: str,
                          shares: int,
                          price: float,
                          avg_volume: float = 1000000) -> ExecutionResult:
        """
        Simulate order execution.

        Args:
            ticker: Stock ticker
            side: 'buy' or 'sell'
            shares: Number of shares
            price: Current market price
            avg_volume: Average daily volume
        """
        # Commission
        commission = max(shares * self.commission_per_share, self.min_commission)

        # Spread cost (pay half spread on each side)
        spread_cost = price * self.spread_pct / 2

        # Market impact (increases with order size relative to volume)
        participation_rate = shares / avg_volume if avg_volume > 0 else 0.01
        market_impact = price * self.impact_coefficient * np.sqrt(participation_rate)

        # Total slippage
        if side == 'buy':
            executed_price = price + spread_cost + market_impact
        else:
            executed_price = price - spread_cost - market_impact

        slippage_pct = abs(executed_price - price) / price

        # Total cost
        total_cost = commission + (shares * abs(executed_price - price))

        return ExecutionResult(
            ticker=ticker,
            side=side,
            requested_shares=shares,
            executed_shares=shares,  # Assume full fill
            requested_price=price,
            executed_price=executed_price,
            slippage_pct=slippage_pct,
            commission=commission,
            market_impact=market_impact * shares,
            total_cost=total_cost
        )

    def estimate_round_trip_cost(self,
                                ticker: str,
                                shares: int,
                                price: float,
                                avg_volume: float = 1000000) -> float:
        """Estimate total round-trip cost (buy + sell)."""
        buy = self.simulate_execution(ticker, 'buy', shares, price, avg_volume)
        sell = self.simulate_execution(ticker, 'sell', shares, price, avg_volume)
        return buy.total_cost + sell.total_cost
