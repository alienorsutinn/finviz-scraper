"""
Walk-Forward Backtesting Framework

Advanced backtesting with:
- Walk-forward optimization
- Out-of-sample testing
- Strategy parameter optimization
- Performance attribution
- Rolling window analysis
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)


class OptimizationMethod(Enum):
    """Optimization methods for parameter tuning."""
    GRID_SEARCH = "grid_search"
    RANDOM_SEARCH = "random_search"
    BAYESIAN = "bayesian"
    GENETIC = "genetic"


class PerformanceMetric(Enum):
    """Performance metrics for optimization."""
    SHARPE_RATIO = "sharpe_ratio"
    SORTINO_RATIO = "sortino_ratio"
    CALMAR_RATIO = "calmar_ratio"
    MAX_DRAWDOWN = "max_drawdown"
    TOTAL_RETURN = "total_return"
    INFORMATION_RATIO = "information_ratio"


@dataclass
class WalkForwardConfig:
    """Configuration for walk-forward analysis."""
    # Window sizes
    training_window_days: int = 252  # 1 year training
    validation_window_days: int = 63  # 3 months validation
    test_window_days: int = 21  # 1 month out-of-sample
    step_days: int = 21  # Step forward 1 month

    # Optimization settings
    optimization_method: OptimizationMethod = OptimizationMethod.GRID_SEARCH
    optimization_metric: PerformanceMetric = PerformanceMetric.SHARPE_RATIO
    n_iterations: int = 100  # For random/bayesian search

    # Constraints
    min_samples_train: int = 100
    min_samples_test: int = 20
    max_lookback_days: int = 756  # 3 years max

    # Risk adjustments
    apply_transaction_costs: bool = True
    transaction_cost_bps: float = 10.0  # 10 basis points
    apply_slippage: bool = True
    slippage_bps: float = 5.0


@dataclass
class WindowResult:
    """Results for a single walk-forward window."""
    window_id: int
    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime

    # Optimized parameters
    best_params: Dict[str, Any]
    train_metric: float
    validation_metric: float

    # Out-of-sample performance
    test_return: float
    test_sharpe: float
    test_max_drawdown: float
    test_trades: int

    # Attribution
    alpha: float
    beta: float
    tracking_error: float


@dataclass
class WalkForwardResult:
    """Complete walk-forward analysis results."""
    config: WalkForwardConfig
    windows: List[WindowResult]

    # Aggregate metrics
    total_return: float = 0.0
    annualized_return: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    max_drawdown: float = 0.0
    calmar_ratio: float = 0.0

    # Stability metrics
    parameter_stability: float = 0.0  # How stable are optimal params
    oos_degradation: float = 0.0  # In-sample vs out-of-sample performance drop
    win_rate: float = 0.0  # % of profitable windows

    # Attribution
    avg_alpha: float = 0.0
    avg_beta: float = 0.0
    information_ratio: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'total_return': self.total_return,
            'annualized_return': self.annualized_return,
            'sharpe_ratio': self.sharpe_ratio,
            'sortino_ratio': self.sortino_ratio,
            'max_drawdown': self.max_drawdown,
            'calmar_ratio': self.calmar_ratio,
            'parameter_stability': self.parameter_stability,
            'oos_degradation': self.oos_degradation,
            'win_rate': self.win_rate,
            'avg_alpha': self.avg_alpha,
            'avg_beta': self.avg_beta,
            'information_ratio': self.information_ratio,
            'n_windows': len(self.windows),
        }


class ParameterSpace:
    """Defines parameter search space for optimization."""

    def __init__(self):
        self.params: Dict[str, Dict[str, Any]] = {}

    def add_continuous(
        self,
        name: str,
        low: float,
        high: float,
        log_scale: bool = False,
    ) -> 'ParameterSpace':
        """Add continuous parameter."""
        self.params[name] = {
            'type': 'continuous',
            'low': low,
            'high': high,
            'log_scale': log_scale,
        }
        return self

    def add_discrete(
        self,
        name: str,
        values: List[Any],
    ) -> 'ParameterSpace':
        """Add discrete parameter."""
        self.params[name] = {
            'type': 'discrete',
            'values': values,
        }
        return self

    def add_integer(
        self,
        name: str,
        low: int,
        high: int,
    ) -> 'ParameterSpace':
        """Add integer parameter."""
        self.params[name] = {
            'type': 'integer',
            'low': low,
            'high': high,
        }
        return self

    def sample_random(self) -> Dict[str, Any]:
        """Sample random parameters from space."""
        result = {}
        for name, spec in self.params.items():
            if spec['type'] == 'continuous':
                if spec.get('log_scale'):
                    result[name] = np.exp(
                        np.random.uniform(np.log(spec['low']), np.log(spec['high']))
                    )
                else:
                    result[name] = np.random.uniform(spec['low'], spec['high'])
            elif spec['type'] == 'discrete':
                result[name] = np.random.choice(spec['values'])
            elif spec['type'] == 'integer':
                result[name] = np.random.randint(spec['low'], spec['high'] + 1)
        return result

    def grid_points(self, n_points: int = 5) -> List[Dict[str, Any]]:
        """Generate grid of parameter combinations."""
        grids = {}
        for name, spec in self.params.items():
            if spec['type'] == 'continuous':
                if spec.get('log_scale'):
                    grids[name] = np.exp(
                        np.linspace(np.log(spec['low']), np.log(spec['high']), n_points)
                    )
                else:
                    grids[name] = np.linspace(spec['low'], spec['high'], n_points)
            elif spec['type'] == 'discrete':
                grids[name] = spec['values']
            elif spec['type'] == 'integer':
                grids[name] = list(range(spec['low'], spec['high'] + 1))

        # Generate all combinations
        import itertools
        keys = list(grids.keys())
        values = [grids[k] for k in keys]
        combinations = list(itertools.product(*values))

        return [dict(zip(keys, combo)) for combo in combinations]


class StrategyEvaluator:
    """Evaluates trading strategy with given parameters."""

    def __init__(
        self,
        strategy_fn: Callable[[pd.DataFrame, Dict[str, Any]], pd.Series],
        benchmark_returns: Optional[pd.Series] = None,
    ):
        """
        Initialize evaluator.

        Args:
            strategy_fn: Function that takes data and params, returns position signals
            benchmark_returns: Optional benchmark returns for attribution
        """
        self.strategy_fn = strategy_fn
        self.benchmark_returns = benchmark_returns

    def evaluate(
        self,
        data: pd.DataFrame,
        params: Dict[str, Any],
        config: WalkForwardConfig,
    ) -> Dict[str, float]:
        """
        Evaluate strategy with given parameters.

        Returns dict with metrics.
        """
        try:
            # Get signals from strategy
            signals = self.strategy_fn(data, params)

            # Calculate returns
            if 'returns' in data.columns:
                asset_returns = data['returns']
            elif 'close' in data.columns:
                asset_returns = data['close'].pct_change()
            else:
                raise ValueError("Data must have 'returns' or 'close' column")

            # Strategy returns
            strategy_returns = signals.shift(1) * asset_returns

            # Apply transaction costs
            if config.apply_transaction_costs:
                turnover = signals.diff().abs()
                costs = turnover * (config.transaction_cost_bps / 10000)
                strategy_returns = strategy_returns - costs

            # Apply slippage
            if config.apply_slippage:
                slippage = signals.diff().abs() * (config.slippage_bps / 10000)
                strategy_returns = strategy_returns - slippage

            # Calculate metrics
            strategy_returns = strategy_returns.dropna()

            if len(strategy_returns) < 5:
                return {'sharpe_ratio': -999, 'total_return': -999}

            total_return = (1 + strategy_returns).prod() - 1
            mean_return = strategy_returns.mean()
            std_return = strategy_returns.std()

            # Annualized metrics
            ann_factor = np.sqrt(252)
            sharpe = (mean_return / std_return * ann_factor) if std_return > 0 else 0

            # Downside deviation for Sortino
            downside = strategy_returns[strategy_returns < 0]
            downside_std = downside.std() if len(downside) > 0 else std_return
            sortino = (mean_return / downside_std * ann_factor) if downside_std > 0 else 0

            # Max drawdown
            cumulative = (1 + strategy_returns).cumprod()
            rolling_max = cumulative.expanding().max()
            drawdown = (cumulative - rolling_max) / rolling_max
            max_dd = drawdown.min()

            # Calmar ratio
            ann_return = (1 + total_return) ** (252 / len(strategy_returns)) - 1
            calmar = abs(ann_return / max_dd) if max_dd != 0 else 0

            return {
                'sharpe_ratio': sharpe,
                'sortino_ratio': sortino,
                'total_return': total_return,
                'max_drawdown': max_dd,
                'calmar_ratio': calmar,
                'annualized_return': ann_return,
                'n_trades': int(signals.diff().abs().sum() / 2),
            }

        except Exception as e:
            LOGGER.warning(f"Strategy evaluation failed: {e}")
            return {'sharpe_ratio': -999, 'total_return': -999}


class WalkForwardOptimizer:
    """Walk-forward optimization engine."""

    def __init__(
        self,
        config: WalkForwardConfig,
        evaluator: StrategyEvaluator,
        param_space: ParameterSpace,
    ):
        self.config = config
        self.evaluator = evaluator
        self.param_space = param_space

    def _get_metric_value(
        self,
        metrics: Dict[str, float],
        metric: PerformanceMetric,
    ) -> float:
        """Extract optimization metric from results."""
        mapping = {
            PerformanceMetric.SHARPE_RATIO: 'sharpe_ratio',
            PerformanceMetric.SORTINO_RATIO: 'sortino_ratio',
            PerformanceMetric.CALMAR_RATIO: 'calmar_ratio',
            PerformanceMetric.MAX_DRAWDOWN: 'max_drawdown',
            PerformanceMetric.TOTAL_RETURN: 'total_return',
            PerformanceMetric.INFORMATION_RATIO: 'information_ratio',
        }
        key = mapping.get(metric, 'sharpe_ratio')
        value = metrics.get(key, -999)

        # For max_drawdown, less negative is better
        if metric == PerformanceMetric.MAX_DRAWDOWN:
            value = -abs(value)

        return value

    def _optimize_window(
        self,
        train_data: pd.DataFrame,
        val_data: pd.DataFrame,
    ) -> Tuple[Dict[str, Any], float, float]:
        """
        Optimize parameters on training data, validate on validation data.

        Returns: (best_params, train_metric, val_metric)
        """
        method = self.config.optimization_method
        metric = self.config.optimization_metric

        best_params = None
        best_train_metric = -np.inf
        best_val_metric = -np.inf

        if method == OptimizationMethod.GRID_SEARCH:
            candidates = self.param_space.grid_points(n_points=5)
        elif method == OptimizationMethod.RANDOM_SEARCH:
            candidates = [
                self.param_space.sample_random()
                for _ in range(self.config.n_iterations)
            ]
        else:
            # Default to random search
            candidates = [
                self.param_space.sample_random()
                for _ in range(self.config.n_iterations)
            ]

        for params in candidates:
            # Evaluate on training data
            train_metrics = self.evaluator.evaluate(train_data, params, self.config)
            train_value = self._get_metric_value(train_metrics, metric)

            if train_value > best_train_metric:
                # Validate on held-out data
                val_metrics = self.evaluator.evaluate(val_data, params, self.config)
                val_value = self._get_metric_value(val_metrics, metric)

                best_params = params
                best_train_metric = train_value
                best_val_metric = val_value

        return best_params or {}, best_train_metric, best_val_metric

    def run(
        self,
        data: pd.DataFrame,
        date_column: str = 'date',
    ) -> WalkForwardResult:
        """
        Run walk-forward optimization.

        Args:
            data: DataFrame with date column and price/return data
            date_column: Name of date column

        Returns:
            WalkForwardResult with all window results and aggregate metrics
        """
        if date_column not in data.columns:
            if isinstance(data.index, pd.DatetimeIndex):
                data = data.reset_index()
                data.columns = [date_column] + list(data.columns[1:])
            else:
                raise ValueError(f"Data must have '{date_column}' column or DatetimeIndex")

        data = data.sort_values(date_column)
        dates = pd.to_datetime(data[date_column])

        min_date = dates.min()
        max_date = dates.max()

        windows = []
        window_id = 0

        # Calculate window boundaries
        train_days = self.config.training_window_days
        val_days = self.config.validation_window_days
        test_days = self.config.test_window_days
        step_days = self.config.step_days

        current_start = min_date

        all_test_returns = []

        while True:
            train_end = current_start + timedelta(days=train_days)
            val_end = train_end + timedelta(days=val_days)
            test_end = val_end + timedelta(days=test_days)

            if test_end > max_date:
                break

            # Split data
            train_mask = (dates >= current_start) & (dates < train_end)
            val_mask = (dates >= train_end) & (dates < val_end)
            test_mask = (dates >= val_end) & (dates < test_end)

            train_data = data[train_mask].copy()
            val_data = data[val_mask].copy()
            test_data = data[test_mask].copy()

            if len(train_data) < self.config.min_samples_train:
                current_start += timedelta(days=step_days)
                continue

            if len(test_data) < self.config.min_samples_test:
                current_start += timedelta(days=step_days)
                continue

            LOGGER.info(
                f"Window {window_id}: Train {current_start.date()} to {train_end.date()}, "
                f"Test {val_end.date()} to {test_end.date()}"
            )

            # Optimize on train+val
            best_params, train_metric, val_metric = self._optimize_window(
                train_data, val_data
            )

            # Evaluate on test (out-of-sample)
            test_metrics = self.evaluator.evaluate(test_data, best_params, self.config)

            # Calculate attribution
            alpha, beta, tracking_error = self._calculate_attribution(
                test_data, best_params
            )

            window_result = WindowResult(
                window_id=window_id,
                train_start=current_start,
                train_end=train_end,
                test_start=val_end,
                test_end=test_end,
                best_params=best_params,
                train_metric=train_metric,
                validation_metric=val_metric,
                test_return=test_metrics.get('total_return', 0),
                test_sharpe=test_metrics.get('sharpe_ratio', 0),
                test_max_drawdown=test_metrics.get('max_drawdown', 0),
                test_trades=test_metrics.get('n_trades', 0),
                alpha=alpha,
                beta=beta,
                tracking_error=tracking_error,
            )

            windows.append(window_result)
            all_test_returns.append(test_metrics.get('total_return', 0))

            window_id += 1
            current_start += timedelta(days=step_days)

        # Calculate aggregate metrics
        result = self._aggregate_results(windows, all_test_returns)

        return result

    def _calculate_attribution(
        self,
        data: pd.DataFrame,
        params: Dict[str, Any],
    ) -> Tuple[float, float, float]:
        """Calculate alpha, beta, and tracking error."""
        if self.evaluator.benchmark_returns is None:
            return 0.0, 1.0, 0.0

        try:
            signals = self.evaluator.strategy_fn(data, params)

            if 'returns' in data.columns:
                asset_returns = data['returns']
            else:
                asset_returns = data['close'].pct_change()

            strategy_returns = (signals.shift(1) * asset_returns).dropna()

            # Align with benchmark
            benchmark = self.evaluator.benchmark_returns.reindex(strategy_returns.index)

            if len(benchmark.dropna()) < 5:
                return 0.0, 1.0, 0.0

            # Linear regression for alpha/beta
            from scipy import stats
            slope, intercept, _, _, _ = stats.linregress(
                benchmark.dropna(), strategy_returns.loc[benchmark.dropna().index]
            )

            beta = slope
            alpha = intercept * 252  # Annualized

            # Tracking error
            tracking_diff = strategy_returns - benchmark
            tracking_error = tracking_diff.std() * np.sqrt(252)

            return alpha, beta, tracking_error

        except Exception as e:
            LOGGER.warning(f"Attribution calculation failed: {e}")
            return 0.0, 1.0, 0.0

    def _aggregate_results(
        self,
        windows: List[WindowResult],
        test_returns: List[float],
    ) -> WalkForwardResult:
        """Aggregate window results into final result."""
        if not windows:
            return WalkForwardResult(config=self.config, windows=[])

        # Chain returns
        total_return = np.prod([1 + r for r in test_returns]) - 1

        # Calculate other metrics
        returns_array = np.array(test_returns)
        mean_return = returns_array.mean()
        std_return = returns_array.std()

        sharpe = (mean_return / std_return * np.sqrt(12)) if std_return > 0 else 0

        # Sortino
        downside = returns_array[returns_array < 0]
        downside_std = downside.std() if len(downside) > 0 else std_return
        sortino = (mean_return / downside_std * np.sqrt(12)) if downside_std > 0 else 0

        # Max drawdown from cumulative returns
        cumulative = np.cumprod([1 + r for r in test_returns])
        rolling_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - rolling_max) / rolling_max
        max_dd = drawdown.min()

        # Calmar
        n_years = len(windows) / 12  # Assuming monthly windows
        ann_return = (1 + total_return) ** (1 / n_years) - 1 if n_years > 0 else 0
        calmar = abs(ann_return / max_dd) if max_dd != 0 else 0

        # Parameter stability (measure variance in optimal params)
        param_stability = self._calculate_param_stability(windows)

        # OOS degradation
        train_metrics = [w.train_metric for w in windows]
        test_sharpes = [w.test_sharpe for w in windows]
        avg_train = np.mean(train_metrics) if train_metrics else 0
        avg_test = np.mean(test_sharpes) if test_sharpes else 0
        oos_degradation = (avg_train - avg_test) / avg_train if avg_train > 0 else 0

        # Win rate
        win_rate = sum(1 for r in test_returns if r > 0) / len(test_returns)

        # Attribution
        alphas = [w.alpha for w in windows]
        betas = [w.beta for w in windows]

        return WalkForwardResult(
            config=self.config,
            windows=windows,
            total_return=total_return,
            annualized_return=ann_return,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            max_drawdown=max_dd,
            calmar_ratio=calmar,
            parameter_stability=param_stability,
            oos_degradation=oos_degradation,
            win_rate=win_rate,
            avg_alpha=np.mean(alphas),
            avg_beta=np.mean(betas),
            information_ratio=sharpe,  # Simplified
        )

    def _calculate_param_stability(self, windows: List[WindowResult]) -> float:
        """Calculate how stable optimal parameters are across windows."""
        if len(windows) < 2:
            return 1.0

        # Collect all parameter values
        all_params = [w.best_params for w in windows]

        # Get common keys
        common_keys = set(all_params[0].keys())
        for p in all_params[1:]:
            common_keys &= set(p.keys())

        if not common_keys:
            return 1.0

        # Calculate coefficient of variation for each param
        cvs = []
        for key in common_keys:
            values = [p[key] for p in all_params if isinstance(p.get(key), (int, float))]
            if len(values) > 1 and np.mean(values) != 0:
                cv = np.std(values) / abs(np.mean(values))
                cvs.append(cv)

        if not cvs:
            return 1.0

        # Return stability score (1 - avg CV, clamped to [0, 1])
        avg_cv = np.mean(cvs)
        return max(0, min(1, 1 - avg_cv))


# Convenience functions

def create_momentum_strategy() -> Callable[[pd.DataFrame, Dict[str, Any]], pd.Series]:
    """Create a simple momentum strategy function."""
    def strategy(data: pd.DataFrame, params: Dict[str, Any]) -> pd.Series:
        lookback = params.get('lookback', 20)
        threshold = params.get('threshold', 0.0)

        if 'close' in data.columns:
            prices = data['close']
        elif 'adj_close' in data.columns:
            prices = data['adj_close']
        else:
            return pd.Series(0, index=data.index)

        momentum = prices.pct_change(lookback)
        signals = (momentum > threshold).astype(float)

        return signals

    return strategy


def create_mean_reversion_strategy() -> Callable[[pd.DataFrame, Dict[str, Any]], pd.Series]:
    """Create a mean reversion strategy function."""
    def strategy(data: pd.DataFrame, params: Dict[str, Any]) -> pd.Series:
        lookback = params.get('lookback', 20)
        z_threshold = params.get('z_threshold', 2.0)

        if 'close' in data.columns:
            prices = data['close']
        else:
            return pd.Series(0, index=data.index)

        rolling_mean = prices.rolling(lookback).mean()
        rolling_std = prices.rolling(lookback).std()
        z_score = (prices - rolling_mean) / rolling_std

        # Buy when oversold, sell when overbought
        signals = pd.Series(0.0, index=data.index)
        signals[z_score < -z_threshold] = 1.0
        signals[z_score > z_threshold] = -1.0

        return signals

    return strategy


def run_walk_forward_analysis(
    data: pd.DataFrame,
    strategy_fn: Callable,
    param_space: ParameterSpace,
    config: Optional[WalkForwardConfig] = None,
    benchmark_returns: Optional[pd.Series] = None,
) -> WalkForwardResult:
    """
    Convenience function to run walk-forward analysis.

    Args:
        data: Price/return data
        strategy_fn: Strategy function
        param_space: Parameter search space
        config: Walk-forward configuration
        benchmark_returns: Optional benchmark for attribution

    Returns:
        WalkForwardResult
    """
    if config is None:
        config = WalkForwardConfig()

    evaluator = StrategyEvaluator(strategy_fn, benchmark_returns)
    optimizer = WalkForwardOptimizer(config, evaluator, param_space)

    return optimizer.run(data)
