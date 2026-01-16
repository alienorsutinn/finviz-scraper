"""
Monte Carlo Simulation Engine

Advanced simulation capabilities:
- Path generation with various models
- Portfolio risk analysis
- VaR/CVaR calculation
- Drawdown distribution
- Confidence intervals
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats

LOGGER = logging.getLogger(__name__)


class ReturnModel(Enum):
    """Models for return generation."""
    NORMAL = "normal"
    STUDENT_T = "student_t"
    HISTORICAL_BOOTSTRAP = "historical_bootstrap"
    BLOCK_BOOTSTRAP = "block_bootstrap"
    GARCH = "garch"


class CorrelationModel(Enum):
    """Models for correlation structure."""
    CONSTANT = "constant"
    DCC = "dcc"  # Dynamic Conditional Correlation
    COPULA = "copula"


@dataclass
class MonteCarloConfig:
    """Configuration for Monte Carlo simulation."""
    n_simulations: int = 10000
    n_periods: int = 252  # 1 year of daily returns
    return_model: ReturnModel = ReturnModel.NORMAL
    correlation_model: CorrelationModel = CorrelationModel.CONSTANT

    # Bootstrap settings
    block_size: int = 21  # For block bootstrap

    # Student-t settings
    degrees_of_freedom: float = 5.0

    # GARCH settings (simplified)
    garch_omega: float = 0.00001
    garch_alpha: float = 0.05
    garch_beta: float = 0.90

    # Analysis settings
    confidence_levels: List[float] = field(
        default_factory=lambda: [0.95, 0.99]
    )
    risk_free_rate: float = 0.04  # Annual


@dataclass
class SimulationResult:
    """Results from Monte Carlo simulation."""
    config: MonteCarloConfig
    paths: np.ndarray  # Shape: (n_simulations, n_periods)

    # Summary statistics
    mean_return: float = 0.0
    median_return: float = 0.0
    std_return: float = 0.0
    skewness: float = 0.0
    kurtosis: float = 0.0

    # Risk metrics
    var_95: float = 0.0
    var_99: float = 0.0
    cvar_95: float = 0.0  # Expected Shortfall
    cvar_99: float = 0.0

    # Drawdown analysis
    max_drawdown_mean: float = 0.0
    max_drawdown_median: float = 0.0
    max_drawdown_95: float = 0.0  # 95th percentile worst drawdown

    # Probability metrics
    prob_positive: float = 0.0
    prob_beat_benchmark: float = 0.0
    prob_drawdown_exceed_10: float = 0.0
    prob_drawdown_exceed_20: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (excluding paths)."""
        return {
            'mean_return': self.mean_return,
            'median_return': self.median_return,
            'std_return': self.std_return,
            'skewness': self.skewness,
            'kurtosis': self.kurtosis,
            'var_95': self.var_95,
            'var_99': self.var_99,
            'cvar_95': self.cvar_95,
            'cvar_99': self.cvar_99,
            'max_drawdown_mean': self.max_drawdown_mean,
            'max_drawdown_median': self.max_drawdown_median,
            'max_drawdown_95': self.max_drawdown_95,
            'prob_positive': self.prob_positive,
            'prob_beat_benchmark': self.prob_beat_benchmark,
            'prob_drawdown_exceed_10': self.prob_drawdown_exceed_10,
            'prob_drawdown_exceed_20': self.prob_drawdown_exceed_20,
        }

    def get_percentile(self, p: float) -> float:
        """Get return at given percentile."""
        final_values = self.paths[:, -1]
        return np.percentile(final_values, p * 100)

    def get_confidence_interval(
        self,
        confidence: float = 0.95,
    ) -> Tuple[float, float]:
        """Get confidence interval for final returns."""
        final_values = self.paths[:, -1]
        alpha = 1 - confidence
        lower = np.percentile(final_values, alpha / 2 * 100)
        upper = np.percentile(final_values, (1 - alpha / 2) * 100)
        return lower, upper


class ReturnGenerator:
    """Generates returns based on specified model."""

    def __init__(self, config: MonteCarloConfig):
        self.config = config

    def fit(
        self,
        historical_returns: np.ndarray,
    ) -> Dict[str, Any]:
        """
        Fit model to historical returns.

        Returns parameters dict.
        """
        params = {
            'mean': np.mean(historical_returns),
            'std': np.std(historical_returns),
        }

        if self.config.return_model == ReturnModel.STUDENT_T:
            # Fit t-distribution
            df, loc, scale = stats.t.fit(historical_returns)
            params['df'] = df
            params['loc'] = loc
            params['scale'] = scale

        elif self.config.return_model in [
            ReturnModel.HISTORICAL_BOOTSTRAP,
            ReturnModel.BLOCK_BOOTSTRAP,
        ]:
            params['historical'] = historical_returns.copy()

        elif self.config.return_model == ReturnModel.GARCH:
            # Simplified GARCH(1,1) fitting
            params['omega'] = self.config.garch_omega
            params['alpha'] = self.config.garch_alpha
            params['beta'] = self.config.garch_beta
            params['initial_variance'] = np.var(historical_returns)

        return params

    def generate(
        self,
        params: Dict[str, Any],
        n_paths: int,
        n_periods: int,
    ) -> np.ndarray:
        """
        Generate return paths.

        Returns array of shape (n_paths, n_periods).
        """
        model = self.config.return_model

        if model == ReturnModel.NORMAL:
            return self._generate_normal(params, n_paths, n_periods)
        elif model == ReturnModel.STUDENT_T:
            return self._generate_student_t(params, n_paths, n_periods)
        elif model == ReturnModel.HISTORICAL_BOOTSTRAP:
            return self._generate_bootstrap(params, n_paths, n_periods)
        elif model == ReturnModel.BLOCK_BOOTSTRAP:
            return self._generate_block_bootstrap(params, n_paths, n_periods)
        elif model == ReturnModel.GARCH:
            return self._generate_garch(params, n_paths, n_periods)
        else:
            return self._generate_normal(params, n_paths, n_periods)

    def _generate_normal(
        self,
        params: Dict[str, Any],
        n_paths: int,
        n_periods: int,
    ) -> np.ndarray:
        """Generate normal returns."""
        mean = params.get('mean', 0)
        std = params.get('std', 0.01)
        return np.random.normal(mean, std, size=(n_paths, n_periods))

    def _generate_student_t(
        self,
        params: Dict[str, Any],
        n_paths: int,
        n_periods: int,
    ) -> np.ndarray:
        """Generate Student-t returns."""
        df = params.get('df', self.config.degrees_of_freedom)
        loc = params.get('loc', 0)
        scale = params.get('scale', 0.01)
        return stats.t.rvs(df, loc=loc, scale=scale, size=(n_paths, n_periods))

    def _generate_bootstrap(
        self,
        params: Dict[str, Any],
        n_paths: int,
        n_periods: int,
    ) -> np.ndarray:
        """Generate returns via historical bootstrap."""
        historical = params.get('historical')
        if historical is None:
            return self._generate_normal(params, n_paths, n_periods)

        indices = np.random.randint(0, len(historical), size=(n_paths, n_periods))
        return historical[indices]

    def _generate_block_bootstrap(
        self,
        params: Dict[str, Any],
        n_paths: int,
        n_periods: int,
    ) -> np.ndarray:
        """Generate returns via block bootstrap."""
        historical = params.get('historical')
        if historical is None:
            return self._generate_normal(params, n_paths, n_periods)

        block_size = self.config.block_size
        n_blocks = (n_periods + block_size - 1) // block_size

        paths = np.zeros((n_paths, n_periods))

        for i in range(n_paths):
            blocks = []
            for _ in range(n_blocks):
                start = np.random.randint(0, max(1, len(historical) - block_size))
                block = historical[start:start + block_size]
                blocks.append(block)
            path = np.concatenate(blocks)[:n_periods]
            paths[i] = path

        return paths

    def _generate_garch(
        self,
        params: Dict[str, Any],
        n_paths: int,
        n_periods: int,
    ) -> np.ndarray:
        """Generate GARCH(1,1) returns."""
        omega = params.get('omega', self.config.garch_omega)
        alpha = params.get('alpha', self.config.garch_alpha)
        beta = params.get('beta', self.config.garch_beta)
        initial_var = params.get('initial_variance', 0.0001)

        paths = np.zeros((n_paths, n_periods))

        for i in range(n_paths):
            variance = initial_var
            for t in range(n_periods):
                z = np.random.normal()
                r = np.sqrt(variance) * z
                paths[i, t] = r
                # Update variance
                variance = omega + alpha * r ** 2 + beta * variance

        return paths


class MonteCarloSimulator:
    """Main Monte Carlo simulation engine."""

    def __init__(self, config: Optional[MonteCarloConfig] = None):
        self.config = config or MonteCarloConfig()
        self.generator = ReturnGenerator(self.config)

    def simulate(
        self,
        historical_returns: np.ndarray,
        initial_value: float = 1.0,
        benchmark_return: Optional[float] = None,
    ) -> SimulationResult:
        """
        Run Monte Carlo simulation.

        Args:
            historical_returns: Historical return series for calibration
            initial_value: Starting portfolio value
            benchmark_return: Optional benchmark for comparison

        Returns:
            SimulationResult with paths and statistics
        """
        # Fit model to historical data
        params = self.generator.fit(historical_returns)

        # Generate return paths
        return_paths = self.generator.generate(
            params,
            self.config.n_simulations,
            self.config.n_periods,
        )

        # Convert to cumulative wealth paths
        wealth_paths = initial_value * np.cumprod(1 + return_paths, axis=1)

        # Calculate final returns
        final_returns = (wealth_paths[:, -1] - initial_value) / initial_value

        # Calculate statistics
        result = self._calculate_statistics(
            wealth_paths,
            final_returns,
            benchmark_return,
        )

        result.paths = wealth_paths

        return result

    def simulate_portfolio(
        self,
        returns_matrix: np.ndarray,
        weights: np.ndarray,
        initial_value: float = 1.0,
    ) -> SimulationResult:
        """
        Simulate portfolio with multiple assets.

        Args:
            returns_matrix: Historical returns (n_periods, n_assets)
            weights: Portfolio weights
            initial_value: Starting value

        Returns:
            SimulationResult
        """
        # Calculate portfolio returns
        portfolio_returns = returns_matrix @ weights

        return self.simulate(portfolio_returns, initial_value)

    def _calculate_statistics(
        self,
        wealth_paths: np.ndarray,
        final_returns: np.ndarray,
        benchmark_return: Optional[float],
    ) -> SimulationResult:
        """Calculate all statistics from simulation paths."""
        result = SimulationResult(
            config=self.config,
            paths=np.array([]),  # Will be set later
        )

        # Basic statistics
        result.mean_return = np.mean(final_returns)
        result.median_return = np.median(final_returns)
        result.std_return = np.std(final_returns)
        result.skewness = stats.skew(final_returns)
        result.kurtosis = stats.kurtosis(final_returns)

        # VaR (Value at Risk)
        result.var_95 = np.percentile(final_returns, 5)
        result.var_99 = np.percentile(final_returns, 1)

        # CVaR (Conditional VaR / Expected Shortfall)
        result.cvar_95 = np.mean(final_returns[final_returns <= result.var_95])
        result.cvar_99 = np.mean(final_returns[final_returns <= result.var_99])

        # Drawdown analysis
        drawdowns = self._calculate_max_drawdowns(wealth_paths)
        result.max_drawdown_mean = np.mean(drawdowns)
        result.max_drawdown_median = np.median(drawdowns)
        result.max_drawdown_95 = np.percentile(drawdowns, 95)

        # Probability metrics
        result.prob_positive = np.mean(final_returns > 0)

        if benchmark_return is not None:
            result.prob_beat_benchmark = np.mean(final_returns > benchmark_return)
        else:
            result.prob_beat_benchmark = np.nan

        result.prob_drawdown_exceed_10 = np.mean(drawdowns > 0.10)
        result.prob_drawdown_exceed_20 = np.mean(drawdowns > 0.20)

        return result

    def _calculate_max_drawdowns(self, wealth_paths: np.ndarray) -> np.ndarray:
        """Calculate maximum drawdown for each path."""
        n_paths = wealth_paths.shape[0]
        max_drawdowns = np.zeros(n_paths)

        for i in range(n_paths):
            path = wealth_paths[i]
            running_max = np.maximum.accumulate(path)
            drawdown = (running_max - path) / running_max
            max_drawdowns[i] = np.max(drawdown)

        return max_drawdowns

    def stress_test(
        self,
        historical_returns: np.ndarray,
        stress_scenarios: Dict[str, Dict[str, float]],
        initial_value: float = 1.0,
    ) -> Dict[str, SimulationResult]:
        """
        Run stress test simulations under different scenarios.

        Args:
            historical_returns: Base historical returns
            stress_scenarios: Dict of scenario name to parameter adjustments
            initial_value: Starting value

        Returns:
            Dict of scenario name to SimulationResult
        """
        results = {}

        # Base case
        results['base'] = self.simulate(historical_returns, initial_value)

        for name, adjustments in stress_scenarios.items():
            # Adjust returns
            adjusted_returns = historical_returns.copy()

            if 'mean_shift' in adjustments:
                adjusted_returns += adjustments['mean_shift']

            if 'vol_multiplier' in adjustments:
                mean = np.mean(adjusted_returns)
                adjusted_returns = mean + (adjusted_returns - mean) * adjustments['vol_multiplier']

            if 'tail_shock' in adjustments:
                # Add tail events
                shock_prob = adjustments.get('shock_prob', 0.01)
                shock_size = adjustments['tail_shock']
                shocks = np.random.binomial(1, shock_prob, len(adjusted_returns))
                adjusted_returns = adjusted_returns - shocks * shock_size

            results[name] = self.simulate(adjusted_returns, initial_value)

        return results


class PortfolioRiskAnalyzer:
    """Analyze portfolio risk using Monte Carlo."""

    def __init__(self, config: Optional[MonteCarloConfig] = None):
        self.config = config or MonteCarloConfig()
        self.simulator = MonteCarloSimulator(self.config)

    def analyze_var(
        self,
        returns: pd.DataFrame,
        weights: np.ndarray,
        horizon_days: int = 1,
        confidence: float = 0.95,
    ) -> Dict[str, float]:
        """
        Calculate VaR and CVaR for portfolio.

        Args:
            returns: Asset returns DataFrame
            weights: Portfolio weights
            horizon_days: VaR horizon
            confidence: Confidence level

        Returns:
            Dict with VaR metrics
        """
        # Calculate portfolio returns
        portfolio_returns = (returns @ weights).values

        # Scale to horizon
        if horizon_days > 1:
            # Simplified: scale by sqrt(t)
            portfolio_returns = portfolio_returns * np.sqrt(horizon_days)

        # Run simulation
        config = MonteCarloConfig(
            n_simulations=self.config.n_simulations,
            n_periods=horizon_days,
        )
        simulator = MonteCarloSimulator(config)
        result = simulator.simulate(portfolio_returns)

        alpha = 1 - confidence
        var = np.percentile(result.paths[:, -1] - 1, alpha * 100)
        cvar = np.mean(
            (result.paths[:, -1] - 1)[result.paths[:, -1] - 1 <= var]
        )

        return {
            'var': abs(var),
            'cvar': abs(cvar),
            'confidence': confidence,
            'horizon_days': horizon_days,
        }

    def analyze_drawdown_risk(
        self,
        returns: pd.DataFrame,
        weights: np.ndarray,
        max_acceptable_drawdown: float = 0.20,
    ) -> Dict[str, float]:
        """
        Analyze drawdown risk for portfolio.

        Args:
            returns: Asset returns DataFrame
            weights: Portfolio weights
            max_acceptable_drawdown: Maximum acceptable drawdown

        Returns:
            Dict with drawdown risk metrics
        """
        portfolio_returns = (returns @ weights).values

        result = self.simulator.simulate(portfolio_returns)

        return {
            'prob_exceed_drawdown': result.prob_drawdown_exceed_20,
            'expected_max_drawdown': result.max_drawdown_mean,
            'max_drawdown_95_percentile': result.max_drawdown_95,
            'acceptable_drawdown': max_acceptable_drawdown,
        }


# Convenience functions

def run_monte_carlo(
    returns: np.ndarray,
    n_simulations: int = 10000,
    n_periods: int = 252,
    model: ReturnModel = ReturnModel.NORMAL,
) -> SimulationResult:
    """
    Convenience function to run Monte Carlo simulation.

    Args:
        returns: Historical returns
        n_simulations: Number of paths
        n_periods: Simulation horizon
        model: Return model to use

    Returns:
        SimulationResult
    """
    config = MonteCarloConfig(
        n_simulations=n_simulations,
        n_periods=n_periods,
        return_model=model,
    )
    simulator = MonteCarloSimulator(config)
    return simulator.simulate(returns)


def calculate_var(
    returns: np.ndarray,
    confidence: float = 0.95,
    method: str = 'historical',
) -> float:
    """
    Calculate Value at Risk.

    Args:
        returns: Return series
        confidence: Confidence level
        method: 'historical', 'parametric', or 'monte_carlo'

    Returns:
        VaR value (positive number representing loss)
    """
    alpha = 1 - confidence

    if method == 'historical':
        var = np.percentile(returns, alpha * 100)
    elif method == 'parametric':
        mean = np.mean(returns)
        std = np.std(returns)
        var = mean + stats.norm.ppf(alpha) * std
    elif method == 'monte_carlo':
        result = run_monte_carlo(returns, n_simulations=10000, n_periods=1)
        var = result.var_95 if confidence == 0.95 else result.var_99
    else:
        var = np.percentile(returns, alpha * 100)

    return abs(var)


def calculate_cvar(
    returns: np.ndarray,
    confidence: float = 0.95,
) -> float:
    """
    Calculate Conditional Value at Risk (Expected Shortfall).

    Args:
        returns: Return series
        confidence: Confidence level

    Returns:
        CVaR value (positive number representing expected loss)
    """
    alpha = 1 - confidence
    var = np.percentile(returns, alpha * 100)
    cvar = np.mean(returns[returns <= var])
    return abs(cvar)
