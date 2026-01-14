"""
Advanced portfolio optimization using modern portfolio theory.

Implements:
- Minimum Variance Portfolio
- Mean-Variance Optimization (Markowitz)
- Risk Parity Portfolio
- Maximum Sharpe Ratio Portfolio

Uses cvxpy for convex optimization.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import cvxpy as cp
import numpy as np
import pandas as pd
from scipy.optimize import minimize

LOGGER = logging.getLogger(__name__)


class OptimizationMethod(Enum):
    """Portfolio optimization methods."""

    MIN_VARIANCE = "min_variance"
    MEAN_VARIANCE = "mean_variance"
    RISK_PARITY = "risk_parity"
    MAX_SHARPE = "max_sharpe"
    EQUAL_WEIGHT = "equal_weight"


class CovarianceMethod(Enum):
    """Covariance estimation methods."""

    SAMPLE = "sample"  # Simple sample covariance
    SHRINKAGE = "shrinkage"  # Ledoit-Wolf shrinkage
    EXPONENTIAL = "exponential"  # Exponentially weighted


@dataclass
class PortfolioWeights:
    """Optimized portfolio weights."""

    tickers: List[str]
    weights: np.ndarray
    method: OptimizationMethod

    # Portfolio statistics
    expected_return: float
    volatility: float
    sharpe_ratio: float

    # Additional metrics
    diversification_ratio: Optional[float] = None  # For risk parity
    herfindahl_index: Optional[float] = None  # Concentration measure

    def to_dict(self) -> Dict[str, float]:
        """Convert to ticker -> weight dictionary."""
        return dict(zip(self.tickers, self.weights))


class PortfolioOptimizer:
    """Advanced portfolio optimization using modern portfolio theory."""

    def __init__(
        self,
        returns: pd.DataFrame,
        risk_free_rate: float = 0.04,
        cov_method: CovarianceMethod = CovarianceMethod.SAMPLE,
    ):
        """
        Initialize optimizer.

        Args:
            returns: DataFrame of asset returns (dates x tickers)
            risk_free_rate: Annual risk-free rate (default 4%)
            cov_method: Covariance estimation method
        """
        self.returns = returns
        self.risk_free_rate = risk_free_rate
        self.cov_method = cov_method

        # Compute statistics
        self.mean_returns = returns.mean()  # Daily returns
        self.cov_matrix = self._estimate_covariance(returns, cov_method)
        self.tickers = list(returns.columns)

        LOGGER.info(f"Initialized optimizer with {len(self.tickers)} assets")
        LOGGER.info(f"Mean daily return: {self.mean_returns.mean():.4f}")
        LOGGER.info(f"Mean daily volatility: {np.sqrt(np.diag(self.cov_matrix)).mean():.4f}")

    def optimize(
        self,
        method: OptimizationMethod,
        target_return: Optional[float] = None,
        max_weight: float = 0.20,
        min_weight: float = 0.0,
    ) -> PortfolioWeights:
        """
        Optimize portfolio using specified method.

        Args:
            method: Optimization method to use
            target_return: Target return for mean-variance optimization
            max_weight: Maximum weight per asset (default 20%)
            min_weight: Minimum weight per asset (default 0%, no short selling)

        Returns:
            PortfolioWeights with optimized allocation
        """
        LOGGER.info(f"Optimizing portfolio using {method.value}")

        if method == OptimizationMethod.MIN_VARIANCE:
            weights = self.min_variance(max_weight=max_weight, min_weight=min_weight)
        elif method == OptimizationMethod.MEAN_VARIANCE:
            weights = self.mean_variance(
                target_return=target_return,
                max_weight=max_weight,
                min_weight=min_weight,
            )
        elif method == OptimizationMethod.RISK_PARITY:
            weights = self.risk_parity(max_weight=max_weight, min_weight=min_weight)
        elif method == OptimizationMethod.MAX_SHARPE:
            weights = self.max_sharpe(max_weight=max_weight, min_weight=min_weight)
        elif method == OptimizationMethod.EQUAL_WEIGHT:
            weights = self.equal_weight()
        else:
            raise ValueError(f"Unknown optimization method: {method}")

        # Calculate portfolio statistics
        portfolio_return = np.dot(weights, self.mean_returns) * 252  # Annualized
        portfolio_vol = np.sqrt(np.dot(weights, np.dot(self.cov_matrix, weights))) * np.sqrt(252)  # Annualized
        sharpe = (portfolio_return - self.risk_free_rate) / portfolio_vol if portfolio_vol > 0 else 0

        # Calculate concentration metrics
        herfindahl = np.sum(weights**2)  # HHI: lower = more diversified

        # Diversification ratio (for risk parity)
        asset_vols = np.sqrt(np.diag(self.cov_matrix))
        weighted_vol = np.dot(weights, asset_vols)
        diversification_ratio = weighted_vol / portfolio_vol if portfolio_vol > 0 else 1.0

        LOGGER.info(f"Portfolio return: {portfolio_return:.2%}")
        LOGGER.info(f"Portfolio volatility: {portfolio_vol:.2%}")
        LOGGER.info(f"Sharpe ratio: {sharpe:.2f}")
        LOGGER.info(f"Herfindahl index: {herfindahl:.4f}")

        return PortfolioWeights(
            tickers=self.tickers,
            weights=weights,
            method=method,
            expected_return=portfolio_return,
            volatility=portfolio_vol,
            sharpe_ratio=sharpe,
            diversification_ratio=diversification_ratio,
            herfindahl_index=herfindahl,
        )

    def min_variance(self, max_weight: float = 0.20, min_weight: float = 0.0) -> np.ndarray:
        """
        Compute minimum variance portfolio.

        Minimizes portfolio volatility without regard to returns.

        Args:
            max_weight: Maximum weight per asset
            min_weight: Minimum weight per asset

        Returns:
            Optimal weights array
        """
        n = len(self.tickers)

        # Define optimization variables
        w = cp.Variable(n)

        # Objective: minimize variance
        portfolio_variance = cp.quad_form(w, self.cov_matrix)
        objective = cp.Minimize(portfolio_variance)

        # Constraints
        constraints = [
            cp.sum(w) == 1,  # Fully invested
            w >= min_weight,  # No short selling (or min weight)
            w <= max_weight,  # Position size limit
        ]

        # Solve
        problem = cp.Problem(objective, constraints)
        problem.solve(solver=cp.OSQP, verbose=False)

        if problem.status not in ["optimal", "optimal_inaccurate"]:
            LOGGER.warning(f"Optimization did not converge: {problem.status}")
            return self.equal_weight()

        return w.value

    def mean_variance(
        self,
        target_return: Optional[float] = None,
        max_weight: float = 0.20,
        min_weight: float = 0.0,
    ) -> np.ndarray:
        """
        Compute mean-variance optimal portfolio (Markowitz).

        Minimizes variance for a given target return, or maximizes
        Sharpe ratio if no target return specified.

        Args:
            target_return: Target annual return (if None, maximizes Sharpe)
            max_weight: Maximum weight per asset
            min_weight: Minimum weight per asset

        Returns:
            Optimal weights array
        """
        if target_return is None:
            # No target return specified, maximize Sharpe ratio instead
            return self.max_sharpe(max_weight=max_weight, min_weight=min_weight)

        n = len(self.tickers)

        # Convert target return to daily
        target_daily = target_return / 252

        # Define optimization variables
        w = cp.Variable(n)

        # Objective: minimize variance
        portfolio_variance = cp.quad_form(w, self.cov_matrix)
        objective = cp.Minimize(portfolio_variance)

        # Constraints
        portfolio_return = self.mean_returns.values @ w
        constraints = [
            cp.sum(w) == 1,  # Fully invested
            portfolio_return >= target_daily,  # Meet target return
            w >= min_weight,  # No short selling
            w <= max_weight,  # Position size limit
        ]

        # Solve
        problem = cp.Problem(objective, constraints)
        problem.solve(solver=cp.OSQP, verbose=False)

        if problem.status not in ["optimal", "optimal_inaccurate"]:
            LOGGER.warning(f"Optimization did not converge: {problem.status}. Using min variance.")
            return self.min_variance(max_weight=max_weight, min_weight=min_weight)

        return w.value

    def max_sharpe(self, max_weight: float = 0.20, min_weight: float = 0.0) -> np.ndarray:
        """
        Compute maximum Sharpe ratio portfolio.

        Maximizes (return - risk_free_rate) / volatility.

        Args:
            max_weight: Maximum weight per asset
            min_weight: Minimum weight per asset

        Returns:
            Optimal weights array
        """
        n = len(self.tickers)

        # Convert risk-free rate to daily
        rf_daily = self.risk_free_rate / 252

        # Define optimization variables
        w = cp.Variable(n)

        # Objective: maximize Sharpe ratio
        # We use the trick: max Sharpe = max (r - rf) / sqrt(w'Σw)
        # Equivalent to: minimize w'Σw subject to (r - rf)'w = 1
        portfolio_return = (self.mean_returns.values - rf_daily) @ w
        portfolio_variance = cp.quad_form(w, self.cov_matrix)

        # Use a different formulation: maximize return / sqrt(variance)
        # This is non-convex, so we use scipy.optimize instead
        def neg_sharpe(weights):
            """Negative Sharpe ratio (for minimization)."""
            ret = np.dot(weights, self.mean_returns) * 252
            vol = np.sqrt(np.dot(weights, np.dot(self.cov_matrix, weights))) * np.sqrt(252)
            sharpe = (ret - self.risk_free_rate) / vol if vol > 0 else -np.inf
            return -sharpe  # Negative for minimization

        # Constraints for scipy.optimize
        constraints = (
            {"type": "eq", "fun": lambda w: np.sum(w) - 1},  # Fully invested
        )

        bounds = tuple((min_weight, max_weight) for _ in range(n))

        # Initial guess: equal weight
        w0 = np.ones(n) / n

        # Optimize
        result = minimize(
            neg_sharpe,
            w0,
            method="SLSQP",
            bounds=bounds,
            constraints=constraints,
            options={"maxiter": 1000, "ftol": 1e-9},
        )

        if not result.success:
            LOGGER.warning(f"Max Sharpe optimization failed: {result.message}. Using min variance.")
            return self.min_variance(max_weight=max_weight, min_weight=min_weight)

        return result.x

    def risk_parity(self, max_weight: float = 0.20, min_weight: float = 0.01) -> np.ndarray:
        """
        Compute risk parity portfolio.

        Each asset contributes equally to portfolio risk.
        Risk contribution of asset i = w_i * (Σw)_i / sqrt(w'Σw)

        Args:
            max_weight: Maximum weight per asset
            min_weight: Minimum weight per asset (default 1% to avoid zeros)

        Returns:
            Optimal weights array
        """
        n = len(self.tickers)

        def risk_contributions(weights):
            """Calculate risk contribution of each asset."""
            portfolio_vol = np.sqrt(np.dot(weights, np.dot(self.cov_matrix, weights)))
            marginal_contrib = np.dot(self.cov_matrix, weights)
            risk_contrib = weights * marginal_contrib / portfolio_vol
            return risk_contrib

        def risk_parity_objective(weights):
            """
            Objective: minimize sum of squared deviations from equal risk contribution.

            Target: each asset contributes 1/n of total risk.
            """
            risk_contrib = risk_contributions(weights)
            target = np.ones(n) / n
            return np.sum((risk_contrib - target) ** 2)

        # Constraints
        constraints = (
            {"type": "eq", "fun": lambda w: np.sum(w) - 1},  # Fully invested
        )

        bounds = tuple((min_weight, max_weight) for _ in range(n))

        # Initial guess: equal weight
        w0 = np.ones(n) / n

        # Optimize
        result = minimize(
            risk_parity_objective,
            w0,
            method="SLSQP",
            bounds=bounds,
            constraints=constraints,
            options={"maxiter": 1000, "ftol": 1e-9},
        )

        if not result.success:
            LOGGER.warning(f"Risk parity optimization failed: {result.message}. Using equal weight.")
            return self.equal_weight()

        return result.x

    def equal_weight(self) -> np.ndarray:
        """
        Compute equal weight portfolio (1/n rule).

        Returns:
            Equal weights array
        """
        n = len(self.tickers)
        return np.ones(n) / n

    def _estimate_covariance(self, returns: pd.DataFrame, method: CovarianceMethod) -> np.ndarray:
        """
        Estimate covariance matrix.

        Args:
            returns: DataFrame of returns
            method: Estimation method

        Returns:
            Covariance matrix (numpy array)
        """
        if method == CovarianceMethod.SAMPLE:
            return returns.cov().values

        elif method == CovarianceMethod.SHRINKAGE:
            # Ledoit-Wolf shrinkage estimator
            return self._ledoit_wolf_shrinkage(returns)

        elif method == CovarianceMethod.EXPONENTIAL:
            # Exponentially weighted covariance
            return returns.ewm(span=60).cov().iloc[-len(returns.columns) :].values

        else:
            raise ValueError(f"Unknown covariance method: {method}")

    def _ledoit_wolf_shrinkage(self, returns: pd.DataFrame) -> np.ndarray:
        """
        Ledoit-Wolf shrinkage estimator for covariance.

        Shrinks sample covariance towards constant correlation matrix.

        Args:
            returns: DataFrame of returns

        Returns:
            Shrunk covariance matrix
        """
        # Sample covariance
        S = returns.cov().values
        n_samples, n_assets = returns.shape

        # Target: constant correlation matrix
        mean_var = np.mean(np.diag(S))
        mean_corr = (np.sum(S) - np.trace(S)) / (n_assets * (n_assets - 1))

        # Target covariance: diagonal = sample var, off-diagonal = mean_corr * sqrt(var_i * var_j)
        std_devs = np.sqrt(np.diag(S))
        F = mean_corr * np.outer(std_devs, std_devs)
        np.fill_diagonal(F, np.diag(S))

        # Shrinkage intensity (simplified)
        # Full Ledoit-Wolf calculation is more complex
        delta = min(1, max(0, 0.5))  # Fixed shrinkage intensity

        # Shrink
        shrunk_cov = delta * F + (1 - delta) * S

        return shrunk_cov


def optimize_portfolio(
    returns: pd.DataFrame,
    method: str = "max_sharpe",
    risk_free_rate: float = 0.04,
    max_weight: float = 0.20,
    target_return: Optional[float] = None,
) -> PortfolioWeights:
    """
    Convenience function for portfolio optimization.

    Args:
        returns: DataFrame of asset returns (dates x tickers)
        method: Optimization method (min_variance, mean_variance, risk_parity, max_sharpe, equal_weight)
        risk_free_rate: Annual risk-free rate
        max_weight: Maximum weight per asset
        target_return: Target return for mean-variance optimization

    Returns:
        PortfolioWeights with optimized allocation
    """
    optimizer = PortfolioOptimizer(returns, risk_free_rate=risk_free_rate)

    method_enum = OptimizationMethod(method)

    return optimizer.optimize(
        method=method_enum,
        target_return=target_return,
        max_weight=max_weight,
    )
