"""
Portfolio Optimization Module

Implements multiple optimization approaches:
- Mean-Variance (Markowitz)
- Black-Litterman
- Hierarchical Risk Parity (HRP)
- Risk Parity
- Maximum Diversification
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import optimize
from scipy.cluster import hierarchy
from scipy.spatial.distance import squareform

LOGGER = logging.getLogger(__name__)


class OptimizationObjective(Enum):
    """Optimization objectives."""
    MAX_SHARPE = "max_sharpe"
    MIN_VARIANCE = "min_variance"
    MAX_RETURN = "max_return"
    RISK_PARITY = "risk_parity"
    MAX_DIVERSIFICATION = "max_diversification"
    TARGET_RETURN = "target_return"
    TARGET_RISK = "target_risk"


@dataclass
class PortfolioConstraints:
    """Portfolio optimization constraints."""
    min_weight: float = 0.0
    max_weight: float = 1.0
    max_sector_weight: float = 0.40
    min_assets: int = 5
    max_assets: int = 50
    long_only: bool = True
    sector_constraints: Dict[str, float] = field(default_factory=dict)
    asset_constraints: Dict[str, Tuple[float, float]] = field(default_factory=dict)


@dataclass
class OptimizationResult:
    """Result from portfolio optimization."""
    weights: np.ndarray
    tickers: List[str]
    expected_return: float
    expected_volatility: float
    sharpe_ratio: float

    # Additional metrics
    diversification_ratio: float = 0.0
    effective_n: float = 0.0  # Effective number of assets
    max_weight: float = 0.0
    turnover: float = 0.0  # vs previous weights

    # Risk contributions
    risk_contributions: Optional[np.ndarray] = None
    marginal_risk: Optional[np.ndarray] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'weights': dict(zip(self.tickers, self.weights.tolist())),
            'expected_return': self.expected_return,
            'expected_volatility': self.expected_volatility,
            'sharpe_ratio': self.sharpe_ratio,
            'diversification_ratio': self.diversification_ratio,
            'effective_n': self.effective_n,
            'max_weight': self.max_weight,
        }

    def get_allocation(self, min_weight: float = 0.01) -> pd.DataFrame:
        """Get allocation as DataFrame."""
        df = pd.DataFrame({
            'ticker': self.tickers,
            'weight': self.weights,
        })
        df = df[df['weight'] >= min_weight].sort_values('weight', ascending=False)
        return df


class MeanVarianceOptimizer:
    """Classic Markowitz Mean-Variance optimizer."""

    def __init__(
        self,
        returns: pd.DataFrame,
        risk_free_rate: float = 0.04,
        constraints: Optional[PortfolioConstraints] = None,
    ):
        """
        Initialize optimizer.

        Args:
            returns: Asset returns DataFrame
            risk_free_rate: Annual risk-free rate
            constraints: Portfolio constraints
        """
        self.returns = returns
        self.tickers = list(returns.columns)
        self.n_assets = len(self.tickers)
        self.risk_free_rate = risk_free_rate
        self.constraints = constraints or PortfolioConstraints()

        # Calculate statistics
        self.mean_returns = returns.mean() * 252  # Annualized
        self.cov_matrix = returns.cov() * 252  # Annualized

    def optimize(
        self,
        objective: OptimizationObjective = OptimizationObjective.MAX_SHARPE,
        target_return: Optional[float] = None,
        target_risk: Optional[float] = None,
    ) -> OptimizationResult:
        """
        Run optimization.

        Args:
            objective: Optimization objective
            target_return: Target return for TARGET_RETURN objective
            target_risk: Target risk for TARGET_RISK objective

        Returns:
            OptimizationResult
        """
        if objective == OptimizationObjective.MAX_SHARPE:
            weights = self._optimize_sharpe()
        elif objective == OptimizationObjective.MIN_VARIANCE:
            weights = self._optimize_min_variance()
        elif objective == OptimizationObjective.MAX_RETURN:
            weights = self._optimize_max_return()
        elif objective == OptimizationObjective.TARGET_RETURN:
            weights = self._optimize_target_return(target_return or 0.10)
        elif objective == OptimizationObjective.TARGET_RISK:
            weights = self._optimize_target_risk(target_risk or 0.15)
        else:
            weights = self._optimize_sharpe()

        return self._create_result(weights)

    def _optimize_sharpe(self) -> np.ndarray:
        """Maximize Sharpe ratio."""
        def neg_sharpe(weights):
            port_return = np.dot(weights, self.mean_returns)
            port_vol = np.sqrt(np.dot(weights.T, np.dot(self.cov_matrix, weights)))
            return -(port_return - self.risk_free_rate) / port_vol

        return self._run_optimization(neg_sharpe)

    def _optimize_min_variance(self) -> np.ndarray:
        """Minimize portfolio variance."""
        def variance(weights):
            return np.dot(weights.T, np.dot(self.cov_matrix, weights))

        return self._run_optimization(variance)

    def _optimize_max_return(self) -> np.ndarray:
        """Maximize expected return."""
        def neg_return(weights):
            return -np.dot(weights, self.mean_returns)

        return self._run_optimization(neg_return)

    def _optimize_target_return(self, target: float) -> np.ndarray:
        """Minimize variance for target return."""
        def variance(weights):
            return np.dot(weights.T, np.dot(self.cov_matrix, weights))

        extra_constraints = [{
            'type': 'eq',
            'fun': lambda w: np.dot(w, self.mean_returns) - target,
        }]

        return self._run_optimization(variance, extra_constraints)

    def _optimize_target_risk(self, target: float) -> np.ndarray:
        """Maximize return for target risk."""
        def neg_return(weights):
            return -np.dot(weights, self.mean_returns)

        extra_constraints = [{
            'type': 'eq',
            'fun': lambda w: np.sqrt(np.dot(w.T, np.dot(self.cov_matrix, w))) - target,
        }]

        return self._run_optimization(neg_return, extra_constraints)

    def _run_optimization(
        self,
        objective_fn,
        extra_constraints: Optional[List] = None,
    ) -> np.ndarray:
        """Run scipy optimization."""
        # Initial weights
        init_weights = np.ones(self.n_assets) / self.n_assets

        # Bounds
        bounds = tuple(
            (self.constraints.min_weight, self.constraints.max_weight)
            for _ in range(self.n_assets)
        )

        # Constraints
        constraints = [
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1},  # Weights sum to 1
        ]

        if extra_constraints:
            constraints.extend(extra_constraints)

        # Optimize
        result = optimize.minimize(
            objective_fn,
            init_weights,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            options={'maxiter': 1000},
        )

        if not result.success:
            LOGGER.warning(f"Optimization did not converge: {result.message}")

        return result.x

    def _create_result(self, weights: np.ndarray) -> OptimizationResult:
        """Create OptimizationResult from weights."""
        port_return = np.dot(weights, self.mean_returns)
        port_vol = np.sqrt(np.dot(weights.T, np.dot(self.cov_matrix, weights)))
        sharpe = (port_return - self.risk_free_rate) / port_vol

        # Diversification ratio
        asset_vols = np.sqrt(np.diag(self.cov_matrix))
        weighted_vol = np.dot(weights, asset_vols)
        div_ratio = weighted_vol / port_vol

        # Effective N
        effective_n = 1 / np.sum(weights ** 2)

        # Risk contributions
        marginal_risk = np.dot(self.cov_matrix, weights) / port_vol
        risk_contributions = weights * marginal_risk

        return OptimizationResult(
            weights=weights,
            tickers=self.tickers,
            expected_return=port_return,
            expected_volatility=port_vol,
            sharpe_ratio=sharpe,
            diversification_ratio=div_ratio,
            effective_n=effective_n,
            max_weight=np.max(weights),
            risk_contributions=risk_contributions,
            marginal_risk=marginal_risk,
        )

    def efficient_frontier(
        self,
        n_points: int = 50,
    ) -> pd.DataFrame:
        """
        Calculate efficient frontier.

        Returns DataFrame with return, risk, sharpe for each point.
        """
        min_ret = self.mean_returns.min()
        max_ret = self.mean_returns.max()

        target_returns = np.linspace(min_ret, max_ret, n_points)
        frontier = []

        for target in target_returns:
            try:
                weights = self._optimize_target_return(target)
                result = self._create_result(weights)
                frontier.append({
                    'target_return': target,
                    'return': result.expected_return,
                    'volatility': result.expected_volatility,
                    'sharpe': result.sharpe_ratio,
                })
            except Exception:
                continue

        return pd.DataFrame(frontier)


class BlackLittermanOptimizer:
    """Black-Litterman model for portfolio optimization."""

    def __init__(
        self,
        returns: pd.DataFrame,
        market_caps: Optional[pd.Series] = None,
        risk_free_rate: float = 0.04,
        tau: float = 0.05,
    ):
        """
        Initialize Black-Litterman optimizer.

        Args:
            returns: Asset returns DataFrame
            market_caps: Market capitalizations for equilibrium weights
            risk_free_rate: Annual risk-free rate
            tau: Uncertainty parameter (typically 0.01-0.05)
        """
        self.returns = returns
        self.tickers = list(returns.columns)
        self.n_assets = len(self.tickers)
        self.risk_free_rate = risk_free_rate
        self.tau = tau

        # Covariance matrix
        self.cov_matrix = returns.cov() * 252

        # Market cap weights (if not provided, use equal weight)
        if market_caps is not None:
            self.market_weights = market_caps / market_caps.sum()
        else:
            self.market_weights = pd.Series(
                1 / self.n_assets,
                index=self.tickers,
            )

        # Calculate implied equilibrium returns
        self.delta = self._calculate_risk_aversion()
        self.pi = self._calculate_equilibrium_returns()

    def _calculate_risk_aversion(self) -> float:
        """Calculate implied risk aversion from market."""
        # Simplified: use typical market Sharpe of 0.4
        market_return = 0.08  # Assumed market return
        market_vol = 0.16  # Assumed market volatility
        return (market_return - self.risk_free_rate) / (market_vol ** 2)

    def _calculate_equilibrium_returns(self) -> np.ndarray:
        """Calculate implied equilibrium returns."""
        weights = self.market_weights.values
        return self.delta * np.dot(self.cov_matrix, weights)

    def add_views(
        self,
        views: List[Dict[str, Any]],
    ) -> 'BlackLittermanOptimizer':
        """
        Add investor views.

        View format:
        {
            'assets': ['AAPL', 'MSFT'],  # Assets involved
            'weights': [1, -1],  # Relative weights (for relative view)
            'return': 0.05,  # Expected return
            'confidence': 0.5,  # Confidence in view (0-1)
        }
        """
        self.views = views
        return self

    def optimize(self) -> OptimizationResult:
        """
        Run Black-Litterman optimization.

        Returns:
            OptimizationResult
        """
        if not hasattr(self, 'views') or not self.views:
            # No views, return equilibrium
            posterior_returns = self.pi
        else:
            posterior_returns = self._calculate_posterior_returns()

        # Use mean-variance optimizer with posterior returns
        mv_optimizer = MeanVarianceOptimizer(
            self.returns,
            self.risk_free_rate,
        )
        mv_optimizer.mean_returns = pd.Series(
            posterior_returns,
            index=self.tickers,
        )

        return mv_optimizer.optimize(OptimizationObjective.MAX_SHARPE)

    def _calculate_posterior_returns(self) -> np.ndarray:
        """Calculate posterior returns using Black-Litterman formula."""
        # Build P matrix (pick matrix)
        P = np.zeros((len(self.views), self.n_assets))
        Q = np.zeros(len(self.views))
        omega_diag = np.zeros(len(self.views))

        for i, view in enumerate(self.views):
            assets = view.get('assets', [])
            weights = view.get('weights', [1])
            Q[i] = view.get('return', 0)
            confidence = view.get('confidence', 0.5)

            for asset, weight in zip(assets, weights):
                if asset in self.tickers:
                    j = self.tickers.index(asset)
                    P[i, j] = weight

            # Omega diagonal (view uncertainty)
            # Higher confidence = lower uncertainty
            omega_diag[i] = (1 - confidence) * self.tau * np.dot(
                P[i], np.dot(self.cov_matrix, P[i])
            )

        Omega = np.diag(omega_diag)

        # Black-Litterman formula
        tau_sigma = self.tau * self.cov_matrix

        # Posterior covariance
        inv_tau_sigma = np.linalg.inv(tau_sigma)
        inv_omega = np.linalg.inv(Omega + 1e-10 * np.eye(len(self.views)))

        M = np.linalg.inv(inv_tau_sigma + np.dot(P.T, np.dot(inv_omega, P)))

        # Posterior returns
        posterior_returns = np.dot(
            M,
            np.dot(inv_tau_sigma, self.pi) + np.dot(P.T, np.dot(inv_omega, Q))
        )

        return posterior_returns


class HierarchicalRiskParity:
    """Hierarchical Risk Parity (HRP) optimizer."""

    def __init__(
        self,
        returns: pd.DataFrame,
        linkage_method: str = 'single',
    ):
        """
        Initialize HRP optimizer.

        Args:
            returns: Asset returns DataFrame
            linkage_method: Hierarchical clustering method
        """
        self.returns = returns
        self.tickers = list(returns.columns)
        self.n_assets = len(self.tickers)
        self.linkage_method = linkage_method

        # Calculate correlation and covariance
        self.corr_matrix = returns.corr()
        self.cov_matrix = returns.cov() * 252

    def optimize(self) -> OptimizationResult:
        """
        Run HRP optimization.

        Returns:
            OptimizationResult
        """
        # Step 1: Tree clustering
        dist_matrix = self._get_distance_matrix()
        link = hierarchy.linkage(squareform(dist_matrix), method=self.linkage_method)

        # Step 2: Quasi-diagonalization
        sorted_indices = self._get_quasi_diagonal(link)

        # Step 3: Recursive bisection
        weights = self._recursive_bisection(sorted_indices)

        return self._create_result(weights)

    def _get_distance_matrix(self) -> np.ndarray:
        """Convert correlation to distance matrix."""
        return np.sqrt(0.5 * (1 - self.corr_matrix.values))

    def _get_quasi_diagonal(self, link: np.ndarray) -> List[int]:
        """Get quasi-diagonal ordering from linkage."""
        link = link.astype(int)
        sort_idx = pd.Series([link[-1, 0], link[-1, 1]])
        num_items = link[-1, 3]

        while sort_idx.max() >= self.n_assets:
            sort_idx.index = range(0, sort_idx.shape[0] * 2, 2)
            df0 = sort_idx[sort_idx >= self.n_assets]
            i = df0.index
            j = df0.values - self.n_assets

            sort_idx[i] = link[j, 0]
            df1 = pd.Series(link[j, 1], index=i + 1)
            sort_idx = pd.concat([sort_idx, df1])
            sort_idx = sort_idx.sort_index()
            sort_idx.index = range(sort_idx.shape[0])

        return sort_idx.tolist()

    def _recursive_bisection(self, sorted_indices: List[int]) -> np.ndarray:
        """Allocate weights using recursive bisection."""
        weights = pd.Series(1.0, index=sorted_indices)
        clusters = [sorted_indices]

        while len(clusters) > 0:
            # Bisect each cluster
            new_clusters = []
            for cluster in clusters:
                if len(cluster) > 1:
                    # Split in half
                    mid = len(cluster) // 2
                    left = cluster[:mid]
                    right = cluster[mid:]

                    # Calculate cluster variances
                    left_var = self._get_cluster_variance(left)
                    right_var = self._get_cluster_variance(right)

                    # Allocate inversely to variance
                    alpha = 1 - left_var / (left_var + right_var)

                    weights[left] *= alpha
                    weights[right] *= (1 - alpha)

                    if len(left) > 1:
                        new_clusters.append(left)
                    if len(right) > 1:
                        new_clusters.append(right)

            clusters = new_clusters

        # Convert to array with correct ordering
        result = np.zeros(self.n_assets)
        for i, idx in enumerate(sorted_indices):
            result[idx] = weights.iloc[i]

        return result / result.sum()

    def _get_cluster_variance(self, indices: List[int]) -> float:
        """Get variance of cluster using inverse-variance weights."""
        cov_slice = self.cov_matrix.iloc[indices, indices].values
        ivp = 1 / np.diag(cov_slice)
        ivp /= ivp.sum()
        return np.dot(ivp, np.dot(cov_slice, ivp))

    def _create_result(self, weights: np.ndarray) -> OptimizationResult:
        """Create OptimizationResult from weights."""
        port_return = np.dot(weights, self.returns.mean() * 252)
        port_vol = np.sqrt(np.dot(weights.T, np.dot(self.cov_matrix, weights)))
        sharpe = port_return / port_vol if port_vol > 0 else 0

        return OptimizationResult(
            weights=weights,
            tickers=self.tickers,
            expected_return=port_return,
            expected_volatility=port_vol,
            sharpe_ratio=sharpe,
            effective_n=1 / np.sum(weights ** 2),
            max_weight=np.max(weights),
        )


class RiskParityOptimizer:
    """Risk Parity (Equal Risk Contribution) optimizer."""

    def __init__(
        self,
        returns: pd.DataFrame,
        target_risk_budget: Optional[np.ndarray] = None,
    ):
        """
        Initialize Risk Parity optimizer.

        Args:
            returns: Asset returns DataFrame
            target_risk_budget: Target risk contribution per asset (default: equal)
        """
        self.returns = returns
        self.tickers = list(returns.columns)
        self.n_assets = len(self.tickers)
        self.cov_matrix = returns.cov() * 252

        if target_risk_budget is None:
            self.risk_budget = np.ones(self.n_assets) / self.n_assets
        else:
            self.risk_budget = target_risk_budget / target_risk_budget.sum()

    def optimize(self) -> OptimizationResult:
        """
        Run risk parity optimization.

        Returns:
            OptimizationResult
        """
        def risk_budget_objective(weights):
            port_vol = np.sqrt(np.dot(weights.T, np.dot(self.cov_matrix, weights)))
            marginal_risk = np.dot(self.cov_matrix, weights) / port_vol
            risk_contributions = weights * marginal_risk
            risk_contrib_normalized = risk_contributions / risk_contributions.sum()

            # Minimize squared difference from target budget
            return np.sum((risk_contrib_normalized - self.risk_budget) ** 2)

        # Initial weights
        init_weights = np.ones(self.n_assets) / self.n_assets

        # Bounds and constraints
        bounds = tuple((0.01, 1.0) for _ in range(self.n_assets))
        constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1}]

        result = optimize.minimize(
            risk_budget_objective,
            init_weights,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            options={'maxiter': 1000},
        )

        weights = result.x

        # Create result
        port_return = np.dot(weights, self.returns.mean() * 252)
        port_vol = np.sqrt(np.dot(weights.T, np.dot(self.cov_matrix, weights)))
        sharpe = port_return / port_vol if port_vol > 0 else 0

        return OptimizationResult(
            weights=weights,
            tickers=self.tickers,
            expected_return=port_return,
            expected_volatility=port_vol,
            sharpe_ratio=sharpe,
            effective_n=1 / np.sum(weights ** 2),
            max_weight=np.max(weights),
        )


# Convenience functions

def optimize_portfolio(
    returns: pd.DataFrame,
    method: str = 'mean_variance',
    objective: OptimizationObjective = OptimizationObjective.MAX_SHARPE,
    **kwargs,
) -> OptimizationResult:
    """
    Convenience function for portfolio optimization.

    Args:
        returns: Asset returns DataFrame
        method: 'mean_variance', 'black_litterman', 'hrp', 'risk_parity'
        objective: Optimization objective (for MV)
        **kwargs: Additional arguments for optimizer

    Returns:
        OptimizationResult
    """
    if method == 'mean_variance':
        optimizer = MeanVarianceOptimizer(returns, **kwargs)
        return optimizer.optimize(objective)
    elif method == 'black_litterman':
        optimizer = BlackLittermanOptimizer(returns, **kwargs)
        return optimizer.optimize()
    elif method == 'hrp':
        optimizer = HierarchicalRiskParity(returns, **kwargs)
        return optimizer.optimize()
    elif method == 'risk_parity':
        optimizer = RiskParityOptimizer(returns, **kwargs)
        return optimizer.optimize()
    else:
        raise ValueError(f"Unknown method: {method}")


def compare_optimizers(
    returns: pd.DataFrame,
    methods: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Compare different optimization methods.

    Args:
        returns: Asset returns DataFrame
        methods: List of methods to compare

    Returns:
        DataFrame comparing results
    """
    if methods is None:
        methods = ['mean_variance', 'hrp', 'risk_parity']

    results = []
    for method in methods:
        try:
            result = optimize_portfolio(returns, method=method)
            results.append({
                'method': method,
                'expected_return': result.expected_return,
                'expected_volatility': result.expected_volatility,
                'sharpe_ratio': result.sharpe_ratio,
                'effective_n': result.effective_n,
                'max_weight': result.max_weight,
            })
        except Exception as e:
            LOGGER.warning(f"Method {method} failed: {e}")

    return pd.DataFrame(results)
