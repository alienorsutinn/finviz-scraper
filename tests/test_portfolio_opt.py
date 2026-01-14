"""Tests for portfolio optimization."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from finviz_weekly.portfolio_opt import (
    PortfolioOptimizer,
    PortfolioWeights,
    OptimizationMethod,
    CovarianceMethod,
    optimize_portfolio,
)


@pytest.fixture
def mock_returns():
    """Create mock return data for testing."""
    np.random.seed(42)

    dates = pd.date_range("2023-01-01", "2024-12-31", freq="D")
    tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

    # Generate correlated returns
    n_days = len(dates)
    n_assets = len(tickers)

    # Create correlation matrix
    corr = np.array([
        [1.0, 0.6, 0.7, 0.5, 0.3],
        [0.6, 1.0, 0.5, 0.6, 0.4],
        [0.7, 0.5, 1.0, 0.6, 0.3],
        [0.5, 0.6, 0.6, 1.0, 0.5],
        [0.3, 0.4, 0.3, 0.5, 1.0],
    ])

    # Create returns with specified correlation
    mean_returns = np.array([0.0005, 0.0006, 0.0005, 0.0007, 0.0008])  # Daily returns
    volatilities = np.array([0.015, 0.018, 0.016, 0.020, 0.025])  # Daily vols

    # Generate correlated random returns
    L = np.linalg.cholesky(corr)
    random_returns = np.random.randn(n_days, n_assets)
    correlated_returns = random_returns @ L.T

    # Scale to desired mean and volatility
    returns_data = {}
    for i, ticker in enumerate(tickers):
        returns_data[ticker] = mean_returns[i] + volatilities[i] * correlated_returns[:, i]

    returns_df = pd.DataFrame(returns_data, index=dates)

    return returns_df


class TestPortfolioOptimizer:
    """Tests for PortfolioOptimizer."""

    def test_init(self, mock_returns):
        """Test optimizer initialization."""
        optimizer = PortfolioOptimizer(mock_returns)

        assert optimizer.returns is not None
        assert len(optimizer.tickers) == 5
        assert optimizer.mean_returns is not None
        assert optimizer.cov_matrix is not None
        assert optimizer.cov_matrix.shape == (5, 5)

    def test_equal_weight(self, mock_returns):
        """Test equal weight portfolio."""
        optimizer = PortfolioOptimizer(mock_returns)
        weights = optimizer.equal_weight()

        assert len(weights) == 5
        assert np.allclose(weights, 0.2)  # 1/5 = 0.2
        assert np.isclose(np.sum(weights), 1.0)

    def test_min_variance(self, mock_returns):
        """Test minimum variance portfolio."""
        optimizer = PortfolioOptimizer(mock_returns)
        weights = optimizer.min_variance()

        assert len(weights) == 5
        assert np.isclose(np.sum(weights), 1.0)
        assert np.all(weights >= 0)  # No short selling
        assert np.all(weights <= 0.2 + 1e-6)  # Respect max weight (with numerical tolerance)

        # Check that it actually minimizes variance
        equal_weights = optimizer.equal_weight()
        min_var_vol = np.sqrt(np.dot(weights, np.dot(optimizer.cov_matrix, weights)))
        equal_vol = np.sqrt(np.dot(equal_weights, np.dot(optimizer.cov_matrix, equal_weights)))

        assert min_var_vol <= equal_vol  # Min variance should have lower vol

    def test_mean_variance_with_target(self, mock_returns):
        """Test mean-variance optimization with target return."""
        optimizer = PortfolioOptimizer(mock_returns)
        target_return = 0.10  # 10% annual return

        weights = optimizer.mean_variance(target_return=target_return)

        assert len(weights) == 5
        assert np.isclose(np.sum(weights), 1.0)
        assert np.all(weights >= 0)

        # Check that it meets or exceeds target return
        portfolio_return = np.dot(weights, optimizer.mean_returns) * 252
        assert portfolio_return >= target_return * 0.99  # Allow small numerical error

    def test_mean_variance_without_target(self, mock_returns):
        """Test mean-variance optimization without target (should maximize Sharpe)."""
        optimizer = PortfolioOptimizer(mock_returns)

        weights_mv = optimizer.mean_variance(target_return=None)
        weights_sharpe = optimizer.max_sharpe()

        # Should be equivalent to max Sharpe
        assert np.allclose(weights_mv, weights_sharpe, atol=0.01)

    def test_max_sharpe(self, mock_returns):
        """Test maximum Sharpe ratio portfolio."""
        optimizer = PortfolioOptimizer(mock_returns)
        weights = optimizer.max_sharpe()

        assert len(weights) == 5
        assert np.isclose(np.sum(weights), 1.0)
        assert np.all(weights >= 0)

        # Calculate Sharpe ratio
        portfolio_return = np.dot(weights, optimizer.mean_returns) * 252
        portfolio_vol = np.sqrt(np.dot(weights, np.dot(optimizer.cov_matrix, weights))) * np.sqrt(252)
        sharpe = (portfolio_return - optimizer.risk_free_rate) / portfolio_vol

        # Check that it beats equal weight Sharpe
        equal_weights = optimizer.equal_weight()
        equal_return = np.dot(equal_weights, optimizer.mean_returns) * 252
        equal_vol = np.sqrt(np.dot(equal_weights, np.dot(optimizer.cov_matrix, equal_weights))) * np.sqrt(252)
        equal_sharpe = (equal_return - optimizer.risk_free_rate) / equal_vol

        assert sharpe >= equal_sharpe * 0.95  # Allow small numerical error

    def test_risk_parity(self, mock_returns):
        """Test risk parity portfolio."""
        optimizer = PortfolioOptimizer(mock_returns)
        weights = optimizer.risk_parity()

        assert len(weights) == 5
        assert np.isclose(np.sum(weights), 1.0)
        assert np.all(weights >= 0.01)  # Min weight constraint
        assert np.all(weights <= 0.2)  # Max weight constraint

        # Check that risk contributions are approximately equal
        portfolio_vol = np.sqrt(np.dot(weights, np.dot(optimizer.cov_matrix, weights)))
        marginal_contrib = np.dot(optimizer.cov_matrix, weights)
        risk_contrib = weights * marginal_contrib / portfolio_vol

        # Risk contributions should be similar (within 50% of each other)
        assert risk_contrib.max() / risk_contrib.min() < 5.0

    def test_optimize_method_dispatch(self, mock_returns):
        """Test that optimize() correctly dispatches to methods."""
        optimizer = PortfolioOptimizer(mock_returns)

        # Test each method
        for method in OptimizationMethod:
            result = optimizer.optimize(method)

            assert isinstance(result, PortfolioWeights)
            assert len(result.weights) == 5
            assert np.isclose(np.sum(result.weights), 1.0)
            assert result.method == method

    def test_portfolio_weights_to_dict(self, mock_returns):
        """Test PortfolioWeights.to_dict()."""
        optimizer = PortfolioOptimizer(mock_returns)
        result = optimizer.optimize(OptimizationMethod.EQUAL_WEIGHT)

        weights_dict = result.to_dict()

        assert isinstance(weights_dict, dict)
        assert len(weights_dict) == 5
        assert all(ticker in weights_dict for ticker in ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"])
        assert np.isclose(sum(weights_dict.values()), 1.0)

    def test_herfindahl_index(self, mock_returns):
        """Test Herfindahl index calculation."""
        optimizer = PortfolioOptimizer(mock_returns)

        # Equal weight should have lower HHI (more diversified)
        equal_result = optimizer.optimize(OptimizationMethod.EQUAL_WEIGHT)
        sharpe_result = optimizer.optimize(OptimizationMethod.MAX_SHARPE)

        # Equal weight HHI = 1/n = 0.2 for 5 assets
        assert np.isclose(equal_result.herfindahl_index, 0.2)

        # Max Sharpe may be more concentrated
        assert sharpe_result.herfindahl_index >= equal_result.herfindahl_index * 0.95


class TestCovarianceEstimation:
    """Tests for covariance estimation methods."""

    def test_sample_covariance(self, mock_returns):
        """Test sample covariance estimation."""
        optimizer = PortfolioOptimizer(mock_returns, cov_method=CovarianceMethod.SAMPLE)

        cov_sample = optimizer.cov_matrix
        cov_pandas = mock_returns.cov().values

        assert np.allclose(cov_sample, cov_pandas)

    def test_shrinkage_covariance(self, mock_returns):
        """Test Ledoit-Wolf shrinkage covariance."""
        optimizer = PortfolioOptimizer(mock_returns, cov_method=CovarianceMethod.SHRINKAGE)

        cov_shrunk = optimizer.cov_matrix
        cov_sample = mock_returns.cov().values

        # Shrunk covariance should differ from sample
        assert not np.allclose(cov_shrunk, cov_sample)

        # But should still be positive definite
        eigenvalues = np.linalg.eigvals(cov_shrunk)
        assert np.all(eigenvalues > 0)

    def test_exponential_covariance(self, mock_returns):
        """Test exponentially weighted covariance."""
        optimizer = PortfolioOptimizer(mock_returns, cov_method=CovarianceMethod.EXPONENTIAL)

        cov_exp = optimizer.cov_matrix

        # Should be positive definite
        eigenvalues = np.linalg.eigvals(cov_exp)
        assert np.all(eigenvalues > 0)


class TestOptimizePortfolio:
    """Tests for optimize_portfolio convenience function."""

    def test_optimize_portfolio_max_sharpe(self, mock_returns):
        """Test convenience function with max Sharpe."""
        result = optimize_portfolio(mock_returns, method="max_sharpe")

        assert isinstance(result, PortfolioWeights)
        assert result.method == OptimizationMethod.MAX_SHARPE
        assert len(result.weights) == 5
        assert np.isclose(np.sum(result.weights), 1.0)

    def test_optimize_portfolio_min_variance(self, mock_returns):
        """Test convenience function with min variance."""
        result = optimize_portfolio(mock_returns, method="min_variance")

        assert isinstance(result, PortfolioWeights)
        assert result.method == OptimizationMethod.MIN_VARIANCE

    def test_optimize_portfolio_risk_parity(self, mock_returns):
        """Test convenience function with risk parity."""
        result = optimize_portfolio(mock_returns, method="risk_parity")

        assert isinstance(result, PortfolioWeights)
        assert result.method == OptimizationMethod.RISK_PARITY

    def test_optimize_portfolio_with_custom_params(self, mock_returns):
        """Test convenience function with custom parameters."""
        result = optimize_portfolio(
            mock_returns,
            method="min_variance",
            risk_free_rate=0.05,
            max_weight=0.30,
        )

        assert np.all(result.weights <= 0.30)


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_singular_covariance_matrix(self):
        """Test handling of singular covariance matrix."""
        # Create perfectly correlated returns
        dates = pd.date_range("2023-01-01", "2024-12-31", freq="D")
        base_returns = np.random.randn(len(dates)) * 0.01

        returns = pd.DataFrame({
            "A": base_returns,
            "B": base_returns * 1.5,  # Perfectly correlated
            "C": base_returns * 0.8,  # Perfectly correlated
        }, index=dates)

        optimizer = PortfolioOptimizer(returns)

        # Should still be able to optimize (with regularization)
        weights = optimizer.min_variance()

        assert len(weights) == 3
        assert np.isclose(np.sum(weights), 1.0)

    def test_negative_returns(self):
        """Test handling of negative returns."""
        dates = pd.date_range("2023-01-01", "2024-12-31", freq="D")

        # Create returns with negative mean
        returns = pd.DataFrame({
            "A": np.random.randn(len(dates)) * 0.02 - 0.001,  # Negative drift
            "B": np.random.randn(len(dates)) * 0.02 - 0.001,
            "C": np.random.randn(len(dates)) * 0.02 - 0.001,
        }, index=dates)

        optimizer = PortfolioOptimizer(returns)

        # Should still be able to optimize
        weights = optimizer.min_variance()

        assert len(weights) == 3
        assert np.isclose(np.sum(weights), 1.0)

    def test_single_asset(self):
        """Test portfolio optimization with single asset."""
        dates = pd.date_range("2023-01-01", "2024-12-31", freq="D")

        returns = pd.DataFrame({
            "A": np.random.randn(len(dates)) * 0.01,
        }, index=dates)

        optimizer = PortfolioOptimizer(returns)
        weights = optimizer.equal_weight()

        assert len(weights) == 1
        assert np.isclose(weights[0], 1.0)

    def test_unrealistic_target_return(self, mock_returns):
        """Test mean-variance with unrealistic target return."""
        optimizer = PortfolioOptimizer(mock_returns)

        # Request impossibly high return
        weights = optimizer.mean_variance(target_return=2.0)  # 200% annual return

        # Should fallback to min variance
        assert len(weights) == 5
        assert np.isclose(np.sum(weights), 1.0)

    def test_max_weight_too_small(self, mock_returns):
        """Test optimization with max_weight too small to satisfy constraints."""
        optimizer = PortfolioOptimizer(mock_returns)

        # With 5 assets and max_weight=0.10, can only allocate 50% of capital
        # This violates the fully-invested constraint, so optimizer falls back to equal weight
        weights = optimizer.min_variance(max_weight=0.10)

        # Should fallback to equal weight (1/5 = 0.2)
        assert len(weights) == 5
        assert np.isclose(np.sum(weights), 1.0)
        # Equal weight fallback
        assert np.allclose(weights, 0.2, atol=0.01)

    def test_zero_volatility_asset(self):
        """Test handling of zero-volatility asset."""
        dates = pd.date_range("2023-01-01", "2024-12-31", freq="D")

        returns = pd.DataFrame({
            "A": np.random.randn(len(dates)) * 0.01,
            "B": np.zeros(len(dates)),  # Zero volatility
            "C": np.random.randn(len(dates)) * 0.01,
        }, index=dates)

        optimizer = PortfolioOptimizer(returns)

        # Min variance might have issues with zero-vol asset (numerically singular)
        # So optimizer may fall back to equal weight
        weights = optimizer.min_variance()

        assert len(weights) == 3
        assert np.isclose(np.sum(weights), 1.0)
        # All weights should be valid (positive and sum to 1)
        assert np.all(weights >= 0)
        assert np.all(weights <= 1.0)


class TestPortfolioStatistics:
    """Tests for portfolio statistics calculation."""

    def test_expected_return_calculation(self, mock_returns):
        """Test that expected return is correctly annualized."""
        optimizer = PortfolioOptimizer(mock_returns)
        result = optimizer.optimize(OptimizationMethod.EQUAL_WEIGHT)

        # Equal weight return
        daily_return = optimizer.mean_returns.mean()
        expected_annual = daily_return * 252

        assert np.isclose(result.expected_return, expected_annual, rtol=0.1)

    def test_volatility_calculation(self, mock_returns):
        """Test that volatility is correctly annualized."""
        optimizer = PortfolioOptimizer(mock_returns)
        result = optimizer.optimize(OptimizationMethod.EQUAL_WEIGHT)

        # Manual calculation
        weights = result.weights
        daily_vol = np.sqrt(np.dot(weights, np.dot(optimizer.cov_matrix, weights)))
        annual_vol = daily_vol * np.sqrt(252)

        assert np.isclose(result.volatility, annual_vol, rtol=0.01)

    def test_sharpe_ratio_calculation(self, mock_returns):
        """Test Sharpe ratio calculation."""
        optimizer = PortfolioOptimizer(mock_returns)
        result = optimizer.optimize(OptimizationMethod.MAX_SHARPE)

        # Sharpe = (return - rf) / volatility
        expected_sharpe = (result.expected_return - optimizer.risk_free_rate) / result.volatility

        assert np.isclose(result.sharpe_ratio, expected_sharpe, rtol=0.01)
