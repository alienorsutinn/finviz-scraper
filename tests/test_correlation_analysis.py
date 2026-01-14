"""Tests for correlation analysis module."""
from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from finviz_weekly.correlation_analysis import (
    CorrelationAnalyzer,
    CorrelationConfig,
    CorrelationResult,
    DecayResult,
)


@pytest.fixture
def mock_factor_data():
    """Create mock factor data for testing."""
    np.random.seed(42)
    n_samples = 500

    # Create factors with known correlations
    base = np.random.randn(n_samples)

    data = pd.DataFrame({
        "score_quality": base + np.random.randn(n_samples) * 0.3,  # Correlated with base
        "score_value": -base + np.random.randn(n_samples) * 0.3,  # Anti-correlated
        "score_growth": np.random.randn(n_samples),  # Independent
        "score_momentum": base * 0.5 + np.random.randn(n_samples) * 0.5,  # Partially correlated
        "score_risk": np.random.randn(n_samples),  # Independent
        "score_oversold": np.random.randn(n_samples),  # Independent
    })

    return data


class TestCorrelationConfig:
    """Tests for CorrelationConfig."""

    def test_default_config(self):
        """Test default configuration."""
        config = CorrelationConfig()

        assert config.rolling_window_days == 90
        assert config.min_observations == 30
        assert config.n_clusters == 3
        assert config.variance_threshold == 0.95

    def test_custom_config(self):
        """Test custom configuration."""
        config = CorrelationConfig(
            rolling_window_days=60,
            n_clusters=4,
            n_components=5,
        )

        assert config.rolling_window_days == 60
        assert config.n_clusters == 4
        assert config.n_components == 5


class TestCorrelationAnalyzer:
    """Tests for CorrelationAnalyzer."""

    def test_init(self):
        """Test analyzer initialization."""
        analyzer = CorrelationAnalyzer()
        assert analyzer.config is not None

    def test_init_with_config(self):
        """Test initialization with custom config."""
        config = CorrelationConfig(n_clusters=2)
        analyzer = CorrelationAnalyzer(config)
        assert analyzer.config.n_clusters == 2

    def test_analyze_basic(self, mock_factor_data):
        """Test basic correlation analysis."""
        analyzer = CorrelationAnalyzer()
        result = analyzer.analyze(mock_factor_data)

        assert isinstance(result, CorrelationResult)
        assert len(result.factors_analyzed) == 6
        assert not result.correlation_matrix.empty
        assert isinstance(result.factor_clusters, dict)

    def test_analyze_correlation_matrix(self, mock_factor_data):
        """Test correlation matrix computation."""
        analyzer = CorrelationAnalyzer()
        result = analyzer.analyze(mock_factor_data)

        corr_matrix = result.correlation_matrix

        # Check matrix properties
        assert corr_matrix.shape == (6, 6)
        assert all(corr_matrix.index == corr_matrix.columns)

        # Diagonal should be 1
        for i in range(len(corr_matrix)):
            assert abs(corr_matrix.iloc[i, i] - 1.0) < 0.01

        # Quality and value should be anti-correlated
        assert result.correlation_matrix.loc["score_quality", "score_value"] < 0

    def test_analyze_high_correlations(self, mock_factor_data):
        """Test high correlation pair detection."""
        analyzer = CorrelationAnalyzer()
        result = analyzer.analyze(mock_factor_data, correlation_threshold=0.5)

        # Should find quality-momentum correlation
        pair_names = [(f1, f2) for f1, f2, _ in result.high_correlation_pairs]
        high_corr_factors = set()
        for f1, f2 in pair_names:
            high_corr_factors.add(f1)
            high_corr_factors.add(f2)

        # Quality and momentum should be detected as correlated
        assert "score_quality" in high_corr_factors or "score_momentum" in high_corr_factors

    def test_analyze_clustering(self, mock_factor_data):
        """Test factor clustering."""
        config = CorrelationConfig(n_clusters=2)
        analyzer = CorrelationAnalyzer(config)
        result = analyzer.analyze(mock_factor_data)

        # Check cluster assignments
        assert len(result.factor_clusters) == 6
        assert all(isinstance(c, int) for c in result.factor_clusters.values())

        # Check cluster centroids
        assert len(result.cluster_centroids) <= 2

    def test_analyze_pca(self, mock_factor_data):
        """Test PCA analysis."""
        config = CorrelationConfig(n_components=3)
        analyzer = CorrelationAnalyzer(config)
        result = analyzer.analyze(mock_factor_data)

        # Check PCA results
        assert len(result.pca_explained_variance) == 3
        assert result.pca_cumulative_variance > 0
        assert result.pca_cumulative_variance <= 1.0

        # Variance should sum correctly
        assert abs(sum(result.pca_explained_variance) - result.pca_cumulative_variance) < 0.01

    def test_analyze_insufficient_data(self):
        """Test with insufficient data."""
        analyzer = CorrelationAnalyzer()

        small_data = pd.DataFrame({
            "score_quality": [1, 2, 3],
            "score_value": [3, 2, 1],
        })

        with pytest.raises(ValueError, match="Insufficient data"):
            analyzer.analyze(small_data)

    def test_analyze_insufficient_factors(self, mock_factor_data):
        """Test with insufficient factors."""
        analyzer = CorrelationAnalyzer()

        with pytest.raises(ValueError, match="Need at least 2 factors"):
            analyzer.analyze(mock_factor_data[["score_quality"]])

    def test_analyze_decay(self, mock_factor_data):
        """Test factor decay analysis."""
        analyzer = CorrelationAnalyzer()

        result = analyzer.analyze_decay(
            mock_factor_data,
            factor="score_quality",
            horizons=[5, 10, 20],
        )

        assert isinstance(result, DecayResult)
        assert result.factor_name == "score_quality"
        assert len(result.horizons) == 3
        assert len(result.autocorrelations) == 3

    def test_analyze_decay_nonexistent_factor(self, mock_factor_data):
        """Test decay analysis with nonexistent factor."""
        analyzer = CorrelationAnalyzer()

        with pytest.raises(ValueError, match="not found"):
            analyzer.analyze_decay(mock_factor_data, factor="nonexistent")

    def test_get_redundant_factors(self, mock_factor_data):
        """Test redundant factor detection."""
        analyzer = CorrelationAnalyzer()
        result = analyzer.analyze(mock_factor_data, correlation_threshold=0.5)

        redundant = analyzer.get_redundant_factors(result, threshold=0.6)

        # Should return a list
        assert isinstance(redundant, list)

    def test_suggest_factor_combinations(self, mock_factor_data):
        """Test factor combination suggestions."""
        analyzer = CorrelationAnalyzer()
        result = analyzer.analyze(mock_factor_data)

        suggestions = analyzer.suggest_factor_combinations(result)

        assert isinstance(suggestions, dict)
        # Each cluster should have suggestions
        for cluster_name, factors in suggestions.items():
            assert isinstance(factors, list)


class TestCorrelationResult:
    """Tests for CorrelationResult."""

    def test_to_dict(self, mock_factor_data):
        """Test result serialization."""
        analyzer = CorrelationAnalyzer()
        result = analyzer.analyze(mock_factor_data)

        result_dict = result.to_dict()

        assert isinstance(result_dict, dict)
        assert "analysis_date" in result_dict
        assert "factors_analyzed" in result_dict
        assert "correlation_matrix" in result_dict
        assert "high_correlation_pairs" in result_dict


class TestRollingCorrelations:
    """Tests for rolling correlation analysis."""

    def test_rolling_correlations(self, mock_factor_data):
        """Test rolling correlation computation."""
        config = CorrelationConfig(rolling_window_days=50, min_observations=20)
        analyzer = CorrelationAnalyzer(config)

        result = analyzer.analyze(mock_factor_data)

        # Should have rolling correlation stats
        assert not result.rolling_correlation_means.empty
        assert not result.rolling_correlation_stds.empty


class TestEdgeCases:
    """Tests for edge cases."""

    def test_constant_factor(self):
        """Test with a constant factor."""
        np.random.seed(42)
        data = pd.DataFrame({
            "score_quality": np.random.randn(100),
            "score_value": np.ones(100),  # Constant
        })

        analyzer = CorrelationAnalyzer()
        # Should handle constant factor gracefully
        result = analyzer.analyze(data)
        assert isinstance(result, CorrelationResult)

    def test_nan_values(self):
        """Test with NaN values."""
        np.random.seed(42)
        data = pd.DataFrame({
            "score_quality": np.random.randn(100),
            "score_value": np.random.randn(100),
        })
        data.loc[10:20, "score_quality"] = np.nan

        analyzer = CorrelationAnalyzer()
        result = analyzer.analyze(data)

        assert isinstance(result, CorrelationResult)
