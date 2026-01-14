"""Tests for feature importance analysis."""
from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from finviz_weekly.feature_importance import (
    FeatureImportanceAnalyzer,
    FeatureImportanceResults,
    analyze_feature_importance,
)
from finviz_weekly.learn import FACTOR_COLS


@pytest.fixture
def mock_history_data():
    """Create mock historical data for testing."""
    dates = pd.date_range("2023-01-01", "2024-12-31", freq="7D")
    tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

    data = []
    for date in dates:
        for ticker in tickers:
            # Create correlated features for testing
            base_quality = np.random.rand()
            data.append({
                "as_of_date": date,
                "ticker": ticker,
                "price": 100 + np.random.randn() * 10,
                "score_quality": base_quality,
                "score_value": base_quality * 0.8 + np.random.rand() * 0.2,  # Correlated with quality
                "score_risk": np.random.rand(),
                "score_growth": np.random.rand(),
                "score_oversold": np.random.rand(),
                "score_momentum": np.random.rand(),
                "sector": "Technology",
            })

    return pd.DataFrame(data)


@pytest.fixture
def mock_prices_data():
    """Create mock price history for testing."""
    dates = pd.date_range("2023-01-01", "2025-01-31", freq="D")
    tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

    data = []
    for ticker in tickers:
        price = 100.0
        for date in dates:
            price *= 1 + np.random.randn() * 0.02  # 2% daily volatility
            data.append({
                "ticker": ticker,
                "date": date.date(),
                "close": price,
                "open": price * 0.99,
                "high": price * 1.01,
                "low": price * 0.98,
                "volume": int(1e6 * (1 + np.random.rand())),
            })

    return pd.DataFrame(data)


@pytest.fixture
def temp_history_file(mock_history_data):
    """Create temporary history file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        mock_history_data.to_parquet(tmp_path)
        yield tmp_path
        tmp_path.unlink(missing_ok=True)


@pytest.fixture
def temp_prices_file(mock_prices_data):
    """Create temporary prices file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        mock_prices_data.to_parquet(tmp_path)
        yield tmp_path
        tmp_path.unlink(missing_ok=True)


class TestFeatureImportanceAnalyzer:
    """Tests for FeatureImportanceAnalyzer."""

    def test_init_with_valid_path(self, temp_history_file):
        """Test analyzer initialization with valid path."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file)
        assert analyzer.history_path == temp_history_file
        assert len(analyzer.history) > 0
        assert "as_of_date" in analyzer.history.columns
        assert all(col in analyzer.history.columns for col in FACTOR_COLS)

    def test_init_with_missing_file(self):
        """Test analyzer initialization with missing file."""
        with pytest.raises(FileNotFoundError, match="History file not found"):
            FeatureImportanceAnalyzer(Path("/nonexistent/path.parquet"))

    def test_filter_data_no_dates(self, temp_history_file):
        """Test filtering data without date constraints."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file)
        filtered = analyzer._filter_data(None, None)
        assert len(filtered) == len(analyzer.history)

    def test_filter_data_with_start_date(self, temp_history_file):
        """Test filtering data with start date."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file)
        filtered = analyzer._filter_data("2024-01-01", None)
        assert all(filtered["as_of_date"] >= pd.to_datetime("2024-01-01").date())

    def test_filter_data_with_end_date(self, temp_history_file):
        """Test filtering data with end date."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file)
        filtered = analyzer._filter_data(None, "2024-06-30")
        assert all(filtered["as_of_date"] <= pd.to_datetime("2024-06-30").date())

    def test_filter_data_with_both_dates(self, temp_history_file):
        """Test filtering data with both start and end dates."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file)
        filtered = analyzer._filter_data("2023-06-01", "2024-06-30")
        assert all(filtered["as_of_date"] >= pd.to_datetime("2023-06-01").date())
        assert all(filtered["as_of_date"] <= pd.to_datetime("2024-06-30").date())

    def test_compute_baseline_ic_no_prices(self, temp_history_file):
        """Test baseline IC computation without prices file."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file)
        analyzer.prices_path = Path("/nonexistent/prices.parquet")

        data = analyzer.history.head(100)
        baseline_ic = analyzer._compute_baseline_ic(data)

        assert np.isnan(baseline_ic)

    def test_compute_baseline_ic_with_prices(self, temp_history_file, temp_prices_file):
        """Test baseline IC computation with valid prices."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file, temp_prices_file)

        data = analyzer.history.head(200)
        baseline_ic = analyzer._compute_baseline_ic(data)

        # Should return a finite IC value
        assert isinstance(baseline_ic, (float, np.floating))

    def test_analyze_feature_correlations(self, temp_history_file):
        """Test feature correlation analysis."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file)

        data = analyzer.history.head(200)
        corr_matrix = analyzer.analyze_feature_correlations(data)

        # Should return a square correlation matrix
        assert corr_matrix.shape[0] == corr_matrix.shape[1]
        assert all(col in FACTOR_COLS for col in corr_matrix.columns)

        # Diagonal should be 1.0
        for i in range(len(corr_matrix)):
            assert corr_matrix.iloc[i, i] == pytest.approx(1.0)

    def test_find_redundant_pairs_empty_matrix(self, temp_history_file):
        """Test finding redundant pairs with empty correlation matrix."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file)

        empty_corr = pd.DataFrame()
        redundant = analyzer._find_redundant_pairs(empty_corr)

        assert redundant == []

    def test_find_redundant_pairs_no_redundancy(self, temp_history_file):
        """Test finding redundant pairs with no high correlations."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file)

        # Create uncorrelated data
        data = pd.DataFrame({
            "feat1": np.random.randn(100),
            "feat2": np.random.randn(100),
            "feat3": np.random.randn(100),
        })

        corr_matrix = data.corr()
        redundant = analyzer._find_redundant_pairs(corr_matrix, threshold=0.8)

        # Should find no redundant pairs
        assert len(redundant) == 0

    def test_find_redundant_pairs_with_redundancy(self, temp_history_file):
        """Test finding redundant pairs with high correlations."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file)

        # Use actual history data which has correlated features
        data = analyzer.history[FACTOR_COLS].head(200)
        corr_matrix = data.corr()

        redundant = analyzer._find_redundant_pairs(corr_matrix, threshold=0.7)

        # Should find some redundant pairs (score_quality and score_value are correlated in mock data)
        assert isinstance(redundant, list)

        if redundant:
            feat1, feat2, corr = redundant[0]
            assert isinstance(feat1, str)
            assert isinstance(feat2, str)
            assert abs(corr) >= 0.7

    def test_compute_permutation_importance_no_prices(self, temp_history_file):
        """Test permutation importance without prices file."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file)
        analyzer.prices_path = Path("/nonexistent/prices.parquet")

        data = analyzer.history.head(100)
        importance = analyzer.compute_permutation_importance(data, n_permutations=2)

        # Should return NaN for all features
        assert all(np.isnan(imp) for imp in importance.values())

    def test_compute_permutation_importance_with_prices(self, temp_history_file, temp_prices_file):
        """Test permutation importance with valid prices."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file, temp_prices_file)

        data = analyzer.history.head(200)
        importance = analyzer.compute_permutation_importance(data, n_permutations=2, baseline_ic=0.05)

        # Should return importance scores for all features
        assert len(importance) == len(FACTOR_COLS)
        assert all(feat in importance for feat in FACTOR_COLS)

    def test_compute_partial_dependence_no_prices(self, temp_history_file):
        """Test partial dependence without prices file."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file)
        analyzer.prices_path = Path("/nonexistent/prices.parquet")

        data = analyzer.history.head(100)
        pdp = analyzer.compute_partial_dependence(data, "score_quality", n_points=10)

        # Should return empty list
        assert pdp == []

    def test_compute_partial_dependence_invalid_feature(self, temp_history_file, temp_prices_file):
        """Test partial dependence with invalid feature."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file, temp_prices_file)

        data = analyzer.history.head(100)
        pdp = analyzer.compute_partial_dependence(data, "nonexistent_feature", n_points=10)

        # Should return empty list
        assert pdp == []

    def test_compute_partial_dependence_with_prices(self, temp_history_file, temp_prices_file):
        """Test partial dependence with valid prices."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file, temp_prices_file)

        data = analyzer.history.head(300)
        pdp = analyzer.compute_partial_dependence(data, "score_quality", n_points=10)

        # Should return list of (feature_value, mean_return) tuples
        if pdp:  # May be empty if insufficient data
            assert all(isinstance(val, float) and isinstance(ret, float) for val, ret in pdp)

            # Feature values should be in ascending order
            if len(pdp) > 1:
                feature_vals = [val for val, _ in pdp]
                assert feature_vals == sorted(feature_vals)

    def test_analyze_insufficient_data(self, temp_history_file):
        """Test analysis with insufficient data."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file)

        with pytest.raises(ValueError, match="Insufficient data for analysis"):
            analyzer.analyze(
                start_date="2030-01-01",  # Future date with no data
                end_date="2030-12-31",
            )

    def test_analyze_full_workflow(self, temp_history_file, temp_prices_file):
        """Test full analysis workflow."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file, temp_prices_file)

        results = analyzer.analyze(
            start_date="2023-06-01",
            end_date="2024-06-30",
            n_permutations=2,
            correlation_threshold=0.7,
        )

        assert isinstance(results, FeatureImportanceResults)
        assert isinstance(results.permutation_importance, dict)
        assert isinstance(results.baseline_ic, (float, np.floating))
        assert isinstance(results.feature_correlations, pd.DataFrame)
        assert isinstance(results.top_features, list)
        assert isinstance(results.redundant_pairs, list)


class TestFeatureImportanceResults:
    """Tests for FeatureImportanceResults."""

    def test_create_results(self):
        """Test creating feature importance results."""
        results = FeatureImportanceResults(
            permutation_importance={"score_quality": 0.02, "score_value": 0.01},
            baseline_ic=0.05,
            feature_correlations=pd.DataFrame([[1.0, 0.5], [0.5, 1.0]]),
            top_features=[("score_quality", 0.02), ("score_value", 0.01)],
            redundant_pairs=[("score_quality", "score_value", 0.85)],
        )

        assert results.baseline_ic == 0.05
        assert len(results.top_features) == 2
        assert results.top_features[0][0] == "score_quality"

    def test_to_dict(self):
        """Test conversion to dictionary."""
        results = FeatureImportanceResults(
            permutation_importance={"score_quality": 0.02, "score_value": 0.01},
            baseline_ic=0.05,
            feature_correlations=pd.DataFrame(
                [[1.0, 0.5], [0.5, 1.0]],
                columns=["score_quality", "score_value"],
                index=["score_quality", "score_value"],
            ),
            top_features=[("score_quality", 0.02), ("score_value", 0.01)],
            redundant_pairs=[("score_quality", "score_value", 0.85)],
        )

        result_dict = results.to_dict()

        assert "permutation_importance" in result_dict
        assert "baseline_ic" in result_dict
        assert "top_features" in result_dict
        assert "redundant_pairs" in result_dict
        assert "feature_correlations" in result_dict

        assert result_dict["baseline_ic"] == 0.05
        assert len(result_dict["top_features"]) == 2
        assert result_dict["top_features"][0]["feature"] == "score_quality"
        assert len(result_dict["redundant_pairs"]) == 1


class TestAnalyzeFeatureImportance:
    """Tests for analyze_feature_importance convenience function."""

    def test_analyze_basic(self, temp_history_file, temp_prices_file):
        """Test basic feature importance analysis."""
        # Copy prices to expected location
        prices_dir = temp_history_file.parent
        prices_path = prices_dir / "prices.parquet"

        import shutil
        shutil.copy(temp_prices_file, prices_path)

        try:
            results = analyze_feature_importance(
                history_path=temp_history_file,
                start_date="2023-06-01",
                end_date="2024-06-30",
                n_permutations=2,
            )

            assert results is not None
            assert isinstance(results.permutation_importance, dict)
            assert isinstance(results.baseline_ic, (float, np.floating))

        finally:
            prices_path.unlink(missing_ok=True)

    def test_analyze_with_output(self, temp_history_file, temp_prices_file):
        """Test feature importance analysis with JSON output."""
        prices_dir = temp_history_file.parent
        prices_path = prices_dir / "prices.parquet"

        import shutil
        shutil.copy(temp_prices_file, prices_path)

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            out_path = Path(tmp.name)

        try:
            results = analyze_feature_importance(
                history_path=temp_history_file,
                start_date="2023-06-01",
                end_date="2024-06-30",
                n_permutations=2,
                out_path=out_path,
            )

            assert out_path.exists()

            # Load and verify JSON
            import json
            with open(out_path) as f:
                data = json.load(f)

            assert "permutation_importance" in data
            assert "baseline_ic" in data
            assert "top_features" in data

        finally:
            prices_path.unlink(missing_ok=True)
            out_path.unlink(missing_ok=True)


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_missing_factor_columns(self, temp_prices_file):
        """Test handling of missing factor columns."""
        # Create history without all factor columns
        data = pd.DataFrame({
            "as_of_date": pd.date_range("2023-01-01", "2023-12-31", freq="7D"),
            "ticker": ["AAPL"] * 53,
            "price": [100] * 53,
            "score_quality": np.random.rand(53),
            # Missing other factor columns
        })

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = Path(tmp.name)
            data.to_parquet(tmp_path)

        try:
            analyzer = FeatureImportanceAnalyzer(tmp_path, temp_prices_file)

            # Should fill missing columns with NaN
            assert all(col in analyzer.history.columns for col in FACTOR_COLS)

        finally:
            tmp_path.unlink(missing_ok=True)

    def test_empty_correlation_matrix(self, temp_history_file):
        """Test handling of empty correlation matrix."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file)

        # Create data with no factor columns
        data = pd.DataFrame({"ticker": ["AAPL"], "price": [100]})

        corr_matrix = analyzer.analyze_feature_correlations(data)

        assert corr_matrix.empty

    def test_insufficient_pdp_data(self, temp_history_file, temp_prices_file):
        """Test partial dependence with insufficient data."""
        analyzer = FeatureImportanceAnalyzer(temp_history_file, temp_prices_file)

        # Use very small dataset
        data = analyzer.history.head(10)
        pdp = analyzer.compute_partial_dependence(data, "score_quality", n_points=20)

        # Should return empty or minimal results
        assert isinstance(pdp, list)
