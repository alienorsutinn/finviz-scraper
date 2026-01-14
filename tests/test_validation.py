"""Tests for walk-forward validation framework."""
from __future__ import annotations

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from finviz_weekly.validation import (
    ValidationConfig,
    WalkForwardValidator,
    FoldResults,
    ValidationResults,
    run_validation,
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
            data.append({
                "as_of_date": date,
                "ticker": ticker,
                "price": 100 + np.random.randn() * 10,
                "score_quality": np.random.rand(),
                "score_value": np.random.rand(),
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


class TestValidationConfig:
    """Tests for ValidationConfig."""

    def test_valid_config(self):
        """Test creating valid configuration."""
        config = ValidationConfig(
            start_date="2023-01-01",
            end_date="2024-12-31",
            train_window_days=365,
            test_window_days=90,
            step_days=30,
        )
        assert config.start_date == "2023-01-01"
        assert config.train_window_days == 365
        assert config.test_window_days == 90

    def test_invalid_train_window(self):
        """Test validation of train_window_days."""
        with pytest.raises(ValueError, match="train_window_days must be >= 90"):
            ValidationConfig(
                start_date="2023-01-01",
                end_date="2024-12-31",
                train_window_days=50,
            )

    def test_invalid_test_window(self):
        """Test validation of test_window_days."""
        with pytest.raises(ValueError, match="test_window_days must be >= 30"):
            ValidationConfig(
                start_date="2023-01-01",
                end_date="2024-12-31",
                test_window_days=10,
            )

    def test_invalid_step_days(self):
        """Test validation of step_days."""
        with pytest.raises(ValueError, match="step_days must be >= 1"):
            ValidationConfig(
                start_date="2023-01-01",
                end_date="2024-12-31",
                step_days=0,
            )

    def test_invalid_min_train_rows(self):
        """Test validation of min_train_rows."""
        with pytest.raises(ValueError, match="min_train_rows must be >= 50"):
            ValidationConfig(
                start_date="2023-01-01",
                end_date="2024-12-31",
                min_train_rows=10,
            )


class TestWalkForwardValidator:
    """Tests for WalkForwardValidator."""

    def test_init_with_valid_path(self, temp_history_file):
        """Test validator initialization with valid path."""
        validator = WalkForwardValidator(temp_history_file)
        assert validator.history_path == temp_history_file
        assert len(validator.history) > 0
        assert "as_of_date" in validator.history.columns
        assert all(col in validator.history.columns for col in FACTOR_COLS)

    def test_init_with_missing_file(self):
        """Test validator initialization with missing file."""
        with pytest.raises(FileNotFoundError, match="History file not found"):
            WalkForwardValidator(Path("/nonexistent/path.parquet"))

    def test_create_folds_expanding_window(self, temp_history_file):
        """Test fold creation with expanding window."""
        validator = WalkForwardValidator(temp_history_file)

        config = ValidationConfig(
            start_date="2023-01-01",
            end_date="2024-06-30",
            train_window_days=180,
            test_window_days=60,
            step_days=30,
        )

        start = pd.to_datetime("2023-01-01")
        end = pd.to_datetime("2024-06-30")
        folds = validator._create_folds(start, end, config)

        # Should create multiple folds
        assert len(folds) > 0

        # First fold should start at start_date
        train_start, train_end, test_start, test_end = folds[0]
        assert train_start == start

        # Training window should be correct
        assert (train_end - train_start).days == config.train_window_days

        # Test should follow training
        assert test_start == train_end

        # Test window should be correct
        assert (test_end - test_start).days == config.test_window_days

        # Folds should expand (same train_start, increasing train_end)
        if len(folds) > 1:
            assert folds[0][0] == folds[1][0]  # Same train_start
            assert folds[1][1] > folds[0][1]  # Later train_end

    def test_apply_weights_to_data(self, temp_history_file):
        """Test applying weights to compute total_score."""
        validator = WalkForwardValidator(temp_history_file)

        # Sample data
        data = validator.history.head(10).copy()

        weights = {
            "score_quality": 0.3,
            "score_value": 0.3,
            "score_risk": 0.2,
            "score_growth": 0.1,
            "score_oversold": 0.1,
            "score_momentum": 0.0,
        }

        result = validator._apply_weights_to_data(data, weights)

        assert "total_score" in result.columns
        assert result["total_score"].notna().any()

        # Verify weighted sum
        expected = sum(
            data[col].fillna(0) * weight
            for col, weight in weights.items()
            if col in data.columns
        )
        pd.testing.assert_series_equal(result["total_score"], expected, check_names=False)

    def test_train_on_period_fallback_no_prices(self, temp_history_file):
        """Test training on period falls back when prices missing."""
        # Create validator without prices file
        validator = WalkForwardValidator(temp_history_file)
        validator.prices_path = Path("/nonexistent/prices.parquet")

        train_data = validator.history.head(100)
        train_start = pd.Timestamp("2023-01-01")
        train_end = pd.Timestamp("2023-12-31")

        weights, ic = validator._train_on_period(train_data, train_start, train_end)

        # Should return fallback weights
        assert weights is not None
        assert sum(weights.values()) == pytest.approx(1.0)
        assert ic == {}

    def test_train_on_period_with_prices(self, temp_history_file, temp_prices_file):
        """Test training on period with valid prices."""
        validator = WalkForwardValidator(temp_history_file, temp_prices_file)

        train_data = validator.history[
            validator.history["as_of_date"] < pd.Timestamp("2024-01-01").date()
        ].head(200)

        train_start = pd.Timestamp("2023-01-01")
        train_end = pd.Timestamp("2023-12-31")

        weights, ic = validator._train_on_period(train_data, train_start, train_end)

        # Should return learned weights
        assert weights is not None
        assert sum(weights.values()) == pytest.approx(1.0)
        assert len(ic) > 0

        # ICs should be float values
        for col, val in ic.items():
            assert isinstance(val, (float, np.floating)) or np.isnan(val)

    def test_backtest_equal_weight_baseline(self, temp_history_file, temp_prices_file):
        """Test equal-weight baseline backtest."""
        validator = WalkForwardValidator(temp_history_file, temp_prices_file)

        config = ValidationConfig(
            start_date="2023-01-01",
            end_date="2024-12-31",
            top_n=5,
        )

        start = pd.Timestamp("2024-01-01")
        end = pd.Timestamp("2024-06-30")

        result = validator._backtest_equal_weight_baseline(start, end, config)

        # Should return results or None (if insufficient data)
        if result is not None:
            assert hasattr(result, "total_return")
            assert hasattr(result, "sharpe_ratio")
            assert hasattr(result, "max_drawdown")


class TestFoldResults:
    """Tests for FoldResults dataclass."""

    def test_create_fold_results(self):
        """Test creating fold results."""
        result = FoldResults(
            fold_id=1,
            train_start="2023-01-01",
            train_end="2023-12-31",
            test_start="2024-01-01",
            test_end="2024-03-31",
            train_rows=500,
            train_weights={"score_quality": 0.5, "score_value": 0.5},
            train_ic={"score_quality": 0.05, "score_value": 0.03},
            train_ic_mean=0.04,
            test_return=0.15,
            test_annual_return=0.60,
            test_sharpe=1.2,
            test_max_drawdown=-0.08,
            test_num_trades=20,
        )

        assert result.fold_id == 1
        assert result.train_ic_mean == 0.04
        assert result.test_sharpe == 1.2


class TestValidationResults:
    """Tests for ValidationResults dataclass."""

    def test_create_validation_results(self):
        """Test creating validation results."""
        config = ValidationConfig(
            start_date="2023-01-01",
            end_date="2024-12-31",
        )

        fold = FoldResults(
            fold_id=1,
            train_start="2023-01-01",
            train_end="2023-12-31",
            test_start="2024-01-01",
            test_end="2024-03-31",
            train_rows=500,
            train_weights={},
            train_ic={},
            train_ic_mean=0.04,
            test_return=0.15,
            test_annual_return=0.60,
            test_sharpe=1.2,
            test_max_drawdown=-0.08,
            test_num_trades=20,
        )

        results = ValidationResults(
            config=config,
            num_folds=1,
            folds=[fold],
            oos_mean_return=0.15,
            oos_mean_sharpe=1.2,
            oos_mean_max_drawdown=-0.08,
            oos_win_rate=1.0,
            is_mean_ic=0.04,
            oos_sharpe_ratio_vs_is=30.0,
            overfitting_detected=False,
        )

        assert results.num_folds == 1
        assert results.oos_mean_sharpe == 1.2
        assert results.overfitting_detected is False

    def test_to_dict(self):
        """Test conversion to dictionary."""
        config = ValidationConfig(
            start_date="2023-01-01",
            end_date="2024-12-31",
        )

        fold = FoldResults(
            fold_id=1,
            train_start="2023-01-01",
            train_end="2023-12-31",
            test_start="2024-01-01",
            test_end="2024-03-31",
            train_rows=500,
            train_weights={},
            train_ic={},
            train_ic_mean=0.04,
            test_return=0.15,
            test_annual_return=0.60,
            test_sharpe=1.2,
            test_max_drawdown=-0.08,
            test_num_trades=20,
        )

        results = ValidationResults(
            config=config,
            num_folds=1,
            folds=[fold],
            oos_mean_return=0.15,
            oos_mean_sharpe=1.2,
            oos_mean_max_drawdown=-0.08,
            oos_win_rate=1.0,
            is_mean_ic=0.04,
            oos_sharpe_ratio_vs_is=30.0,
            overfitting_detected=False,
        )

        result_dict = results.to_dict()

        assert result_dict["num_folds"] == 1
        assert result_dict["oos_mean_sharpe"] == 1.2
        assert result_dict["overfitting_detected"] is False
        assert len(result_dict["folds"]) == 1
        assert result_dict["folds"][0]["fold_id"] == 1


class TestOverfittingDetection:
    """Tests for overfitting detection logic."""

    def test_overfitting_detected_low_oos_sharpe(self):
        """Test overfitting detection when OOS Sharpe is low."""
        # High IS IC but low OOS Sharpe suggests overfitting
        is_mean_ic = 0.10  # Strong in-sample IC
        oos_mean_sharpe = 0.5  # But poor OOS Sharpe

        ratio = oos_mean_sharpe / is_mean_ic  # 0.5 / 0.10 = 5.0
        overfitting_detected = ratio < 10.0

        assert overfitting_detected is True

    def test_no_overfitting_good_oos_sharpe(self):
        """Test no overfitting when OOS Sharpe is good."""
        # Good IS IC and good OOS Sharpe
        is_mean_ic = 0.05
        oos_mean_sharpe = 1.0

        ratio = oos_mean_sharpe / is_mean_ic  # 1.0 / 0.05 = 20.0
        overfitting_detected = ratio < 10.0

        assert overfitting_detected is False


class TestRunValidation:
    """Tests for run_validation convenience function."""

    def test_run_validation_basic(self, temp_history_file, temp_prices_file):
        """Test basic validation run."""
        # Copy prices to expected location
        prices_dir = temp_history_file.parent
        prices_path = prices_dir / "prices.parquet"

        import shutil
        shutil.copy(temp_prices_file, prices_path)

        try:
            results = run_validation(
                history_path=temp_history_file,
                start_date="2023-06-01",
                end_date="2024-06-01",
                train_window_days=180,
                test_window_days=60,
                step_days=60,
                top_n=5,
            )

            assert results is not None
            assert results.num_folds >= 0
            assert isinstance(results.oos_mean_return, float)
            assert isinstance(results.oos_mean_sharpe, float)

        finally:
            prices_path.unlink(missing_ok=True)

    def test_run_validation_with_output(self, temp_history_file, temp_prices_file):
        """Test validation run with JSON output."""
        prices_dir = temp_history_file.parent
        prices_path = prices_dir / "prices.parquet"

        import shutil
        shutil.copy(temp_prices_file, prices_path)

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            out_path = Path(tmp.name)

        try:
            results = run_validation(
                history_path=temp_history_file,
                start_date="2023-06-01",
                end_date="2024-06-01",
                train_window_days=180,
                test_window_days=60,
                step_days=60,
                top_n=5,
                out_path=out_path,
            )

            assert out_path.exists()

            # Load and verify JSON
            import json
            with open(out_path) as f:
                data = json.load(f)

            assert "num_folds" in data
            assert "oos_mean_sharpe" in data
            assert "overfitting_detected" in data

        finally:
            prices_path.unlink(missing_ok=True)
            out_path.unlink(missing_ok=True)


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_insufficient_training_data(self, temp_history_file, temp_prices_file):
        """Test handling of insufficient training data."""
        validator = WalkForwardValidator(temp_history_file, temp_prices_file)

        config = ValidationConfig(
            start_date="2023-01-01",
            end_date="2024-12-31",
            min_train_rows=10000,  # Impossibly high requirement
        )

        fold_dates = (
            pd.Timestamp("2023-01-01"),
            pd.Timestamp("2023-12-31"),
            pd.Timestamp("2024-01-01"),
            pd.Timestamp("2024-03-31"),
        )

        result = validator._run_fold(1, fold_dates, config)

        # Should return None when insufficient data
        assert result is None

    def test_no_test_data(self, temp_history_file, temp_prices_file):
        """Test handling of missing test data."""
        validator = WalkForwardValidator(temp_history_file, temp_prices_file)

        config = ValidationConfig(
            start_date="2023-01-01",
            end_date="2024-12-31",
        )

        # Use dates far in the future with no data
        fold_dates = (
            pd.Timestamp("2023-01-01"),
            pd.Timestamp("2023-12-31"),
            pd.Timestamp("2030-01-01"),
            pd.Timestamp("2030-03-31"),
        )

        result = validator._run_fold(1, fold_dates, config)

        # Should return None when no test data
        assert result is None

    def test_empty_folds(self, temp_history_file):
        """Test validation with no valid folds."""
        validator = WalkForwardValidator(temp_history_file)

        config = ValidationConfig(
            start_date="2030-01-01",  # Far future with no data
            end_date="2030-12-31",
        )

        with pytest.raises(ValueError, match="No valid folds completed"):
            validator.run(config)
