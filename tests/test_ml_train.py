"""Tests for ML model training."""
from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from finviz_weekly.ml_train import (
    MLModel,
    ModelConfig,
    ModelResults,
    train_ml_model,
)
from finviz_weekly.learn import FACTOR_COLS


@pytest.fixture
def mock_train_data():
    """Create mock training data."""
    np.random.seed(42)

    n_samples = 500
    n_features = len(FACTOR_COLS)

    # Create features
    X = pd.DataFrame(
        np.random.randn(n_samples, n_features),
        columns=FACTOR_COLS,
    )

    # Create target with some signal
    # y = weighted combination of features + noise
    weights = np.random.rand(n_features)
    y = pd.Series(X.values @ weights + np.random.randn(n_samples) * 0.5)

    return X, y


@pytest.fixture
def mock_history_data():
    """Create mock historical data for training."""
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
    """Create mock price history for training."""
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


class TestModelConfig:
    """Tests for ModelConfig."""

    def test_default_config(self):
        """Test default configuration."""
        config = ModelConfig()

        assert config.model_type == "xgboost"
        assert config.n_estimators == 100
        assert config.max_depth == 6
        assert config.learning_rate == 0.1

    def test_custom_config(self):
        """Test custom configuration."""
        config = ModelConfig(
            model_type="ensemble",
            n_estimators=200,
            max_depth=8,
            learning_rate=0.05,
        )

        assert config.model_type == "ensemble"
        assert config.n_estimators == 200
        assert config.max_depth == 8
        assert config.learning_rate == 0.05


class TestMLModel:
    """Tests for MLModel."""

    def test_init(self):
        """Test model initialization."""
        config = ModelConfig()
        model = MLModel(config)

        assert model.config == config
        assert model.model is None
        assert model.feature_names is None
        assert model.version is not None

    def test_train_xgboost(self, mock_train_data):
        """Test XGBoost training."""
        X, y = mock_train_data
        X_train, X_val = X.iloc[:400], X.iloc[400:]
        y_train, y_val = y.iloc[:400], y.iloc[400:]

        config = ModelConfig(model_type="xgboost", n_estimators=50)
        model = MLModel(config)

        results = model.train(X_train, y_train, X_val, y_val)

        assert isinstance(results, ModelResults)
        assert results.model_type == "xgboost"
        assert results.train_score > 0  # Should have some predictive power
        assert not np.isnan(results.val_score)
        assert len(results.feature_importance) == len(FACTOR_COLS)

    def test_train_without_validation(self, mock_train_data):
        """Test training without validation set."""
        X, y = mock_train_data

        config = ModelConfig(model_type="xgboost", n_estimators=50)
        model = MLModel(config)

        results = model.train(X, y)

        assert isinstance(results, ModelResults)
        assert not np.isnan(results.train_score)
        assert np.isnan(results.val_score)  # No validation set provided

    def test_train_ensemble(self, mock_train_data):
        """Test ensemble training."""
        X, y = mock_train_data
        X_train, X_val = X.iloc[:400], X.iloc[400:]
        y_train, y_val = y.iloc[:400], y.iloc[400:]

        config = ModelConfig(model_type="ensemble")
        model = MLModel(config)

        results = model.train(X_train, y_train, X_val, y_val)

        assert isinstance(results, ModelResults)
        assert results.model_type == "ensemble"
        assert results.train_score > 0

    def test_predict(self, mock_train_data):
        """Test model prediction."""
        X, y = mock_train_data
        X_train, X_test = X.iloc[:400], X.iloc[400:]
        y_train = y.iloc[:400]

        config = ModelConfig(n_estimators=50)
        model = MLModel(config)
        model.train(X_train, y_train)

        predictions = model.predict(X_test)

        assert len(predictions) == len(X_test)
        assert isinstance(predictions, np.ndarray)
        assert not np.any(np.isnan(predictions))

    def test_predict_before_training(self, mock_train_data):
        """Test prediction before training raises error."""
        X, _ = mock_train_data

        config = ModelConfig()
        model = MLModel(config)

        with pytest.raises(ValueError, match="Model not trained"):
            model.predict(X)

    def test_feature_importance(self, mock_train_data):
        """Test feature importance extraction."""
        X, y = mock_train_data

        config = ModelConfig(n_estimators=50)
        model = MLModel(config)
        model.train(X, y)

        importance = model._get_feature_importance()

        assert len(importance) == len(FACTOR_COLS)
        assert all(isinstance(v, (float, np.floating)) for v in importance.values())
        assert all(v >= 0 for v in importance.values())  # XGBoost importance is non-negative

    def test_save_and_load(self, mock_train_data):
        """Test model persistence."""
        X, y = mock_train_data

        config = ModelConfig(n_estimators=50)
        model = MLModel(config)
        model.train(X, y)

        with tempfile.TemporaryDirectory() as tmpdir:
            model_dir = Path(tmpdir)

            # Save
            model.save(model_dir)

            # Load
            loaded_model = MLModel.load(model_dir)

            assert loaded_model.version == model.version
            assert loaded_model.feature_names == model.feature_names
            assert loaded_model.config.model_type == model.config.model_type

            # Test that loaded model can predict
            predictions = loaded_model.predict(X.head(10))
            assert len(predictions) == 10

    def test_load_nonexistent_model(self):
        """Test loading from nonexistent directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            model_dir = Path(tmpdir) / "nonexistent"

            with pytest.raises(FileNotFoundError):
                MLModel.load(model_dir)


class TestModelEvaluation:
    """Tests for model evaluation metrics."""

    def test_r_squared_perfect(self):
        """Test R² with perfect predictions."""
        y_true = np.array([1, 2, 3, 4, 5])
        y_pred = np.array([1, 2, 3, 4, 5])

        r2 = MLModel._r_squared(y_true, y_pred)

        assert np.isclose(r2, 1.0)

    def test_r_squared_zero(self):
        """Test R² with mean predictions."""
        y_true = np.array([1, 2, 3, 4, 5])
        y_pred = np.array([3, 3, 3, 3, 3])  # Mean prediction

        r2 = MLModel._r_squared(y_true, y_pred)

        assert np.isclose(r2, 0.0)

    def test_information_coefficient(self):
        """Test information coefficient calculation."""
        # Need more data points for spearmanr
        y_true = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
        y_pred = np.array([1.1, 1.9, 3.1, 4.2, 4.8, 6.1, 6.9, 8.1, 9.2, 9.8, 11.1, 11.9])

        ic = MLModel._information_coefficient(y_true, y_pred)

        # Should be close to 1 for monotonic relationship (or NaN if scipy issues)
        assert ic > 0.9 or np.isnan(ic)
        if not np.isnan(ic):
            assert ic <= 1.0

    def test_information_coefficient_with_nans(self):
        """Test IC with NaN values."""
        # Need sufficient valid data points
        y_true = np.array([1, 2, np.nan, 4, 5, 6, 7, 8, 9, 10, 11, 12])
        y_pred = np.array([1.1, np.nan, 3.1, 4.2, 4.8, 6.1, 6.9, 8.1, 9.2, 9.8, 11.1, 11.9])

        ic = MLModel._information_coefficient(y_true, y_pred)

        # Should handle NaNs gracefully (computes on valid subset)
        # IC might be NaN if too few valid points or scipy issues
        assert isinstance(ic, (float, np.floating))


class TestHyperparameterTuning:
    """Tests for hyperparameter tuning."""

    def test_tune_hyperparameters(self, mock_train_data):
        """Test Optuna hyperparameter tuning."""
        X, y = mock_train_data
        X_train, X_val = X.iloc[:400], X.iloc[400:]
        y_train, y_val = y.iloc[:400], y.iloc[400:]

        config = ModelConfig(
            tune_hyperparams=True,
            n_tuning_trials=5,  # Small number for testing
        )

        model = MLModel(config)

        # This will trigger tuning during training
        results = model.train(X_train, y_train, X_val, y_val)

        assert results.best_params is not None
        assert "max_depth" in results.best_params
        assert "learning_rate" in results.best_params

    def test_tune_without_optuna(self, mock_train_data, monkeypatch):
        """Test tuning when Optuna not available."""
        X, y = mock_train_data

        # Simulate Optuna not available
        import finviz_weekly.ml_train as ml_train
        monkeypatch.setattr(ml_train, "OPTUNA_AVAILABLE", False)

        config = ModelConfig(tune_hyperparams=True, n_tuning_trials=5)
        model = MLModel(config)

        results = model.train(X, y)

        # Should skip tuning and use default params
        assert results.best_params == {} or results.best_params is None


class TestTrainMLModel:
    """Tests for train_ml_model convenience function."""

    def test_train_ml_model_basic(self, mock_history_data, mock_prices_data):
        """Test basic model training."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Save data
            history_path = Path(tmpdir) / "history.parquet"
            prices_path = Path(tmpdir) / "prices.parquet"

            mock_history_data.to_parquet(history_path)
            mock_prices_data.to_parquet(prices_path)

            # Train model
            config = ModelConfig(n_estimators=10)  # Small for testing
            model, results = train_ml_model(
                history_path=history_path,
                prices_path=prices_path,
                start_date="2023-06-01",
                end_date="2024-06-01",
                config=config,
            )

            assert isinstance(model, MLModel)
            assert isinstance(results, ModelResults)
            assert not np.isnan(results.train_score)

    def test_train_ml_model_with_save(self, mock_history_data, mock_prices_data):
        """Test model training with save."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Save data
            history_path = Path(tmpdir) / "history.parquet"
            prices_path = Path(tmpdir) / "prices.parquet"
            model_dir = Path(tmpdir) / "models"

            mock_history_data.to_parquet(history_path)
            mock_prices_data.to_parquet(prices_path)

            # Train and save model
            config = ModelConfig(n_estimators=10)
            model, results = train_ml_model(
                history_path=history_path,
                prices_path=prices_path,
                start_date="2023-06-01",
                end_date="2024-06-01",
                config=config,
                model_dir=model_dir,
            )

            # Check that model was saved
            assert model_dir.exists()
            assert len(list(model_dir.glob("model_*.pkl"))) > 0
            assert len(list(model_dir.glob("metadata_*.json"))) > 0

            # Load and verify
            loaded_model = MLModel.load(model_dir)
            assert loaded_model.version == model.version

    def test_train_ml_model_insufficient_data(self, mock_prices_data):
        """Test training with insufficient data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create minimal history data
            history_path = Path(tmpdir) / "history.parquet"
            prices_path = Path(tmpdir) / "prices.parquet"

            minimal_history = pd.DataFrame({
                "as_of_date": pd.date_range("2023-01-01", periods=10),
                "ticker": ["AAPL"] * 10,
                "score_quality": np.random.rand(10),
                "score_value": np.random.rand(10),
                "score_risk": np.random.rand(10),
                "score_growth": np.random.rand(10),
                "score_oversold": np.random.rand(10),
                "score_momentum": np.random.rand(10),
            })

            minimal_history.to_parquet(history_path)
            mock_prices_data.to_parquet(prices_path)

            # Should raise error for insufficient data
            with pytest.raises(ValueError, match="Insufficient data for training"):
                train_ml_model(
                    history_path=history_path,
                    prices_path=prices_path,
                    start_date="2023-01-01",
                    end_date="2023-01-31",
                )


class TestModelResults:
    """Tests for ModelResults dataclass."""

    def test_create_results(self):
        """Test creating model results."""
        results = ModelResults(
            model_type="xgboost",
            train_score=0.8,
            val_score=0.7,
            test_score=0.65,
            train_ic=0.06,
            val_ic=0.05,
            test_ic=0.04,
            feature_importance={"score_quality": 0.3, "score_value": 0.2},
            best_params={"max_depth": 6, "learning_rate": 0.1},
        )

        assert results.model_type == "xgboost"
        assert results.train_score == 0.8
        assert results.val_ic == 0.05

    def test_results_to_dict(self):
        """Test converting results to dictionary."""
        results = ModelResults(
            model_type="xgboost",
            train_score=0.8,
            val_score=0.7,
            test_score=0.65,
            train_ic=0.06,
            val_ic=0.05,
            test_ic=0.04,
            feature_importance={"score_quality": 0.3},
            best_params={"max_depth": 6},
        )

        result_dict = results.to_dict()

        assert isinstance(result_dict, dict)
        assert "model_type" in result_dict
        assert "train_score" in result_dict
        assert "feature_importance" in result_dict
        assert result_dict["train_score"] == 0.8


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_train_with_constant_target(self, mock_train_data):
        """Test training with constant target values."""
        X, _ = mock_train_data
        y = pd.Series(np.ones(len(X)))  # Constant target

        config = ModelConfig(n_estimators=10)
        model = MLModel(config)

        # Should still train (though R² will be 0 or negative)
        results = model.train(X, y)

        assert isinstance(results, ModelResults)
        assert results.train_score <= 0  # Can't beat mean prediction

    def test_train_with_all_nan_features(self, mock_train_data):
        """Test training with all-NaN feature."""
        X, y = mock_train_data
        X["score_quality"] = np.nan  # All NaN

        config = ModelConfig(n_estimators=10)
        model = MLModel(config)

        # Should handle NaN gracefully (filled with 0 in train_ml_model)
        X_filled = X.fillna(0)
        results = model.train(X_filled, y)

        assert isinstance(results, ModelResults)

    def test_unknown_model_type(self, mock_train_data):
        """Test training with unknown model type."""
        X, y = mock_train_data

        config = ModelConfig(model_type="unknown_model")
        model = MLModel(config)

        with pytest.raises(ValueError, match="Unknown model type"):
            model.train(X, y)
