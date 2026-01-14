"""Tests for automated retraining."""
from __future__ import annotations

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xgboost as xgb

from finviz_weekly.model_registry import ModelRegistry
from finviz_weekly.retraining import (
    RetrainingConfig,
    RetrainingHistory,
    RetrainingResult,
    RetrainingScheduler,
)


@pytest.fixture
def mock_model():
    """Create a mock model for testing."""
    np.random.seed(42)
    X = pd.DataFrame(np.random.randn(100, 5), columns=[f"feature_{i}" for i in range(5)])
    y = pd.Series(np.random.randn(100))

    model = xgb.XGBRegressor(n_estimators=10, max_depth=3, random_state=42)
    model.fit(X, y)

    return model


@pytest.fixture
def temp_registry(mock_model):
    """Create a temporary registry."""
    with tempfile.TemporaryDirectory() as tmpdir:
        registry_path = Path(tmpdir) / "registry"
        registry = ModelRegistry(registry_path)
        yield registry, Path(tmpdir)


@pytest.fixture
def mock_history_data():
    """Create mock historical data for training."""
    dates = pd.date_range("2023-01-01", "2024-12-31", freq="7D")
    tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

    data = []
    np.random.seed(42)
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
    """Create mock price data for training."""
    dates = pd.date_range("2023-01-01", "2025-06-30", freq="D")
    tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

    data = []
    np.random.seed(42)
    for ticker in tickers:
        price = 100.0
        for date in dates:
            price *= 1 + np.random.randn() * 0.02
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


class TestRetrainingConfig:
    """Tests for RetrainingConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = RetrainingConfig()

        assert config.retrain_interval_days == 30
        assert config.min_days_between_retrains == 7
        assert config.ic_drop_threshold == 0.20
        assert config.training_lookback_days == 730
        assert config.require_oos_validation is True

    def test_custom_config(self):
        """Test custom configuration values."""
        config = RetrainingConfig(
            retrain_interval_days=14,
            ic_drop_threshold=0.15,
            training_lookback_days=365,
        )

        assert config.retrain_interval_days == 14
        assert config.ic_drop_threshold == 0.15
        assert config.training_lookback_days == 365


class TestRetrainingResult:
    """Tests for RetrainingResult."""

    def test_create_result(self):
        """Test creating a retraining result."""
        result = RetrainingResult(
            retrain_id="retrain_20240115_120000",
            started_at="2024-01-15T12:00:00",
            completed_at="2024-01-15T12:30:00",
            trigger="scheduled",
            training_success=True,
            new_model_id="xgboost_v2",
            training_samples=1000,
            training_start_date="2022-01-01",
            training_end_date="2024-01-01",
            old_model_id="xgboost_v1",
            old_ic=0.05,
            new_ic=0.06,
            ic_improvement=0.01,
            old_sharpe=1.2,
            new_sharpe=1.5,
            sharpe_improvement=0.3,
            deployment_decision="deployed",
            deployment_reason="IC improved by 0.01",
        )

        assert result.retrain_id == "retrain_20240115_120000"
        assert result.training_success is True
        assert result.deployment_decision == "deployed"

    def test_result_to_dict(self):
        """Test converting result to dictionary."""
        result = RetrainingResult(
            retrain_id="test_retrain",
            started_at="2024-01-15T12:00:00",
            completed_at="2024-01-15T12:30:00",
            trigger="manual",
            training_success=True,
            new_model_id="new_model",
            training_samples=500,
            training_start_date="2022-01-01",
            training_end_date="2024-01-01",
            old_model_id="old_model",
            old_ic=0.04,
            new_ic=0.05,
            ic_improvement=0.01,
            old_sharpe=None,
            new_sharpe=None,
            sharpe_improvement=None,
            deployment_decision="deployed",
            deployment_reason="IC improved",
        )

        data = result.to_dict()

        assert isinstance(data, dict)
        assert data["retrain_id"] == "test_retrain"
        assert data["training"]["success"] is True
        assert data["deployment"]["decision"] == "deployed"


class TestRetrainingHistory:
    """Tests for RetrainingHistory."""

    def test_empty_history(self):
        """Test empty history."""
        history = RetrainingHistory()

        assert len(history.history) == 0
        assert history.get_last_retrain() is None
        assert len(history.get_successful_retrains()) == 0

    def test_add_result(self):
        """Test adding results to history."""
        history = RetrainingHistory()

        result = RetrainingResult(
            retrain_id="retrain_1",
            started_at="2024-01-15T12:00:00",
            completed_at="2024-01-15T12:30:00",
            trigger="scheduled",
            training_success=True,
            new_model_id="model_1",
            training_samples=500,
            training_start_date="2022-01-01",
            training_end_date="2024-01-01",
            old_model_id=None,
            old_ic=0.0,
            new_ic=0.05,
            ic_improvement=0.05,
            old_sharpe=None,
            new_sharpe=1.2,
            sharpe_improvement=None,
            deployment_decision="deployed",
            deployment_reason="First model",
        )

        history.add(result)

        assert len(history.history) == 1
        assert history.get_last_retrain() == result

    def test_get_successful_retrains(self):
        """Test getting successful retrains."""
        history = RetrainingHistory()

        # Add successful result
        history.add(RetrainingResult(
            retrain_id="retrain_1",
            started_at="2024-01-15T12:00:00",
            completed_at="2024-01-15T12:30:00",
            trigger="scheduled",
            training_success=True,
            new_model_id="model_1",
            training_samples=500,
            training_start_date="2022-01-01",
            training_end_date="2024-01-01",
            old_model_id=None,
            old_ic=0.0,
            new_ic=0.05,
            ic_improvement=0.05,
            old_sharpe=None,
            new_sharpe=None,
            sharpe_improvement=None,
            deployment_decision="deployed",
            deployment_reason="First model",
        ))

        # Add failed result
        history.add(RetrainingResult(
            retrain_id="retrain_2",
            started_at="2024-02-15T12:00:00",
            completed_at="2024-02-15T12:30:00",
            trigger="scheduled",
            training_success=False,
            new_model_id=None,
            training_samples=0,
            training_start_date="2022-01-01",
            training_end_date="2024-02-01",
            old_model_id="model_1",
            old_ic=0.05,
            new_ic=0.0,
            ic_improvement=0.0,
            old_sharpe=None,
            new_sharpe=None,
            sharpe_improvement=None,
            deployment_decision="rejected",
            deployment_reason="Training failed",
            error_message="Insufficient data",
        ))

        successful = history.get_successful_retrains()
        assert len(successful) == 1
        assert successful[0].retrain_id == "retrain_1"

    def test_history_to_dict(self):
        """Test converting history to dictionary."""
        history = RetrainingHistory()

        history.add(RetrainingResult(
            retrain_id="retrain_1",
            started_at="2024-01-15T12:00:00",
            completed_at="2024-01-15T12:30:00",
            trigger="scheduled",
            training_success=True,
            new_model_id="model_1",
            training_samples=500,
            training_start_date="2022-01-01",
            training_end_date="2024-01-01",
            old_model_id=None,
            old_ic=0.0,
            new_ic=0.05,
            ic_improvement=0.05,
            old_sharpe=None,
            new_sharpe=None,
            sharpe_improvement=None,
            deployment_decision="deployed",
            deployment_reason="First model",
        ))

        data = history.to_dict()

        assert data["count"] == 1
        assert len(data["history"]) == 1

    def test_history_from_dict(self):
        """Test creating history from dictionary."""
        data = {
            "count": 1,
            "history": [{
                "retrain_id": "retrain_1",
                "started_at": "2024-01-15T12:00:00",
                "completed_at": "2024-01-15T12:30:00",
                "trigger": "scheduled",
                "training": {
                    "success": True,
                    "new_model_id": "model_1",
                    "training_samples": 500,
                    "training_start_date": "2022-01-01",
                    "training_end_date": "2024-01-01",
                },
                "comparison": {
                    "old_model_id": None,
                    "old_ic": 0.0,
                    "new_ic": 0.05,
                    "ic_improvement": 0.05,
                    "old_sharpe": None,
                    "new_sharpe": None,
                    "sharpe_improvement": None,
                },
                "deployment": {
                    "decision": "deployed",
                    "reason": "First model",
                },
            }],
        }

        history = RetrainingHistory.from_dict(data)

        assert len(history.history) == 1
        assert history.history[0].retrain_id == "retrain_1"


class TestRetrainingScheduler:
    """Tests for RetrainingScheduler."""

    def test_init(self, temp_registry):
        """Test scheduler initialization."""
        registry, tmpdir = temp_registry

        scheduler = RetrainingScheduler(
            registry=registry,
            history_path=tmpdir / "history.json",
        )

        assert scheduler.registry is not None
        assert scheduler.config is not None

    def test_init_with_custom_config(self, temp_registry):
        """Test scheduler with custom config."""
        registry, tmpdir = temp_registry

        config = RetrainingConfig(retrain_interval_days=7)

        scheduler = RetrainingScheduler(
            registry=registry,
            config=config,
            history_path=tmpdir / "history.json",
        )

        assert scheduler.config.retrain_interval_days == 7

    def test_should_retrain_no_production(self, temp_registry):
        """Test should_retrain when no production model exists."""
        registry, tmpdir = temp_registry

        scheduler = RetrainingScheduler(
            registry=registry,
            history_path=tmpdir / "history.json",
        )

        should_retrain, reason = scheduler.should_retrain()

        assert should_retrain is True
        assert "No production model" in reason

    def test_should_retrain_cooldown(self, temp_registry, mock_model):
        """Test should_retrain during cooldown period."""
        registry, tmpdir = temp_registry

        # Register and deploy a model
        metadata = registry.register(
            model=mock_model,
            model_type="xgboost",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=500,
            feature_names=["f1"],
            train_ic=0.05,
            val_ic=0.04,
        )
        registry.deploy_to_production(metadata.model_id)

        # Create scheduler with recent retrain
        scheduler = RetrainingScheduler(
            registry=registry,
            history_path=tmpdir / "history.json",
        )

        # Add recent retrain to history
        recent_result = RetrainingResult(
            retrain_id="recent_retrain",
            started_at=datetime.now().isoformat(),
            completed_at=datetime.now().isoformat(),
            trigger="scheduled",
            training_success=True,
            new_model_id=metadata.model_id,
            training_samples=500,
            training_start_date="2022-01-01",
            training_end_date="2024-01-01",
            old_model_id=None,
            old_ic=0.0,
            new_ic=0.05,
            ic_improvement=0.05,
            old_sharpe=None,
            new_sharpe=None,
            sharpe_improvement=None,
            deployment_decision="deployed",
            deployment_reason="First model",
        )
        scheduler.history.add(recent_result)

        should_retrain, reason = scheduler.should_retrain()

        assert should_retrain is False
        assert "Cooldown" in reason

    def test_make_deployment_decision_no_production(self, temp_registry):
        """Test deployment decision when no production model exists."""
        registry, tmpdir = temp_registry

        scheduler = RetrainingScheduler(
            registry=registry,
            history_path=tmpdir / "history.json",
        )

        decision, reason = scheduler._make_deployment_decision(
            old_ic=0.0,
            new_ic=0.05,
            old_sharpe=None,
            new_sharpe=None,
            has_production_model=False,
        )

        assert decision == "deployed"
        assert "No existing production model" in reason

    def test_make_deployment_decision_improvement(self, temp_registry):
        """Test deployment decision with IC improvement."""
        registry, tmpdir = temp_registry

        scheduler = RetrainingScheduler(
            registry=registry,
            history_path=tmpdir / "history.json",
        )

        decision, reason = scheduler._make_deployment_decision(
            old_ic=0.04,
            new_ic=0.06,
            old_sharpe=1.0,
            new_sharpe=1.2,
            has_production_model=True,
        )

        assert decision == "deployed"
        assert "improved" in reason.lower()

    def test_make_deployment_decision_degradation(self, temp_registry):
        """Test deployment decision with IC degradation."""
        registry, tmpdir = temp_registry

        scheduler = RetrainingScheduler(
            registry=registry,
            history_path=tmpdir / "history.json",
        )

        decision, reason = scheduler._make_deployment_decision(
            old_ic=0.06,
            new_ic=0.03,
            old_sharpe=1.5,
            new_sharpe=0.8,
            has_production_model=True,
        )

        assert decision == "rejected"
        assert "degraded" in reason.lower()

    def test_make_deployment_decision_insufficient_improvement(self, temp_registry):
        """Test deployment decision with insufficient improvement."""
        registry, tmpdir = temp_registry

        scheduler = RetrainingScheduler(
            registry=registry,
            history_path=tmpdir / "history.json",
        )

        decision, reason = scheduler._make_deployment_decision(
            old_ic=0.05,
            new_ic=0.052,  # Only 0.002 improvement
            old_sharpe=1.0,
            new_sharpe=1.02,
            has_production_model=True,
        )

        assert decision == "rejected"
        assert "Insufficient improvement" in reason

    def test_get_retraining_status_no_history(self, temp_registry, mock_model):
        """Test getting retraining status with no history."""
        registry, tmpdir = temp_registry

        # Register and deploy a model
        metadata = registry.register(
            model=mock_model,
            model_type="xgboost",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=500,
            feature_names=["f1"],
            train_ic=0.05,
            val_ic=0.04,
        )
        registry.deploy_to_production(metadata.model_id)

        scheduler = RetrainingScheduler(
            registry=registry,
            history_path=tmpdir / "history.json",
        )

        status = scheduler.get_retraining_status()

        assert status["production_model"] == metadata.model_id
        assert status["total_retrains"] == 0
        assert status["last_retrain"] is None


class TestRetrainingIntegration:
    """Integration tests for retraining."""

    def test_full_retrain_cycle(
        self,
        temp_registry,
        mock_history_data,
        mock_prices_data,
    ):
        """Test a full retraining cycle."""
        registry, tmpdir = temp_registry

        # Save test data
        history_path = tmpdir / "history.parquet"
        prices_path = tmpdir / "prices.parquet"

        mock_history_data.to_parquet(history_path)
        mock_prices_data.to_parquet(prices_path)

        # Create scheduler
        config = RetrainingConfig(
            training_lookback_days=365,
            min_training_samples=50,  # Lower for testing
        )

        scheduler = RetrainingScheduler(
            registry=registry,
            config=config,
            history_path=tmpdir / "retrain_history.json",
        )

        # Should need retraining (no production model)
        should_retrain, reason = scheduler.should_retrain()
        assert should_retrain is True

        # Run retraining
        result = scheduler.retrain(
            history_path=history_path,
            prices_path=prices_path,
            trigger="manual",
        )

        # Check result
        assert result.training_success is True
        assert result.new_model_id is not None
        assert result.deployment_decision == "deployed"

        # Verify model is now in production
        production_id = registry.get_production_model_id()
        assert production_id == result.new_model_id

        # History should be updated
        assert len(scheduler.history.history) == 1
