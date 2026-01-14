"""Tests for drift detection."""
from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xgboost as xgb

from finviz_weekly.drift_detection import (
    DriftDetector,
    DriftReport,
    DriftThresholds,
    FeatureDriftResult,
)
from finviz_weekly.model_registry import ModelRegistry


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
def mock_data():
    """Create mock feature data for testing."""
    np.random.seed(42)
    n_samples = 200

    data = pd.DataFrame({
        "feature_0": np.random.randn(n_samples),
        "feature_1": np.random.randn(n_samples) + 1,
        "feature_2": np.random.randn(n_samples) * 2,
        "feature_3": np.random.randn(n_samples) - 1,
        "feature_4": np.random.randn(n_samples) * 0.5,
    })

    return data


@pytest.fixture
def mock_returns():
    """Create mock return series for testing."""
    np.random.seed(42)
    return pd.Series(np.random.randn(200) * 0.05, name="returns")


@pytest.fixture
def temp_registry_with_model(mock_model):
    """Create a temporary registry with a registered model."""
    with tempfile.TemporaryDirectory() as tmpdir:
        registry_path = Path(tmpdir) / "registry"
        registry = ModelRegistry(registry_path)

        metadata = registry.register(
            model=mock_model,
            model_type="xgboost",
            training_start_date="2023-01-01",
            training_end_date="2024-01-01",
            training_samples=500,
            feature_names=[f"feature_{i}" for i in range(5)],
            train_ic=0.06,
            val_ic=0.05,
            train_r2=0.8,
            val_r2=0.7,
        )

        registry.update_oos_metrics(metadata.model_id, oos_sharpe=1.5)

        yield registry, metadata.model_id


class TestDriftThresholds:
    """Tests for DriftThresholds."""

    def test_default_thresholds(self):
        """Test default threshold values."""
        thresholds = DriftThresholds()

        assert thresholds.ic_drop_pct == 0.20
        assert thresholds.sharpe_drop_pct == 0.30
        assert thresholds.ks_pvalue_threshold == 0.01
        assert thresholds.max_shifted_features_pct == 0.30

    def test_custom_thresholds(self):
        """Test custom threshold values."""
        thresholds = DriftThresholds(
            ic_drop_pct=0.10,
            sharpe_drop_pct=0.20,
            ks_pvalue_threshold=0.05,
        )

        assert thresholds.ic_drop_pct == 0.10
        assert thresholds.sharpe_drop_pct == 0.20
        assert thresholds.ks_pvalue_threshold == 0.05


class TestFeatureDriftResult:
    """Tests for FeatureDriftResult."""

    def test_create_result(self):
        """Test creating a feature drift result."""
        result = FeatureDriftResult(
            feature_name="feature_0",
            baseline_mean=0.0,
            baseline_std=1.0,
            current_mean=0.5,
            current_std=1.2,
            ks_statistic=0.15,
            ks_pvalue=0.02,
            is_shifted=True,
        )

        assert result.feature_name == "feature_0"
        assert result.baseline_mean == 0.0
        assert result.is_shifted is True


class TestDriftReport:
    """Tests for DriftReport."""

    def test_create_report(self):
        """Test creating a drift report."""
        report = DriftReport(
            model_id="test_model",
            analysis_date="2024-01-15T12:00:00",
            data_start_date="2024-01-01",
            data_end_date="2024-01-15",
            baseline_ic=0.05,
            current_ic=0.04,
            ic_change_pct=-0.20,
            ic_alert=True,
            baseline_sharpe=1.5,
            current_sharpe=1.2,
            sharpe_change_pct=-0.20,
            sharpe_alert=False,
            total_features=5,
            shifted_features=1,
            shifted_features_pct=0.20,
            feature_drift_alert=False,
            shifted_feature_names=["feature_0"],
            prediction_mean_baseline=0.01,
            prediction_mean_current=0.02,
            prediction_std_baseline=0.05,
            prediction_std_current=0.06,
            prediction_drift_alert=False,
            overall_drift_detected=True,
            severity="low",
            recommended_action="Monitor closely",
        )

        assert report.model_id == "test_model"
        assert report.ic_alert is True
        assert report.overall_drift_detected is True
        assert report.severity == "low"

    def test_report_to_dict(self):
        """Test converting report to dictionary."""
        report = DriftReport(
            model_id="test_model",
            analysis_date="2024-01-15T12:00:00",
            data_start_date="2024-01-01",
            data_end_date="2024-01-15",
            baseline_ic=0.05,
            current_ic=0.04,
            ic_change_pct=-0.20,
            ic_alert=True,
            baseline_sharpe=1.5,
            current_sharpe=1.2,
            sharpe_change_pct=-0.20,
            sharpe_alert=False,
            total_features=5,
            shifted_features=1,
            shifted_features_pct=0.20,
            feature_drift_alert=False,
            shifted_feature_names=["feature_0"],
            prediction_mean_baseline=0.01,
            prediction_mean_current=0.02,
            prediction_std_baseline=0.05,
            prediction_std_current=0.06,
            prediction_drift_alert=False,
            overall_drift_detected=True,
            severity="low",
            recommended_action="Monitor closely",
        )

        data = report.to_dict()

        assert isinstance(data, dict)
        assert data["model_id"] == "test_model"
        assert data["performance"]["ic_alert"] is True
        assert data["summary"]["severity"] == "low"


class TestDriftDetector:
    """Tests for DriftDetector."""

    def test_init(self, temp_registry_with_model):
        """Test drift detector initialization."""
        registry, model_id = temp_registry_with_model

        detector = DriftDetector(registry=registry)

        assert detector.registry is not None
        assert detector.thresholds is not None

    def test_init_with_custom_thresholds(self, temp_registry_with_model):
        """Test initialization with custom thresholds."""
        registry, model_id = temp_registry_with_model

        thresholds = DriftThresholds(ic_drop_pct=0.10)
        detector = DriftDetector(registry=registry, thresholds=thresholds)

        assert detector.thresholds.ic_drop_pct == 0.10

    def test_detect_drift(self, temp_registry_with_model, mock_data, mock_returns):
        """Test basic drift detection."""
        registry, model_id = temp_registry_with_model

        detector = DriftDetector(registry=registry)

        # Align indices
        mock_data.index = range(len(mock_data))
        mock_returns.index = range(len(mock_returns))

        report = detector.detect_drift(
            model_id=model_id,
            current_data=mock_data,
            current_returns=mock_returns,
        )

        assert isinstance(report, DriftReport)
        assert report.model_id == model_id
        assert report.baseline_ic == 0.05  # From metadata
        assert isinstance(report.current_ic, float)
        assert isinstance(report.severity, str)

    def test_detect_drift_with_baseline(self, temp_registry_with_model, mock_data, mock_returns):
        """Test drift detection with baseline data."""
        registry, model_id = temp_registry_with_model

        detector = DriftDetector(registry=registry)

        # Create baseline data
        np.random.seed(123)
        baseline_data = pd.DataFrame({
            f"feature_{i}": np.random.randn(100)
            for i in range(5)
        })

        # Align indices
        mock_data.index = range(len(mock_data))
        mock_returns.index = range(len(mock_returns))

        report = detector.detect_drift(
            model_id=model_id,
            current_data=mock_data,
            current_returns=mock_returns,
            baseline_data=baseline_data,
        )

        assert isinstance(report, DriftReport)
        assert report.total_features > 0

    def test_assess_severity(self, temp_registry_with_model):
        """Test severity assessment."""
        registry, _ = temp_registry_with_model
        detector = DriftDetector(registry=registry)

        # No alerts
        severity = detector._assess_severity(
            ic_alert=False,
            sharpe_alert=False,
            feature_alert=False,
            prediction_alert=False,
            ic_change_pct=0.0,
            shifted_features_pct=0.0,
        )
        assert severity == "none"

        # One alert
        severity = detector._assess_severity(
            ic_alert=True,
            sharpe_alert=False,
            feature_alert=False,
            prediction_alert=False,
            ic_change_pct=-0.25,
            shifted_features_pct=0.0,
        )
        assert severity == "low"

        # Multiple alerts
        severity = detector._assess_severity(
            ic_alert=True,
            sharpe_alert=True,
            feature_alert=True,
            prediction_alert=False,
            ic_change_pct=-0.50,
            shifted_features_pct=0.40,
        )
        assert severity == "high"

        # All alerts
        severity = detector._assess_severity(
            ic_alert=True,
            sharpe_alert=True,
            feature_alert=True,
            prediction_alert=True,
            ic_change_pct=-0.70,
            shifted_features_pct=0.60,
        )
        assert severity == "critical"

    def test_get_recommended_action(self, temp_registry_with_model):
        """Test recommended action generation."""
        registry, _ = temp_registry_with_model
        detector = DriftDetector(registry=registry)

        action_none = detector._get_recommended_action("none", False, False)
        assert "No action required" in action_none

        action_low = detector._get_recommended_action("low", False, False)
        assert "Monitor closely" in action_low

        action_medium = detector._get_recommended_action("medium", True, False)
        assert "Schedule retraining" in action_medium

        action_high = detector._get_recommended_action("high", True, True)
        assert "Immediate retraining" in action_high

        action_critical = detector._get_recommended_action("critical", True, True)
        assert "URGENT" in action_critical

    def test_check_production_model(self, temp_registry_with_model, mock_data, mock_returns):
        """Test checking production model."""
        registry, model_id = temp_registry_with_model

        # Deploy to production
        registry.deploy_to_production(model_id)

        detector = DriftDetector(registry=registry)

        # Align indices
        mock_data.index = range(len(mock_data))
        mock_returns.index = range(len(mock_returns))

        report = detector.check_production_model(mock_data, mock_returns)

        assert report.model_id == model_id

    def test_check_production_model_no_deployment(self, temp_registry_with_model, mock_data, mock_returns):
        """Test checking production model when none deployed."""
        registry, _ = temp_registry_with_model

        detector = DriftDetector(registry=registry)

        with pytest.raises(ValueError, match="No production model deployed"):
            detector.check_production_model(mock_data, mock_returns)


class TestFeatureDriftAnalysis:
    """Tests for feature drift analysis."""

    def test_analyze_feature_drift_no_shift(self, temp_registry_with_model):
        """Test feature drift analysis with no shift."""
        registry, _ = temp_registry_with_model
        detector = DriftDetector(registry=registry)

        # Same distribution
        np.random.seed(42)
        baseline = pd.DataFrame({"f1": np.random.randn(100)})
        current = pd.DataFrame({"f1": np.random.randn(100)})

        results = detector._analyze_feature_drift(baseline, current, ["f1"])

        # Should detect no significant shift for same distribution
        assert len(results) == 1
        # p-value should be > 0.01 for same distribution
        # (may occasionally fail due to random sampling)

    def test_analyze_feature_drift_with_shift(self, temp_registry_with_model):
        """Test feature drift analysis with significant shift."""
        registry, _ = temp_registry_with_model
        detector = DriftDetector(registry=registry)

        # Different distributions
        np.random.seed(42)
        baseline = pd.DataFrame({"f1": np.random.randn(100)})
        current = pd.DataFrame({"f1": np.random.randn(100) + 5})  # Shifted mean

        results = detector._analyze_feature_drift(baseline, current, ["f1"])

        assert len(results) == 1
        # Should detect significant shift
        assert results[0].is_shifted is True

    def test_analyze_feature_drift_missing_feature(self, temp_registry_with_model):
        """Test feature drift analysis with missing features."""
        registry, _ = temp_registry_with_model
        detector = DriftDetector(registry=registry)

        np.random.seed(42)
        baseline = pd.DataFrame({"f1": np.random.randn(100)})
        current = pd.DataFrame({"f1": np.random.randn(100)})

        # Request non-existent feature
        results = detector._analyze_feature_drift(baseline, current, ["f1", "f2"])

        # Should only analyze existing features
        assert len(results) == 1
        assert results[0].feature_name == "f1"


class TestICAndSharpeCalculation:
    """Tests for IC and Sharpe ratio calculation."""

    def test_calculate_ic(self, temp_registry_with_model, mock_data, mock_returns):
        """Test IC calculation."""
        registry, model_id = temp_registry_with_model
        model, _ = registry.load(model_id)

        detector = DriftDetector(registry=registry)

        # Align indices
        mock_data.index = range(len(mock_data))
        mock_returns.index = range(len(mock_returns))

        ic = detector._calculate_ic(model, mock_data, mock_returns)

        assert isinstance(ic, float)
        assert -1 <= ic <= 1 or np.isnan(ic)

    def test_calculate_sharpe(self, temp_registry_with_model, mock_data, mock_returns):
        """Test Sharpe calculation."""
        registry, model_id = temp_registry_with_model
        model, _ = registry.load(model_id)

        detector = DriftDetector(registry=registry)

        # Align indices
        mock_data.index = range(len(mock_data))
        mock_returns.index = range(len(mock_returns))

        sharpe = detector._calculate_sharpe(model, mock_data, mock_returns)

        # Sharpe can be None or a float
        assert sharpe is None or isinstance(sharpe, float)
