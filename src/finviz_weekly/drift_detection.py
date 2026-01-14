"""
Model Drift Detection for monitoring ML model performance.

Provides:
- Performance degradation detection (IC, Sharpe ratio)
- Feature distribution shift detection (KS test)
- Prediction distribution monitoring
- Alert generation for retraining triggers
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

from .model_registry import ModelRegistry, ModelMetadata

LOGGER = logging.getLogger(__name__)


@dataclass
class DriftThresholds:
    """Thresholds for drift detection."""

    # Performance degradation thresholds
    ic_drop_pct: float = 0.20  # Alert if IC drops by 20%
    sharpe_drop_pct: float = 0.30  # Alert if Sharpe drops by 30%
    r2_drop_pct: float = 0.25  # Alert if R² drops by 25%

    # Feature distribution thresholds (KS test)
    ks_pvalue_threshold: float = 0.01  # Alert if p-value < 0.01
    max_shifted_features_pct: float = 0.30  # Alert if >30% features shifted

    # Prediction distribution thresholds
    prediction_mean_shift_std: float = 2.0  # Alert if mean shifts >2 std
    prediction_std_change_pct: float = 0.50  # Alert if std changes by 50%


@dataclass
class FeatureDriftResult:
    """Result of feature drift analysis."""

    feature_name: str
    baseline_mean: float
    baseline_std: float
    current_mean: float
    current_std: float
    ks_statistic: float
    ks_pvalue: float
    is_shifted: bool


@dataclass
class DriftReport:
    """Complete drift analysis report."""

    model_id: str
    analysis_date: str
    data_start_date: str
    data_end_date: str

    # Performance drift
    baseline_ic: float
    current_ic: float
    ic_change_pct: float
    ic_alert: bool

    baseline_sharpe: Optional[float]
    current_sharpe: Optional[float]
    sharpe_change_pct: Optional[float]
    sharpe_alert: bool

    # Feature drift
    total_features: int
    shifted_features: int
    shifted_features_pct: float
    feature_drift_alert: bool
    shifted_feature_names: List[str]

    # Prediction drift
    prediction_mean_baseline: float
    prediction_mean_current: float
    prediction_std_baseline: float
    prediction_std_current: float
    prediction_drift_alert: bool

    # Overall assessment
    overall_drift_detected: bool
    severity: str  # "none", "low", "medium", "high", "critical"
    recommended_action: str

    # Detailed feature results
    feature_results: List[FeatureDriftResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "model_id": self.model_id,
            "analysis_date": self.analysis_date,
            "data_start_date": self.data_start_date,
            "data_end_date": self.data_end_date,
            "performance": {
                "baseline_ic": self.baseline_ic,
                "current_ic": self.current_ic,
                "ic_change_pct": self.ic_change_pct,
                "ic_alert": self.ic_alert,
                "baseline_sharpe": self.baseline_sharpe,
                "current_sharpe": self.current_sharpe,
                "sharpe_change_pct": self.sharpe_change_pct,
                "sharpe_alert": self.sharpe_alert,
            },
            "feature_drift": {
                "total_features": self.total_features,
                "shifted_features": self.shifted_features,
                "shifted_features_pct": self.shifted_features_pct,
                "feature_drift_alert": self.feature_drift_alert,
                "shifted_feature_names": self.shifted_feature_names,
            },
            "prediction_drift": {
                "prediction_mean_baseline": self.prediction_mean_baseline,
                "prediction_mean_current": self.prediction_mean_current,
                "prediction_std_baseline": self.prediction_std_baseline,
                "prediction_std_current": self.prediction_std_current,
                "prediction_drift_alert": self.prediction_drift_alert,
            },
            "summary": {
                "overall_drift_detected": self.overall_drift_detected,
                "severity": self.severity,
                "recommended_action": self.recommended_action,
            },
        }


class DriftDetector:
    """
    Detect model drift by comparing current performance to baseline.

    Monitors:
    - Performance degradation (IC, Sharpe ratio decline)
    - Feature distribution shifts (KS test)
    - Prediction distribution changes
    """

    def __init__(
        self,
        registry: Optional[ModelRegistry] = None,
        thresholds: Optional[DriftThresholds] = None,
    ):
        """
        Initialize drift detector.

        Args:
            registry: Model registry for loading models
            thresholds: Drift detection thresholds
        """
        self.registry = registry or ModelRegistry()
        self.thresholds = thresholds or DriftThresholds()

        LOGGER.info("Drift detector initialized")

    def detect_drift(
        self,
        model_id: str,
        current_data: pd.DataFrame,
        current_returns: pd.Series,
        baseline_data: Optional[pd.DataFrame] = None,
    ) -> DriftReport:
        """
        Detect drift for a specific model.

        Args:
            model_id: Model identifier to analyze
            current_data: Current feature data
            current_returns: Current realized returns for IC calculation
            baseline_data: Optional baseline data (uses training data if None)

        Returns:
            DriftReport with drift analysis results
        """
        LOGGER.info(f"Analyzing drift for model: {model_id}")

        # Load model and metadata
        model, metadata = self.registry.load(model_id)

        # Get baseline metrics from metadata
        baseline_ic = metadata.val_ic
        baseline_sharpe = metadata.oos_sharpe

        # Calculate current performance
        current_ic = self._calculate_ic(model, current_data, current_returns)
        current_sharpe = self._calculate_sharpe(model, current_data, current_returns)

        # Performance drift
        ic_change_pct = (current_ic - baseline_ic) / baseline_ic if baseline_ic != 0 else 0.0
        ic_alert = abs(ic_change_pct) > self.thresholds.ic_drop_pct and ic_change_pct < 0

        if baseline_sharpe and current_sharpe:
            sharpe_change_pct = (current_sharpe - baseline_sharpe) / baseline_sharpe if baseline_sharpe != 0 else 0.0
            sharpe_alert = abs(sharpe_change_pct) > self.thresholds.sharpe_drop_pct and sharpe_change_pct < 0
        else:
            sharpe_change_pct = None
            sharpe_alert = False

        # Feature drift analysis
        feature_results = self._analyze_feature_drift(
            baseline_data,
            current_data,
            metadata.feature_names,
        )

        shifted_features = [f.feature_name for f in feature_results if f.is_shifted]
        shifted_pct = len(shifted_features) / len(feature_results) if feature_results else 0.0
        feature_drift_alert = shifted_pct > self.thresholds.max_shifted_features_pct

        # Prediction drift analysis
        predictions_baseline = self._get_baseline_predictions(model, baseline_data) if baseline_data is not None else None
        predictions_current = model.predict(current_data[metadata.feature_names].fillna(0))

        if predictions_baseline is not None:
            pred_mean_baseline = float(np.mean(predictions_baseline))
            pred_std_baseline = float(np.std(predictions_baseline))
        else:
            # Use metadata or estimate from current
            pred_mean_baseline = 0.0
            pred_std_baseline = float(np.std(predictions_current)) if len(predictions_current) > 0 else 0.01

        pred_mean_current = float(np.mean(predictions_current))
        pred_std_current = float(np.std(predictions_current))

        # Check prediction drift
        mean_shift = abs(pred_mean_current - pred_mean_baseline) / pred_std_baseline if pred_std_baseline > 0 else 0.0
        std_change_pct = abs(pred_std_current - pred_std_baseline) / pred_std_baseline if pred_std_baseline > 0 else 0.0

        prediction_drift_alert = (
            mean_shift > self.thresholds.prediction_mean_shift_std or
            std_change_pct > self.thresholds.prediction_std_change_pct
        )

        # Overall assessment
        overall_drift = ic_alert or sharpe_alert or feature_drift_alert or prediction_drift_alert

        severity = self._assess_severity(
            ic_alert, sharpe_alert, feature_drift_alert, prediction_drift_alert,
            ic_change_pct, shifted_pct
        )

        recommended_action = self._get_recommended_action(severity, ic_alert, feature_drift_alert)

        report = DriftReport(
            model_id=model_id,
            analysis_date=datetime.now().isoformat(),
            data_start_date=str(current_data.index.min()) if hasattr(current_data.index, 'min') else "",
            data_end_date=str(current_data.index.max()) if hasattr(current_data.index, 'max') else "",
            baseline_ic=baseline_ic,
            current_ic=current_ic,
            ic_change_pct=ic_change_pct,
            ic_alert=ic_alert,
            baseline_sharpe=baseline_sharpe,
            current_sharpe=current_sharpe,
            sharpe_change_pct=sharpe_change_pct,
            sharpe_alert=sharpe_alert,
            total_features=len(feature_results),
            shifted_features=len(shifted_features),
            shifted_features_pct=shifted_pct,
            feature_drift_alert=feature_drift_alert,
            shifted_feature_names=shifted_features,
            prediction_mean_baseline=pred_mean_baseline,
            prediction_mean_current=pred_mean_current,
            prediction_std_baseline=pred_std_baseline,
            prediction_std_current=pred_std_current,
            prediction_drift_alert=prediction_drift_alert,
            overall_drift_detected=overall_drift,
            severity=severity,
            recommended_action=recommended_action,
            feature_results=feature_results,
        )

        LOGGER.info(f"Drift analysis complete. Severity: {severity}")
        LOGGER.info(f"IC change: {ic_change_pct:.1%}, Features shifted: {len(shifted_features)}/{len(feature_results)}")

        return report

    def _calculate_ic(
        self,
        model: Any,
        data: pd.DataFrame,
        returns: pd.Series,
    ) -> float:
        """Calculate information coefficient between predictions and returns."""
        try:
            # Get feature names from model if available
            if hasattr(model, "feature_names"):
                feature_names = model.feature_names
            elif hasattr(model, "feature_names_in_"):
                feature_names = list(model.feature_names_in_)
            else:
                feature_names = list(data.columns)

            available_features = [f for f in feature_names if f in data.columns]
            if not available_features:
                LOGGER.warning("No matching features found for IC calculation")
                return 0.0

            predictions = model.predict(data[available_features].fillna(0))

            # Align predictions with returns
            common_idx = data.index.intersection(returns.index)
            if len(common_idx) < 10:
                LOGGER.warning(f"Insufficient overlap for IC: {len(common_idx)} samples")
                return 0.0

            pred_aligned = pd.Series(predictions, index=data.index).loc[common_idx]
            returns_aligned = returns.loc[common_idx]

            # Calculate Spearman correlation
            if SCIPY_AVAILABLE:
                corr, _ = stats.spearmanr(pred_aligned, returns_aligned, nan_policy='omit')
                return float(corr) if not np.isnan(corr) else 0.0
            else:
                return float(pred_aligned.corr(returns_aligned, method='spearman'))

        except Exception as e:
            LOGGER.error(f"Error calculating IC: {e}")
            return 0.0

    def _calculate_sharpe(
        self,
        model: Any,
        data: pd.DataFrame,
        returns: pd.Series,
    ) -> Optional[float]:
        """Calculate Sharpe ratio based on model predictions."""
        try:
            # Get feature names
            if hasattr(model, "feature_names"):
                feature_names = model.feature_names
            elif hasattr(model, "feature_names_in_"):
                feature_names = list(model.feature_names_in_)
            else:
                feature_names = list(data.columns)

            available_features = [f for f in feature_names if f in data.columns]
            if not available_features:
                return None

            predictions = model.predict(data[available_features].fillna(0))

            # Create long/short portfolio based on predictions
            pred_series = pd.Series(predictions, index=data.index)

            # Top/bottom quintile returns
            common_idx = data.index.intersection(returns.index)
            if len(common_idx) < 20:
                return None

            pred_aligned = pred_series.loc[common_idx]
            returns_aligned = returns.loc[common_idx]

            # Long top quintile
            top_cutoff = pred_aligned.quantile(0.8)
            top_returns = returns_aligned[pred_aligned >= top_cutoff]

            if len(top_returns) < 5:
                return None

            mean_return = top_returns.mean()
            std_return = top_returns.std()

            if std_return == 0 or np.isnan(std_return):
                return None

            # Annualize (assuming 21-day returns, ~12 periods per year)
            annualized_sharpe = (mean_return / std_return) * np.sqrt(12)

            return float(annualized_sharpe)

        except Exception as e:
            LOGGER.error(f"Error calculating Sharpe: {e}")
            return None

    def _analyze_feature_drift(
        self,
        baseline_data: Optional[pd.DataFrame],
        current_data: pd.DataFrame,
        feature_names: List[str],
    ) -> List[FeatureDriftResult]:
        """Analyze feature distribution drift using KS test."""
        results = []

        for feature in feature_names:
            if feature not in current_data.columns:
                continue

            current_values = current_data[feature].dropna()

            if baseline_data is not None and feature in baseline_data.columns:
                baseline_values = baseline_data[feature].dropna()
            else:
                # Use current data's first half as baseline if no baseline provided
                split_idx = len(current_values) // 2
                baseline_values = current_values.iloc[:split_idx]
                current_values = current_values.iloc[split_idx:]

            if len(baseline_values) < 10 or len(current_values) < 10:
                continue

            baseline_mean = float(baseline_values.mean())
            baseline_std = float(baseline_values.std())
            current_mean = float(current_values.mean())
            current_std = float(current_values.std())

            # KS test
            if SCIPY_AVAILABLE:
                ks_stat, ks_pvalue = stats.ks_2samp(baseline_values, current_values)
            else:
                # Fallback: simple mean/std comparison
                ks_stat = abs(current_mean - baseline_mean) / (baseline_std + 0.001)
                ks_pvalue = 1.0 if ks_stat < 0.5 else 0.001

            is_shifted = ks_pvalue < self.thresholds.ks_pvalue_threshold

            results.append(FeatureDriftResult(
                feature_name=feature,
                baseline_mean=baseline_mean,
                baseline_std=baseline_std,
                current_mean=current_mean,
                current_std=current_std,
                ks_statistic=float(ks_stat),
                ks_pvalue=float(ks_pvalue),
                is_shifted=is_shifted,
            ))

        return results

    def _get_baseline_predictions(
        self,
        model: Any,
        baseline_data: pd.DataFrame,
    ) -> Optional[np.ndarray]:
        """Get predictions on baseline data."""
        try:
            if hasattr(model, "feature_names"):
                feature_names = model.feature_names
            elif hasattr(model, "feature_names_in_"):
                feature_names = list(model.feature_names_in_)
            else:
                return None

            available_features = [f for f in feature_names if f in baseline_data.columns]
            if not available_features:
                return None

            return model.predict(baseline_data[available_features].fillna(0))

        except Exception as e:
            LOGGER.error(f"Error getting baseline predictions: {e}")
            return None

    def _assess_severity(
        self,
        ic_alert: bool,
        sharpe_alert: bool,
        feature_alert: bool,
        prediction_alert: bool,
        ic_change_pct: float,
        shifted_features_pct: float,
    ) -> str:
        """Assess overall drift severity."""
        alert_count = sum([ic_alert, sharpe_alert, feature_alert, prediction_alert])

        if alert_count == 0:
            return "none"
        elif alert_count == 1:
            return "low"
        elif alert_count == 2:
            return "medium"
        elif alert_count == 3:
            return "high"
        else:
            return "critical"

    def _get_recommended_action(
        self,
        severity: str,
        ic_alert: bool,
        feature_alert: bool,
    ) -> str:
        """Get recommended action based on drift severity."""
        if severity == "none":
            return "No action required. Model performing within expectations."
        elif severity == "low":
            return "Monitor closely. Consider retraining in the next scheduled cycle."
        elif severity == "medium":
            if ic_alert:
                return "Schedule retraining within 1 week. Performance degradation detected."
            else:
                return "Investigate feature drift. May require data pipeline review."
        elif severity == "high":
            return "Immediate retraining recommended. Multiple drift indicators triggered."
        else:  # critical
            return "URGENT: Retrain model immediately. Consider fallback to previous version."

    def check_production_model(
        self,
        current_data: pd.DataFrame,
        current_returns: pd.Series,
    ) -> DriftReport:
        """
        Check drift for the current production model.

        Args:
            current_data: Current feature data
            current_returns: Current realized returns

        Returns:
            DriftReport for production model
        """
        production_id = self.registry.get_production_model_id()

        if not production_id:
            raise ValueError("No production model deployed")

        return self.detect_drift(production_id, current_data, current_returns)


def run_drift_check(
    model_id: str,
    data_path: Path,
    prices_path: Path,
    registry_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
) -> DriftReport:
    """
    Run drift check for a model.

    Args:
        model_id: Model identifier to check
        data_path: Path to current fundamentals data
        prices_path: Path to price data
        registry_path: Path to model registry
        output_path: Optional path to save report

    Returns:
        DriftReport with drift analysis
    """
    from .learn import _compute_forward_returns_from_prices, _winsorize

    LOGGER.info(f"Running drift check for model: {model_id}")

    # Load data
    data = pd.read_parquet(data_path)
    data["as_of_date"] = pd.to_datetime(data["as_of_date"]).dt.date

    prices = pd.read_parquet(prices_path)
    prices["ticker"] = prices["ticker"].astype(str).str.upper()
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce").dt.date

    # Compute forward returns
    joined = _compute_forward_returns_from_prices(data, prices, horizon_trading_days=21)
    joined["forward_return"] = _winsorize(joined["forward_return"])
    joined = joined.dropna(subset=["forward_return"])

    # Initialize detector
    registry = ModelRegistry(registry_path) if registry_path else ModelRegistry()
    detector = DriftDetector(registry=registry)

    # Run drift check
    report = detector.detect_drift(
        model_id=model_id,
        current_data=joined,
        current_returns=joined["forward_return"],
    )

    # Save report if output path provided
    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(report.to_dict(), f, indent=2)
        LOGGER.info(f"Drift report saved to {output_path}")

    return report
