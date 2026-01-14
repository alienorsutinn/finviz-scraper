"""
Automated Model Retraining for ML production deployment.

Provides:
- Scheduled retraining (monthly, weekly, custom)
- Performance-triggered retraining
- Model comparison and deployment
- Retraining history tracking
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .drift_detection import DriftDetector, DriftReport
from .ml_train import MLModel, ModelConfig, ModelResults, train_ml_model
from .model_registry import ModelMetadata, ModelRegistry

LOGGER = logging.getLogger(__name__)


@dataclass
class RetrainingConfig:
    """Configuration for automated retraining."""

    # Schedule settings
    retrain_interval_days: int = 30  # Monthly retraining by default
    min_days_between_retrains: int = 7  # Minimum cooldown period

    # Performance triggers
    ic_drop_threshold: float = 0.20  # Retrain if IC drops by 20%
    sharpe_drop_threshold: float = 0.30  # Retrain if Sharpe drops by 30%

    # Data requirements
    min_training_samples: int = 500  # Minimum samples for training
    training_lookback_days: int = 730  # 2 years of training data

    # Deployment criteria
    min_improvement_ic: float = 0.01  # Deploy if IC improves by at least 0.01
    min_improvement_sharpe: float = 0.1  # Deploy if Sharpe improves by at least 0.1
    require_oos_validation: bool = True  # Require OOS validation before deploy

    # Model configuration
    model_config: Optional[ModelConfig] = None


@dataclass
class RetrainingResult:
    """Result of a retraining attempt."""

    retrain_id: str
    started_at: str
    completed_at: str
    trigger: str  # "scheduled", "performance", "manual"

    # Training results
    training_success: bool
    new_model_id: Optional[str]
    training_samples: int
    training_start_date: str
    training_end_date: str

    # Performance comparison
    old_model_id: Optional[str]
    old_ic: float
    new_ic: float
    ic_improvement: float

    old_sharpe: Optional[float]
    new_sharpe: Optional[float]
    sharpe_improvement: Optional[float]

    # Deployment decision
    deployment_decision: str  # "deployed", "rejected", "no_production_model"
    deployment_reason: str

    # Errors
    error_message: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "retrain_id": self.retrain_id,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "trigger": self.trigger,
            "training": {
                "success": self.training_success,
                "new_model_id": self.new_model_id,
                "training_samples": self.training_samples,
                "training_start_date": self.training_start_date,
                "training_end_date": self.training_end_date,
            },
            "comparison": {
                "old_model_id": self.old_model_id,
                "old_ic": self.old_ic,
                "new_ic": self.new_ic,
                "ic_improvement": self.ic_improvement,
                "old_sharpe": self.old_sharpe,
                "new_sharpe": self.new_sharpe,
                "sharpe_improvement": self.sharpe_improvement,
            },
            "deployment": {
                "decision": self.deployment_decision,
                "reason": self.deployment_reason,
            },
            "error_message": self.error_message,
        }


@dataclass
class RetrainingHistory:
    """History of retraining attempts."""

    history: List[RetrainingResult] = field(default_factory=list)

    def add(self, result: RetrainingResult) -> None:
        """Add a retraining result to history."""
        self.history.append(result)

    def get_last_retrain(self) -> Optional[RetrainingResult]:
        """Get the most recent retraining result."""
        if not self.history:
            return None
        return self.history[-1]

    def get_successful_retrains(self) -> List[RetrainingResult]:
        """Get all successful retraining attempts."""
        return [r for r in self.history if r.training_success]

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "count": len(self.history),
            "history": [r.to_dict() for r in self.history],
        }

    @classmethod
    def from_dict(cls, data: dict) -> RetrainingHistory:
        """Create from dictionary."""
        history = cls()
        for item in data.get("history", []):
            result = RetrainingResult(
                retrain_id=item["retrain_id"],
                started_at=item["started_at"],
                completed_at=item["completed_at"],
                trigger=item["trigger"],
                training_success=item["training"]["success"],
                new_model_id=item["training"]["new_model_id"],
                training_samples=item["training"]["training_samples"],
                training_start_date=item["training"]["training_start_date"],
                training_end_date=item["training"]["training_end_date"],
                old_model_id=item["comparison"]["old_model_id"],
                old_ic=item["comparison"]["old_ic"],
                new_ic=item["comparison"]["new_ic"],
                ic_improvement=item["comparison"]["ic_improvement"],
                old_sharpe=item["comparison"]["old_sharpe"],
                new_sharpe=item["comparison"]["new_sharpe"],
                sharpe_improvement=item["comparison"]["sharpe_improvement"],
                deployment_decision=item["deployment"]["decision"],
                deployment_reason=item["deployment"]["reason"],
                error_message=item.get("error_message"),
            )
            history.add(result)
        return history


class RetrainingScheduler:
    """
    Automated model retraining scheduler.

    Handles:
    - Scheduled retraining (monthly, weekly)
    - Performance-triggered retraining
    - Model comparison and deployment decisions
    - Retraining history tracking
    """

    def __init__(
        self,
        registry: Optional[ModelRegistry] = None,
        config: Optional[RetrainingConfig] = None,
        history_path: Optional[Path] = None,
    ):
        """
        Initialize retraining scheduler.

        Args:
            registry: Model registry for model management
            config: Retraining configuration
            history_path: Path to store retraining history
        """
        self.registry = registry or ModelRegistry()
        self.config = config or RetrainingConfig()
        self.history_path = history_path or Path("models/retraining_history.json")

        # Load history
        self.history = self._load_history()

        LOGGER.info("Retraining scheduler initialized")
        LOGGER.info(f"Retrain interval: {self.config.retrain_interval_days} days")

    def _load_history(self) -> RetrainingHistory:
        """Load retraining history from disk."""
        if self.history_path.exists():
            with open(self.history_path) as f:
                data = json.load(f)
                return RetrainingHistory.from_dict(data)
        return RetrainingHistory()

    def _save_history(self) -> None:
        """Save retraining history to disk."""
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.history_path, "w") as f:
            json.dump(self.history.to_dict(), f, indent=2)

    def should_retrain(
        self,
        drift_report: Optional[DriftReport] = None,
    ) -> Tuple[bool, str]:
        """
        Check if retraining is needed.

        Args:
            drift_report: Optional drift report to check performance

        Returns:
            (should_retrain, reason) tuple
        """
        # Check cooldown period
        last_retrain = self.history.get_last_retrain()
        if last_retrain:
            last_retrain_date = datetime.fromisoformat(last_retrain.completed_at)
            days_since_retrain = (datetime.now() - last_retrain_date).days

            if days_since_retrain < self.config.min_days_between_retrains:
                return False, f"Cooldown: {self.config.min_days_between_retrains - days_since_retrain} days remaining"

            # Check scheduled interval
            if days_since_retrain >= self.config.retrain_interval_days:
                return True, f"Scheduled: {days_since_retrain} days since last retrain"

        else:
            # No previous retraining, check if production model exists
            production_id = self.registry.get_production_model_id()
            if not production_id:
                return True, "No production model deployed"

        # Check performance triggers from drift report
        if drift_report:
            if drift_report.ic_alert:
                return True, f"Performance: IC dropped by {abs(drift_report.ic_change_pct):.1%}"

            if drift_report.sharpe_alert:
                return True, f"Performance: Sharpe dropped by {abs(drift_report.sharpe_change_pct):.1%}"

            if drift_report.severity in ("high", "critical"):
                return True, f"Drift: {drift_report.severity} severity detected"

        return False, "No retraining needed"

    def retrain(
        self,
        history_path: Path,
        prices_path: Path,
        trigger: str = "manual",
        force: bool = False,
    ) -> RetrainingResult:
        """
        Retrain the model.

        Args:
            history_path: Path to fundamentals history data
            prices_path: Path to price data
            trigger: Trigger reason ("scheduled", "performance", "manual")
            force: Force retraining even if not needed

        Returns:
            RetrainingResult with training outcome
        """
        retrain_id = f"retrain_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        started_at = datetime.now().isoformat()

        LOGGER.info(f"Starting retraining: {retrain_id} (trigger: {trigger})")

        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.config.training_lookback_days)

        # Get current production model for comparison
        production_id = self.registry.get_production_model_id()
        old_ic = 0.0
        old_sharpe = None

        if production_id:
            _, prod_metadata = self.registry.load(production_id)
            old_ic = prod_metadata.val_ic
            old_sharpe = prod_metadata.oos_sharpe

        try:
            # Train new model
            model_config = self.config.model_config or ModelConfig()

            model, results = train_ml_model(
                history_path=history_path,
                prices_path=prices_path,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                config=model_config,
            )

            # Register new model
            metadata = self.registry.register(
                model=model.model,
                model_type=model_config.model_type,
                training_start_date=start_date.strftime("%Y-%m-%d"),
                training_end_date=end_date.strftime("%Y-%m-%d"),
                training_samples=len(model.feature_names) if model.feature_names else 0,
                feature_names=model.feature_names or [],
                train_ic=results.train_ic,
                val_ic=results.val_ic,
                train_r2=results.train_score,
                val_r2=results.val_score,
                hyperparameters=results.best_params or {},
                tags=["retrain", trigger],
                notes=f"Automated retraining triggered by: {trigger}",
            )

            new_model_id = metadata.model_id
            new_ic = results.val_ic
            new_sharpe = None  # Will be set after OOS validation

            # Calculate improvements
            ic_improvement = new_ic - old_ic
            sharpe_improvement = None

            # Make deployment decision
            deployment_decision, deployment_reason = self._make_deployment_decision(
                old_ic=old_ic,
                new_ic=new_ic,
                old_sharpe=old_sharpe,
                new_sharpe=new_sharpe,
                has_production_model=production_id is not None,
            )

            if deployment_decision == "deployed":
                self.registry.deploy_to_production(new_model_id)
                LOGGER.info(f"Deployed new model: {new_model_id}")

            result = RetrainingResult(
                retrain_id=retrain_id,
                started_at=started_at,
                completed_at=datetime.now().isoformat(),
                trigger=trigger,
                training_success=True,
                new_model_id=new_model_id,
                training_samples=metadata.training_samples,
                training_start_date=start_date.strftime("%Y-%m-%d"),
                training_end_date=end_date.strftime("%Y-%m-%d"),
                old_model_id=production_id,
                old_ic=old_ic,
                new_ic=new_ic,
                ic_improvement=ic_improvement,
                old_sharpe=old_sharpe,
                new_sharpe=new_sharpe,
                sharpe_improvement=sharpe_improvement,
                deployment_decision=deployment_decision,
                deployment_reason=deployment_reason,
            )

        except Exception as e:
            LOGGER.error(f"Retraining failed: {e}", exc_info=True)

            result = RetrainingResult(
                retrain_id=retrain_id,
                started_at=started_at,
                completed_at=datetime.now().isoformat(),
                trigger=trigger,
                training_success=False,
                new_model_id=None,
                training_samples=0,
                training_start_date=start_date.strftime("%Y-%m-%d"),
                training_end_date=end_date.strftime("%Y-%m-%d"),
                old_model_id=production_id,
                old_ic=old_ic,
                new_ic=0.0,
                ic_improvement=0.0,
                old_sharpe=old_sharpe,
                new_sharpe=None,
                sharpe_improvement=None,
                deployment_decision="rejected",
                deployment_reason=f"Training failed: {str(e)}",
                error_message=str(e),
            )

        # Save to history
        self.history.add(result)
        self._save_history()

        LOGGER.info(f"Retraining completed: {result.deployment_decision}")

        return result

    def _make_deployment_decision(
        self,
        old_ic: float,
        new_ic: float,
        old_sharpe: Optional[float],
        new_sharpe: Optional[float],
        has_production_model: bool,
    ) -> Tuple[str, str]:
        """
        Decide whether to deploy the new model.

        Returns:
            (decision, reason) tuple
        """
        if not has_production_model:
            return "deployed", "No existing production model"

        ic_improvement = new_ic - old_ic

        # Check IC improvement
        if ic_improvement >= self.config.min_improvement_ic:
            return "deployed", f"IC improved by {ic_improvement:.4f}"

        # Check Sharpe improvement
        if old_sharpe is not None and new_sharpe is not None:
            sharpe_improvement = new_sharpe - old_sharpe
            if sharpe_improvement >= self.config.min_improvement_sharpe:
                return "deployed", f"Sharpe improved by {sharpe_improvement:.2f}"

        # Check if new model is significantly worse
        if ic_improvement < -0.02:
            return "rejected", f"IC degraded by {abs(ic_improvement):.4f}"

        # Neutral case - keep existing model
        return "rejected", f"Insufficient improvement (IC: {ic_improvement:.4f})"

    def run_scheduled_check(
        self,
        history_path: Path,
        prices_path: Path,
        drift_report: Optional[DriftReport] = None,
    ) -> Optional[RetrainingResult]:
        """
        Run scheduled retraining check.

        Args:
            history_path: Path to fundamentals history
            prices_path: Path to price data
            drift_report: Optional drift report

        Returns:
            RetrainingResult if retraining was triggered, None otherwise
        """
        should_retrain, reason = self.should_retrain(drift_report)

        LOGGER.info(f"Retraining check: should_retrain={should_retrain}, reason={reason}")

        if should_retrain:
            trigger = "performance" if "Performance" in reason or "Drift" in reason else "scheduled"
            return self.retrain(history_path, prices_path, trigger=trigger)

        return None

    def get_retraining_status(self) -> Dict:
        """Get current retraining status."""
        last_retrain = self.history.get_last_retrain()
        production_id = self.registry.get_production_model_id()

        status = {
            "production_model": production_id,
            "total_retrains": len(self.history.history),
            "successful_retrains": len(self.history.get_successful_retrains()),
            "last_retrain": None,
            "next_scheduled_retrain": None,
        }

        if last_retrain:
            last_retrain_date = datetime.fromisoformat(last_retrain.completed_at)
            days_since = (datetime.now() - last_retrain_date).days
            next_retrain_date = last_retrain_date + timedelta(days=self.config.retrain_interval_days)

            status["last_retrain"] = {
                "retrain_id": last_retrain.retrain_id,
                "date": last_retrain.completed_at,
                "days_ago": days_since,
                "success": last_retrain.training_success,
                "deployment": last_retrain.deployment_decision,
            }
            status["next_scheduled_retrain"] = next_retrain_date.isoformat()

        return status


def run_automated_retraining(
    history_path: Path,
    prices_path: Path,
    registry_path: Optional[Path] = None,
    config: Optional[RetrainingConfig] = None,
    force: bool = False,
) -> RetrainingResult:
    """
    Run automated retraining pipeline.

    Args:
        history_path: Path to fundamentals history
        prices_path: Path to price data
        registry_path: Path to model registry
        config: Retraining configuration
        force: Force retraining even if not scheduled

    Returns:
        RetrainingResult with outcome
    """
    LOGGER.info("Starting automated retraining pipeline")

    registry = ModelRegistry(registry_path) if registry_path else ModelRegistry()
    scheduler = RetrainingScheduler(registry=registry, config=config)

    if force:
        return scheduler.retrain(history_path, prices_path, trigger="manual", force=True)

    # Check for drift on production model
    production_id = registry.get_production_model_id()
    drift_report = None

    if production_id:
        from .drift_detection import run_drift_check
        try:
            drift_report = run_drift_check(
                model_id=production_id,
                data_path=history_path,
                prices_path=prices_path,
                registry_path=registry_path,
            )
        except Exception as e:
            LOGGER.warning(f"Drift check failed: {e}")

    # Run scheduled check
    result = scheduler.run_scheduled_check(history_path, prices_path, drift_report)

    if result is None:
        LOGGER.info("No retraining needed at this time")
        # Return a placeholder result
        return RetrainingResult(
            retrain_id=f"check_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            started_at=datetime.now().isoformat(),
            completed_at=datetime.now().isoformat(),
            trigger="scheduled_check",
            training_success=False,
            new_model_id=None,
            training_samples=0,
            training_start_date="",
            training_end_date="",
            old_model_id=production_id,
            old_ic=0.0,
            new_ic=0.0,
            ic_improvement=0.0,
            old_sharpe=None,
            new_sharpe=None,
            sharpe_improvement=None,
            deployment_decision="skipped",
            deployment_reason="No retraining needed",
        )

    return result
