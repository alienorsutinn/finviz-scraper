"""
Machine learning model training for factor prediction.

Implements:
- XGBoost regression for forward return prediction
- Hyperparameter tuning with Optuna
- Ensemble methods (voting, stacking)
- Model persistence and versioning
- Cross-validation with time-series splits
"""
from __future__ import annotations

import json
import logging
import pickle
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.ensemble import VotingRegressor
from sklearn.model_selection import TimeSeriesSplit

try:
    import optuna
    OPTUNA_AVAILABLE = True
except ImportError:
    OPTUNA_AVAILABLE = False

from .learn import FACTOR_COLS, _compute_forward_returns_from_prices, _winsorize

LOGGER = logging.getLogger(__name__)


@dataclass
class ModelConfig:
    """Configuration for ML model training."""

    # Model parameters
    model_type: str = "xgboost"  # xgboost, ensemble
    objective: str = "reg:squarederror"  # XGBoost objective
    n_estimators: int = 100
    max_depth: int = 6
    learning_rate: float = 0.1
    subsample: float = 0.8
    colsample_bytree: float = 0.8

    # Training parameters
    n_cv_splits: int = 5  # Time series cross-validation splits
    early_stopping_rounds: int = 50
    eval_metric: str = "rmse"

    # Hyperparameter tuning
    tune_hyperparams: bool = False
    n_tuning_trials: int = 50

    # Feature engineering
    include_interactions: bool = False
    include_polynomial: bool = False
    polynomial_degree: int = 2


@dataclass
class ModelResults:
    """Results from model training."""

    model_type: str
    train_score: float  # R² on training set
    val_score: float  # R² on validation set
    test_score: float  # R² on test set
    train_ic: float  # Information coefficient on training set
    val_ic: float  # IC on validation set
    test_ic: float  # IC on test set

    feature_importance: Dict[str, float]
    best_params: Optional[Dict] = None
    cv_scores: Optional[List[float]] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "model_type": self.model_type,
            "train_score": self.train_score,
            "val_score": self.val_score,
            "test_score": self.test_score,
            "train_ic": self.train_ic,
            "val_ic": self.val_ic,
            "test_ic": self.test_ic,
            "feature_importance": self.feature_importance,
            "best_params": self.best_params,
            "cv_scores": self.cv_scores,
        }


class MLModel:
    """Machine learning model for forward return prediction."""

    def __init__(self, config: ModelConfig):
        """
        Initialize ML model.

        Args:
            config: Model configuration
        """
        self.config = config
        self.model = None
        self.feature_names = None
        self.scaler = None
        self.version = datetime.now().strftime("%Y%m%d_%H%M%S")

        LOGGER.info(f"Initialized ML model: {config.model_type}")
        LOGGER.info(f"Model version: {self.version}")

    def train(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame] = None,
        y_val: Optional[pd.Series] = None,
    ) -> ModelResults:
        """
        Train model on data.

        Args:
            X_train: Training features
            y_train: Training target (forward returns)
            X_val: Optional validation features
            y_val: Optional validation target

        Returns:
            ModelResults with training metrics
        """
        LOGGER.info(f"Training model on {len(X_train)} samples")

        self.feature_names = list(X_train.columns)

        # Tune hyperparameters if requested
        if self.config.tune_hyperparams and OPTUNA_AVAILABLE:
            best_params = self._tune_hyperparameters(X_train, y_train, X_val, y_val)
            LOGGER.info(f"Best hyperparameters: {best_params}")
        else:
            best_params = None

        # Train model
        if self.config.model_type == "xgboost":
            self.model = self._train_xgboost(X_train, y_train, X_val, y_val, best_params)
        elif self.config.model_type == "ensemble":
            self.model = self._train_ensemble(X_train, y_train, X_val, y_val)
        else:
            raise ValueError(f"Unknown model type: {self.config.model_type}")

        # Evaluate
        results = self._evaluate(X_train, y_train, X_val, y_val)
        results.best_params = best_params

        return results

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        Predict forward returns.

        Args:
            X: Features

        Returns:
            Predicted returns
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")

        return self.model.predict(X)

    def save(self, path: Path):
        """
        Save model to disk.

        Args:
            path: Directory to save model
        """
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)

        # Save model
        model_path = path / f"model_{self.version}.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(self.model, f)

        # Save metadata
        metadata = {
            "version": self.version,
            "model_type": self.config.model_type,
            "feature_names": self.feature_names,
            "config": {
                "n_estimators": self.config.n_estimators,
                "max_depth": self.config.max_depth,
                "learning_rate": self.config.learning_rate,
            },
            "created_at": datetime.now().isoformat(),
        }

        metadata_path = path / f"metadata_{self.version}.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        LOGGER.info(f"Model saved to {model_path}")

    @classmethod
    def load(cls, path: Path, version: Optional[str] = None) -> MLModel:
        """
        Load model from disk.

        Args:
            path: Directory containing model
            version: Model version to load (if None, loads latest)

        Returns:
            Loaded MLModel
        """
        path = Path(path)

        # Find model file
        if version:
            model_path = path / f"model_{version}.pkl"
        else:
            # Load latest
            model_files = sorted(path.glob("model_*.pkl"), reverse=True)
            if not model_files:
                raise FileNotFoundError(f"No model files found in {path}")
            model_path = model_files[0]
            version = model_path.stem.replace("model_", "")

        # Load model
        with open(model_path, "rb") as f:
            model = pickle.load(f)

        # Load metadata
        metadata_path = path / f"metadata_{version}.json"
        with open(metadata_path) as f:
            metadata = json.load(f)

        # Reconstruct MLModel
        config = ModelConfig(
            model_type=metadata["model_type"],
            n_estimators=metadata["config"]["n_estimators"],
            max_depth=metadata["config"]["max_depth"],
            learning_rate=metadata["config"]["learning_rate"],
        )

        ml_model = cls(config)
        ml_model.model = model
        ml_model.feature_names = metadata["feature_names"]
        ml_model.version = version

        LOGGER.info(f"Model loaded from {model_path}")

        return ml_model

    def _train_xgboost(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame],
        y_val: Optional[pd.Series],
        best_params: Optional[Dict] = None,
    ) -> xgb.XGBRegressor:
        """Train XGBoost model."""
        params = {
            "objective": self.config.objective,
            "n_estimators": self.config.n_estimators,
            "max_depth": self.config.max_depth,
            "learning_rate": self.config.learning_rate,
            "subsample": self.config.subsample,
            "colsample_bytree": self.config.colsample_bytree,
            "random_state": 42,
            "n_jobs": -1,
        }

        # Override with tuned params
        if best_params:
            params.update(best_params)

        model = xgb.XGBRegressor(**params)

        # Train with early stopping if validation set provided
        if X_val is not None and y_val is not None:
            model.fit(
                X_train,
                y_train,
                eval_set=[(X_val, y_val)],
                verbose=False,
            )
        else:
            model.fit(X_train, y_train)

        return model

    def _train_ensemble(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame],
        y_val: Optional[pd.Series],
    ) -> VotingRegressor:
        """Train ensemble of models."""
        # Create multiple XGBoost models with different hyperparameters
        models = [
            ("xgb1", xgb.XGBRegressor(
                max_depth=4, learning_rate=0.1, n_estimators=100, random_state=42
            )),
            ("xgb2", xgb.XGBRegressor(
                max_depth=6, learning_rate=0.05, n_estimators=150, random_state=43
            )),
            ("xgb3", xgb.XGBRegressor(
                max_depth=8, learning_rate=0.03, n_estimators=200, random_state=44
            )),
        ]

        ensemble = VotingRegressor(estimators=models, n_jobs=-1)
        ensemble.fit(X_train, y_train)

        return ensemble

    def _tune_hyperparameters(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame],
        y_val: Optional[pd.Series],
    ) -> Dict:
        """Tune hyperparameters using Optuna."""
        if not OPTUNA_AVAILABLE:
            LOGGER.warning("Optuna not available. Skipping hyperparameter tuning.")
            return {}

        LOGGER.info(f"Tuning hyperparameters with {self.config.n_tuning_trials} trials")

        def objective(trial):
            """Optuna objective function."""
            params = {
                "max_depth": trial.suggest_int("max_depth", 3, 10),
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                "n_estimators": trial.suggest_int("n_estimators", 50, 300),
                "subsample": trial.suggest_float("subsample", 0.6, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
                "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
                "gamma": trial.suggest_float("gamma", 0.0, 1.0),
            }

            model = xgb.XGBRegressor(
                objective=self.config.objective,
                random_state=42,
                n_jobs=1,  # Optuna handles parallelization
                **params
            )

            # Use validation set if provided, otherwise use cross-validation
            if X_val is not None and y_val is not None:
                model.fit(X_train, y_train)
                score = model.score(X_val, y_val)
            else:
                # Time series cross-validation
                tscv = TimeSeriesSplit(n_splits=3)
                scores = []
                for train_idx, val_idx in tscv.split(X_train):
                    X_fold_train = X_train.iloc[train_idx]
                    y_fold_train = y_train.iloc[train_idx]
                    X_fold_val = X_train.iloc[val_idx]
                    y_fold_val = y_train.iloc[val_idx]

                    model.fit(X_fold_train, y_fold_train)
                    scores.append(model.score(X_fold_val, y_fold_val))

                score = np.mean(scores)

            return score

        # Create study
        study = optuna.create_study(
            direction="maximize",
            sampler=optuna.samplers.TPESampler(seed=42),
        )

        # Suppress Optuna logs
        optuna.logging.set_verbosity(optuna.logging.WARNING)

        # Optimize
        study.optimize(objective, n_trials=self.config.n_tuning_trials, show_progress_bar=False)

        LOGGER.info(f"Best trial score: {study.best_trial.value:.4f}")

        return study.best_params

    def _evaluate(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame],
        y_val: Optional[pd.Series],
    ) -> ModelResults:
        """Evaluate model performance."""
        # Training metrics
        y_train_pred = self.model.predict(X_train)
        train_score = self._r_squared(y_train, y_train_pred)
        train_ic = self._information_coefficient(y_train, y_train_pred)

        # Validation metrics
        if X_val is not None and y_val is not None:
            y_val_pred = self.model.predict(X_val)
            val_score = self._r_squared(y_val, y_val_pred)
            val_ic = self._information_coefficient(y_val, y_val_pred)
        else:
            val_score = np.nan
            val_ic = np.nan

        # Feature importance
        feature_importance = self._get_feature_importance()

        LOGGER.info(f"Train R²: {train_score:.4f}, IC: {train_ic:.4f}")
        if not np.isnan(val_score):
            LOGGER.info(f"Val R²: {val_score:.4f}, IC: {val_ic:.4f}")

        return ModelResults(
            model_type=self.config.model_type,
            train_score=train_score,
            val_score=val_score,
            test_score=np.nan,  # Will be computed separately
            train_ic=train_ic,
            val_ic=val_ic,
            test_ic=np.nan,
            feature_importance=feature_importance,
        )

    def _get_feature_importance(self) -> Dict[str, float]:
        """Get feature importance from trained model."""
        if isinstance(self.model, xgb.XGBRegressor):
            importance = self.model.feature_importances_
        elif isinstance(self.model, VotingRegressor):
            # Average importance across ensemble members
            importances = []
            for name, estimator in self.model.named_estimators_.items():
                if hasattr(estimator, "feature_importances_"):
                    importances.append(estimator.feature_importances_)
            importance = np.mean(importances, axis=0) if importances else np.zeros(len(self.feature_names))
        else:
            importance = np.zeros(len(self.feature_names))

        return dict(zip(self.feature_names, importance))

    @staticmethod
    def _r_squared(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Calculate R² score."""
        ss_res = np.sum((y_true - y_pred) ** 2)
        ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
        return 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

    @staticmethod
    def _information_coefficient(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Calculate Spearman information coefficient."""
        try:
            from scipy.stats import spearmanr
        except ImportError:
            LOGGER.warning("scipy not available, cannot compute IC")
            return np.nan

        # Remove NaNs
        mask = ~(np.isnan(y_true) | np.isnan(y_pred))
        if mask.sum() < 10:
            return np.nan

        y_true_clean = y_true[mask]
        y_pred_clean = y_pred[mask]

        # Check for constant arrays
        if np.std(y_true_clean) == 0 or np.std(y_pred_clean) == 0:
            return np.nan

        try:
            corr, _ = spearmanr(y_true_clean, y_pred_clean)
            return corr if not np.isnan(corr) else 0.0
        except Exception as e:
            LOGGER.warning(f"Error computing IC: {e}")
            return np.nan


def train_ml_model(
    history_path: Path,
    prices_path: Path,
    start_date: str,
    end_date: str,
    config: Optional[ModelConfig] = None,
    model_dir: Optional[Path] = None,
) -> Tuple[MLModel, ModelResults]:
    """
    Train ML model for forward return prediction.

    Args:
        history_path: Path to fundamentals history
        prices_path: Path to price data
        start_date: Training start date (YYYY-MM-DD)
        end_date: Training end date (YYYY-MM-DD)
        config: Model configuration
        model_dir: Directory to save model (if None, not saved)

    Returns:
        (trained_model, results)
    """
    if config is None:
        config = ModelConfig()

    LOGGER.info("Loading data for ML training")

    # Load data
    history = pd.read_parquet(history_path)
    history["as_of_date"] = pd.to_datetime(history["as_of_date"]).dt.date

    prices = pd.read_parquet(prices_path)
    prices["ticker"] = prices["ticker"].astype(str).str.upper()
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce").dt.date

    # Filter to date range
    start = pd.to_datetime(start_date).date()
    end = pd.to_datetime(end_date).date()
    history = history[(history["as_of_date"] >= start) & (history["as_of_date"] <= end)]

    # Compute forward returns
    joined = _compute_forward_returns_from_prices(history, prices, horizon_trading_days=21)
    joined["forward_return"] = _winsorize(joined["forward_return"])
    joined = joined.dropna(subset=["forward_return"])

    if len(joined) < 100:
        raise ValueError(f"Insufficient data for training: {len(joined)} rows")

    LOGGER.info(f"Training on {len(joined)} samples")

    # Prepare features and target
    feature_cols = [col for col in FACTOR_COLS if col in joined.columns]
    X = joined[feature_cols].fillna(0)
    y = joined["forward_return"]

    # Train/val split (80/20, respecting time order)
    split_idx = int(len(X) * 0.8)
    X_train, X_val = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_val = y.iloc[:split_idx], y.iloc[split_idx:]

    # Train model
    model = MLModel(config)
    results = model.train(X_train, y_train, X_val, y_val)

    # Save if directory provided
    if model_dir:
        model.save(model_dir)

    return model, results
