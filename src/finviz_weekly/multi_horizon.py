"""
Multi-Horizon Prediction Models.

Train separate models for different time horizons:
- 5-day: Short-term momentum, technical factors
- 21-day: Medium-term value, quality factors
- 63-day: Long-term fundamentals

Each horizon may have different feature importance and optimal hyperparameters.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)

# Optional imports
try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False

try:
    import optuna
    OPTUNA_AVAILABLE = True
except ImportError:
    OPTUNA_AVAILABLE = False


@dataclass
class HorizonConfig:
    """Configuration for a single horizon model."""

    horizon_days: int
    name: str

    # Feature groups to emphasize
    feature_weights: Dict[str, float] = field(default_factory=dict)

    # Model hyperparameters (defaults optimized per horizon)
    max_depth: int = 6
    learning_rate: float = 0.01
    n_estimators: int = 500
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    reg_alpha: float = 0.1
    reg_lambda: float = 1.0

    # Training settings
    early_stopping_rounds: int = 50
    validation_size: float = 0.2


@dataclass
class MultiHorizonConfig:
    """Configuration for multi-horizon prediction system."""

    # Standard horizons
    horizons: List[HorizonConfig] = field(default_factory=list)

    # Feature groups
    momentum_features: List[str] = field(default_factory=list)
    value_features: List[str] = field(default_factory=list)
    quality_features: List[str] = field(default_factory=list)
    technical_features: List[str] = field(default_factory=list)

    # Ensemble settings
    ensemble_method: str = "weighted_average"  # or "stacking"

    def __post_init__(self):
        if not self.horizons:
            # Default horizons with optimized settings
            self.horizons = [
                HorizonConfig(
                    horizon_days=5,
                    name="short_term",
                    feature_weights={
                        "momentum": 0.4,
                        "technical": 0.3,
                        "value": 0.15,
                        "quality": 0.15,
                    },
                    max_depth=4,
                    learning_rate=0.05,
                    n_estimators=300,
                ),
                HorizonConfig(
                    horizon_days=21,
                    name="medium_term",
                    feature_weights={
                        "momentum": 0.25,
                        "technical": 0.15,
                        "value": 0.35,
                        "quality": 0.25,
                    },
                    max_depth=6,
                    learning_rate=0.02,
                    n_estimators=500,
                ),
                HorizonConfig(
                    horizon_days=63,
                    name="long_term",
                    feature_weights={
                        "momentum": 0.1,
                        "technical": 0.1,
                        "value": 0.4,
                        "quality": 0.4,
                    },
                    max_depth=5,
                    learning_rate=0.01,
                    n_estimators=800,
                ),
            ]

        # Default feature groups if not specified
        if not self.momentum_features:
            self.momentum_features = [
                "perf_week", "perf_month", "perf_quarter", "perf_half_y",
                "rel_volume", "sma20", "sma50", "sma200",
            ]
        if not self.value_features:
            self.value_features = [
                "p_e", "forward_p_e", "p_s", "p_b", "p_fcf",
                "peg", "ev_ebitda", "dividend_yield",
            ]
        if not self.quality_features:
            self.quality_features = [
                "roe", "roi", "roa", "profit_margin", "oper_margin",
                "gross_margin", "current_ratio", "quick_ratio",
                "debt_eq", "eps_growth_this_y", "eps_growth_next_y",
            ]
        if not self.technical_features:
            self.technical_features = [
                "rsi_14", "volatility_w", "volatility_m",
                "beta", "atr", "52w_high_pct", "52w_low_pct",
            ]


@dataclass
class HorizonModelResult:
    """Results from training a single horizon model."""

    horizon_days: int
    model_name: str
    train_date: str

    # Performance metrics
    train_ic: float  # Information coefficient on train
    val_ic: float  # Information coefficient on validation
    train_rmse: float
    val_rmse: float

    # Feature importance
    feature_importance: Dict[str, float]
    top_features: List[str]

    # Model artifact
    model: Any = None


@dataclass
class MultiHorizonResult:
    """Combined results from multi-horizon training."""

    train_date: str
    horizons_trained: List[int]

    # Per-horizon results
    horizon_results: Dict[int, HorizonModelResult]

    # Ensemble performance
    ensemble_ic: Optional[float] = None
    ensemble_sharpe: Optional[float] = None

    # Feature analysis
    consistent_features: List[str] = field(default_factory=list)
    horizon_specific_features: Dict[int, List[str]] = field(default_factory=dict)


class HorizonModel:
    """
    Single horizon prediction model.

    Wraps XGBoost with horizon-specific configuration.
    """

    def __init__(self, config: HorizonConfig):
        """
        Initialize horizon model.

        Args:
            config: Horizon-specific configuration
        """
        self.config = config
        self.model = None
        self.feature_names: List[str] = []
        self.feature_importance: Dict[str, float] = {}

        if not XGBOOST_AVAILABLE:
            raise ImportError("xgboost is required for horizon models")

    def train(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame] = None,
        y_val: Optional[pd.Series] = None,
    ) -> HorizonModelResult:
        """
        Train the horizon model.

        Args:
            X_train: Training features
            y_train: Training targets
            X_val: Validation features (optional)
            y_val: Validation targets (optional)

        Returns:
            HorizonModelResult with training metrics
        """
        self.feature_names = list(X_train.columns)

        # Prepare DMatrix
        dtrain = xgb.DMatrix(X_train, label=y_train)

        params = {
            "objective": "reg:squarederror",
            "max_depth": self.config.max_depth,
            "learning_rate": self.config.learning_rate,
            "subsample": self.config.subsample,
            "colsample_bytree": self.config.colsample_bytree,
            "reg_alpha": self.config.reg_alpha,
            "reg_lambda": self.config.reg_lambda,
            "verbosity": 0,
        }

        evals = [(dtrain, "train")]
        if X_val is not None and y_val is not None:
            dval = xgb.DMatrix(X_val, label=y_val)
            evals.append((dval, "val"))

        # Train model
        self.model = xgb.train(
            params,
            dtrain,
            num_boost_round=self.config.n_estimators,
            evals=evals,
            early_stopping_rounds=self.config.early_stopping_rounds if X_val is not None else None,
            verbose_eval=False,
        )

        # Get feature importance
        importance = self.model.get_score(importance_type="gain")
        self.feature_importance = {
            self.feature_names[int(k.replace("f", ""))]: v
            for k, v in importance.items()
            if k.startswith("f") and k[1:].isdigit()
        }

        # Fallback: use feature names directly if available
        if not self.feature_importance:
            self.feature_importance = {k: v for k, v in importance.items()}

        # Calculate metrics
        train_pred = self.model.predict(dtrain)
        train_ic = self._calculate_ic(y_train.values, train_pred)
        train_rmse = np.sqrt(np.mean((y_train.values - train_pred) ** 2))

        val_ic = 0.0
        val_rmse = 0.0
        if X_val is not None and y_val is not None:
            val_pred = self.model.predict(dval)
            val_ic = self._calculate_ic(y_val.values, val_pred)
            val_rmse = np.sqrt(np.mean((y_val.values - val_pred) ** 2))

        # Get top features
        sorted_importance = sorted(
            self.feature_importance.items(),
            key=lambda x: x[1],
            reverse=True,
        )
        top_features = [f[0] for f in sorted_importance[:10]]

        return HorizonModelResult(
            horizon_days=self.config.horizon_days,
            model_name=self.config.name,
            train_date=datetime.now().isoformat(),
            train_ic=float(train_ic),
            val_ic=float(val_ic),
            train_rmse=float(train_rmse),
            val_rmse=float(val_rmse),
            feature_importance=self.feature_importance,
            top_features=top_features,
            model=self.model,
        )

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        Generate predictions.

        Args:
            X: Features DataFrame

        Returns:
            Array of predictions
        """
        if self.model is None:
            raise ValueError("Model not trained yet")

        dtest = xgb.DMatrix(X)
        return self.model.predict(dtest)

    def _calculate_ic(self, y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Calculate information coefficient (Spearman correlation)."""
        from scipy.stats import spearmanr

        # Remove NaN values
        valid_mask = ~(np.isnan(y_true) | np.isnan(y_pred))
        if valid_mask.sum() < 10:
            return 0.0

        corr, _ = spearmanr(y_true[valid_mask], y_pred[valid_mask])
        return corr if not np.isnan(corr) else 0.0


class MultiHorizonPredictor:
    """
    Multi-horizon prediction system.

    Trains separate models for each horizon and combines predictions.
    """

    def __init__(self, config: Optional[MultiHorizonConfig] = None):
        """
        Initialize multi-horizon predictor.

        Args:
            config: Multi-horizon configuration
        """
        self.config = config or MultiHorizonConfig()
        self.models: Dict[int, HorizonModel] = {}
        self.results: Optional[MultiHorizonResult] = None

        LOGGER.info(f"Multi-horizon predictor initialized with {len(self.config.horizons)} horizons")

    def train(
        self,
        data: pd.DataFrame,
        target_prefix: str = "alpha",
        feature_cols: Optional[List[str]] = None,
        validation_size: float = 0.2,
    ) -> MultiHorizonResult:
        """
        Train models for all horizons.

        Args:
            data: Training data with features and targets
            target_prefix: Prefix for target columns (e.g., "alpha" for "alpha_5d")
            feature_cols: Feature columns to use (auto-detected if None)
            validation_size: Fraction for validation set

        Returns:
            MultiHorizonResult with training results
        """
        horizon_results = {}

        # Auto-detect features if not provided
        if feature_cols is None:
            target_cols = [c for c in data.columns if c.startswith(target_prefix)]
            feature_cols = [c for c in data.columns if c not in target_cols
                          and c not in ["ticker", "date", "as_of_date"]]

        LOGGER.info(f"Training with {len(feature_cols)} features")

        for horizon_config in self.config.horizons:
            horizon = horizon_config.horizon_days
            target_col = f"{target_prefix}_{horizon}d"

            if target_col not in data.columns:
                LOGGER.warning(f"Target column {target_col} not found, skipping horizon {horizon}")
                continue

            LOGGER.info(f"Training {horizon}-day model...")

            # Prepare data
            train_data = data[feature_cols + [target_col]].dropna()

            if len(train_data) < 100:
                LOGGER.warning(f"Insufficient data for {horizon}-day model: {len(train_data)} rows")
                continue

            X = train_data[feature_cols]
            y = train_data[target_col]

            # Train/validation split (time-based)
            split_idx = int(len(X) * (1 - validation_size))
            X_train, X_val = X.iloc[:split_idx], X.iloc[split_idx:]
            y_train, y_val = y.iloc[:split_idx], y.iloc[split_idx:]

            # Train model
            model = HorizonModel(horizon_config)
            result = model.train(X_train, y_train, X_val, y_val)

            self.models[horizon] = model
            horizon_results[horizon] = result

            LOGGER.info(f"  {horizon}d model: train_ic={result.train_ic:.4f}, val_ic={result.val_ic:.4f}")

        # Analyze feature consistency
        consistent_features = self._find_consistent_features(horizon_results)
        horizon_specific = self._find_horizon_specific_features(horizon_results)

        # Calculate ensemble performance
        ensemble_ic = self._calculate_ensemble_ic(data, target_prefix, feature_cols)

        self.results = MultiHorizonResult(
            train_date=datetime.now().isoformat(),
            horizons_trained=list(horizon_results.keys()),
            horizon_results=horizon_results,
            ensemble_ic=ensemble_ic,
            consistent_features=consistent_features,
            horizon_specific_features=horizon_specific,
        )

        return self.results

    def predict(
        self,
        X: pd.DataFrame,
        horizons: Optional[List[int]] = None,
    ) -> Dict[int, np.ndarray]:
        """
        Generate predictions for specified horizons.

        Args:
            X: Feature DataFrame
            horizons: Horizons to predict (all if None)

        Returns:
            Dict mapping horizon -> predictions
        """
        if horizons is None:
            horizons = list(self.models.keys())

        predictions = {}
        for horizon in horizons:
            if horizon in self.models:
                predictions[horizon] = self.models[horizon].predict(X)

        return predictions

    def predict_ensemble(
        self,
        X: pd.DataFrame,
        weights: Optional[Dict[int, float]] = None,
    ) -> np.ndarray:
        """
        Generate weighted ensemble prediction.

        Args:
            X: Feature DataFrame
            weights: Optional horizon weights (equal if None)

        Returns:
            Ensemble prediction array
        """
        predictions = self.predict(X)

        if not predictions:
            return np.zeros(len(X))

        if weights is None:
            weights = {h: 1.0 / len(predictions) for h in predictions}

        # Weighted average
        ensemble = np.zeros(len(X))
        total_weight = 0

        for horizon, pred in predictions.items():
            weight = weights.get(horizon, 0)
            ensemble += pred * weight
            total_weight += weight

        if total_weight > 0:
            ensemble /= total_weight

        return ensemble

    def _find_consistent_features(
        self,
        horizon_results: Dict[int, HorizonModelResult],
        top_n: int = 10,
    ) -> List[str]:
        """Find features that are important across all horizons."""
        if not horizon_results:
            return []

        # Get top features for each horizon
        feature_sets = []
        for result in horizon_results.values():
            top_features = set(result.top_features[:top_n])
            feature_sets.append(top_features)

        # Find intersection
        if feature_sets:
            consistent = feature_sets[0]
            for fs in feature_sets[1:]:
                consistent = consistent.intersection(fs)
            return list(consistent)

        return []

    def _find_horizon_specific_features(
        self,
        horizon_results: Dict[int, HorizonModelResult],
        top_n: int = 5,
    ) -> Dict[int, List[str]]:
        """Find features that are uniquely important for each horizon."""
        if not horizon_results:
            return {}

        # Get all top features
        all_top = {}
        for horizon, result in horizon_results.items():
            all_top[horizon] = set(result.top_features[:top_n * 2])

        # Find horizon-specific features
        specific = {}
        for horizon, features in all_top.items():
            other_features = set()
            for h, f in all_top.items():
                if h != horizon:
                    other_features.update(f)

            unique = features - other_features
            specific[horizon] = list(unique)[:top_n]

        return specific

    def _calculate_ensemble_ic(
        self,
        data: pd.DataFrame,
        target_prefix: str,
        feature_cols: List[str],
    ) -> Optional[float]:
        """Calculate ensemble IC across all horizons."""
        if not self.models:
            return None

        try:
            X = data[feature_cols].dropna()
            ensemble_pred = self.predict_ensemble(X)

            # Use medium-term target as reference
            target_col = f"{target_prefix}_21d"
            if target_col in data.columns:
                y = data.loc[X.index, target_col]
                valid = ~(np.isnan(y) | np.isnan(ensemble_pred))
                if valid.sum() > 10:
                    from scipy.stats import spearmanr
                    corr, _ = spearmanr(y[valid], ensemble_pred[valid])
                    return float(corr) if not np.isnan(corr) else None
        except Exception as e:
            LOGGER.warning(f"Could not calculate ensemble IC: {e}")

        return None

    def get_feature_importance_comparison(self) -> pd.DataFrame:
        """
        Compare feature importance across horizons.

        Returns:
            DataFrame with feature importance per horizon
        """
        if not self.models:
            return pd.DataFrame()

        # Collect all features
        all_features = set()
        for model in self.models.values():
            all_features.update(model.feature_importance.keys())

        # Build comparison DataFrame
        rows = []
        for feature in all_features:
            row = {"feature": feature}
            for horizon, model in self.models.items():
                row[f"importance_{horizon}d"] = model.feature_importance.get(feature, 0)
            rows.append(row)

        df = pd.DataFrame(rows)

        # Add average importance
        imp_cols = [c for c in df.columns if c.startswith("importance_")]
        df["avg_importance"] = df[imp_cols].mean(axis=1)
        df = df.sort_values("avg_importance", ascending=False)

        return df

    def save(self, path: Path) -> None:
        """Save all models to disk."""
        path.mkdir(parents=True, exist_ok=True)

        for horizon, model in self.models.items():
            model_path = path / f"model_{horizon}d.json"
            model.model.save_model(str(model_path))

        LOGGER.info(f"Saved {len(self.models)} models to {path}")

    def load(self, path: Path) -> None:
        """Load models from disk."""
        for horizon_config in self.config.horizons:
            horizon = horizon_config.horizon_days
            model_path = path / f"model_{horizon}d.json"

            if model_path.exists():
                model = HorizonModel(horizon_config)
                model.model = xgb.Booster()
                model.model.load_model(str(model_path))
                self.models[horizon] = model

        LOGGER.info(f"Loaded {len(self.models)} models from {path}")
