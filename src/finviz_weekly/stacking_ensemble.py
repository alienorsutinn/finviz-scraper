"""
Stacking Ensemble Meta-Learner

Advanced ensemble that learns optimal model combinations:
- Trains a meta-model on base model predictions
- Supports multiple stacking levels
- Cross-validation to prevent leakage
- Dynamic weight adjustment based on recent performance
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Callable
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class BaseModelConfig:
    """Configuration for a base model in the ensemble."""
    name: str
    model_type: str  # 'xgboost', 'lightgbm', 'factor_weights', 'random_forest'
    weight: float = 1.0  # Initial weight
    params: Dict = field(default_factory=dict)
    feature_subset: Optional[List[str]] = None  # Use specific features only


@dataclass
class StackingConfig:
    """Configuration for stacking ensemble."""
    base_models: List[BaseModelConfig] = field(default_factory=list)
    meta_model_type: str = "ridge"  # ridge, xgboost, linear
    use_cv_predictions: bool = True  # Use CV to generate base predictions
    n_folds: int = 5
    include_original_features: bool = False  # Include raw features in meta-model
    dynamic_weighting: bool = True  # Adjust weights based on recent performance
    lookback_periods: int = 12  # Periods for dynamic weight calculation


@dataclass
class ModelPerformance:
    """Track model performance over time."""
    model_name: str
    period: str
    ic: float  # Information coefficient
    sharpe: float
    hit_rate: float
    mse: float


class BaseModel:
    """Wrapper for base models with common interface."""

    def __init__(self, config: BaseModelConfig):
        self.config = config
        self.model = None
        self.is_fitted = False
        self.feature_names: List[str] = []

    def fit(self, X: pd.DataFrame, y: pd.Series):
        """Fit the base model."""
        # Select feature subset if specified
        if self.config.feature_subset:
            X = X[self.config.feature_subset]

        self.feature_names = list(X.columns)

        if self.config.model_type == "xgboost":
            self._fit_xgboost(X, y)
        elif self.config.model_type == "lightgbm":
            self._fit_lightgbm(X, y)
        elif self.config.model_type == "random_forest":
            self._fit_random_forest(X, y)
        elif self.config.model_type == "factor_weights":
            self._fit_factor_weights(X, y)
        elif self.config.model_type == "ridge":
            self._fit_ridge(X, y)
        else:
            raise ValueError(f"Unknown model type: {self.config.model_type}")

        self.is_fitted = True

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Generate predictions."""
        if not self.is_fitted:
            raise RuntimeError("Model not fitted")

        # Select feature subset if specified
        if self.config.feature_subset:
            X = X[self.config.feature_subset]

        if self.config.model_type == "xgboost":
            import xgboost as xgb
            dtest = xgb.DMatrix(X)
            return self.model.predict(dtest)
        elif self.config.model_type == "lightgbm":
            return self.model.predict(X)
        elif self.config.model_type in ["random_forest", "ridge"]:
            return self.model.predict(X)
        elif self.config.model_type == "factor_weights":
            # Weighted sum of factors
            weights = self.model
            return (X * pd.Series(weights)).sum(axis=1).values
        else:
            raise ValueError(f"Unknown model type: {self.config.model_type}")

    def _fit_xgboost(self, X: pd.DataFrame, y: pd.Series):
        """Fit XGBoost model."""
        try:
            import xgboost as xgb

            params = {
                "objective": "reg:squarederror",
                "max_depth": 6,
                "learning_rate": 0.01,
                "subsample": 0.8,
                "colsample_bytree": 0.8,
                "reg_alpha": 0.1,
                "reg_lambda": 1.0,
            }
            params.update(self.config.params)

            dtrain = xgb.DMatrix(X, label=y)
            self.model = xgb.train(
                params,
                dtrain,
                num_boost_round=params.get('n_estimators', 500)
            )
        except ImportError:
            logger.error("xgboost not installed")
            raise

    def _fit_lightgbm(self, X: pd.DataFrame, y: pd.Series):
        """Fit LightGBM model."""
        try:
            import lightgbm as lgb

            params = {
                "objective": "regression",
                "max_depth": 6,
                "learning_rate": 0.01,
                "n_estimators": 500,
                "subsample": 0.8,
                "colsample_bytree": 0.8,
                "reg_alpha": 0.1,
                "reg_lambda": 1.0,
                "verbose": -1,
            }
            params.update(self.config.params)

            self.model = lgb.LGBMRegressor(**params)
            self.model.fit(X, y)
        except ImportError:
            logger.error("lightgbm not installed")
            raise

    def _fit_random_forest(self, X: pd.DataFrame, y: pd.Series):
        """Fit Random Forest model."""
        from sklearn.ensemble import RandomForestRegressor

        params = {
            "n_estimators": 200,
            "max_depth": 10,
            "min_samples_leaf": 5,
            "n_jobs": -1,
        }
        params.update(self.config.params)

        self.model = RandomForestRegressor(**params)
        self.model.fit(X, y)

    def _fit_factor_weights(self, X: pd.DataFrame, y: pd.Series):
        """Fit using Information Coefficient based weights."""
        # Calculate IC for each factor
        weights = {}
        for col in X.columns:
            ic = X[col].corr(y, method='spearman')
            weights[col] = max(0, ic)  # Only positive weights

        # Normalize weights
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        else:
            weights = {k: 1.0 / len(weights) for k in weights}

        self.model = weights

    def _fit_ridge(self, X: pd.DataFrame, y: pd.Series):
        """Fit Ridge regression."""
        from sklearn.linear_model import Ridge

        params = {"alpha": 1.0}
        params.update(self.config.params)

        self.model = Ridge(**params)
        self.model.fit(X, y)


class StackingEnsemble:
    """Stacking ensemble with meta-learner."""

    def __init__(self, config: Optional[StackingConfig] = None):
        self.config = config or self._default_config()
        self.base_models: Dict[str, BaseModel] = {}
        self.meta_model: Optional[BaseModel] = None
        self.is_fitted = False
        self.performance_history: List[ModelPerformance] = []
        self.dynamic_weights: Dict[str, float] = {}

    def _default_config(self) -> StackingConfig:
        """Create default stacking configuration."""
        return StackingConfig(
            base_models=[
                BaseModelConfig(
                    name="xgboost_main",
                    model_type="xgboost",
                    weight=0.35,
                    params={"max_depth": 6, "learning_rate": 0.01}
                ),
                BaseModelConfig(
                    name="xgboost_shallow",
                    model_type="xgboost",
                    weight=0.2,
                    params={"max_depth": 3, "learning_rate": 0.05}
                ),
                BaseModelConfig(
                    name="random_forest",
                    model_type="random_forest",
                    weight=0.2,
                    params={"n_estimators": 200, "max_depth": 8}
                ),
                BaseModelConfig(
                    name="factor_weights",
                    model_type="factor_weights",
                    weight=0.15
                ),
                BaseModelConfig(
                    name="ridge",
                    model_type="ridge",
                    weight=0.1,
                    params={"alpha": 1.0}
                ),
            ],
            meta_model_type="ridge",
            use_cv_predictions=True,
            n_folds=5
        )

    def fit(self, X: pd.DataFrame, y: pd.Series,
            groups: Optional[pd.Series] = None):
        """
        Fit the stacking ensemble.

        Args:
            X: Feature matrix
            y: Target variable
            groups: Optional time period groups for proper CV
        """
        logger.info(f"Fitting stacking ensemble with {len(self.config.base_models)} base models")

        # Initialize base models
        for model_config in self.config.base_models:
            self.base_models[model_config.name] = BaseModel(model_config)
            self.dynamic_weights[model_config.name] = model_config.weight

        # Generate out-of-fold predictions for meta-model
        if self.config.use_cv_predictions:
            oof_predictions = self._generate_oof_predictions(X, y, groups)
        else:
            # Fit on full data and use in-sample predictions
            oof_predictions = self._generate_insample_predictions(X, y)

        # Train meta-model on OOF predictions
        meta_features = pd.DataFrame(oof_predictions)

        if self.config.include_original_features:
            meta_features = pd.concat([meta_features, X.reset_index(drop=True)], axis=1)

        meta_config = BaseModelConfig(
            name="meta_model",
            model_type=self.config.meta_model_type,
            params={"alpha": 0.1} if self.config.meta_model_type == "ridge" else {}
        )
        self.meta_model = BaseModel(meta_config)
        self.meta_model.fit(meta_features, y.reset_index(drop=True))

        # Fit final base models on full data
        for name, model in self.base_models.items():
            logger.debug(f"Fitting base model: {name}")
            model.fit(X, y)

        self.is_fitted = True
        logger.info("Stacking ensemble fitted successfully")

    def _generate_oof_predictions(self, X: pd.DataFrame, y: pd.Series,
                                  groups: Optional[pd.Series] = None) -> Dict[str, np.ndarray]:
        """Generate out-of-fold predictions using cross-validation."""
        from sklearn.model_selection import KFold, GroupKFold

        n_samples = len(X)
        oof_preds = {name: np.zeros(n_samples) for name in self.base_models.keys()}

        if groups is not None:
            cv = GroupKFold(n_splits=self.config.n_folds)
            splits = cv.split(X, y, groups)
        else:
            cv = KFold(n_splits=self.config.n_folds, shuffle=True, random_state=42)
            splits = cv.split(X)

        for fold, (train_idx, val_idx) in enumerate(splits):
            logger.debug(f"Processing fold {fold + 1}/{self.config.n_folds}")

            X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
            y_train = y.iloc[train_idx]

            for name, model in self.base_models.items():
                # Create fresh model for this fold
                fold_model = BaseModel(model.config)
                fold_model.fit(X_train, y_train)
                oof_preds[name][val_idx] = fold_model.predict(X_val)

        return oof_preds

    def _generate_insample_predictions(self, X: pd.DataFrame,
                                       y: pd.Series) -> Dict[str, np.ndarray]:
        """Generate in-sample predictions (not recommended, use for debugging)."""
        predictions = {}
        for name, model in self.base_models.items():
            model.fit(X, y)
            predictions[name] = model.predict(X)
        return predictions

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Generate predictions using the stacking ensemble."""
        if not self.is_fitted:
            raise RuntimeError("Ensemble not fitted")

        # Get base model predictions
        base_preds = {}
        for name, model in self.base_models.items():
            base_preds[name] = model.predict(X)

        # Create meta-features
        meta_features = pd.DataFrame(base_preds)

        if self.config.include_original_features:
            meta_features = pd.concat([meta_features, X.reset_index(drop=True)], axis=1)

        # Generate final predictions
        return self.meta_model.predict(meta_features)

    def predict_with_uncertainty(self, X: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        """
        Generate predictions with uncertainty estimates.

        Returns:
            Tuple of (predictions, uncertainty)
        """
        if not self.is_fitted:
            raise RuntimeError("Ensemble not fitted")

        # Get all base predictions
        base_preds = []
        for name, model in self.base_models.items():
            base_preds.append(model.predict(X))

        base_preds = np.array(base_preds)

        # Ensemble prediction
        final_pred = self.predict(X)

        # Uncertainty as std of base predictions
        uncertainty = np.std(base_preds, axis=0)

        return final_pred, uncertainty

    def get_model_contributions(self, X: pd.DataFrame) -> pd.DataFrame:
        """Get contribution of each base model to final prediction."""
        if not self.is_fitted:
            raise RuntimeError("Ensemble not fitted")

        contributions = {}
        for name, model in self.base_models.items():
            pred = model.predict(X)
            weight = self.dynamic_weights.get(name, 1.0 / len(self.base_models))
            contributions[name] = pred * weight

        return pd.DataFrame(contributions, index=X.index)

    def update_weights(self, X_val: pd.DataFrame, y_val: pd.Series,
                      period: str = "latest"):
        """Update model weights based on recent performance."""
        if not self.config.dynamic_weighting:
            return

        performances = {}

        for name, model in self.base_models.items():
            pred = model.predict(X_val)

            # Calculate IC
            ic = pd.Series(pred).corr(y_val, method='spearman')

            # Calculate MSE
            mse = np.mean((pred - y_val.values) ** 2)

            # Track performance
            perf = ModelPerformance(
                model_name=name,
                period=period,
                ic=ic,
                sharpe=0.0,  # Would need returns data
                hit_rate=np.mean(np.sign(pred) == np.sign(y_val.values)),
                mse=mse
            )
            self.performance_history.append(perf)

            # Weight by IC (positive IC = better)
            performances[name] = max(0, ic)

        # Normalize weights
        total = sum(performances.values())
        if total > 0:
            for name in performances:
                self.dynamic_weights[name] = performances[name] / total
        else:
            # Equal weights if all negative
            for name in performances:
                self.dynamic_weights[name] = 1.0 / len(performances)

        logger.info(f"Updated dynamic weights: {self.dynamic_weights}")

    def get_feature_importance(self) -> pd.DataFrame:
        """Aggregate feature importance across base models."""
        importance_dfs = []

        for name, model in self.base_models.items():
            if model.config.model_type in ["xgboost", "lightgbm", "random_forest"]:
                try:
                    if model.config.model_type == "xgboost":
                        scores = model.model.get_score(importance_type="gain")
                        imp_df = pd.DataFrame([
                            {"feature": k, "importance": v, "model": name}
                            for k, v in scores.items()
                        ])
                    elif model.config.model_type == "lightgbm":
                        imp_df = pd.DataFrame({
                            "feature": model.feature_names,
                            "importance": model.model.feature_importances_,
                            "model": name
                        })
                    elif model.config.model_type == "random_forest":
                        imp_df = pd.DataFrame({
                            "feature": model.feature_names,
                            "importance": model.model.feature_importances_,
                            "model": name
                        })

                    importance_dfs.append(imp_df)
                except Exception as e:
                    logger.debug(f"Could not get feature importance for {name}: {e}")

        if not importance_dfs:
            return pd.DataFrame()

        # Aggregate across models
        all_imp = pd.concat(importance_dfs)
        agg_imp = all_imp.groupby("feature")["importance"].agg(["mean", "std"]).reset_index()
        agg_imp.columns = ["feature", "importance_mean", "importance_std"]
        return agg_imp.sort_values("importance_mean", ascending=False)

    def save(self, path: str):
        """Save the ensemble to disk."""
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)

        # Save config
        with open(path / "config.pkl", "wb") as f:
            pickle.dump(self.config, f)

        # Save base models
        for name, model in self.base_models.items():
            with open(path / f"base_{name}.pkl", "wb") as f:
                pickle.dump(model, f)

        # Save meta model
        if self.meta_model:
            with open(path / "meta_model.pkl", "wb") as f:
                pickle.dump(self.meta_model, f)

        # Save weights and performance
        with open(path / "state.pkl", "wb") as f:
            pickle.dump({
                "dynamic_weights": self.dynamic_weights,
                "performance_history": self.performance_history,
                "is_fitted": self.is_fitted
            }, f)

        logger.info(f"Ensemble saved to {path}")

    @classmethod
    def load(cls, path: str) -> "StackingEnsemble":
        """Load ensemble from disk."""
        path = Path(path)

        # Load config
        with open(path / "config.pkl", "rb") as f:
            config = pickle.load(f)

        ensemble = cls(config)

        # Load base models
        for model_config in config.base_models:
            model_path = path / f"base_{model_config.name}.pkl"
            if model_path.exists():
                with open(model_path, "rb") as f:
                    ensemble.base_models[model_config.name] = pickle.load(f)

        # Load meta model
        meta_path = path / "meta_model.pkl"
        if meta_path.exists():
            with open(meta_path, "rb") as f:
                ensemble.meta_model = pickle.load(f)

        # Load state
        state_path = path / "state.pkl"
        if state_path.exists():
            with open(state_path, "rb") as f:
                state = pickle.load(f)
                ensemble.dynamic_weights = state["dynamic_weights"]
                ensemble.performance_history = state["performance_history"]
                ensemble.is_fitted = state["is_fitted"]

        logger.info(f"Ensemble loaded from {path}")
        return ensemble


def create_default_ensemble() -> StackingEnsemble:
    """Create a default stacking ensemble with common configurations."""
    return StackingEnsemble()


def train_stacking_ensemble(X: pd.DataFrame, y: pd.Series,
                           time_column: Optional[str] = None) -> StackingEnsemble:
    """
    Train a stacking ensemble with sensible defaults.

    Args:
        X: Feature matrix
        y: Target variable (forward returns)
        time_column: Column name for time periods (for proper CV)
    """
    ensemble = create_default_ensemble()

    groups = None
    if time_column and time_column in X.columns:
        groups = X[time_column]
        X = X.drop(columns=[time_column])

    ensemble.fit(X, y, groups=groups)
    return ensemble


class AdaptiveStackingEnsemble(StackingEnsemble):
    """
    Stacking ensemble with adaptive regime-based weighting.

    Adjusts model weights based on detected market regime.
    """

    def __init__(self, config: Optional[StackingConfig] = None):
        super().__init__(config)
        self.regime_weights: Dict[str, Dict[str, float]] = {
            "bull": {},
            "bear": {},
            "high_vol": {},
            "low_vol": {},
        }

    def learn_regime_weights(self, X: pd.DataFrame, y: pd.Series,
                            regimes: pd.Series):
        """Learn optimal weights for each regime."""
        for regime in regimes.unique():
            mask = regimes == regime
            X_regime = X[mask]
            y_regime = y[mask]

            if len(X_regime) < 50:
                continue

            # Evaluate each model on this regime
            performances = {}
            for name, model in self.base_models.items():
                pred = model.predict(X_regime)
                ic = pd.Series(pred).corr(y_regime, method='spearman')
                performances[name] = max(0, ic)

            # Normalize
            total = sum(performances.values())
            if total > 0:
                self.regime_weights[regime] = {
                    k: v / total for k, v in performances.items()
                }

        logger.info(f"Learned regime weights: {self.regime_weights}")

    def predict_with_regime(self, X: pd.DataFrame,
                           regime: str = "bull") -> np.ndarray:
        """Generate predictions using regime-specific weights."""
        if regime in self.regime_weights and self.regime_weights[regime]:
            weights = self.regime_weights[regime]
        else:
            weights = self.dynamic_weights

        # Weighted average of base predictions
        predictions = np.zeros(len(X))
        for name, model in self.base_models.items():
            weight = weights.get(name, 1.0 / len(self.base_models))
            predictions += weight * model.predict(X)

        return predictions
