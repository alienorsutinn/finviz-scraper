"""Feature importance analysis for factor-based models."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .learn import (
    FACTOR_COLS,
    _compute_forward_returns_from_prices,
    _spearman_ic,
    _winsorize,
    TrainConfig,
)

LOGGER = logging.getLogger(__name__)


@dataclass
class FeatureImportanceResults:
    """Results from feature importance analysis."""

    # Permutation importance
    permutation_importance: Dict[str, float]  # {feature: importance_score}
    baseline_ic: float  # Baseline IC with all features

    # Feature correlations
    feature_correlations: pd.DataFrame  # Correlation matrix of features

    # Summary statistics
    top_features: List[Tuple[str, float]]  # [(feature, importance), ...] sorted by importance
    redundant_pairs: List[Tuple[str, str, float]]  # [(feat1, feat2, correlation), ...] high correlation pairs

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "permutation_importance": self.permutation_importance,
            "baseline_ic": self.baseline_ic,
            "top_features": [{"feature": f, "importance": float(imp)} for f, imp in self.top_features],
            "redundant_pairs": [
                {"feature1": f1, "feature2": f2, "correlation": float(corr)}
                for f1, f2, corr in self.redundant_pairs
            ],
            "feature_correlations": self.feature_correlations.to_dict(),
        }


class FeatureImportanceAnalyzer:
    """Analyze feature importance for factor-based models."""

    def __init__(self, history_path: Path, prices_path: Optional[Path] = None):
        """
        Initialize analyzer.

        Args:
            history_path: Path to finviz_fundamentals_history.parquet
            prices_path: Optional path to prices.parquet
        """
        self.history_path = history_path

        if not history_path.exists():
            raise FileNotFoundError(f"History file not found: {history_path}")

        # Infer prices path from history path if not provided
        if prices_path is None:
            self.prices_path = history_path.parent / "prices.parquet"
        else:
            self.prices_path = prices_path

        LOGGER.info(f"Loading history from {history_path}")
        self.history = pd.read_parquet(history_path)
        self.history["as_of_date"] = pd.to_datetime(self.history["as_of_date"]).dt.date

        # Ensure factor columns exist
        for col in FACTOR_COLS:
            if col not in self.history.columns:
                LOGGER.warning(f"Factor column {col} not found in history, filling with NaN")
                self.history[col] = np.nan

        LOGGER.info(f"Loaded {len(self.history)} rows, {len(self.history['as_of_date'].unique())} unique dates")

    def analyze(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        n_permutations: int = 10,
        correlation_threshold: float = 0.8,
    ) -> FeatureImportanceResults:
        """
        Run comprehensive feature importance analysis.

        Args:
            start_date: Optional start date (YYYY-MM-DD) for analysis period
            end_date: Optional end date (YYYY-MM-DD) for analysis period
            n_permutations: Number of permutations for importance calculation
            correlation_threshold: Threshold for identifying redundant feature pairs

        Returns:
            FeatureImportanceResults with importance metrics
        """
        # Filter data to analysis period
        data = self._filter_data(start_date, end_date)

        if len(data) < 100:
            raise ValueError(f"Insufficient data for analysis: {len(data)} rows")

        LOGGER.info(f"Analyzing {len(data)} rows from {len(data['as_of_date'].unique())} dates")

        # Compute baseline IC with all features
        baseline_ic = self._compute_baseline_ic(data)
        LOGGER.info(f"Baseline mean IC: {baseline_ic:.4f}")

        # Compute permutation importance
        perm_importance = self.compute_permutation_importance(
            data,
            n_permutations=n_permutations,
            baseline_ic=baseline_ic,
        )

        # Analyze feature correlations
        feature_corr = self.analyze_feature_correlations(data)

        # Identify top features
        top_features = sorted(perm_importance.items(), key=lambda x: x[1], reverse=True)

        # Identify redundant feature pairs
        redundant_pairs = self._find_redundant_pairs(feature_corr, correlation_threshold)

        LOGGER.info(f"\nTop 3 features by importance:")
        for feat, imp in top_features[:3]:
            LOGGER.info(f"  {feat}: {imp:.4f}")

        if redundant_pairs:
            LOGGER.info(f"\nFound {len(redundant_pairs)} highly correlated feature pairs")

        return FeatureImportanceResults(
            permutation_importance=perm_importance,
            baseline_ic=baseline_ic,
            feature_correlations=feature_corr,
            top_features=top_features,
            redundant_pairs=redundant_pairs,
        )

    def _filter_data(self, start_date: Optional[str], end_date: Optional[str]) -> pd.DataFrame:
        """Filter data to specified date range."""
        data = self.history.copy()

        if start_date:
            start = pd.to_datetime(start_date).date()
            data = data[data["as_of_date"] >= start]

        if end_date:
            end = pd.to_datetime(end_date).date()
            data = data[data["as_of_date"] <= end]

        return data

    def _compute_baseline_ic(self, data: pd.DataFrame) -> float:
        """Compute baseline IC with all features."""
        if not self.prices_path.exists():
            LOGGER.warning(f"Prices file not found: {self.prices_path}")
            return np.nan

        prices = pd.read_parquet(self.prices_path)
        prices["ticker"] = prices["ticker"].astype(str).str.upper()
        prices["date"] = pd.to_datetime(prices["date"], errors="coerce").dt.date

        # Ensure data has correct date type
        data = data.copy()
        if pd.api.types.is_datetime64_any_dtype(data["as_of_date"]):
            data["as_of_date"] = pd.to_datetime(data["as_of_date"]).dt.date

        # Compute forward returns
        joined = _compute_forward_returns_from_prices(
            data,
            prices,
            horizon_trading_days=21,
        )
        joined["forward_return"] = _winsorize(joined["forward_return"])

        # Compute mean IC across all factors
        ics = []
        for col in FACTOR_COLS:
            if col in joined.columns:
                ic = _spearman_ic(joined[col], joined["forward_return"])
                if not np.isnan(ic):
                    ics.append(ic)

        return float(np.mean(ics)) if ics else np.nan

    def compute_permutation_importance(
        self,
        data: pd.DataFrame,
        n_permutations: int = 10,
        baseline_ic: Optional[float] = None,
    ) -> Dict[str, float]:
        """
        Compute permutation importance for each feature.

        Permutation importance measures how much model performance degrades
        when a feature's values are randomly shuffled.

        Args:
            data: DataFrame with features and forward returns
            n_permutations: Number of permutation runs per feature
            baseline_ic: Optional baseline IC (computed if not provided)

        Returns:
            Dict mapping feature name to importance score (baseline_ic - permuted_ic)
        """
        LOGGER.info(f"Computing permutation importance with {n_permutations} permutations per feature")

        if baseline_ic is None:
            baseline_ic = self._compute_baseline_ic(data)

        if not self.prices_path.exists():
            LOGGER.warning("Prices file not found, cannot compute permutation importance")
            return {col: np.nan for col in FACTOR_COLS}

        prices = pd.read_parquet(self.prices_path)
        prices["ticker"] = prices["ticker"].astype(str).str.upper()
        prices["date"] = pd.to_datetime(prices["date"], errors="coerce").dt.date

        # Ensure data has correct date type
        data = data.copy()
        if pd.api.types.is_datetime64_any_dtype(data["as_of_date"]):
            data["as_of_date"] = pd.to_datetime(data["as_of_date"]).dt.date

        # Compute forward returns
        joined = _compute_forward_returns_from_prices(
            data,
            prices,
            horizon_trading_days=21,
        )
        joined["forward_return"] = _winsorize(joined["forward_return"])

        importance = {}

        for feature in FACTOR_COLS:
            if feature not in joined.columns:
                importance[feature] = np.nan
                continue

            # Run multiple permutations
            permuted_ics = []

            for _ in range(n_permutations):
                # Create permuted version
                permuted_data = joined.copy()
                permuted_data[feature] = np.random.permutation(permuted_data[feature].values)

                # Compute IC with permuted feature
                permuted_ic = _spearman_ic(permuted_data[feature], permuted_data["forward_return"])

                if not np.isnan(permuted_ic):
                    permuted_ics.append(permuted_ic)

            # Importance = baseline IC - mean permuted IC
            if permuted_ics:
                mean_permuted_ic = np.mean(permuted_ics)
                importance[feature] = baseline_ic - mean_permuted_ic
            else:
                importance[feature] = np.nan

            LOGGER.debug(f"  {feature}: {importance[feature]:.4f}")

        return importance

    def analyze_feature_correlations(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Compute correlation matrix of features.

        Args:
            data: DataFrame with features

        Returns:
            Correlation matrix (DataFrame)
        """
        LOGGER.info("Analyzing feature correlations")

        # Select only factor columns that exist
        available_factors = [col for col in FACTOR_COLS if col in data.columns]

        if not available_factors:
            return pd.DataFrame()

        # Compute correlation matrix
        corr_matrix = data[available_factors].corr(method="spearman")

        return corr_matrix

    def _find_redundant_pairs(
        self,
        corr_matrix: pd.DataFrame,
        threshold: float = 0.8,
    ) -> List[Tuple[str, str, float]]:
        """
        Find pairs of features with high correlation.

        Args:
            corr_matrix: Correlation matrix
            threshold: Correlation threshold for redundancy

        Returns:
            List of (feature1, feature2, correlation) tuples
        """
        if corr_matrix.empty:
            return []

        redundant = []

        # Iterate over upper triangle of correlation matrix
        for i in range(len(corr_matrix.columns)):
            for j in range(i + 1, len(corr_matrix.columns)):
                feat1 = corr_matrix.columns[i]
                feat2 = corr_matrix.columns[j]
                corr = corr_matrix.iloc[i, j]

                if abs(corr) >= threshold:
                    redundant.append((feat1, feat2, float(corr)))

        # Sort by absolute correlation (descending)
        redundant.sort(key=lambda x: abs(x[2]), reverse=True)

        return redundant

    def compute_partial_dependence(
        self,
        data: pd.DataFrame,
        feature: str,
        n_points: int = 20,
    ) -> List[Tuple[float, float]]:
        """
        Compute partial dependence of forward returns on a feature.

        Partial dependence shows the marginal effect of a feature on the target
        (forward returns), averaging over all other features.

        Args:
            data: DataFrame with features and forward returns
            feature: Feature name to analyze
            n_points: Number of points to sample along feature range

        Returns:
            List of (feature_value, mean_forward_return) tuples
        """
        LOGGER.info(f"Computing partial dependence for {feature}")

        if feature not in data.columns:
            LOGGER.warning(f"Feature {feature} not found in data")
            return []

        if not self.prices_path.exists():
            LOGGER.warning("Prices file not found, cannot compute partial dependence")
            return []

        prices = pd.read_parquet(self.prices_path)
        prices["ticker"] = prices["ticker"].astype(str).str.upper()
        prices["date"] = pd.to_datetime(prices["date"], errors="coerce").dt.date

        # Ensure data has correct date type
        data = data.copy()
        if pd.api.types.is_datetime64_any_dtype(data["as_of_date"]):
            data["as_of_date"] = pd.to_datetime(data["as_of_date"]).dt.date

        # Compute forward returns
        joined = _compute_forward_returns_from_prices(
            data,
            prices,
            horizon_trading_days=21,
        )
        joined = joined.dropna(subset=["forward_return", feature])

        if len(joined) < 50:
            LOGGER.warning(f"Insufficient data for partial dependence: {len(joined)} rows")
            return []

        # Get feature value range
        feature_min = joined[feature].quantile(0.05)
        feature_max = joined[feature].quantile(0.95)
        feature_values = np.linspace(feature_min, feature_max, n_points)

        # Compute mean forward return for each feature value
        pdp_values = []

        for val in feature_values:
            # Find rows with similar feature values (within 10% tolerance)
            tolerance = (feature_max - feature_min) * 0.1
            mask = (joined[feature] >= val - tolerance) & (joined[feature] <= val + tolerance)

            if mask.sum() >= 10:  # Need at least 10 samples
                mean_return = joined.loc[mask, "forward_return"].mean()
                pdp_values.append((float(val), float(mean_return)))

        return pdp_values


def analyze_feature_importance(
    history_path: str | Path,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    n_permutations: int = 10,
    out_path: Optional[str | Path] = None,
) -> FeatureImportanceResults:
    """
    Convenience function to run feature importance analysis.

    Args:
        history_path: Path to finviz_fundamentals_history.parquet
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)
        n_permutations: Number of permutations for importance calculation
        out_path: Optional path to save results JSON

    Returns:
        FeatureImportanceResults
    """
    analyzer = FeatureImportanceAnalyzer(Path(history_path))
    results = analyzer.analyze(
        start_date=start_date,
        end_date=end_date,
        n_permutations=n_permutations,
    )

    if out_path:
        import json

        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with open(out_path, "w") as f:
            json.dump(results.to_dict(), f, indent=2)

        LOGGER.info(f"Feature importance results saved to {out_path}")

    return results
