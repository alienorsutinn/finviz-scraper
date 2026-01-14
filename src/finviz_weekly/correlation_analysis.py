"""
Advanced Correlation Analysis for factor research.

Provides:
- Rolling factor correlations over time
- Correlation heatmaps and matrices
- Factor clustering to identify redundant factors
- Principal Component Analysis (PCA) for dimensionality reduction
- Factor decay analysis
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from scipy import stats
    from scipy.cluster import hierarchy
    from scipy.spatial.distance import squareform
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    from sklearn.decomposition import PCA
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import AgglomerativeClustering
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

from .learn import FACTOR_COLS

LOGGER = logging.getLogger(__name__)


@dataclass
class CorrelationConfig:
    """Configuration for correlation analysis."""

    # Rolling window settings
    rolling_window_days: int = 90  # Window size for rolling correlations
    min_observations: int = 30  # Minimum observations per window

    # Clustering settings
    n_clusters: int = 3  # Number of factor clusters
    cluster_method: str = "ward"  # Linkage method for hierarchical clustering

    # PCA settings
    n_components: int = 3  # Number of principal components to retain
    variance_threshold: float = 0.95  # Retain components explaining this much variance

    # Decay analysis
    decay_horizons: List[int] = field(default_factory=lambda: [5, 10, 21, 63])  # Trading days


@dataclass
class CorrelationResult:
    """Result of correlation analysis."""

    analysis_date: str
    factors_analyzed: List[str]

    # Static correlation matrix
    correlation_matrix: pd.DataFrame

    # Rolling correlations summary
    rolling_correlation_means: pd.DataFrame
    rolling_correlation_stds: pd.DataFrame

    # Clustering results
    factor_clusters: Dict[str, int]  # factor -> cluster_id
    cluster_centroids: Dict[int, List[str]]  # cluster_id -> factor names

    # PCA results
    pca_explained_variance: List[float]
    pca_components: pd.DataFrame  # loadings matrix
    pca_cumulative_variance: float

    # Highly correlated pairs
    high_correlation_pairs: List[Tuple[str, str, float]]

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "analysis_date": self.analysis_date,
            "factors_analyzed": self.factors_analyzed,
            "correlation_matrix": self.correlation_matrix.to_dict(),
            "rolling_correlation_means": self.rolling_correlation_means.to_dict(),
            "rolling_correlation_stds": self.rolling_correlation_stds.to_dict(),
            "factor_clusters": self.factor_clusters,
            "cluster_centroids": self.cluster_centroids,
            "pca_explained_variance": self.pca_explained_variance,
            "pca_components": self.pca_components.to_dict() if self.pca_components is not None else None,
            "pca_cumulative_variance": self.pca_cumulative_variance,
            "high_correlation_pairs": [
                {"factor1": f1, "factor2": f2, "correlation": corr}
                for f1, f2, corr in self.high_correlation_pairs
            ],
        }


@dataclass
class DecayResult:
    """Result of factor decay analysis."""

    factor_name: str
    horizons: List[int]
    autocorrelations: List[float]  # Autocorrelation at each horizon
    half_life_days: Optional[float]  # Days until correlation drops to 0.5
    decay_rate: float  # Exponential decay rate


class CorrelationAnalyzer:
    """
    Analyze factor correlations and relationships.

    Provides tools for:
    - Understanding factor redundancy
    - Identifying diversification opportunities
    - Reducing dimensionality
    - Analyzing signal persistence
    """

    def __init__(self, config: Optional[CorrelationConfig] = None):
        """
        Initialize correlation analyzer.

        Args:
            config: Analysis configuration
        """
        self.config = config or CorrelationConfig()

        if not SCIPY_AVAILABLE:
            LOGGER.warning("scipy not available. Some features will be limited.")
        if not SKLEARN_AVAILABLE:
            LOGGER.warning("sklearn not available. PCA and clustering disabled.")

        LOGGER.info("Correlation analyzer initialized")

    def analyze(
        self,
        data: pd.DataFrame,
        factors: Optional[List[str]] = None,
        correlation_threshold: float = 0.7,
    ) -> CorrelationResult:
        """
        Run comprehensive correlation analysis.

        Args:
            data: DataFrame with factor scores
            factors: List of factor columns to analyze (default: FACTOR_COLS)
            correlation_threshold: Threshold for flagging high correlations

        Returns:
            CorrelationResult with analysis results
        """
        factors = factors or [f for f in FACTOR_COLS if f in data.columns]

        if len(factors) < 2:
            raise ValueError(f"Need at least 2 factors for correlation analysis, got {len(factors)}")

        LOGGER.info(f"Analyzing correlations for {len(factors)} factors")

        # Extract factor data
        factor_data = data[factors].dropna()

        if len(factor_data) < self.config.min_observations:
            raise ValueError(f"Insufficient data: {len(factor_data)} < {self.config.min_observations}")

        # Static correlation matrix
        corr_matrix = factor_data.corr(method="spearman")

        # Rolling correlations
        rolling_means, rolling_stds = self._compute_rolling_correlations(factor_data, factors)

        # Factor clustering
        clusters, centroids = self._cluster_factors(corr_matrix, factors)

        # PCA analysis
        pca_variance, pca_components, cumulative_var = self._run_pca(factor_data, factors)

        # Find high correlation pairs
        high_corr_pairs = self._find_high_correlations(corr_matrix, factors, correlation_threshold)

        result = CorrelationResult(
            analysis_date=datetime.now().isoformat(),
            factors_analyzed=factors,
            correlation_matrix=corr_matrix,
            rolling_correlation_means=rolling_means,
            rolling_correlation_stds=rolling_stds,
            factor_clusters=clusters,
            cluster_centroids=centroids,
            pca_explained_variance=pca_variance,
            pca_components=pca_components,
            pca_cumulative_variance=cumulative_var,
            high_correlation_pairs=high_corr_pairs,
        )

        LOGGER.info(f"Analysis complete. Found {len(high_corr_pairs)} highly correlated pairs")

        return result

    def _compute_rolling_correlations(
        self,
        data: pd.DataFrame,
        factors: List[str],
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Compute rolling correlations between all factor pairs."""
        window = self.config.rolling_window_days

        # Initialize storage
        all_rolling_corrs = {}

        for i, f1 in enumerate(factors):
            for f2 in factors[i + 1:]:
                pair_name = f"{f1}_vs_{f2}"

                # Compute rolling correlation
                rolling_corr = data[f1].rolling(window=window, min_periods=self.config.min_observations).corr(data[f2])
                all_rolling_corrs[pair_name] = rolling_corr

        rolling_df = pd.DataFrame(all_rolling_corrs)

        # Compute summary statistics
        rolling_means = rolling_df.mean().to_frame("mean_correlation")
        rolling_stds = rolling_df.std().to_frame("std_correlation")

        return rolling_means, rolling_stds

    def _cluster_factors(
        self,
        corr_matrix: pd.DataFrame,
        factors: List[str],
    ) -> Tuple[Dict[str, int], Dict[int, List[str]]]:
        """Cluster factors based on correlation similarity."""
        if not SKLEARN_AVAILABLE or len(factors) < 3:
            # Return single cluster if sklearn not available or too few factors
            clusters = {f: 0 for f in factors}
            centroids = {0: factors}
            return clusters, centroids

        # Convert correlation to distance
        distance_matrix = 1 - np.abs(corr_matrix.values)
        np.fill_diagonal(distance_matrix, 0)

        # Hierarchical clustering
        n_clusters = min(self.config.n_clusters, len(factors))
        clustering = AgglomerativeClustering(
            n_clusters=n_clusters,
            metric="precomputed",
            linkage="average",
        )

        labels = clustering.fit_predict(distance_matrix)

        # Build results
        clusters = {factor: int(label) for factor, label in zip(factors, labels)}
        centroids = {}
        for cluster_id in range(n_clusters):
            centroids[cluster_id] = [f for f, c in clusters.items() if c == cluster_id]

        return clusters, centroids

    def _run_pca(
        self,
        data: pd.DataFrame,
        factors: List[str],
    ) -> Tuple[List[float], pd.DataFrame, float]:
        """Run PCA on factor data."""
        if not SKLEARN_AVAILABLE:
            return [], pd.DataFrame(), 0.0

        # Standardize data
        scaler = StandardScaler()
        scaled_data = scaler.fit_transform(data)

        # Run PCA
        n_components = min(self.config.n_components, len(factors))
        pca = PCA(n_components=n_components)
        pca.fit(scaled_data)

        # Get explained variance
        explained_variance = list(pca.explained_variance_ratio_)
        cumulative_variance = float(np.sum(explained_variance))

        # Get component loadings
        loadings = pd.DataFrame(
            pca.components_.T,
            index=factors,
            columns=[f"PC{i + 1}" for i in range(n_components)],
        )

        return explained_variance, loadings, cumulative_variance

    def _find_high_correlations(
        self,
        corr_matrix: pd.DataFrame,
        factors: List[str],
        threshold: float,
    ) -> List[Tuple[str, str, float]]:
        """Find factor pairs with correlation above threshold."""
        high_corr_pairs = []

        for i, f1 in enumerate(factors):
            for f2 in factors[i + 1:]:
                corr = corr_matrix.loc[f1, f2]
                if abs(corr) >= threshold:
                    high_corr_pairs.append((f1, f2, float(corr)))

        # Sort by absolute correlation
        high_corr_pairs.sort(key=lambda x: abs(x[2]), reverse=True)

        return high_corr_pairs

    def analyze_decay(
        self,
        data: pd.DataFrame,
        factor: str,
        horizons: Optional[List[int]] = None,
    ) -> DecayResult:
        """
        Analyze factor signal decay over time.

        Measures how quickly a factor signal loses predictive power.

        Args:
            data: DataFrame with factor scores and dates
            factor: Factor column to analyze
            horizons: List of horizons (trading days) to measure

        Returns:
            DecayResult with decay analysis
        """
        horizons = horizons or self.config.decay_horizons

        if factor not in data.columns:
            raise ValueError(f"Factor {factor} not found in data")

        factor_series = data[factor].dropna()

        # Compute autocorrelations at each horizon
        autocorrs = []
        for horizon in horizons:
            if len(factor_series) > horizon:
                shifted = factor_series.shift(horizon)
                valid_mask = ~(factor_series.isna() | shifted.isna())
                if valid_mask.sum() >= self.config.min_observations:
                    corr = factor_series[valid_mask].corr(shifted[valid_mask])
                    autocorrs.append(float(corr) if not np.isnan(corr) else 0.0)
                else:
                    autocorrs.append(0.0)
            else:
                autocorrs.append(0.0)

        # Estimate half-life (when correlation drops to 0.5)
        half_life = None
        for i, (h, ac) in enumerate(zip(horizons, autocorrs)):
            if ac < 0.5:
                if i > 0:
                    # Linear interpolation
                    prev_h, prev_ac = horizons[i - 1], autocorrs[i - 1]
                    if prev_ac > 0.5:
                        half_life = prev_h + (h - prev_h) * (prev_ac - 0.5) / (prev_ac - ac)
                break

        # Estimate exponential decay rate
        if len(autocorrs) >= 2 and autocorrs[0] > 0:
            try:
                # Fit exponential decay: corr(t) = exp(-rate * t)
                valid_autocorrs = [(h, ac) for h, ac in zip(horizons, autocorrs) if ac > 0]
                if len(valid_autocorrs) >= 2:
                    log_autocorrs = [np.log(ac) for _, ac in valid_autocorrs]
                    hs = [h for h, _ in valid_autocorrs]
                    # Simple linear regression on log scale
                    decay_rate = -np.polyfit(hs, log_autocorrs, 1)[0]
                else:
                    decay_rate = 0.0
            except Exception:
                decay_rate = 0.0
        else:
            decay_rate = 0.0

        return DecayResult(
            factor_name=factor,
            horizons=horizons,
            autocorrelations=autocorrs,
            half_life_days=half_life,
            decay_rate=float(decay_rate),
        )

    def get_redundant_factors(
        self,
        result: CorrelationResult,
        threshold: float = 0.8,
    ) -> List[str]:
        """
        Identify redundant factors that can be removed.

        Returns factors that have high correlation with other factors
        and contribute less unique information.

        Args:
            result: CorrelationResult from analyze()
            threshold: Correlation threshold for redundancy

        Returns:
            List of factor names that could be removed
        """
        redundant = set()

        for f1, f2, corr in result.high_correlation_pairs:
            if abs(corr) >= threshold:
                # Mark the factor with lower average absolute correlation as redundant
                f1_avg = result.correlation_matrix[f1].abs().mean()
                f2_avg = result.correlation_matrix[f2].abs().mean()

                if f1_avg < f2_avg:
                    redundant.add(f1)
                else:
                    redundant.add(f2)

        return list(redundant)

    def suggest_factor_combinations(
        self,
        result: CorrelationResult,
        min_factors_per_cluster: int = 1,
    ) -> Dict[str, List[str]]:
        """
        Suggest optimal factor combinations for diversification.

        Returns representative factors from each cluster.

        Args:
            result: CorrelationResult from analyze()
            min_factors_per_cluster: Minimum factors to select from each cluster

        Returns:
            Dict mapping cluster names to suggested factors
        """
        suggestions = {}

        for cluster_id, factors in result.cluster_centroids.items():
            if len(factors) == 0:
                continue

            # Select factors with highest average information content
            # (measured by average absolute correlation - lower is better for diversity)
            factor_scores = []
            for f in factors:
                avg_corr = result.correlation_matrix[f].abs().mean()
                factor_scores.append((f, avg_corr))

            # Sort by lowest correlation (most unique)
            factor_scores.sort(key=lambda x: x[1])

            # Select top factors
            n_select = max(min_factors_per_cluster, len(factors) // 2)
            selected = [f for f, _ in factor_scores[:n_select]]

            suggestions[f"cluster_{cluster_id}"] = selected

        return suggestions


def run_correlation_analysis(
    history_path: Path,
    output_path: Optional[Path] = None,
    config: Optional[CorrelationConfig] = None,
) -> CorrelationResult:
    """
    Run correlation analysis on historical data.

    Args:
        history_path: Path to fundamentals history
        output_path: Optional path to save results
        config: Analysis configuration

    Returns:
        CorrelationResult with analysis
    """
    import json

    LOGGER.info(f"Running correlation analysis on {history_path}")

    # Load data
    data = pd.read_parquet(history_path)

    # Run analysis
    analyzer = CorrelationAnalyzer(config)
    result = analyzer.analyze(data)

    # Save if output path provided
    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            json.dump(result.to_dict(), f, indent=2, default=str)

        LOGGER.info(f"Results saved to {output_path}")

    return result
