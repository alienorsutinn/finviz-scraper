"""
Advanced Factor Analytics Module

Provides advanced factor analysis capabilities:
- Principal Component Analysis (PCA) for factor dimensionality reduction
- Rolling correlation tracking with regime shift detection
- Factor decay analysis (signal persistence over time)
- Factor clustering and similarity analysis
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)


class CorrelationRegime(Enum):
    """Correlation regime classification."""
    STABLE = "stable"
    RISING = "rising"
    FALLING = "falling"
    BREAKDOWN = "breakdown"  # Correlations breaking historical patterns


@dataclass
class PCAResult:
    """Results from PCA analysis."""
    n_components: int
    explained_variance_ratio: List[float]
    cumulative_variance: List[float]
    loadings: pd.DataFrame  # Factor loadings matrix
    scores: pd.DataFrame  # Transformed data (principal components)
    feature_importance: Dict[str, float]  # Contribution of each feature
    recommended_components: int  # Number to explain 90% variance


@dataclass
class FactorDecayResult:
    """Results from factor decay analysis."""
    factor_name: str
    decay_half_life: float  # Days until signal loses half its predictive power
    ic_by_horizon: Dict[int, float]  # IC at different forward horizons
    decay_rate: float  # Exponential decay rate
    is_persistent: bool  # True if half-life > 21 days
    optimal_holding_period: int  # Days


@dataclass
class CorrelationShift:
    """Detected correlation regime shift."""
    date: str
    factor_pair: Tuple[str, str]
    old_correlation: float
    new_correlation: float
    shift_magnitude: float
    regime: CorrelationRegime


@dataclass
class RollingCorrelationResult:
    """Results from rolling correlation analysis."""
    correlation_matrix: pd.DataFrame  # Current correlations
    rolling_correlations: Dict[str, pd.Series]  # Time series of correlations
    regime_shifts: List[CorrelationShift]
    unstable_pairs: List[Tuple[str, str]]  # Pairs with high correlation volatility
    cluster_assignments: Dict[str, int]  # Factor -> cluster mapping


class PCAAnalyzer:
    """Principal Component Analysis for factor reduction."""

    def __init__(self, n_components: Optional[int] = None, variance_threshold: float = 0.90):
        """
        Initialize PCA analyzer.

        Args:
            n_components: Number of components (None = auto-select)
            variance_threshold: Minimum cumulative variance to explain
        """
        self.n_components = n_components
        self.variance_threshold = variance_threshold
        self._pca = None
        self._scaler = None

    def fit_transform(self, data: pd.DataFrame,
                     factor_columns: Optional[List[str]] = None) -> PCAResult:
        """
        Perform PCA on factor data.

        Args:
            data: DataFrame with factor scores
            factor_columns: Columns to include (None = all numeric)
        """
        from sklearn.preprocessing import StandardScaler
        from sklearn.decomposition import PCA

        # Select factor columns
        if factor_columns:
            X = data[factor_columns].copy()
        else:
            X = data.select_dtypes(include=[np.number]).copy()

        # Drop rows with NaN
        X = X.dropna()
        feature_names = list(X.columns)

        if len(X) < 10:
            raise ValueError("Insufficient data for PCA (need at least 10 rows)")

        # Standardize
        self._scaler = StandardScaler()
        X_scaled = self._scaler.fit_transform(X)

        # Determine number of components
        n_components = self.n_components or min(len(feature_names), len(X) - 1)

        # Fit PCA
        self._pca = PCA(n_components=n_components)
        scores = self._pca.fit_transform(X_scaled)

        # Calculate cumulative variance
        explained_var = self._pca.explained_variance_ratio_.tolist()
        cumulative_var = np.cumsum(explained_var).tolist()

        # Find recommended number of components
        recommended = 1
        for i, cum_var in enumerate(cumulative_var):
            if cum_var >= self.variance_threshold:
                recommended = i + 1
                break
        else:
            recommended = len(cumulative_var)

        # Create loadings DataFrame
        loadings_df = pd.DataFrame(
            self._pca.components_.T,
            index=feature_names,
            columns=[f"PC{i+1}" for i in range(n_components)]
        )

        # Create scores DataFrame
        scores_df = pd.DataFrame(
            scores,
            index=X.index,
            columns=[f"PC{i+1}" for i in range(n_components)]
        )

        # Calculate feature importance (sum of squared loadings)
        feature_importance = {}
        for feature in feature_names:
            importance = np.sum(loadings_df.loc[feature] ** 2 * explained_var)
            feature_importance[feature] = float(importance)

        return PCAResult(
            n_components=n_components,
            explained_variance_ratio=explained_var,
            cumulative_variance=cumulative_var,
            loadings=loadings_df,
            scores=scores_df,
            feature_importance=feature_importance,
            recommended_components=recommended
        )

    def get_redundant_factors(self, result: PCAResult,
                             threshold: float = 0.05) -> List[str]:
        """Identify factors that contribute little to variance."""
        redundant = []
        for factor, importance in result.feature_importance.items():
            if importance < threshold:
                redundant.append(factor)
        return redundant


class RollingCorrelationTracker:
    """Track factor correlations over time with regime detection."""

    def __init__(self, window: int = 63, min_periods: int = 21):
        """
        Initialize tracker.

        Args:
            window: Rolling window size (default 63 = 3 months)
            min_periods: Minimum observations required
        """
        self.window = window
        self.min_periods = min_periods

    def calculate_rolling_correlations(self, data: pd.DataFrame,
                                       factors: List[str]) -> RollingCorrelationResult:
        """Calculate rolling correlations between factors."""
        # Current correlation matrix
        current_corr = data[factors].corr()

        # Rolling correlations for each pair
        rolling_corrs = {}
        for i, f1 in enumerate(factors):
            for f2 in factors[i+1:]:
                pair_name = f"{f1}_vs_{f2}"
                rolling_corrs[pair_name] = data[f1].rolling(
                    window=self.window,
                    min_periods=self.min_periods
                ).corr(data[f2])

        # Detect regime shifts
        regime_shifts = self._detect_regime_shifts(rolling_corrs, factors)

        # Find unstable pairs (high correlation volatility)
        unstable_pairs = self._find_unstable_pairs(rolling_corrs, factors)

        # Cluster factors
        clusters = self._cluster_factors(current_corr, factors)

        return RollingCorrelationResult(
            correlation_matrix=current_corr,
            rolling_correlations=rolling_corrs,
            regime_shifts=regime_shifts,
            unstable_pairs=unstable_pairs,
            cluster_assignments=clusters
        )

    def _detect_regime_shifts(self, rolling_corrs: Dict[str, pd.Series],
                             factors: List[str],
                             shift_threshold: float = 0.3) -> List[CorrelationShift]:
        """Detect significant correlation regime shifts."""
        shifts = []

        for pair_name, corr_series in rolling_corrs.items():
            if corr_series.isna().all():
                continue

            # Calculate rolling mean and std
            corr_mean = corr_series.rolling(21).mean()
            corr_std = corr_series.rolling(21).std()

            # Detect shifts (correlation moves > 2 std)
            for i in range(22, len(corr_series)):
                if pd.isna(corr_series.iloc[i]) or pd.isna(corr_mean.iloc[i-1]):
                    continue

                diff = abs(corr_series.iloc[i] - corr_mean.iloc[i-1])
                if corr_std.iloc[i-1] > 0 and diff > 2 * corr_std.iloc[i-1]:
                    # Parse factor names from pair
                    parts = pair_name.split("_vs_")
                    if len(parts) == 2:
                        f1, f2 = parts

                        # Determine regime
                        if corr_series.iloc[i] > corr_mean.iloc[i-1]:
                            regime = CorrelationRegime.RISING
                        else:
                            regime = CorrelationRegime.FALLING

                        if diff > shift_threshold:
                            regime = CorrelationRegime.BREAKDOWN

                        shifts.append(CorrelationShift(
                            date=str(corr_series.index[i]),
                            factor_pair=(f1, f2),
                            old_correlation=float(corr_mean.iloc[i-1]),
                            new_correlation=float(corr_series.iloc[i]),
                            shift_magnitude=float(diff),
                            regime=regime
                        ))

        return shifts

    def _find_unstable_pairs(self, rolling_corrs: Dict[str, pd.Series],
                            factors: List[str],
                            volatility_threshold: float = 0.2) -> List[Tuple[str, str]]:
        """Find factor pairs with high correlation volatility."""
        unstable = []

        for pair_name, corr_series in rolling_corrs.items():
            if corr_series.isna().all():
                continue

            volatility = corr_series.std()
            if volatility > volatility_threshold:
                parts = pair_name.split("_vs_")
                if len(parts) == 2:
                    unstable.append((parts[0], parts[1]))

        return unstable

    def _cluster_factors(self, corr_matrix: pd.DataFrame,
                        factors: List[str],
                        n_clusters: int = 3) -> Dict[str, int]:
        """Cluster factors based on correlation similarity."""
        try:
            from sklearn.cluster import AgglomerativeClustering

            # Convert correlation to distance
            distance = 1 - corr_matrix.abs()

            # Cluster
            clustering = AgglomerativeClustering(
                n_clusters=min(n_clusters, len(factors)),
                metric='precomputed',
                linkage='average'
            )
            labels = clustering.fit_predict(distance)

            return {factors[i]: int(labels[i]) for i in range(len(factors))}

        except Exception as e:
            logger.warning(f"Clustering failed: {e}")
            return {f: 0 for f in factors}


class FactorDecayAnalyzer:
    """Analyze factor signal persistence and decay."""

    def __init__(self, horizons: List[int] = None):
        """
        Initialize analyzer.

        Args:
            horizons: Forward return horizons to test (default: 5, 10, 21, 42, 63)
        """
        self.horizons = horizons or [5, 10, 21, 42, 63]

    def analyze_decay(self, factor_scores: pd.Series,
                     forward_returns: pd.DataFrame,
                     factor_name: str) -> FactorDecayResult:
        """
        Analyze how quickly a factor signal decays.

        Args:
            factor_scores: Factor scores indexed by date
            forward_returns: DataFrame with forward returns at different horizons
            factor_name: Name of the factor
        """
        # Calculate IC at each horizon
        ic_by_horizon = {}
        for horizon in self.horizons:
            col_name = f"fwd_ret_{horizon}d"
            if col_name in forward_returns.columns:
                # Align data
                aligned = pd.concat([factor_scores, forward_returns[col_name]], axis=1).dropna()
                if len(aligned) > 30:
                    ic = aligned.iloc[:, 0].corr(aligned.iloc[:, 1], method='spearman')
                    ic_by_horizon[horizon] = float(ic)

        if not ic_by_horizon:
            return FactorDecayResult(
                factor_name=factor_name,
                decay_half_life=0,
                ic_by_horizon={},
                decay_rate=0,
                is_persistent=False,
                optimal_holding_period=21
            )

        # Fit exponential decay
        horizons = np.array(list(ic_by_horizon.keys()))
        ics = np.array(list(ic_by_horizon.values()))

        # Handle negative ICs (some factors are negatively predictive)
        ic_magnitude = np.abs(ics)
        initial_ic = ic_magnitude[0] if len(ic_magnitude) > 0 else 0.05

        # Calculate decay rate using log-linear regression
        if initial_ic > 0 and len(ic_magnitude) > 1:
            try:
                # Avoid log(0)
                ic_safe = np.maximum(ic_magnitude, 0.001)
                log_ic = np.log(ic_safe / initial_ic)
                slope, _, _, _, _ = stats.linregress(horizons, log_ic)
                decay_rate = -slope

                # Half-life = ln(2) / decay_rate
                if decay_rate > 0:
                    half_life = np.log(2) / decay_rate
                else:
                    half_life = 252  # Very persistent (1 year)
            except Exception:
                decay_rate = 0.02
                half_life = 35
        else:
            decay_rate = 0.02
            half_life = 35

        # Determine optimal holding period (where IC is still significant)
        optimal_period = 21
        for horizon in sorted(ic_by_horizon.keys()):
            if abs(ic_by_horizon[horizon]) > 0.02:  # Still meaningful IC
                optimal_period = horizon

        return FactorDecayResult(
            factor_name=factor_name,
            decay_half_life=float(half_life),
            ic_by_horizon=ic_by_horizon,
            decay_rate=float(decay_rate),
            is_persistent=half_life > 21,
            optimal_holding_period=optimal_period
        )

    def rank_factors_by_persistence(self,
                                   factor_data: pd.DataFrame,
                                   forward_returns: pd.DataFrame,
                                   factors: List[str]) -> pd.DataFrame:
        """Rank factors by signal persistence."""
        results = []

        for factor in factors:
            if factor in factor_data.columns:
                decay = self.analyze_decay(
                    factor_data[factor],
                    forward_returns,
                    factor
                )
                results.append({
                    'factor': factor,
                    'half_life': decay.decay_half_life,
                    'decay_rate': decay.decay_rate,
                    'is_persistent': decay.is_persistent,
                    'optimal_period': decay.optimal_holding_period,
                    'ic_5d': decay.ic_by_horizon.get(5, 0),
                    'ic_21d': decay.ic_by_horizon.get(21, 0),
                    'ic_63d': decay.ic_by_horizon.get(63, 0),
                })

        df = pd.DataFrame(results)
        if not df.empty:
            df = df.sort_values('half_life', ascending=False)

        return df


def run_factor_analytics(data: pd.DataFrame,
                        factors: List[str],
                        forward_returns: Optional[pd.DataFrame] = None) -> Dict:
    """
    Run comprehensive factor analytics.

    Returns dict with PCA, correlation, and decay analysis results.
    """
    results = {}

    # PCA Analysis
    try:
        pca_analyzer = PCAAnalyzer()
        pca_result = pca_analyzer.fit_transform(data, factors)
        results['pca'] = {
            'explained_variance': pca_result.explained_variance_ratio,
            'cumulative_variance': pca_result.cumulative_variance,
            'recommended_components': pca_result.recommended_components,
            'feature_importance': pca_result.feature_importance,
            'redundant_factors': pca_analyzer.get_redundant_factors(pca_result),
        }
    except Exception as e:
        logger.error(f"PCA analysis failed: {e}")
        results['pca'] = None

    # Rolling Correlation Analysis
    try:
        corr_tracker = RollingCorrelationTracker()
        corr_result = corr_tracker.calculate_rolling_correlations(data, factors)
        results['correlation'] = {
            'current_matrix': corr_result.correlation_matrix.to_dict(),
            'regime_shifts': len(corr_result.regime_shifts),
            'unstable_pairs': corr_result.unstable_pairs,
            'clusters': corr_result.cluster_assignments,
        }
    except Exception as e:
        logger.error(f"Correlation analysis failed: {e}")
        results['correlation'] = None

    # Factor Decay Analysis
    if forward_returns is not None:
        try:
            decay_analyzer = FactorDecayAnalyzer()
            decay_df = decay_analyzer.rank_factors_by_persistence(
                data, forward_returns, factors
            )
            results['decay'] = decay_df.to_dict('records') if not decay_df.empty else []
        except Exception as e:
            logger.error(f"Decay analysis failed: {e}")
            results['decay'] = None

    return results
