"""
Visualization utilities for factor analysis and research.

Provides:
- Correlation heatmaps
- Factor distribution plots
- Time series charts
- Cluster visualizations
- Performance attribution charts
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

try:
    import seaborn as sns
    SEABORN_AVAILABLE = True
except ImportError:
    SEABORN_AVAILABLE = False

LOGGER = logging.getLogger(__name__)


@dataclass
class PlotConfig:
    """Configuration for plots."""

    figsize: Tuple[int, int] = (12, 8)
    dpi: int = 100
    style: str = "whitegrid"  # seaborn style
    palette: str = "RdYlGn"  # Color palette
    title_fontsize: int = 14
    label_fontsize: int = 12
    tick_fontsize: int = 10


class FactorVisualizer:
    """
    Create visualizations for factor analysis.

    Supports:
    - Correlation heatmaps
    - Distribution plots
    - Time series charts
    - Cluster dendrograms
    """

    def __init__(self, config: Optional[PlotConfig] = None):
        """
        Initialize visualizer.

        Args:
            config: Plot configuration
        """
        self.config = config or PlotConfig()

        if not MATPLOTLIB_AVAILABLE:
            LOGGER.warning("matplotlib not available. Visualization disabled.")

        if SEABORN_AVAILABLE:
            sns.set_style(self.config.style)

    def plot_correlation_heatmap(
        self,
        correlation_matrix: pd.DataFrame,
        title: str = "Factor Correlation Matrix",
        output_path: Optional[Path] = None,
        annotate: bool = True,
        mask_diagonal: bool = True,
    ) -> Optional[plt.Figure]:
        """
        Plot correlation heatmap.

        Args:
            correlation_matrix: Correlation matrix DataFrame
            title: Plot title
            output_path: Optional path to save figure
            annotate: Whether to show correlation values
            mask_diagonal: Whether to mask diagonal

        Returns:
            matplotlib Figure or None if matplotlib unavailable
        """
        if not MATPLOTLIB_AVAILABLE:
            LOGGER.warning("matplotlib not available")
            return None

        fig, ax = plt.subplots(figsize=self.config.figsize, dpi=self.config.dpi)

        # Create mask for diagonal if requested
        mask = None
        if mask_diagonal:
            mask = np.eye(len(correlation_matrix), dtype=bool)

        # Plot heatmap
        if SEABORN_AVAILABLE:
            sns.heatmap(
                correlation_matrix,
                annot=annotate,
                fmt=".2f",
                cmap=self.config.palette,
                center=0,
                square=True,
                mask=mask,
                ax=ax,
                vmin=-1,
                vmax=1,
                cbar_kws={"label": "Correlation"},
            )
        else:
            im = ax.imshow(correlation_matrix.values, cmap=self.config.palette, vmin=-1, vmax=1)
            plt.colorbar(im, ax=ax, label="Correlation")

            # Add labels
            ax.set_xticks(range(len(correlation_matrix.columns)))
            ax.set_yticks(range(len(correlation_matrix.index)))
            ax.set_xticklabels(correlation_matrix.columns, rotation=45, ha="right")
            ax.set_yticklabels(correlation_matrix.index)

            # Add annotations
            if annotate:
                for i in range(len(correlation_matrix)):
                    for j in range(len(correlation_matrix)):
                        if not (mask_diagonal and i == j):
                            ax.text(j, i, f"{correlation_matrix.iloc[i, j]:.2f}",
                                   ha="center", va="center", fontsize=8)

        ax.set_title(title, fontsize=self.config.title_fontsize)
        plt.tight_layout()

        if output_path:
            fig.savefig(output_path, dpi=self.config.dpi, bbox_inches="tight")
            LOGGER.info(f"Saved correlation heatmap to {output_path}")

        return fig

    def plot_factor_distributions(
        self,
        data: pd.DataFrame,
        factors: List[str],
        title: str = "Factor Distributions",
        output_path: Optional[Path] = None,
        ncols: int = 3,
    ) -> Optional[plt.Figure]:
        """
        Plot distribution histograms for multiple factors.

        Args:
            data: DataFrame with factor values
            factors: List of factor columns
            title: Plot title
            output_path: Optional path to save figure
            ncols: Number of columns in subplot grid

        Returns:
            matplotlib Figure or None
        """
        if not MATPLOTLIB_AVAILABLE:
            return None

        nrows = (len(factors) + ncols - 1) // ncols
        fig, axes = plt.subplots(nrows, ncols, figsize=(4 * ncols, 3 * nrows), dpi=self.config.dpi)
        axes = np.atleast_2d(axes).flatten()

        for i, factor in enumerate(factors):
            ax = axes[i]
            values = data[factor].dropna()

            if SEABORN_AVAILABLE:
                sns.histplot(values, ax=ax, kde=True, color="steelblue")
            else:
                ax.hist(values, bins=30, edgecolor="black", alpha=0.7)

            ax.set_title(factor, fontsize=self.config.label_fontsize)
            ax.set_xlabel("")

            # Add statistics
            stats_text = f"μ={values.mean():.2f}\nσ={values.std():.2f}"
            ax.text(0.95, 0.95, stats_text, transform=ax.transAxes,
                   ha="right", va="top", fontsize=8,
                   bbox=dict(boxstyle="round", facecolor="white", alpha=0.8))

        # Hide empty subplots
        for j in range(i + 1, len(axes)):
            axes[j].set_visible(False)

        fig.suptitle(title, fontsize=self.config.title_fontsize)
        plt.tight_layout()

        if output_path:
            fig.savefig(output_path, dpi=self.config.dpi, bbox_inches="tight")

        return fig

    def plot_rolling_correlations(
        self,
        rolling_data: pd.DataFrame,
        pairs: Optional[List[str]] = None,
        title: str = "Rolling Factor Correlations",
        output_path: Optional[Path] = None,
    ) -> Optional[plt.Figure]:
        """
        Plot rolling correlations over time.

        Args:
            rolling_data: DataFrame with rolling correlations
            pairs: Optional list of specific pairs to plot
            title: Plot title
            output_path: Optional path to save figure

        Returns:
            matplotlib Figure or None
        """
        if not MATPLOTLIB_AVAILABLE:
            return None

        if pairs:
            plot_data = rolling_data[pairs]
        else:
            # Limit to top 10 pairs
            plot_data = rolling_data.iloc[:, :10]

        fig, ax = plt.subplots(figsize=self.config.figsize, dpi=self.config.dpi)

        for col in plot_data.columns:
            ax.plot(plot_data.index, plot_data[col], label=col, alpha=0.7)

        ax.axhline(y=0, color="black", linestyle="--", linewidth=0.5)
        ax.set_xlabel("Date", fontsize=self.config.label_fontsize)
        ax.set_ylabel("Correlation", fontsize=self.config.label_fontsize)
        ax.set_title(title, fontsize=self.config.title_fontsize)
        ax.legend(loc="best", fontsize=8)
        ax.set_ylim(-1, 1)

        plt.tight_layout()

        if output_path:
            fig.savefig(output_path, dpi=self.config.dpi, bbox_inches="tight")

        return fig

    def plot_pca_variance(
        self,
        explained_variance: List[float],
        title: str = "PCA Explained Variance",
        output_path: Optional[Path] = None,
    ) -> Optional[plt.Figure]:
        """
        Plot PCA explained variance.

        Args:
            explained_variance: List of variance ratios per component
            title: Plot title
            output_path: Optional path to save figure

        Returns:
            matplotlib Figure or None
        """
        if not MATPLOTLIB_AVAILABLE:
            return None

        fig, ax = plt.subplots(figsize=(10, 6), dpi=self.config.dpi)

        n_components = len(explained_variance)
        cumulative = np.cumsum(explained_variance)

        # Bar plot for individual variance
        bars = ax.bar(range(1, n_components + 1), explained_variance,
                     color="steelblue", alpha=0.7, label="Individual")

        # Line plot for cumulative variance
        ax.plot(range(1, n_components + 1), cumulative,
               "ro-", label="Cumulative")

        # Add percentage labels
        for i, (ind, cum) in enumerate(zip(explained_variance, cumulative)):
            ax.text(i + 1, ind + 0.02, f"{ind:.1%}", ha="center", fontsize=9)

        ax.set_xlabel("Principal Component", fontsize=self.config.label_fontsize)
        ax.set_ylabel("Explained Variance Ratio", fontsize=self.config.label_fontsize)
        ax.set_title(title, fontsize=self.config.title_fontsize)
        ax.set_xticks(range(1, n_components + 1))
        ax.legend(loc="best")
        ax.set_ylim(0, 1.1)

        plt.tight_layout()

        if output_path:
            fig.savefig(output_path, dpi=self.config.dpi, bbox_inches="tight")

        return fig

    def plot_factor_clusters(
        self,
        correlation_matrix: pd.DataFrame,
        cluster_labels: Dict[str, int],
        title: str = "Factor Clusters",
        output_path: Optional[Path] = None,
    ) -> Optional[plt.Figure]:
        """
        Plot clustered correlation heatmap.

        Args:
            correlation_matrix: Correlation matrix
            cluster_labels: Dict mapping factors to cluster IDs
            title: Plot title
            output_path: Optional path to save figure

        Returns:
            matplotlib Figure or None
        """
        if not MATPLOTLIB_AVAILABLE:
            return None

        # Reorder factors by cluster
        factors_ordered = sorted(cluster_labels.keys(), key=lambda x: cluster_labels[x])
        corr_ordered = correlation_matrix.loc[factors_ordered, factors_ordered]

        fig, ax = plt.subplots(figsize=self.config.figsize, dpi=self.config.dpi)

        if SEABORN_AVAILABLE:
            # Create cluster color mapping
            n_clusters = max(cluster_labels.values()) + 1
            colors = plt.cm.Set3(np.linspace(0, 1, n_clusters))

            row_colors = [colors[cluster_labels[f]] for f in factors_ordered]

            # Use clustermap for hierarchical visualization
            g = sns.clustermap(
                corr_ordered,
                row_cluster=False,
                col_cluster=False,
                cmap=self.config.palette,
                center=0,
                row_colors=row_colors,
                col_colors=row_colors,
                figsize=self.config.figsize,
            )
            g.fig.suptitle(title, y=1.02)
            fig = g.fig
        else:
            im = ax.imshow(corr_ordered.values, cmap=self.config.palette, vmin=-1, vmax=1)
            plt.colorbar(im, ax=ax)

            ax.set_xticks(range(len(factors_ordered)))
            ax.set_yticks(range(len(factors_ordered)))
            ax.set_xticklabels(factors_ordered, rotation=45, ha="right")
            ax.set_yticklabels(factors_ordered)
            ax.set_title(title, fontsize=self.config.title_fontsize)

            plt.tight_layout()

        if output_path:
            fig.savefig(output_path, dpi=self.config.dpi, bbox_inches="tight")

        return fig

    def plot_factor_performance(
        self,
        returns_by_factor: Dict[str, pd.Series],
        title: str = "Factor Performance Over Time",
        output_path: Optional[Path] = None,
    ) -> Optional[plt.Figure]:
        """
        Plot cumulative returns by factor.

        Args:
            returns_by_factor: Dict mapping factor names to return series
            title: Plot title
            output_path: Optional path to save figure

        Returns:
            matplotlib Figure or None
        """
        if not MATPLOTLIB_AVAILABLE:
            return None

        fig, ax = plt.subplots(figsize=self.config.figsize, dpi=self.config.dpi)

        for factor_name, returns in returns_by_factor.items():
            cumulative = (1 + returns).cumprod()
            ax.plot(cumulative.index, cumulative.values, label=factor_name, linewidth=1.5)

        ax.axhline(y=1, color="black", linestyle="--", linewidth=0.5)
        ax.set_xlabel("Date", fontsize=self.config.label_fontsize)
        ax.set_ylabel("Cumulative Return", fontsize=self.config.label_fontsize)
        ax.set_title(title, fontsize=self.config.title_fontsize)
        ax.legend(loc="best")

        plt.tight_layout()

        if output_path:
            fig.savefig(output_path, dpi=self.config.dpi, bbox_inches="tight")

        return fig

    def plot_decay_analysis(
        self,
        factor_name: str,
        horizons: List[int],
        autocorrelations: List[float],
        half_life: Optional[float] = None,
        title: Optional[str] = None,
        output_path: Optional[Path] = None,
    ) -> Optional[plt.Figure]:
        """
        Plot factor signal decay analysis.

        Args:
            factor_name: Name of factor
            horizons: List of horizons (days)
            autocorrelations: Autocorrelation at each horizon
            half_life: Estimated half-life
            title: Plot title
            output_path: Optional path to save figure

        Returns:
            matplotlib Figure or None
        """
        if not MATPLOTLIB_AVAILABLE:
            return None

        fig, ax = plt.subplots(figsize=(10, 6), dpi=self.config.dpi)

        ax.plot(horizons, autocorrelations, "bo-", linewidth=2, markersize=8)
        ax.fill_between(horizons, autocorrelations, alpha=0.3)

        # Add half-life line
        if half_life:
            ax.axvline(x=half_life, color="red", linestyle="--",
                      label=f"Half-life: {half_life:.1f} days")
            ax.axhline(y=0.5, color="gray", linestyle=":", alpha=0.5)

        ax.axhline(y=0, color="black", linestyle="-", linewidth=0.5)

        ax.set_xlabel("Horizon (Trading Days)", fontsize=self.config.label_fontsize)
        ax.set_ylabel("Autocorrelation", fontsize=self.config.label_fontsize)
        ax.set_title(title or f"Signal Decay: {factor_name}", fontsize=self.config.title_fontsize)
        ax.set_ylim(-0.2, 1.1)

        if half_life:
            ax.legend(loc="best")

        plt.tight_layout()

        if output_path:
            fig.savefig(output_path, dpi=self.config.dpi, bbox_inches="tight")

        return fig

    def plot_factor_scatter(
        self,
        data: pd.DataFrame,
        factor_x: str,
        factor_y: str,
        color_by: Optional[str] = None,
        title: Optional[str] = None,
        output_path: Optional[Path] = None,
    ) -> Optional[plt.Figure]:
        """
        Create scatter plot of two factors.

        Args:
            data: DataFrame with factor values
            factor_x: X-axis factor
            factor_y: Y-axis factor
            color_by: Optional column to color by
            title: Plot title
            output_path: Optional path to save figure

        Returns:
            matplotlib Figure or None
        """
        if not MATPLOTLIB_AVAILABLE:
            return None

        fig, ax = plt.subplots(figsize=(10, 8), dpi=self.config.dpi)

        x = data[factor_x]
        y = data[factor_y]

        if color_by and color_by in data.columns:
            scatter = ax.scatter(x, y, c=data[color_by], cmap="viridis", alpha=0.6)
            plt.colorbar(scatter, ax=ax, label=color_by)
        else:
            ax.scatter(x, y, alpha=0.6, color="steelblue")

        # Add trend line
        valid_mask = ~(x.isna() | y.isna())
        if valid_mask.sum() > 10:
            z = np.polyfit(x[valid_mask], y[valid_mask], 1)
            p = np.poly1d(z)
            x_line = np.linspace(x[valid_mask].min(), x[valid_mask].max(), 100)
            ax.plot(x_line, p(x_line), "r--", alpha=0.8, label=f"Trend (slope={z[0]:.3f})")
            ax.legend()

        ax.set_xlabel(factor_x, fontsize=self.config.label_fontsize)
        ax.set_ylabel(factor_y, fontsize=self.config.label_fontsize)
        ax.set_title(title or f"{factor_x} vs {factor_y}", fontsize=self.config.title_fontsize)

        plt.tight_layout()

        if output_path:
            fig.savefig(output_path, dpi=self.config.dpi, bbox_inches="tight")

        return fig


def create_visualizer(config: Optional[PlotConfig] = None) -> FactorVisualizer:
    """Create a factor visualizer."""
    return FactorVisualizer(config)
