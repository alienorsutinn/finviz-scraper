"""
Research tools for validating and improving investment strategies.

Features:
- Factor correlation analysis
- Factor decay analysis (signal persistence)
- Rolling backtest performance
- Attribution analysis
"""
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

LOGGER = logging.getLogger(__name__)


@dataclass
class FactorAnalysis:
    """Results from factor correlation analysis."""

    correlation_matrix: pd.DataFrame
    highly_correlated_pairs: List[Tuple[str, str, float]]  # (factor1, factor2, correlation)
    redundant_factors: List[str]  # Factors that are highly correlated with others


@dataclass
class DecayAnalysis:
    """Results from factor decay analysis."""

    factor_name: str
    decay_by_period: Dict[int, float]  # Period (days) -> correlation with future returns
    optimal_holding_period: int  # Period with highest correlation
    half_life: Optional[int]  # Days until correlation decays to 50%


class ResearchToolkit:
    """Toolkit for investment strategy research and validation."""

    def analyze_factor_correlations(
        self,
        history_df: pd.DataFrame,
        factor_columns: Optional[List[str]] = None,
        threshold: float = 0.70,
    ) -> FactorAnalysis:
        """
        Analyze correlations between scoring factors.

        Args:
            history_df: Historical screening data
            factor_columns: List of factor columns to analyze (auto-detect if None)
            threshold: Correlation threshold for identifying redundancy

        Returns:
            FactorAnalysis object
        """
        if factor_columns is None:
            # Auto-detect score columns
            factor_columns = [col for col in history_df.columns if col.startswith("score_")]

        if not factor_columns:
            LOGGER.warning("No factor columns found")
            return FactorAnalysis(
                correlation_matrix=pd.DataFrame(),
                highly_correlated_pairs=[],
                redundant_factors=[],
            )

        # Calculate correlation matrix
        factor_data = history_df[factor_columns].copy()
        corr_matrix = factor_data.corr()

        # Find highly correlated pairs
        highly_correlated = []
        for i in range(len(factor_columns)):
            for j in range(i + 1, len(factor_columns)):
                corr = corr_matrix.iloc[i, j]
                if abs(corr) >= threshold:
                    highly_correlated.append((factor_columns[i], factor_columns[j], corr))

        # Identify redundant factors (highly correlated with multiple others)
        factor_corr_counts = {}
        for f1, f2, _ in highly_correlated:
            factor_corr_counts[f1] = factor_corr_counts.get(f1, 0) + 1
            factor_corr_counts[f2] = factor_corr_counts.get(f2, 0) + 1

        # Factors with 2+ high correlations are considered redundant
        redundant = [f for f, count in factor_corr_counts.items() if count >= 2]

        return FactorAnalysis(
            correlation_matrix=corr_matrix,
            highly_correlated_pairs=sorted(highly_correlated, key=lambda x: -abs(x[2])),
            redundant_factors=redundant,
        )

    def analyze_factor_decay(
        self,
        history_df: pd.DataFrame,
        factor_column: str,
        returns_column: str = "returns_1m",
        periods: Optional[List[int]] = None,
    ) -> DecayAnalysis:
        """
        Analyze how long a factor signal persists.

        Args:
            history_df: Historical screening data with returns
            factor_column: Factor column to analyze
            returns_column: Returns column (or compute forward returns)
            periods: List of periods (days) to analyze

        Returns:
            DecayAnalysis object
        """
        if periods is None:
            periods = [1, 3, 5, 7, 14, 21, 30, 60, 90]

        if factor_column not in history_df.columns:
            LOGGER.error(f"Factor {factor_column} not found in data")
            return DecayAnalysis(
                factor_name=factor_column,
                decay_by_period={},
                optimal_holding_period=0,
                half_life=None,
            )

        # Ensure data is sorted by date
        if "as_of_date" in history_df.columns:
            history_df = history_df.sort_values("as_of_date")

        decay_by_period = {}

        # For each period, calculate correlation between factor and forward returns
        for period in periods:
            # Compute forward returns for this period
            if "as_of_date" in history_df.columns and "ticker" in history_df.columns:
                # Group by ticker and compute forward returns
                history_df[f"_fwd_ret_{period}d"] = history_df.groupby("ticker")[
                    "price" if "price" in history_df.columns else "close"
                ].pct_change(periods=period)

                # Calculate correlation
                valid_data = history_df[[factor_column, f"_fwd_ret_{period}d"]].dropna()
                if len(valid_data) > 30:  # Need minimum samples
                    corr = valid_data[factor_column].corr(valid_data[f"_fwd_ret_{period}d"])
                    decay_by_period[period] = corr
            else:
                LOGGER.warning("Need 'as_of_date' and 'ticker' columns for decay analysis")
                break

        if not decay_by_period:
            return DecayAnalysis(
                factor_name=factor_column,
                decay_by_period={},
                optimal_holding_period=0,
                half_life=None,
            )

        # Find optimal holding period (highest correlation)
        optimal_period = max(decay_by_period.items(), key=lambda x: x[1])[0]

        # Calculate half-life (where correlation drops to 50% of peak)
        peak_corr = max(decay_by_period.values())
        half_life = None
        if peak_corr > 0:
            target = peak_corr * 0.5
            for period in sorted(decay_by_period.keys()):
                if decay_by_period[period] <= target:
                    half_life = period
                    break

        return DecayAnalysis(
            factor_name=factor_column,
            decay_by_period=decay_by_period,
            optimal_holding_period=optimal_period,
            half_life=half_life,
        )

    def rolling_backtest_performance(
        self,
        history_df: pd.DataFrame,
        factor_column: str,
        window_days: int = 90,
        step_days: int = 30,
        top_n: int = 20,
    ) -> pd.DataFrame:
        """
        Compute rolling backtest performance over time.

        Args:
            history_df: Historical screening data
            factor_column: Factor to backtest
            window_days: Backtest window size (days)
            step_days: Step size between windows (days)
            top_n: Number of top stocks to hold

        Returns:
            DataFrame with rolling performance metrics
        """
        if "as_of_date" not in history_df.columns:
            LOGGER.error("Need 'as_of_date' column for rolling backtest")
            return pd.DataFrame()

        history_df = history_df.sort_values("as_of_date")
        history_df["as_of_date"] = pd.to_datetime(history_df["as_of_date"])

        start_date = history_df["as_of_date"].min()
        end_date = history_df["as_of_date"].max()

        results = []

        current_date = start_date
        while current_date + timedelta(days=window_days) <= end_date:
            window_end = current_date + timedelta(days=window_days)

            # Get data for this window
            window_data = history_df[
                (history_df["as_of_date"] >= current_date) & (history_df["as_of_date"] < window_end)
            ]

            if len(window_data) > 0 and factor_column in window_data.columns:
                # Get top N stocks by factor
                top_stocks = window_data.nlargest(top_n, factor_column)

                # Calculate average score and dispersion
                avg_score = top_stocks[factor_column].mean()
                score_std = top_stocks[factor_column].std()

                results.append(
                    {
                        "start_date": current_date,
                        "end_date": window_end,
                        "avg_score": avg_score,
                        "score_std": score_std,
                        "num_stocks": len(top_stocks),
                    }
                )

            current_date += timedelta(days=step_days)

        return pd.DataFrame(results)

    def attribution_analysis(
        self,
        portfolio_returns: pd.Series,
        factor_returns: Dict[str, pd.Series],
    ) -> pd.DataFrame:
        """
        Attribute portfolio returns to different factors.

        Args:
            portfolio_returns: Time series of portfolio returns
            factor_returns: Dict of factor name -> factor returns time series

        Returns:
            DataFrame with factor contributions
        """
        # Align all series
        aligned_data = pd.DataFrame({"portfolio": portfolio_returns})
        for factor_name, returns in factor_returns.items():
            aligned_data[factor_name] = returns

        aligned_data = aligned_data.dropna()

        if len(aligned_data) < 30:
            LOGGER.warning("Insufficient data for attribution analysis")
            return pd.DataFrame()

        # Simple regression-based attribution
        from sklearn.linear_model import LinearRegression

        X = aligned_data[[c for c in aligned_data.columns if c != "portfolio"]].values
        y = aligned_data["portfolio"].values

        model = LinearRegression()
        model.fit(X, y)

        # Calculate factor contributions
        factor_names = [c for c in aligned_data.columns if c != "portfolio"]
        contributions = []

        for i, factor_name in enumerate(factor_names):
            contribution = model.coef_[i] * aligned_data[factor_name].mean()
            contributions.append(
                {
                    "factor": factor_name,
                    "beta": model.coef_[i],
                    "avg_return": aligned_data[factor_name].mean(),
                    "contribution": contribution,
                }
            )

        result = pd.DataFrame(contributions)
        result["contribution_pct"] = (
            result["contribution"] / result["contribution"].sum() * 100
        )

        return result.sort_values("contribution", ascending=False)


def print_correlation_analysis(analysis: FactorAnalysis):
    """Print factor correlation analysis results."""
    print("\n" + "=" * 80)
    print("FACTOR CORRELATION ANALYSIS")
    print("=" * 80)

    print(f"\nCorrelation Matrix ({len(analysis.correlation_matrix)} factors):")
    print(analysis.correlation_matrix.round(2))

    if analysis.highly_correlated_pairs:
        print(f"\nHighly Correlated Pairs (|r| >= 0.70):")
        for f1, f2, corr in analysis.highly_correlated_pairs[:10]:
            print(f"  {f1:30s} <-> {f2:30s}  r = {corr:5.2f}")
        if len(analysis.highly_correlated_pairs) > 10:
            print(f"  ... and {len(analysis.highly_correlated_pairs) - 10} more")

    if analysis.redundant_factors:
        print(f"\nPotentially Redundant Factors:")
        for factor in analysis.redundant_factors:
            print(f"  - {factor}")
        print("\n💡 Consider removing redundant factors to simplify model")
    else:
        print("\n✅ No highly redundant factors detected")

    print("=" * 80)


def print_decay_analysis(analysis: DecayAnalysis):
    """Print factor decay analysis results."""
    print("\n" + "=" * 80)
    print(f"FACTOR DECAY ANALYSIS - {analysis.factor_name}")
    print("=" * 80)

    if not analysis.decay_by_period:
        print("\nNo decay data available")
        return

    print("\nCorrelation with Forward Returns:")
    print(f"{'Period (days)':<15} {'Correlation':<15} {'Bar'}")
    print("-" * 60)

    max_corr = max(analysis.decay_by_period.values())
    for period in sorted(analysis.decay_by_period.keys()):
        corr = analysis.decay_by_period[period]
        bar_len = int(abs(corr) / max_corr * 40) if max_corr > 0 else 0
        bar = "█" * bar_len
        star = " ⭐" if period == analysis.optimal_holding_period else ""
        print(f"{period:<15} {corr:>6.3f}         {bar}{star}")

    print(f"\nOptimal Holding Period: {analysis.optimal_holding_period} days")
    if analysis.half_life:
        print(f"Signal Half-Life:       {analysis.half_life} days")
    else:
        print("Signal Half-Life:       >90 days (long-lasting signal)")

    print("=" * 80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Research toolkit for strategy analysis")
    parser.add_argument("--history", required=True, help="Path to history parquet file")
    parser.add_argument(
        "--analysis",
        choices=["correlation", "decay", "rolling"],
        default="correlation",
        help="Analysis type",
    )
    parser.add_argument("--factor", help="Factor column for decay analysis")

    args = parser.parse_args()

    # Load history
    history_df = pd.read_parquet(args.history)
    print(f"Loaded {len(history_df)} rows from {args.history}")

    toolkit = ResearchToolkit()

    if args.analysis == "correlation":
        analysis = toolkit.analyze_factor_correlations(history_df)
        print_correlation_analysis(analysis)

    elif args.analysis == "decay":
        if not args.factor:
            print("Error: --factor required for decay analysis")
            exit(1)
        analysis = toolkit.analyze_factor_decay(history_df, args.factor)
        print_decay_analysis(analysis)

    elif args.analysis == "rolling":
        if not args.factor:
            print("Error: --factor required for rolling analysis")
            exit(1)
        results = toolkit.rolling_backtest_performance(history_df, args.factor)
        print("\nRolling Performance:")
        print(results.to_string(index=False))
