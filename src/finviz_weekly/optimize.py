"""
ML-based factor weight optimization using Bayesian optimization.

Automatically find optimal factor weights that maximize backtest performance.
"""
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

LOGGER = logging.getLogger(__name__)


@dataclass
class OptimizationResult:
    """Result from factor weight optimization."""

    optimal_weights: Dict[str, float]
    best_sharpe: float
    best_return: float
    iterations: int
    improvement_pct: float  # vs equal weights


class FactorOptimizer:
    """Optimize factor weights using ML techniques."""

    def __init__(
        self,
        history_path: Path,
        start_date: str,
        end_date: str,
        objective: str = "sharpe",  # "sharpe", "return", or "risk_adjusted"
    ):
        """
        Initialize factor optimizer.

        Args:
            history_path: Path to historical scoring data
            start_date: Optimization start date (YYYY-MM-DD)
            end_date: Optimization end date (YYYY-MM-DD)
            objective: Optimization objective
        """
        self.history_path = history_path
        self.start_date = start_date
        self.end_date = end_date
        self.objective = objective

        # Load history
        self.history = pd.read_parquet(history_path)
        LOGGER.info(f"Loaded {len(self.history)} rows from history")

    def optimize_weights(
        self,
        factors: List[str],
        n_iterations: int = 50,
        n_initial_points: int = 10,
    ) -> OptimizationResult:
        """
        Optimize factor weights using Bayesian optimization.

        Args:
            factors: List of factor columns to optimize
            n_iterations: Number of optimization iterations
            n_initial_points: Number of random initial points

        Returns:
            OptimizationResult with optimal weights
        """
        # Validate factors
        missing = [f for f in factors if f not in self.history.columns]
        if missing:
            raise ValueError(f"Factors not found in history: {missing}")

        LOGGER.info(f"Optimizing {len(factors)} factors over {n_iterations} iterations")

        try:
            from skopt import gp_minimize
            from skopt.space import Real
        except ImportError:
            LOGGER.error("scikit-optimize not installed. Install with: pip install scikit-optimize")
            # Fallback to grid search
            return self._optimize_grid_search(factors)

        # Define search space (weights between 0 and 1 for each factor)
        space = [Real(0.0, 1.0, name=f"weight_{f}") for f in factors]

        # Objective function
        def objective_function(weights):
            """Evaluate a set of weights."""
            return -self._evaluate_weights(factors, weights)  # Negative because we minimize

        # Run optimization
        result = gp_minimize(
            objective_function,
            space,
            n_calls=n_iterations,
            n_initial_points=n_initial_points,
            random_state=42,
            verbose=False,
        )

        # Get optimal weights
        optimal_weights_list = result.x
        optimal_weights = {f: w for f, w in zip(factors, optimal_weights_list)}

        # Normalize weights to sum to 1
        total = sum(optimal_weights.values())
        if total > 0:
            optimal_weights = {f: w / total for f, w in optimal_weights.items()}

        # Evaluate optimal weights
        best_sharpe = -result.fun

        # Calculate actual return for optimal weights
        best_return = self._calculate_portfolio_return(factors, optimal_weights_list)

        # Calculate improvement vs equal weights
        equal_weights = [1.0 / len(factors)] * len(factors)
        baseline_sharpe = -objective_function(equal_weights)
        improvement_pct = ((best_sharpe - baseline_sharpe) / abs(baseline_sharpe)) * 100 if baseline_sharpe != 0 else 0

        LOGGER.info(f"Optimization complete. Best Sharpe: {best_sharpe:.3f}, Return: {best_return*100:.1f}% (improvement: {improvement_pct:+.1f}%)")

        return OptimizationResult(
            optimal_weights=optimal_weights,
            best_sharpe=best_sharpe,
            best_return=best_return,
            iterations=n_iterations,
            improvement_pct=improvement_pct,
        )

    def _calculate_portfolio_return(self, factors: List[str], weights: List[float]) -> float:
        """
        Calculate cumulative return for a set of factor weights.

        Args:
            factors: List of factor names
            weights: List of factor weights

        Returns:
            Total cumulative return (e.g., 0.25 for 25% return)
        """
        # Get period returns from backtest
        weights_dict = {f: w for f, w in zip(factors, weights)}

        history_filtered = self.history[
            (self.history["as_of_date"] >= self.start_date) & (self.history["as_of_date"] <= self.end_date)
        ].copy()

        if len(history_filtered) == 0:
            return 0.0

        # Calculate composite score
        history_filtered["composite_score"] = 0.0
        for factor, weight in weights_dict.items():
            if factor in history_filtered.columns:
                factor_values = pd.to_numeric(history_filtered[factor], errors="coerce").fillna(0)
                history_filtered["composite_score"] += factor_values * weight

        # Backtest returns
        dates = sorted(history_filtered["as_of_date"].unique())
        returns = []

        for i in range(len(dates) - 1):
            current_date = dates[i]
            next_date = dates[i + 1]

            current_data = history_filtered[history_filtered["as_of_date"] == current_date]
            top_stocks = current_data.nlargest(20, "composite_score")

            if len(top_stocks) == 0:
                continue

            tickers = top_stocks["ticker"].tolist()
            next_data = history_filtered[
                (history_filtered["as_of_date"] == next_date) & (history_filtered["ticker"].isin(tickers))
            ]

            if len(next_data) > 0 and "price" in next_data.columns:
                merged = top_stocks[["ticker", "price"]].merge(
                    next_data[["ticker", "price"]], on="ticker", suffixes=("_current", "_next")
                )

                if len(merged) > 0:
                    # Avoid division by zero
                    merged = merged[merged["price_current"] > 0]
                    if len(merged) > 0:
                        merged["return"] = (merged["price_next"] - merged["price_current"]) / merged["price_current"]
                        period_return = merged["return"].mean()
                        returns.append(period_return)

        if len(returns) == 0:
            return 0.0

        # Compound returns: (1+r1)*(1+r2)*...*(1+rn) - 1
        returns_array = np.array(returns)
        cumulative = np.prod(1 + returns_array) - 1

        return float(cumulative)

    def _evaluate_weights(self, factors: List[str], weights: List[float]) -> float:
        """
        Evaluate a set of factor weights using backtesting.

        Args:
            factors: List of factor names
            weights: List of factor weights

        Returns:
            Performance metric (Sharpe ratio by default)
        """
        # Create composite score
        weights_dict = {f: w for f, w in zip(factors, weights)}

        # Calculate weighted composite score for each stock
        history_filtered = self.history[
            (self.history["as_of_date"] >= self.start_date) & (self.history["as_of_date"] <= self.end_date)
        ].copy()

        if len(history_filtered) == 0:
            return 0.0

        # Compute composite score
        history_filtered["composite_score"] = 0.0
        for factor, weight in weights_dict.items():
            if factor in history_filtered.columns:
                factor_values = pd.to_numeric(history_filtered[factor], errors="coerce").fillna(0)
                history_filtered["composite_score"] += factor_values * weight

        # Simple backtest: select top 20 stocks by composite score each period
        # and measure forward returns

        # Group by date
        dates = sorted(history_filtered["as_of_date"].unique())

        returns = []
        for i in range(len(dates) - 1):
            current_date = dates[i]
            next_date = dates[i + 1]

            # Get top 20 stocks by composite score
            current_data = history_filtered[history_filtered["as_of_date"] == current_date]
            top_stocks = current_data.nlargest(20, "composite_score")

            if len(top_stocks) == 0:
                continue

            # Calculate returns to next period
            tickers = top_stocks["ticker"].tolist()
            next_data = history_filtered[
                (history_filtered["as_of_date"] == next_date) & (history_filtered["ticker"].isin(tickers))
            ]

            if len(next_data) > 0 and "price" in next_data.columns:
                # Merge to get price changes
                merged = top_stocks[["ticker", "price"]].merge(
                    next_data[["ticker", "price"]], on="ticker", suffixes=("_current", "_next")
                )

                if len(merged) > 0:
                    # Avoid division by zero
                    merged = merged[merged["price_current"] > 0]
                    if len(merged) > 0:
                        merged["return"] = (merged["price_next"] - merged["price_current"]) / merged["price_current"]
                        period_return = merged["return"].mean()
                        returns.append(period_return)

        if len(returns) < 10:
            return 0.0

        # Calculate Sharpe ratio
        returns_array = np.array(returns)
        mean_return = returns_array.mean()
        std_return = returns_array.std()

        if std_return == 0:
            return 0.0

        sharpe = mean_return / std_return * np.sqrt(252 / 7)  # Annualized assuming weekly rebalance

        return sharpe

    def _optimize_grid_search(self, factors: List[str]) -> OptimizationResult:
        """
        Fallback optimization using simple grid search.

        Used when scikit-optimize is not available.
        """
        LOGGER.info("Using grid search optimization (slower)")

        # Simple grid: test a few weight combinations
        best_sharpe = -np.inf
        best_weights = None

        # Test equal weights
        equal_weights = [1.0 / len(factors)] * len(factors)
        sharpe = self._evaluate_weights(factors, equal_weights)
        if sharpe > best_sharpe:
            best_sharpe = sharpe
            best_weights = equal_weights

        # Test emphasizing each factor
        for i in range(len(factors)):
            weights = [0.1 / (len(factors) - 1)] * len(factors)
            weights[i] = 0.9  # 90% weight on one factor
            sharpe = self._evaluate_weights(factors, weights)
            if sharpe > best_sharpe:
                best_sharpe = sharpe
                best_weights = weights

        optimal_weights = {f: w for f, w in zip(factors, best_weights)}

        return OptimizationResult(
            optimal_weights=optimal_weights,
            best_sharpe=best_sharpe,
            best_return=0.0,
            iterations=len(factors) + 1,
            improvement_pct=0.0,
        )

    def save_optimal_weights(self, result: OptimizationResult, output_path: Path):
        """Save optimal weights to JSON file."""
        output = {
            "optimization_date": pd.Timestamp.now().isoformat(),
            "optimization_period": {"start": self.start_date, "end": self.end_date},
            "objective": self.objective,
            "weights": result.optimal_weights,
            "performance": {
                "sharpe_ratio": result.best_sharpe,
                "improvement_pct": result.improvement_pct,
                "iterations": result.iterations,
            },
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(output, indent=2))

        LOGGER.info(f"Saved optimal weights to {output_path}")


def print_optimization_result(result: OptimizationResult):
    """Print optimization results."""
    print("\n" + "=" * 80)
    print("FACTOR WEIGHT OPTIMIZATION RESULTS")
    print("=" * 80)

    print(f"\nObjective:           Maximize Sharpe Ratio")
    print(f"Iterations:          {result.iterations}")
    print(f"Best Sharpe Ratio:   {result.best_sharpe:.3f}")
    print(f"Improvement:         {result.improvement_pct:+.1f}% vs equal weights")

    print("\nOptimal Factor Weights:")
    print(f"{'Factor':<40} {'Weight':<10} {'Bar'}")
    print("-" * 80)

    sorted_weights = sorted(result.optimal_weights.items(), key=lambda x: -x[1])
    for factor, weight in sorted_weights:
        bar_len = int(weight * 50)
        bar = "█" * bar_len
        print(f"{factor:<40} {weight*100:>5.1f}%    {bar}")

    print("\n💡 Tip: Use these weights in your scoring configuration")
    print("=" * 80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Optimize factor weights")
    parser.add_argument("--history", required=True, help="Path to history parquet file")
    parser.add_argument("--start-date", required=True, help="Optimization start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="Optimization end date (YYYY-MM-DD)")
    parser.add_argument("--output", help="Output path for optimal weights JSON")
    parser.add_argument("--iterations", type=int, default=50, help="Number of optimization iterations")
    parser.add_argument(
        "--factors",
        nargs="+",
        default=["score_quality", "score_value", "score_growth", "score_momentum"],
        help="Factors to optimize",
    )

    args = parser.parse_args()

    optimizer = FactorOptimizer(
        history_path=Path(args.history),
        start_date=args.start_date,
        end_date=args.end_date,
    )

    result = optimizer.optimize_weights(args.factors, n_iterations=args.iterations)

    print_optimization_result(result)

    if args.output:
        optimizer.save_optimal_weights(result, Path(args.output))
