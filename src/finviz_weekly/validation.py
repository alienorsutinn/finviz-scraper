"""Walk-forward validation framework for out-of-sample testing."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .backtest import Backtester, BacktestConfig, BacktestResults
from .learn import train_weights, TrainConfig, FACTOR_COLS, FALLBACK_WEIGHTS

LOGGER = logging.getLogger(__name__)


@dataclass
class ValidationConfig:
    """Configuration for walk-forward validation."""

    start_date: str  # ISO format YYYY-MM-DD
    end_date: str  # ISO format YYYY-MM-DD
    train_window_days: int = 365  # Rolling training window (1 year)
    test_window_days: int = 90  # OOS testing window (3 months)
    step_days: int = 30  # Step forward by 1 month between folds
    min_train_rows: int = 200  # Minimum rows required for training

    # Backtest configuration
    top_n: int = 20
    rebalance_days: int = 7
    min_price: float = 5.0

    def __post_init__(self):
        """Validate configuration."""
        if self.train_window_days < 90:
            raise ValueError(f"train_window_days must be >= 90, got {self.train_window_days}")

        if self.test_window_days < 30:
            raise ValueError(f"test_window_days must be >= 30, got {self.test_window_days}")

        if self.step_days < 1:
            raise ValueError(f"step_days must be >= 1, got {self.step_days}")

        if self.min_train_rows < 50:
            raise ValueError(f"min_train_rows must be >= 50, got {self.min_train_rows}")


@dataclass
class FoldResults:
    """Results from a single validation fold."""

    fold_id: int
    train_start: str
    train_end: str
    test_start: str
    test_end: str

    # Training metrics
    train_rows: int
    train_weights: Dict[str, float]
    train_ic: Dict[str, float]
    train_ic_mean: float

    # Out-of-sample test metrics
    test_return: float
    test_annual_return: float
    test_sharpe: float
    test_max_drawdown: float
    test_num_trades: int

    # Baseline comparisons
    baseline_equal_weight_return: Optional[float] = None
    baseline_equal_weight_sharpe: Optional[float] = None


@dataclass
class ValidationResults:
    """Results from walk-forward validation."""

    config: ValidationConfig
    num_folds: int
    folds: List[FoldResults]

    # Aggregate OOS metrics
    oos_mean_return: float
    oos_mean_sharpe: float
    oos_mean_max_drawdown: float
    oos_win_rate: float  # % of folds with positive returns

    # Overfitting detection
    is_mean_ic: float  # Mean in-sample IC across folds
    oos_sharpe_ratio_vs_is: float  # OOS Sharpe / IS mean IC (overfitting indicator)
    overfitting_detected: bool  # True if OOS metrics significantly worse than IS

    # Baseline comparisons
    outperformance_vs_equal_weight: Optional[float] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "config": {
                "start_date": self.config.start_date,
                "end_date": self.config.end_date,
                "train_window_days": self.config.train_window_days,
                "test_window_days": self.config.test_window_days,
                "step_days": self.config.step_days,
                "top_n": self.config.top_n,
            },
            "num_folds": self.num_folds,
            "oos_mean_return": self.oos_mean_return,
            "oos_mean_sharpe": self.oos_mean_sharpe,
            "oos_mean_max_drawdown": self.oos_mean_max_drawdown,
            "oos_win_rate": self.oos_win_rate,
            "is_mean_ic": self.is_mean_ic,
            "oos_sharpe_ratio_vs_is": self.oos_sharpe_ratio_vs_is,
            "overfitting_detected": self.overfitting_detected,
            "outperformance_vs_equal_weight": self.outperformance_vs_equal_weight,
            "folds": [
                {
                    "fold_id": f.fold_id,
                    "train_start": f.train_start,
                    "train_end": f.train_end,
                    "test_start": f.test_start,
                    "test_end": f.test_end,
                    "train_rows": f.train_rows,
                    "train_ic_mean": f.train_ic_mean,
                    "test_return": f.test_return,
                    "test_annual_return": f.test_annual_return,
                    "test_sharpe": f.test_sharpe,
                    "test_max_drawdown": f.test_max_drawdown,
                }
                for f in self.folds
            ],
        }


class WalkForwardValidator:
    """Walk-forward validation with expanding window."""

    def __init__(self, history_path: Path, prices_path: Optional[Path] = None):
        """
        Initialize validator.

        Args:
            history_path: Path to finviz_fundamentals_history.parquet
            prices_path: Optional path to prices.parquet (will use history/prices.parquet if None)
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

        LOGGER.info(f"Loaded {len(self.history)} rows, {self.history['as_of_date'].nunique()} unique dates")

    def run(self, config: ValidationConfig) -> ValidationResults:
        """
        Run walk-forward validation.

        Args:
            config: Validation configuration

        Returns:
            ValidationResults with OOS performance metrics
        """
        start = pd.to_datetime(config.start_date)
        end = pd.to_datetime(config.end_date)

        # Get time folds (expanding window)
        folds = self._create_folds(start, end, config)
        LOGGER.info(f"Created {len(folds)} validation folds")

        # Run each fold
        fold_results = []
        for i, fold_dates in enumerate(folds):
            LOGGER.info(f"\n{'='*60}")
            LOGGER.info(f"Fold {i+1}/{len(folds)}")
            LOGGER.info(f"{'='*60}")

            result = self._run_fold(i + 1, fold_dates, config)
            if result:
                fold_results.append(result)

        if not fold_results:
            raise ValueError("No valid folds completed. Check data availability and date ranges.")

        # Aggregate results
        return self._aggregate_results(config, fold_results)

    def _create_folds(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        config: ValidationConfig,
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
        """
        Create expanding window time folds.

        Returns:
            List of (train_start, train_end, test_start, test_end) tuples
        """
        folds = []

        # Start with initial training window
        train_start = start
        train_end = start + pd.Timedelta(days=config.train_window_days)

        while train_end < end:
            test_start = train_end
            test_end = test_start + pd.Timedelta(days=config.test_window_days)

            if test_end > end:
                break

            folds.append((train_start, train_end, test_start, test_end))

            # Expand window: keep same train_start, step forward
            train_end = train_end + pd.Timedelta(days=config.step_days)

        return folds

    def _run_fold(
        self,
        fold_id: int,
        fold_dates: Tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp],
        config: ValidationConfig,
    ) -> Optional[FoldResults]:
        """Run a single validation fold."""
        train_start, train_end, test_start, test_end = fold_dates

        LOGGER.info(f"Train: {train_start.date()} to {train_end.date()}")
        LOGGER.info(f"Test:  {test_start.date()} to {test_end.date()}")

        # Get training data
        train_data = self.history[
            (self.history["as_of_date"] >= train_start.date()) &
            (self.history["as_of_date"] < train_end.date())
        ].copy()

        if len(train_data) < config.min_train_rows:
            LOGGER.warning(
                f"Insufficient training data: {len(train_data)} < {config.min_train_rows}. Skipping fold."
            )
            return None

        # Train weights on training period
        train_weights, train_ic = self._train_on_period(train_data, train_start, train_end)
        train_ic_mean = np.nanmean(list(train_ic.values())) if train_ic else np.nan

        LOGGER.info(f"Trained weights: {train_weights}")
        LOGGER.info(f"Train IC (mean): {train_ic_mean:.4f}")

        # Apply weights to test period data
        test_data = self._apply_weights_to_data(
            self.history[
                (self.history["as_of_date"] >= test_start.date()) &
                (self.history["as_of_date"] < test_end.date())
            ].copy(),
            train_weights,
        )

        if len(test_data) == 0:
            LOGGER.warning("No test data available. Skipping fold.")
            return None

        # Run backtest on OOS period
        test_results = self._backtest_period(test_data, test_start, test_end, config)

        if test_results is None:
            LOGGER.warning("Backtest failed. Skipping fold.")
            return None

        LOGGER.info(f"OOS Return: {test_results.total_return:.2%}")
        LOGGER.info(f"OOS Sharpe: {test_results.sharpe_ratio:.2f}")
        LOGGER.info(f"OOS Max DD: {test_results.max_drawdown:.2%}")

        # Baseline: equal-weight portfolio
        baseline_equal_result = self._backtest_equal_weight_baseline(test_start, test_end, config)

        return FoldResults(
            fold_id=fold_id,
            train_start=train_start.strftime("%Y-%m-%d"),
            train_end=train_end.strftime("%Y-%m-%d"),
            test_start=test_start.strftime("%Y-%m-%d"),
            test_end=test_end.strftime("%Y-%m-%d"),
            train_rows=len(train_data),
            train_weights=train_weights,
            train_ic=train_ic,
            train_ic_mean=train_ic_mean,
            test_return=test_results.total_return,
            test_annual_return=test_results.annual_return,
            test_sharpe=test_results.sharpe_ratio,
            test_max_drawdown=test_results.max_drawdown,
            test_num_trades=len(test_results.trades),
            baseline_equal_weight_return=baseline_equal_result.total_return if baseline_equal_result else None,
            baseline_equal_weight_sharpe=baseline_equal_result.sharpe_ratio if baseline_equal_result else None,
        )

    def _train_on_period(
        self,
        train_data: pd.DataFrame,
        train_start: pd.Timestamp,
        train_end: pd.Timestamp,
    ) -> Tuple[Dict[str, float], Dict[str, float]]:
        """
        Train factor weights on a specific period.

        Returns:
            (weights_dict, ic_dict)
        """
        # Compute forward returns using prices
        if not self.prices_path.exists():
            LOGGER.warning(f"Prices file not found: {self.prices_path}. Using fallback weights.")
            return dict(FALLBACK_WEIGHTS), {}

        prices = pd.read_parquet(self.prices_path)
        prices["ticker"] = prices["ticker"].astype(str).str.upper()
        prices["date"] = pd.to_datetime(prices["date"], errors="coerce").dt.date

        # Filter prices to training period + lookforward
        price_start = train_start.date()
        price_end = (train_end + pd.Timedelta(days=90)).date()  # +90 days for forward returns
        prices = prices[
            (prices["date"] >= price_start) &
            (prices["date"] <= price_end)
        ]

        # Compute ICs using learn.py logic
        from .learn import _compute_forward_returns_from_prices, _spearman_ic, _normalize_positive_weights, _winsorize

        # Ensure train_data has as_of_date as datetime.date for learn.py
        train_data = train_data.copy()
        if pd.api.types.is_datetime64_any_dtype(train_data["as_of_date"]):
            train_data["as_of_date"] = pd.to_datetime(train_data["as_of_date"]).dt.date

        joined = _compute_forward_returns_from_prices(
            train_data,
            prices,
            horizon_trading_days=21,
        )
        joined["forward_return"] = _winsorize(joined["forward_return"])

        n_forward = int(joined["forward_return"].notna().sum())
        if n_forward < 50:
            LOGGER.warning(f"Insufficient forward return rows: {n_forward}. Using fallback weights.")
            return dict(FALLBACK_WEIGHTS), {}

        # Compute ICs
        ic_map = {}
        for col in FACTOR_COLS:
            if col in joined.columns:
                ic_map[col] = _spearman_ic(joined[col], joined["forward_return"])
            else:
                ic_map[col] = np.nan

        weights = _normalize_positive_weights(ic_map)

        return weights, ic_map

    def _apply_weights_to_data(
        self,
        data: pd.DataFrame,
        weights: Dict[str, float],
    ) -> pd.DataFrame:
        """Apply learned weights to compute total_score."""
        data = data.copy()

        # Compute weighted total score
        total_score = pd.Series(0.0, index=data.index)
        for col, weight in weights.items():
            if col in data.columns:
                total_score += data[col].fillna(0) * weight

        data["total_score"] = total_score
        return data

    def _backtest_period(
        self,
        data: pd.DataFrame,
        start: pd.Timestamp,
        end: pd.Timestamp,
        config: ValidationConfig,
    ) -> Optional[BacktestResults]:
        """Run backtest on a specific period."""
        # Create temporary history file for backtester
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = Path(tmp.name)
            data.to_parquet(tmp_path)

        try:
            backtester = Backtester(tmp_path, self.prices_path)

            bt_config = BacktestConfig(
                start_date=start.strftime("%Y-%m-%d"),
                end_date=end.strftime("%Y-%m-%d"),
                score_column="total_score",
                top_n=config.top_n,
                rebalance_days=config.rebalance_days,
                min_price=config.min_price,
            )

            results = backtester.run(bt_config)
            return results

        except Exception as e:
            LOGGER.error(f"Backtest failed: {e}", exc_info=True)
            return None
        finally:
            tmp_path.unlink(missing_ok=True)

    def _backtest_equal_weight_baseline(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        config: ValidationConfig,
    ) -> Optional[BacktestResults]:
        """Run equal-weight baseline backtest."""
        # Use equal weights for all factors
        equal_weights = {col: 1.0 / len(FACTOR_COLS) for col in FACTOR_COLS}

        data = self.history[
            (self.history["as_of_date"] >= start.date()) &
            (self.history["as_of_date"] < end.date())
        ].copy()

        data = self._apply_weights_to_data(data, equal_weights)

        return self._backtest_period(data, start, end, config)

    def _aggregate_results(
        self,
        config: ValidationConfig,
        folds: List[FoldResults],
    ) -> ValidationResults:
        """Aggregate results across all folds."""
        # OOS metrics
        oos_returns = [f.test_return for f in folds]
        oos_sharpes = [f.test_sharpe for f in folds]
        oos_drawdowns = [f.test_max_drawdown for f in folds]

        oos_mean_return = float(np.mean(oos_returns))
        oos_mean_sharpe = float(np.mean(oos_sharpes))
        oos_mean_max_drawdown = float(np.mean(oos_drawdowns))
        oos_win_rate = float(np.mean([1 if r > 0 else 0 for r in oos_returns]))

        # In-sample IC
        is_mean_ic = float(np.mean([f.train_ic_mean for f in folds]))

        # Overfitting detection: OOS Sharpe should be at least 50% of IS IC
        # (heuristic: IC of 0.05 should yield Sharpe ~1.0 if not overfitting)
        oos_sharpe_ratio_vs_is = oos_mean_sharpe / is_mean_ic if is_mean_ic > 0 else 0.0
        overfitting_detected = oos_sharpe_ratio_vs_is < 10.0  # Sharpe/IC ratio < 10 suggests overfitting

        # Baseline comparison
        baseline_returns = [f.baseline_equal_weight_return for f in folds if f.baseline_equal_weight_return is not None]
        if baseline_returns:
            baseline_mean_return = float(np.mean(baseline_returns))
            outperformance = oos_mean_return - baseline_mean_return
        else:
            outperformance = None

        LOGGER.info(f"\n{'='*60}")
        LOGGER.info("VALIDATION SUMMARY")
        LOGGER.info(f"{'='*60}")
        LOGGER.info(f"Number of folds: {len(folds)}")
        LOGGER.info(f"OOS Mean Return: {oos_mean_return:.2%}")
        LOGGER.info(f"OOS Mean Sharpe: {oos_mean_sharpe:.2f}")
        LOGGER.info(f"OOS Win Rate: {oos_win_rate:.1%}")
        LOGGER.info(f"IS Mean IC: {is_mean_ic:.4f}")
        LOGGER.info(f"Overfitting Detected: {overfitting_detected}")
        if outperformance is not None:
            LOGGER.info(f"Outperformance vs Equal-Weight: {outperformance:.2%}")
        LOGGER.info(f"{'='*60}\n")

        return ValidationResults(
            config=config,
            num_folds=len(folds),
            folds=folds,
            oos_mean_return=oos_mean_return,
            oos_mean_sharpe=oos_mean_sharpe,
            oos_mean_max_drawdown=oos_mean_max_drawdown,
            oos_win_rate=oos_win_rate,
            is_mean_ic=is_mean_ic,
            oos_sharpe_ratio_vs_is=oos_sharpe_ratio_vs_is,
            overfitting_detected=overfitting_detected,
            outperformance_vs_equal_weight=outperformance,
        )


def run_validation(
    history_path: str | Path,
    start_date: str = "2023-01-01",
    end_date: str = "2025-01-01",
    train_window_days: int = 365,
    test_window_days: int = 90,
    step_days: int = 30,
    top_n: int = 20,
    out_path: Optional[str | Path] = None,
) -> ValidationResults:
    """
    Run walk-forward validation and save results.

    Args:
        history_path: Path to finviz_fundamentals_history.parquet
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        train_window_days: Training window size in days
        test_window_days: Test window size in days
        step_days: Step size between folds
        top_n: Number of stocks to hold
        out_path: Optional path to save results JSON

    Returns:
        ValidationResults
    """
    config = ValidationConfig(
        start_date=start_date,
        end_date=end_date,
        train_window_days=train_window_days,
        test_window_days=test_window_days,
        step_days=step_days,
        top_n=top_n,
    )

    validator = WalkForwardValidator(Path(history_path))
    results = validator.run(config)

    # Save results if output path specified
    if out_path:
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with open(out_path, "w") as f:
            json.dump(results.to_dict(), f, indent=2)

        LOGGER.info(f"Validation results saved to {out_path}")

    return results
