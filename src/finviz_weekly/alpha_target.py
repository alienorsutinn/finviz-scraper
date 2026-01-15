"""
Alpha Target Module for predicting excess returns.

Instead of predicting raw returns, we predict alpha (excess returns)
relative to:
- Market (SPY)
- Sector
- Industry

This removes market/sector noise and focuses on stock-specific alpha.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False

LOGGER = logging.getLogger(__name__)


@dataclass
class AlphaTargetConfig:
    """Configuration for alpha target calculation."""

    # Benchmark settings
    market_benchmark: str = "SPY"  # Market benchmark ticker

    # Return horizons (in trading days)
    horizons: List[int] = None  # Default: [5, 21, 63]

    # Alpha calculation method
    method: str = "excess"  # "excess", "residual", or "information_ratio"

    # Risk adjustment
    risk_adjust: bool = True  # Divide by volatility
    vol_lookback: int = 21  # Days for volatility calculation

    # Winsorization
    winsorize_pct: float = 0.01  # Winsorize at 1st/99th percentile

    def __post_init__(self):
        if self.horizons is None:
            self.horizons = [5, 21, 63]


@dataclass
class AlphaResult:
    """Alpha calculation results for a dataset."""

    horizon: int
    method: str
    num_stocks: int

    # Statistics
    mean_alpha: float
    median_alpha: float
    std_alpha: float
    skew_alpha: float

    # Top/bottom performers
    top_alpha_tickers: List[str]
    bottom_alpha_tickers: List[str]

    # Benchmark comparison
    avg_raw_return: float
    avg_benchmark_return: float
    alpha_vs_raw_correlation: float


class AlphaTargetCalculator:
    """
    Calculate alpha (excess returns) for ML targets.

    This provides better signal-to-noise for prediction by
    removing market/sector beta from returns.
    """

    def __init__(self, config: Optional[AlphaTargetConfig] = None):
        """
        Initialize alpha calculator.

        Args:
            config: Calculator configuration
        """
        self.config = config or AlphaTargetConfig()

        if not YFINANCE_AVAILABLE:
            raise ImportError("yfinance is required for alpha calculation")

        # Cache for benchmark returns
        self._benchmark_cache: Dict[str, pd.Series] = {}
        self._sector_cache: Dict[str, pd.Series] = {}

        LOGGER.info("Alpha target calculator initialized")

    def fetch_benchmark_returns(
        self,
        start_date: str,
        end_date: str,
        benchmark: Optional[str] = None,
    ) -> pd.Series:
        """
        Fetch benchmark returns for the period.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            benchmark: Benchmark ticker (default: SPY)

        Returns:
            Series of daily returns indexed by date
        """
        benchmark = benchmark or self.config.market_benchmark
        cache_key = f"{benchmark}_{start_date}_{end_date}"

        if cache_key in self._benchmark_cache:
            return self._benchmark_cache[cache_key]

        try:
            data = yf.download(
                benchmark,
                start=start_date,
                end=end_date,
                progress=False,
            )

            if data.empty:
                LOGGER.warning(f"No data for benchmark {benchmark}")
                return pd.Series(dtype=float)

            returns = data["Adj Close"].pct_change().dropna()
            self._benchmark_cache[cache_key] = returns

            return returns

        except Exception as e:
            LOGGER.error(f"Error fetching benchmark returns: {e}")
            return pd.Series(dtype=float)

    def calculate_forward_returns(
        self,
        prices: pd.DataFrame,
        horizon: int,
    ) -> pd.DataFrame:
        """
        Calculate forward returns for each stock.

        Args:
            prices: DataFrame with columns as tickers, index as dates
            horizon: Forward return horizon in days

        Returns:
            DataFrame of forward returns
        """
        # Shift prices forward and calculate return
        forward_prices = prices.shift(-horizon)
        returns = (forward_prices - prices) / prices

        return returns

    def calculate_excess_returns(
        self,
        stock_returns: pd.DataFrame,
        benchmark_returns: pd.Series,
    ) -> pd.DataFrame:
        """
        Calculate excess returns over benchmark.

        Args:
            stock_returns: DataFrame of stock returns (tickers as columns)
            benchmark_returns: Series of benchmark returns

        Returns:
            DataFrame of excess returns (alpha)
        """
        # Align dates
        common_dates = stock_returns.index.intersection(benchmark_returns.index)

        if len(common_dates) == 0:
            LOGGER.warning("No overlapping dates between stocks and benchmark")
            return pd.DataFrame()

        stock_aligned = stock_returns.loc[common_dates]
        bench_aligned = benchmark_returns.loc[common_dates]

        # Calculate excess returns
        excess = stock_aligned.sub(bench_aligned, axis=0)

        return excess

    def calculate_sector_excess_returns(
        self,
        stock_returns: pd.DataFrame,
        sector_mapping: Dict[str, str],
        sector_returns: Dict[str, pd.Series],
    ) -> pd.DataFrame:
        """
        Calculate excess returns over sector benchmarks.

        Args:
            stock_returns: DataFrame of stock returns
            sector_mapping: Dict mapping ticker -> sector
            sector_returns: Dict mapping sector -> returns series

        Returns:
            DataFrame of sector-adjusted excess returns
        """
        excess = stock_returns.copy()

        for ticker in stock_returns.columns:
            sector = sector_mapping.get(ticker)
            if sector and sector in sector_returns:
                sector_ret = sector_returns[sector]
                common_dates = stock_returns.index.intersection(sector_ret.index)
                excess.loc[common_dates, ticker] = (
                    stock_returns.loc[common_dates, ticker] -
                    sector_ret.loc[common_dates]
                )

        return excess

    def calculate_residual_returns(
        self,
        stock_returns: pd.DataFrame,
        benchmark_returns: pd.Series,
        lookback: int = 252,
    ) -> pd.DataFrame:
        """
        Calculate residual returns (CAPM alpha).

        Regresses each stock's returns against benchmark and returns
        the residual (idiosyncratic return).

        Args:
            stock_returns: DataFrame of stock returns
            benchmark_returns: Series of benchmark returns
            lookback: Days for rolling regression

        Returns:
            DataFrame of residual returns
        """
        from scipy import stats

        residuals = pd.DataFrame(index=stock_returns.index, columns=stock_returns.columns)

        common_dates = stock_returns.index.intersection(benchmark_returns.index)
        stock_aligned = stock_returns.loc[common_dates]
        bench_aligned = benchmark_returns.loc[common_dates].values

        for ticker in stock_returns.columns:
            stock_ret = stock_aligned[ticker].values

            # Simple OLS regression
            valid_mask = ~(np.isnan(stock_ret) | np.isnan(bench_aligned))
            if valid_mask.sum() < 30:
                continue

            slope, intercept, _, _, _ = stats.linregress(
                bench_aligned[valid_mask],
                stock_ret[valid_mask],
            )

            # Calculate residuals (actual - predicted)
            predicted = intercept + slope * bench_aligned
            residuals.loc[common_dates, ticker] = stock_ret - predicted

        return residuals.astype(float)

    def calculate_risk_adjusted_alpha(
        self,
        alpha: pd.DataFrame,
        vol_lookback: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Calculate risk-adjusted alpha (information ratio style).

        Args:
            alpha: DataFrame of alpha values
            vol_lookback: Lookback period for volatility

        Returns:
            DataFrame of risk-adjusted alpha
        """
        vol_lookback = vol_lookback or self.config.vol_lookback

        # Calculate rolling volatility
        rolling_vol = alpha.rolling(window=vol_lookback).std()

        # Avoid division by zero
        rolling_vol = rolling_vol.replace(0, np.nan)

        # Risk-adjusted alpha
        risk_adj_alpha = alpha / rolling_vol

        return risk_adj_alpha

    def winsorize(self, data: pd.DataFrame, pct: Optional[float] = None) -> pd.DataFrame:
        """
        Winsorize extreme values.

        Args:
            data: DataFrame to winsorize
            pct: Percentile for winsorization (default from config)

        Returns:
            Winsorized DataFrame
        """
        pct = pct or self.config.winsorize_pct

        result = data.copy()

        for col in result.columns:
            lower = result[col].quantile(pct)
            upper = result[col].quantile(1 - pct)
            result[col] = result[col].clip(lower=lower, upper=upper)

        return result

    def calculate_alpha_targets(
        self,
        prices: pd.DataFrame,
        sector_mapping: Optional[Dict[str, str]] = None,
        sector_etfs: Optional[Dict[str, str]] = None,
    ) -> Dict[int, pd.DataFrame]:
        """
        Calculate alpha targets for all configured horizons.

        Args:
            prices: DataFrame with stock prices (columns=tickers, index=dates)
            sector_mapping: Optional dict mapping ticker -> sector
            sector_etfs: Optional dict mapping sector -> ETF ticker

        Returns:
            Dict mapping horizon -> DataFrame of alpha values
        """
        results = {}

        # Get date range
        start_date = prices.index.min().strftime("%Y-%m-%d")
        end_date = (prices.index.max() + timedelta(days=90)).strftime("%Y-%m-%d")

        # Fetch benchmark returns
        benchmark_returns = self.fetch_benchmark_returns(start_date, end_date)

        # Fetch sector returns if provided
        sector_returns = {}
        if sector_etfs:
            for sector, etf in sector_etfs.items():
                sector_returns[sector] = self.fetch_benchmark_returns(
                    start_date, end_date, benchmark=etf
                )

        for horizon in self.config.horizons:
            LOGGER.info(f"Calculating {horizon}-day alpha targets")

            # Calculate forward returns
            forward_returns = self.calculate_forward_returns(prices, horizon)

            # Calculate cumulative benchmark return for horizon
            if not benchmark_returns.empty:
                benchmark_forward = benchmark_returns.rolling(window=horizon).sum()
            else:
                benchmark_forward = pd.Series(0, index=prices.index)

            # Calculate alpha based on method
            if self.config.method == "excess":
                alpha = self.calculate_excess_returns(forward_returns, benchmark_forward)
            elif self.config.method == "residual":
                daily_returns = prices.pct_change()
                alpha = self.calculate_residual_returns(daily_returns, benchmark_returns)
                # Convert to forward-looking
                alpha = alpha.rolling(window=horizon).sum()
            else:
                alpha = forward_returns  # Raw returns

            # Apply sector adjustment if available
            if sector_mapping and sector_returns:
                sector_forward = {}
                for sector, ret in sector_returns.items():
                    sector_forward[sector] = ret.rolling(window=horizon).sum()
                alpha = self.calculate_sector_excess_returns(
                    alpha, sector_mapping, sector_forward
                )

            # Risk adjustment
            if self.config.risk_adjust:
                alpha = self.calculate_risk_adjusted_alpha(alpha)

            # Winsorize
            alpha = self.winsorize(alpha)

            results[horizon] = alpha

        return results

    def prepare_ml_targets(
        self,
        prices: pd.DataFrame,
        features: pd.DataFrame,
        sector_mapping: Optional[Dict[str, str]] = None,
    ) -> pd.DataFrame:
        """
        Prepare ML-ready dataset with alpha targets.

        Args:
            prices: DataFrame with stock prices
            features: DataFrame with features (same index as prices)
            sector_mapping: Optional ticker -> sector mapping

        Returns:
            DataFrame with features and alpha target columns
        """
        # Default sector ETFs
        sector_etfs = {
            "Technology": "XLK",
            "Healthcare": "XLV",
            "Financial": "XLF",
            "Consumer Cyclical": "XLY",
            "Consumer Defensive": "XLP",
            "Energy": "XLE",
            "Industrials": "XLI",
            "Basic Materials": "XLB",
            "Utilities": "XLU",
            "Real Estate": "XLRE",
            "Communication Services": "XLC",
        }

        # Calculate alpha targets
        alpha_targets = self.calculate_alpha_targets(
            prices,
            sector_mapping=sector_mapping,
            sector_etfs=sector_etfs if sector_mapping else None,
        )

        # Merge features with targets
        result = features.copy()

        for horizon, alpha_df in alpha_targets.items():
            col_name = f"alpha_{horizon}d"

            # Stack alpha values to match feature format
            alpha_stacked = alpha_df.stack().reset_index()
            alpha_stacked.columns = ["date", "ticker", col_name]

            # Merge
            if "date" in result.columns and "ticker" in result.columns:
                result = result.merge(
                    alpha_stacked,
                    on=["date", "ticker"],
                    how="left",
                )
            else:
                # Assume result has same structure
                result[col_name] = alpha_stacked[col_name].values[:len(result)]

        return result

    def analyze_alpha_distribution(
        self,
        alpha_targets: Dict[int, pd.DataFrame],
    ) -> Dict[int, AlphaResult]:
        """
        Analyze alpha distribution for each horizon.

        Args:
            alpha_targets: Dict of horizon -> alpha DataFrame

        Returns:
            Dict of horizon -> AlphaResult
        """
        results = {}

        for horizon, alpha_df in alpha_targets.items():
            # Flatten for statistics
            alpha_flat = alpha_df.values.flatten()
            alpha_flat = alpha_flat[~np.isnan(alpha_flat)]

            if len(alpha_flat) == 0:
                continue

            # Get top/bottom performers (most recent date)
            latest = alpha_df.iloc[-1].dropna().sort_values(ascending=False)
            top_tickers = latest.head(5).index.tolist()
            bottom_tickers = latest.tail(5).index.tolist()

            results[horizon] = AlphaResult(
                horizon=horizon,
                method=self.config.method,
                num_stocks=alpha_df.shape[1],
                mean_alpha=float(np.mean(alpha_flat)),
                median_alpha=float(np.median(alpha_flat)),
                std_alpha=float(np.std(alpha_flat)),
                skew_alpha=float(pd.Series(alpha_flat).skew()),
                top_alpha_tickers=top_tickers,
                bottom_alpha_tickers=bottom_tickers,
                avg_raw_return=0.0,  # Would need raw returns to calculate
                avg_benchmark_return=0.0,
                alpha_vs_raw_correlation=0.0,
            )

        return results


def create_alpha_target_column(
    data: pd.DataFrame,
    prices_col: str = "price",
    ticker_col: str = "ticker",
    date_col: str = "as_of_date",
    horizon: int = 21,
    benchmark: str = "SPY",
) -> pd.DataFrame:
    """
    Add alpha target column to existing dataset.

    Convenience function for quick alpha target creation.

    Args:
        data: DataFrame with stock data
        prices_col: Column name for prices
        ticker_col: Column name for tickers
        date_col: Column name for dates
        horizon: Forward return horizon
        benchmark: Benchmark ticker

    Returns:
        DataFrame with added alpha target column
    """
    result = data.copy()

    # Pivot to get prices matrix
    if date_col in data.columns and ticker_col in data.columns:
        prices = data.pivot(index=date_col, columns=ticker_col, values=prices_col)
    else:
        LOGGER.warning("Cannot create alpha target without date and ticker columns")
        return result

    # Calculate alpha
    config = AlphaTargetConfig(horizons=[horizon])
    calculator = AlphaTargetCalculator(config)

    alpha_targets = calculator.calculate_alpha_targets(prices)

    if horizon in alpha_targets:
        alpha_df = alpha_targets[horizon]

        # Stack back to original format
        alpha_stacked = alpha_df.stack().reset_index()
        alpha_stacked.columns = [date_col, ticker_col, f"alpha_{horizon}d"]

        result = result.merge(
            alpha_stacked,
            on=[date_col, ticker_col],
            how="left",
        )

    return result
