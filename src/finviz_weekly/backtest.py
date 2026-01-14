"""Backtesting framework for evaluating scoring strategies."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import pandas as pd

LOGGER = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    """Configuration for backtest."""

    start_date: str  # ISO format YYYY-MM-DD
    end_date: str  # ISO format YYYY-MM-DD
    score_column: str = "total_score"  # Column to rank stocks by
    top_n: int = 20  # Number of top-ranked stocks to hold
    rebalance_days: int = 7  # Rebalance every N days
    min_price: float = 5.0  # Minimum price filter
    max_position_size: float = 0.10  # Max 10% per position
    initial_capital: float = 100000.0

    # Enhanced backtesting parameters (Phase 6A)
    commission_pct: float = 0.001  # 0.1% commission per trade (10 bps)
    slippage_bps: float = 5.0  # 5 basis points slippage per trade
    min_volume: float = 100000.0  # Minimum daily volume filter
    min_market_cap: float = 0.0  # Minimum market cap filter (0 = no filter)


@dataclass
class BacktestResults:
    """Results from backtest."""

    config: BacktestConfig
    total_return: float
    annual_return: float
    sharpe_ratio: float
    max_drawdown: float
    num_trades: int
    win_rate: float
    avg_holding_days: float
    portfolio_values: pd.Series
    trades: pd.DataFrame

    # Enhanced metrics (Phase 6A)
    total_commission: float = 0.0  # Total commission costs
    total_slippage: float = 0.0  # Total slippage costs
    return_before_costs: float = 0.0  # Return before transaction costs
    turnover: float = 0.0  # Annual portfolio turnover rate


class Backtester:
    """Backtest scoring strategies using historical data."""

    def __init__(self, history_path: Path, prices_path: Optional[Path] = None):
        """
        Initialize backtester.

        Args:
            history_path: Path to finviz_fundamentals_history.parquet
            prices_path: Optional path to historical prices data
        """
        self.history_path = history_path
        self.prices_path = prices_path

        if not history_path.exists():
            raise FileNotFoundError(f"History file not found: {history_path}")

        LOGGER.info(f"Loading history from {history_path}")
        self.history = pd.read_parquet(history_path)
        self.history["as_of_date"] = pd.to_datetime(self.history["as_of_date"])

    def run(self, config: BacktestConfig) -> BacktestResults:
        """
        Run backtest with given configuration.

        Args:
            config: Backtest configuration

        Returns:
            BacktestResults with performance metrics
        """
        start = pd.to_datetime(config.start_date)
        end = pd.to_datetime(config.end_date)

        # Filter history to date range
        hist_filtered = self.history[
            (self.history["as_of_date"] >= start) & (self.history["as_of_date"] <= end)
        ].copy()

        if len(hist_filtered) == 0:
            raise ValueError(f"No data found between {start} and {end}")

        LOGGER.info(f"Backtesting from {start.date()} to {end.date()}")
        LOGGER.info(f"Found {len(hist_filtered)} rows across {hist_filtered['as_of_date'].nunique()} dates")

        # Get rebalance dates
        rebalance_dates = self._get_rebalance_dates(hist_filtered, config.rebalance_days)
        LOGGER.info(f"Rebalancing on {len(rebalance_dates)} dates")

        # Initialize portfolio and cost tracking (Phase 6A)
        capital = config.initial_capital
        portfolio = {}  # {ticker: shares}
        trades = []
        portfolio_values = []
        total_commission = 0.0
        total_slippage = 0.0
        total_turnover = 0.0

        for date in rebalance_dates:
            # Get snapshot for this date
            snapshot = hist_filtered[hist_filtered["as_of_date"] == date].copy()

            # Apply filters
            snapshot = self._apply_filters(snapshot, config)

            # Get top N by configured score column
            if config.score_column not in snapshot.columns:
                LOGGER.warning(f"No {config.score_column} column on {date}, skipping")
                continue

            top_stocks = snapshot.nlargest(config.top_n, config.score_column)

            # Rebalance portfolio with transaction costs (Phase 6A)
            new_portfolio, new_trades, trade_costs = self._rebalance(
                portfolio, top_stocks, capital, config, date
            )

            # Deduct transaction costs from capital
            capital -= trade_costs["commission"] + trade_costs["slippage"]
            total_commission += trade_costs["commission"]
            total_slippage += trade_costs["slippage"]
            total_turnover += trade_costs["turnover"]

            portfolio = new_portfolio
            trades.extend(new_trades)

            # Calculate portfolio value
            portfolio_value = self._calculate_portfolio_value(portfolio, snapshot, capital)
            portfolio_values.append({"date": date, "value": portfolio_value})

            LOGGER.debug(f"{date.date()}: Portfolio value ${portfolio_value:,.0f}")

        # Calculate metrics with enhanced tracking (Phase 6A)
        pv_df = pd.DataFrame(portfolio_values).set_index("date")["value"]

        # Calculate returns before and after costs
        initial = config.initial_capital
        final = pv_df.iloc[-1]
        return_after_costs = (final - initial) / initial
        return_before_costs = (final + total_commission + total_slippage - initial) / initial

        # Calculate annual turnover
        days = (pd.Timestamp(config.end_date) - pd.Timestamp(config.start_date)).days
        years = days / 365.25
        annual_turnover = total_turnover / years if years > 0 else 0

        results = self._calculate_metrics(pv_df, config)
        results.trades = pd.DataFrame(trades)
        results.total_commission = total_commission
        results.total_slippage = total_slippage
        results.return_before_costs = return_before_costs
        results.turnover = annual_turnover

        LOGGER.info(f"Transaction costs: commission=${total_commission:,.0f} slippage=${total_slippage:,.0f}")
        LOGGER.info(f"Return before costs: {return_before_costs:.1%} after costs: {return_after_costs:.1%}")
        LOGGER.info(f"Annual turnover: {annual_turnover:.1%}")

        return results

    def _get_rebalance_dates(self, hist: pd.DataFrame, days: int) -> List[datetime]:
        """Get dates to rebalance portfolio."""
        unique_dates = sorted(hist["as_of_date"].unique())

        # Take every Nth date
        rebalance_dates = unique_dates[::days]

        return rebalance_dates

    def _apply_filters(self, df: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
        """Apply filters to snapshot (Phase 6A: enhanced with liquidity filters)."""
        # Price filter
        if "price" in df.columns:
            df = df[pd.to_numeric(df["price"], errors="coerce") >= config.min_price]

        # Volume filter (Phase 6A)
        if config.min_volume > 0 and "volume" in df.columns:
            df = df[pd.to_numeric(df["volume"], errors="coerce") >= config.min_volume]

        # Market cap filter (Phase 6A)
        if config.min_market_cap > 0 and "market_cap" in df.columns:
            df = df[pd.to_numeric(df["market_cap"], errors="coerce") >= config.min_market_cap]

        # Remove missing scores
        if config.score_column in df.columns:
            df = df[df[config.score_column].notna()]

        return df

    def _rebalance(
        self,
        current_portfolio: dict,
        target_stocks: pd.DataFrame,
        capital: float,
        config: BacktestConfig,
        date: datetime,
    ) -> tuple[dict, List[dict], dict]:
        """Rebalance portfolio to match target allocation (Phase 6A: enhanced with transaction costs)."""
        new_portfolio = {}
        trades = []
        total_trade_value = 0.0
        total_commission = 0.0
        total_slippage = 0.0

        # Equal weight allocation
        target_tickers = set(target_stocks["ticker"].values)
        position_size = capital * config.max_position_size

        for _, row in target_stocks.iterrows():
            ticker = row["ticker"]
            price = pd.to_numeric(row.get("price", 0), errors="coerce")

            if pd.isna(price) or price <= 0:
                continue

            # Calculate target shares
            target_shares = int(position_size / price)

            if target_shares > 0:
                new_portfolio[ticker] = target_shares

                # Record trade if different from current
                current_shares = current_portfolio.get(ticker, 0)
                if current_shares != target_shares:
                    shares_traded = abs(target_shares - current_shares)
                    trade_value = shares_traded * price

                    # Calculate transaction costs (Phase 6A)
                    commission = trade_value * config.commission_pct
                    slippage = trade_value * (config.slippage_bps / 10000.0)

                    total_trade_value += trade_value
                    total_commission += commission
                    total_slippage += slippage

                    trades.append({
                        "date": date,
                        "ticker": ticker,
                        "action": "BUY" if target_shares > current_shares else "SELL",
                        "shares": shares_traded,
                        "price": price,
                        "commission": commission,
                        "slippage": slippage,
                    })

        # Sell positions not in target
        for ticker in current_portfolio:
            if ticker not in target_tickers:
                # Find price for this ticker on this date
                ticker_data = target_stocks[target_stocks["ticker"] == ticker]
                price = ticker_data["price"].iloc[0] if len(ticker_data) > 0 else None

                if price:
                    shares_traded = current_portfolio[ticker]
                    trade_value = shares_traded * price

                    # Calculate transaction costs (Phase 6A)
                    commission = trade_value * config.commission_pct
                    slippage = trade_value * (config.slippage_bps / 10000.0)

                    total_trade_value += trade_value
                    total_commission += commission
                    total_slippage += slippage

                    trades.append({
                        "date": date,
                        "ticker": ticker,
                        "action": "SELL",
                        "shares": shares_traded,
                        "price": price,
                        "commission": commission,
                        "slippage": slippage,
                    })

        # Calculate turnover (total trade value / portfolio value)
        portfolio_value = capital
        turnover = total_trade_value / portfolio_value if portfolio_value > 0 else 0.0

        costs = {
            "commission": total_commission,
            "slippage": total_slippage,
            "turnover": turnover,
        }

        return new_portfolio, trades, costs

    def _calculate_portfolio_value(
        self, portfolio: dict, snapshot: pd.DataFrame, cash: float
    ) -> float:
        """Calculate current portfolio value."""
        total_value = cash

        for ticker, shares in portfolio.items():
            ticker_data = snapshot[snapshot["ticker"] == ticker]
            if len(ticker_data) > 0:
                price = pd.to_numeric(ticker_data["price"].iloc[0], errors="coerce")
                if pd.notna(price) and price > 0:
                    total_value += shares * price

        return total_value

    def _calculate_metrics(
        self, portfolio_values: pd.Series, config: BacktestConfig
    ) -> BacktestResults:
        """Calculate performance metrics."""
        # Total return
        initial = portfolio_values.iloc[0]
        final = portfolio_values.iloc[-1]
        total_return = (final - initial) / initial

        # Annual return
        days = (portfolio_values.index[-1] - portfolio_values.index[0]).days
        years = days / 365.25
        annual_return = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0

        # Sharpe ratio (simplified: assumes daily returns, risk-free rate = 0)
        returns = portfolio_values.pct_change().dropna()
        sharpe = (returns.mean() / returns.std() * (252 ** 0.5)) if returns.std() > 0 else 0

        # Max drawdown
        cummax = portfolio_values.cummax()
        drawdown = (portfolio_values - cummax) / cummax
        max_drawdown = drawdown.min()

        return BacktestResults(
            config=config,
            total_return=total_return,
            annual_return=annual_return,
            sharpe_ratio=sharpe,
            max_drawdown=max_drawdown,
            num_trades=0,  # Calculated from trades df
            win_rate=0.0,  # Calculated from trades df
            avg_holding_days=0.0,
            portfolio_values=portfolio_values,
            trades=pd.DataFrame(),  # Set later
        )


def run_backtest_cli(
    history_path: str = "data/history/finviz_fundamentals_history.parquet",
    start_date: str = "2024-01-01",
    end_date: str = "2025-01-01",
    top_n: int = 20,
) -> None:
    """
    Run backtest from command line.

    Args:
        history_path: Path to history file
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        top_n: Number of stocks to hold
    """
    import argparse

    parser = argparse.ArgumentParser(description="Backtest scoring strategy")
    parser.add_argument("--history", default=history_path, help="Path to history file")
    parser.add_argument("--start", default=start_date, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=end_date, help="End date (YYYY-MM-DD)")
    parser.add_argument("--top-n", type=int, default=top_n, help="Number of stocks to hold")
    parser.add_argument("--rebalance-days", type=int, default=7, help="Days between rebalances")

    args = parser.parse_args()

    config = BacktestConfig(
        start_date=args.start,
        end_date=args.end,
        top_n=args.top_n,
        rebalance_days=args.rebalance_days,
    )

    backtester = Backtester(Path(args.history))
    results = backtester.run(config)

    print("\n" + "=" * 60)
    print("BACKTEST RESULTS")
    print("=" * 60)
    print(f"Period: {config.start_date} to {config.end_date}")
    print(f"Strategy: Top {config.top_n} stocks, rebalance every {config.rebalance_days} days")
    print()
    print(f"Total Return:    {results.total_return:>8.1%}")
    print(f"Annual Return:   {results.annual_return:>8.1%}")
    print(f"Sharpe Ratio:    {results.sharpe_ratio:>8.2f}")
    print(f"Max Drawdown:    {results.max_drawdown:>8.1%}")
    print(f"Trades:          {results.num_trades:>8}")
    print("=" * 60)


def compare_strategies(
    history_path: Path,
    start_date: str,
    end_date: str,
    strategies: List[tuple[str, str]],
    top_n: int = 20,
    rebalance_days: int = 7,
) -> pd.DataFrame:
    """
    Compare multiple scoring strategies on historical data.

    Args:
        history_path: Path to history file
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        strategies: List of (strategy_name, score_column) tuples
        top_n: Number of stocks to hold
        rebalance_days: Days between rebalances

    Returns:
        DataFrame comparing strategy performance
    """
    backtester = Backtester(history_path)
    results = []

    for strategy_name, score_column in strategies:
        LOGGER.info(f"\nBacktesting strategy: {strategy_name} (using {score_column})")

        config = BacktestConfig(
            start_date=start_date,
            end_date=end_date,
            score_column=score_column,
            top_n=top_n,
            rebalance_days=rebalance_days,
        )

        try:
            result = backtester.run(config)
            results.append({
                "strategy": strategy_name,
                "score_column": score_column,
                "total_return": result.total_return,
                "annual_return": result.annual_return,
                "sharpe_ratio": result.sharpe_ratio,
                "max_drawdown": result.max_drawdown,
                "num_trades": len(result.trades),
            })
        except Exception as e:
            LOGGER.error(f"Failed to backtest {strategy_name}: {e}")
            results.append({
                "strategy": strategy_name,
                "score_column": score_column,
                "total_return": None,
                "annual_return": None,
                "sharpe_ratio": None,
                "max_drawdown": None,
                "num_trades": None,
            })

    return pd.DataFrame(results)


if __name__ == "__main__":
    run_backtest_cli()
