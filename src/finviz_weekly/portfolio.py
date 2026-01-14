"""
Portfolio construction tools with risk management and optimization.

Features:
- Sector diversification
- Position sizing with risk management
- Portfolio optimization (maximize Sharpe ratio)
- Rebalancing recommendations
"""
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

LOGGER = logging.getLogger(__name__)


@dataclass
class PortfolioConfig:
    """Configuration for portfolio construction."""

    # Portfolio size
    num_positions: int = 20
    min_position_size: float = 0.02  # 2% minimum
    max_position_size: float = 0.10  # 10% maximum

    # Sector diversification
    max_sector_weight: float = 0.30  # Max 30% in any sector
    min_sectors: int = 3  # At least 3 different sectors

    # Risk management
    target_volatility: Optional[float] = None  # Annualized volatility target
    max_correlation: float = 0.70  # Avoid highly correlated positions

    # Optimization
    optimize_sharpe: bool = False  # Use mean-variance optimization
    risk_free_rate: float = 0.04  # 4% risk-free rate

    # Constraints
    min_market_cap: float = 1e9  # $1B minimum market cap
    min_avg_volume: Optional[float] = None  # Minimum average volume
    max_price: Optional[float] = None  # Maximum price filter


@dataclass
class Position:
    """Represents a single portfolio position."""

    ticker: str
    company: str
    sector: str
    weight: float
    score: float
    market_cap: float
    price: float
    reason: str  # Why this position was selected


@dataclass
class Portfolio:
    """Represents a constructed portfolio."""

    positions: List[Position]
    total_weight: float
    num_sectors: int
    sector_weights: Dict[str, float]
    expected_return: Optional[float] = None
    expected_volatility: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    metadata: Dict = None


class PortfolioBuilder:
    """Build diversified portfolios from screening results."""

    def __init__(self, config: PortfolioConfig):
        """Initialize portfolio builder with configuration."""
        self.config = config

    def build_portfolio(
        self,
        scored_df: pd.DataFrame,
        score_column: str = "score_master",
        returns_data: Optional[pd.DataFrame] = None,
    ) -> Portfolio:
        """
        Build a diversified portfolio from screening results.

        Args:
            scored_df: Scored screening results
            score_column: Column to rank stocks by
            returns_data: Optional historical returns for optimization

        Returns:
            Portfolio object
        """
        # Apply filters
        filtered_df = self._apply_filters(scored_df)

        if len(filtered_df) == 0:
            LOGGER.warning("No stocks passed portfolio filters")
            return Portfolio(positions=[], total_weight=0, num_sectors=0, sector_weights={})

        # Select stocks
        if self.config.optimize_sharpe and returns_data is not None:
            positions = self._build_optimized_portfolio(filtered_df, score_column, returns_data)
        else:
            positions = self._build_equal_weight_portfolio(filtered_df, score_column)

        # Calculate portfolio metrics
        sector_weights = self._calculate_sector_weights(positions)
        num_sectors = len(sector_weights)
        total_weight = sum(p.weight for p in positions)

        portfolio = Portfolio(
            positions=positions,
            total_weight=total_weight,
            num_sectors=num_sectors,
            sector_weights=sector_weights,
        )

        # Calculate expected return/volatility if returns data provided
        if returns_data is not None:
            self._calculate_portfolio_metrics(portfolio, returns_data)

        return portfolio

    def _apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply portfolio filters."""
        result = df.copy()

        # Market cap filter
        if "market_cap" in result.columns:
            result["_mcap_numeric"] = result["market_cap"].apply(self._parse_market_cap)
            result = result[result["_mcap_numeric"] >= self.config.min_market_cap]

        # Volume filter
        if self.config.min_avg_volume and "avg_volume" in result.columns:
            result = result[
                pd.to_numeric(result["avg_volume"], errors="coerce") >= self.config.min_avg_volume
            ]

        # Price filter
        if self.config.max_price and "price" in result.columns:
            result = result[pd.to_numeric(result["price"], errors="coerce") <= self.config.max_price]

        # Remove missing data
        required_cols = ["ticker", "company", "sector"]
        result = result.dropna(subset=[c for c in required_cols if c in result.columns])

        return result

    def _parse_market_cap(self, mcap_str) -> float:
        """Parse market cap string (e.g., '10.5B') to numeric."""
        if pd.isna(mcap_str):
            return 0.0
        try:
            mcap_str = str(mcap_str).replace("$", "").upper()
            if "B" in mcap_str:
                return float(mcap_str.replace("B", "")) * 1e9
            elif "M" in mcap_str:
                return float(mcap_str.replace("M", "")) * 1e6
            elif "K" in mcap_str:
                return float(mcap_str.replace("K", "")) * 1e3
            else:
                return float(mcap_str)
        except (ValueError, AttributeError):
            return 0.0

    def _build_equal_weight_portfolio(self, df: pd.DataFrame, score_column: str) -> List[Position]:
        """Build equal-weighted portfolio with sector constraints."""
        positions = []
        sector_weights = {}

        # Sort by score
        df_sorted = df.sort_values(score_column, ascending=False)

        for _, row in df_sorted.iterrows():
            if len(positions) >= self.config.num_positions:
                break

            ticker = str(row["ticker"])
            sector = str(row.get("sector", "Unknown"))
            score = float(row[score_column])

            # Check sector constraint
            current_sector_weight = sector_weights.get(sector, 0)
            proposed_weight = 1.0 / self.config.num_positions

            if current_sector_weight + proposed_weight <= self.config.max_sector_weight:
                # Add position
                position = Position(
                    ticker=ticker,
                    company=str(row.get("company", "")),
                    sector=sector,
                    weight=proposed_weight,
                    score=score,
                    market_cap=self._parse_market_cap(row.get("market_cap", 0)),
                    price=float(row.get("price", 0)),
                    reason=f"Top {len(positions)+1} by {score_column}",
                )
                positions.append(position)
                sector_weights[sector] = current_sector_weight + proposed_weight

        # Normalize weights
        total_weight = sum(p.weight for p in positions)
        if total_weight > 0:
            for p in positions:
                p.weight /= total_weight

        return positions

    def _build_optimized_portfolio(
        self, df: pd.DataFrame, score_column: str, returns_data: pd.DataFrame
    ) -> List[Position]:
        """Build Sharpe-optimized portfolio using mean-variance optimization."""
        # This is a simplified version - full implementation would use scipy.optimize
        # For now, use a heuristic approach with score weighting

        positions = []
        df_sorted = df.sort_values(score_column, ascending=False).head(self.config.num_positions * 2)

        # Get tickers that have returns data
        available_tickers = [t for t in df_sorted["ticker"] if t in returns_data.columns]

        if len(available_tickers) < self.config.num_positions:
            LOGGER.warning(
                f"Only {len(available_tickers)} tickers have returns data, using equal weight"
            )
            return self._build_equal_weight_portfolio(df, score_column)

        # Calculate expected returns and volatility for each ticker
        ticker_stats = {}
        for ticker in available_tickers:
            if ticker in returns_data.columns:
                returns = returns_data[ticker].dropna()
                if len(returns) > 20:  # Need minimum history
                    expected_return = returns.mean() * 252  # Annualized
                    volatility = returns.std() * np.sqrt(252)  # Annualized
                    sharpe = (
                        (expected_return - self.config.risk_free_rate) / volatility if volatility > 0 else 0
                    )
                    ticker_stats[ticker] = {
                        "expected_return": expected_return,
                        "volatility": volatility,
                        "sharpe": sharpe,
                    }

        if not ticker_stats:
            LOGGER.warning("No valid returns data, using equal weight")
            return self._build_equal_weight_portfolio(df, score_column)

        # Select top Sharpe ratio stocks
        sorted_by_sharpe = sorted(ticker_stats.items(), key=lambda x: x[1]["sharpe"], reverse=True)
        selected_tickers = [t for t, _ in sorted_by_sharpe[: self.config.num_positions]]

        # Weight by Sharpe ratio (capped by sector constraints)
        sector_weights_dict = {}

        for ticker in selected_tickers:
            row = df_sorted[df_sorted["ticker"] == ticker].iloc[0]
            sector = str(row.get("sector", "Unknown"))
            sharpe = ticker_stats[ticker]["sharpe"]

            # Initial weight proportional to Sharpe ratio
            weight = max(sharpe, 0)  # Ensure non-negative

            position = Position(
                ticker=ticker,
                company=str(row.get("company", "")),
                sector=sector,
                weight=weight,  # Will normalize later
                score=float(row[score_column]),
                market_cap=self._parse_market_cap(row.get("market_cap", 0)),
                price=float(row.get("price", 0)),
                reason=f"Sharpe ratio: {sharpe:.2f}",
            )
            positions.append(position)
            sector_weights_dict[sector] = sector_weights_dict.get(sector, 0) + weight

        # Normalize weights
        total_weight = sum(p.weight for p in positions)
        if total_weight > 0:
            for p in positions:
                p.weight /= total_weight

            # Apply min/max position size constraints
            for p in positions:
                p.weight = np.clip(p.weight, self.config.min_position_size, self.config.max_position_size)

            # Re-normalize after clipping
            total_weight = sum(p.weight for p in positions)
            for p in positions:
                p.weight /= total_weight

        return positions

    def _calculate_sector_weights(self, positions: List[Position]) -> Dict[str, float]:
        """Calculate sector weights from positions."""
        sector_weights = {}
        for position in positions:
            sector_weights[position.sector] = sector_weights.get(position.sector, 0) + position.weight
        return sector_weights

    def _calculate_portfolio_metrics(self, portfolio: Portfolio, returns_data: pd.DataFrame):
        """Calculate expected return, volatility, and Sharpe ratio."""
        # Get returns for portfolio tickers
        tickers = [p.ticker for p in portfolio.positions if p.ticker in returns_data.columns]
        weights = np.array([p.weight for p in portfolio.positions if p.ticker in returns_data.columns])

        if len(tickers) == 0:
            return

        returns_subset = returns_data[tickers].dropna()
        if len(returns_subset) < 20:
            return

        # Calculate portfolio expected return
        mean_returns = returns_subset.mean()
        portfolio.expected_return = (mean_returns * weights).sum() * 252  # Annualized

        # Calculate portfolio volatility
        cov_matrix = returns_subset.cov() * 252  # Annualized
        portfolio.expected_volatility = np.sqrt(weights @ cov_matrix @ weights)

        # Calculate Sharpe ratio
        if portfolio.expected_volatility > 0:
            portfolio.sharpe_ratio = (
                portfolio.expected_return - self.config.risk_free_rate
            ) / portfolio.expected_volatility

    def generate_rebalance_recommendations(
        self, current_portfolio: Portfolio, new_screening_results: pd.DataFrame, score_column: str = "score_master"
    ) -> Tuple[List[str], List[str], List[Position]]:
        """
        Generate rebalancing recommendations.

        Args:
            current_portfolio: Current portfolio holdings
            new_screening_results: Latest screening results
            score_column: Score column to use

        Returns:
            Tuple of (tickers_to_sell, tickers_to_buy, new_positions)
        """
        # Build new optimal portfolio
        new_portfolio = self.build_portfolio(new_screening_results, score_column)

        # Compare
        current_tickers = {p.ticker for p in current_portfolio.positions}
        new_tickers = {p.ticker for p in new_portfolio.positions}

        tickers_to_sell = list(current_tickers - new_tickers)
        tickers_to_buy = list(new_tickers - current_tickers)

        return tickers_to_sell, tickers_to_buy, new_portfolio.positions


def print_portfolio_summary(portfolio: Portfolio):
    """Print human-readable portfolio summary."""
    print("\n" + "=" * 80)
    print(f"PORTFOLIO SUMMARY - {len(portfolio.positions)} Positions")
    print("=" * 80)

    if portfolio.expected_return is not None:
        print(f"\nExpected Annual Return: {portfolio.expected_return*100:.1f}%")
    if portfolio.expected_volatility is not None:
        print(f"Expected Volatility:    {portfolio.expected_volatility*100:.1f}%")
    if portfolio.sharpe_ratio is not None:
        print(f"Sharpe Ratio:           {portfolio.sharpe_ratio:.2f}")

    print(f"\nSector Diversification ({portfolio.num_sectors} sectors):")
    for sector, weight in sorted(portfolio.sector_weights.items(), key=lambda x: -x[1]):
        print(f"  {sector:30s} {weight*100:5.1f}%")

    print(f"\nPositions:")
    print(f"{'Ticker':<8} {'Company':<25} {'Sector':<20} {'Weight':<8} {'Score':<8} {'Reason'}")
    print("-" * 100)
    for pos in sorted(portfolio.positions, key=lambda p: -p.weight):
        print(
            f"{pos.ticker:<8} {pos.company[:24]:<25} {pos.sector[:19]:<20} "
            f"{pos.weight*100:6.1f}%  {pos.score:6.1f}  {pos.reason[:30]}"
        )

    print("=" * 80)


if __name__ == "__main__":
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Build portfolio from screening results")
    parser.add_argument("--data-dir", default="data/latest", help="Data directory")
    parser.add_argument("--score", default="score_master", help="Score column to use")
    parser.add_argument("--num-positions", type=int, default=20, help="Number of positions")
    parser.add_argument("--max-sector", type=float, default=0.30, help="Max sector weight")

    args = parser.parse_args()

    # Load data
    scored_path = Path(args.data_dir) / "finviz_scored.parquet"
    if not scored_path.exists():
        print(f"Error: {scored_path} not found")
        exit(1)

    scored_df = pd.read_parquet(scored_path)

    # Build portfolio
    config = PortfolioConfig(
        num_positions=args.num_positions,
        max_sector_weight=args.max_sector,
    )
    builder = PortfolioBuilder(config)
    portfolio = builder.build_portfolio(scored_df, args.score)

    # Print summary
    print_portfolio_summary(portfolio)
