"""
Institutional Ownership Scraper

Tracks institutional holdings from 13F filings:
- Top institutional holders
- Ownership percentage changes
- New positions and exits
- Hedge fund vs mutual fund breakdown
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import time

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class InstitutionalConfig:
    """Configuration for institutional ownership scraping."""

    min_holders: int = 5  # Minimum institutional holders
    significant_change_pct: float = 10.0  # Significant ownership change threshold
    new_position_weight: float = 2.0  # Weight for new positions
    exit_penalty: float = -1.5  # Penalty for institutional exits
    request_delay: float = 0.5  # Delay between API calls
    cache_hours: int = 24  # Cache duration


@dataclass
class InstitutionalHolder:
    """Single institutional holder data."""

    holder_name: str
    shares: int
    date_reported: str
    pct_held: float
    value: float
    pct_change: Optional[float] = None  # Change from previous quarter


@dataclass
class InstitutionalData:
    """Institutional ownership data for a ticker."""

    ticker: str
    fetch_date: str

    # Ownership metrics
    institutional_pct: float = 0.0  # % held by institutions
    num_holders: int = 0
    shares_outstanding: Optional[int] = None

    # Change metrics
    net_institutional_change: float = 0.0  # Net % change in institutional ownership
    new_positions: int = 0  # Number of new institutional positions
    closed_positions: int = 0  # Number of exits
    increased_positions: int = 0
    decreased_positions: int = 0

    # Top holders
    top_holders: List[InstitutionalHolder] = field(default_factory=list)

    # Calculated scores
    ownership_score: float = 50.0  # 0-100
    momentum_score: float = 50.0  # Based on changes
    conviction_score: float = 50.0  # Based on position sizing
    combined_score: float = 50.0


class InstitutionalOwnershipScraper:
    """Scrape institutional ownership data."""

    def __init__(self, config: Optional[InstitutionalConfig] = None):
        self.config = config or InstitutionalConfig()
        self._cache: Dict[str, Tuple[datetime, InstitutionalData]] = {}

    def _is_cached(self, ticker: str) -> bool:
        """Check if data is cached and valid."""
        if ticker not in self._cache:
            return False
        cache_time, _ = self._cache[ticker]
        return datetime.now() - cache_time < timedelta(hours=self.config.cache_hours)

    def _get_cached(self, ticker: str) -> Optional[InstitutionalData]:
        """Get cached data if valid."""
        if self._is_cached(ticker):
            return self._cache[ticker][1]
        return None

    def _set_cache(self, ticker: str, data: InstitutionalData):
        """Cache the data."""
        self._cache[ticker] = (datetime.now(), data)

    def fetch_institutional_data(self, ticker: str) -> InstitutionalData:
        """Fetch institutional ownership data for a ticker."""
        # Check cache
        cached = self._get_cached(ticker)
        if cached:
            logger.debug(f"Using cached data for {ticker}")
            return cached

        try:
            import yfinance as yf

            stock = yf.Ticker(ticker)
            data = InstitutionalData(
                ticker=ticker,
                fetch_date=datetime.now().strftime("%Y-%m-%d")
            )

            # Get institutional holders
            try:
                holders_df = stock.institutional_holders
                if holders_df is not None and not holders_df.empty:
                    data.num_holders = len(holders_df)

                    # Parse top holders
                    for _, row in holders_df.head(10).iterrows():
                        holder = InstitutionalHolder(
                            holder_name=str(row.get('Holder', 'Unknown')),
                            shares=int(row.get('Shares', 0)),
                            date_reported=str(row.get('Date Reported', '')),
                            pct_held=float(row.get('% Out', 0) if pd.notna(row.get('% Out')) else 0),
                            value=float(row.get('Value', 0) if pd.notna(row.get('Value')) else 0),
                            pct_change=float(row.get('% Change', 0)) if pd.notna(row.get('% Change')) else None
                        )
                        data.top_holders.append(holder)

                    # Calculate aggregate metrics
                    if '% Out' in holders_df.columns:
                        data.institutional_pct = holders_df['% Out'].sum() * 100

                    # Analyze changes
                    if '% Change' in holders_df.columns:
                        changes = holders_df['% Change'].dropna()
                        if len(changes) > 0:
                            data.increased_positions = (changes > 0).sum()
                            data.decreased_positions = (changes < 0).sum()
                            data.net_institutional_change = changes.mean() * 100

                            # New positions (very large increases)
                            data.new_positions = (changes > 100).sum()  # >100% increase = likely new
                            data.closed_positions = (changes < -90).sum()  # >90% decrease = likely exit

            except Exception as e:
                logger.warning(f"Error fetching institutional holders for {ticker}: {e}")

            # Get shares outstanding for context
            try:
                info = stock.info
                if info:
                    data.shares_outstanding = info.get('sharesOutstanding')
            except Exception as e:
                logger.debug(f"Could not get shares outstanding for {ticker}: {e}")

            # Calculate scores
            data = self._calculate_scores(data)

            # Cache the result
            self._set_cache(ticker, data)

            time.sleep(self.config.request_delay)
            return data

        except Exception as e:
            logger.error(f"Error fetching institutional data for {ticker}: {e}")
            return InstitutionalData(
                ticker=ticker,
                fetch_date=datetime.now().strftime("%Y-%m-%d")
            )

    def _calculate_scores(self, data: InstitutionalData) -> InstitutionalData:
        """Calculate institutional ownership scores."""

        # Ownership score (higher institutional ownership = more confidence)
        # Sweet spot is 40-80% institutional ownership
        if data.institutional_pct > 0:
            if data.institutional_pct < 20:
                data.ownership_score = 30 + data.institutional_pct  # Low institutional = lower score
            elif data.institutional_pct < 40:
                data.ownership_score = 50 + (data.institutional_pct - 20)
            elif data.institutional_pct < 80:
                data.ownership_score = 70 + (data.institutional_pct - 40) * 0.5
            else:
                data.ownership_score = 90 - (data.institutional_pct - 80) * 0.5  # Too high can be crowded

        # Momentum score (based on position changes)
        momentum_base = 50.0
        if data.increased_positions > 0 or data.decreased_positions > 0:
            net_direction = data.increased_positions - data.decreased_positions
            total_changes = data.increased_positions + data.decreased_positions
            if total_changes > 0:
                direction_ratio = net_direction / total_changes
                momentum_base += direction_ratio * 30  # +/- 30 points

        # Add bonus for new positions, penalty for exits
        momentum_base += data.new_positions * self.config.new_position_weight
        momentum_base += data.closed_positions * self.config.exit_penalty

        # Factor in net change percentage
        if data.net_institutional_change != 0:
            change_contribution = min(max(data.net_institutional_change, -20), 20)  # Cap at +/- 20%
            momentum_base += change_contribution

        data.momentum_score = max(0, min(100, momentum_base))

        # Conviction score (based on concentration and size)
        if data.top_holders:
            # Higher conviction if top holders have significant positions
            top_holder_pct = sum(h.pct_held for h in data.top_holders[:5])
            data.conviction_score = min(100, 30 + top_holder_pct * 100)

        # Combined score (weighted average)
        data.combined_score = (
            data.ownership_score * 0.3 +
            data.momentum_score * 0.5 +  # Momentum most important
            data.conviction_score * 0.2
        )

        return data

    def fetch_batch(self, tickers: List[str],
                    show_progress: bool = True) -> Dict[str, InstitutionalData]:
        """Fetch institutional data for multiple tickers."""
        results = {}

        for i, ticker in enumerate(tickers):
            if show_progress and i % 10 == 0:
                logger.info(f"Fetching institutional data: {i}/{len(tickers)}")

            results[ticker] = self.fetch_institutional_data(ticker)

        return results

    def get_top_institutional_picks(self,
                                    data: Dict[str, InstitutionalData],
                                    min_holders: Optional[int] = None,
                                    top_n: int = 20) -> pd.DataFrame:
        """Get top stocks by institutional momentum."""
        min_holders = min_holders or self.config.min_holders

        rows = []
        for ticker, inst_data in data.items():
            if inst_data.num_holders >= min_holders:
                rows.append({
                    'ticker': ticker,
                    'institutional_pct': inst_data.institutional_pct,
                    'num_holders': inst_data.num_holders,
                    'net_change': inst_data.net_institutional_change,
                    'new_positions': inst_data.new_positions,
                    'increased': inst_data.increased_positions,
                    'decreased': inst_data.decreased_positions,
                    'ownership_score': inst_data.ownership_score,
                    'momentum_score': inst_data.momentum_score,
                    'combined_score': inst_data.combined_score
                })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        return df.nlargest(top_n, 'combined_score')

    def detect_accumulation(self,
                           data: Dict[str, InstitutionalData],
                           min_new_positions: int = 2,
                           min_increase_ratio: float = 0.6) -> List[str]:
        """Detect stocks being accumulated by institutions."""
        accumulating = []

        for ticker, inst_data in data.items():
            total_changes = inst_data.increased_positions + inst_data.decreased_positions
            if total_changes == 0:
                continue

            increase_ratio = inst_data.increased_positions / total_changes

            if (inst_data.new_positions >= min_new_positions or
                (increase_ratio >= min_increase_ratio and inst_data.net_institutional_change > 5)):
                accumulating.append(ticker)

        return accumulating

    def detect_distribution(self,
                           data: Dict[str, InstitutionalData],
                           min_exits: int = 2,
                           min_decrease_ratio: float = 0.6) -> List[str]:
        """Detect stocks being distributed (sold) by institutions."""
        distributing = []

        for ticker, inst_data in data.items():
            total_changes = inst_data.increased_positions + inst_data.decreased_positions
            if total_changes == 0:
                continue

            decrease_ratio = inst_data.decreased_positions / total_changes

            if (inst_data.closed_positions >= min_exits or
                (decrease_ratio >= min_decrease_ratio and inst_data.net_institutional_change < -5)):
                distributing.append(ticker)

        return distributing


def calculate_institutional_score(data: InstitutionalData) -> float:
    """Calculate a simple institutional score for integration with other factors."""
    return data.combined_score


def create_institutional_features(data: Dict[str, InstitutionalData]) -> pd.DataFrame:
    """Create features for ML model from institutional data."""
    rows = []

    for ticker, inst_data in data.items():
        rows.append({
            'ticker': ticker,
            'inst_pct': inst_data.institutional_pct,
            'inst_holders': inst_data.num_holders,
            'inst_net_change': inst_data.net_institutional_change,
            'inst_new_positions': inst_data.new_positions,
            'inst_exits': inst_data.closed_positions,
            'inst_buyers_ratio': (
                inst_data.increased_positions /
                max(1, inst_data.increased_positions + inst_data.decreased_positions)
            ),
            'inst_ownership_score': inst_data.ownership_score,
            'inst_momentum_score': inst_data.momentum_score,
            'inst_conviction_score': inst_data.conviction_score,
            'inst_combined_score': inst_data.combined_score
        })

    return pd.DataFrame(rows).set_index('ticker') if rows else pd.DataFrame()
