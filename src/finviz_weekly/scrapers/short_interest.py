"""
Short Interest Tracking for contrarian signals.

Provides:
- Short interest data fetching
- Short interest ratio (SI/float) calculation
- Short squeeze detection
- Short interest scoring for screening integration
"""
from __future__ import annotations

import logging
import re
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

try:
    import requests
    from bs4 import BeautifulSoup
    SCRAPING_AVAILABLE = True
except ImportError:
    SCRAPING_AVAILABLE = False

LOGGER = logging.getLogger(__name__)


@dataclass
class ShortInterestConfig:
    """Configuration for short interest tracking."""

    # Short squeeze thresholds
    high_short_interest_pct: float = 15.0  # >15% considered high
    extreme_short_interest_pct: float = 25.0  # >25% considered extreme
    days_to_cover_threshold: float = 5.0  # >5 days considered high

    # Scoring parameters
    contrarian_weight: float = 0.6  # Weight for contrarian signal
    squeeze_bonus: float = 20.0  # Bonus points for squeeze candidates

    # Data freshness
    max_data_age_days: int = 14  # Maximum age of short data


@dataclass
class ShortInterestData:
    """Short interest data for a single ticker."""

    ticker: str
    fetch_date: str

    # Core metrics
    short_interest: int = 0  # Number of shares short
    shares_float: int = 0  # Total shares in float
    shares_outstanding: int = 0  # Total shares outstanding

    # Calculated ratios
    short_percent_float: float = 0.0  # SI / Float
    short_percent_outstanding: float = 0.0  # SI / Outstanding
    days_to_cover: float = 0.0  # SI / Avg Daily Volume

    # Historical context
    short_interest_change_pct: float = 0.0  # Change from previous report
    short_interest_trend: str = "stable"  # increasing, decreasing, stable

    # Squeeze indicators
    is_squeeze_candidate: bool = False
    squeeze_score: float = 0.0  # 0-100

    # Overall score (0-100, higher = more interesting for contrarian)
    contrarian_score: float = 50.0

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "ticker": self.ticker,
            "fetch_date": self.fetch_date,
            "short_interest": self.short_interest,
            "shares_float": self.shares_float,
            "shares_outstanding": self.shares_outstanding,
            "short_percent_float": self.short_percent_float,
            "short_percent_outstanding": self.short_percent_outstanding,
            "days_to_cover": self.days_to_cover,
            "short_interest_change_pct": self.short_interest_change_pct,
            "short_interest_trend": self.short_interest_trend,
            "is_squeeze_candidate": self.is_squeeze_candidate,
            "squeeze_score": self.squeeze_score,
            "contrarian_score": self.contrarian_score,
        }


class ShortInterestTracker:
    """
    Track and analyze short interest data.

    Uses yfinance for basic short data and calculates
    squeeze potential and contrarian signals.
    """

    def __init__(self, config: Optional[ShortInterestConfig] = None):
        """
        Initialize short interest tracker.

        Args:
            config: Tracker configuration
        """
        self.config = config or ShortInterestConfig()

        if not YFINANCE_AVAILABLE:
            raise ImportError("yfinance is required for short interest tracking")

        LOGGER.info("Short interest tracker initialized")

    def fetch_short_data(self, ticker: str) -> Optional[ShortInterestData]:
        """
        Fetch short interest data for a ticker.

        Args:
            ticker: Stock ticker symbol

        Returns:
            ShortInterestData with analysis, or None if fetch fails
        """
        try:
            stock = yf.Ticker(ticker)
            info = stock.info

            # Extract short interest metrics
            short_interest = info.get("sharesShort", 0) or 0
            shares_float = info.get("floatShares", 0) or 0
            shares_outstanding = info.get("sharesOutstanding", 0) or 0
            short_percent_float = info.get("shortPercentOfFloat", 0) or 0
            days_to_cover = info.get("shortRatio", 0) or 0

            # Previous short interest for trend
            prev_short = info.get("sharesShortPriorMonth", 0) or 0

            # Calculate percentages if not provided
            if short_percent_float == 0 and shares_float > 0:
                short_percent_float = (short_interest / shares_float) * 100

            short_percent_outstanding = 0.0
            if shares_outstanding > 0:
                short_percent_outstanding = (short_interest / shares_outstanding) * 100

            # Calculate change from previous report
            if prev_short > 0:
                short_interest_change_pct = ((short_interest - prev_short) / prev_short) * 100
            else:
                short_interest_change_pct = 0.0

            # Determine trend
            if short_interest_change_pct > 5:
                short_interest_trend = "increasing"
            elif short_interest_change_pct < -5:
                short_interest_trend = "decreasing"
            else:
                short_interest_trend = "stable"

            # Check for squeeze candidate
            is_squeeze_candidate = self._is_squeeze_candidate(
                short_percent_float,
                days_to_cover,
                short_interest_trend,
            )

            # Calculate squeeze score
            squeeze_score = self._calculate_squeeze_score(
                short_percent_float,
                days_to_cover,
                short_interest_change_pct,
            )

            # Calculate contrarian score
            contrarian_score = self._calculate_contrarian_score(
                short_percent_float,
                days_to_cover,
                is_squeeze_candidate,
                squeeze_score,
            )

            return ShortInterestData(
                ticker=ticker,
                fetch_date=datetime.now().isoformat(),
                short_interest=int(short_interest),
                shares_float=int(shares_float),
                shares_outstanding=int(shares_outstanding),
                short_percent_float=float(short_percent_float),
                short_percent_outstanding=float(short_percent_outstanding),
                days_to_cover=float(days_to_cover),
                short_interest_change_pct=float(short_interest_change_pct),
                short_interest_trend=short_interest_trend,
                is_squeeze_candidate=is_squeeze_candidate,
                squeeze_score=float(squeeze_score),
                contrarian_score=float(contrarian_score),
            )

        except Exception as e:
            LOGGER.error(f"Error fetching short data for {ticker}: {e}")
            return None

    def _is_squeeze_candidate(
        self,
        short_percent_float: float,
        days_to_cover: float,
        trend: str,
    ) -> bool:
        """
        Determine if stock is a short squeeze candidate.

        Criteria:
        - High short interest (>15% of float)
        - High days to cover (>5 days)
        - Increasing short interest (shorts are adding)
        """
        has_high_short = short_percent_float >= self.config.high_short_interest_pct
        has_high_dtc = days_to_cover >= self.config.days_to_cover_threshold

        return has_high_short and has_high_dtc

    def _calculate_squeeze_score(
        self,
        short_percent_float: float,
        days_to_cover: float,
        change_pct: float,
    ) -> float:
        """
        Calculate short squeeze potential score (0-100).

        Higher scores indicate higher squeeze potential.
        """
        score = 0.0

        # Short percent contribution (0-40 points)
        if short_percent_float >= self.config.extreme_short_interest_pct:
            score += 40
        elif short_percent_float >= self.config.high_short_interest_pct:
            ratio = (short_percent_float - self.config.high_short_interest_pct) / (
                self.config.extreme_short_interest_pct - self.config.high_short_interest_pct
            )
            score += 20 + (20 * ratio)
        elif short_percent_float >= 10:
            score += (short_percent_float - 10) * 4  # 0-20 points

        # Days to cover contribution (0-30 points)
        if days_to_cover >= 10:
            score += 30
        elif days_to_cover >= self.config.days_to_cover_threshold:
            ratio = (days_to_cover - self.config.days_to_cover_threshold) / (
                10 - self.config.days_to_cover_threshold
            )
            score += 15 + (15 * ratio)
        elif days_to_cover >= 2:
            score += (days_to_cover - 2) * 5  # 0-15 points

        # Change momentum contribution (0-30 points)
        if change_pct > 20:
            score += 30  # Rapidly increasing short interest
        elif change_pct > 10:
            score += 20 + ((change_pct - 10) * 1)
        elif change_pct > 0:
            score += change_pct * 2  # 0-20 points

        return min(100.0, score)

    def _calculate_contrarian_score(
        self,
        short_percent_float: float,
        days_to_cover: float,
        is_squeeze_candidate: bool,
        squeeze_score: float,
    ) -> float:
        """
        Calculate contrarian opportunity score (0-100).

        Higher scores indicate more interesting contrarian opportunities.
        """
        score = 50.0  # Neutral starting point

        # High short interest is a contrarian bullish signal
        if short_percent_float >= self.config.extreme_short_interest_pct:
            score += 25
        elif short_percent_float >= self.config.high_short_interest_pct:
            score += 15
        elif short_percent_float >= 10:
            score += 5

        # Days to cover adds to contrarian score
        if days_to_cover >= 7:
            score += 15
        elif days_to_cover >= self.config.days_to_cover_threshold:
            score += 10
        elif days_to_cover >= 3:
            score += 5

        # Squeeze candidate bonus
        if is_squeeze_candidate:
            score += self.config.squeeze_bonus

        # Cap at 100
        return min(100.0, max(0.0, score))

    def fetch_batch(
        self,
        tickers: List[str],
        progress_callback: Optional[callable] = None,
    ) -> Dict[str, ShortInterestData]:
        """
        Fetch short interest data for multiple tickers.

        Args:
            tickers: List of ticker symbols
            progress_callback: Optional callback for progress updates

        Returns:
            Dict mapping ticker to ShortInterestData
        """
        results = {}

        for i, ticker in enumerate(tickers):
            if progress_callback:
                progress_callback(i + 1, len(tickers), ticker)

            data = self.fetch_short_data(ticker)
            if data:
                results[ticker] = data

        LOGGER.info(f"Fetched short interest data for {len(results)}/{len(tickers)} tickers")
        return results

    def to_dataframe(self, short_data: Dict[str, ShortInterestData]) -> pd.DataFrame:
        """
        Convert short interest data to DataFrame.

        Args:
            short_data: Dict of ticker -> ShortInterestData

        Returns:
            DataFrame with short interest metrics
        """
        records = [data.to_dict() for data in short_data.values()]
        return pd.DataFrame(records)

    def get_squeeze_candidates(
        self,
        short_data: Dict[str, ShortInterestData],
        min_squeeze_score: float = 50.0,
    ) -> pd.DataFrame:
        """
        Get tickers with high short squeeze potential.

        Args:
            short_data: Dict of ticker -> ShortInterestData
            min_squeeze_score: Minimum squeeze score to include

        Returns:
            DataFrame of squeeze candidates sorted by score
        """
        candidates = []

        for ticker, data in short_data.items():
            if data.squeeze_score >= min_squeeze_score or data.is_squeeze_candidate:
                candidates.append({
                    "ticker": ticker,
                    "short_percent_float": data.short_percent_float,
                    "days_to_cover": data.days_to_cover,
                    "short_change_pct": data.short_interest_change_pct,
                    "trend": data.short_interest_trend,
                    "squeeze_score": data.squeeze_score,
                    "contrarian_score": data.contrarian_score,
                })

        df = pd.DataFrame(candidates)
        if not df.empty:
            df = df.sort_values("squeeze_score", ascending=False)

        return df

    def get_high_short_interest(
        self,
        short_data: Dict[str, ShortInterestData],
        min_short_pct: float = 10.0,
    ) -> pd.DataFrame:
        """
        Get tickers with high short interest.

        Args:
            short_data: Dict of ticker -> ShortInterestData
            min_short_pct: Minimum short percent of float

        Returns:
            DataFrame of high short interest tickers
        """
        high_short = []

        for ticker, data in short_data.items():
            if data.short_percent_float >= min_short_pct:
                high_short.append({
                    "ticker": ticker,
                    "short_percent_float": data.short_percent_float,
                    "short_percent_outstanding": data.short_percent_outstanding,
                    "days_to_cover": data.days_to_cover,
                    "trend": data.short_interest_trend,
                    "contrarian_score": data.contrarian_score,
                })

        df = pd.DataFrame(high_short)
        if not df.empty:
            df = df.sort_values("short_percent_float", ascending=False)

        return df


def calculate_short_interest_score(
    data: pd.DataFrame,
    short_data: Dict[str, ShortInterestData],
) -> pd.DataFrame:
    """
    Add short interest contrarian score to stock data.

    Args:
        data: DataFrame with stock data (must have 'ticker' column)
        short_data: Dict of ticker -> ShortInterestData

    Returns:
        DataFrame with added short_contrarian_score column
    """
    result = data.copy()

    # Create score lookup
    score_map = {
        ticker: si.contrarian_score
        for ticker, si in short_data.items()
    }

    # Create squeeze candidate lookup
    squeeze_map = {
        ticker: si.is_squeeze_candidate
        for ticker, si in short_data.items()
    }

    # Add columns
    result["short_contrarian_score"] = result["ticker"].map(score_map)
    result["is_squeeze_candidate"] = result["ticker"].map(squeeze_map)

    # Fill missing with neutral
    result["short_contrarian_score"] = result["short_contrarian_score"].fillna(50.0)
    result["is_squeeze_candidate"] = result["is_squeeze_candidate"].fillna(False)

    return result
