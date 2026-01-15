"""
Analyst Estimates Scraper for earnings revisions and price targets.

Provides:
- EPS estimate revisions (current vs 7/30/90 days ago)
- Revenue estimate revisions
- Price target data (mean, high, low)
- Analyst ratings distribution
- Estimate surprise history
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
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
class AnalystEstimatesConfig:
    """Configuration for analyst estimates scraper."""

    # Revision thresholds
    significant_revision_pct: float = 5.0  # Flag revisions > 5%
    strong_revision_pct: float = 10.0  # Strong signal > 10%

    # Price target config
    upside_threshold: float = 20.0  # Flag > 20% upside
    downside_threshold: float = -10.0  # Flag > 10% downside

    # Rating scoring
    strong_buy_score: float = 100.0
    buy_score: float = 75.0
    hold_score: float = 50.0
    sell_score: float = 25.0
    strong_sell_score: float = 0.0


@dataclass
class AnalystData:
    """Analyst estimates data for a single ticker."""

    ticker: str
    fetch_date: str

    # EPS Estimates
    eps_current_year: Optional[float] = None
    eps_next_year: Optional[float] = None
    eps_current_quarter: Optional[float] = None
    eps_next_quarter: Optional[float] = None

    # EPS Revisions (% change)
    eps_revision_7d: Optional[float] = None  # 7-day revision
    eps_revision_30d: Optional[float] = None  # 30-day revision
    eps_revision_90d: Optional[float] = None  # 90-day revision

    # Revenue Estimates
    revenue_current_year: Optional[float] = None
    revenue_next_year: Optional[float] = None
    revenue_growth_estimate: Optional[float] = None

    # Price Targets
    price_target_mean: Optional[float] = None
    price_target_high: Optional[float] = None
    price_target_low: Optional[float] = None
    price_target_median: Optional[float] = None
    num_analysts: int = 0

    # Current price for upside calculation
    current_price: Optional[float] = None
    upside_to_target: Optional[float] = None  # % upside to mean target

    # Analyst Ratings
    strong_buy: int = 0
    buy: int = 0
    hold: int = 0
    sell: int = 0
    strong_sell: int = 0

    # Earnings Surprise History
    avg_surprise_pct: Optional[float] = None  # Average EPS surprise %
    beat_rate: Optional[float] = None  # % of quarters that beat

    # Composite Scores (0-100)
    revision_score: float = 50.0  # Based on EPS revisions
    rating_score: float = 50.0  # Based on analyst ratings
    target_score: float = 50.0  # Based on price target upside
    combined_score: float = 50.0  # Overall analyst sentiment

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "ticker": self.ticker,
            "fetch_date": self.fetch_date,
            "eps_current_year": self.eps_current_year,
            "eps_next_year": self.eps_next_year,
            "eps_current_quarter": self.eps_current_quarter,
            "eps_next_quarter": self.eps_next_quarter,
            "eps_revision_7d": self.eps_revision_7d,
            "eps_revision_30d": self.eps_revision_30d,
            "eps_revision_90d": self.eps_revision_90d,
            "revenue_current_year": self.revenue_current_year,
            "revenue_next_year": self.revenue_next_year,
            "revenue_growth_estimate": self.revenue_growth_estimate,
            "price_target_mean": self.price_target_mean,
            "price_target_high": self.price_target_high,
            "price_target_low": self.price_target_low,
            "price_target_median": self.price_target_median,
            "num_analysts": self.num_analysts,
            "current_price": self.current_price,
            "upside_to_target": self.upside_to_target,
            "strong_buy": self.strong_buy,
            "buy": self.buy,
            "hold": self.hold,
            "sell": self.sell,
            "strong_sell": self.strong_sell,
            "avg_surprise_pct": self.avg_surprise_pct,
            "beat_rate": self.beat_rate,
            "revision_score": self.revision_score,
            "rating_score": self.rating_score,
            "target_score": self.target_score,
            "combined_score": self.combined_score,
        }


class AnalystEstimatesScraper:
    """
    Scrape and analyze analyst estimates data.

    Uses yfinance to fetch analyst data and calculates
    revision momentum and sentiment indicators.
    """

    def __init__(self, config: Optional[AnalystEstimatesConfig] = None):
        """
        Initialize analyst estimates scraper.

        Args:
            config: Scraper configuration
        """
        self.config = config or AnalystEstimatesConfig()

        if not YFINANCE_AVAILABLE:
            raise ImportError("yfinance is required for analyst estimates scraping")

        LOGGER.info("Analyst estimates scraper initialized")

    def fetch_analyst_data(self, ticker: str) -> Optional[AnalystData]:
        """
        Fetch analyst estimates data for a ticker.

        Args:
            ticker: Stock ticker symbol

        Returns:
            AnalystData with analysis, or None if fetch fails
        """
        try:
            stock = yf.Ticker(ticker)
            info = stock.info

            # Get current price
            hist = stock.history(period="5d")
            current_price = hist["Close"].iloc[-1] if not hist.empty else None

            # Extract analyst estimates
            data = AnalystData(
                ticker=ticker,
                fetch_date=datetime.now().isoformat(),
                current_price=current_price,
            )

            # EPS estimates
            data.eps_current_year = info.get("forwardEps")
            data.eps_next_year = info.get("trailingEps")  # Use as proxy

            # Get earnings estimates if available
            try:
                earnings_est = stock.earnings_estimate
                if earnings_est is not None and not earnings_est.empty:
                    if "0q" in earnings_est.columns:
                        data.eps_current_quarter = earnings_est.loc["avg", "0q"] if "avg" in earnings_est.index else None
                    if "+1q" in earnings_est.columns:
                        data.eps_next_quarter = earnings_est.loc["avg", "+1q"] if "avg" in earnings_est.index else None
            except Exception:
                pass

            # Get EPS revisions (trend data)
            try:
                eps_trend = stock.eps_trend
                if eps_trend is not None and not eps_trend.empty:
                    # Calculate revision from trend data
                    if "0y" in eps_trend.columns and "current" in eps_trend.index:
                        current = eps_trend.loc["current", "0y"]
                        if "7daysAgo" in eps_trend.index:
                            days_ago_7 = eps_trend.loc["7daysAgo", "0y"]
                            if days_ago_7 and days_ago_7 != 0:
                                data.eps_revision_7d = ((current - days_ago_7) / abs(days_ago_7)) * 100
                        if "30daysAgo" in eps_trend.index:
                            days_ago_30 = eps_trend.loc["30daysAgo", "0y"]
                            if days_ago_30 and days_ago_30 != 0:
                                data.eps_revision_30d = ((current - days_ago_30) / abs(days_ago_30)) * 100
                        if "90daysAgo" in eps_trend.index:
                            days_ago_90 = eps_trend.loc["90daysAgo", "0y"]
                            if days_ago_90 and days_ago_90 != 0:
                                data.eps_revision_90d = ((current - days_ago_90) / abs(days_ago_90)) * 100
            except Exception:
                pass

            # Revenue estimates
            try:
                revenue_est = stock.revenue_estimate
                if revenue_est is not None and not revenue_est.empty:
                    if "0y" in revenue_est.columns and "avg" in revenue_est.index:
                        data.revenue_current_year = revenue_est.loc["avg", "0y"]
                    if "+1y" in revenue_est.columns and "avg" in revenue_est.index:
                        data.revenue_next_year = revenue_est.loc["avg", "+1y"]
                    if data.revenue_current_year and data.revenue_next_year:
                        data.revenue_growth_estimate = (
                            (data.revenue_next_year - data.revenue_current_year) /
                            data.revenue_current_year * 100
                        )
            except Exception:
                pass

            # Price targets
            data.price_target_mean = info.get("targetMeanPrice")
            data.price_target_high = info.get("targetHighPrice")
            data.price_target_low = info.get("targetLowPrice")
            data.price_target_median = info.get("targetMedianPrice")
            data.num_analysts = info.get("numberOfAnalystOpinions", 0) or 0

            # Calculate upside
            if data.price_target_mean and current_price:
                data.upside_to_target = ((data.price_target_mean - current_price) / current_price) * 100

            # Analyst ratings
            try:
                recommendations = stock.recommendations
                if recommendations is not None and not recommendations.empty:
                    # Get most recent recommendations (last 3 months)
                    recent = recommendations.tail(30)
                    if "To Grade" in recent.columns:
                        grades = recent["To Grade"].str.lower()
                        data.strong_buy = grades.str.contains("strong buy|strongbuy", na=False).sum()
                        data.buy = grades.str.contains("buy|outperform|overweight", na=False).sum() - data.strong_buy
                        data.hold = grades.str.contains("hold|neutral|equal|market perform", na=False).sum()
                        data.sell = grades.str.contains("sell|underperform|underweight", na=False).sum()
                        data.strong_sell = grades.str.contains("strong sell", na=False).sum()
            except Exception:
                pass

            # Try to get recommendation summary from info
            rec_mean = info.get("recommendationMean")  # 1=strong buy, 5=strong sell
            if rec_mean and data.strong_buy == 0 and data.buy == 0:
                # Estimate distribution from mean
                total = data.num_analysts or 10
                if rec_mean <= 1.5:
                    data.strong_buy = int(total * 0.6)
                    data.buy = int(total * 0.3)
                    data.hold = total - data.strong_buy - data.buy
                elif rec_mean <= 2.5:
                    data.buy = int(total * 0.5)
                    data.hold = int(total * 0.3)
                    data.strong_buy = total - data.buy - data.hold
                elif rec_mean <= 3.5:
                    data.hold = int(total * 0.6)
                    data.buy = int(total * 0.2)
                    data.sell = total - data.hold - data.buy
                else:
                    data.sell = int(total * 0.4)
                    data.hold = int(total * 0.4)
                    data.strong_sell = total - data.sell - data.hold

            # Earnings surprise history
            try:
                earnings_hist = stock.earnings_history
                if earnings_hist is not None and not earnings_hist.empty:
                    if "surprisePercent" in earnings_hist.columns:
                        surprises = earnings_hist["surprisePercent"].dropna()
                        if len(surprises) > 0:
                            data.avg_surprise_pct = surprises.mean() * 100
                            data.beat_rate = (surprises > 0).mean() * 100
            except Exception:
                pass

            # Calculate scores
            data.revision_score = self._calculate_revision_score(data)
            data.rating_score = self._calculate_rating_score(data)
            data.target_score = self._calculate_target_score(data)
            data.combined_score = self._calculate_combined_score(data)

            return data

        except Exception as e:
            LOGGER.error(f"Error fetching analyst data for {ticker}: {e}")
            return None

    def _calculate_revision_score(self, data: AnalystData) -> float:
        """
        Calculate revision score (0-100) based on EPS revisions.

        Higher scores indicate positive revision momentum.
        """
        score = 50.0  # Neutral starting point

        # Weight recent revisions more heavily
        weights = {"7d": 0.5, "30d": 0.3, "90d": 0.2}

        revisions = {
            "7d": data.eps_revision_7d,
            "30d": data.eps_revision_30d,
            "90d": data.eps_revision_90d,
        }

        total_weight = 0
        weighted_revision = 0

        for period, revision in revisions.items():
            if revision is not None:
                weight = weights[period]
                weighted_revision += revision * weight
                total_weight += weight

        if total_weight > 0:
            avg_revision = weighted_revision / total_weight

            # Convert revision % to score
            # +10% revision = 100 score, -10% revision = 0 score
            if avg_revision >= self.config.strong_revision_pct:
                score = 100.0
            elif avg_revision <= -self.config.strong_revision_pct:
                score = 0.0
            else:
                # Linear scale between -10% and +10%
                score = 50 + (avg_revision / self.config.strong_revision_pct) * 50

        return max(0.0, min(100.0, score))

    def _calculate_rating_score(self, data: AnalystData) -> float:
        """
        Calculate rating score (0-100) based on analyst recommendations.
        """
        total = data.strong_buy + data.buy + data.hold + data.sell + data.strong_sell

        if total == 0:
            return 50.0  # Neutral if no ratings

        weighted_sum = (
            data.strong_buy * self.config.strong_buy_score +
            data.buy * self.config.buy_score +
            data.hold * self.config.hold_score +
            data.sell * self.config.sell_score +
            data.strong_sell * self.config.strong_sell_score
        )

        return weighted_sum / total

    def _calculate_target_score(self, data: AnalystData) -> float:
        """
        Calculate target score (0-100) based on price target upside.
        """
        if data.upside_to_target is None:
            return 50.0

        upside = data.upside_to_target

        # Scale: -20% = 0, 0% = 50, +40% = 100
        if upside >= 40:
            return 100.0
        elif upside <= -20:
            return 0.0
        else:
            # Linear scale
            return 50 + (upside / 40) * 50

    def _calculate_combined_score(self, data: AnalystData) -> float:
        """
        Calculate combined analyst sentiment score.

        Weights:
        - Revisions: 40% (most predictive)
        - Ratings: 30%
        - Price targets: 30%
        """
        revision_weight = 0.4
        rating_weight = 0.3
        target_weight = 0.3

        # Adjust weights based on data availability
        available_weight = 0
        score = 0

        if data.eps_revision_7d is not None or data.eps_revision_30d is not None:
            score += data.revision_score * revision_weight
            available_weight += revision_weight

        if data.strong_buy + data.buy + data.hold + data.sell > 0:
            score += data.rating_score * rating_weight
            available_weight += rating_weight

        if data.upside_to_target is not None:
            score += data.target_score * target_weight
            available_weight += target_weight

        if available_weight > 0:
            return score / available_weight
        return 50.0

    def fetch_batch(
        self,
        tickers: List[str],
        progress_callback: Optional[callable] = None,
    ) -> Dict[str, AnalystData]:
        """
        Fetch analyst data for multiple tickers.

        Args:
            tickers: List of ticker symbols
            progress_callback: Optional callback for progress updates

        Returns:
            Dict mapping ticker to AnalystData
        """
        results = {}

        for i, ticker in enumerate(tickers):
            if progress_callback:
                progress_callback(i + 1, len(tickers), ticker)

            data = self.fetch_analyst_data(ticker)
            if data:
                results[ticker] = data

        LOGGER.info(f"Fetched analyst data for {len(results)}/{len(tickers)} tickers")
        return results

    def to_dataframe(self, analyst_data: Dict[str, AnalystData]) -> pd.DataFrame:
        """Convert analyst data to DataFrame."""
        records = [data.to_dict() for data in analyst_data.values()]
        return pd.DataFrame(records)

    def get_revision_leaders(
        self,
        analyst_data: Dict[str, AnalystData],
        min_revision_pct: float = 5.0,
        top_n: int = 20,
    ) -> pd.DataFrame:
        """
        Get tickers with strongest positive revisions.

        Args:
            analyst_data: Dict of ticker -> AnalystData
            min_revision_pct: Minimum revision % to include
            top_n: Number of results to return

        Returns:
            DataFrame of revision leaders
        """
        leaders = []

        for ticker, data in analyst_data.items():
            # Use best available revision
            revision = data.eps_revision_7d or data.eps_revision_30d or data.eps_revision_90d
            if revision and revision >= min_revision_pct:
                leaders.append({
                    "ticker": ticker,
                    "revision_7d": data.eps_revision_7d,
                    "revision_30d": data.eps_revision_30d,
                    "revision_90d": data.eps_revision_90d,
                    "revision_score": data.revision_score,
                    "combined_score": data.combined_score,
                })

        df = pd.DataFrame(leaders)
        if not df.empty:
            df = df.sort_values("revision_score", ascending=False).head(top_n)

        return df

    def get_highly_rated(
        self,
        analyst_data: Dict[str, AnalystData],
        min_rating_score: float = 70.0,
        min_analysts: int = 5,
    ) -> pd.DataFrame:
        """
        Get tickers with strong analyst ratings.

        Args:
            analyst_data: Dict of ticker -> AnalystData
            min_rating_score: Minimum rating score
            min_analysts: Minimum number of analyst ratings

        Returns:
            DataFrame of highly rated tickers
        """
        rated = []

        for ticker, data in analyst_data.items():
            total_ratings = data.strong_buy + data.buy + data.hold + data.sell + data.strong_sell
            if data.rating_score >= min_rating_score and total_ratings >= min_analysts:
                rated.append({
                    "ticker": ticker,
                    "rating_score": data.rating_score,
                    "strong_buy": data.strong_buy,
                    "buy": data.buy,
                    "hold": data.hold,
                    "sell": data.sell,
                    "upside_pct": data.upside_to_target,
                    "combined_score": data.combined_score,
                })

        df = pd.DataFrame(rated)
        if not df.empty:
            df = df.sort_values("rating_score", ascending=False)

        return df


def calculate_analyst_score(
    data: pd.DataFrame,
    analyst_data: Dict[str, AnalystData],
) -> pd.DataFrame:
    """
    Add analyst sentiment scores to stock data.

    Args:
        data: DataFrame with stock data (must have 'ticker' column)
        analyst_data: Dict of ticker -> AnalystData

    Returns:
        DataFrame with added analyst score columns
    """
    result = data.copy()

    # Create score lookups
    revision_map = {t: a.revision_score for t, a in analyst_data.items()}
    rating_map = {t: a.rating_score for t, a in analyst_data.items()}
    target_map = {t: a.target_score for t, a in analyst_data.items()}
    combined_map = {t: a.combined_score for t, a in analyst_data.items()}
    upside_map = {t: a.upside_to_target for t, a in analyst_data.items()}

    # Add columns
    result["analyst_revision_score"] = result["ticker"].map(revision_map)
    result["analyst_rating_score"] = result["ticker"].map(rating_map)
    result["analyst_target_score"] = result["ticker"].map(target_map)
    result["analyst_combined_score"] = result["ticker"].map(combined_map)
    result["analyst_upside_pct"] = result["ticker"].map(upside_map)

    # Fill missing with neutral
    for col in ["analyst_revision_score", "analyst_rating_score",
                "analyst_target_score", "analyst_combined_score"]:
        result[col] = result[col].fillna(50.0)

    return result
