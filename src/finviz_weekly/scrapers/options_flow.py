"""
Options Flow Scraper for unusual options activity.

Provides:
- Scraping options volume and open interest data
- Detection of unusual activity (volume > 2x average)
- Call/put ratio analysis
- Options sentiment score for screening integration
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
class OptionsFlowConfig:
    """Configuration for options flow analysis."""

    # Unusual activity thresholds
    volume_multiplier: float = 2.0  # Flag if volume > 2x average
    oi_change_threshold: float = 0.1  # Flag if OI changes > 10%

    # Sentiment calculation
    put_call_neutral_low: float = 0.7  # Below = bullish
    put_call_neutral_high: float = 1.0  # Above = bearish

    # Data settings
    lookback_days: int = 30  # Days of historical data
    min_volume: int = 100  # Minimum volume to consider
    min_open_interest: int = 500  # Minimum OI to consider

    # Expiration filtering
    min_dte: int = 7  # Minimum days to expiration
    max_dte: int = 90  # Maximum days to expiration


@dataclass
class OptionsData:
    """Options data for a single ticker."""

    ticker: str
    fetch_date: str

    # Volume metrics
    total_call_volume: int = 0
    total_put_volume: int = 0
    avg_daily_volume: float = 0.0

    # Open interest metrics
    total_call_oi: int = 0
    total_put_oi: int = 0

    # Ratios
    put_call_volume_ratio: float = 1.0
    put_call_oi_ratio: float = 1.0

    # Unusual activity flags
    unusual_call_volume: bool = False
    unusual_put_volume: bool = False
    volume_spike_magnitude: float = 1.0

    # Near-money activity (strikes within 5% of current price)
    near_money_call_volume: int = 0
    near_money_put_volume: int = 0

    # Sentiment score (0-100, higher = more bullish)
    sentiment_score: float = 50.0

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "ticker": self.ticker,
            "fetch_date": self.fetch_date,
            "total_call_volume": self.total_call_volume,
            "total_put_volume": self.total_put_volume,
            "avg_daily_volume": self.avg_daily_volume,
            "total_call_oi": self.total_call_oi,
            "total_put_oi": self.total_put_oi,
            "put_call_volume_ratio": self.put_call_volume_ratio,
            "put_call_oi_ratio": self.put_call_oi_ratio,
            "unusual_call_volume": self.unusual_call_volume,
            "unusual_put_volume": self.unusual_put_volume,
            "volume_spike_magnitude": self.volume_spike_magnitude,
            "near_money_call_volume": self.near_money_call_volume,
            "near_money_put_volume": self.near_money_put_volume,
            "sentiment_score": self.sentiment_score,
        }


class OptionsFlowScraper:
    """
    Scrape and analyze options flow data.

    Uses yfinance to fetch options chain data and calculates
    sentiment indicators based on unusual activity patterns.
    """

    def __init__(self, config: Optional[OptionsFlowConfig] = None):
        """
        Initialize options flow scraper.

        Args:
            config: Scraper configuration
        """
        self.config = config or OptionsFlowConfig()

        if not YFINANCE_AVAILABLE:
            raise ImportError("yfinance is required for options flow scraping")

        LOGGER.info("Options flow scraper initialized")

    def fetch_options_data(self, ticker: str) -> Optional[OptionsData]:
        """
        Fetch and analyze options data for a ticker.

        Args:
            ticker: Stock ticker symbol

        Returns:
            OptionsData with analysis results, or None if fetch fails
        """
        try:
            stock = yf.Ticker(ticker)

            # Get current price for near-money calculations
            hist = stock.history(period="5d")
            if hist.empty:
                LOGGER.warning(f"No price data for {ticker}")
                return None

            current_price = hist["Close"].iloc[-1]

            # Get options expirations
            try:
                expirations = stock.options
            except Exception:
                LOGGER.warning(f"No options data available for {ticker}")
                return None

            if not expirations:
                return None

            # Filter expirations by DTE
            today = datetime.now().date()
            valid_expirations = []
            for exp in expirations:
                exp_date = datetime.strptime(exp, "%Y-%m-%d").date()
                dte = (exp_date - today).days
                if self.config.min_dte <= dte <= self.config.max_dte:
                    valid_expirations.append(exp)

            if not valid_expirations:
                LOGGER.warning(f"No valid expirations for {ticker}")
                return None

            # Aggregate options data across expirations
            total_call_volume = 0
            total_put_volume = 0
            total_call_oi = 0
            total_put_oi = 0
            near_money_call_volume = 0
            near_money_put_volume = 0

            near_money_range = current_price * 0.05  # 5% range

            for exp in valid_expirations[:5]:  # Limit to first 5 expirations
                try:
                    chain = stock.option_chain(exp)

                    # Process calls
                    calls = chain.calls
                    calls = calls[calls["volume"] >= self.config.min_volume]

                    total_call_volume += calls["volume"].sum()
                    total_call_oi += calls["openInterest"].sum()

                    # Near-money calls
                    near_money_calls = calls[
                        (calls["strike"] >= current_price - near_money_range) &
                        (calls["strike"] <= current_price + near_money_range)
                    ]
                    near_money_call_volume += near_money_calls["volume"].sum()

                    # Process puts
                    puts = chain.puts
                    puts = puts[puts["volume"] >= self.config.min_volume]

                    total_put_volume += puts["volume"].sum()
                    total_put_oi += puts["openInterest"].sum()

                    # Near-money puts
                    near_money_puts = puts[
                        (puts["strike"] >= current_price - near_money_range) &
                        (puts["strike"] <= current_price + near_money_range)
                    ]
                    near_money_put_volume += near_money_puts["volume"].sum()

                except Exception as e:
                    LOGGER.debug(f"Error processing {exp} for {ticker}: {e}")
                    continue

            # Calculate ratios
            total_volume = total_call_volume + total_put_volume
            if total_call_volume > 0:
                put_call_volume_ratio = total_put_volume / total_call_volume
            else:
                put_call_volume_ratio = 1.0

            if total_call_oi > 0:
                put_call_oi_ratio = total_put_oi / total_call_oi
            else:
                put_call_oi_ratio = 1.0

            # Estimate average daily volume (simplified)
            avg_daily_volume = total_volume / len(valid_expirations) if valid_expirations else 0

            # Detect unusual activity
            unusual_call_volume = total_call_volume > avg_daily_volume * self.config.volume_multiplier
            unusual_put_volume = total_put_volume > avg_daily_volume * self.config.volume_multiplier

            if avg_daily_volume > 0:
                volume_spike_magnitude = total_volume / avg_daily_volume
            else:
                volume_spike_magnitude = 1.0

            # Calculate sentiment score (0-100)
            sentiment_score = self._calculate_sentiment(
                put_call_volume_ratio,
                put_call_oi_ratio,
                unusual_call_volume,
                unusual_put_volume,
                near_money_call_volume,
                near_money_put_volume,
            )

            return OptionsData(
                ticker=ticker,
                fetch_date=datetime.now().isoformat(),
                total_call_volume=int(total_call_volume),
                total_put_volume=int(total_put_volume),
                avg_daily_volume=float(avg_daily_volume),
                total_call_oi=int(total_call_oi),
                total_put_oi=int(total_put_oi),
                put_call_volume_ratio=float(put_call_volume_ratio),
                put_call_oi_ratio=float(put_call_oi_ratio),
                unusual_call_volume=unusual_call_volume,
                unusual_put_volume=unusual_put_volume,
                volume_spike_magnitude=float(volume_spike_magnitude),
                near_money_call_volume=int(near_money_call_volume),
                near_money_put_volume=int(near_money_put_volume),
                sentiment_score=float(sentiment_score),
            )

        except Exception as e:
            LOGGER.error(f"Error fetching options data for {ticker}: {e}")
            return None

    def _calculate_sentiment(
        self,
        put_call_volume_ratio: float,
        put_call_oi_ratio: float,
        unusual_call_volume: bool,
        unusual_put_volume: bool,
        near_money_call_volume: int,
        near_money_put_volume: int,
    ) -> float:
        """
        Calculate options sentiment score (0-100).

        Higher scores indicate more bullish sentiment.
        """
        score = 50.0  # Neutral starting point

        # Put/Call volume ratio impact (-20 to +20)
        if put_call_volume_ratio < self.config.put_call_neutral_low:
            # Low P/C ratio = bullish
            ratio_score = 20 * (self.config.put_call_neutral_low - put_call_volume_ratio) / self.config.put_call_neutral_low
            score += min(ratio_score, 20)
        elif put_call_volume_ratio > self.config.put_call_neutral_high:
            # High P/C ratio = bearish
            ratio_score = 20 * (put_call_volume_ratio - self.config.put_call_neutral_high) / self.config.put_call_neutral_high
            score -= min(ratio_score, 20)

        # Put/Call OI ratio impact (-10 to +10)
        if put_call_oi_ratio < 0.8:
            score += 10 * (0.8 - put_call_oi_ratio)
        elif put_call_oi_ratio > 1.2:
            score -= 10 * (put_call_oi_ratio - 1.2)

        # Unusual activity impact (-15 to +15)
        if unusual_call_volume and not unusual_put_volume:
            score += 15  # Bullish signal
        elif unusual_put_volume and not unusual_call_volume:
            score -= 15  # Bearish signal

        # Near-money activity impact (-10 to +10)
        total_near_money = near_money_call_volume + near_money_put_volume
        if total_near_money > 0:
            call_ratio = near_money_call_volume / total_near_money
            score += (call_ratio - 0.5) * 20  # -10 to +10

        # Clamp to 0-100
        return max(0.0, min(100.0, score))

    def fetch_batch(
        self,
        tickers: List[str],
        progress_callback: Optional[callable] = None,
    ) -> Dict[str, OptionsData]:
        """
        Fetch options data for multiple tickers.

        Args:
            tickers: List of ticker symbols
            progress_callback: Optional callback for progress updates

        Returns:
            Dict mapping ticker to OptionsData
        """
        results = {}

        for i, ticker in enumerate(tickers):
            if progress_callback:
                progress_callback(i + 1, len(tickers), ticker)

            data = self.fetch_options_data(ticker)
            if data:
                results[ticker] = data

        LOGGER.info(f"Fetched options data for {len(results)}/{len(tickers)} tickers")
        return results

    def to_dataframe(self, options_data: Dict[str, OptionsData]) -> pd.DataFrame:
        """
        Convert options data to DataFrame.

        Args:
            options_data: Dict of ticker -> OptionsData

        Returns:
            DataFrame with options metrics
        """
        records = [data.to_dict() for data in options_data.values()]
        return pd.DataFrame(records)

    def get_unusual_activity(
        self,
        options_data: Dict[str, OptionsData],
        min_volume_spike: float = 2.0,
    ) -> pd.DataFrame:
        """
        Get tickers with unusual options activity.

        Args:
            options_data: Dict of ticker -> OptionsData
            min_volume_spike: Minimum volume spike magnitude

        Returns:
            DataFrame of tickers with unusual activity
        """
        unusual = []

        for ticker, data in options_data.items():
            if data.volume_spike_magnitude >= min_volume_spike:
                unusual.append({
                    "ticker": ticker,
                    "volume_spike": data.volume_spike_magnitude,
                    "unusual_calls": data.unusual_call_volume,
                    "unusual_puts": data.unusual_put_volume,
                    "put_call_ratio": data.put_call_volume_ratio,
                    "sentiment_score": data.sentiment_score,
                    "signal": "BULLISH" if data.sentiment_score > 60 else (
                        "BEARISH" if data.sentiment_score < 40 else "NEUTRAL"
                    ),
                })

        df = pd.DataFrame(unusual)
        if not df.empty:
            df = df.sort_values("volume_spike", ascending=False)

        return df


def calculate_options_sentiment_score(
    data: pd.DataFrame,
    options_data: Dict[str, OptionsData],
) -> pd.DataFrame:
    """
    Add options sentiment score to stock data.

    Args:
        data: DataFrame with stock data (must have 'ticker' column)
        options_data: Dict of ticker -> OptionsData

    Returns:
        DataFrame with added options_sentiment column
    """
    result = data.copy()

    # Create sentiment lookup
    sentiment_map = {
        ticker: opts.sentiment_score
        for ticker, opts in options_data.items()
    }

    # Add sentiment column
    result["options_sentiment"] = result["ticker"].map(sentiment_map)

    # Fill missing with neutral
    result["options_sentiment"] = result["options_sentiment"].fillna(50.0)

    return result
