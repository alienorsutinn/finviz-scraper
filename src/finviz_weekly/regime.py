"""
Market regime detection for identifying bull/bear/sideways markets.

Uses SPY (S&P 500 ETF) and VIX for regime classification.
"""
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Optional

import pandas as pd
import numpy as np

LOGGER = logging.getLogger(__name__)


class MarketRegime(Enum):
    """Market regime classification."""

    BULL = "bull"  # Uptrending market
    BEAR = "bear"  # Downtrending market
    SIDEWAYS = "sideways"  # Range-bound market
    VOLATILE = "volatile"  # High volatility, uncertain direction
    UNKNOWN = "unknown"  # Insufficient data


@dataclass
class RegimeAnalysis:
    """Market regime analysis result."""

    current_regime: MarketRegime
    confidence: float  # 0-1
    spy_trend: str  # "up", "down", "flat"
    vix_level: str  # "low", "medium", "high"
    days_in_regime: int
    metadata: Dict


class RegimeDetector:
    """Detect and classify market regimes."""

    def __init__(
        self,
        ma_short: int = 20,  # 20-day MA
        ma_medium: int = 50,  # 50-day MA
        ma_long: int = 200,  # 200-day MA
        vix_low_threshold: float = 15.0,
        vix_high_threshold: float = 25.0,
    ):
        """
        Initialize regime detector with parameters.

        Args:
            ma_short: Short-term moving average period
            ma_medium: Medium-term moving average period
            ma_long: Long-term moving average period
            vix_low_threshold: VIX level below which is considered "low"
            vix_high_threshold: VIX level above which is considered "high"
        """
        self.ma_short = ma_short
        self.ma_medium = ma_medium
        self.ma_long = ma_long
        self.vix_low = vix_low_threshold
        self.vix_high = vix_high_threshold

    def detect_regime(
        self,
        spy_prices: pd.Series,
        vix_prices: Optional[pd.Series] = None,
    ) -> RegimeAnalysis:
        """
        Detect current market regime from SPY and VIX data.

        Args:
            spy_prices: SPY closing prices (time series)
            vix_prices: Optional VIX closing prices

        Returns:
            RegimeAnalysis object
        """
        if len(spy_prices) < self.ma_long:
            LOGGER.warning(f"Insufficient data: {len(spy_prices)} < {self.ma_long} days")
            return RegimeAnalysis(
                current_regime=MarketRegime.UNKNOWN,
                confidence=0.0,
                spy_trend="unknown",
                vix_level="unknown",
                days_in_regime=0,
                metadata={},
            )

        # Calculate moving averages
        spy_prices = spy_prices.sort_index()
        ma_20 = spy_prices.rolling(window=self.ma_short).mean()
        ma_50 = spy_prices.rolling(window=self.ma_medium).mean()
        ma_200 = spy_prices.rolling(window=self.ma_long).mean()

        # Get latest values
        current_price = spy_prices.iloc[-1]
        current_ma_20 = ma_20.iloc[-1]
        current_ma_50 = ma_50.iloc[-1]
        current_ma_200 = ma_200.iloc[-1]

        # Determine trend
        spy_trend = self._classify_trend(current_price, current_ma_20, current_ma_50, current_ma_200)

        # VIX analysis
        vix_level = "unknown"
        current_vix = None
        if vix_prices is not None and len(vix_prices) > 0:
            current_vix = vix_prices.iloc[-1]
            vix_level = self._classify_vix(current_vix)

        # Classify regime
        regime, confidence = self._classify_regime(spy_trend, vix_level)

        # Estimate days in current regime
        days_in_regime = self._estimate_regime_duration(spy_prices, ma_20, ma_50, regime)

        metadata = {
            "spy_price": float(current_price),
            "ma_20": float(current_ma_20),
            "ma_50": float(current_ma_50),
            "ma_200": float(current_ma_200),
            "vix": float(current_vix) if current_vix else None,
        }

        return RegimeAnalysis(
            current_regime=regime,
            confidence=confidence,
            spy_trend=spy_trend,
            vix_level=vix_level,
            days_in_regime=days_in_regime,
            metadata=metadata,
        )

    def _classify_trend(self, price: float, ma_20: float, ma_50: float, ma_200: float) -> str:
        """Classify SPY trend based on moving averages."""
        # Strong uptrend: price > MA20 > MA50 > MA200
        if price > ma_20 > ma_50 > ma_200:
            return "strong_up"

        # Uptrend: price > MA20 and MA50 > MA200
        if price > ma_20 and ma_50 > ma_200:
            return "up"

        # Strong downtrend: price < MA20 < MA50 < MA200
        if price < ma_20 < ma_50 < ma_200:
            return "strong_down"

        # Downtrend: price < MA20 and MA50 < MA200
        if price < ma_20 and ma_50 < ma_200:
            return "down"

        # Sideways/flat
        return "flat"

    def _classify_vix(self, vix: float) -> str:
        """Classify VIX level."""
        if vix < self.vix_low:
            return "low"
        elif vix > self.vix_high:
            return "high"
        else:
            return "medium"

    def _classify_regime(self, spy_trend: str, vix_level: str) -> tuple[MarketRegime, float]:
        """
        Classify market regime from trend and volatility.

        Returns:
            Tuple of (regime, confidence)
        """
        # High volatility regime
        if vix_level == "high":
            if spy_trend in ["strong_down", "down"]:
                return MarketRegime.BEAR, 0.9
            else:
                return MarketRegime.VOLATILE, 0.7

        # Bull market: uptrend with low/medium volatility
        if spy_trend in ["strong_up", "up"]:
            confidence = 0.9 if spy_trend == "strong_up" else 0.7
            return MarketRegime.BULL, confidence

        # Bear market: downtrend
        if spy_trend in ["strong_down", "down"]:
            confidence = 0.9 if spy_trend == "strong_down" else 0.7
            return MarketRegime.BEAR, confidence

        # Sideways market
        if spy_trend == "flat":
            return MarketRegime.SIDEWAYS, 0.6

        return MarketRegime.UNKNOWN, 0.3

    def _estimate_regime_duration(
        self,
        prices: pd.Series,
        ma_20: pd.Series,
        ma_50: pd.Series,
        current_regime: MarketRegime,
    ) -> int:
        """Estimate number of days in current regime."""
        if current_regime == MarketRegime.UNKNOWN:
            return 0

        # Look back to find when regime started
        days = 0
        for i in range(len(prices) - 1, max(0, len(prices) - 200), -1):
            if i < len(ma_20) and i < len(ma_50):
                price = prices.iloc[i]
                ma20 = ma_20.iloc[i]
                ma50 = ma_50.iloc[i]

                # Check if still in same regime
                if current_regime == MarketRegime.BULL:
                    if price < ma20 or ma20 < ma50:
                        break
                elif current_regime == MarketRegime.BEAR:
                    if price > ma20 or ma20 > ma50:
                        break
                elif current_regime == MarketRegime.SIDEWAYS:
                    if abs(price - ma50) / ma50 > 0.05:  # 5% threshold
                        break

                days += 1
            else:
                break

        return days

    def get_regime_from_yfinance(self, start_date: Optional[str] = None) -> RegimeAnalysis:
        """
        Get current market regime using yfinance data.

        Args:
            start_date: Optional start date (YYYY-MM-DD), defaults to 1 year ago

        Returns:
            RegimeAnalysis object
        """
        try:
            import yfinance as yf

            # Default to 1 year of data
            if start_date is None:
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

            # Fetch SPY and VIX data
            spy = yf.Ticker("SPY")
            spy_hist = spy.history(start=start_date)

            vix = yf.Ticker("^VIX")
            vix_hist = vix.history(start=start_date)

            if spy_hist.empty:
                LOGGER.error("Failed to fetch SPY data")
                return RegimeAnalysis(
                    current_regime=MarketRegime.UNKNOWN,
                    confidence=0.0,
                    spy_trend="unknown",
                    vix_level="unknown",
                    days_in_regime=0,
                    metadata={},
                )

            spy_prices = spy_hist["Close"]
            vix_prices = vix_hist["Close"] if not vix_hist.empty else None

            return self.detect_regime(spy_prices, vix_prices)

        except ImportError:
            LOGGER.error("yfinance not installed. Install with: pip install yfinance")
            return RegimeAnalysis(
                current_regime=MarketRegime.UNKNOWN,
                confidence=0.0,
                spy_trend="unknown",
                vix_level="unknown",
                days_in_regime=0,
                metadata={},
            )
        except Exception as e:
            LOGGER.error(f"Failed to detect regime: {e}")
            return RegimeAnalysis(
                current_regime=MarketRegime.UNKNOWN,
                confidence=0.0,
                spy_trend="unknown",
                vix_level="unknown",
                days_in_regime=0,
                metadata={},
            )


def print_regime_analysis(analysis: RegimeAnalysis):
    """Print human-readable regime analysis."""
    print("\n" + "=" * 80)
    print("MARKET REGIME ANALYSIS")
    print("=" * 80)

    # Regime emoji
    regime_emoji = {
        MarketRegime.BULL: "🐂",
        MarketRegime.BEAR: "🐻",
        MarketRegime.SIDEWAYS: "↔️",
        MarketRegime.VOLATILE: "⚡",
        MarketRegime.UNKNOWN: "❓",
    }

    emoji = regime_emoji.get(analysis.current_regime, "")
    print(
        f"\nCurrent Regime: {emoji} {analysis.current_regime.value.upper()} "
        f"(confidence: {analysis.confidence*100:.0f}%)"
    )
    print(f"SPY Trend:      {analysis.spy_trend}")
    print(f"VIX Level:      {analysis.vix_level}")
    print(f"Days in Regime: {analysis.days_in_regime}")

    if analysis.metadata:
        print("\nMarket Data:")
        if "spy_price" in analysis.metadata:
            print(f"  SPY Price:  ${analysis.metadata['spy_price']:.2f}")
        if "ma_20" in analysis.metadata:
            print(f"  MA(20):     ${analysis.metadata['ma_20']:.2f}")
        if "ma_50" in analysis.metadata:
            print(f"  MA(50):     ${analysis.metadata['ma_50']:.2f}")
        if "ma_200" in analysis.metadata:
            print(f"  MA(200):    ${analysis.metadata['ma_200']:.2f}")
        if analysis.metadata.get("vix"):
            print(f"  VIX:        {analysis.metadata['vix']:.2f}")

    # Investment implications
    print("\nInvestment Implications:")
    if analysis.current_regime == MarketRegime.BULL:
        print("  ✓ Favor growth and momentum strategies")
        print("  ✓ Consider increased exposure to equities")
        print("  ✓ Look for breakout opportunities")
    elif analysis.current_regime == MarketRegime.BEAR:
        print("  ⚠ Focus on quality and defensive stocks")
        print("  ⚠ Reduce exposure or hedge positions")
        print("  ⚠ Preserve capital, wait for better entry points")
    elif analysis.current_regime == MarketRegime.SIDEWAYS:
        print("  → Range-trading strategies may work")
        print("  → Focus on stock-picking over market timing")
        print("  → Consider sector rotation")
    elif analysis.current_regime == MarketRegime.VOLATILE:
        print("  ⚡ Reduce position sizes")
        print("  ⚡ Focus on high-conviction ideas")
        print("  ⚡ Consider hedging strategies")

    print("=" * 80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Detect current market regime")
    parser.add_argument("--start-date", help="Start date for analysis (YYYY-MM-DD)")

    args = parser.parse_args()

    detector = RegimeDetector()
    analysis = detector.get_regime_from_yfinance(args.start_date)

    print_regime_analysis(analysis)
