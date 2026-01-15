"""Tests for options flow scraper."""
from __future__ import annotations

import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch


class TestOptionsFlowConfig:
    """Tests for OptionsFlowConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        from finviz_weekly.scrapers.options_flow import OptionsFlowConfig

        config = OptionsFlowConfig()

        assert config.volume_multiplier == 2.0
        assert config.min_dte == 7
        assert config.max_dte == 90
        assert config.put_call_neutral_low == 0.7
        assert config.put_call_neutral_high == 1.0

    def test_custom_config(self):
        """Test custom configuration."""
        from finviz_weekly.scrapers.options_flow import OptionsFlowConfig

        config = OptionsFlowConfig(
            volume_multiplier=3.0,
            min_dte=14,
            max_dte=60,
        )

        assert config.volume_multiplier == 3.0
        assert config.min_dte == 14
        assert config.max_dte == 60


class TestOptionsData:
    """Tests for OptionsData dataclass."""

    def test_options_data_creation(self):
        """Test creating OptionsData."""
        from finviz_weekly.scrapers.options_flow import OptionsData

        data = OptionsData(
            ticker="AAPL",
            fetch_date="2025-01-15T10:00:00",
            total_call_volume=10000,
            total_put_volume=5000,
            put_call_volume_ratio=0.5,
            sentiment_score=65.0,
        )

        assert data.ticker == "AAPL"
        assert data.total_call_volume == 10000
        assert data.total_put_volume == 5000
        assert data.put_call_volume_ratio == 0.5
        assert data.sentiment_score == 65.0

    def test_options_data_to_dict(self):
        """Test converting OptionsData to dict."""
        from finviz_weekly.scrapers.options_flow import OptionsData

        data = OptionsData(
            ticker="MSFT",
            fetch_date="2025-01-15T10:00:00",
            sentiment_score=55.0,
        )

        result = data.to_dict()

        assert isinstance(result, dict)
        assert result["ticker"] == "MSFT"
        assert result["sentiment_score"] == 55.0
        assert "total_call_volume" in result


class TestOptionsFlowScraper:
    """Tests for OptionsFlowScraper."""

    @patch("finviz_weekly.scrapers.options_flow.YFINANCE_AVAILABLE", True)
    def test_scraper_init(self):
        """Test scraper initialization."""
        from finviz_weekly.scrapers.options_flow import OptionsFlowConfig, OptionsFlowScraper

        config = OptionsFlowConfig()
        scraper = OptionsFlowScraper(config)

        assert scraper.config == config

    @patch("finviz_weekly.scrapers.options_flow.YFINANCE_AVAILABLE", False)
    def test_scraper_init_no_yfinance(self):
        """Test scraper initialization without yfinance."""
        from finviz_weekly.scrapers.options_flow import OptionsFlowScraper

        with pytest.raises(ImportError):
            OptionsFlowScraper()

    @patch("finviz_weekly.scrapers.options_flow.YFINANCE_AVAILABLE", True)
    def test_calculate_sentiment_bullish(self):
        """Test sentiment calculation for bullish signals."""
        from finviz_weekly.scrapers.options_flow import OptionsFlowConfig, OptionsFlowScraper

        scraper = OptionsFlowScraper()

        # Low P/C ratio + unusual call volume = bullish
        score = scraper._calculate_sentiment(
            put_call_volume_ratio=0.5,  # Low = bullish
            put_call_oi_ratio=0.7,
            unusual_call_volume=True,
            unusual_put_volume=False,
            near_money_call_volume=1000,
            near_money_put_volume=200,
        )

        assert score > 60  # Should be bullish

    @patch("finviz_weekly.scrapers.options_flow.YFINANCE_AVAILABLE", True)
    def test_calculate_sentiment_bearish(self):
        """Test sentiment calculation for bearish signals."""
        from finviz_weekly.scrapers.options_flow import OptionsFlowScraper

        scraper = OptionsFlowScraper()

        # High P/C ratio + unusual put volume = bearish
        score = scraper._calculate_sentiment(
            put_call_volume_ratio=1.5,  # High = bearish
            put_call_oi_ratio=1.3,
            unusual_call_volume=False,
            unusual_put_volume=True,
            near_money_call_volume=200,
            near_money_put_volume=1000,
        )

        assert score < 40  # Should be bearish

    @patch("finviz_weekly.scrapers.options_flow.YFINANCE_AVAILABLE", True)
    def test_calculate_sentiment_neutral(self):
        """Test sentiment calculation for neutral signals."""
        from finviz_weekly.scrapers.options_flow import OptionsFlowScraper

        scraper = OptionsFlowScraper()

        score = scraper._calculate_sentiment(
            put_call_volume_ratio=0.85,  # Neutral range
            put_call_oi_ratio=1.0,
            unusual_call_volume=False,
            unusual_put_volume=False,
            near_money_call_volume=500,
            near_money_put_volume=500,
        )

        assert 40 <= score <= 60  # Should be neutral

    @patch("finviz_weekly.scrapers.options_flow.YFINANCE_AVAILABLE", True)
    def test_to_dataframe(self):
        """Test converting options data to DataFrame."""
        from finviz_weekly.scrapers.options_flow import OptionsData, OptionsFlowScraper

        scraper = OptionsFlowScraper()

        options_data = {
            "AAPL": OptionsData(
                ticker="AAPL",
                fetch_date="2025-01-15",
                sentiment_score=65.0,
            ),
            "MSFT": OptionsData(
                ticker="MSFT",
                fetch_date="2025-01-15",
                sentiment_score=55.0,
            ),
        }

        df = scraper.to_dataframe(options_data)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "ticker" in df.columns
        assert "sentiment_score" in df.columns

    @patch("finviz_weekly.scrapers.options_flow.YFINANCE_AVAILABLE", True)
    def test_get_unusual_activity(self):
        """Test getting unusual activity tickers."""
        from finviz_weekly.scrapers.options_flow import OptionsData, OptionsFlowScraper

        scraper = OptionsFlowScraper()

        options_data = {
            "AAPL": OptionsData(
                ticker="AAPL",
                fetch_date="2025-01-15",
                volume_spike_magnitude=3.5,
                unusual_call_volume=True,
                put_call_volume_ratio=0.5,
                sentiment_score=70.0,
            ),
            "MSFT": OptionsData(
                ticker="MSFT",
                fetch_date="2025-01-15",
                volume_spike_magnitude=1.2,
                sentiment_score=50.0,
            ),
        }

        unusual = scraper.get_unusual_activity(options_data, min_volume_spike=2.0)

        assert len(unusual) == 1
        assert unusual.iloc[0]["ticker"] == "AAPL"


class TestCalculateOptionsSentimentScore:
    """Tests for calculate_options_sentiment_score function."""

    def test_add_sentiment_to_data(self):
        """Test adding sentiment scores to stock data."""
        from finviz_weekly.scrapers.options_flow import (
            OptionsData,
            calculate_options_sentiment_score,
        )

        data = pd.DataFrame({
            "ticker": ["AAPL", "MSFT", "GOOGL"],
            "price": [150.0, 350.0, 140.0],
        })

        options_data = {
            "AAPL": OptionsData(
                ticker="AAPL",
                fetch_date="2025-01-15",
                sentiment_score=70.0,
            ),
            "MSFT": OptionsData(
                ticker="MSFT",
                fetch_date="2025-01-15",
                sentiment_score=30.0,
            ),
        }

        result = calculate_options_sentiment_score(data, options_data)

        assert "options_sentiment" in result.columns
        assert result.loc[result["ticker"] == "AAPL", "options_sentiment"].iloc[0] == 70.0
        assert result.loc[result["ticker"] == "MSFT", "options_sentiment"].iloc[0] == 30.0
        assert result.loc[result["ticker"] == "GOOGL", "options_sentiment"].iloc[0] == 50.0  # Default
