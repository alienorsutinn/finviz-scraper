"""Tests for analyst estimates scraper."""
from __future__ import annotations

import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch


class TestAnalystEstimatesConfig:
    """Tests for AnalystEstimatesConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        from finviz_weekly.scrapers.analyst_estimates import AnalystEstimatesConfig

        config = AnalystEstimatesConfig()

        assert config.significant_revision_pct == 5.0
        assert config.strong_revision_pct == 10.0
        assert config.upside_threshold == 20.0


class TestAnalystData:
    """Tests for AnalystData dataclass."""

    def test_analyst_data_creation(self):
        """Test creating AnalystData."""
        from finviz_weekly.scrapers.analyst_estimates import AnalystData

        data = AnalystData(
            ticker="AAPL",
            fetch_date="2025-01-15T10:00:00",
            eps_current_year=6.5,
            eps_revision_7d=3.5,
            price_target_mean=200.0,
            current_price=175.0,
            strong_buy=10,
            buy=15,
            hold=5,
        )

        assert data.ticker == "AAPL"
        assert data.eps_current_year == 6.5
        assert data.eps_revision_7d == 3.5
        assert data.strong_buy == 10

    def test_analyst_data_to_dict(self):
        """Test converting AnalystData to dict."""
        from finviz_weekly.scrapers.analyst_estimates import AnalystData

        data = AnalystData(
            ticker="MSFT",
            fetch_date="2025-01-15T10:00:00",
            revision_score=75.0,
        )

        result = data.to_dict()

        assert isinstance(result, dict)
        assert result["ticker"] == "MSFT"
        assert result["revision_score"] == 75.0


class TestAnalystEstimatesScraper:
    """Tests for AnalystEstimatesScraper."""

    @patch("finviz_weekly.scrapers.analyst_estimates.YFINANCE_AVAILABLE", True)
    def test_scraper_init(self):
        """Test scraper initialization."""
        from finviz_weekly.scrapers.analyst_estimates import AnalystEstimatesConfig, AnalystEstimatesScraper

        config = AnalystEstimatesConfig()
        scraper = AnalystEstimatesScraper(config)

        assert scraper.config == config

    @patch("finviz_weekly.scrapers.analyst_estimates.YFINANCE_AVAILABLE", False)
    def test_scraper_init_no_yfinance(self):
        """Test scraper initialization without yfinance."""
        from finviz_weekly.scrapers.analyst_estimates import AnalystEstimatesScraper

        with pytest.raises(ImportError):
            AnalystEstimatesScraper()

    @patch("finviz_weekly.scrapers.analyst_estimates.YFINANCE_AVAILABLE", True)
    def test_calculate_revision_score_positive(self):
        """Test revision score for positive revisions."""
        from finviz_weekly.scrapers.analyst_estimates import AnalystData, AnalystEstimatesScraper

        scraper = AnalystEstimatesScraper()

        data = AnalystData(
            ticker="TEST",
            fetch_date="2025-01-15",
            eps_revision_7d=8.0,  # Strong positive
            eps_revision_30d=5.0,
        )

        score = scraper._calculate_revision_score(data)
        assert score > 70  # Should be bullish

    @patch("finviz_weekly.scrapers.analyst_estimates.YFINANCE_AVAILABLE", True)
    def test_calculate_revision_score_negative(self):
        """Test revision score for negative revisions."""
        from finviz_weekly.scrapers.analyst_estimates import AnalystData, AnalystEstimatesScraper

        scraper = AnalystEstimatesScraper()

        data = AnalystData(
            ticker="TEST",
            fetch_date="2025-01-15",
            eps_revision_7d=-8.0,  # Strong negative
            eps_revision_30d=-5.0,
        )

        score = scraper._calculate_revision_score(data)
        assert score < 30  # Should be bearish

    @patch("finviz_weekly.scrapers.analyst_estimates.YFINANCE_AVAILABLE", True)
    def test_calculate_rating_score(self):
        """Test rating score calculation."""
        from finviz_weekly.scrapers.analyst_estimates import AnalystData, AnalystEstimatesScraper

        scraper = AnalystEstimatesScraper()

        # Mostly buys
        data = AnalystData(
            ticker="TEST",
            fetch_date="2025-01-15",
            strong_buy=5,
            buy=10,
            hold=3,
            sell=1,
            strong_sell=0,
        )

        score = scraper._calculate_rating_score(data)
        assert score > 70  # Should be bullish

    @patch("finviz_weekly.scrapers.analyst_estimates.YFINANCE_AVAILABLE", True)
    def test_calculate_target_score(self):
        """Test price target score calculation."""
        from finviz_weekly.scrapers.analyst_estimates import AnalystData, AnalystEstimatesScraper

        scraper = AnalystEstimatesScraper()

        data = AnalystData(
            ticker="TEST",
            fetch_date="2025-01-15",
            upside_to_target=25.0,  # 25% upside
        )

        score = scraper._calculate_target_score(data)
        assert score > 60  # Should be positive

    @patch("finviz_weekly.scrapers.analyst_estimates.YFINANCE_AVAILABLE", True)
    def test_to_dataframe(self):
        """Test converting analyst data to DataFrame."""
        from finviz_weekly.scrapers.analyst_estimates import AnalystData, AnalystEstimatesScraper

        scraper = AnalystEstimatesScraper()

        analyst_data = {
            "AAPL": AnalystData(
                ticker="AAPL",
                fetch_date="2025-01-15",
                revision_score=75.0,
            ),
            "MSFT": AnalystData(
                ticker="MSFT",
                fetch_date="2025-01-15",
                revision_score=65.0,
            ),
        }

        df = scraper.to_dataframe(analyst_data)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "ticker" in df.columns
        assert "revision_score" in df.columns


class TestCalculateAnalystScore:
    """Tests for calculate_analyst_score function."""

    def test_add_analyst_score_to_data(self):
        """Test adding analyst scores to stock data."""
        from finviz_weekly.scrapers.analyst_estimates import (
            AnalystData,
            calculate_analyst_score,
        )

        data = pd.DataFrame({
            "ticker": ["AAPL", "MSFT", "GOOGL"],
            "price": [175.0, 350.0, 140.0],
        })

        analyst_data = {
            "AAPL": AnalystData(
                ticker="AAPL",
                fetch_date="2025-01-15",
                revision_score=80.0,
                rating_score=75.0,
                target_score=70.0,
                combined_score=75.0,
                upside_to_target=15.0,
            ),
            "MSFT": AnalystData(
                ticker="MSFT",
                fetch_date="2025-01-15",
                revision_score=60.0,
                rating_score=65.0,
                target_score=55.0,
                combined_score=60.0,
                upside_to_target=10.0,
            ),
        }

        result = calculate_analyst_score(data, analyst_data)

        assert "analyst_combined_score" in result.columns
        assert result.loc[result["ticker"] == "AAPL", "analyst_combined_score"].iloc[0] == 75.0
        assert result.loc[result["ticker"] == "GOOGL", "analyst_combined_score"].iloc[0] == 50.0  # Default
