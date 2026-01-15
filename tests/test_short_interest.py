"""Tests for short interest tracker."""
from __future__ import annotations

import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch


class TestShortInterestConfig:
    """Tests for ShortInterestConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        from finviz_weekly.scrapers.short_interest import ShortInterestConfig

        config = ShortInterestConfig()

        assert config.high_short_interest_pct == 15.0
        assert config.extreme_short_interest_pct == 25.0
        assert config.days_to_cover_threshold == 5.0
        assert config.contrarian_weight == 0.6
        assert config.squeeze_bonus == 20.0

    def test_custom_config(self):
        """Test custom configuration."""
        from finviz_weekly.scrapers.short_interest import ShortInterestConfig

        config = ShortInterestConfig(
            high_short_interest_pct=20.0,
            extreme_short_interest_pct=30.0,
            days_to_cover_threshold=7.0,
        )

        assert config.high_short_interest_pct == 20.0
        assert config.extreme_short_interest_pct == 30.0
        assert config.days_to_cover_threshold == 7.0


class TestShortInterestData:
    """Tests for ShortInterestData dataclass."""

    def test_short_data_creation(self):
        """Test creating ShortInterestData."""
        from finviz_weekly.scrapers.short_interest import ShortInterestData

        data = ShortInterestData(
            ticker="GME",
            fetch_date="2025-01-15T10:00:00",
            short_interest=10000000,
            shares_float=50000000,
            short_percent_float=20.0,
            days_to_cover=6.5,
            is_squeeze_candidate=True,
            squeeze_score=75.0,
            contrarian_score=80.0,
        )

        assert data.ticker == "GME"
        assert data.short_interest == 10000000
        assert data.short_percent_float == 20.0
        assert data.days_to_cover == 6.5
        assert data.is_squeeze_candidate is True
        assert data.squeeze_score == 75.0

    def test_short_data_to_dict(self):
        """Test converting ShortInterestData to dict."""
        from finviz_weekly.scrapers.short_interest import ShortInterestData

        data = ShortInterestData(
            ticker="AMC",
            fetch_date="2025-01-15T10:00:00",
            short_percent_float=18.0,
            squeeze_score=60.0,
        )

        result = data.to_dict()

        assert isinstance(result, dict)
        assert result["ticker"] == "AMC"
        assert result["short_percent_float"] == 18.0
        assert result["squeeze_score"] == 60.0
        assert "contrarian_score" in result


class TestShortInterestTracker:
    """Tests for ShortInterestTracker."""

    @patch("finviz_weekly.scrapers.short_interest.YFINANCE_AVAILABLE", True)
    def test_tracker_init(self):
        """Test tracker initialization."""
        from finviz_weekly.scrapers.short_interest import ShortInterestConfig, ShortInterestTracker

        config = ShortInterestConfig()
        tracker = ShortInterestTracker(config)

        assert tracker.config == config

    @patch("finviz_weekly.scrapers.short_interest.YFINANCE_AVAILABLE", False)
    def test_tracker_init_no_yfinance(self):
        """Test tracker initialization without yfinance."""
        from finviz_weekly.scrapers.short_interest import ShortInterestTracker

        with pytest.raises(ImportError):
            ShortInterestTracker()

    @patch("finviz_weekly.scrapers.short_interest.YFINANCE_AVAILABLE", True)
    def test_is_squeeze_candidate_true(self):
        """Test squeeze candidate detection - positive case."""
        from finviz_weekly.scrapers.short_interest import ShortInterestTracker

        tracker = ShortInterestTracker()

        # High short interest + high days to cover
        is_candidate = tracker._is_squeeze_candidate(
            short_percent_float=20.0,  # > 15%
            days_to_cover=7.0,  # > 5
            trend="increasing",
        )

        assert is_candidate is True

    @patch("finviz_weekly.scrapers.short_interest.YFINANCE_AVAILABLE", True)
    def test_is_squeeze_candidate_false_low_short(self):
        """Test squeeze candidate detection - low short interest."""
        from finviz_weekly.scrapers.short_interest import ShortInterestTracker

        tracker = ShortInterestTracker()

        is_candidate = tracker._is_squeeze_candidate(
            short_percent_float=10.0,  # < 15%
            days_to_cover=7.0,
            trend="stable",
        )

        assert is_candidate is False

    @patch("finviz_weekly.scrapers.short_interest.YFINANCE_AVAILABLE", True)
    def test_is_squeeze_candidate_false_low_dtc(self):
        """Test squeeze candidate detection - low days to cover."""
        from finviz_weekly.scrapers.short_interest import ShortInterestTracker

        tracker = ShortInterestTracker()

        is_candidate = tracker._is_squeeze_candidate(
            short_percent_float=20.0,
            days_to_cover=3.0,  # < 5
            trend="stable",
        )

        assert is_candidate is False

    @patch("finviz_weekly.scrapers.short_interest.YFINANCE_AVAILABLE", True)
    def test_calculate_squeeze_score_high(self):
        """Test squeeze score calculation for high squeeze potential."""
        from finviz_weekly.scrapers.short_interest import ShortInterestTracker

        tracker = ShortInterestTracker()

        score = tracker._calculate_squeeze_score(
            short_percent_float=30.0,  # Extreme
            days_to_cover=10.0,  # Very high
            change_pct=25.0,  # Rapidly increasing
        )

        assert score >= 80  # Should be high

    @patch("finviz_weekly.scrapers.short_interest.YFINANCE_AVAILABLE", True)
    def test_calculate_squeeze_score_low(self):
        """Test squeeze score calculation for low squeeze potential."""
        from finviz_weekly.scrapers.short_interest import ShortInterestTracker

        tracker = ShortInterestTracker()

        score = tracker._calculate_squeeze_score(
            short_percent_float=5.0,  # Low
            days_to_cover=2.0,  # Low
            change_pct=-5.0,  # Decreasing
        )

        assert score < 30  # Should be low

    @patch("finviz_weekly.scrapers.short_interest.YFINANCE_AVAILABLE", True)
    def test_calculate_contrarian_score(self):
        """Test contrarian score calculation."""
        from finviz_weekly.scrapers.short_interest import ShortInterestTracker

        tracker = ShortInterestTracker()

        score = tracker._calculate_contrarian_score(
            short_percent_float=25.0,  # High
            days_to_cover=7.0,  # High
            is_squeeze_candidate=True,
            squeeze_score=70.0,
        )

        assert score > 70  # Should be high contrarian opportunity

    @patch("finviz_weekly.scrapers.short_interest.YFINANCE_AVAILABLE", True)
    def test_to_dataframe(self):
        """Test converting short data to DataFrame."""
        from finviz_weekly.scrapers.short_interest import ShortInterestData, ShortInterestTracker

        tracker = ShortInterestTracker()

        short_data = {
            "GME": ShortInterestData(
                ticker="GME",
                fetch_date="2025-01-15",
                short_percent_float=20.0,
                squeeze_score=75.0,
            ),
            "AMC": ShortInterestData(
                ticker="AMC",
                fetch_date="2025-01-15",
                short_percent_float=18.0,
                squeeze_score=60.0,
            ),
        }

        df = tracker.to_dataframe(short_data)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "ticker" in df.columns
        assert "short_percent_float" in df.columns
        assert "squeeze_score" in df.columns

    @patch("finviz_weekly.scrapers.short_interest.YFINANCE_AVAILABLE", True)
    def test_get_squeeze_candidates(self):
        """Test getting squeeze candidate tickers."""
        from finviz_weekly.scrapers.short_interest import ShortInterestData, ShortInterestTracker

        tracker = ShortInterestTracker()

        short_data = {
            "GME": ShortInterestData(
                ticker="GME",
                fetch_date="2025-01-15",
                short_percent_float=25.0,
                days_to_cover=8.0,
                is_squeeze_candidate=True,
                squeeze_score=80.0,
                contrarian_score=85.0,
            ),
            "AAPL": ShortInterestData(
                ticker="AAPL",
                fetch_date="2025-01-15",
                short_percent_float=2.0,
                days_to_cover=1.5,
                is_squeeze_candidate=False,
                squeeze_score=10.0,
                contrarian_score=50.0,
            ),
        }

        candidates = tracker.get_squeeze_candidates(short_data, min_squeeze_score=50.0)

        assert len(candidates) == 1
        assert candidates.iloc[0]["ticker"] == "GME"

    @patch("finviz_weekly.scrapers.short_interest.YFINANCE_AVAILABLE", True)
    def test_get_high_short_interest(self):
        """Test getting high short interest tickers."""
        from finviz_weekly.scrapers.short_interest import ShortInterestData, ShortInterestTracker

        tracker = ShortInterestTracker()

        short_data = {
            "GME": ShortInterestData(
                ticker="GME",
                fetch_date="2025-01-15",
                short_percent_float=25.0,
                short_percent_outstanding=20.0,
                days_to_cover=8.0,
                short_interest_trend="increasing",
                contrarian_score=85.0,
            ),
            "MSFT": ShortInterestData(
                ticker="MSFT",
                fetch_date="2025-01-15",
                short_percent_float=5.0,
                short_percent_outstanding=4.0,
                days_to_cover=2.0,
                short_interest_trend="stable",
                contrarian_score=50.0,
            ),
        }

        high_short = tracker.get_high_short_interest(short_data, min_short_pct=10.0)

        assert len(high_short) == 1
        assert high_short.iloc[0]["ticker"] == "GME"


class TestCalculateShortInterestScore:
    """Tests for calculate_short_interest_score function."""

    def test_add_short_score_to_data(self):
        """Test adding short interest scores to stock data."""
        from finviz_weekly.scrapers.short_interest import (
            ShortInterestData,
            calculate_short_interest_score,
        )

        data = pd.DataFrame({
            "ticker": ["GME", "MSFT", "AAPL"],
            "price": [20.0, 350.0, 150.0],
        })

        short_data = {
            "GME": ShortInterestData(
                ticker="GME",
                fetch_date="2025-01-15",
                contrarian_score=85.0,
                is_squeeze_candidate=True,
            ),
            "MSFT": ShortInterestData(
                ticker="MSFT",
                fetch_date="2025-01-15",
                contrarian_score=45.0,
                is_squeeze_candidate=False,
            ),
        }

        result = calculate_short_interest_score(data, short_data)

        assert "short_contrarian_score" in result.columns
        assert "is_squeeze_candidate" in result.columns
        assert result.loc[result["ticker"] == "GME", "short_contrarian_score"].iloc[0] == 85.0
        assert result.loc[result["ticker"] == "GME", "is_squeeze_candidate"].iloc[0] is True
        assert result.loc[result["ticker"] == "MSFT", "short_contrarian_score"].iloc[0] == 45.0
        assert result.loc[result["ticker"] == "AAPL", "short_contrarian_score"].iloc[0] == 50.0  # Default
