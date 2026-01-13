"""Tests for enhancement module."""
import pandas as pd
import pytest

from finviz_weekly.enhance import merge_enhanced_data
from finviz_weekly.score_enhanced import (
    calculate_insider_score,
    calculate_earnings_score,
    calculate_financial_health_score,
    calculate_enhanced_total_score,
)


def test_merge_enhanced_data_basic():
    """Test basic merging of enhanced data."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT"],
        "market_cap": [3000000000000, 2500000000000],
    })

    enhanced_data = {
        "AAPL": {
            "insider": {"net_value": 1000000.0, "total_buys": 5, "total_sells": 2, "buy_value": 3000000.0, "sell_value": 2000000.0},
            "earnings": {"avg_day_0_alpha": 2.5, "positive_reaction_pct": 75.0, "avg_rsi": 55.0, "total_events": 12},
            "financials": {"ratios": {"net_margin": 0.25, "roe": 1.5, "roa": 0.8, "current_ratio": 1.8, "debt_to_equity": 1.2, "gross_margin": 0.38, "quick_ratio": 1.5}},
        }
    }

    result = merge_enhanced_data(df, enhanced_data)

    # Check new columns exist
    assert "insider_net_value" in result.columns
    assert "earnings_avg_alpha" in result.columns
    assert "net_margin" in result.columns

    # Check values for AAPL
    assert result.loc[0, "insider_net_value"] == 1000000.0
    assert result.loc[0, "earnings_avg_alpha"] == 2.5
    assert result.loc[0, "net_margin"] == 0.25

    # Check values for MSFT (not in enhanced_data)
    assert result.loc[1, "insider_net_value"] == 0.0
    assert result.loc[1, "earnings_avg_alpha"] == 0.0
    assert pd.isna(result.loc[1, "net_margin"])


def test_merge_enhanced_data_empty_enhanced():
    """Test merging with empty enhanced data."""
    df = pd.DataFrame({
        "ticker": ["AAPL"],
        "market_cap": [3000000000000],
    })

    enhanced_data = {}
    result = merge_enhanced_data(df, enhanced_data)

    # Check columns added with default values
    assert "insider_net_value" in result.columns
    assert result.loc[0, "insider_net_value"] == 0.0


def test_calculate_insider_score_positive_buying():
    """Test insider score calculation with net buying."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "insider_net_value": [5000000.0, 2500000.0, -1000000.0],
    })

    scores = calculate_insider_score(df)

    # AAPL has maximum buying (should be highest score)
    assert scores[0] > 5.0  # Above neutral
    assert scores[0] == 10.0  # Max normalized value

    # MSFT has positive buying (should be above neutral)
    assert 5.0 < scores[1] < 10.0

    # GOOGL has net selling (should be below neutral)
    assert scores[2] < 5.0


def test_calculate_insider_score_no_data():
    """Test insider score with no data."""
    df = pd.DataFrame({
        "ticker": ["AAPL"],
    })

    scores = calculate_insider_score(df)
    assert scores[0] == 5.0  # Neutral default


def test_calculate_earnings_score_positive_alpha():
    """Test earnings score with positive alpha."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "earnings_avg_alpha": [3.0, 1.5, -2.0],
    })

    scores = calculate_earnings_score(df)

    # AAPL has max positive alpha (should be highest)
    assert scores[0] == 10.0

    # MSFT has moderate positive alpha
    assert 5.0 < scores[1] < 10.0

    # GOOGL has negative alpha
    assert scores[2] < 5.0


def test_calculate_earnings_score_clipping():
    """Test earnings score clips extreme values."""
    df = pd.DataFrame({
        "ticker": ["EXTREME_POS", "EXTREME_NEG"],
        "earnings_avg_alpha": [10.0, -10.0],  # Beyond ±3 range
    })

    scores = calculate_earnings_score(df)

    # Should be clipped to 10 and 0
    assert scores[0] == 10.0
    assert scores[1] == 0.0


def test_calculate_financial_health_score_strong():
    """Test financial health score with strong metrics."""
    df = pd.DataFrame({
        "ticker": ["AAPL"],
        "net_margin": [0.25],
        "roe": [1.5],
        "current_ratio": [2.0],
    })

    scores = calculate_financial_health_score(df)

    # Strong metrics should give high score
    assert scores[0] > 5.0


def test_calculate_financial_health_score_missing_data():
    """Test financial health score with missing data."""
    df = pd.DataFrame({
        "ticker": ["AAPL"],
        "net_margin": [None],
        "roe": [None],
        "current_ratio": [None],
    })

    scores = calculate_financial_health_score(df)

    # Should return neutral when data is missing
    assert scores[0] == 5.0


def test_calculate_enhanced_total_score_default_weights():
    """Test enhanced total score with default weights."""
    df = pd.DataFrame({
        "ticker": ["AAPL"],
        "total_score": [80.0],  # Traditional score
        "insider_net_value": [5000000.0],  # Positive
        "earnings_avg_alpha": [2.0],  # Positive
        "net_margin": [0.25],
        "roe": [1.5],
        "current_ratio": [2.0],
    })

    score = calculate_enhanced_total_score(df)

    # Enhanced score should be between 0 and 100
    assert 0 <= score[0] <= 100

    # With all positive factors, should be relatively high
    assert score[0] > 50.0


def test_calculate_enhanced_total_score_custom_weights():
    """Test enhanced total score with custom weights."""
    df = pd.DataFrame({
        "ticker": ["AAPL"],
        "total_score": [80.0],
        "insider_net_value": [5000000.0],
        "earnings_avg_alpha": [2.0],
        "net_margin": [0.25],
        "roe": [1.5],
        "current_ratio": [2.0],
    })

    custom_weights = {
        "traditional": 0.50,
        "insider": 0.30,
        "earnings": 0.10,
        "financial": 0.10,
    }

    score = calculate_enhanced_total_score(df, weights=custom_weights)

    # Score should still be in valid range
    assert 0 <= score[0] <= 100


def test_calculate_enhanced_total_score_no_traditional():
    """Test enhanced total score without traditional score."""
    df = pd.DataFrame({
        "ticker": ["AAPL"],
        "insider_net_value": [5000000.0],
        "earnings_avg_alpha": [2.0],
        "net_margin": [0.25],
        "roe": [1.5],
        "current_ratio": [2.0],
    })

    score = calculate_enhanced_total_score(df)

    # Should still calculate, using neutral for traditional
    assert 0 <= score[0] <= 100


def test_calculate_enhanced_total_score_weights_normalization():
    """Test that weights are normalized if they don't sum to 1.0."""
    df = pd.DataFrame({
        "ticker": ["AAPL"],
        "total_score": [80.0],
        "insider_net_value": [5000000.0],
        "earnings_avg_alpha": [2.0],
        "net_margin": [0.25],
        "roe": [1.5],
        "current_ratio": [2.0],
    })

    # Weights that don't sum to 1.0
    weights = {
        "traditional": 0.60,
        "insider": 0.15,
        "earnings": 0.15,
        "financial": 0.15,  # Sum = 1.05
    }

    score = calculate_enhanced_total_score(df, weights=weights)

    # Should normalize and still work
    assert 0 <= score[0] <= 100


def test_merge_enhanced_data_partial():
    """Test merging when only some enhanced data is present."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT"],
        "market_cap": [3000000000000, 2500000000000],
    })

    enhanced_data = {
        "AAPL": {
            "insider": {"net_value": 1000000.0, "total_buys": 5, "total_sells": 2, "buy_value": 3000000.0, "sell_value": 2000000.0},
            "earnings": None,  # No earnings data
            "financials": None,  # No financials data
        },
        "MSFT": {
            "insider": None,
            "earnings": {"avg_day_0_alpha": 1.5, "positive_reaction_pct": 60.0, "avg_rsi": 52.0, "total_events": 10},
            "financials": None,
        }
    }

    result = merge_enhanced_data(df, enhanced_data)

    # AAPL should have insider data but not earnings/financials
    assert result.loc[0, "insider_net_value"] == 1000000.0
    assert result.loc[0, "earnings_avg_alpha"] == 0.0

    # MSFT should have earnings data but not insider
    assert result.loc[1, "insider_net_value"] == 0.0
    assert result.loc[1, "earnings_avg_alpha"] == 1.5
