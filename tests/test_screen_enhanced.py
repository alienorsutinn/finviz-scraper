"""Tests for enhanced scoring in screening module."""
import pandas as pd
import pytest
from finviz_weekly.screen import score_snapshot


def test_enhanced_scoring_with_all_data():
    """Test that enhanced scores are calculated when all enhanced columns present."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "company": ["Apple", "Microsoft", "Google"],
        "price": [150.0, 300.0, 120.0],
        "market_cap": ["2.5T", "2.3T", "1.5T"],
        # Traditional columns for base scores
        "forward_p_e": [25.0, 28.0, 22.0],
        "roe": [0.30, 0.35, 0.28],
        "current_ratio": [1.8, 2.0, 2.2],
        "perf_quarter": [5.0, 8.0, 3.0],
        # Enhanced columns
        "insider_net_value": [1000000.0, -500000.0, 2000000.0],
        "earnings_avg_alpha": [0.02, 0.01, 0.03],
        "net_margin": [0.25, 0.30, 0.22],
    })

    scored, screens = score_snapshot(df, normalize_by="none")

    # Check that enhanced scores were calculated
    assert "score_insider" in scored.columns
    assert "score_earnings" in scored.columns
    assert "score_financial_health" in scored.columns

    # Check that enhanced composite scores were created
    assert "score_insider_momentum" in scored.columns
    assert "score_earnings_surprise" in scored.columns
    assert "score_quality_growth_enhanced" in scored.columns
    assert "score_enhanced_master" in scored.columns

    # Check that enhanced screens were added
    assert "insider_momentum" in screens
    assert "earnings_surprise" in screens
    assert "quality_growth_enhanced" in screens
    assert "enhanced_master" in screens

    # Verify scores are numeric and in valid range
    assert scored["score_insider"].notna().all()
    assert (scored["score_insider"] >= 0).all() and (scored["score_insider"] <= 100).all()
    assert scored["score_earnings"].notna().all()
    assert (scored["score_earnings"] >= 0).all() and (scored["score_earnings"] <= 100).all()


def test_enhanced_scoring_insider_only():
    """Test enhanced scoring with only insider data."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT"],
        "company": ["Apple", "Microsoft"],
        "forward_p_e": [25.0, 28.0],
        "roe": [0.30, 0.35],
        "perf_quarter": [5.0, 8.0],
        # Only insider data
        "insider_net_value": [1000000.0, -500000.0],
    })

    scored, screens = score_snapshot(df, normalize_by="none")

    # Should have insider scores
    assert "score_insider" in scored.columns
    assert "score_insider_momentum" in scored.columns

    # Should NOT have earnings or financials scores
    assert "score_earnings" not in scored.columns
    assert "score_financial_health" not in scored.columns

    # Should NOT have enhanced_master (requires at least 2 sources)
    assert "score_enhanced_master" not in scored.columns

    # Check screens
    assert "insider_momentum" in screens
    assert "earnings_surprise" not in screens
    assert "quality_growth_enhanced" not in screens
    assert "enhanced_master" not in screens


def test_enhanced_scoring_earnings_only():
    """Test enhanced scoring with only earnings data."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT"],
        "company": ["Apple", "Microsoft"],
        "forward_p_e": [25.0, 28.0],
        "roe": [0.30, 0.35],
        "perf_quarter": [5.0, 8.0],
        # Only earnings data
        "earnings_avg_alpha": [0.02, 0.01],
    })

    scored, screens = score_snapshot(df, normalize_by="none")

    # Should have earnings scores
    assert "score_earnings" in scored.columns
    assert "score_earnings_surprise" in scored.columns

    # Should NOT have insider or financials scores
    assert "score_insider" not in scored.columns
    assert "score_financial_health" not in scored.columns

    # Should NOT have enhanced_master (needs at least 2 sources)
    assert "score_enhanced_master" not in scored.columns

    # Check screens
    assert "earnings_surprise" in screens
    assert "insider_momentum" not in screens
    assert "quality_growth_enhanced" not in screens


def test_enhanced_scoring_financials_only():
    """Test enhanced scoring with only financials data."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT"],
        "company": ["Apple", "Microsoft"],
        "forward_p_e": [25.0, 28.0],
        "roe": [0.30, 0.35],
        "perf_quarter": [5.0, 8.0],
        # Only financials data (need all three for financial_health score)
        "net_margin": [0.25, 0.30],
        "current_ratio": [1.8, 2.0],
    })

    scored, screens = score_snapshot(df, normalize_by="none")

    # Should have financial health scores
    assert "score_financial_health" in scored.columns
    assert "score_quality_growth_enhanced" in scored.columns

    # Should NOT have insider or earnings scores
    assert "score_insider" not in scored.columns
    assert "score_earnings" not in scored.columns

    # Should NOT have enhanced_master (needs at least 2 sources)
    assert "score_enhanced_master" not in scored.columns


def test_enhanced_scoring_partial_combinations():
    """Test enhanced scoring with various combinations of data."""
    # Insider + Earnings (no financials)
    df1 = pd.DataFrame({
        "ticker": ["AAPL"],
        "forward_p_e": [25.0],
        "roe": [0.30],
        "perf_quarter": [5.0],
        "insider_net_value": [1000000.0],
        "earnings_avg_alpha": [0.02],
    })

    scored1, _ = score_snapshot(df1, normalize_by="none")
    assert "score_insider" in scored1.columns
    assert "score_earnings" in scored1.columns
    assert "score_enhanced_master" in scored1.columns  # Should have 2-factor version
    assert "score_financial_health" not in scored1.columns

    # Insider + Financials (no earnings)
    df2 = pd.DataFrame({
        "ticker": ["AAPL"],
        "forward_p_e": [25.0],
        "roe": [0.30],
        "perf_quarter": [5.0],
        "insider_net_value": [1000000.0],
        "net_margin": [0.25],
        "current_ratio": [1.8],
    })

    scored2, _ = score_snapshot(df2, normalize_by="none")
    assert "score_insider" in scored2.columns
    assert "score_financial_health" in scored2.columns
    assert "score_enhanced_master" in scored2.columns  # Should have 2-factor version
    assert "score_earnings" not in scored2.columns

    # Earnings + Financials (no insider)
    df3 = pd.DataFrame({
        "ticker": ["AAPL"],
        "forward_p_e": [25.0],
        "roe": [0.30],
        "perf_quarter": [5.0],
        "earnings_avg_alpha": [0.02],
        "net_margin": [0.25],
        "current_ratio": [1.8],
    })

    scored3, _ = score_snapshot(df3, normalize_by="none")
    assert "score_earnings" in scored3.columns
    assert "score_financial_health" in scored3.columns
    assert "score_enhanced_master" in scored3.columns  # Should have 2-factor version
    assert "score_insider" not in scored3.columns


def test_enhanced_scoring_without_enhanced_data():
    """Test that traditional scoring works without enhanced data."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT"],
        "company": ["Apple", "Microsoft"],
        "forward_p_e": [25.0, 28.0],
        "roe": [0.30, 0.35],
        "current_ratio": [1.8, 2.0],
        "perf_quarter": [5.0, 8.0],
    })

    scored, screens = score_snapshot(df, normalize_by="none")

    # Should NOT have any enhanced scores
    assert "score_insider" not in scored.columns
    assert "score_earnings" not in scored.columns
    assert "score_financial_health" not in scored.columns
    assert "score_insider_momentum" not in scored.columns
    assert "score_earnings_surprise" not in scored.columns
    assert "score_quality_growth_enhanced" not in scored.columns
    assert "score_enhanced_master" not in scored.columns

    # Should still have traditional scores
    assert "score_quality" in scored.columns
    assert "score_value" in scored.columns
    assert "score_master" in scored.columns

    # Should have traditional screens
    assert "quality_value" in screens
    assert "compounders" in screens

    # Should NOT have enhanced screens
    assert "insider_momentum" not in screens
    assert "earnings_surprise" not in screens


def test_enhanced_scoring_composite_weights():
    """Test that composite score weights are correct."""
    df = pd.DataFrame({
        "ticker": ["AAPL"],
        "forward_p_e": [25.0],
        "roe": [0.30],
        "perf_quarter": [5.0],
        "perf_month": [2.0],
        "rsi_14": [45.0],
        "insider_net_value": [1000000.0],
        "earnings_avg_alpha": [0.02],
        "net_margin": [0.25],
        "current_ratio": [1.8],
    })

    scored, _ = score_snapshot(df, normalize_by="none")

    # Manually calculate insider_momentum to verify weights
    expected_insider_momentum = (
        0.40 * scored.loc[0, "score_insider"] +
        0.30 * scored.loc[0, "score_momentum"] +
        0.20 * scored.loc[0, "score_quality"] +
        0.10 * scored.loc[0, "score_value"]
    )
    assert abs(scored.loc[0, "score_insider_momentum"] - expected_insider_momentum) < 0.01

    # Manually calculate earnings_surprise to verify weights
    expected_earnings_surprise = (
        0.40 * scored.loc[0, "score_earnings"] +
        0.30 * scored.loc[0, "score_quality"] +
        0.20 * scored.loc[0, "score_growth"] +
        0.10 * scored.loc[0, "score_momentum"]
    )
    assert abs(scored.loc[0, "score_earnings_surprise"] - expected_earnings_surprise) < 0.01

    # Manually calculate quality_growth_enhanced to verify weights
    expected_quality_growth = (
        0.35 * scored.loc[0, "score_financial_health"] +
        0.30 * scored.loc[0, "score_quality"] +
        0.25 * scored.loc[0, "score_growth"] +
        0.10 * scored.loc[0, "score_value"]
    )
    assert abs(scored.loc[0, "score_quality_growth_enhanced"] - expected_quality_growth) < 0.01


def test_enhanced_scoring_with_normalize_by_sector():
    """Test that enhanced scoring works with sector normalization."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT", "JPM", "BAC"],
        "company": ["Apple", "Microsoft", "JPMorgan", "BofA"],
        "sector": ["Technology", "Technology", "Financials", "Financials"],
        "forward_p_e": [25.0, 28.0, 12.0, 10.0],
        "roe": [0.30, 0.35, 0.15, 0.12],
        "current_ratio": [1.8, 2.0, 1.2, 1.1],
        "perf_quarter": [5.0, 8.0, 2.0, 1.0],
        "insider_net_value": [1000000.0, -500000.0, 500000.0, -200000.0],
        "earnings_avg_alpha": [0.02, 0.01, 0.015, 0.005],
        "net_margin": [0.25, 0.30, 0.20, 0.18],
    })

    scored, screens = score_snapshot(df, normalize_by="sector")

    # Should have all enhanced scores
    assert "score_insider" in scored.columns
    assert "score_earnings" in scored.columns
    assert "score_financial_health" in scored.columns
    assert "score_enhanced_master" in scored.columns

    # Scores should be calculated
    assert scored["score_insider"].notna().all()
    assert scored["score_earnings"].notna().all()

    # Enhanced screens should exist
    assert "insider_momentum" in screens
    assert "earnings_surprise" in screens
    assert "enhanced_master" in screens


def test_enhanced_scoring_preserves_traditional_themes():
    """Test that adding enhanced scores doesn't break traditional themes."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT"],
        "company": ["Apple", "Microsoft"],
        "forward_p_e": [25.0, 28.0],
        "roe": [0.30, 0.35],
        "current_ratio": [1.8, 2.0],
        "perf_quarter": [5.0, 8.0],
        "insider_net_value": [1000000.0, -500000.0],
        "earnings_avg_alpha": [0.02, 0.01],
        "net_margin": [0.25, 0.30],
    })

    scored, screens = score_snapshot(df, normalize_by="none")

    # Traditional themes should still exist
    assert "quality_value" in screens
    assert "oversold_quality" in screens
    assert "compounders" in screens
    assert "hq_low_leverage" in screens
    assert "turnaround_value" in screens
    assert "garp" in screens

    # Traditional scores should still be calculated
    assert "score_quality" in scored.columns
    assert "score_value" in scored.columns
    assert "score_master" in scored.columns

    # Enhanced themes should be added
    assert "insider_momentum" in screens
    assert "earnings_surprise" in screens


def test_enhanced_scores_ranking():
    """Test that enhanced scores correctly rank stocks."""
    df = pd.DataFrame({
        "ticker": ["STRONG_INSIDER", "WEAK_INSIDER", "NEUTRAL"],
        "forward_p_e": [25.0, 25.0, 25.0],
        "roe": [0.30, 0.30, 0.30],
        "perf_quarter": [5.0, 5.0, 5.0],
        # Different levels of insider buying
        "insider_net_value": [5000000.0, 500000.0, 0.0],
    })

    scored, screens = score_snapshot(df, normalize_by="none")

    # Stocks with stronger insider buying should rank higher
    assert scored.loc[0, "score_insider"] > scored.loc[1, "score_insider"]
    assert scored.loc[1, "score_insider"] > scored.loc[2, "score_insider"]

    # Check screen ranking
    insider_screen = screens["insider_momentum"]
    assert insider_screen.ranked.loc[0, "ticker"] == "STRONG_INSIDER"
    assert insider_screen.ranked.loc[1, "ticker"] == "WEAK_INSIDER"
    assert insider_screen.ranked.loc[2, "ticker"] == "NEUTRAL"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
