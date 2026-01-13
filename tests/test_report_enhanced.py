"""Tests for enhanced reporting functionality."""
import pandas as pd
import pytest
from finviz_weekly.report import _enhanced_data_summaries


def test_enhanced_data_summaries_no_enhanced_data():
    """Test that no sections are generated without enhanced data."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT"],
        "company": ["Apple", "Microsoft"],
        "sector": ["Technology", "Technology"],
        "price": [150.0, 300.0],
    })

    sections = _enhanced_data_summaries(df)
    assert sections == []


def test_enhanced_data_summaries_with_insider_data():
    """Test insider trading summary generation."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOGL", "META"],
        "company": ["Apple", "Microsoft", "Google", "Meta"],
        "sector": ["Technology", "Technology", "Technology", "Technology"],
        "insider_net_value": [1000000.0, -500000.0, 2000000.0, 0.0],
        "insider_total_buys": [5, 2, 8, 0],
        "insider_total_sells": [1, 6, 1, 0],
    })

    sections = _enhanced_data_summaries(df)

    assert len(sections) > 0
    content = "\n".join(sections)

    # Should have insider section header
    assert "Insider Trading Highlights" in content

    # Should have top buying table
    assert "Top 10 Insider Buying" in content
    assert "GOOGL" in content  # Highest net value

    # Should have summary stats
    assert "Summary" in content
    assert "stocks with net buying" in content


def test_enhanced_data_summaries_with_earnings_data():
    """Test earnings quality summary generation."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "company": ["Apple", "Microsoft", "Google"],
        "sector": ["Technology", "Technology", "Technology"],
        "earnings_avg_alpha": [0.03, 0.01, -0.02],  # 3%, 1%, -2%
        "earnings_win_rate": [0.75, 0.60, 0.40],  # 75%, 60%, 40%
        "earnings_total_events": [4, 3, 5],
    })

    sections = _enhanced_data_summaries(df)

    content = "\n".join(sections)

    # Should have earnings section
    assert "Earnings Quality Highlights" in content
    assert "Top 10 Consistent Earnings Beaters" in content

    # Should show AAPL first (highest avg alpha)
    assert "AAPL" in content

    # Should have summary
    assert "stocks with 2+ earnings events" in content


def test_enhanced_data_summaries_with_financial_data():
    """Test financial health summary generation."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "company": ["Apple", "Microsoft", "Google"],
        "sector": ["Technology", "Technology", "Technology"],
        "net_margin": [0.25, 0.30, 0.22],  # 25%, 30%, 22%
        "roe": [0.35, 0.40, 0.28],  # 35%, 40%, 28%
        "current_ratio": [1.8, 2.0, 2.2],
        "debt_to_equity": [1.5, 0.8, 0.5],
    })

    sections = _enhanced_data_summaries(df)

    content = "\n".join(sections)

    # Should have financial section
    assert "Financial Health Highlights" in content
    assert "Top 10 Financial Health" in content

    # Should have summary with stats
    assert "stocks with financial data" in content
    assert "net margin" in content.lower()


def test_enhanced_data_summaries_with_all_data():
    """Test that all sections are generated when all enhanced data present."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT"],
        "company": ["Apple", "Microsoft"],
        "sector": ["Technology", "Technology"],
        # Insider data
        "insider_net_value": [1000000.0, -500000.0],
        "insider_total_buys": [5, 2],
        "insider_total_sells": [1, 6],
        # Earnings data
        "earnings_avg_alpha": [0.03, 0.01],
        "earnings_win_rate": [0.75, 0.60],
        "earnings_total_events": [4, 3],
        # Financial data
        "net_margin": [0.25, 0.30],
        "roe": [0.35, 0.40],
        "current_ratio": [1.8, 2.0],
        "debt_to_equity": [1.5, 0.8],
    })

    sections = _enhanced_data_summaries(df)

    content = "\n".join(sections)

    # Should have all three sections
    assert "Enhanced Data Insights" in content
    assert "Insider Trading Highlights" in content
    assert "Earnings Quality Highlights" in content
    assert "Financial Health Highlights" in content


def test_enhanced_data_summaries_empty_insider_activity():
    """Test handling of dataframe with insider columns but no activity."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT"],
        "company": ["Apple", "Microsoft"],
        "sector": ["Technology", "Technology"],
        "insider_net_value": [0.0, 0.0],  # No activity
        "insider_total_buys": [0, 0],
        "insider_total_sells": [0, 0],
    })

    sections = _enhanced_data_summaries(df)

    content = "\n".join(sections)

    # Should still have insider section but show no activity
    assert "Insider Trading Highlights" in content
    assert "No insider activity in dataset" in content


def test_enhanced_data_summaries_insufficient_earnings_events():
    """Test handling when stocks have <2 earnings events."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT"],
        "company": ["Apple", "Microsoft"],
        "sector": ["Technology", "Technology"],
        "earnings_avg_alpha": [0.03, 0.01],
        "earnings_win_rate": [0.75, 0.60],
        "earnings_total_events": [1, 1],  # Only 1 event each
    })

    sections = _enhanced_data_summaries(df)

    content = "\n".join(sections)

    # Should show no earnings data (filtered out)
    assert "Earnings Quality Highlights" in content
    assert "No earnings data in dataset" in content


def test_enhanced_data_summaries_partial_financial_data():
    """Test handling when only some financial metrics are present."""
    df = pd.DataFrame({
        "ticker": ["AAPL"],
        "company": ["Apple"],
        "sector": ["Technology"],
        "net_margin": [0.25],  # Has margin
        "roe": [0.35],  # Has ROE
        # Missing current_ratio - should not generate financial section
    })

    sections = _enhanced_data_summaries(df)

    # Should not have financial health section (requires all 3 metrics)
    content = "\n".join(sections)
    assert "Financial Health Highlights" not in content


def test_enhanced_data_summaries_insider_selling_only():
    """Test insider summary when only selling present."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "company": ["Apple", "Microsoft", "Google"],
        "sector": ["Technology", "Technology", "Technology"],
        "insider_net_value": [-1000000.0, -500000.0, -2000000.0],  # All selling
        "insider_total_buys": [0, 0, 0],
        "insider_total_sells": [5, 3, 8],
    })

    sections = _enhanced_data_summaries(df)

    content = "\n".join(sections)

    # Should have insider selling table
    assert "Top 10 Insider Selling" in content
    assert "GOOGL" in content  # Largest selling


def test_enhanced_data_summaries_realistic_data():
    """Test with realistic mixed data."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOGL", "META", "TSLA", "NVDA", "AMD", "INTC", "ORCL", "CRM"],
        "company": ["Apple", "Microsoft", "Google", "Meta", "Tesla", "Nvidia", "AMD", "Intel", "Oracle", "Salesforce"],
        "sector": ["Technology"] * 10,
        # Mixed insider activity
        "insider_net_value": [2000000, -1000000, 1500000, 500000, -500000, 3000000, 800000, -300000, 200000, 1000000],
        "insider_total_buys": [8, 2, 6, 3, 1, 10, 4, 1, 2, 5],
        "insider_total_sells": [1, 8, 2, 1, 6, 0, 1, 5, 1, 2],
        # Mixed earnings performance
        "earnings_avg_alpha": [0.04, 0.02, -0.01, 0.03, -0.02, 0.05, 0.01, -0.03, 0.02, 0.01],
        "earnings_win_rate": [0.80, 0.70, 0.45, 0.75, 0.40, 0.85, 0.65, 0.35, 0.70, 0.60],
        "earnings_total_events": [5, 4, 6, 4, 5, 3, 4, 6, 4, 3],
        # Mixed financial health
        "net_margin": [0.26, 0.32, 0.24, 0.28, 0.12, 0.40, 0.18, 0.15, 0.30, 0.22],
        "roe": [0.38, 0.42, 0.32, 0.35, 0.18, 0.50, 0.25, 0.20, 0.35, 0.28],
        "current_ratio": [2.0, 2.2, 1.9, 1.8, 1.5, 2.5, 1.7, 1.6, 2.1, 1.9],
        "debt_to_equity": [1.2, 0.8, 0.5, 0.6, 1.8, 0.4, 0.9, 1.5, 1.0, 0.7],
    })

    sections = _enhanced_data_summaries(df)

    assert len(sections) > 10  # Should have multiple sections with tables

    content = "\n".join(sections)

    # Verify all major sections present
    assert "Enhanced Data Insights" in content
    assert "Insider Trading Highlights" in content
    assert "Earnings Quality Highlights" in content
    assert "Financial Health Highlights" in content

    # Verify top performers are mentioned
    assert "NVDA" in content  # Highest insider buying and financial metrics
    assert "MSFT" in content  # Largest insider selling

    # Verify summaries present
    assert "stocks with net buying" in content
    assert "with positive avg alpha" in content
    assert "stocks with financial data" in content


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
