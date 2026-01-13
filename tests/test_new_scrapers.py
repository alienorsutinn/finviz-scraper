"""Tests for new scraper modules (insider, earnings, financials)."""
import pytest

from finviz_weekly.earnings import calculate_earnings_statistics
from finviz_weekly.financials import calculate_financial_ratios, compare_periods
from finviz_weekly.insider import aggregate_insider_by_ticker


def test_insider_aggregation():
    """Test insider trading aggregation."""
    transactions = [
        {
            "ticker": "AAPL",
            "insider_name": "Tim Cook",
            "transaction_type": "Buy - ESOP",
            "value": 1000000.0,
        },
        {
            "ticker": "AAPL",
            "insider_name": "Tim Cook",
            "transaction_type": "Sale",
            "value": 500000.0,
        },
        {
            "ticker": "MSFT",
            "insider_name": "Satya Nadella",
            "transaction_type": "Buy",
            "value": 2000000.0,
        },
    ]
    
    result = aggregate_insider_by_ticker(transactions)
    
    assert "AAPL" in result
    assert "MSFT" in result
    assert result["AAPL"]["total_buys"] == 1
    assert result["AAPL"]["total_sells"] == 1
    assert result["AAPL"]["buy_value"] == 1000000.0
    assert result["AAPL"]["sell_value"] == 500000.0
    assert result["AAPL"]["net_value"] == 500000.0
    assert result["AAPL"]["unique_insiders"] == 1  # Same person


def test_earnings_statistics_calculation():
    """Test earnings statistics calculation."""
    earnings_data = [
        {
            "day_0_pct": 0.05,  # +5%
            "week_plus_1_pct": 0.10,  # +10%
            "rsi": 50,
            "day_0_alpha": 0.03,
            "week_plus_1_alpha": 0.05,
        },
        {
            "day_0_pct": -0.03,  # -3%
            "week_plus_1_pct": -0.05,  # -5%
            "rsi": 60,
            "day_0_alpha": -0.01,
            "week_plus_1_alpha": -0.02,
        },
        {
            "day_0_pct": 0.02,  # +2%
            "week_plus_1_pct": 0.08,  # +8%
            "rsi": 55,
            "day_0_alpha": 0.01,
            "week_plus_1_alpha": 0.04,
        },
    ]
    
    stats = calculate_earnings_statistics(earnings_data)
    
    assert stats["total_events"] == 3
    assert stats["positive_reactions"] == 2
    assert stats["negative_reactions"] == 1
    assert abs(stats["avg_day_0_change"] - 0.0133) < 0.001  # Average of 5%, -3%, 2%
    assert abs(stats["avg_rsi"] - 55.0) < 0.1


def test_earnings_statistics_empty():
    """Test earnings statistics with empty data."""
    stats = calculate_earnings_statistics([])
    assert stats == {}


def test_financial_ratios_calculation():
    """Test financial ratio calculation."""
    statements = {
        "income_statement": {
            "Revenue": {"FY 2024": 100000000.0, "FY 2023": 80000000.0},
            "Net Income": {"FY 2024": 15000000.0, "FY 2023": 12000000.0},
            "Gross Profit": {"FY 2024": 40000000.0, "FY 2023": 32000000.0},
        },
        "balance_sheet": {
            "Total Assets": {"FY 2024": 200000000.0, "FY 2023": 180000000.0},
            "Total Equity": {"FY 2024": 120000000.0, "FY 2023": 110000000.0},
            "Total Debt": {"FY 2024": 50000000.0, "FY 2023": 45000000.0},
            "Current Assets": {"FY 2024": 80000000.0, "FY 2023": 70000000.0},
            "Current Liabilities": {"FY 2024": 40000000.0, "FY 2023": 35000000.0},
            "Cash": {"FY 2024": 30000000.0, "FY 2023": 25000000.0},
        },
        "cash_flow": {
            "Cash from Operating Activities": {"FY 2024": 20000000.0, "FY 2023": 18000000.0},
            "Capital Expenditures": {"FY 2024": -5000000.0, "FY 2023": -4000000.0},
        },
    }
    
    ratios = calculate_financial_ratios(statements)
    
    # Check profitability ratios
    assert "net_margin" in ratios
    assert abs(ratios["net_margin"] - 0.15) < 0.001  # 15M / 100M = 15%
    
    assert "gross_margin" in ratios
    assert abs(ratios["gross_margin"] - 0.40) < 0.001  # 40M / 100M = 40%
    
    # Check efficiency ratios
    assert "roa" in ratios
    assert abs(ratios["roa"] - 0.075) < 0.001  # 15M / 200M = 7.5%
    
    assert "roe" in ratios
    assert abs(ratios["roe"] - 0.125) < 0.001  # 15M / 120M = 12.5%
    
    # Check liquidity ratios
    assert "current_ratio" in ratios
    assert abs(ratios["current_ratio"] - 2.0) < 0.001  # 80M / 40M = 2.0
    
    # Check leverage ratios
    assert "debt_to_equity" in ratios
    assert abs(ratios["debt_to_equity"] - 0.4167) < 0.001  # 50M / 120M


def test_financial_ratios_missing_data():
    """Test financial ratio calculation with missing data."""
    statements = {
        "income_statement": {},
        "balance_sheet": {},
        "cash_flow": {},
    }
    
    ratios = calculate_financial_ratios(statements)
    assert isinstance(ratios, dict)
    # Should return empty or minimal ratios without crashing


def test_period_growth_calculation():
    """Test period-over-period growth calculation."""
    statements = {
        "income_statement": {
            "Revenue": {"FY 2024": 100000000.0, "FY 2023": 80000000.0},
            "Net Income": {"FY 2024": 15000000.0, "FY 2023": 12000000.0},
        },
        "balance_sheet": {
            "Total Assets": {"FY 2024": 200000000.0, "FY 2023": 180000000.0},
            "Total Equity": {"FY 2024": 120000000.0, "FY 2023": 110000000.0},
        },
    }
    
    growth = compare_periods(statements)
    
    assert growth["revenue_growth"] is not None
    assert abs(growth["revenue_growth"] - 0.25) < 0.001  # 25% growth
    
    assert growth["income_growth"] is not None
    assert abs(growth["income_growth"] - 0.25) < 0.001  # 25% growth
    
    assert growth["asset_growth"] is not None
    assert abs(growth["asset_growth"] - 0.1111) < 0.001  # ~11.1% growth


def test_period_growth_single_period():
    """Test growth calculation with only one period."""
    statements = {
        "income_statement": {
            "Revenue": {"FY 2024": 100000000.0},  # Only one period
        },
    }
    
    growth = compare_periods(statements)
    
    # Should handle gracefully
    assert growth["revenue_growth"] is None


def test_insider_aggregation_empty():
    """Test insider aggregation with empty list."""
    result = aggregate_insider_by_ticker([])
    assert result == {}


def test_insider_aggregation_multiple_insiders():
    """Test insider aggregation with multiple insiders per ticker."""
    transactions = [
        {
            "ticker": "AAPL",
            "insider_name": "Tim Cook",
            "transaction_type": "Buy",
            "value": 1000000.0,
        },
        {
            "ticker": "AAPL",
            "insider_name": "Jeff Williams",
            "transaction_type": "Buy",
            "value": 500000.0,
        },
    ]
    
    result = aggregate_insider_by_ticker(transactions)
    
    assert result["AAPL"]["unique_insiders"] == 2
    assert result["AAPL"]["total_buys"] == 2
    assert result["AAPL"]["buy_value"] == 1500000.0


def test_financial_ratios_zero_division():
    """Test that zero division is handled gracefully."""
    statements = {
        "income_statement": {
            "Revenue": {"FY 2024": 0.0},  # Zero revenue
            "Net Income": {"FY 2024": 10000.0},
        },
        "balance_sheet": {
            "Total Equity": {"FY 2024": 0.0},  # Zero equity
        },
    }
    
    ratios = calculate_financial_ratios(statements)
    
    # Should not crash and should skip ratios with zero denominators
    assert "net_margin" not in ratios or ratios["net_margin"] is None
    assert "roe" not in ratios or ratios["roe"] is None
