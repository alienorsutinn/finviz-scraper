"""Integration tests for the full pipeline."""
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from finviz_weekly.backtest import Backtester, BacktestConfig
from finviz_weekly.quality import DataQualityChecker


@pytest.fixture
def sample_fundamentals():
    """Create sample fundamentals dataframe."""
    data = {
        "ticker": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"],
        "price": [150.0, 350.0, 2800.0, 3300.0, 700.0],
        "market_cap": [2.5e12, 2.3e12, 1.8e12, 1.7e12, 7.0e11],
        "pe": [28.5, 32.1, 25.3, 62.4, 180.0],
        "sector": ["Technology"] * 5,
        "industry": ["Software", "Software", "Internet", "Retail", "Auto"],
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_scored():
    """Create sample scored dataframe."""
    data = {
        "ticker": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"],
        "total_score": [85.0, 82.0, 78.0, 75.0, 65.0],
        "quality_score": [90.0, 88.0, 85.0, 80.0, 70.0],
        "value_score": [80.0, 76.0, 71.0, 70.0, 60.0],
        "zone": ["ADD", "ADD", "HOLD", "HOLD", "REDUCE"],
    }
    return pd.DataFrame(data)


def test_quality_checker_fundamentals(sample_fundamentals):
    """Test quality checker on fundamentals data."""
    checker = DataQualityChecker({"min_rows": 3})
    metrics = checker.check_fundamentals(sample_fundamentals)

    assert metrics.total_rows == 5
    assert metrics.completeness > 0.9
    assert metrics.passed
    assert len(metrics.validation_errors) == 0


def test_quality_checker_scored(sample_scored):
    """Test quality checker on scored data."""
    checker = DataQualityChecker()
    metrics = checker.check_scored(sample_scored)

    assert metrics.total_rows == 5
    assert metrics.passed
    assert "total_score" in sample_scored.columns


def test_quality_checker_detects_duplicates():
    """Test that quality checker detects duplicate tickers."""
    data = pd.DataFrame({
        "ticker": ["AAPL", "AAPL", "MSFT"],  # Duplicate
        "price": [150.0, 150.0, 350.0],
    })

    checker = DataQualityChecker({"min_rows": 1})
    metrics = checker.check_fundamentals(data)

    assert metrics.duplicate_rows == 1
    assert not metrics.passed


def test_quality_checker_detects_low_completeness():
    """Test that quality checker detects low data completeness."""
    data = pd.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "price": [150.0, None, None],  # 67% null
        "market_cap": [2.5e12, None, None],
    })

    checker = DataQualityChecker({"min_rows": 1, "min_completeness": 0.8})
    metrics = checker.check_fundamentals(data)

    assert metrics.completeness < 0.8
    assert not metrics.passed


def test_quality_checker_detects_outliers():
    """Test that quality checker detects outliers."""
    data = pd.DataFrame({
        "ticker": ["AAPL", "BADCO"],
        "price": [150.0, -10.0],  # Negative price is outlier
        "pe": [28.5, 5000.0],  # Extreme P/E is outlier
        "market_cap": [2.5e12, -1e12],  # Negative market cap is outlier
    })

    checker = DataQualityChecker({"min_rows": 1})
    metrics = checker.check_fundamentals(data)

    assert metrics.outliers_detected > 0


def test_quality_report_generation(sample_fundamentals, tmp_path):
    """Test that quality report is generated correctly."""
    checker = DataQualityChecker()
    metrics = checker.check_fundamentals(sample_fundamentals)

    report_path = tmp_path / "quality_report.md"
    checker.save_report(metrics, report_path)

    assert report_path.exists()
    content = report_path.read_text()
    assert "Data Quality Report" in content
    assert "PASSED" in content or "FAILED" in content


def test_backtest_config_creation():
    """Test backtesting configuration."""
    config = BacktestConfig(
        start_date="2024-01-01",
        end_date="2024-12-31",
        top_n=20,
    )

    assert config.start_date == "2024-01-01"
    assert config.top_n == 20
    assert config.initial_capital == 100000.0


def test_backtest_requires_history_file():
    """Test that backtester requires history file."""
    with pytest.raises(FileNotFoundError):
        Backtester(Path("nonexistent.parquet"))


def test_quality_checker_critical_fields():
    """Test that quality checker validates critical fields."""
    data = pd.DataFrame({
        "ticker": ["AAPL"],
        "price": [150.0],
        # Missing market_cap, sector, industry
    })

    checker = DataQualityChecker({"min_rows": 1})
    metrics = checker.check_fundamentals(data)

    assert not metrics.passed
    assert any("market_cap" in error for error in metrics.validation_errors)
    assert any("sector" in error for error in metrics.validation_errors)


def test_quality_field_completeness_calculation(sample_fundamentals):
    """Test field completeness calculation."""
    # Add some null values to a critical field
    df = sample_fundamentals.copy()
    df.loc[0, "market_cap"] = None
    df.loc[1, "market_cap"] = None  # 60% complete

    checker = DataQualityChecker({"min_rows": 1})
    metrics = checker.check_fundamentals(df)

    # market_cap is a critical field, should be tracked
    assert "market_cap" in metrics.field_completeness
    assert metrics.field_completeness["market_cap"] == 0.6


def test_integration_save_and_load_quality_report(sample_fundamentals, tmp_path):
    """Test end-to-end quality check workflow."""
    # Check quality
    checker = DataQualityChecker()
    metrics = checker.check_fundamentals(sample_fundamentals)

    # Save report
    report_path = tmp_path / "reports" / "quality.md"
    checker.save_report(metrics, report_path)

    # Verify saved
    assert report_path.exists()
    assert report_path.stat().st_size > 0

    # Verify content
    content = report_path.read_text()
    assert "ticker" in content.lower()
    assert "completeness" in content.lower()
