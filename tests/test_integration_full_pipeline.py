"""
Integration test for complete scrape → screen → backtest workflow.

This test verifies that all major components work together correctly:
1. Scraper fetches data for test tickers
2. Screen filters and scores stocks
3. Output files have correct schema
4. Data flows correctly between modules
"""
import subprocess
import sys
import os
from pathlib import Path
from datetime import date

import pandas as pd
import pytest


# Set up environment for subprocess tests
def get_test_env():
    """Get environment variables for subprocess tests."""
    env = os.environ.copy()
    # Add src directory to PYTHONPATH
    src_dir = Path(__file__).parent.parent / "src"
    pythonpath = str(src_dir)
    if "PYTHONPATH" in env:
        pythonpath = f"{pythonpath}:{env['PYTHONPATH']}"
    env["PYTHONPATH"] = pythonpath
    return env


@pytest.mark.slow
@pytest.mark.network
def test_full_pipeline_integration(tmp_path):
    """
    Test complete pipeline: scrape → screen → verify outputs.

    This is a smoke test ensuring all major components integrate correctly.

    Note: Requires network access to Finviz. Run with: pytest -m slow
    """
    # Use small set of well-known tickers for fast test
    test_tickers = ["AAPL", "MSFT", "GOOGL"]

    # Run scraper
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "finviz_weekly.scrape",
            "--mode",
            "tickers",
            "--tickers",
            *test_tickers,
            "--out-dir",
            str(tmp_path),
            "--log-level",
            "WARNING",  # Reduce noise
        ],
        capture_output=True,
        text=True,
        timeout=120,  # 2 minute timeout
        env=get_test_env(),
    )

    # Check scraper succeeded
    assert result.returncode == 0, f"Scraper failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"

    # Verify latest snapshot exists
    latest_dir = tmp_path / "latest"
    assert latest_dir.exists(), "Latest snapshot directory not created"

    parquet_file = latest_dir / "finviz_fundamentals.parquet"
    csv_file = latest_dir / "finviz_fundamentals.csv"

    assert parquet_file.exists(), "Parquet output not created"
    assert csv_file.exists(), "CSV output not created"

    # Verify data schema
    df = pd.read_parquet(parquet_file)

    # Check we got data for all tickers
    assert len(df) >= len(test_tickers), f"Expected at least {len(test_tickers)} rows, got {len(df)}"

    # Verify required columns exist
    required_cols = ["ticker", "company", "sector", "industry", "market_cap", "price"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    assert not missing_cols, f"Missing required columns: {missing_cols}"

    # Verify data types are reasonable
    assert df["ticker"].dtype == object
    assert df["price"].dtype in [float, "float64"]
    assert df["market_cap"].dtype in [float, "float64"]

    # Verify no completely empty rows
    assert not df[required_cols].isna().all(axis=1).any(), "Found completely empty rows"

    # Check as_of_date if present
    if "as_of_date" in df.columns:
        dates = df["as_of_date"].unique()
        assert len(dates) == 1, f"Expected single as_of_date, got {len(dates)}"

        # Parse and verify date is recent
        as_of = date.fromisoformat(str(dates[0]))
        today = date.today()
        days_diff = (today - as_of).days
        assert days_diff >= 0 and days_diff < 7, f"as_of_date seems wrong: {as_of} (today: {today})"

    # Verify metadata file
    meta_file = latest_dir / "meta.json"
    assert meta_file.exists(), "Metadata file not created"

    import json
    meta = json.loads(meta_file.read_text())
    assert "as_of" in meta
    assert "rows" in meta
    assert meta["rows"] >= len(test_tickers)


@pytest.mark.slow
@pytest.mark.network
def test_screening_workflow(tmp_path):
    """
    Test screening workflow after scraping.

    This ensures screen.py can process scraped data correctly.

    Note: Requires network access to Finviz. Run with: pytest -m slow
    """
    # First scrape some data
    test_tickers = ["AAPL", "MSFT"]

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "finviz_weekly.scrape",
            "--mode",
            "tickers",
            "--tickers",
            *test_tickers,
            "--out-dir",
            str(tmp_path),
            "--log-level",
            "WARNING",
        ],
        capture_output=True,
        text=True,
        timeout=120,
        env=get_test_env(),
    )

    assert result.returncode == 0, f"Scraper failed: {result.stderr}"

    # Now run screening
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "finviz_weekly.screen",
            "--out-dir",
            str(tmp_path),
            "--top-n",
            "5",
            "--min-market-cap",
            "0",  # Accept any market cap for test
            "--min-price",
            "0",   # Accept any price for test
            "--log-level",
            "WARNING",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env=get_test_env(),
    )

    # Screening might fail if there's insufficient data, but shouldn't crash
    # We're mainly testing that it runs without exceptions
    if result.returncode != 0:
        # Check if it's a known acceptable error
        acceptable_errors = [
            "no data to screen",
            "insufficient candidates",
            "empty dataframe",
        ]
        stderr_lower = result.stderr.lower()
        stdout_lower = result.stdout.lower()

        has_acceptable_error = any(
            err in stderr_lower or err in stdout_lower
            for err in acceptable_errors
        )

        assert has_acceptable_error, (
            f"Screening failed with unexpected error:\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )
    else:
        # If screening succeeded, verify output
        screen_file = tmp_path / "screened.csv"
        if screen_file.exists():
            screen_df = pd.read_csv(screen_file)
            assert len(screen_df) > 0, "Screening produced empty results"
            assert "ticker" in screen_df.columns, "Screening output missing ticker column"


def test_selectors_module():
    """
    Test that new selectors module works correctly.

    This verifies Phase 2 HTML parsing abstraction.
    """
    from finviz_weekly.selectors import FinvizSelectors, FinvizUrls, FinvizPatterns

    # Test selectors are non-empty strings
    assert isinstance(FinvizSelectors.EARNINGS_TABLE, str)
    assert len(FinvizSelectors.EARNINGS_TABLE) > 0

    assert isinstance(FinvizSelectors.INSIDER_TABLE, str)
    assert len(FinvizSelectors.INSIDER_TABLE) > 0

    assert isinstance(FinvizSelectors.SCREENER_TABLE, str)
    assert len(FinvizSelectors.SCREENER_TABLE) > 0

    # Test URL builders
    quote_url = FinvizUrls.quote("AAPL")
    assert "finviz.com" in quote_url
    assert "AAPL" in quote_url

    earnings_url = FinvizUrls.earnings("AAPL")
    assert "finviz.com" in earnings_url
    assert "AAPL" in earnings_url
    assert "p=d" in earnings_url  # Daily earnings view

    insider_url = FinvizUrls.insider("AAPL")
    assert "finviz.com" in insider_url
    assert "AAPL" in insider_url
    assert "ty=c" in insider_url  # Charts view

    financials_url = FinvizUrls.financials("AAPL")
    assert "finviz.com" in financials_url
    assert "AAPL" in financials_url
    assert "ta=1" in financials_url  # Table view

    screener_url = FinvizUrls.screener()
    assert "finviz.com" in screener_url
    assert "screener.ashx" in screener_url

    screener_filtered_url = FinvizUrls.screener("v=111&f=cap_mega")
    assert "v=111" in screener_filtered_url
    assert "cap_mega" in screener_filtered_url

    # Test pattern matching
    assert FinvizPatterns.is_ticker("AAPL")
    assert FinvizPatterns.is_ticker("MSFT")
    assert FinvizPatterns.is_ticker("A")  # Single letter valid
    assert not FinvizPatterns.is_ticker("Apple Inc.")
    assert not FinvizPatterns.is_ticker("123")
    assert not FinvizPatterns.is_ticker("")

    # Test percentage extraction
    assert FinvizPatterns.extract_percentage("+5.23%") == pytest.approx(0.0523)
    assert FinvizPatterns.extract_percentage("-1.5%") == pytest.approx(-0.015)
    assert FinvizPatterns.extract_percentage("10%") == pytest.approx(0.10)
    assert FinvizPatterns.extract_percentage("0%") == pytest.approx(0.0)
    assert FinvizPatterns.extract_percentage("invalid") is None
    assert FinvizPatterns.extract_percentage("") is None


def test_utils_sanitization():
    """
    Test that new utils module sanitizes sensitive data.

    This verifies Phase 1B log sanitization implementation.
    """
    from finviz_weekly.utils import sanitize_url

    # Test API key masking
    url = "https://api.example.com?api_key=secret123&other=value"
    sanitized = sanitize_url(url)
    assert "secret123" not in sanitized
    assert "***MASKED***" in sanitized
    assert "other=value" in sanitized  # Non-sensitive params preserved

    # Test OpenAI key masking
    key = "sk-proj-abc123def456ghi789"
    sanitized = sanitize_url(key)
    assert "abc123" not in sanitized
    assert "sk-***MASKED***" in sanitized

    # Test token masking
    url = "https://api.example.com?token=bearer_token_12345"
    sanitized = sanitize_url(url)
    assert "bearer_token_12345" not in sanitized
    assert "***MASKED***" in sanitized

    # Test password masking
    url = "https://user:password123@example.com/path?password=secret"
    sanitized = sanitize_url(url)
    assert "password123" not in sanitized
    assert "secret" not in sanitized
    assert "***MASKED***" in sanitized

    # Test non-string input
    assert sanitize_url(None) == "None"
    assert sanitize_url(123) == "123"


def test_config_validation():
    """
    Test that config validation catches invalid inputs.

    This verifies Phase 1B config validation implementation.
    """
    from finviz_weekly.config import RateLimits, RunConfig, HttpConfig, env_config

    # Test RateLimits validation
    with pytest.raises(ValueError, match="rate_per_sec must be in"):
        RateLimits(rate_per_sec=0)  # Too low

    with pytest.raises(ValueError, match="rate_per_sec must be in"):
        RateLimits(rate_per_sec=100)  # Too high

    with pytest.raises(ValueError, match="page_sleep_min must be >= 0"):
        RateLimits(page_sleep_min=-1)

    with pytest.raises(ValueError, match="page_sleep_max.*must be >= page_sleep_min"):
        RateLimits(page_sleep_min=2.0, page_sleep_max=1.0)

    with pytest.raises(ValueError, match="concurrency must be in"):
        RateLimits(concurrency=0)

    with pytest.raises(ValueError, match="concurrency must be in"):
        RateLimits(concurrency=100)

    # Test HttpConfig validation
    with pytest.raises(ValueError, match="timeout_connect must be >= 1"):
        HttpConfig(proxy=None, timeout_connect=0)

    with pytest.raises(ValueError, match="max_retries must be in"):
        HttpConfig(proxy=None, max_retries=-1)

    with pytest.raises(ValueError, match="max_retries must be in"):
        HttpConfig(proxy=None, max_retries=20)

    # Test RunConfig validation (via env_config)
    with pytest.raises(ValueError, match="mode must be one of"):
        env_config(mode="invalid_mode", out_dir="/tmp/test")

    with pytest.raises(ValueError, match="ticker_limit must be >= 1"):
        env_config(mode="tickers", ticker_limit=0, out_dir="/tmp/test")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
