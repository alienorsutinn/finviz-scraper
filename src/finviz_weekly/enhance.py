"""Enhanced data collection and merging."""
from __future__ import annotations

import logging
from typing import Dict, List, Callable, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import pandas as pd
import requests

from .config import HttpConfig
from .insider import scrape_insider_trading, aggregate_insider_by_ticker
from .earnings import scrape_earnings_reactions, aggregate_earnings_stats
from .financials import scrape_financial_statements, calculate_financial_ratios

LOGGER = logging.getLogger(__name__)


def _scrape_with_retry(
    scraper_func: Callable,
    ticker: str,
    session: requests.Session,
    http_config: HttpConfig,
    max_retries: int = 3,
) -> Tuple[str, str, Any]:
    """
    Scrape data with exponential backoff retry logic.

    Args:
        scraper_func: Function to scrape data
        ticker: Ticker symbol
        session: Requests session
        http_config: HTTP configuration
        max_retries: Maximum number of retry attempts

    Returns:
        Tuple of (ticker, data_type, result)
    """
    data_type = scraper_func.__name__.replace("scrape_", "").replace("_trading", "").replace("_reactions", "").replace("_statements", "")

    for attempt in range(max_retries):
        try:
            result = scraper_func(ticker, session, http_config)
            return (ticker, data_type, result)
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                LOGGER.warning(
                    "%s scrape failed for %s (attempt %d/%d): %s. Retrying in %ds...",
                    data_type, ticker, attempt + 1, max_retries, e, wait_time
                )
                time.sleep(wait_time)
            else:
                LOGGER.error("%s scrape failed for %s after %d attempts: %s", data_type, ticker, max_retries, e)
                return (ticker, data_type, None)
        except Exception as e:
            LOGGER.error("%s scrape failed for %s: %s", data_type, ticker, e)
            return (ticker, data_type, None)

    return (ticker, data_type, None)


def _scrape_enhanced_data_parallel(
    tickers: List[str],
    session: requests.Session,
    http_config: HttpConfig,
    include_insider: bool,
    include_earnings: bool,
    include_financials: bool,
    max_workers: int,
) -> Dict[str, Dict]:
    """
    Scrape enhanced data using parallel workers.

    This function scrapes multiple data sources concurrently for each ticker,
    significantly speeding up data collection while respecting rate limits.
    """
    LOGGER.info("Starting parallel enhanced data scraping with %d workers", max_workers)

    enhanced_data = {}
    for ticker in tickers:
        enhanced_data[ticker] = {
            "insider": None,
            "earnings": None,
            "financials": None,
        }

    # Build list of scraping tasks
    tasks = []
    if include_insider:
        for ticker in tickers:
            tasks.append((scrape_insider_trading, ticker))

    if include_earnings:
        for ticker in tickers:
            tasks.append((scrape_earnings_reactions, ticker))

    if include_financials:
        for ticker in tickers:
            tasks.append((scrape_financial_statements, ticker))

    LOGGER.info("Queued %d scraping tasks for %d tickers", len(tasks), len(tickers))

    # Execute tasks in parallel
    all_insider = []
    all_earnings = []
    all_financials = []
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_task = {
            executor.submit(_scrape_with_retry, func, ticker, session, http_config): (func, ticker)
            for func, ticker in tasks
        }

        # Process results as they complete
        for future in as_completed(future_to_task):
            ticker, data_type, result = future.result()
            completed += 1

            if result:
                if data_type == "insider":
                    all_insider.extend(result)
                elif data_type == "earnings":
                    all_earnings.extend(result)
                elif data_type == "financials":
                    all_financials.append((ticker, result))

            if completed % 10 == 0:
                LOGGER.info("Progress: %d/%d tasks completed (%.0f%%)", completed, len(tasks), completed / len(tasks) * 100)

    LOGGER.info("Parallel scraping complete: %d/%d tasks finished", completed, len(tasks))

    # Aggregate insider data
    if all_insider:
        insider_stats = aggregate_insider_by_ticker(all_insider)
        for ticker, stats in insider_stats.items():
            if ticker in enhanced_data:
                enhanced_data[ticker]["insider"] = stats
        LOGGER.info("Aggregated insider data for %d tickers", len(insider_stats))

    # Aggregate earnings data
    if all_earnings:
        earnings_stats = aggregate_earnings_stats(all_earnings)
        for ticker, stats in earnings_stats.items():
            if ticker in enhanced_data:
                enhanced_data[ticker]["earnings"] = stats
        LOGGER.info("Aggregated earnings data for %d tickers", len(earnings_stats))

    # Process financial data
    if all_financials:
        for ticker, statements in all_financials:
            if ticker in enhanced_data and statements:
                ratios = calculate_financial_ratios(statements)
                enhanced_data[ticker]["financials"] = ratios
        LOGGER.info("Calculated financial ratios for %d tickers", len(all_financials))

    return enhanced_data


def scrape_enhanced_data(
    tickers: List[str],
    session: requests.Session,
    http_config: HttpConfig,
    include_insider: bool = False,
    include_earnings: bool = False,
    include_financials: bool = False,
    parallel: bool = True,
    max_workers: int = 3,
) -> Dict[str, Dict]:
    """
    Scrape enhanced data sources for given tickers.

    Args:
        tickers: List of ticker symbols
        session: Requests session
        http_config: HTTP configuration
        include_insider: Whether to scrape insider trading data
        include_earnings: Whether to scrape earnings reaction data
        include_financials: Whether to scrape financial statements
        parallel: Whether to use parallel scraping (default: True)
        max_workers: Maximum number of parallel workers (default: 3)

    Returns:
        Dictionary with enhanced data by ticker
    """
    enhanced_data = {}

    # Initialize structure for each ticker
    for ticker in tickers:
        enhanced_data[ticker] = {
            "insider": None,
            "earnings": None,
            "financials": None,
        }

    # Use parallel scraping if enabled and multiple sources requested
    sources_count = sum([include_insider, include_earnings, include_financials])
    if parallel and sources_count > 1:
        return _scrape_enhanced_data_parallel(
            tickers, session, http_config,
            include_insider, include_earnings, include_financials,
            max_workers
        )

    # Sequential scraping (fallback or single source)
    # Scrape insider trading
    if include_insider:
        LOGGER.info("Scraping insider trading for %d tickers...", len(tickers))
        all_insider = []
        success_count = 0
        for ticker in tickers:
            try:
                transactions = scrape_insider_trading(ticker, session, http_config)
                all_insider.extend(transactions)
                success_count += 1
            except Exception as e:
                LOGGER.warning("Insider scrape failed for %s: %s", ticker, e)

        if all_insider:
            insider_stats = aggregate_insider_by_ticker(all_insider)
            for ticker, stats in insider_stats.items():
                if ticker in enhanced_data:
                    enhanced_data[ticker]["insider"] = stats
            LOGGER.info("Collected insider data for %d/%d tickers", len(insider_stats), success_count)

    # Scrape earnings reactions
    if include_earnings:
        LOGGER.info("Scraping earnings reactions for %d tickers...", len(tickers))
        all_earnings = []
        success_count = 0
        for ticker in tickers:
            try:
                earnings = scrape_earnings_reactions(ticker, session, http_config)
                all_earnings.extend(earnings)
                success_count += 1
            except Exception as e:
                LOGGER.warning("Earnings scrape failed for %s: %s", ticker, e)

        if all_earnings:
            earnings_stats = aggregate_earnings_stats(all_earnings)
            for ticker, stats in earnings_stats.items():
                if ticker in enhanced_data:
                    enhanced_data[ticker]["earnings"] = stats
            LOGGER.info("Collected earnings data for %d/%d tickers", len(earnings_stats), success_count)

    # Scrape financial statements
    if include_financials:
        LOGGER.info("Scraping financial statements for %d tickers...", len(tickers))
        success_count = 0
        for ticker in tickers:
            try:
                statements = scrape_financial_statements(ticker, session, http_config)
                ratios = calculate_financial_ratios(statements)
                enhanced_data[ticker]["financials"] = {
                    "statements": statements,
                    "ratios": ratios,
                }
                success_count += 1
            except Exception as e:
                LOGGER.warning("Financial scrape failed for %s: %s", ticker, e)

        LOGGER.info("Collected financial data for %d tickers", success_count)

    return enhanced_data


def merge_enhanced_data(df: pd.DataFrame, enhanced_data: Dict[str, Dict]) -> pd.DataFrame:
    """
    Merge enhanced data into fundamentals dataframe.

    Args:
        df: Fundamentals dataframe with 'ticker' column
        enhanced_data: Enhanced data dictionary from scrape_enhanced_data()

    Returns:
        Enhanced dataframe with new columns
    """
    enhanced_df = df.copy()

    # Helper function to safely get nested values
    def safe_get(ticker, *keys, default=None):
        value = enhanced_data.get(ticker, {})
        for key in keys:
            if value is None:
                return default
            value = value.get(key) if isinstance(value, dict) else None
        return value if value is not None else default

    # Add insider columns
    enhanced_df["insider_net_value"] = enhanced_df["ticker"].map(
        lambda t: safe_get(t, "insider", "net_value", default=0.0)
    )
    enhanced_df["insider_total_buys"] = enhanced_df["ticker"].map(
        lambda t: safe_get(t, "insider", "total_buys", default=0)
    )
    enhanced_df["insider_total_sells"] = enhanced_df["ticker"].map(
        lambda t: safe_get(t, "insider", "total_sells", default=0)
    )
    enhanced_df["insider_buy_value"] = enhanced_df["ticker"].map(
        lambda t: safe_get(t, "insider", "buy_value", default=0.0)
    )
    enhanced_df["insider_sell_value"] = enhanced_df["ticker"].map(
        lambda t: safe_get(t, "insider", "sell_value", default=0.0)
    )

    # Add earnings columns
    enhanced_df["earnings_avg_alpha"] = enhanced_df["ticker"].map(
        lambda t: safe_get(t, "earnings", "avg_day_0_alpha", default=0.0)
    )
    enhanced_df["earnings_avg_rsi"] = enhanced_df["ticker"].map(
        lambda t: safe_get(t, "earnings", "avg_rsi", default=50.0)
    )
    enhanced_df["earnings_win_rate"] = enhanced_df["ticker"].map(
        lambda t: safe_get(t, "earnings", "positive_reaction_pct", default=0.0)
    )
    enhanced_df["earnings_total_events"] = enhanced_df["ticker"].map(
        lambda t: safe_get(t, "earnings", "total_events", default=0)
    )

    # Add financial ratio columns
    enhanced_df["net_margin"] = enhanced_df["ticker"].map(
        lambda t: safe_get(t, "financials", "ratios", "net_margin", default=None)
    )
    enhanced_df["gross_margin"] = enhanced_df["ticker"].map(
        lambda t: safe_get(t, "financials", "ratios", "gross_margin", default=None)
    )
    enhanced_df["roe"] = enhanced_df["ticker"].map(
        lambda t: safe_get(t, "financials", "ratios", "roe", default=None)
    )
    enhanced_df["roa"] = enhanced_df["ticker"].map(
        lambda t: safe_get(t, "financials", "ratios", "roa", default=None)
    )
    enhanced_df["debt_to_equity"] = enhanced_df["ticker"].map(
        lambda t: safe_get(t, "financials", "ratios", "debt_to_equity", default=None)
    )
    enhanced_df["current_ratio"] = enhanced_df["ticker"].map(
        lambda t: safe_get(t, "financials", "ratios", "current_ratio", default=None)
    )
    enhanced_df["quick_ratio"] = enhanced_df["ticker"].map(
        lambda t: safe_get(t, "financials", "ratios", "quick_ratio", default=None)
    )

    new_columns = len(enhanced_df.columns) - len(df.columns)
    LOGGER.info("Added %d enhanced columns to dataframe", new_columns)

    return enhanced_df


__all__ = [
    "scrape_enhanced_data",
    "merge_enhanced_data",
]
