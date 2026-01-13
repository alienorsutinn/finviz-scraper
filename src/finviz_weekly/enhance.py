"""Enhanced data collection and merging."""
from __future__ import annotations

import logging
from typing import Dict, List
import pandas as pd
import requests

from .config import HttpConfig
from .insider import scrape_insider_trading, aggregate_insider_by_ticker
from .earnings import scrape_earnings_reactions, aggregate_earnings_stats
from .financials import scrape_financial_statements, calculate_financial_ratios

LOGGER = logging.getLogger(__name__)


def scrape_enhanced_data(
    tickers: List[str],
    session: requests.Session,
    http_config: HttpConfig,
    include_insider: bool = False,
    include_earnings: bool = False,
    include_financials: bool = False,
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
