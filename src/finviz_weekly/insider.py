"""Scraper for Finviz insider trading data."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from .config import HttpConfig
from .http import request_with_retries
from .parse import parse_human_number
from .selectors import FinvizSelectors, FinvizUrls

LOGGER = logging.getLogger(__name__)


def scrape_insider_trading(ticker: str, session, http_config: HttpConfig) -> List[Dict]:
    """
    Scrape insider trading data for a ticker.
    
    Args:
        ticker: Stock ticker symbol
        session: Requests session
        http_config: HTTP configuration
        
    Returns:
        List of insider trading transactions
    """
    url = FinvizUrls.insider(ticker)

    try:
        response = request_with_retries(session, url, http_config)
        soup = BeautifulSoup(response.text, "html.parser")

        # Find the insider trading table using centralized selector
        table = soup.select_one(FinvizSelectors.INSIDER_TABLE)
        if not table:
            LOGGER.warning(f"No insider trading table found for {ticker}")
            return []

        transactions = []
        rows = table.select(FinvizSelectors.INSIDER_ROW)[1:]  # Skip header

        for row in rows:
            cells = row.select(FinvizSelectors.INSIDER_CELLS)
            if len(cells) < 9:
                continue
            
            try:
                transaction = {
                    "ticker": ticker,
                    "insider_name": cells[0].text.strip(),
                    "relationship": cells[1].text.strip(),
                    "date": cells[2].text.strip(),
                    "transaction_type": cells[3].text.strip(),
                    "cost": parse_human_number(cells[4].text.strip()),
                    "shares": parse_human_number(cells[5].text.strip()),
                    "value": parse_human_number(cells[6].text.strip()),
                    "shares_total": parse_human_number(cells[7].text.strip()),
                    "sec_form": cells[8].text.strip(),
                }
                transactions.append(transaction)
            except (AttributeError, IndexError, ValueError, KeyError, TypeError) as e:
                LOGGER.debug(f"Error parsing insider row for {ticker}: {e}")
                continue

        LOGGER.info(f"Found {len(transactions)} insider transactions for {ticker}")
        return transactions

    except (AttributeError, ValueError, KeyError) as e:
        LOGGER.error(f"Error scraping insider trading for {ticker}: {e}", exc_info=True)
        return []


def scrape_insider_summary(session, http_config: HttpConfig, 
                          filter_type: str = "buy") -> List[Dict]:
    """
    Scrape the main insider trading page for all recent transactions.
    
    Args:
        session: Requests session
        http_config: HTTP configuration
        filter_type: Filter by transaction type (buy, sell, all)
        
    Returns:
        List of recent insider transactions across all stocks
    """
    url = FinvizUrls.insider_summary(filter_type)

    try:
        response = request_with_retries(session, url, http_config)
        soup = BeautifulSoup(response.text, "html.parser")

        # Find the table using centralized selector
        table = soup.select_one(FinvizSelectors.INSIDER_TABLE)
        if not table:
            LOGGER.warning("No insider trading table found")
            return []

        transactions = []
        # Select rows with class "odd" or "even"
        rows = table.select(f"{FinvizSelectors.INSIDER_ROW}.odd, {FinvizSelectors.INSIDER_ROW}.even")

        for row in rows:
            cells = row.select(FinvizSelectors.INSIDER_CELLS)
            if len(cells) < 9:
                continue
            
            try:
                # Extract ticker from link
                ticker_link = cells[0].find("a")
                ticker = ticker_link["href"].split("t=")[1].split("&")[0] if ticker_link else ""
                
                transaction = {
                    "ticker": ticker,
                    "insider_name": cells[1].text.strip(),
                    "relationship": cells[2].text.strip(),
                    "date": cells[3].text.strip(),
                    "transaction_type": cells[4].text.strip(),
                    "cost": parse_human_number(cells[5].text.strip()),
                    "shares": parse_human_number(cells[6].text.strip()),
                    "value": parse_human_number(cells[7].text.strip()),
                    "shares_total": parse_human_number(cells[8].text.strip()),
                }
                transactions.append(transaction)
            except (AttributeError, IndexError, ValueError, KeyError, TypeError) as e:
                LOGGER.debug(f"Error parsing insider row: {e}")
                continue

        LOGGER.info(f"Found {len(transactions)} insider transactions (filter={filter_type})")
        return transactions

    except (AttributeError, ValueError, KeyError) as e:
        LOGGER.error(f"Error scraping insider trading summary: {e}", exc_info=True)
        return []


def aggregate_insider_by_ticker(transactions: List[Dict]) -> Dict[str, Dict]:
    """
    Aggregate insider trading data by ticker.
    
    Args:
        transactions: List of insider transactions
        
    Returns:
        Dictionary with aggregated stats per ticker
    """
    from collections import defaultdict
    
    stats = defaultdict(lambda: {
        "total_buys": 0,
        "total_sells": 0,
        "buy_value": 0.0,
        "sell_value": 0.0,
        "net_value": 0.0,
        "unique_insiders": set(),
        "transactions": [],
    })
    
    for txn in transactions:
        ticker = txn["ticker"]
        stats[ticker]["transactions"].append(txn)
        stats[ticker]["unique_insiders"].add(txn["insider_name"])

        txn_type_lower = txn["transaction_type"].lower()

        if "buy" in txn_type_lower or "purchase" in txn_type_lower:
            stats[ticker]["total_buys"] += 1
            if txn["value"]:
                stats[ticker]["buy_value"] += txn["value"]
        elif "sell" in txn_type_lower or "sale" in txn_type_lower:
            stats[ticker]["total_sells"] += 1
            if txn["value"]:
                stats[ticker]["sell_value"] += txn["value"]
    
    # Convert sets to counts and calculate net
    result = {}
    for ticker, data in stats.items():
        data["unique_insiders"] = len(data["unique_insiders"])
        data["net_value"] = data["buy_value"] - data["sell_value"]
        result[ticker] = data
    
    return result


__all__ = [
    "scrape_insider_trading",
    "scrape_insider_summary",
    "aggregate_insider_by_ticker",
]
