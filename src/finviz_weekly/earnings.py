"""Scraper for Finviz earnings reaction data."""
from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from .config import HttpConfig
from .http import request_with_retries
from .parse import parse_percent
from .selectors import FinvizSelectors, FinvizUrls

LOGGER = logging.getLogger(__name__)


def scrape_earnings_reactions(ticker: str, session, http_config: HttpConfig) -> List[Dict]:
    """
    Scrape historical earnings reaction data for a ticker.
    
    This includes price movements from -3 days to +1 week around earnings,
    compared to SPY performance, along with RSI at the time.
    
    Args:
        ticker: Stock ticker symbol
        session: Requests session
        http_config: HTTP configuration
        
    Returns:
        List of earnings reaction data points
    """
    url = FinvizUrls.earnings_reactions(ticker)

    try:
        response = request_with_retries(session, url, http_config)
        soup = BeautifulSoup(response.text, "html.parser")

        # Find earnings table using centralized selector
        tables = soup.select(FinvizSelectors.EARNINGS_REACTION_TABLE)
        
        earnings_data = []
        
        for table in tables:
            # Check if this is the earnings reaction table
            header = table.select_one(FinvizSelectors.EARNINGS_REACTION_ROW)
            if not header or "Report Date" not in header.text:
                continue

            rows = table.select(FinvizSelectors.EARNINGS_REACTION_ROW)[1:]  # Skip header

            for row in rows:
                cells = row.select(FinvizSelectors.EARNINGS_REACTION_CELLS)
                if len(cells) < 15:  # Need all columns
                    continue
                
                try:
                    # Parse report date and time
                    date_text = cells[0].text.strip()
                    date_match = re.search(r"(\w+ \d+, \d+)", date_text)
                    report_date = date_match.group(1) if date_match else date_text
                    
                    # Determine if BMO (before market open) or AMC (after market close)
                    timing = "BMO" if "BMO" in date_text else "AMC" if "AMC" in date_text else "Unknown"
                    
                    # Parse quarter
                    quarter_text = cells[1].text.strip() if len(cells) > 1 else ""
                    
                    # Price movements
                    prices = {
                        "day_minus_3": _parse_price(cells[2]) if len(cells) > 2 else None,
                        "day_minus_2": _parse_price(cells[3]) if len(cells) > 3 else None,
                        "day_minus_1": _parse_price(cells[4]) if len(cells) > 4 else None,
                        "open": _parse_price(cells[5]) if len(cells) > 5 else None,
                        "high": _parse_price(cells[6]) if len(cells) > 6 else None,
                        "low": _parse_price(cells[7]) if len(cells) > 7 else None,
                        "close": _parse_price(cells[8]) if len(cells) > 8 else None,
                        "day_plus_1": _parse_price(cells[9]) if len(cells) > 9 else None,
                        "day_plus_2": _parse_price(cells[10]) if len(cells) > 10 else None,
                        "day_plus_3": _parse_price(cells[11]) if len(cells) > 11 else None,
                        "week_plus_1": _parse_price(cells[12]) if len(cells) > 12 else None,
                        "week_minus_1": _parse_price(cells[13]) if len(cells) > 13 else None,
                    }
                    
                    # Find the percentage changes row (next row)
                    pct_row = row.find_next_sibling(FinvizSelectors.EARNINGS_REACTION_ROW)
                    if pct_row:
                        pct_cells = pct_row.select(FinvizSelectors.EARNINGS_REACTION_CELLS)
                        
                        # Stock percentage changes
                        stock_changes = {}
                        if len(pct_cells) > 1 and ticker in pct_cells[0].text:
                            stock_changes = {
                                "day_minus_3_pct": parse_percent(pct_cells[1].text) if len(pct_cells) > 1 else None,
                                "day_minus_2_pct": parse_percent(pct_cells[2].text) if len(pct_cells) > 2 else None,
                                "day_minus_1_pct": parse_percent(pct_cells[3].text) if len(pct_cells) > 3 else None,
                                "day_0_pct": parse_percent(pct_cells[4].text) if len(pct_cells) > 4 else None,
                                "day_plus_1_pct": parse_percent(pct_cells[5].text) if len(pct_cells) > 5 else None,
                                "day_plus_2_pct": parse_percent(pct_cells[6].text) if len(pct_cells) > 6 else None,
                                "day_plus_3_pct": parse_percent(pct_cells[7].text) if len(pct_cells) > 7 else None,
                                "week_plus_1_pct": parse_percent(pct_cells[8].text) if len(pct_cells) > 8 else None,
                                "week_minus_1_pct": parse_percent(pct_cells[9].text) if len(pct_cells) > 9 else None,
                            }
                            
                            # RSI
                            rsi_cell = pct_cells[10] if len(pct_cells) > 10 else None
                            rsi = int(rsi_cell.text.strip()) if rsi_cell and rsi_cell.text.strip().isdigit() else None
                        
                        # SPY comparison row
                        spy_row = pct_row.find_next_sibling(FinvizSelectors.EARNINGS_REACTION_ROW)
                        spy_changes = {}
                        if spy_row:
                            spy_cells = spy_row.select(FinvizSelectors.EARNINGS_REACTION_CELLS)
                            if len(spy_cells) > 1 and "SPY" in spy_cells[0].text:
                                spy_changes = {
                                    "spy_day_minus_3_pct": parse_percent(spy_cells[1].text) if len(spy_cells) > 1 else None,
                                    "spy_day_minus_2_pct": parse_percent(spy_cells[2].text) if len(spy_cells) > 2 else None,
                                    "spy_day_minus_1_pct": parse_percent(spy_cells[3].text) if len(spy_cells) > 3 else None,
                                    "spy_day_0_pct": parse_percent(spy_cells[4].text) if len(spy_cells) > 4 else None,
                                    "spy_day_plus_1_pct": parse_percent(spy_cells[5].text) if len(spy_cells) > 5 else None,
                                    "spy_day_plus_2_pct": parse_percent(spy_cells[6].text) if len(spy_cells) > 6 else None,
                                    "spy_day_plus_3_pct": parse_percent(spy_cells[7].text) if len(spy_cells) > 7 else None,
                                    "spy_week_plus_1_pct": parse_percent(spy_cells[8].text) if len(spy_cells) > 8 else None,
                                    "spy_week_minus_1_pct": parse_percent(spy_cells[9].text) if len(spy_cells) > 9 else None,
                                }
                    
                    earnings_event = {
                        "ticker": ticker,
                        "report_date": report_date,
                        "timing": timing,
                        "quarter": quarter_text,
                        "rsi": rsi,
                        **prices,
                        **stock_changes,
                        **spy_changes,
                    }
                    
                    # Calculate alpha (stock performance vs SPY)
                    if stock_changes.get("day_0_pct") and spy_changes.get("spy_day_0_pct"):
                        earnings_event["day_0_alpha"] = stock_changes["day_0_pct"] - spy_changes["spy_day_0_pct"]
                    
                    if stock_changes.get("week_plus_1_pct") and spy_changes.get("spy_week_plus_1_pct"):
                        earnings_event["week_plus_1_alpha"] = (
                            stock_changes["week_plus_1_pct"] - spy_changes["spy_week_plus_1_pct"]
                        )
                    
                    earnings_data.append(earnings_event)
                    
                except (AttributeError, IndexError, ValueError, KeyError, TypeError) as e:
                    LOGGER.debug(f"Error parsing earnings row for {ticker}: {e}")
                    continue

        LOGGER.info(f"Found {len(earnings_data)} earnings events for {ticker}")
        return earnings_data

    except (requests.exceptions.RequestException, AttributeError, ValueError) as e:
        LOGGER.error(f"Error scraping earnings reactions for {ticker}: {e}", exc_info=True)
        return []


def _parse_price(cell) -> Optional[float]:
    """Parse price from table cell."""
    if not cell:
        return None
    
    try:
        text = cell.text.strip()
        # Remove any non-numeric characters except decimal point
        text = re.sub(r"[^\d.]", "", text)
        return float(text) if text else None
    except (ValueError, AttributeError):
        return None


def calculate_earnings_statistics(earnings_data: List[Dict]) -> Dict:
    """
    Calculate aggregate statistics from earnings data.
    
    Args:
        earnings_data: List of earnings events
        
    Returns:
        Dictionary with aggregate statistics
    """
    if not earnings_data:
        return {}
    
    stats = {
        "total_events": len(earnings_data),
        "avg_day_0_change": 0.0,
        "avg_week_1_change": 0.0,
        "positive_reactions": 0,
        "negative_reactions": 0,
        "avg_rsi": 0.0,
        "avg_alpha_day_0": 0.0,
        "avg_alpha_week_1": 0.0,
    }
    
    day_0_changes = []
    week_1_changes = []
    rsi_values = []
    alpha_day_0 = []
    alpha_week_1 = []
    
    for event in earnings_data:
        if event.get("day_0_pct") is not None:
            day_0_changes.append(event["day_0_pct"])
            if event["day_0_pct"] > 0:
                stats["positive_reactions"] += 1
            else:
                stats["negative_reactions"] += 1
        
        if event.get("week_plus_1_pct") is not None:
            week_1_changes.append(event["week_plus_1_pct"])
        
        if event.get("rsi") is not None:
            rsi_values.append(event["rsi"])
        
        if event.get("day_0_alpha") is not None:
            alpha_day_0.append(event["day_0_alpha"])
        
        if event.get("week_plus_1_alpha") is not None:
            alpha_week_1.append(event["week_plus_1_alpha"])
    
    if day_0_changes:
        stats["avg_day_0_change"] = sum(day_0_changes) / len(day_0_changes)
    
    if week_1_changes:
        stats["avg_week_1_change"] = sum(week_1_changes) / len(week_1_changes)
    
    if rsi_values:
        stats["avg_rsi"] = sum(rsi_values) / len(rsi_values)
    
    if alpha_day_0:
        stats["avg_alpha_day_0"] = sum(alpha_day_0) / len(alpha_day_0)
    
    if alpha_week_1:
        stats["avg_alpha_week_1"] = sum(alpha_week_1) / len(alpha_week_1)
    
    return stats


def aggregate_earnings_stats(earnings_data: List[Dict]) -> Dict[str, Dict]:
    """
    Aggregate earnings statistics by ticker.

    Args:
        earnings_data: List of earnings events from multiple tickers

    Returns:
        Dictionary mapping ticker to earnings statistics
    """
    from collections import defaultdict

    # Group by ticker
    by_ticker = defaultdict(list)
    for event in earnings_data:
        ticker = event.get("ticker")
        if ticker:
            by_ticker[ticker].append(event)

    # Calculate stats for each ticker
    result = {}
    for ticker, events in by_ticker.items():
        stats = calculate_earnings_statistics(events)

        # Add ticker-specific aggregations
        result[ticker] = {
            "total_events": stats["total_events"],
            "avg_day_0_change": stats["avg_day_0_change"],
            "avg_week_1_change": stats["avg_week_1_change"],
            "avg_day_0_alpha": stats["avg_alpha_day_0"],
            "avg_week_1_alpha": stats["avg_alpha_week_1"],
            "avg_rsi": stats["avg_rsi"],
            "positive_reactions": stats["positive_reactions"],
            "negative_reactions": stats["negative_reactions"],
            "positive_reaction_pct": (
                stats["positive_reactions"] / stats["total_events"] * 100 if stats["total_events"] > 0 else 0
            ),
        }

    return result


__all__ = [
    "scrape_earnings_reactions",
    "calculate_earnings_statistics",
    "aggregate_earnings_stats",
]
