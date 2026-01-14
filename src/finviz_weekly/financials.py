"""Enhanced financial statement scraper for Finviz."""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from .config import HttpConfig
from .http import request_with_retries
from .parse import parse_human_number
from .selectors import FinvizSelectors, FinvizUrls

LOGGER = logging.getLogger(__name__)


def scrape_financial_statements(
    ticker: str, session, http_config: HttpConfig
) -> Dict[str, Dict]:
    """
    Scrape detailed financial statements from Finviz.
    
    Gets income statement, balance sheet, and cash flow data
    for multiple periods (annual and quarterly).
    
    Args:
        ticker: Stock ticker symbol
        session: Requests session
        http_config: HTTP configuration
        
    Returns:
        Dictionary with 'income_statement', 'balance_sheet', 'cash_flow' keys
    """
    result = {
        "income_statement": {},
        "balance_sheet": {},
        "cash_flow": {},
    }
    
    # Scrape each statement type
    for statement_type in ["income", "balance", "cash"]:
        data = _scrape_statement(ticker, statement_type, session, http_config)
        
        if statement_type == "income":
            result["income_statement"] = data
        elif statement_type == "balance":
            result["balance_sheet"] = data
        elif statement_type == "cash":
            result["cash_flow"] = data
    
    return result


def _scrape_statement(
    ticker: str, statement_type: str, session, http_config: HttpConfig
) -> Dict:
    """
    Scrape a specific financial statement.
    
    Args:
        ticker: Stock ticker symbol
        statement_type: One of 'income', 'balance', 'cash'
        session: Requests session
        http_config: HTTP configuration
        
    Returns:
        Dictionary with statement data
    """
    url = FinvizUrls.financials(ticker, statement_type)

    try:
        response = request_with_retries(session, url, http_config)
        soup = BeautifulSoup(response.text, "html.parser")

        # Find the financial table using centralized selector
        table = soup.select_one(FinvizSelectors.FINANCIAL_TABLE)
        if not table:
            LOGGER.warning(f"No {statement_type} statement table found for {ticker}")
            return {}

        data = {}
        rows = table.select(FinvizSelectors.FINANCIAL_ROW)
        
        # First row typically contains period headers
        header_row = rows[0] if rows else None
        periods = []
        
        if header_row:
            period_cells = header_row.select(FinvizSelectors.FINANCIAL_CELLS)
            # Skip first cell (empty) and extract period names
            periods = [cell.text.strip() for cell in period_cells[1:]]

        # Parse each metric row
        for row in rows[1:]:
            cells = row.select(FinvizSelectors.FINANCIAL_CELLS)
            if len(cells) < 2:
                continue
            
            metric_name = cells[0].text.strip()
            if not metric_name:
                continue
            
            # Parse values for each period
            values = {}
            for i, cell in enumerate(cells[1:], start=0):
                if i < len(periods):
                    period = periods[i]
                    value_text = cell.text.strip()
                    
                    # Parse the value
                    value = parse_human_number(value_text)
                    values[period] = value
            
            data[metric_name] = values
        
        LOGGER.info(f"Scraped {len(data)} metrics from {statement_type} statement for {ticker}")
        return data

    except (AttributeError, ValueError, KeyError, IndexError, TypeError) as e:
        LOGGER.error(f"Error scraping {statement_type} statement for {ticker}: {e}", exc_info=True)
        return {}


def calculate_financial_ratios(statements: Dict[str, Dict]) -> Dict[str, float]:
    """
    Calculate common financial ratios from statement data.
    
    Args:
        statements: Dictionary with income_statement, balance_sheet, cash_flow
        
    Returns:
        Dictionary of calculated ratios
    """
    ratios = {}
    
    try:
        income = statements.get("income_statement", {})
        balance = statements.get("balance_sheet", {})
        cash_flow = statements.get("cash_flow", {})
        
        # Get most recent period data (usually first column)
        def get_latest(data: Dict, metric: str) -> Optional[float]:
            if metric not in data:
                return None
            values = data[metric]
            if not values:
                return None
            # Get first value (most recent)
            return list(values.values())[0]
        
        # Profitability Ratios
        revenue = get_latest(income, "Revenue")
        net_income = get_latest(income, "Net Income")
        gross_profit = get_latest(income, "Gross Profit")
        
        if revenue and revenue != 0:
            if net_income is not None:
                ratios["net_margin"] = net_income / revenue
            if gross_profit is not None:
                ratios["gross_margin"] = gross_profit / revenue
        
        # Efficiency Ratios
        total_assets = get_latest(balance, "Total Assets")
        total_equity = get_latest(balance, "Total Equity")
        
        if total_assets and total_assets != 0:
            if net_income is not None:
                ratios["roa"] = net_income / total_assets  # Return on Assets
        
        if total_equity and total_equity != 0:
            if net_income is not None:
                ratios["roe"] = net_income / total_equity  # Return on Equity
        
        # Liquidity Ratios
        current_assets = get_latest(balance, "Current Assets")
        current_liabilities = get_latest(balance, "Current Liabilities")
        cash = get_latest(balance, "Cash")
        
        if current_liabilities and current_liabilities != 0:
            if current_assets is not None:
                ratios["current_ratio"] = current_assets / current_liabilities
            if cash is not None:
                ratios["cash_ratio"] = cash / current_liabilities
        
        # Leverage Ratios
        total_debt = get_latest(balance, "Total Debt")
        
        if total_equity and total_equity != 0:
            if total_debt is not None:
                ratios["debt_to_equity"] = total_debt / total_equity
        
        if total_assets and total_assets != 0:
            if total_debt is not None:
                ratios["debt_to_assets"] = total_debt / total_assets
        
        # Cash Flow Ratios
        operating_cash_flow = get_latest(cash_flow, "Cash from Operating Activities")
        capex = get_latest(cash_flow, "Capital Expenditures")
        
        if operating_cash_flow:
            if capex:
                ratios["free_cash_flow"] = operating_cash_flow + capex  # capex is negative
            
            if net_income and net_income != 0:
                ratios["cash_flow_to_income"] = operating_cash_flow / net_income

    except (AttributeError, ValueError, KeyError, TypeError, ZeroDivisionError) as e:
        LOGGER.error(f"Error calculating financial ratios: {e}", exc_info=True)

    return ratios


def compare_periods(statements: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Calculate period-over-period growth rates.
    
    Args:
        statements: Dictionary with financial statement data
        
    Returns:
        Dictionary with growth metrics
    """
    growth = {
        "revenue_growth": None,
        "income_growth": None,
        "asset_growth": None,
        "equity_growth": None,
    }
    
    try:
        income = statements.get("income_statement", {})
        balance = statements.get("balance_sheet", {})
        
        # Helper to calculate growth
        def calc_growth(data: Dict, metric: str) -> Optional[float]:
            if metric not in data:
                return None
            values = list(data[metric].values())
            if len(values) < 2:
                return None
            latest = values[0]
            previous = values[1]
            if previous and previous != 0:
                return (latest - previous) / abs(previous)
            return None
        
        growth["revenue_growth"] = calc_growth(income, "Revenue")
        growth["income_growth"] = calc_growth(income, "Net Income")
        growth["asset_growth"] = calc_growth(balance, "Total Assets")
        growth["equity_growth"] = calc_growth(balance, "Total Equity")

    except (AttributeError, ValueError, KeyError, IndexError, TypeError, ZeroDivisionError) as e:
        LOGGER.error(f"Error calculating period growth: {e}", exc_info=True)

    return growth


__all__ = [
    "scrape_financial_statements",
    "calculate_financial_ratios",
    "compare_periods",
]
