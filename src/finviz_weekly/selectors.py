"""
CSS selectors and HTML parsing constants for Finviz pages.

This module centralizes all HTML selectors in one place, making it easy
to update if Finviz changes their page structure.

Usage:
    from finviz_weekly.selectors import FinvizSelectors

    table = soup.select_one(FinvizSelectors.EARNINGS_TABLE)
    rows = table.select(FinvizSelectors.EARNINGS_ROW)
"""


class FinvizSelectors:
    """CSS selectors for Finviz pages - single source of truth."""

    # Earnings reaction page selectors
    EARNINGS_REACTION_TABLE = "table.fullview-ratings-outer"
    EARNINGS_REACTION_ROW = "tr"
    EARNINGS_REACTION_CELLS = "td"

    # General earnings selectors
    EARNINGS_TABLE = "table.body-table, table.snapshot-table2"
    EARNINGS_ROW = "tr.styled-row, tr"
    EARNINGS_DATE_CELL = "td:first-child"
    EARNINGS_PCT_CELLS = "td"

    # Insider trading page selectors
    INSIDER_TABLE = "table.body-table, table.snapshot-table2"
    INSIDER_ROW = "tr"
    INSIDER_CELLS = "td"

    # Financial statements page selectors
    FINANCIAL_TABLE = "table.snapshot-table2, table.body-table"
    FINANCIAL_ROW = "tr"
    FINANCIAL_CELLS = "td"

    # Screener page selectors
    SCREENER_TABLE = "table.screener-table, table"
    SCREENER_ROW = "tr"
    SCREENER_TICKER_LINK = "a.screener-link-primary"

    # Quote page selectors
    QUOTE_FUNDAMENTALS_TABLE = "table.snapshot-table2"
    QUOTE_TABLE_ROW = "tr"
    QUOTE_TABLE_CELLS = "td"

    @classmethod
    def get_earnings_table(cls) -> str:
        """Get selector for earnings table."""
        return cls.EARNINGS_TABLE

    @classmethod
    def get_insider_table(cls) -> str:
        """Get selector for insider trading table."""
        return cls.INSIDER_TABLE

    @classmethod
    def get_financial_table(cls) -> str:
        """Get selector for financial statements table."""
        return cls.FINANCIAL_TABLE

    @classmethod
    def validate_selectors(cls):
        """
        Validate that all selectors are non-empty strings.

        Raises:
            ValueError: If any selector is invalid

        Example:
            >>> FinvizSelectors.validate_selectors()  # Passes silently
        """
        import inspect

        for name, value in inspect.getmembers(cls):
            if name.isupper() and isinstance(value, str):
                if not value.strip():
                    raise ValueError(f"Selector {name} is empty")


class FinvizUrls:
    """
    URL patterns for Finviz pages.

    Usage:
        url = FinvizUrls.earnings("AAPL")
        # Returns: "https://finviz.com/quote.ashx?t=AAPL&p=d"
    """

    BASE = "https://finviz.com"

    @classmethod
    def quote(cls, ticker: str) -> str:
        """Get quote page URL for a ticker."""
        return f"{cls.BASE}/quote.ashx?t={ticker}"

    @classmethod
    def earnings(cls, ticker: str) -> str:
        """Get earnings page URL for a ticker."""
        return f"{cls.BASE}/quote.ashx?t={ticker}&p=d"

    @classmethod
    def earnings_reactions(cls, ticker: str) -> str:
        """Get earnings reactions page URL for a ticker."""
        return f"{cls.BASE}/quote.ashx?t={ticker}&p=d&ty=ea"

    @classmethod
    def insider(cls, ticker: str) -> str:
        """Get insider trading page URL for a ticker."""
        return f"{cls.BASE}/quote.ashx?t={ticker}&ty=sec&p=it"

    @classmethod
    def insider_summary(cls, filter_type: str = "all") -> str:
        """
        Get insider trading summary page URL.

        Args:
            filter_type: Filter type ("buy", "sell", or "all")

        Returns:
            URL for insider trading summary page
        """
        filter_map = {
            "buy": "?tc=1",
            "sell": "?tc=2",
            "all": "",
        }
        return f"{cls.BASE}/insidertrading.ashx{filter_map.get(filter_type, '')}"

    @classmethod
    def financials(cls, ticker: str, statement_type: str = "income") -> str:
        """
        Get financial statements page URL for a ticker.

        Args:
            ticker: Stock ticker symbol
            statement_type: Type of statement ("income", "balance", "cash")

        Returns:
            URL for financial statement page
        """
        type_map = {
            "income": "is",  # Income Statement
            "balance": "bs",  # Balance Sheet
            "cash": "cf",    # Cash Flow
        }
        param = type_map.get(statement_type, "is")
        return f"{cls.BASE}/quote.ashx?t={ticker}&p=d&ty={param}"

    @classmethod
    def screener(cls, filters: str = "") -> str:
        """Get screener URL with optional filters."""
        if filters:
            return f"{cls.BASE}/screener.ashx?{filters}"
        return f"{cls.BASE}/screener.ashx"


class FinvizPatterns:
    """
    Common regex patterns and text matching for Finviz data.

    Usage:
        import re
        if re.match(FinvizPatterns.TICKER, text):
            ...
    """

    # Ticker symbol pattern (1-5 uppercase letters/digits)
    TICKER = r'^[A-Z]{1,5}$'

    # Market cap pattern (e.g., "10.5B", "500M")
    MARKET_CAP = r'^\$?(\d+\.?\d*)\s*([BMK])$'

    # Percentage pattern (e.g., "+5.23%", "-1.5%")
    PERCENTAGE = r'^([+-]?\d+\.?\d*)\s*%$'

    # Price pattern (e.g., "$125.50")
    PRICE = r'^\$?(\d+\.?\d*)$'

    # Date pattern (various formats)
    DATE_MMDDYYYY = r'^(\d{1,2})/(\d{1,2})/(\d{4})$'
    DATE_ISO = r'^(\d{4})-(\d{2})-(\d{2})$'

    @classmethod
    def is_ticker(cls, text: str) -> bool:
        """Check if text looks like a ticker symbol."""
        import re
        return bool(re.match(cls.TICKER, text.strip().upper()))

    @classmethod
    def extract_percentage(cls, text: str) -> float | None:
        """
        Extract percentage as decimal from text.

        Args:
            text: Text like "+5.23%" or "-1.5%"

        Returns:
            Decimal value (0.0523 or -0.015) or None if invalid

        Example:
            >>> FinvizPatterns.extract_percentage("+5.23%")
            0.0523
            >>> FinvizPatterns.extract_percentage("-1.5%")
            -0.015
        """
        import re
        match = re.match(cls.PERCENTAGE, text.strip())
        if match:
            try:
                return float(match.group(1)) / 100.0
            except ValueError:
                return None
        return None


# Validate selectors at module load time
FinvizSelectors.validate_selectors()


if __name__ == "__main__":
    # Test the selectors
    print("=== Finviz Selectors ===")
    print(f"Earnings table: {FinvizSelectors.EARNINGS_TABLE}")
    print(f"Insider table: {FinvizSelectors.INSIDER_TABLE}")
    print(f"Financial table: {FinvizSelectors.FINANCIAL_TABLE}")

    print("\n=== Finviz URLs ===")
    print(f"Quote: {FinvizUrls.quote('AAPL')}")
    print(f"Earnings: {FinvizUrls.earnings('AAPL')}")
    print(f"Insider: {FinvizUrls.insider('AAPL')}")
    print(f"Financials: {FinvizUrls.financials('AAPL')}")

    print("\n=== Finviz Patterns ===")
    print(f"Is 'AAPL' a ticker? {FinvizPatterns.is_ticker('AAPL')}")
    print(f"Is 'Apple' a ticker? {FinvizPatterns.is_ticker('Apple')}")
    print(f"Extract '+5.23%': {FinvizPatterns.extract_percentage('+5.23%')}")
    print(f"Extract '-1.5%': {FinvizPatterns.extract_percentage('-1.5%')}")

    print("\n✅ Selectors module loaded successfully")
