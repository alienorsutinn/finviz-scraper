"""
Tests for selectors module - comprehensive coverage.

This ensures all selector definitions and URL builders work correctly.
"""
import pytest
from finviz_weekly.selectors import FinvizSelectors, FinvizUrls, FinvizPatterns


class TestFinvizSelectors:
    """Test FinvizSelectors class."""

    def test_earnings_reaction_selectors(self):
        """Test earnings reaction selectors exist and are non-empty."""
        assert isinstance(FinvizSelectors.EARNINGS_REACTION_TABLE, str)
        assert len(FinvizSelectors.EARNINGS_REACTION_TABLE) > 0
        assert isinstance(FinvizSelectors.EARNINGS_REACTION_ROW, str)
        assert len(FinvizSelectors.EARNINGS_REACTION_ROW) > 0
        assert isinstance(FinvizSelectors.EARNINGS_REACTION_CELLS, str)
        assert len(FinvizSelectors.EARNINGS_REACTION_CELLS) > 0

    def test_general_earnings_selectors(self):
        """Test general earnings selectors."""
        assert isinstance(FinvizSelectors.EARNINGS_TABLE, str)
        assert len(FinvizSelectors.EARNINGS_TABLE) > 0
        assert isinstance(FinvizSelectors.EARNINGS_ROW, str)
        assert isinstance(FinvizSelectors.EARNINGS_DATE_CELL, str)
        assert isinstance(FinvizSelectors.EARNINGS_PCT_CELLS, str)

    def test_insider_selectors(self):
        """Test insider trading selectors."""
        assert isinstance(FinvizSelectors.INSIDER_TABLE, str)
        assert len(FinvizSelectors.INSIDER_TABLE) > 0
        assert isinstance(FinvizSelectors.INSIDER_ROW, str)
        assert isinstance(FinvizSelectors.INSIDER_CELLS, str)

    def test_financial_selectors(self):
        """Test financial statement selectors."""
        assert isinstance(FinvizSelectors.FINANCIAL_TABLE, str)
        assert len(FinvizSelectors.FINANCIAL_TABLE) > 0
        assert isinstance(FinvizSelectors.FINANCIAL_ROW, str)
        assert isinstance(FinvizSelectors.FINANCIAL_CELLS, str)

    def test_screener_selectors(self):
        """Test screener selectors."""
        assert isinstance(FinvizSelectors.SCREENER_TABLE, str)
        assert len(FinvizSelectors.SCREENER_TABLE) > 0
        assert isinstance(FinvizSelectors.SCREENER_ROW, str)
        assert isinstance(FinvizSelectors.SCREENER_TICKER_LINK, str)

    def test_quote_selectors(self):
        """Test quote page selectors."""
        assert isinstance(FinvizSelectors.QUOTE_FUNDAMENTALS_TABLE, str)
        assert len(FinvizSelectors.QUOTE_FUNDAMENTALS_TABLE) > 0
        assert isinstance(FinvizSelectors.QUOTE_TABLE_ROW, str)
        assert isinstance(FinvizSelectors.QUOTE_TABLE_CELLS, str)

    def test_get_earnings_table(self):
        """Test earnings table getter."""
        result = FinvizSelectors.get_earnings_table()
        assert result == FinvizSelectors.EARNINGS_TABLE

    def test_get_insider_table(self):
        """Test insider table getter."""
        result = FinvizSelectors.get_insider_table()
        assert result == FinvizSelectors.INSIDER_TABLE

    def test_get_financial_table(self):
        """Test financial table getter."""
        result = FinvizSelectors.get_financial_table()
        assert result == FinvizSelectors.FINANCIAL_TABLE

    def test_validate_selectors(self):
        """Test selector validation."""
        # Should not raise for valid selectors
        FinvizSelectors.validate_selectors()


class TestFinvizUrls:
    """Test FinvizUrls class."""

    def test_base_url(self):
        """Test base URL is defined."""
        assert FinvizUrls.BASE == "https://finviz.com"

    def test_quote_url(self):
        """Test quote URL builder."""
        url = FinvizUrls.quote("AAPL")
        assert "https://finviz.com/quote.ashx" in url
        assert "t=AAPL" in url

    def test_earnings_url(self):
        """Test earnings URL builder."""
        url = FinvizUrls.earnings("MSFT")
        assert "https://finviz.com/quote.ashx" in url
        assert "t=MSFT" in url
        assert "p=d" in url

    def test_earnings_reactions_url(self):
        """Test earnings reactions URL builder."""
        url = FinvizUrls.earnings_reactions("GOOGL")
        assert "https://finviz.com/quote.ashx" in url
        assert "t=GOOGL" in url
        assert "p=d" in url
        assert "ty=ea" in url

    def test_insider_url(self):
        """Test insider trading URL builder."""
        url = FinvizUrls.insider("TSLA")
        assert "https://finviz.com/quote.ashx" in url
        assert "t=TSLA" in url
        assert "ty=sec" in url
        assert "p=it" in url

    def test_insider_summary_url_buy(self):
        """Test insider summary URL with buy filter."""
        url = FinvizUrls.insider_summary("buy")
        assert "https://finviz.com/insidertrading.ashx" in url
        assert "tc=1" in url

    def test_insider_summary_url_sell(self):
        """Test insider summary URL with sell filter."""
        url = FinvizUrls.insider_summary("sell")
        assert "https://finviz.com/insidertrading.ashx" in url
        assert "tc=2" in url

    def test_insider_summary_url_all(self):
        """Test insider summary URL with all filter."""
        url = FinvizUrls.insider_summary("all")
        assert "https://finviz.com/insidertrading.ashx" in url
        assert "tc=" not in url  # No filter param for "all"

    def test_insider_summary_url_default(self):
        """Test insider summary URL with default (all)."""
        url = FinvizUrls.insider_summary()
        assert "https://finviz.com/insidertrading.ashx" in url
        assert "tc=" not in url

    def test_financials_url_income(self):
        """Test financials URL for income statement."""
        url = FinvizUrls.financials("NVDA", "income")
        assert "https://finviz.com/quote.ashx" in url
        assert "t=NVDA" in url
        assert "ty=is" in url

    def test_financials_url_balance(self):
        """Test financials URL for balance sheet."""
        url = FinvizUrls.financials("AMD", "balance")
        assert "https://finviz.com/quote.ashx" in url
        assert "t=AMD" in url
        assert "ty=bs" in url

    def test_financials_url_cash(self):
        """Test financials URL for cash flow."""
        url = FinvizUrls.financials("INTC", "cash")
        assert "https://finviz.com/quote.ashx" in url
        assert "t=INTC" in url
        assert "ty=cf" in url

    def test_financials_url_default(self):
        """Test financials URL with default (income)."""
        url = FinvizUrls.financials("META")
        assert "https://finviz.com/quote.ashx" in url
        assert "t=META" in url
        assert "ty=is" in url  # Default to income statement

    def test_financials_url_invalid_type(self):
        """Test financials URL with invalid type defaults to income."""
        url = FinvizUrls.financials("NFLX", "invalid")
        assert "ty=is" in url  # Should default to income

    def test_screener_url_no_filters(self):
        """Test screener URL without filters."""
        url = FinvizUrls.screener()
        assert "https://finviz.com/screener.ashx" in url
        # Should not have query params
        assert url == "https://finviz.com/screener.ashx"

    def test_screener_url_with_filters(self):
        """Test screener URL with filters."""
        url = FinvizUrls.screener("v=111&f=cap_mega")
        assert "https://finviz.com/screener.ashx" in url
        assert "v=111" in url
        assert "f=cap_mega" in url

    def test_screener_url_complex_filters(self):
        """Test screener URL with complex filters."""
        filters = "v=111&f=cap_mega,ind_technology&r=1"
        url = FinvizUrls.screener(filters)
        assert "https://finviz.com/screener.ashx" in url
        assert "v=111" in url
        assert "cap_mega" in url
        assert "ind_technology" in url
        assert "r=1" in url


class TestFinvizPatterns:
    """Test FinvizPatterns class."""

    def test_ticker_pattern_exists(self):
        """Test ticker pattern is defined."""
        assert hasattr(FinvizPatterns, "TICKER")
        assert isinstance(FinvizPatterns.TICKER, str)

    def test_market_cap_pattern_exists(self):
        """Test market cap pattern is defined."""
        assert hasattr(FinvizPatterns, "MARKET_CAP")
        assert isinstance(FinvizPatterns.MARKET_CAP, str)

    def test_percentage_pattern_exists(self):
        """Test percentage pattern is defined."""
        assert hasattr(FinvizPatterns, "PERCENTAGE")
        assert isinstance(FinvizPatterns.PERCENTAGE, str)

    def test_is_ticker_valid_symbols(self):
        """Test is_ticker with valid ticker symbols."""
        assert FinvizPatterns.is_ticker("AAPL")
        assert FinvizPatterns.is_ticker("MSFT")
        assert FinvizPatterns.is_ticker("A")
        assert FinvizPatterns.is_ticker("GOOGL")
        # Dots are not in the pattern, so BRK.B would fail
        # Pattern is ^[A-Z]{1,5}$ - only letters, 1-5 chars

    def test_is_ticker_invalid_symbols(self):
        """Test is_ticker with invalid symbols."""
        assert not FinvizPatterns.is_ticker("Apple Inc.")
        assert not FinvizPatterns.is_ticker("123")
        assert not FinvizPatterns.is_ticker("")
        assert not FinvizPatterns.is_ticker("TOOLONG")  # More than 5 chars
        # "abc" will be uppercased to "ABC" which is valid
        assert not FinvizPatterns.is_ticker("BRK.B")  # Dot not allowed

    def test_extract_percentage_positive(self):
        """Test extract_percentage with positive values."""
        assert FinvizPatterns.extract_percentage("+5.23%") == pytest.approx(0.0523)
        assert FinvizPatterns.extract_percentage("10%") == pytest.approx(0.10)
        assert FinvizPatterns.extract_percentage("+0.5%") == pytest.approx(0.005)

    def test_extract_percentage_negative(self):
        """Test extract_percentage with negative values."""
        assert FinvizPatterns.extract_percentage("-1.5%") == pytest.approx(-0.015)
        assert FinvizPatterns.extract_percentage("-10%") == pytest.approx(-0.10)

    def test_extract_percentage_zero(self):
        """Test extract_percentage with zero."""
        assert FinvizPatterns.extract_percentage("0%") == pytest.approx(0.0)
        assert FinvizPatterns.extract_percentage("+0%") == pytest.approx(0.0)

    def test_extract_percentage_with_spaces(self):
        """Test extract_percentage with spaces."""
        assert FinvizPatterns.extract_percentage(" +5.23% ") == pytest.approx(0.0523)
        assert FinvizPatterns.extract_percentage("  10%  ") == pytest.approx(0.10)

    def test_extract_percentage_invalid(self):
        """Test extract_percentage with invalid input."""
        assert FinvizPatterns.extract_percentage("invalid") is None
        assert FinvizPatterns.extract_percentage("") is None
        assert FinvizPatterns.extract_percentage("abc%") is None
        assert FinvizPatterns.extract_percentage("N/A") is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
