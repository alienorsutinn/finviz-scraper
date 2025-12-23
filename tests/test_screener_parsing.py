from pathlib import Path

from bs4 import BeautifulSoup

from finviz_weekly.screener import _extract_tickers_from_html, get_industries
from finviz_weekly.http import request_with_retries


class DummyResponse:
    def __init__(self, text: str):
        self.text = text


class DummySession:
    def __init__(self, response: DummyResponse):
        self.response = response
        self.requested = []

    def get(self, url, headers=None, timeout=None):
        self.requested.append(url)
        return self.response


class DummyHttpConfig:
    max_retries = 1
    timeout_connect = 1
    timeout_read = 1


class DummyRetryResponse(DummyResponse):
    def raise_for_status(self):
        return None


def test_extract_tickers_from_html():
    html = Path("tests/fixtures/tickers_page.html").read_text()
    tickers = _extract_tickers_from_html(html)
    assert tickers == ["AAPL", "MSFT", "GOOG"]


def test_get_industries_parses_options(monkeypatch):
    html = Path("tests/fixtures/industries.html").read_text()
    response = DummyRetryResponse(html)

    def fake_request(session, url, config):
        return response

    monkeypatch.setattr("finviz_weekly.screener.request_with_retries", fake_request)
    industries = get_industries(None, DummyHttpConfig())
    assert industries == ["aerospace", "biotech"]
