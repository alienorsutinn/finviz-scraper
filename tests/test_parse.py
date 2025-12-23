import pandas as pd

from finviz_weekly.parse import is_valid_ticker, parse_human_number, parse_missing, parse_percent, parse_range


def test_parse_percent():
    assert parse_percent("7.25%") == 0.0725
    assert parse_percent("-") is pd.NA


def test_parse_human_number():
    assert parse_human_number("147.01B") == 147_010_000_000
    assert parse_human_number("1.5M") == 1_500_000
    assert parse_human_number("42") == 42
    assert parse_human_number("-") is pd.NA


def test_parse_range():
    assert parse_range("1 - 2") == (1.0, 2.0)
    assert parse_range("-") is pd.NA


def test_is_valid_ticker():
    assert is_valid_ticker("AAPL")
    assert not is_valid_ticker("BAD1")


def test_parse_missing():
    assert parse_missing("-") is pd.NA
    assert parse_missing("value") == "value"
