"""Parsing helpers for Finviz values."""
from __future__ import annotations

import math
import re
from typing import Optional, Tuple, Union

import pandas as pd

MISSING = {"", "-", None}


def parse_missing(value: Optional[str]) -> Optional[object]:
    """Return pd.NA for missing sentinel values."""

    if value in MISSING:
        return pd.NA
    return value


def parse_percent(value: Optional[str]) -> Optional[float]:
    """Parse percentage strings like '7.25%' into decimals."""

    if value in MISSING:
        return pd.NA
    try:
        cleaned = value.strip().rstrip("%")
        return float(cleaned) / 100
    except (ValueError, AttributeError):
        return pd.NA


_MAGNITUDE = {
    "K": 1_000,
    "M": 1_000_000,
    "B": 1_000_000_000,
    "T": 1_000_000_000_000,
}


def parse_human_number(value: Optional[str]) -> Optional[Union[int, float]]:
    """Parse human readable numbers like 147.01B into numeric values."""

    if value in MISSING:
        return pd.NA
    try:
        s = value.strip().replace(",", "")
        suffix = s[-1]
        if suffix in _MAGNITUDE:
            base = float(s[:-1]) * _MAGNITUDE[suffix]
        else:
            base = float(s)
        if math.isfinite(base) and base.is_integer():
            return int(base)
        return base
    except (ValueError, AttributeError):
        return pd.NA


def parse_range(value: Optional[str]) -> Optional[Tuple[Optional[float], Optional[float]]]:
    """Parse ranges like '745.55 - 1084.22'."""

    if value in MISSING:
        return pd.NA
    try:
        parts = [p.strip() for p in value.split("-")]
        if len(parts) != 2:
            return pd.NA
        start = float(parts[0]) if parts[0] else pd.NA
        end = float(parts[1]) if parts[1] else pd.NA
        return (start, end)
    except (ValueError, AttributeError):
        return pd.NA


TICKER_RE = re.compile(r"^[A-Z]{1,5}(\.[A-Z]{1,2})?$")


def is_valid_ticker(text: str) -> bool:
    """Validate ticker text similar to notebook logic."""

    return bool(TICKER_RE.match(text.strip()))
