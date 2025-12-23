"""Parsing helpers for Finviz values."""
from __future__ import annotations

import math
import re
from typing import Optional, Tuple, Union

import pandas as pd

# Finviz missing sentinels
_MISSING_STRINGS = {"", "-", "N/A", "NA", "null", "None"}


def parse_missing(value: object) -> object:
    """Return pd.NA for missing sentinel values (robust to whitespace / nbsp)."""
    if value is None:
        return pd.NA
    if isinstance(value, str):
        # normalize whitespace (including non-breaking spaces)
        v = value.replace("\xa0", " ").strip()
        if v in _MISSING_STRINGS:
            return pd.NA
        return v
    return value


def parse_percent(value: Optional[str]) -> object:
    """Parse percentage strings like '7.25%' into decimals."""
    v = parse_missing(value)
    if v is pd.NA:
        return pd.NA
    if not isinstance(v, str):
        return pd.NA
    try:
        cleaned = v.rstrip("%").strip()
        if cleaned == "":
            return pd.NA
        return float(cleaned) / 100
    except ValueError:
        return pd.NA


_MAGNITUDE = {
    "K": 1_000,
    "M": 1_000_000,
    "B": 1_000_000_000,
    "T": 1_000_000_000_000,
}


def parse_human_number(value: Optional[str]) -> object:
    """Parse human readable numbers like 147.01B into numeric values."""
    v = parse_missing(value)
    if v is pd.NA:
        return pd.NA
    if not isinstance(v, str):
        return pd.NA

    try:
        s = v.replace(",", "").strip()
        if s == "":
            return pd.NA

        suffix = s[-1]
        if suffix in _MAGNITUDE:
            base = float(s[:-1]) * _MAGNITUDE[suffix]
        else:
            base = float(s)

        if not math.isfinite(base):
            return pd.NA
        return int(base) if float(base).is_integer() else base
    except ValueError:
        return pd.NA
    except IndexError:
        # ultra-defensive: if s became empty somehow
        return pd.NA


def parse_range(value: Optional[str]) -> object:
    """Parse ranges like '745.55 - 1084.22'."""
    v = parse_missing(value)
    if v is pd.NA:
        return pd.NA
    if not isinstance(v, str):
        return pd.NA
    try:
        parts = [p.strip() for p in v.split("-")]
        if len(parts) != 2:
            return pd.NA
        if parts[0] == "" or parts[1] == "":
            return pd.NA
        return (float(parts[0]), float(parts[1]))
    except ValueError:
        return pd.NA


TICKER_RE = re.compile(r"^[A-Z]{1,5}(\.[A-Z]{1,2})?$")


def is_valid_ticker(text: str) -> bool:
    """Validate ticker text similar to notebook logic."""
    if not isinstance(text, str):
        return False
    return bool(TICKER_RE.match(text.strip().upper()))
