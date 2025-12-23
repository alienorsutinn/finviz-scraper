"""Quick preflight to verify finvizfinance works."""
from __future__ import annotations

import json

from finviz_weekly.fundamentals import scrape_fundamentals


def main():
    sample = scrape_fundamentals("AAPL")
    print(json.dumps(sample, indent=2))


if __name__ == "__main__":
    main()
