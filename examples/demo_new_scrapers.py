#!/usr/bin/env python3
"""
Demo script showcasing the new Finviz scraper modules:
- Insider Trading
- Earnings Reactions
- Financial Statements

This script demonstrates how to use each scraper independently.
For production use, integrate these into your main pipeline.
"""

import sys
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
import requests

from finviz_weekly.config import HttpConfig
from finviz_weekly.earnings import aggregate_earnings_stats, scrape_earnings_reactions
from finviz_weekly.financials import calculate_financial_ratios, scrape_financial_statements
from finviz_weekly.insider import aggregate_insider_by_ticker, scrape_insider_trading


def demo_insider_trading(tickers: list[str]):
    """Demonstrate insider trading scraper."""
    print("\n" + "=" * 80)
    print("INSIDER TRADING DEMO")
    print("=" * 80)

    session = requests.Session()
    http_config = HttpConfig()

    all_transactions = []
    for ticker in tickers:
        print(f"\nScraping insider trades for {ticker}...")
        try:
            transactions = scrape_insider_trading(ticker, session, http_config)
            all_transactions.extend(transactions)
            print(f"  Found {len(transactions)} transactions")
        except Exception as e:
            print(f"  Error: {e}")

    # Aggregate by ticker
    if all_transactions:
        stats = aggregate_insider_by_ticker(all_transactions)
        print("\n--- Aggregated Insider Activity ---")
        df = pd.DataFrame(stats).T
        print(df)

        # Interpretation
        print("\n--- Interpretation ---")
        for ticker, data in stats.items():
            net = data["net_value"]
            if net > 0:
                print(f"{ticker}: Net BUYING (${net:,.0f}) - Bullish signal")
            elif net < 0:
                print(f"{ticker}: Net SELLING (${abs(net):,.0f}) - Bearish signal")
            else:
                print(f"{ticker}: Neutral insider activity")


def demo_earnings_reactions(tickers: list[str]):
    """Demonstrate earnings reaction scraper."""
    print("\n" + "=" * 80)
    print("EARNINGS REACTIONS DEMO")
    print("=" * 80)

    session = requests.Session()
    http_config = HttpConfig()

    all_earnings = []
    for ticker in tickers:
        print(f"\nScraping earnings reactions for {ticker}...")
        try:
            earnings = scrape_earnings_reactions(ticker, session, http_config)
            all_earnings.extend(earnings)
            print(f"  Found {len(earnings)} earnings events")
        except Exception as e:
            print(f"  Error: {e}")

    # Aggregate statistics
    if all_earnings:
        stats = aggregate_earnings_stats(all_earnings)
        print("\n--- Aggregated Earnings Stats ---")
        df = pd.DataFrame(stats).T
        print(df)

        # Interpretation
        print("\n--- Interpretation ---")
        for ticker, data in stats.items():
            avg_alpha = data["avg_day_0_alpha"]
            if avg_alpha > 2:
                print(f"{ticker}: Strong positive earnings surprise history (+{avg_alpha:.1f}% alpha)")
            elif avg_alpha < -2:
                print(f"{ticker}: Weak earnings history ({avg_alpha:.1f}% alpha)")
            else:
                print(f"{ticker}: Mixed earnings performance ({avg_alpha:.1f}% alpha)")


def demo_financial_statements(tickers: list[str]):
    """Demonstrate financial statement scraper."""
    print("\n" + "=" * 80)
    print("FINANCIAL STATEMENTS DEMO")
    print("=" * 80)

    session = requests.Session()
    http_config = HttpConfig()

    for ticker in tickers:
        print(f"\nScraping financial statements for {ticker}...")
        try:
            statements = scrape_financial_statements(ticker, session, http_config)

            # Show sample data
            print(f"\n  Income Statement items: {len(statements['income_statement'])}")
            print(f"  Balance Sheet items: {len(statements['balance_sheet'])}")
            print(f"  Cash Flow items: {len(statements['cash_flow'])}")

            # Calculate ratios
            ratios = calculate_financial_ratios(statements)
            if ratios:
                print("\n  --- Key Financial Ratios ---")
                for ratio_name, value in ratios.items():
                    print(f"    {ratio_name}: {value:.3f}")

                # Interpretation
                print("\n  --- Interpretation ---")
                if "net_margin" in ratios and ratios["net_margin"] > 0.15:
                    print(f"    Strong profitability (net margin: {ratios['net_margin']*100:.1f}%)")
                if "roe" in ratios and ratios["roe"] > 0.15:
                    print(f"    Excellent return on equity ({ratios['roe']*100:.1f}%)")
                if "current_ratio" in ratios and ratios["current_ratio"] > 2:
                    print(f"    Strong liquidity (current ratio: {ratios['current_ratio']:.2f})")
            else:
                print("  No ratios calculated (missing data)")

        except Exception as e:
            print(f"  Error: {e}")


def main():
    """Run all demos."""
    # Example tickers - replace with your own
    tickers = ["AAPL", "TSLA"]

    print("=" * 80)
    print("FINVIZ NEW SCRAPERS DEMO")
    print("=" * 80)
    print(f"Testing with tickers: {', '.join(tickers)}")
    print("\nNote: This will make live requests to Finviz.com")
    print("Please be respectful of rate limits!\n")

    try:
        # Run each demo
        demo_insider_trading(tickers)
        demo_earnings_reactions(tickers)
        demo_financial_statements(tickers)

        # Summary
        print("\n" + "=" * 80)
        print("DEMO COMPLETE")
        print("=" * 80)
        print("\nNext steps:")
        print("1. Integrate these scrapers into your main pipeline")
        print("2. Add to scoring model (e.g., boost scores for insider buying)")
        print("3. Use for filtering (e.g., only stocks with positive earnings alpha)")
        print("4. Store data for backtesting and analysis")
        print("\nSee docs/NEW_SCRAPERS.md for more details!")

    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user")
    except Exception as e:
        print(f"\n\nDemo failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
