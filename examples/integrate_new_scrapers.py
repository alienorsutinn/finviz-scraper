#!/usr/bin/env python3
"""
Example: Integrating new scrapers into the main pipeline.

This script shows how to enhance the main finviz_weekly pipeline
with insider trading, earnings reactions, and financial statement data.

Usage:
  python examples/integrate_new_scrapers.py --ticker AAPL
  python examples/integrate_new_scrapers.py --tickers AAPL,TSLA,MSFT
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import argparse
import logging
from datetime import datetime

import pandas as pd
import requests

from finviz_weekly.config import HttpConfig
from finviz_weekly.earnings import aggregate_earnings_stats, scrape_earnings_reactions
from finviz_weekly.financials import calculate_financial_ratios, scrape_financial_statements
from finviz_weekly.insider import aggregate_insider_by_ticker, scrape_insider_trading
from finviz_weekly.score import get_fundamentals

LOGGER = logging.getLogger(__name__)


def enhance_fundamentals_with_new_data(
    fundamentals_df: pd.DataFrame, session: requests.Session, http_config: HttpConfig
) -> pd.DataFrame:
    """
    Enhance fundamentals dataframe with insider, earnings, and financial data.

    This is the integration pattern you should use in your pipeline.
    """
    tickers = fundamentals_df["ticker"].tolist()

    # 1. Scrape insider trading
    LOGGER.info("Scraping insider trading for %d tickers...", len(tickers))
    all_insider = []
    for ticker in tickers:
        try:
            transactions = scrape_insider_trading(ticker, session, http_config)
            all_insider.extend(transactions)
        except Exception as e:
            LOGGER.warning("Insider scrape failed for %s: %s", ticker, e)

    insider_stats = aggregate_insider_by_ticker(all_insider) if all_insider else {}
    LOGGER.info("Aggregated insider data for %d tickers", len(insider_stats))

    # 2. Scrape earnings reactions
    LOGGER.info("Scraping earnings reactions for %d tickers...", len(tickers))
    all_earnings = []
    for ticker in tickers:
        try:
            earnings = scrape_earnings_reactions(ticker, session, http_config)
            all_earnings.extend(earnings)
        except Exception as e:
            LOGGER.warning("Earnings scrape failed for %s: %s", ticker, e)

    earnings_stats = aggregate_earnings_stats(all_earnings) if all_earnings else {}
    LOGGER.info("Aggregated earnings data for %d tickers", len(earnings_stats))

    # 3. Scrape financial statements
    LOGGER.info("Scraping financial statements for %d tickers...", len(tickers))
    financial_ratios = {}
    for ticker in tickers:
        try:
            statements = scrape_financial_statements(ticker, session, http_config)
            ratios = calculate_financial_ratios(statements)
            financial_ratios[ticker] = ratios
        except Exception as e:
            LOGGER.warning("Financial scrape failed for %s: %s", ticker, e)

    LOGGER.info("Calculated financial ratios for %d tickers", len(financial_ratios))

    # 4. Merge into fundamentals dataframe
    enhanced = fundamentals_df.copy()

    # Add insider columns
    enhanced["insider_net_value"] = enhanced["ticker"].map(lambda t: insider_stats.get(t, {}).get("net_value", 0))
    enhanced["insider_total_buys"] = enhanced["ticker"].map(lambda t: insider_stats.get(t, {}).get("total_buys", 0))
    enhanced["insider_total_sells"] = enhanced["ticker"].map(lambda t: insider_stats.get(t, {}).get("total_sells", 0))

    # Add earnings columns
    enhanced["earnings_avg_alpha"] = enhanced["ticker"].map(lambda t: earnings_stats.get(t, {}).get("avg_day_0_alpha", 0))
    enhanced["earnings_avg_rsi"] = enhanced["ticker"].map(lambda t: earnings_stats.get(t, {}).get("avg_rsi", 50))
    enhanced["earnings_win_rate"] = enhanced["ticker"].map(
        lambda t: earnings_stats.get(t, {}).get("positive_reaction_pct", 0)
    )

    # Add financial ratio columns
    enhanced["net_margin"] = enhanced["ticker"].map(lambda t: financial_ratios.get(t, {}).get("net_margin", None))
    enhanced["roe"] = enhanced["ticker"].map(lambda t: financial_ratios.get(t, {}).get("roe", None))
    enhanced["roa"] = enhanced["ticker"].map(lambda t: financial_ratios.get(t, {}).get("roa", None))
    enhanced["debt_to_equity"] = enhanced["ticker"].map(lambda t: financial_ratios.get(t, {}).get("debt_to_equity", None))
    enhanced["current_ratio"] = enhanced["ticker"].map(lambda t: financial_ratios.get(t, {}).get("current_ratio", None))

    LOGGER.info("Enhanced dataframe with %d new columns", len(enhanced.columns) - len(fundamentals_df.columns))
    return enhanced


def calculate_enhanced_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate additional scores based on new data.

    This shows how to integrate new data into your scoring system.
    """
    scored = df.copy()

    # Insider score (0-10): reward net buying, penalize net selling
    scored["insider_score"] = 5.0  # neutral default
    insider_mask = scored["insider_net_value"].notna()
    if insider_mask.any():
        # Normalize to 0-10 scale
        max_val = scored.loc[insider_mask, "insider_net_value"].abs().max()
        if max_val > 0:
            scored.loc[insider_mask, "insider_score"] = 5 + 5 * (
                scored.loc[insider_mask, "insider_net_value"] / max_val
            )

    # Earnings surprise score (0-10): reward consistent positive alpha
    scored["earnings_score"] = 5.0  # neutral default
    earnings_mask = scored["earnings_avg_alpha"].notna()
    if earnings_mask.any():
        # Alpha > 3% = good, < -3% = bad
        scored.loc[earnings_mask, "earnings_score"] = scored.loc[earnings_mask, "earnings_avg_alpha"].clip(-3, 3)
        scored.loc[earnings_mask, "earnings_score"] = 5 + (scored.loc[earnings_mask, "earnings_score"] / 3) * 5

    # Financial quality score (0-10): combine profitability and safety
    scored["financial_quality"] = 5.0  # neutral default
    fin_mask = scored["net_margin"].notna() & scored["roe"].notna() & scored["current_ratio"].notna()
    if fin_mask.any():
        # High margin, high ROE, healthy liquidity
        margin_score = scored.loc[fin_mask, "net_margin"].clip(0, 0.3) / 0.3 * 3.33
        roe_score = scored.loc[fin_mask, "roe"].clip(0, 0.3) / 0.3 * 3.33
        liquidity_score = scored.loc[fin_mask, "current_ratio"].clip(0, 3) / 3 * 3.33
        scored.loc[fin_mask, "financial_quality"] = margin_score + roe_score + liquidity_score

    # Combined enhanced score
    scored["enhanced_score"] = (
        scored["insider_score"] * 0.3 + scored["earnings_score"] * 0.3 + scored["financial_quality"] * 0.4
    )

    return scored


def main():
    parser = argparse.ArgumentParser(description="Integration example for new scrapers")
    parser.add_argument("--ticker", help="Single ticker")
    parser.add_argument("--tickers", help="Comma-separated tickers")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()), format="%(levelname)s: %(message)s")

    # Parse tickers
    tickers = []
    if args.ticker:
        tickers = [args.ticker.upper()]
    elif args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        # Default demo tickers
        tickers = ["AAPL", "TSLA"]
        LOGGER.info("No tickers specified, using demo: %s", tickers)

    # Setup
    session = requests.Session()
    http_config = HttpConfig()

    # Step 1: Get fundamentals (using existing finviz_weekly function)
    LOGGER.info("Fetching fundamentals for %d tickers...", len(tickers))
    fundamentals = []
    for ticker in tickers:
        try:
            fund = get_fundamentals(ticker, session, http_config)
            if fund:
                fundamentals.append(fund)
        except Exception as e:
            LOGGER.error("Failed to get fundamentals for %s: %s", ticker, e)

    if not fundamentals:
        LOGGER.error("No fundamentals retrieved")
        return

    fundamentals_df = pd.DataFrame(fundamentals)
    LOGGER.info("Retrieved fundamentals for %d tickers", len(fundamentals_df))

    # Step 2: Enhance with new scraper data
    enhanced_df = enhance_fundamentals_with_new_data(fundamentals_df, session, http_config)

    # Step 3: Calculate enhanced scores
    scored_df = calculate_enhanced_scores(enhanced_df)

    # Step 4: Display results
    print("\n" + "=" * 100)
    print("ENHANCED FUNDAMENTALS WITH NEW DATA")
    print("=" * 100)
    print(scored_df[["ticker", "insider_score", "earnings_score", "financial_quality", "enhanced_score"]].to_string())

    # Step 5: Save results
    output_dir = Path("data") / "enhanced"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"enhanced_{datetime.now().strftime('%Y%m%d')}.csv"
    scored_df.to_csv(output_file, index=False)
    LOGGER.info("Saved enhanced data to %s", output_file)

    print("\n" + "=" * 100)
    print("INTEGRATION COMPLETE")
    print("=" * 100)
    print(f"Enhanced {len(scored_df)} tickers with insider, earnings, and financial data")
    print(f"Results saved to: {output_file}")
    print("\nTo integrate into your pipeline:")
    print("1. Add enhance_fundamentals_with_new_data() call after scraping fundamentals")
    print("2. Update your scoring logic to use the new columns")
    print("3. Adjust weights based on backtesting results")


if __name__ == "__main__":
    main()
