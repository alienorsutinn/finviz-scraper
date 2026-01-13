# Additional Finviz Data Scrapers

This document describes the new scraper modules for collecting additional data from Finviz.

## 📊 Available Data Sources

### 1. Insider Trading (`insider.py`)

Scrape insider buy/sell transactions for individual tickers or market-wide.

**What you can scrape:**
- Transaction type (buy/sell/option exercise)
- Insider name and relationship (CEO, CFO, Director)
- Transaction date, cost, shares traded
- Total shares held after transaction
- SEC form type

**Usage Example:**

\`\`\`python
from finviz_weekly.insider import scrape_insider_trading, aggregate_insider_by_ticker
from finviz_weekly.http import create_session
from finviz_weekly.config import HttpConfig

session = create_session(HttpConfig())

# Get insider trades for specific ticker
trades = scrape_insider_trading("AAPL", session, HttpConfig())

# Aggregate stats
stats = aggregate_insider_by_ticker(trades)
print(f"Net insider value: ${stats['AAPL']['net_value']:,.0f}")
\`\`\`

### 2. Earnings Reactions (`earnings.py`)

Scrape historical price reactions around earnings (-3 days to +1 week).

**What you can scrape:**
- Price movements around earnings
- Comparison to SPY performance
- RSI at earnings time
- BMO vs AMC timing

**Usage Example:**

\`\`\`python
from finviz_weekly.earnings import scrape_earnings_reactions, calculate_earnings_statistics

# Get earnings history
earnings = scrape_earnings_reactions("TSLA", session, HttpConfig())

# Calculate stats
stats = calculate_earnings_statistics(earnings)
print(f"Avg reaction: {stats['avg_day_0_change']:.2%}")
\`\`\`

### 3. Enhanced Financial Statements (`financials.py`)

Detailed income statement, balance sheet, and cash flow data.

**What you can scrape:**
- Complete financial statements (all periods)
- Calculated ratios (ROE, ROA, debt/equity, etc.)
- Growth rates (revenue, income, assets)

**Usage Example:**

\`\`\`python
from finviz_weekly.financials import scrape_financial_statements, calculate_financial_ratios

# Get statements
statements = scrape_financial_statements("AAPL", session, HttpConfig())

# Calculate ratios
ratios = calculate_financial_ratios(statements)
print(f"ROE: {ratios['roe']:.2%}")
\`\`\`

## 🎯 Strategy Examples

**Find stocks with heavy insider buying:**
\`\`\`python
insider_data = scrape_insider_summary(session, config, "buy")
heavy_buyers = {t: d for t, d in aggregate_insider_by_ticker(insider_data).items() 
                if d['buy_value'] > 1000000}
\`\`\`

**Find consistent earnings beaters:**
\`\`\`python
stats = calculate_earnings_statistics(scrape_earnings_reactions(ticker, s, c))
if stats['positive_reactions'] >= 4 and stats['avg_alpha_day_0'] > 0.02:
    print(f"{ticker}: Consistent beater!")
\`\`\`

## 📦 All 37 Tests Passing

Run tests with:
\`\`\`bash
pytest tests/test_new_scrapers.py -v
\`\`\`
