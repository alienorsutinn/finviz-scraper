# Enhanced Screening Guide

This guide explains how to use the enhanced screening features that combine traditional fundamental analysis with insider trading, earnings surprises, and detailed financial ratios.

## Overview

Phase 4 adds **enhanced scoring** to the screening workflow, creating new investment themes that leverage insider trading signals, earnings reaction patterns, and deep financial health metrics.

## Quick Start

### 1. Scrape Enhanced Data

First, run the pipeline with enhanced flags to collect additional data:

```bash
# Collect all enhanced data sources
python -m finviz_weekly run --mode universe --ticker-limit 100 \
  --include-insider --include-earnings --include-financials \
  --out data
```

### 2. Run Enhanced Screening

The screening automatically detects enhanced columns and creates new themes:

```bash
# Screen with enhanced data (automatic detection)
python -m finviz_weekly screen --out data
```

### 3. Review Enhanced Screens

Enhanced screens are automatically generated in `data/latest/`:

- `top50_insider_momentum.csv` - Stocks with strong insider buying + momentum
- `top50_earnings_surprise.csv` - Stocks that consistently beat earnings expectations
- `top50_quality_growth_enhanced.csv` - High-quality stocks with strong financial health
- `top50_enhanced_master.csv` - Best combination of all enhanced factors

## Enhanced Scoring Models

### 1. Insider Momentum Score

**Strategy:** Combines insider buying signals with price momentum.

**Weights:**
- 40% Insider Score (net buying activity)
- 30% Momentum Score (recent performance)
- 20% Quality Score (profitability metrics)
- 10% Value Score (valuation multiples)

**Best For:** Finding stocks where insiders are buying before the market catches on.

**Example Use Case:**
```bash
# Look for small/mid-caps with insider buying
python -m finviz_weekly run --mode universe --ticker-limit 500 \
  --include-insider --out data

python -m finviz_weekly screen --out data --min-market-cap 500000000

# Review: data/latest/top50_insider_momentum.csv
```

### 2. Earnings Surprise Score

**Strategy:** Focuses on stocks that consistently outperform earnings expectations.

**Weights:**
- 40% Earnings Score (earnings surprise alpha vs SPY)
- 30% Quality Score (profitability metrics)
- 20% Growth Score (revenue/earnings growth)
- 10% Momentum Score (recent performance)

**Best For:** Finding high-quality execution stories with positive earnings momentum.

**Example Use Case:**
```bash
# Find tech stocks with consistent earnings beats
python -m finviz_weekly run --mode tickers \
  --tickers-file tech_watchlist.txt \
  --include-earnings --out data

python -m finviz_weekly screen --out data

# Review: data/latest/top50_earnings_surprise.csv
```

### 3. Quality Growth Enhanced Score

**Strategy:** Combines financial health analysis with growth metrics.

**Weights:**
- 35% Financial Health Score (ROE, margins, liquidity)
- 30% Quality Score (profitability metrics)
- 25% Growth Score (revenue/earnings growth)
- 10% Value Score (valuation multiples)

**Best For:** Finding compounders with sustainable competitive advantages.

**Example Use Case:**
```bash
# Find quality growth stocks in healthcare
python -m finviz_weekly run --mode universe --ticker-limit 200 \
  --include-financials --out data

python -m finviz_weekly screen --out data --normalize-by sector

# Review: data/latest/top50_quality_growth_enhanced.csv
```

### 4. Enhanced Master Score

**Strategy:** All-in score combining all available enhanced data sources.

**Weights (when all 3 sources available):**
- 25% Quality Score (traditional profitability)
- 20% Value Score (traditional valuation)
- 15% Insider Score (net buying/selling)
- 15% Earnings Score (surprise alpha)
- 15% Financial Health Score (ratios)
- 10% Growth Score (traditional growth)

**Adaptive:** If only 2 of 3 enhanced sources are available, weights are automatically adjusted.

**Best For:** Comprehensive screening using all available signals.

**Example Use Case:**
```bash
# Full enhanced pipeline for watchlist
python -m finviz_weekly run --mode tickers-file \
  --tickers-file my_watchlist.txt \
  --include-insider --include-earnings --include-financials \
  --out data

python -m finviz_weekly screen --out data --watchlist-file my_watchlist.txt

# Review: data/latest/top50_enhanced_master.csv
# Review: data/latest/watchlist.csv (includes all enhanced scores)
```

## Enhanced Score Components

### Insider Score (0-100 scale)

**Formula:** Based on net insider value (total buys - total sells)

- Score > 50: Net insider buying (bullish signal)
- Score = 50: Neutral (no insider activity)
- Score < 50: Net insider selling (bearish signal)

**Data Used:**
- `insider_net_value` - Net value of insider transactions
- `insider_total_buys` - Number of buy transactions
- `insider_total_sells` - Number of sell transactions

**Calculation:** Normalizes net insider value across all stocks, clips to [-1, 1], scales to 0-100.

### Earnings Score (0-100 scale)

**Formula:** Based on earnings day stock performance vs SPY

- Score > 50: Consistent positive earnings surprises
- Score = 50: Neutral earnings reactions
- Score < 50: Consistent negative earnings surprises

**Data Used:**
- `earnings_avg_alpha` - Average day-0 alpha vs SPY across earnings events
- `earnings_win_rate` - Percentage of positive reactions
- `earnings_total_events` - Number of earnings events analyzed

**Calculation:** Alpha > 3% = 100, alpha < -3% = 0, linear scaling between.

### Financial Health Score (0-100 scale)

**Formula:** Combines profitability and safety metrics

**Components (each 0-33.3 points):**
- **Profitability (66.7%):**
  - Net margin (0-30% is good): 33.3 points
  - ROE (0-30% is good): 33.3 points
- **Safety (33.3%):**
  - Current ratio (0-3 range, >2 is good): 33.3 points

**Data Used:**
- `net_margin` - Net profit margin
- `gross_margin` - Gross profit margin
- `roe` - Return on equity
- `roa` - Return on assets
- `current_ratio` - Current assets / current liabilities
- `quick_ratio` - (Current assets - inventory) / current liabilities
- `debt_to_equity` - Total debt / total equity

## Workflow Examples

### Example 1: Weekly Quality Screen with Insider Filter

```bash
# 1. Scrape weekly data with insider signals
python -m finviz_weekly run --mode universe --ticker-limit 500 \
  --include-insider --out data

# 2. Screen for quality + insider buying
python -m finviz_weekly screen --out data \
  --min-market-cap 1000000000 \
  --normalize-by sector

# 3. Review results
cat data/latest/candidates.txt  # Union of all themes
head -20 data/latest/top50_insider_momentum.csv  # Top insider buys
head -20 data/latest/conviction_2plus.csv  # Multi-theme conviction
```

### Example 2: Earnings Season Screen

```bash
# 1. Scrape earnings history for watchlist
python -m finviz_weekly run --mode tickers-file \
  --tickers-file earnings_season_watchlist.txt \
  --include-earnings --out data

# 2. Screen for consistent earnings beaters
python -m finviz_weekly screen --out data

# 3. Find stocks in multiple screens
head -30 data/latest/conviction_2plus.csv
```

### Example 3: Deep Value with Financial Analysis

```bash
# 1. Scrape with detailed financials
python -m finviz_weekly run --mode universe --ticker-limit 300 \
  --include-financials --out data

# 2. Screen with industry normalization
python -m finviz_weekly screen --out data \
  --normalize-by industry \
  --min-market-cap 500000000 \
  --min-price 5.0

# 3. Find quality growth candidates
head -30 data/latest/top50_quality_growth_enhanced.csv
```

### Example 4: Full Enhanced Workflow

```bash
# 1. Collect all enhanced data (slower but comprehensive)
python -m finviz_weekly run --mode universe --ticker-limit 200 \
  --include-insider --include-earnings --include-financials \
  --out data

# 2. Run enhanced screening
python -m finviz_weekly screen --out data \
  --normalize-by sector \
  --candidates-max 150

# 3. Analyze conviction names across all themes
cat data/latest/conviction_3plus.csv  # Stocks in 3+ theme families

# 4. Check specific enhanced screens
head -20 data/latest/top50_enhanced_master.csv
head -20 data/latest/top50_insider_momentum.csv
head -20 data/latest/top50_earnings_surprise.csv
```

## Advanced Features

### Partial Enhanced Data Support

The system gracefully handles partial enhanced data:

- **Insider only:** Creates `insider_momentum` screen only
- **Earnings only:** Creates `earnings_surprise` screen only
- **Financials only:** Creates `quality_growth_enhanced` screen only
- **2 of 3 sources:** Creates `enhanced_master` with adjusted weights
- **All 3 sources:** Creates `enhanced_master` with full weights

### Sector/Industry Normalization

Enhanced scores work seamlessly with sector/industry normalization:

```bash
# Normalize traditional + enhanced scores by sector
python -m finviz_weekly screen --out data --normalize-by sector

# This ensures fair comparison:
# - Tech stocks compared to tech stocks
# - Financial stocks compared to financial stocks
# - Enhanced scores still calculated globally
```

### Integration with Traditional Screens

Enhanced themes complement traditional themes:

**Traditional themes still available:**
- `quality_value` - Quality stocks at value prices
- `oversold_quality` - Oversold quality names
- `compounders` - Long-term quality compounders
- `hq_low_leverage` - High quality with low debt
- `turnaround_value` - Oversold value plays
- `garp` - Growth at reasonable price

**Enhanced themes added:**
- `insider_momentum` - Insider buying + momentum
- `earnings_surprise` - Consistent earnings beats
- `quality_growth_enhanced` - Quality + financial health
- `enhanced_master` - All enhanced factors combined

### Conviction Analysis

The conviction lists automatically include enhanced themes:

```bash
# Stocks appearing in 2+ theme families
cat data/latest/conviction_2plus.csv

# Stocks appearing in 3+ theme families (highest conviction)
cat data/latest/conviction_3plus.csv
```

Enhanced themes are counted as separate theme families, so a stock appearing in both `quality_value` and `insider_momentum` gets a conviction count of 2.

## Output Files

Enhanced screening adds these files to `data/latest/` and `data/runs/<date>/`:

### Enhanced Screen CSVs
- `top50_insider_momentum.csv` - Top 50 by insider momentum score
- `top50_earnings_surprise.csv` - Top 50 by earnings surprise score
- `top50_quality_growth_enhanced.csv` - Top 50 by enhanced quality growth score
- `top50_enhanced_master.csv` - Top 50 by enhanced master score

### Scored Data
- `finviz_scored.parquet` - All stocks with all scores (including enhanced)
- `finviz_scored.csv.gz` - Excel-friendly version

### Enhanced Columns in Scored Data

**Enhanced scores (0-100 scale):**
- `score_insider` - Insider activity score
- `score_earnings` - Earnings surprise quality score
- `score_financial_health` - Financial health composite score
- `score_insider_momentum` - Composite: insider + momentum
- `score_earnings_surprise` - Composite: earnings + quality + growth
- `score_quality_growth_enhanced` - Composite: financial health + quality + growth
- `score_enhanced_master` - Ultimate composite of all enhanced factors

**Enhanced raw data columns:**
- `insider_net_value` - Net value of insider transactions
- `insider_total_buys` - Number of buy transactions
- `insider_total_sells` - Number of sell transactions
- `insider_buy_value` - Total value of buys
- `insider_sell_value` - Total value of sells
- `earnings_avg_alpha` - Average earnings day alpha vs SPY
- `earnings_avg_rsi` - Average RSI at earnings time
- `earnings_win_rate` - Percentage of positive reactions
- `earnings_total_events` - Number of earnings events
- `net_margin` - Net profit margin
- `gross_margin` - Gross profit margin
- `roe` - Return on equity
- `roa` - Return on assets
- `current_ratio` - Current assets / current liabilities
- `quick_ratio` - Quick assets / current liabilities
- `debt_to_equity` - Total debt / total equity

## Performance Considerations

### Scraping Time

Enhanced data collection is slower than basic fundamentals:

- **Fundamentals only:** ~0.5 sec/ticker
- **+ Insider:** ~1.0 sec/ticker (2x slower)
- **+ Earnings:** ~1.0 sec/ticker (2x slower)
- **+ Financials:** ~0.8 sec/ticker (1.6x slower)
- **All three:** ~2.5 sec/ticker (5x slower)

**Recommendations:**
- Start with small ticker lists to test (~50 tickers)
- Use selective flags based on your strategy needs
- Respect rate limits with `--rate-per-sec 0.5`
- Run full universe scans weekly, not daily

### Example Timing

```bash
# Fast: 100 tickers, fundamentals only (~50 seconds)
python -m finviz_weekly run --mode universe --ticker-limit 100 --out data

# Moderate: 100 tickers, insider + earnings (~200 seconds)
python -m finviz_weekly run --mode universe --ticker-limit 100 \
  --include-insider --include-earnings --out data

# Slow: 100 tickers, all enhanced data (~250 seconds)
python -m finviz_weekly run --mode universe --ticker-limit 100 \
  --include-insider --include-earnings --include-financials \
  --out data
```

## Troubleshooting

### Enhanced Screens Not Appearing

**Problem:** Running `screen` but no enhanced screens generated.

**Solution:** Check that you ran `run` with enhanced flags first:

```bash
# Must scrape enhanced data first
python -m finviz_weekly run --mode tickers --tickers "AAPL,MSFT" \
  --include-insider --include-earnings --out data

# Then screen will detect enhanced columns
python -m finviz_weekly screen --out data
```

### Partial Enhanced Data

**Problem:** Only some enhanced columns present in output.

**Explanation:** This is expected! Some tickers may fail to scrape due to:
- Rate limiting / timeouts
- Missing data on Finviz (no insider trades, no earnings history)
- Parsing errors

**Solution:** Enhanced scoring gracefully handles missing data with defaults:
- Missing insider data → score = 50 (neutral)
- Missing earnings data → score = 50 (neutral)
- Missing financial data → score = 50 (neutral)

### Score Values Unexpected

**Problem:** Enhanced scores seem too high/low.

**Explanation:** Enhanced scores are relative rankings within your dataset:
- Scores are based on percentile ranks across all tickers
- A score of 75 means "better than 75% of tickers in dataset"
- Different datasets will produce different absolute scores

**Solution:** Focus on relative rankings, not absolute scores. Compare within screens.

## Next Steps

1. **Backtest Enhanced Strategies:** See `src/finviz_weekly/backtest.py` for testing on historical data

2. **Optimize Weights:** Experiment with custom weight combinations in `score_enhanced.py`

3. **Create Custom Presets:** Design your own composite scores for specific strategies

4. **Automate Weekly:** Set up GitHub Actions or cron jobs for automated enhanced screening

## API Usage

For programmatic access to enhanced scoring:

```python
from finviz_weekly.screen import score_snapshot
import pandas as pd

# Load data with enhanced columns
df = pd.read_parquet("data/latest/finviz_fundamentals.parquet")

# Run enhanced scoring
scored, screens = score_snapshot(df, normalize_by="sector")

# Access enhanced scores
insider_momentum = screens["insider_momentum"].ranked.head(20)
print(insider_momentum[["ticker", "company", "score_insider_momentum"]])

# Access individual enhanced scores
print(scored[["ticker", "score_insider", "score_earnings", "score_financial_health"]])
```

## See Also

- [QUICKSTART.md](../QUICKSTART.md) - Getting started guide
- [README.md](../README.md) - Project overview
- [ROADMAP.md](../ROADMAP.md) - Development roadmap
- [docs/NEW_SCRAPERS.md](NEW_SCRAPERS.md) - New scraper documentation
- [docs/TESTING.md](TESTING.md) - Testing documentation
