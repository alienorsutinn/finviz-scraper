# Backtesting Guide

This guide explains how to backtest scoring strategies using historical data to validate performance.

## Overview

The backtesting framework allows you to:
- Test any scoring strategy on historical data
- Compare multiple strategies side-by-side
- Measure risk-adjusted returns (Sharpe ratio, drawdown)
- Optimize rebalancing frequency and portfolio size

## Quick Start

### Prerequisites

You need historical scored data to run backtests. The system automatically builds this when you run screening:

```bash
# Run screening multiple times to build history
python -m finviz_weekly run --mode universe --ticker-limit 500 --out data
python -m finviz_weekly screen --out data

# Repeat weekly to build history...
# After several runs, you'll have data/history/finviz_scored_history.parquet
```

### Basic Backtest

```bash
python -m finviz_weekly.backtest \
  --history data/history/finviz_scored_history.parquet \
  --start 2024-01-01 \
  --end 2025-01-01 \
  --top-n 20 \
  --rebalance-days 7
```

This tests a strategy that:
- Holds the top 20 stocks by `total_score`
- Rebalances weekly (every 7 days)
- Uses equal-weight allocation

## Backtest Configuration

### BacktestConfig Parameters

```python
from finviz_weekly.backtest import BacktestConfig

config = BacktestConfig(
    start_date="2024-01-01",      # Start date (YYYY-MM-DD)
    end_date="2025-01-01",         # End date (YYYY-MM-DD)
    score_column="total_score",    # Score column to rank by
    top_n=20,                      # Number of stocks to hold
    rebalance_days=7,              # Rebalance frequency
    min_price=5.0,                 # Minimum price filter
    max_position_size=0.10,        # Max 10% per position
    initial_capital=100000.0,      # Starting capital
)
```

### Key Parameters Explained

**score_column**: Which score to use for ranking stocks
- `"total_score"` - Traditional composite score
- `"score_master"` - Master score with all traditional factors
- `"score_quality_value"` - Quality + value theme
- `"score_insider_momentum"` - Enhanced: insider buying + momentum
- `"score_earnings_surprise"` - Enhanced: consistent earnings beats
- `"score_enhanced_master"` - Enhanced: all enhanced factors

**top_n**: Portfolio size
- Smaller (10-15): More concentrated, higher variance
- Medium (20-30): Balanced risk/return
- Larger (40-50): More diversified, lower variance

**rebalance_days**: How often to rebalance
- 1 day: Daily rebalancing (high turnover)
- 7 days: Weekly (balanced)
- 14-30 days: Biweekly/monthly (low turnover)

**max_position_size**: Position size limit
- 0.05 (5%): Max 20 positions
- 0.10 (10%): Max 10 positions (equal weight for top_n=10)
- 0.20 (20%): Max 5 positions

## Comparing Strategies

Use `compare_strategies()` to test multiple strategies at once:

```python
from finviz_weekly.backtest import compare_strategies
from pathlib import Path

# Define strategies to compare
strategies = [
    ("Traditional Master", "score_master"),
    ("Quality Value", "score_quality_value"),
    ("Compounders", "score_compounders"),
    # Enhanced strategies (if data includes enhanced scores)
    ("Insider Momentum", "score_insider_momentum"),
    ("Earnings Surprise", "score_earnings_surprise"),
]

# Run comparison
results_df = compare_strategies(
    history_path=Path("data/history/finviz_scored_history.parquet"),
    start_date="2024-01-01",
    end_date="2025-01-01",
    strategies=strategies,
    top_n=20,
    rebalance_days=7,
)

# Display results
print(results_df.sort_values("sharpe_ratio", ascending=False))
```

**Output example:**
```
                 strategy         score_column  total_return  annual_return  sharpe_ratio  max_drawdown  num_trades
1      Insider Momentum  score_insider_momentum         0.285          0.285         1.82        -0.145          48
4      Earnings Surprise  score_earnings_surprise        0.267          0.267         1.64        -0.158          52
0     Traditional Master            score_master          0.218          0.218         1.35        -0.192          45
2          Quality Value      score_quality_value         0.203          0.203         1.28        -0.182          43
3            Compounders          score_compounders        0.194          0.194         1.22        -0.201          47
```

## Performance Metrics

### Total Return
Total gain/loss over the backtest period.

**Formula:** `(final_value - initial_value) / initial_value`

**Example:** 0.285 = 28.5% total return

### Annual Return
Annualized return (CAGR).

**Formula:** `(1 + total_return)^(1 / years) - 1`

**Example:** For 1-year period, annual_return = total_return

### Sharpe Ratio
Risk-adjusted return measuring excess return per unit of volatility.

**Formula:** `(mean_return / std_return) * sqrt(252)`

**Interpretation:**
- < 1.0: Poor risk-adjusted returns
- 1.0-2.0: Good risk-adjusted returns
- > 2.0: Excellent risk-adjusted returns

**Example:** Sharpe of 1.82 indicates strong risk-adjusted performance

### Max Drawdown
Largest peak-to-trough decline during the backtest.

**Formula:** `min((value - cummax) / cummax)`

**Example:** -0.145 = -14.5% max drawdown

**Interpretation:**
- < -10%: Low drawdown (good)
- -10% to -20%: Moderate drawdown
- > -20%: High drawdown (risky)

### Number of Trades
Total buy + sell transactions during the period.

More trades = higher turnover = higher transaction costs

## Backtest Workflow Examples

### Example 1: Test Single Strategy

```python
from finviz_weekly.backtest import Backtester, BacktestConfig
from pathlib import Path

# Load history
backtester = Backtester(Path("data/history/finviz_scored_history.parquet"))

# Configure strategy
config = BacktestConfig(
    start_date="2024-01-01",
    end_date="2025-01-01",
    score_column="score_quality_value",
    top_n=25,
    rebalance_days=14,  # Biweekly rebalancing
)

# Run backtest
results = backtester.run(config)

# Display results
print(f"Total Return:  {results.total_return:.1%}")
print(f"Annual Return: {results.annual_return:.1%}")
print(f"Sharpe Ratio:  {results.sharpe_ratio:.2f}")
print(f"Max Drawdown:  {results.max_drawdown:.1%}")
print(f"Trades:        {len(results.trades)}")

# Analyze trades
print("\nTop 5 trades by value:")
print(results.trades.sort_values("shares", ascending=False).head())
```

### Example 2: Optimize Rebalancing Frequency

```python
from finviz_weekly.backtest import Backtester, BacktestConfig
from pathlib import Path
import pandas as pd

backtester = Backtester(Path("data/history/finviz_scored_history.parquet"))

results = []
for rebalance_days in [1, 3, 7, 14, 30]:
    config = BacktestConfig(
        start_date="2024-01-01",
        end_date="2025-01-01",
        score_column="score_master",
        top_n=20,
        rebalance_days=rebalance_days,
    )

    result = backtester.run(config)
    results.append({
        "rebalance_days": rebalance_days,
        "total_return": result.total_return,
        "sharpe_ratio": result.sharpe_ratio,
        "num_trades": len(result.trades),
    })

df = pd.DataFrame(results)
print(df)
```

### Example 3: Optimize Portfolio Size

```python
results = []
for top_n in [10, 15, 20, 30, 50]:
    config = BacktestConfig(
        start_date="2024-01-01",
        end_date="2025-01-01",
        score_column="score_master",
        top_n=top_n,
        rebalance_days=7,
    )

    result = backtester.run(config)
    results.append({
        "top_n": top_n,
        "total_return": result.total_return,
        "sharpe_ratio": result.sharpe_ratio,
        "max_drawdown": result.max_drawdown,
    })

df = pd.DataFrame(results)
print(df.sort_values("sharpe_ratio", ascending=False))
```

## Enhanced Scoring Backtest Limitation

⚠️ **Important Limitation:** Enhanced scoring requires specific data columns (insider, earnings, financials) that may not be present in historical data.

### The Challenge

Enhanced scores (`score_insider_momentum`, `score_earnings_surprise`, etc.) depend on:
- `insider_net_value` - Insider trading data
- `earnings_avg_alpha` - Earnings surprise data
- `net_margin`, `roe`, `current_ratio` - Financial ratios

These columns are only added when you run the pipeline with `--include-insider`, `--include-earnings`, `--include-financials` flags (Phase 3+).

**Historical data collected before Phase 3** won't have these columns, so enhanced scores can't be calculated.

### Solutions

**Option 1: Build New History (Recommended)**

Start fresh by scraping with enhanced flags going forward:

```bash
# Weekly: scrape with all enhanced data
python -m finviz_weekly run --mode universe --ticker-limit 500 \
  --include-insider --include-earnings --include-financials \
  --out data

# Screen to generate enhanced scores
python -m finviz_weekly screen --out data

# Repeat weekly for several months to build history
```

After 3-6 months, you'll have enough history to backtest enhanced strategies.

**Option 2: Backfill Historical Data**

Re-scrape historical enhanced data for past dates (advanced):

```python
from finviz_weekly.enhance import scrape_enhanced_data, merge_enhanced_data
from finviz_weekly.screen import score_snapshot
import pandas as pd

# Load historical fundamentals
history = pd.read_parquet("data/history/finviz_fundamentals_history.parquet")

# For each unique date, add enhanced data
for date in history["as_of_date"].unique():
    snapshot = history[history["as_of_date"] == date].copy()
    tickers = snapshot["ticker"].tolist()

    # Scrape enhanced data (WARNING: slow, respects rate limits)
    enhanced = scrape_enhanced_data(
        tickers, session, http_config,
        include_insider=True,
        include_earnings=True,
        include_financials=True,
    )

    # Merge and score
    snapshot = merge_enhanced_data(snapshot, enhanced)
    scored, _ = score_snapshot(snapshot)

    # Save to scored history
    # ... append to finviz_scored_history.parquet
```

**Option 3: Compare Traditional Strategies First**

While waiting for enhanced history, backtest traditional strategies:

```python
strategies = [
    ("Master", "score_master"),
    ("Quality Value", "score_quality_value"),
    ("Compounders", "score_compounders"),
    ("GARP", "score_garp"),
    ("High Quality Low Leverage", "score_hq_low_leverage"),
]

results = compare_strategies(
    Path("data/history/finviz_scored_history.parquet"),
    "2024-01-01",
    "2025-01-01",
    strategies,
)
```

This validates the backtesting framework and establishes baseline performance.

## Best Practices

### 1. Sufficient History
- Minimum: 3 months (1 quarter)
- Good: 6 months (2 quarters)
- Excellent: 12+ months (full year)

### 2. Market Conditions
Test across different market regimes:
- Bull markets (2024 Q1-Q2)
- Bear markets (2022)
- Sideways markets (2015-2016)

### 3. Realism
- Account for transaction costs (not currently implemented)
- Use realistic position sizes
- Consider slippage on small caps

### 4. Overfitting
- Don't optimize too many parameters
- Validate on out-of-sample data
- Prefer simpler strategies

### 5. Data Quality
Check history data quality:

```python
import pandas as pd

history = pd.read_parquet("data/history/finviz_scored_history.parquet")

print(f"Date range: {history['as_of_date'].min()} to {history['as_of_date'].max()}")
print(f"Unique dates: {history['as_of_date'].nunique()}")
print(f"Unique tickers: {history['ticker'].nunique()}")
print(f"\nScore columns available:")
print([col for col in history.columns if col.startswith("score_")])
```

## Troubleshooting

### "No data found between dates"

**Problem:** No historical data in the specified date range.

**Solution:** Check your history file:
```python
history = pd.read_parquet("data/history/finviz_scored_history.parquet")
print(history["as_of_date"].unique())
```

### "No <score_column> column on date"

**Problem:** The requested score column doesn't exist in historical data.

**Solution:** Use a score column that exists:
```python
history = pd.read_parquet("data/history/finviz_scored_history.parquet")
print([col for col in history.columns if col.startswith("score_")])
```

### Enhanced Scores Missing

**Problem:** Trying to backtest `score_insider_momentum` but column doesn't exist.

**Solution:** See "Enhanced Scoring Backtest Limitation" section above. You need to:
1. Scrape with enhanced flags going forward
2. Wait to build sufficient history
3. Or backfill historical enhanced data

### Poor Performance

**Problem:** All strategies show negative returns.

**Solution:** This might be accurate! Strategies can underperform in certain market conditions. Validate:
- Check market performance (SPY) during the same period
- Ensure data quality is good (no missing scores)
- Try different date ranges
- Adjust parameters (top_n, rebalance_days)

## Next Steps

1. **Build History:** Run weekly scrapes with enhanced flags for 3-6 months
2. **Validate Baseline:** Backtest traditional strategies on existing history
3. **Optimize Parameters:** Test different top_n and rebalance_days values
4. **Compare Enhanced:** Once history is built, compare enhanced vs traditional strategies
5. **Analyze Results:** Identify which themes perform best in which conditions

## See Also

- [docs/ENHANCED_SCREENING.md](ENHANCED_SCREENING.md) - Enhanced scoring strategies
- [README.md](../README.md) - Project overview
- [ROADMAP.md](../ROADMAP.md) - Development roadmap
- [src/finviz_weekly/backtest.py](../src/finviz_weekly/backtest.py) - Backtesting code
