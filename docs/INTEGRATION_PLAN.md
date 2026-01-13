# Integration Plan - New Scrapers into Main Pipeline

## Overview

This document outlines the plan to integrate the three new scrapers (insider trading, earnings reactions, and financial statements) into the main finviz_weekly pipeline.

## Current Architecture

```
Main Pipeline (pipeline.py)
├── 1. Build ticker universe (from industries or provided list)
├── 2. Scrape fundamentals for each ticker
├── 3. Calculate scores (quality, value, growth, etc.)
├── 4. Save to checkpoint files (NDJSON)
├── 5. Write final outputs (Parquet, CSV)
└── 6. Update history file
```

## Target Architecture

```
Enhanced Pipeline
├── 1. Build ticker universe (existing)
├── 2. Scrape fundamentals (existing)
├── 3. **NEW: Scrape insider trading (optional)**
├── 4. **NEW: Scrape earnings reactions (optional)**
├── 5. **NEW: Scrape financial statements (optional)**
├── 6. **NEW: Merge enhanced data into fundamentals dataframe**
├── 7. Calculate scores (enhanced with new factors)
├── 8. Save to checkpoints (with new data)
├── 9. Write final outputs
└── 10. Update history file
```

---

## Phase 1: Add Optional Flags (Non-Breaking)

### 1.1 Add CLI Arguments

**File:** `src/finviz_weekly/cli.py`

Add new optional flags to the `run` command:

```python
# In parse_args() function, add after existing run args:
parser.add_argument(
    "--include-insider",
    action=argparse.BooleanOptionalAction,
    default=False,
    help="Include insider trading data (default: disabled)."
)
parser.add_argument(
    "--include-earnings",
    action=argparse.BooleanOptionalAction,
    default=False,
    help="Include earnings reaction data (default: disabled)."
)
parser.add_argument(
    "--include-financials",
    action=argparse.BooleanOptionalAction,
    default=False,
    help="Include financial statement data (default: disabled)."
)
parser.add_argument(
    "--enhanced-scoring",
    action=argparse.BooleanOptionalAction,
    default=False,
    help="Use enhanced scoring with new data sources (default: disabled)."
)
```

### 1.2 Update RunConfig

**File:** `src/finviz_weekly/config.py`

Add new fields to `RunConfig`:

```python
@dataclass
class RunConfig:
    """Runtime configuration."""
    mode: str
    tickers: list[str]
    industry_limit: Optional[int]
    ticker_limit: Optional[int]
    out_dir: str
    formats: Iterable[str]
    log_level: str
    rate_limits: RateLimits
    resume: bool = True
    latest_only_ok: bool = True
    latest_include_as_of_date: bool = True

    # NEW: Enhanced scraping options
    include_insider: bool = False
    include_earnings: bool = False
    include_financials: bool = False
    enhanced_scoring: bool = False
```

Update `env_config()` to accept new parameters:

```python
def env_config(
    mode: str,
    tickers: Optional[list[str]] = None,
    industry_limit: Optional[int] = None,
    ticker_limit: Optional[int] = None,
    out_dir: str = "data",
    formats: Optional[Iterable[str]] = None,
    log_level: str = "INFO",
    rate_per_sec: float = 0.5,
    page_sleep_min: float = 0.8,
    page_sleep_max: float = 1.8,
    resume: bool = True,
    concurrency: int = 6,
    checkpoint_every: int = 10,
    latest_only_ok: bool = True,
    latest_include_as_of_date: bool = True,
    # NEW parameters
    include_insider: bool = False,
    include_earnings: bool = False,
    include_financials: bool = False,
    enhanced_scoring: bool = False,
) -> AppConfig:
    # ... existing code ...

    run = RunConfig(
        mode=mode,
        tickers=tickers or [],
        industry_limit=industry_limit,
        ticker_limit=ticker_limit,
        out_dir=out_dir,
        formats=formats or ["parquet", "csv"],
        log_level=log_level,
        rate_limits=rate_limits,
        resume=resume,
        latest_only_ok=latest_only_ok,
        latest_include_as_of_date=latest_include_as_of_date,
        # NEW fields
        include_insider=include_insider,
        include_earnings=include_earnings,
        include_financials=include_financials,
        enhanced_scoring=enhanced_scoring,
    )
```

---

## Phase 2: Create Enhancement Module

### 2.1 New File: `src/finviz_weekly/enhance.py`

Create a new module to handle data enhancement:

```python
"""Enhanced data collection and merging."""
from __future__ import annotations

import logging
from typing import Dict, List
import pandas as pd
import requests

from .config import HttpConfig
from .insider import scrape_insider_trading, aggregate_insider_by_ticker
from .earnings import scrape_earnings_reactions, aggregate_earnings_stats
from .financials import scrape_financial_statements, calculate_financial_ratios

LOGGER = logging.getLogger(__name__)


def scrape_enhanced_data(
    tickers: List[str],
    session: requests.Session,
    http_config: HttpConfig,
    include_insider: bool = False,
    include_earnings: bool = False,
    include_financials: bool = False,
) -> Dict[str, Dict]:
    """
    Scrape enhanced data sources for given tickers.

    Args:
        tickers: List of ticker symbols
        session: Requests session
        http_config: HTTP configuration
        include_insider: Whether to scrape insider trading data
        include_earnings: Whether to scrape earnings reaction data
        include_financials: Whether to scrape financial statements

    Returns:
        Dictionary with enhanced data by ticker
    """
    enhanced_data = {}

    # Initialize structure for each ticker
    for ticker in tickers:
        enhanced_data[ticker] = {
            "insider": None,
            "earnings": None,
            "financials": None,
        }

    # Scrape insider trading
    if include_insider:
        LOGGER.info("Scraping insider trading for %d tickers...", len(tickers))
        all_insider = []
        for ticker in tickers:
            try:
                transactions = scrape_insider_trading(ticker, session, http_config)
                all_insider.extend(transactions)
            except Exception as e:
                LOGGER.warning("Insider scrape failed for %s: %s", ticker, e)

        if all_insider:
            insider_stats = aggregate_insider_by_ticker(all_insider)
            for ticker, stats in insider_stats.items():
                if ticker in enhanced_data:
                    enhanced_data[ticker]["insider"] = stats
            LOGGER.info("Collected insider data for %d tickers", len(insider_stats))

    # Scrape earnings reactions
    if include_earnings:
        LOGGER.info("Scraping earnings reactions for %d tickers...", len(tickers))
        all_earnings = []
        for ticker in tickers:
            try:
                earnings = scrape_earnings_reactions(ticker, session, http_config)
                all_earnings.extend(earnings)
            except Exception as e:
                LOGGER.warning("Earnings scrape failed for %s: %s", ticker, e)

        if all_earnings:
            earnings_stats = aggregate_earnings_stats(all_earnings)
            for ticker, stats in earnings_stats.items():
                if ticker in enhanced_data:
                    enhanced_data[ticker]["earnings"] = stats
            LOGGER.info("Collected earnings data for %d tickers", len(earnings_stats))

    # Scrape financial statements
    if include_financials:
        LOGGER.info("Scraping financial statements for %d tickers...", len(tickers))
        for ticker in tickers:
            try:
                statements = scrape_financial_statements(ticker, session, http_config)
                ratios = calculate_financial_ratios(statements)
                enhanced_data[ticker]["financials"] = {
                    "statements": statements,
                    "ratios": ratios,
                }
            except Exception as e:
                LOGGER.warning("Financial scrape failed for %s: %s", ticker, e)

        successful = sum(1 for d in enhanced_data.values() if d["financials"] is not None)
        LOGGER.info("Collected financial data for %d tickers", successful)

    return enhanced_data


def merge_enhanced_data(df: pd.DataFrame, enhanced_data: Dict[str, Dict]) -> pd.DataFrame:
    """
    Merge enhanced data into fundamentals dataframe.

    Args:
        df: Fundamentals dataframe with 'ticker' column
        enhanced_data: Enhanced data dictionary from scrape_enhanced_data()

    Returns:
        Enhanced dataframe with new columns
    """
    enhanced_df = df.copy()

    # Add insider columns
    enhanced_df["insider_net_value"] = enhanced_df["ticker"].map(
        lambda t: enhanced_data.get(t, {}).get("insider", {}).get("net_value", 0.0)
    )
    enhanced_df["insider_total_buys"] = enhanced_df["ticker"].map(
        lambda t: enhanced_data.get(t, {}).get("insider", {}).get("total_buys", 0)
    )
    enhanced_df["insider_total_sells"] = enhanced_df["ticker"].map(
        lambda t: enhanced_data.get(t, {}).get("insider", {}).get("total_sells", 0)
    )

    # Add earnings columns
    enhanced_df["earnings_avg_alpha"] = enhanced_df["ticker"].map(
        lambda t: enhanced_data.get(t, {}).get("earnings", {}).get("avg_day_0_alpha", 0.0)
    )
    enhanced_df["earnings_avg_rsi"] = enhanced_df["ticker"].map(
        lambda t: enhanced_data.get(t, {}).get("earnings", {}).get("avg_rsi", 50.0)
    )
    enhanced_df["earnings_win_rate"] = enhanced_df["ticker"].map(
        lambda t: enhanced_data.get(t, {}).get("earnings", {}).get("positive_reaction_pct", 0.0)
    )

    # Add financial ratio columns
    enhanced_df["net_margin"] = enhanced_df["ticker"].map(
        lambda t: enhanced_data.get(t, {}).get("financials", {}).get("ratios", {}).get("net_margin")
    )
    enhanced_df["roe"] = enhanced_df["ticker"].map(
        lambda t: enhanced_data.get(t, {}).get("financials", {}).get("ratios", {}).get("roe")
    )
    enhanced_df["roa"] = enhanced_df["ticker"].map(
        lambda t: enhanced_data.get(t, {}).get("financials", {}).get("ratios", {}).get("roa")
    )
    enhanced_df["debt_to_equity"] = enhanced_df["ticker"].map(
        lambda t: enhanced_data.get(t, {}).get("financials", {}).get("ratios", {}).get("debt_to_equity")
    )
    enhanced_df["current_ratio"] = enhanced_df["ticker"].map(
        lambda t: enhanced_data.get(t, {}).get("financials", {}).get("ratios", {}).get("current_ratio")
    )

    LOGGER.info("Added %d enhanced columns", len(enhanced_df.columns) - len(df.columns))
    return enhanced_df


__all__ = [
    "scrape_enhanced_data",
    "merge_enhanced_data",
]
```

---

## Phase 3: Update Main Pipeline

### 3.1 Modify `pipeline.py`

**File:** `src/finviz_weekly/pipeline.py`

Add import:

```python
from .enhance import scrape_enhanced_data, merge_enhanced_data
```

In the `execute()` function, after scraping fundamentals but before scoring:

```python
def execute(session, config: AppConfig) -> None:
    # ... existing code to scrape fundamentals ...

    # Convert to dataframe
    fundamentals_df = pd.DataFrame(fundamentals)

    # NEW: Scrape enhanced data if requested
    if config.run.include_insider or config.run.include_earnings or config.run.include_financials:
        LOGGER.info("Scraping enhanced data sources...")
        tickers_list = fundamentals_df["ticker"].tolist()

        enhanced_data = scrape_enhanced_data(
            tickers=tickers_list,
            session=session,
            http_config=config.http,
            include_insider=config.run.include_insider,
            include_earnings=config.run.include_earnings,
            include_financials=config.run.include_financials,
        )

        fundamentals_df = merge_enhanced_data(fundamentals_df, enhanced_data)
        LOGGER.info("Enhanced data merged successfully")

    # ... continue with scoring and output ...
```

---

## Phase 4: Enhanced Scoring (Optional)

### 4.1 New File: `src/finviz_weekly/score_enhanced.py`

Create enhanced scoring module:

```python
"""Enhanced scoring with new data sources."""
import pandas as pd
import numpy as np


def calculate_insider_score(df: pd.DataFrame) -> pd.Series:
    """
    Calculate insider score (0-10) based on net insider activity.

    Positive net value = buying signal (score > 5)
    Negative net value = selling signal (score < 5)
    """
    scores = pd.Series(5.0, index=df.index)  # neutral default

    if "insider_net_value" in df.columns:
        mask = df["insider_net_value"].notna()
        if mask.any():
            # Normalize to 0-10 scale
            max_val = df.loc[mask, "insider_net_value"].abs().max()
            if max_val > 0:
                normalized = df.loc[mask, "insider_net_value"] / max_val
                scores[mask] = 5 + 5 * normalized.clip(-1, 1)

    return scores


def calculate_earnings_score(df: pd.DataFrame) -> pd.Series:
    """
    Calculate earnings quality score (0-10) based on earnings surprise history.

    Positive alpha = consistent outperformance (score > 5)
    Negative alpha = consistent underperformance (score < 5)
    """
    scores = pd.Series(5.0, index=df.index)  # neutral default

    if "earnings_avg_alpha" in df.columns:
        mask = df["earnings_avg_alpha"].notna()
        if mask.any():
            # Alpha > 3% = excellent, < -3% = poor
            normalized = df.loc[mask, "earnings_avg_alpha"].clip(-3, 3) / 3
            scores[mask] = 5 + 5 * normalized

    return scores


def calculate_financial_health_score(df: pd.DataFrame) -> pd.Series:
    """
    Calculate financial health score (0-10) based on key ratios.

    Combines profitability (net margin, ROE) and safety (liquidity).
    """
    scores = pd.Series(5.0, index=df.index)  # neutral default

    has_ratios = (
        "net_margin" in df.columns
        and "roe" in df.columns
        and "current_ratio" in df.columns
    )

    if has_ratios:
        mask = (
            df["net_margin"].notna()
            & df["roe"].notna()
            & df["current_ratio"].notna()
        )

        if mask.any():
            # Profitability scores
            margin_score = df.loc[mask, "net_margin"].clip(0, 0.3) / 0.3 * 3.33
            roe_score = df.loc[mask, "roe"].clip(0, 0.3) / 0.3 * 3.33

            # Liquidity score
            liquidity_score = df.loc[mask, "current_ratio"].clip(0, 3) / 3 * 3.33

            scores[mask] = margin_score + roe_score + liquidity_score

    return scores


def calculate_enhanced_total_score(df: pd.DataFrame, weights: dict = None) -> pd.Series:
    """
    Calculate enhanced total score incorporating new data sources.

    Args:
        df: DataFrame with both traditional and enhanced data
        weights: Optional custom weights for each factor

    Returns:
        Enhanced total score (0-100)
    """
    if weights is None:
        weights = {
            "traditional": 0.60,  # Existing factors (quality, value, etc.)
            "insider": 0.15,
            "earnings": 0.15,
            "financial": 0.10,
        }

    # Start with traditional score (if exists)
    if "total_score" in df.columns:
        base_score = df["total_score"] * weights["traditional"]
    else:
        base_score = pd.Series(50.0, index=df.index) * weights["traditional"]

    # Add new factor scores
    insider_score = calculate_insider_score(df) * 10 * weights["insider"]
    earnings_score = calculate_earnings_score(df) * 10 * weights["earnings"]
    financial_score = calculate_financial_health_score(df) * 10 * weights["financial"]

    enhanced_score = base_score + insider_score + earnings_score + financial_score

    return enhanced_score.clip(0, 100)


__all__ = [
    "calculate_insider_score",
    "calculate_earnings_score",
    "calculate_financial_health_score",
    "calculate_enhanced_total_score",
]
```

### 4.2 Update `score.py`

**File:** `src/finviz_weekly/score.py`

Add conditional enhanced scoring:

```python
from .score_enhanced import calculate_enhanced_total_score

def score_fundamentals(df: pd.DataFrame, use_enhanced: bool = False) -> pd.DataFrame:
    """Score fundamentals with optional enhanced scoring."""
    scored = df.copy()

    # ... existing scoring code ...

    if use_enhanced and any(col in scored.columns for col in ["insider_net_value", "earnings_avg_alpha", "net_margin"]):
        LOGGER.info("Calculating enhanced scores...")
        scored["enhanced_score"] = calculate_enhanced_total_score(scored)
        scored["total_score"] = scored["enhanced_score"]  # Replace or supplement

    return scored
```

---

## Phase 5: Update Checkpoint Format

### 5.1 Checkpoint Schema

Update checkpoint format to include new columns:

**Before:**
```json
{"ticker": "AAPL", "market_cap": 3000000000000, "quality_score": 8.5, ...}
```

**After:**
```json
{
  "ticker": "AAPL",
  "market_cap": 3000000000000,
  "quality_score": 8.5,
  "insider_net_value": 5000000.0,
  "insider_total_buys": 10,
  "insider_total_sells": 5,
  "earnings_avg_alpha": 2.3,
  "earnings_win_rate": 75.0,
  "net_margin": 0.25,
  "roe": 1.5,
  ...
}
```

The checkpoint system will automatically handle new columns.

---

## Phase 6: Testing Strategy

### 6.1 Unit Tests

**New file:** `tests/test_enhance.py`

```python
import pandas as pd
import pytest
from finviz_weekly.enhance import merge_enhanced_data
from finviz_weekly.score_enhanced import calculate_enhanced_total_score


def test_merge_enhanced_data():
    """Test merging enhanced data into fundamentals."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "MSFT"],
        "market_cap": [3000000000000, 2500000000000],
    })

    enhanced_data = {
        "AAPL": {
            "insider": {"net_value": 1000000.0, "total_buys": 5, "total_sells": 2},
            "earnings": {"avg_day_0_alpha": 2.5, "positive_reaction_pct": 75.0},
            "financials": {"ratios": {"net_margin": 0.25, "roe": 1.5}},
        }
    }

    result = merge_enhanced_data(df, enhanced_data)

    assert "insider_net_value" in result.columns
    assert result.loc[0, "insider_net_value"] == 1000000.0
    assert result.loc[1, "insider_net_value"] == 0.0  # MSFT not in enhanced_data


def test_calculate_enhanced_total_score():
    """Test enhanced scoring calculation."""
    df = pd.DataFrame({
        "ticker": ["AAPL"],
        "total_score": [80.0],
        "insider_net_value": [5000000.0],
        "earnings_avg_alpha": [2.0],
        "net_margin": [0.25],
        "roe": [1.5],
        "current_ratio": [2.0],
    })

    score = calculate_enhanced_total_score(df)

    assert len(score) == 1
    assert 0 <= score[0] <= 100
    assert score[0] > df["total_score"][0]  # Should be higher with positive factors
```

### 6.2 Integration Test

```bash
# Test with enhanced flags
python -m finviz_weekly run --mode tickers --tickers "AAPL,MSFT" \
  --include-insider --include-earnings --include-financials \
  --enhanced-scoring --out data
```

---

## Phase 7: Documentation Updates

### 7.1 Update README.md

Add usage examples:

```markdown
## Enhanced Data Sources

Scrape additional data to enrich your analysis:

```bash
# Run with all enhanced data sources
python -m finviz_weekly run --mode universe --ticker-limit 500 \
  --include-insider --include-earnings --include-financials \
  --enhanced-scoring --out data
```

### 7.2 Update CLI Help

```bash
python -m finviz_weekly run --help
```

Should show new options.

---

## Rollout Plan

### Week 1: Development
- [ ] Add optional flags to CLI
- [ ] Create enhance.py module
- [ ] Update config.py with new fields

### Week 2: Integration
- [ ] Modify pipeline.py to call enhance functions
- [ ] Create score_enhanced.py module
- [ ] Write unit tests

### Week 3: Testing
- [ ] Test with small ticker set (10 tickers)
- [ ] Test with medium ticker set (100 tickers)
- [ ] Validate data quality and performance

### Week 4: Production
- [ ] Update documentation
- [ ] Update GitHub Actions workflow
- [ ] Deploy to production
- [ ] Monitor for issues

---

## Success Metrics

- ✅ Zero breaking changes to existing functionality
- ✅ New flags default to `false` (opt-in)
- ✅ All existing tests still pass
- ✅ New tests achieve >80% coverage
- ✅ Enhanced scoring improves backtest results
- ✅ Documentation is comprehensive

---

## Future Enhancements

- **Caching**: Cache enhanced data to avoid re-scraping
- **Parallelization**: Scrape enhanced data concurrently
- **Incremental Updates**: Only scrape changed/new tickers
- **Quality Validation**: Automated data quality checks
- **Custom Weights**: User-configurable scoring weights
- **Strategy Presets**: Pre-defined scoring strategies

---

## Questions & Decisions

1. **Default Behavior**: Should enhanced scrapers be opt-in or opt-out?
   - **Decision**: Opt-in (default=False) to avoid breaking changes

2. **Scoring Approach**: Replace or supplement existing score?
   - **Decision**: Create `enhanced_score` column, let user choose

3. **Performance Impact**: Acceptable scraping time increase?
   - **Decision**: ~3x longer, but optional so acceptable

4. **Data Persistence**: Store enhanced data separately or merged?
   - **Decision**: Merged into main dataframe for simplicity

5. **Checkpoint Compatibility**: Backward compatible?
   - **Decision**: Yes, new columns are optional

---

## Summary

This integration plan provides a **non-breaking, incremental approach** to adding enhanced data sources to the main pipeline. Users can opt-in to new features while existing functionality remains unchanged.

**Next Actions:**
1. Review this plan
2. Start with Phase 1 (CLI flags)
3. Test incrementally
4. Roll out to production gradually
