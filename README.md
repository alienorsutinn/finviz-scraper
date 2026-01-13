# Finviz Weekly Scraper

[![CI](https://github.com/alienorsutinn/finviz-scraper/actions/workflows/finviz-snapshot.yml/badge.svg)](https://github.com/alienorsutinn/finviz-scraper/actions/workflows/finviz-snapshot.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A production-grade Python system for scraping, scoring, and analyzing stock fundamentals from Finviz.com. Combines traditional quantitative screening with AI-powered research synthesis to identify investment candidates.

## ✨ Features

### Core Functionality
- **Automated Stock Screening** - Scrapes 500-8000+ US equities from Finviz
- **Multi-Factor Scoring** - Quality, value, risk, growth, momentum, and oversold factors
- **AI Research Layer** - LLM-powered investment research using GPT-4/Claude
- **Crash-Safe Pipeline** - Resumable checkpoints for interrupted scrapes
- **Historical Tracking** - Append-only history for backtesting and ML training
- **Data Quality Monitoring** - Automated validation and outlier detection

### Advanced Features
- **Backtesting Framework** - Test strategies on historical data
- **Machine Learning** - Learn optimal factor weights from data
- **Docker Support** - Containerized deployment
- **GitHub Actions CI/CD** - Automated bi-daily runs
- **REST API Ready** - Export data in Parquet/CSV/JSON

## 🚀 Quick Start

### Prerequisites
- Python 3.11 or higher
- Git
- (Optional) Docker for containerized deployment

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

## Usage

Run the pipeline locally (example tickers mode):

```bash
python -m finviz_weekly run --mode tickers --tickers "BLK,FTNT" --out data
```

Discover the universe, limited to the first 500 tickers, at a gentle rate:

```bash
python -m finviz_weekly run --mode universe --ticker-limit 500 --rate-per-sec 0.5 --out data
```

Key CLI options:
- `--mode [universe|tickers]`
- `--tickers "AAPL,MSFT"` or `--tickers-file config/tickers.txt`
- `--industry-limit N` and `--ticker-limit N`
- `--rate-per-sec`, `--page-sleep-min`, `--page-sleep-max`
- `--out data` and `--formats parquet,csv`
- `--log-level INFO`

Outputs are written to:
- `data/runs/YYYY-MM-DD/finviz_fundamentals.parquet`
- `data/runs/YYYY-MM-DD/finviz_fundamentals.csv`
- `data/runs/YYYY-MM-DD/meta.json`
- `data/latest/finviz_fundamentals.parquet`
- `data/runs/YYYY-MM-DD/finviz_scored.parquet` (canonical scored snapshot)
- `data/runs/YYYY-MM-DD/finviz_scored.csv.gz` (Excel-friendly mirror of the scored snapshot)
- `data/latest/finviz_scored.parquet`
- `data/latest/finviz_scored.csv.gz`
- `data/history/finviz_fundamentals_history.parquet` (append-only with `as_of_date`)

## Weekly workflow
1. Run scraping (`python -m finviz_weekly run ...`).
2. Run screening (`python -m finviz_weekly screen --out data`). This writes `finviz_scored.parquet`/`csv.gz` plus candidates and conviction lists.
3. Run the debate layer (research on by default, safe for Brave free tier with 12 queries/ticker):  
   `python -m finviz_weekly debate --out data --input candidates --max-tickers 20`
   - Env vars: `BRAVE_API_KEY` (primary search), optional `GOOGLE_CSE_API_KEY` + `GOOGLE_CSE_CX` (fallback), `OPENAI_API_KEY` and `OPENAI_MODEL` (default `gpt-5-mini`). Use `--provider mock` to force offline mode for tests.
   - Debate outputs: `data/debate/YYYY-MM-DD/{ticker}.json`, `{ticker}_evidence.json`, `debate_results.csv`, `debate_report.md`.

Screening options (non-breaking defaults):
- `--use-learned` is **off** by default; learned scores are only considered when explicitly enabled **and** history thresholds are met (`--learned-min-unique-dates`, `--learned-min-forward-rows`).
- `--watchlist-file path/to/tickers.txt` appends those tickers to `candidates.txt` and exports `watchlist.csv` under `data/latest` and `data/runs/<date>`.
- `--normalize-by {none,sector,industry}` keeps percentile scoring within groups when group size ≥30, otherwise falls back to global ranks.  
  Example: `python -m finviz_weekly screen --out data --normalize-by sector --watchlist-file watch.txt`.

## GitHub Actions

The workflow in `.github/workflows/weekly.yml` runs every Monday at 01:00 UTC (and on manual dispatch). It scrapes up to 500 tickers at a low rate and uploads artifacts. When the environment variable `PERSIST_RESULTS` is set to `true`, the workflow will commit the `data/` directory back to the repository using the message `chore(data): weekly finviz snapshot YYYY-MM-DD`.

## Notes
- Scraping is subject to website changes; use conservative rate limits.
- Tests avoid network calls by using fixtures and monkeypatching.
- Python 3.11 is required.

## 🐳 Docker Usage

### Run with Docker Compose

```bash
# Copy environment file
cp .env.example .env
# Edit .env with your API keys

# Run scraper
docker-compose up scraper

# Run full workflow (scrape + screen)
docker-compose up full-workflow

# Run tests
docker-compose up test

# Development mode
docker-compose up dev
```

### Build and Run Manually

```bash
# Build image
docker build -t finviz-scraper .

# Run scraper
docker run -v $(pwd)/data:/app/data finviz-scraper
```

## 📊 New Features

### Data Quality Monitoring

Automatically validate scraped data:

```bash
# Check latest data quality
python -m finviz_weekly.quality

# Or use in Python
from finviz_weekly.quality import check_latest_data
passed = check_latest_data()
```

### Backtesting

Test your scoring strategy on historical data:

```bash
# Run backtest
python -m finviz_weekly.backtest \
  --history data/history/finviz_fundamentals_history.parquet \
  --start 2024-01-01 \
  --end 2025-01-01 \
  --top-n 20 \
  --rebalance-days 7
```

Results include:
- Total and annual returns
- Sharpe ratio
- Maximum drawdown
- Win rate and trade statistics

### Pre-commit Hooks

Install code quality hooks:

```bash
pip install pre-commit
pre-commit install
pre-commit run --all-files
```

Hooks include:
- Black code formatting
- isort import sorting
- flake8 linting
- mypy type checking
- Security checks with bandit
- Spell checking


## 🆕 New: Additional Data Sources

We now support scraping **insider trading**, **earnings reactions**, and **detailed financial statements**!

### Insider Trading
```python
from finviz_weekly.insider import scrape_insider_trading, aggregate_insider_by_ticker

# Get insider trades for a ticker
trades = scrape_insider_trading("AAPL", session, http_config)
stats = aggregate_insider_by_ticker(trades)
print(f"Net insider value: ${stats['AAPL']['net_value']:,.0f}")
```

### Earnings Reactions
```python
from finviz_weekly.earnings import scrape_earnings_reactions, calculate_earnings_statistics

# Get historical earnings reactions
earnings = scrape_earnings_reactions("TSLA", session, http_config)
stats = calculate_earnings_statistics(earnings)
print(f"Average earnings reaction: {stats['avg_day_0_change']:.2%}")
```

### Enhanced Financials
```python
from finviz_weekly.financials import scrape_financial_statements, calculate_financial_ratios

# Get detailed financial statements
statements = scrape_financial_statements("AAPL", session, http_config)
ratios = calculate_financial_ratios(statements)
print(f"ROE: {ratios['roe']:.2%}, Debt/Equity: {ratios['debt_to_equity']:.2f}x")
```

📖 **Full Documentation:** See [docs/NEW_SCRAPERS.md](docs/NEW_SCRAPERS.md) for complete guide and strategy examples.
