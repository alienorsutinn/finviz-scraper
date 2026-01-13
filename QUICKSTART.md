# Quick Start Guide - Finviz Scraper

## Overview

This project scrapes financial data from Finviz.com and provides:
- Multi-factor stock screening (quality, value, growth, momentum, risk, oversold)
- Insider trading analysis
- Earnings reaction tracking
- Financial statement analysis
- AI-powered research synthesis
- Backtesting framework

## Installation

### Prerequisites
- Python 3.11+
- pip

### Setup

```bash
# Clone the repository
git clone <your-repo-url>
cd finviz-scraper

# Install dependencies
pip install -e .

# For development
pip install -e ".[dev]"
```

## Basic Usage

### 1. Main Scraper (Fundamentals + Scoring)

```bash
# Scrape all stocks (universe mode)
python -m finviz_weekly run --mode universe --ticker-limit 500 --out data

# Scrape specific tickers
python -m finviz_weekly run --mode tickers --tickers AAPL,TSLA,MSFT --out data

# Scrape from file
python -m finviz_weekly run --mode tickers --tickers-file my_tickers.txt --out data
```

### 2. Screen and Rank Stocks

```bash
# Generate top candidates using multi-factor scoring
python -m finviz_weekly screen --top 50 --out data

# With learned weights (requires historical data)
python -m finviz_weekly screen --top 50 --use-learned --out data
```

### 3. New Scraper Commands

#### Insider Trading

```bash
# Single ticker
python -m finviz_weekly insider --ticker AAPL

# Multiple tickers
python -m finviz_weekly insider --tickers AAPL,TSLA,MSFT

# Output as CSV
python -m finviz_weekly insider --ticker AAPL --output-format csv
```

#### Earnings Reactions

```bash
# Single ticker
python -m finviz_weekly earnings --ticker AAPL

# Multiple tickers
python -m finviz_weekly earnings --tickers AAPL,TSLA,MSFT --output-format csv
```

#### Financial Statements

```bash
# Single ticker
python -m finviz_weekly financials --ticker AAPL

# Multiple tickers
python -m finviz_weekly financials --tickers AAPL,TSLA,MSFT
```

### 4. AI Research (Optional)

Requires API keys in `.env`:

```bash
# Copy template
cp .env.example .env

# Edit .env and add your keys:
# OPENAI_API_KEY=your_key
# BRAVE_API_KEY=your_key (optional)
```

Run AI research on candidates:

```bash
python -m finviz_weekly debate --input candidates --max-tickers 10 --out data
```

## Example Workflows

### Workflow 1: Weekly Stock Screening

```bash
# 1. Scrape universe
python -m finviz_weekly run --mode universe --ticker-limit 1000 --out data

# 2. Generate candidates
python -m finviz_weekly screen --top 50 --out data

# 3. (Optional) AI research on top candidates
python -m finviz_weekly debate --input candidates --max-tickers 20 --out data
```

### Workflow 2: Enhanced Analysis with New Scrapers

```bash
# 1. Scrape fundamentals for your watchlist
python -m finviz_weekly run --mode tickers --tickers-file watchlist.txt --out data

# 2. Get insider trading signals
python -m finviz_weekly insider --tickers-file watchlist.txt > insider_data.json

# 3. Check earnings history
python -m finviz_weekly earnings --tickers-file watchlist.txt > earnings_data.json

# 4. Analyze financials
python -m finviz_weekly financials --tickers-file watchlist.txt > financials_data.json
```

### Workflow 3: Integration Demo

```bash
# Run the integration example
python examples/integrate_new_scrapers.py --tickers AAPL,TSLA,MSFT

# This will:
# - Fetch fundamentals
# - Add insider trading data
# - Add earnings reaction data
# - Add financial ratios
# - Calculate enhanced scores
# - Save to data/enhanced/
```

## Output Locations

```
data/
├── runs/           # Daily snapshots with full data
├── latest/         # Most recent "ok" fundamentals
├── history/        # Concatenated historical data
├── candidates/     # Top-ranked stocks from screening
├── conviction2/    # Second-tier candidates
└── enhanced/       # Enhanced data with new scrapers
```

## Running Tests

```bash
# All tests
pytest

# With coverage
pytest --cov=finviz_weekly --cov-report=html

# Specific test file
pytest tests/test_new_scrapers.py -v
```

## Docker Usage

```bash
# Build image
docker-compose build scraper

# Run scraper
docker-compose run scraper run --mode universe --ticker-limit 500 --out /data

# Run development environment
docker-compose run dev bash

# Run tests
docker-compose run test
```

## GitHub Actions (CI/CD)

The workflow runs automatically:
- **Schedule**: Every Monday at 1 AM UTC
- **Manual**: Go to Actions → weekly-finviz → Run workflow

To enable data persistence in GitHub:
1. Set repository secret: `PERSIST_RESULTS=true`
2. Workflow will commit scraped data back to repo

## Configuration

### Environment Variables (.env)

```bash
# Search API Keys (for AI research)
BRAVE_API_KEY=your_key
GOOGLE_CSE_API_KEY=your_key
GOOGLE_CSE_CX=your_cx

# LLM API Keys (for AI synthesis)
OPENAI_API_KEY=your_key
OPENAI_MODEL=gpt-4o-mini

# Proxy (if needed)
FINVIZ_PROXY=http://proxy:port
```

### Rate Limiting

Default: 0.5 requests/sec (safe for Finviz)

```bash
# Adjust rate limiting
python -m finviz_weekly run --mode universe --rate-per-sec 0.3 --out data
```

## Common Issues

### 1. Proxy/Network Errors
```
Error: Tunnel connection failed: 403 Forbidden
```
**Solution**: Set `FINVIZ_PROXY` in .env or run from a different network

### 2. Rate Limiting
```
Error: 429 Too Many Requests
```
**Solution**: Decrease `--rate-per-sec` or increase `--page-sleep-min`

### 3. Missing Dependencies
```
Error: No module named 'finviz_weekly'
```
**Solution**: Run `pip install -e .` from project root

## Next Steps

1. **Explore Examples**: Check `examples/` directory for integration patterns
2. **Read Documentation**: See `docs/NEW_SCRAPERS.md` for advanced usage
3. **Customize Scoring**: Modify `src/finviz_weekly/score.py` weights
4. **Backtest Strategies**: Use `src/finviz_weekly/backtest.py`
5. **Contribute**: See `CONTRIBUTING.md` for development guidelines

## Support

- **Issues**: Report bugs at [GitHub Issues](https://github.com/your-repo/issues)
- **Documentation**: See `README.md` and `docs/` directory
- **Examples**: Check `examples/` directory

## Key Features

✅ Multi-factor scoring (6 factors)
✅ Crash-safe pipeline with checkpoints
✅ Historical data tracking
✅ Insider trading analysis (NEW)
✅ Earnings reaction tracking (NEW)
✅ Financial statement analysis (NEW)
✅ AI-powered research
✅ Backtesting framework
✅ Docker support
✅ GitHub Actions CI/CD
✅ Comprehensive test suite (37 tests)

Happy scraping! 🚀
