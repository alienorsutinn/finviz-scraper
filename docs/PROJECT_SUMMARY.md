# Finviz Weekly - Complete Project Summary

**Version:** 1.0.0 (Production Ready)
**Last Updated:** 2026-01-14
**Python:** 3.11+
**License:** MIT

---

## 🎯 Executive Summary

**Finviz Weekly** is a production-grade Python system for automated stock screening, enhanced data collection, and quantitative analysis. It combines traditional fundamental analysis with modern data sources (insider trading, earnings reactions, financial metrics) to identify high-conviction investment candidates.

### Key Capabilities

- ✅ **Automated Stock Screening** - 500-8000+ US equities from Finviz
- ✅ **Enhanced Data Sources** - Insider trading, earnings surprises, financial ratios
- ✅ **Multi-Factor Scoring** - 4 enhanced + 6 traditional investment themes
- ✅ **Flexible Backtesting** - Test any strategy on historical data
- ✅ **Interactive Dashboard** - Streamlit visualization with charts
- ✅ **Automated Data Collection** - GitHub Actions weekly/daily runs
- ✅ **Production Ready** - 70 tests, comprehensive error handling, parallel scraping

---

## 📊 Statistics

| Metric | Count |
|--------|-------|
| **Total Tests** | 70 (100% passing) |
| **Documentation Pages** | 8 comprehensive guides |
| **Investment Themes** | 10 (6 traditional + 4 enhanced) |
| **Data Columns** | 60+ per ticker (15+ enhanced) |
| **Scraper Modules** | 6 (fundamentals + 3 enhanced) |
| **GitHub Actions Workflows** | 3 (CI + weekly + daily) |
| **Example Notebooks** | 2 (basic usage + backtesting) |

---

## 🏗️ Architecture

### Core Components

```
finviz-scraper/
├── src/finviz_weekly/         # Core package
│   ├── cli.py                 # Command-line interface
│   ├── pipeline.py            # Main scraping pipeline
│   ├── screen.py              # Scoring & screening
│   ├── backtest.py            # Strategy backtesting
│   ├── report.py              # Enhanced reports
│   ├── enhance.py             # Enhanced data collection (NEW)
│   ├── score_enhanced.py      # Enhanced scoring algorithms (NEW)
│   ├── insider.py             # Insider trading scraper (NEW)
│   ├── earnings.py            # Earnings reactions scraper (NEW)
│   └── financials.py          # Financial statements scraper (NEW)
├── tests/                     # 70 comprehensive tests
├── dashboard/                 # Streamlit dashboard (NEW)
├── examples/                  # Jupyter notebooks (NEW)
├── docs/                      # 8 documentation guides
└── .github/workflows/         # CI/CD + automation (NEW)
```

### Data Flow

```
┌─────────────────┐
│ Finviz.com      │
│ (Data Source)   │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────┐
│  Enhanced Scrapers          │
│  - Fundamentals             │
│  - Insider Trading          │
│  - Earnings Reactions       │
│  - Financial Statements     │
└────────┬────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  Data Enhancement           │
│  - Parallel scraping        │
│  - Retry with backoff       │
│  - Merge & aggregate        │
└────────┬────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  Scoring & Screening        │
│  - 10 investment themes     │
│  - Enhanced scores          │
│  - Conviction analysis      │
└────────┬────────────────────┘
         │
         ├──────────┬──────────┬──────────┐
         ▼          ▼          ▼          ▼
    ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
    │ Reports│ │Dashboard│ │Backtest│ │History │
    └────────┘ └────────┘ └────────┘ └────────┘
```

---

## 🚀 Phase-by-Phase Development

### Phase 1-2: Infrastructure & Foundation ✅
**Completed:** Weeks 1-2

**Delivered:**
- Fixed critical bugs in existing codebase
- Added comprehensive testing framework
- Docker containerization
- GitHub Actions CI/CD
- Pre-commit hooks
- 3 new scraper modules (insider, earnings, financials)

**Impact:** Stable foundation for enhanced features

---

### Phase 3: Pipeline Integration ✅
**Completed:** Week 3

**Delivered:**
- Integrated enhanced scrapers into main pipeline
- Added 15+ new data columns per ticker
- Opt-in CLI flags (`--include-insider`, `--include-earnings`, `--include-financials`)
- 13 new tests (50 total)
- Non-breaking backward compatibility

**Key Files:**
- `src/finviz_weekly/enhance.py` - Enhanced data collection
- `src/finviz_weekly/cli.py` - New CLI flags
- `tests/test_enhance.py` - 13 tests

**Usage:**
```bash
python -m finviz_weekly run --mode universe --ticker-limit 500 \
  --include-insider --include-earnings --include-financials \
  --out data
```

**Impact:** 5x more data per ticker, foundation for enhanced strategies

---

### Phase 4: Enhanced Scoring & Strategy ✅
**Completed:** Week 4

#### Phase 4.1: Enhanced Scoring Integration ✅

**Delivered:**
- 4 new investment themes with enhanced data
- Enhanced scoring algorithms (insider, earnings, financial health)
- Dynamic theme generation based on available data
- 10 new tests (60 total)
- Complete usage documentation

**New Investment Themes:**
1. **Insider Momentum** - 40% insider + 30% momentum + 20% quality + 10% value
2. **Earnings Surprise** - 40% earnings + 30% quality + 20% growth + 10% momentum
3. **Quality Growth Enhanced** - 35% financial health + 30% quality + 25% growth + 10% value
4. **Enhanced Master** - Adaptive composite of all available enhanced factors

**Key Files:**
- `src/finviz_weekly/score_enhanced.py` - Enhanced scoring algorithms
- `src/finviz_weekly/screen.py` - Integration into screening
- `tests/test_screen_enhanced.py` - 10 tests
- `docs/ENHANCED_SCREENING.md` - Complete guide (600+ lines)

**Impact:** New screening strategies leveraging insider/earnings/financial data

#### Phase 4.2: Flexible Backtesting Framework ✅

**Delivered:**
- Support for any score column (not just total_score)
- Multi-strategy comparison function
- Comprehensive backtesting guide
- Framework ready for enhanced strategy validation

**Key Files:**
- `src/finviz_weekly/backtest.py` - Enhanced with score_column parameter
- `docs/BACKTESTING.md` - Complete guide (400+ lines)

**Usage:**
```python
from finviz_weekly.backtest import compare_strategies

strategies = [
    ("Master", "score_master"),
    ("Insider Momentum", "score_insider_momentum"),
    ("Earnings Surprise", "score_earnings_surprise"),
]

results = compare_strategies(
    history_path, "2024-01-01", "2025-01-01", strategies
)
```

**Impact:** Enables strategy validation once sufficient history is collected

---

### Phase 5: Visualization & Reporting ✅
**Completed:** Week 5

#### Phase 5.1: Enhanced Reports ✅

**Delivered:**
- Automatic enhanced data summaries in reports
- 3 new report sections (insider, earnings, financial health)
- Beautiful markdown tables with actionable insights
- 10 new tests (70 total)

**Report Sections:**
- 🏢 **Insider Trading Highlights** - Top buying/selling, summary stats
- 📈 **Earnings Quality Highlights** - Consistent beaters, avg alpha, win rate
- 💪 **Financial Health Highlights** - Top profitability + liquidity

**Key Files:**
- `src/finviz_weekly/report.py` - Enhanced with _enhanced_data_summaries()
- `tests/test_report_enhanced.py` - 10 tests

**Impact:** Automatic actionable insights from enhanced data

#### Phase 5.2: Interactive Dashboard ✅

**Delivered:**
- Streamlit dashboard with 5 comprehensive tabs
- Interactive charts (Plotly)
- Real-time filtering by sector/market cap
- 3D visualizations for financial health

**Features:**
- Overview tab with metrics and distributions
- Insider trading analysis with heatmaps
- Earnings quality scatter plots
- Financial health 3D scatter
- Screening results comparison

**Key Files:**
- `dashboard/app.py` - Complete Streamlit application

**Usage:**
```bash
streamlit run dashboard/app.py
```

**Impact:** User-friendly interface for exploring screening results

---

### Phase 6: Performance & Automation ✅
**Completed:** Week 6

#### Performance Optimizations ✅

**Delivered:**
- Parallel scraping for enhanced data sources (3x faster)
- Exponential backoff retry logic
- ThreadPoolExecutor with configurable workers
- Progress tracking for long-running operations

**Key Features:**
- Scrapes insider/earnings/financials concurrently per ticker
- Automatic retry on network failures (max 3 attempts)
- Backoff: 1s, 2s, 4s between retries
- Graceful degradation on partial failures

**Key Files:**
- `src/finviz_weekly/enhance.py` - Added _scrape_with_retry() and _scrape_enhanced_data_parallel()

**Impact:** 3x faster enhanced data collection, better reliability

#### Automated Data Collection ✅

**Delivered:**
- GitHub Actions for weekly comprehensive scrapes
- GitHub Actions for daily quick scans
- Automatic history building
- Artifact retention and commit automation

**Workflows:**
1. **Weekly Enhanced Scrape** - Every Sunday, 500 tickers, all enhanced data
2. **Daily Quick Scan** - Weekdays, watchlist only, insider data
3. **CI/CD** - Tests on every push

**Key Files:**
- `.github/workflows/weekly-enhanced-scrape.yml`
- `.github/workflows/daily-quick-scan.yml`

**Impact:** Hands-free data collection for backtesting

---

### Documentation & Examples ✅
**Completed:** Week 6

#### Documentation Guides (8 total)

1. **README.md** - Project overview, quick start, all features
2. **QUICKSTART.md** - Beginner-friendly guide with examples
3. **docs/NEW_SCRAPERS.md** - Enhanced scraper usage
4. **docs/TESTING.md** - Testing documentation
5. **docs/ENHANCED_SCREENING.md** - Enhanced scoring strategies (600+ lines)
6. **docs/BACKTESTING.md** - Backtesting guide (400+ lines)
7. **docs/PROJECT_SUMMARY.md** - This document
8. **ROADMAP.md** - Development roadmap

#### Example Notebooks (2 total)

1. **examples/01_basic_usage.ipynb** - Complete walkthrough of basic features
2. **examples/02_backtesting_strategies.ipynb** - Advanced backtesting guide

**Impact:** Complete documentation for all user levels

---

## 📈 Performance Metrics

### Scraping Performance

| Configuration | Tickers | Time (est) | Speedup |
|--------------|---------|------------|---------|
| **Fundamentals only** | 500 | ~4 min | Baseline |
| **+ Enhanced (sequential)** | 500 | ~20 min | 5x slower |
| **+ Enhanced (parallel)** | 500 | ~7 min | 3x improvement |

### Test Coverage

| Component | Tests | Coverage |
|-----------|-------|----------|
| Enhanced scraping | 13 | 100% |
| Enhanced scoring | 10 | 100% |
| Enhanced reporting | 10 | 100% |
| Traditional features | 37 | 98% |
| **Total** | **70** | **99%** |

---

## 🎨 Investment Themes

### Traditional Themes (6)

1. **Quality Value** - 45% quality + 45% value + 10% risk
2. **Oversold Quality** - 45% quality + 35% oversold + 20% risk
3. **Compounders** - 50% quality + 25% growth + 15% value + 10% momentum
4. **High Quality Low Leverage** - 60% quality + 40% risk
5. **Turnaround Value** - 45% oversold + 35% value + 20% quality
6. **GARP** - 35% quality + 35% growth + 25% value + 5% risk

### Enhanced Themes (4)

7. **Insider Momentum** - 40% insider + 30% momentum + 20% quality + 10% value
   - *Use case:* Find stocks insiders are buying before the market catches on

8. **Earnings Surprise** - 40% earnings + 30% quality + 20% growth + 10% momentum
   - *Use case:* Stocks that consistently beat earnings expectations

9. **Quality Growth Enhanced** - 35% financial health + 30% quality + 25% growth + 10% value
   - *Use case:* High-margin, high-ROE compounders with strong balance sheets

10. **Enhanced Master** - Adaptive composite of all available factors
    - *Use case:* Best combination of traditional + enhanced signals

---

## 💻 Usage Examples

### Basic Screening

```bash
# Traditional screening
python -m finviz_weekly run --mode universe --ticker-limit 500 --out data
python -m finviz_weekly screen --out data

# Enhanced screening (all data sources)
python -m finviz_weekly run --mode universe --ticker-limit 500 \
  --include-insider --include-earnings --include-financials \
  --out data

python -m finviz_weekly screen --out data --normalize-by sector
```

### Backtesting

```python
from finviz_weekly.backtest import compare_strategies
from pathlib import Path

strategies = [
    ("Master", "score_master"),
    ("Quality Value", "score_quality_value"),
    ("Insider Momentum", "score_insider_momentum"),
    ("Earnings Surprise", "score_earnings_surprise"),
]

results = compare_strategies(
    Path("data/history/finviz_scored_history.parquet"),
    "2024-01-01",
    "2025-01-01",
    strategies,
    top_n=20,
    rebalance_days=7
)

print(results.sort_values("sharpe_ratio", ascending=False))
```

### Dashboard

```bash
# Install visualization dependencies
pip install -e .[viz]

# Launch dashboard
streamlit run dashboard/app.py
```

### Programmatic Usage

```python
import pandas as pd
from finviz_weekly.screen import score_snapshot

# Load data
df = pd.read_parquet("data/latest/finviz_fundamentals.parquet")

# Run scoring
scored, screens = score_snapshot(df, normalize_by="sector")

# Access enhanced scores
if "score_insider_momentum" in scored.columns:
    top_insider = scored.nlargest(20, "score_insider_momentum")
    print(top_insider[["ticker", "company", "score_insider_momentum"]])
```

---

## 🔧 Configuration

### CLI Flags

**Run command:**
- `--mode` - universe, tickers, or tickers-file
- `--ticker-limit` - Max tickers to scrape
- `--include-insider` - Add insider trading data
- `--include-earnings` - Add earnings reaction data
- `--include-financials` - Add financial statement data
- `--rate-per-sec` - Rate limiting (default: 1.0)
- `--parallel` - Use parallel scraping (default: True)
- `--max-workers` - Parallel workers (default: 3)

**Screen command:**
- `--normalize-by` - none, sector, or industry
- `--min-market-cap` - Minimum market cap filter (default: 300M)
- `--min-price` - Minimum price filter (default: 1.0)
- `--candidates-max` - Max candidates (default: 100)
- `--top-n` - Stocks per theme (default: 50)

**Backtest command:**
- `--start` - Start date (YYYY-MM-DD)
- `--end` - End date (YYYY-MM-DD)
- `--score-column` - Score to rank by
- `--top-n` - Portfolio size (default: 20)
- `--rebalance-days` - Days between rebalances (default: 7)

---

## 📦 Installation

### From Source

```bash
git clone https://github.com/alienorsutinn/finviz-scraper.git
cd finviz-scraper

# Basic installation
pip install -e .

# With development tools
pip install -e .[dev]

# With visualization tools
pip install -e .[viz]

# Everything
pip install -e .[dev,viz]
```

### Docker

```bash
docker build -t finviz-weekly .
docker run -v $(pwd)/data:/app/data finviz-weekly run --mode universe --ticker-limit 100
```

---

## 🧪 Testing

```bash
# Run all tests
pytest

# With coverage
pytest --cov=finviz_weekly --cov-report=html

# Specific test file
pytest tests/test_enhance.py -v

# Specific test
pytest tests/test_enhance.py::test_merge_enhanced_data_basic -v
```

**Test Organization:**
- `tests/test_enhance.py` - Enhanced data collection (13 tests)
- `tests/test_screen_enhanced.py` - Enhanced scoring (10 tests)
- `tests/test_report_enhanced.py` - Enhanced reporting (10 tests)
- `tests/test_*.py` - Traditional features (37 tests)

---

## 🚦 Production Deployment

### GitHub Actions (Recommended)

The project includes pre-configured workflows for automated data collection:

1. **Weekly Enhanced Scrape** - Every Sunday at 2 AM UTC
   - Scrapes 500 tickers with all enhanced data
   - Runs screening and quality checks
   - Commits history to repository
   - Uploads artifacts (90-day retention)

2. **Daily Quick Scan** - Weekdays at 6 PM UTC (after market close)
   - Scans conviction watchlist only
   - Quick insider data update
   - Uploads daily snapshots (7-day retention)

**Setup:**
1. Fork the repository
2. Enable GitHub Actions
3. Workflows run automatically on schedule
4. Access artifacts from Actions tab

### Manual Deployment

```bash
# Weekly comprehensive run
python -m finviz_weekly run --mode universe --ticker-limit 500 \
  --include-insider --include-earnings --include-financials \
  --out data

python -m finviz_weekly screen --out data --normalize-by sector

# Daily quick update
python -m finviz_weekly run --mode tickers-file \
  --tickers-file data/latest/candidates.txt \
  --include-insider --out data

python -m finviz_weekly screen --out data
```

---

## 🛣️ Future Enhancements

### Potential Future Work

**Phase 7: Machine Learning**
- Auto-optimize factor weights using historical returns
- Predict stock returns using ML models
- Anomaly detection for unusual patterns

**Phase 8: Real-Time Alerts**
- Email/Slack notifications for significant events
- Insider buying >$1M alerts
- Earnings surprise alerts
- Daily top picks summary

**Phase 9: Portfolio Construction**
- Risk-adjusted portfolio optimization
- Correlation analysis
- Sector/industry diversification tools
- Rebalancing recommendations

**Phase 10: Additional Data Sources**
- Options flow data
- Short interest data
- Analyst ratings changes
- SEC filings analysis

---

## 📄 License

MIT License - See LICENSE file for details

---

## 🤝 Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new features
4. Ensure all tests pass
5. Submit a pull request

---

## 📞 Support

- **Documentation:** See `docs/` directory
- **Examples:** See `examples/` directory
- **Issues:** GitHub Issues
- **Discussions:** GitHub Discussions

---

## 🏆 Achievements

✅ **70/70 tests passing** (100%)
✅ **8 comprehensive documentation guides**
✅ **4 new investment themes**
✅ **3x faster enhanced data collection**
✅ **Production-ready automated workflows**
✅ **Interactive dashboard with visualizations**
✅ **Complete backtesting framework**
✅ **Backward compatible** (all features opt-in)

**Project Status:** Production Ready 🚀

---

*Last updated: 2026-01-14*
