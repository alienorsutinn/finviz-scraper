# Finviz Scraper - Project Roadmap

## Current State (✅ Completed)

### Phase 1: Foundation & Infrastructure
- ✅ Fixed critical bugs (missing functions, undefined variables)
- ✅ Added comprehensive test suite (37 tests passing)
- ✅ Docker support with multi-stage builds
- ✅ Pre-commit hooks (black, isort, flake8, mypy, bandit)
- ✅ GitHub Actions CI/CD pipeline
- ✅ Data quality monitoring framework
- ✅ Backtesting framework

### Phase 2: New Data Sources
- ✅ Insider trading scraper
- ✅ Earnings reaction analyzer
- ✅ Enhanced financial statements scraper
- ✅ CLI commands for all new scrapers
- ✅ Comprehensive documentation (QUICKSTART.md, README.md)
- ✅ Integration examples

### Phase 3: Pipeline Integration (✅ COMPLETED!)
- ✅ Added optional CLI flags (`--include-insider`, `--include-earnings`, `--include-financials`)
- ✅ Created `enhance.py` module for data scraping and merging
- ✅ Updated `pipeline.py` to call enhancement functions
- ✅ Added 15+ new columns to output (insider, earnings, financial ratios)
- ✅ Created `score_enhanced.py` for enhanced scoring algorithms
- ✅ Added 13 new tests (50 total tests passing)
- ✅ Updated documentation (README.md)
- ✅ All changes non-breaking (opt-in via flags)

**Usage:**
```bash
python -m finviz_weekly run --mode universe --ticker-limit 100 \
  --include-insider --include-earnings --include-financials \
  --out data
```

---

## Next Steps - Strategic Plan

### 🎯 Phase 4: Enhanced Scoring & Strategy (NEXT - 2-4 weeks)

**Priority: HIGH** | **Effort: Medium-High**

#### 3.1 Test New Scrapers in Production
```bash
# Test with real data (respecting rate limits)
python -m finviz_weekly insider --tickers AAPL,MSFT,GOOGL
python -m finviz_weekly earnings --tickers AAPL,MSFT,GOOGL
python -m finviz_weekly financials --tickers AAPL,MSFT,GOOGL
```

**Deliverables:**
- [ ] Validate scraper output quality with 10-20 real tickers
- [ ] Document any edge cases or parsing issues
- [ ] Measure scraping performance (time per ticker)
- [ ] Test rate limiting behavior

#### 3.2 Integrate New Scrapers into Main Pipeline
**File:** `src/finviz_weekly/pipeline.py`

**Changes needed:**
- [ ] Add optional flags: `--include-insider`, `--include-earnings`, `--include-financials`
- [ ] Scrape new data sources after fundamentals
- [ ] Merge new columns into main dataframe
- [ ] Update checkpoint format to include new data
- [ ] Add new data to output files

**Example:**
```python
# After scraping fundamentals, optionally scrape new sources
if config.include_insider:
    insider_data = scrape_insider_for_tickers(tickers, session, config.http)
    fundamentals_df = merge_insider_data(fundamentals_df, insider_data)
```

#### 3.3 Update GitHub Actions Workflow
**File:** `.github/workflows/weekly.yml`

**Changes needed:**
- [ ] Add flags to enable new scrapers in CI
- [ ] Upload new data artifacts (insider, earnings, financials)
- [ ] Add data quality checks for new sources
- [ ] Create separate workflow for daily insider tracking

**Example addition:**
```yaml
- name: Run enhanced scraper
  run: |
    python -m finviz_weekly run --mode universe --ticker-limit 500 \
      --include-insider --include-earnings --out data
```

#### 3.4 Add Data Quality Validation
**File:** `src/finviz_weekly/quality.py`

**Enhancements:**
- [ ] Add validation for insider data (check for required fields)
- [ ] Add validation for earnings data (check date formats, percentage ranges)
- [ ] Add validation for financial ratios (check for reasonable ranges)
- [ ] Create quality reports for each data source

---

### 🚀 Phase 4: Enhanced Scoring & Strategy (NEXT - 2-4 weeks)

**Priority: HIGH** | **Effort: High**

#### 4.1 Integrate New Data into Scoring Model
**File:** `src/finviz_weekly/score.py`

**New scoring factors:**
- [ ] **Insider Score (0-10):** Reward net buying, penalize net selling
- [ ] **Earnings Quality Score (0-10):** Reward consistent positive earnings surprises
- [ ] **Financial Health Score (0-10):** ROE, margins, liquidity ratios
- [ ] Update `total_score` to include new factors with configurable weights

**Example:**
```python
def calculate_enhanced_score(row):
    # Existing scores (60% weight)
    base_score = row['total_score'] * 0.6

    # New scores (40% weight)
    insider_score = calculate_insider_score(row['insider_net_value'])
    earnings_score = calculate_earnings_score(row['earnings_avg_alpha'])
    financial_score = calculate_financial_score(row['roe'], row['net_margin'])

    return base_score + (insider_score * 0.15) + (earnings_score * 0.15) + (financial_score * 0.10)
```

#### 4.2 Backtest Enhanced Strategy
**File:** `src/finviz_weekly/backtest.py`

**Enhancements:**
- [ ] Add support for multi-factor backtesting
- [ ] Compare baseline strategy vs. enhanced strategy
- [ ] Generate performance reports (Sharpe, drawdown, win rate)
- [ ] Optimize factor weights using historical data

**Deliverables:**
- [ ] Backtest report comparing strategies
- [ ] Optimal weight recommendations
- [ ] Strategy documentation

#### 4.3 Create Screening Presets
**New file:** `src/finviz_weekly/presets.py`

**Preset strategies:**
- [ ] **Insider Momentum:** Focus on stocks with strong insider buying
- [ ] **Earnings Surprise:** Focus on stocks with consistent positive earnings surprises
- [ ] **Quality Growth:** Focus on high-margin, high-ROE stocks with revenue growth
- [ ] **Contrarian Value:** Focus on oversold quality stocks with insider buying

**Usage:**
```bash
python -m finviz_weekly screen --preset insider-momentum --top 30
```

---

### 📊 Phase 5: Visualization & Reporting (FUTURE - 1-2 months)

**Priority: MEDIUM** | **Effort: Medium**

#### 5.1 Create Data Dashboards
**New directory:** `dashboards/`

**Tools:** Streamlit, Plotly, or Dash

**Features:**
- [ ] Interactive stock screener with new data sources
- [ ] Insider trading activity heatmap
- [ ] Earnings surprise trends
- [ ] Financial health distribution charts
- [ ] Portfolio construction tool

#### 5.2 Enhanced Reports
**File:** `src/finviz_weekly/report.py`

**Enhancements:**
- [ ] Add insider trading summary section
- [ ] Add earnings quality metrics
- [ ] Add financial health scorecard
- [ ] Generate PDF reports with charts

#### 5.3 Email/Slack Alerts
**New file:** `src/finviz_weekly/alerts.py`

**Alert triggers:**
- [ ] Significant insider buying (>$1M net)
- [ ] Unexpected earnings surprises (>5% alpha)
- [ ] Stocks meeting screening criteria
- [ ] Daily top picks

---

### 🔧 Phase 6: Performance & Reliability (ONGOING)

**Priority: MEDIUM** | **Effort: Low-Medium**

#### 6.1 Performance Optimization
- [ ] Profile scraping performance
- [ ] Implement parallel scraping for new data sources
- [ ] Add Redis caching layer for scraped data
- [ ] Optimize database queries (if using DB)

#### 6.2 Error Handling & Resilience
- [ ] Better handling of network timeouts
- [ ] Retry logic with exponential backoff
- [ ] Circuit breaker pattern for flaky data sources
- [ ] Graceful degradation (continue if one scraper fails)

#### 6.3 Monitoring & Observability
- [ ] Add structured logging with context
- [ ] Create metrics dashboard (scraping time, error rates)
- [ ] Set up alerting for scraper failures
- [ ] Track data quality metrics over time

---

### 🌟 Phase 7: Advanced Features (FUTURE - 3-6 months)

**Priority: LOW-MEDIUM** | **Effort: High**

#### 7.1 Real-Time Monitoring
- [ ] Track insider trades in near real-time
- [ ] Monitor earnings announcements
- [ ] Price movement alerts
- [ ] News sentiment tracking

#### 7.2 Machine Learning Models
- [ ] Train models to predict stock movements
- [ ] Feature engineering from all data sources
- [ ] Model serving infrastructure
- [ ] Backtesting ML strategies

#### 7.3 REST API Service
**New directory:** `api/`

**Features:**
- [ ] FastAPI service for scrapers
- [ ] Authentication & rate limiting
- [ ] API documentation with Swagger
- [ ] Deployment on cloud platform

#### 7.4 Database Integration
- [ ] PostgreSQL for structured data
- [ ] TimescaleDB for time-series data
- [ ] Data retention policies
- [ ] Query optimization

#### 7.5 Portfolio Management
- [ ] Portfolio construction algorithms
- [ ] Risk management tools
- [ ] Position sizing recommendations
- [ ] Rebalancing strategies

---

## Success Metrics

### Phase 3 (Production Testing)
- ✅ Successfully scrape 100+ tickers with new data sources
- ✅ <5% error rate in scraping
- ✅ Data quality score >90%
- ✅ Documentation covers all edge cases

### Phase 4 (Enhanced Scoring)
- ✅ Backtest shows improvement over baseline (higher Sharpe ratio)
- ✅ Enhanced strategy outperforms S&P 500 in backtest
- ✅ User-friendly screening presets created
- ✅ Comprehensive strategy documentation

### Phase 5 (Visualization)
- ✅ Dashboard deployed and accessible
- ✅ Daily reports generated automatically
- ✅ Alert system functioning reliably
- ✅ User feedback incorporated

### Phase 6 (Performance)
- ✅ Scraping time reduced by 30%
- ✅ Error rate <1%
- ✅ Monitoring dashboard operational
- ✅ 99% uptime for scheduled jobs

### Phase 7 (Advanced Features)
- ✅ ML model beats benchmark in backtest
- ✅ API deployed with documentation
- ✅ Database migration complete
- ✅ Real-time monitoring functional

---

## Recommended Immediate Actions (This Week)

1. **Test scrapers with real data** (2-3 hours)
   ```bash
   python -m finviz_weekly insider --tickers AAPL,MSFT,GOOGL,TSLA,NVDA
   python -m finviz_weekly earnings --tickers AAPL,MSFT,GOOGL,TSLA,NVDA
   python -m finviz_weekly financials --tickers AAPL,MSFT,GOOGL,TSLA,NVDA
   ```

2. **Review output quality** (1-2 hours)
   - Check for parsing errors
   - Validate data accuracy against Finviz website
   - Document any issues

3. **Plan integration into main pipeline** (1 hour)
   - Decide on CLI flags vs. config file
   - Design data merge strategy
   - Plan checkpoint format updates

4. **Create integration branch** (30 minutes)
   ```bash
   git checkout -b feature/integrate-new-scrapers
   ```

5. **Update project documentation** (30 minutes)
   - Add roadmap to README.md
   - Update CONTRIBUTING.md with new features
   - Document integration plans

---

## Resources & Dependencies

### Phase 3-4 Dependencies:
- Historical data (at least 6 months for backtesting)
- Access to Finviz (no IP blocks)
- Compute resources for backtesting

### Phase 5 Dependencies:
- Visualization library (Streamlit/Plotly)
- Email/Slack API credentials
- Hosting for dashboard (optional)

### Phase 6 Dependencies:
- Profiling tools (cProfile, py-spy)
- Redis for caching (optional)
- Monitoring platform (Prometheus/Grafana)

### Phase 7 Dependencies:
- ML frameworks (scikit-learn, XGBoost)
- Cloud platform (AWS/GCP/Azure)
- Database (PostgreSQL/TimescaleDB)
- API hosting infrastructure

---

## Questions to Consider

1. **Strategy:** Which scoring approach performs best in backtests?
2. **Integration:** Should new scrapers be always-on or opt-in?
3. **Performance:** What's the acceptable scraping time per ticker?
4. **Quality:** What data quality threshold requires alerts?
5. **Deployment:** Cloud-hosted or self-hosted?
6. **Monetization:** Is this for personal use or service offering?

---

## Getting Started with Phase 3

See `QUICKSTART.md` for usage examples and `examples/integrate_new_scrapers.py` for integration patterns.

**Ready to begin?** Start with testing the scrapers on your watchlist!
