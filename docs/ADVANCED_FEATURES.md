# Advanced Features Guide

This guide covers all advanced features added in Phases 6-8, transforming finviz-scraper into a professional-grade investment research platform.

## Table of Contents

- [Real-time Alerts](#real-time-alerts)
- [Portfolio Construction](#portfolio-construction)
- [Research Tools](#research-tools)
- [Market Regime Detection](#market-regime-detection)
- [ML Factor Optimization](#ml-factor-optimization)
- [REST API](#rest-api)
- [Docker Deployment](#docker-deployment)

---

## Real-time Alerts

Get notified when high-conviction opportunities arise via Email, Slack, or Discord.

### Configuration

Edit `config/alerts_config.yaml`:

```yaml
enabled: true

# Score thresholds
master_score_threshold: 80.0

# Enhanced data triggers
insider_buying_min: 1000000.0  # $1M+ net buying
earnings_alpha_min: 0.05        # 5%+ avg alpha
net_margin_min: 0.20            # 20%+ net margin

# Email notifications
email_enabled: true
email_to: your.email@example.com
smtp_host: smtp.gmail.com
smtp_port: 587
smtp_username: your.email@gmail.com
smtp_password: your-app-password

# Slack notifications
slack_enabled: true
slack_webhook_url: https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### Running Alerts

```bash
# Run alert detection on latest results
python -m finviz_weekly.alerts --data-dir data/latest

# With custom config
python -m finviz_weekly.alerts --config config/alerts_config.yaml
```

### Alert Types

1. **High Master Score**: Stocks scoring above threshold (default: 80)
2. **Insider Buying**: Net insider buying above $1M
3. **Earnings Quality**: Consistent earnings beaters (5%+ alpha, 75%+ win rate)
4. **Financial Health**: High profitability + strong balance sheet
5. **High Conviction**: Appears in 3+ theme families

### Deduplication

Alerts are deduplicated to avoid spam. By default, won't re-alert on the same ticker within 7 days.

---

## Portfolio Construction

Build diversified portfolios with sector balancing and risk management.

### Quick Start

```bash
# Build 20-stock portfolio
python -m finviz_weekly.portfolio \
  --data-dir data/latest \
  --score score_master \
  --num-positions 20 \
  --max-sector 0.30
```

### Python API

```python
from finviz_weekly.portfolio import PortfolioBuilder, PortfolioConfig
import pandas as pd

# Load screening results
scored_df = pd.read_parquet("data/latest/finviz_scored.parquet")

# Configure portfolio builder
config = PortfolioConfig(
    num_positions=20,
    min_position_size=0.02,  # 2% minimum
    max_position_size=0.10,  # 10% maximum
    max_sector_weight=0.30,  # 30% max per sector
    min_sectors=3,           # At least 3 sectors
    min_market_cap=1e9,      # $1B minimum
)

# Build portfolio
builder = PortfolioBuilder(config)
portfolio = builder.build_portfolio(scored_df, score_column="score_master")

# View results
from finviz_weekly.portfolio import print_portfolio_summary
print_portfolio_summary(portfolio)
```

### Features

- **Sector Diversification**: Automatically balance across sectors
- **Position Sizing**: Constrain individual position sizes
- **Risk Management**: Minimum market cap and liquidity filters
- **Optimization**: Optional Sharpe ratio maximization (requires returns data)

### Rebalancing

```python
# Generate rebalance recommendations
sells, buys, new_positions = builder.generate_rebalance_recommendations(
    current_portfolio=current_portfolio,
    new_screening_results=latest_results,
    score_column="score_master"
)

print(f"Sell: {sells}")
print(f"Buy: {buys}")
```

---

## Research Tools

Validate and improve investment strategies with factor analysis.

### Factor Correlation Analysis

Identify redundant factors:

```bash
python -m finviz_weekly.research \
  --history data/history/finviz_scored_history.parquet \
  --analysis correlation
```

```python
from finviz_weekly.research import ResearchToolkit

toolkit = ResearchToolkit()
analysis = toolkit.analyze_factor_correlations(
    history_df,
    factor_columns=["score_quality", "score_value", "score_growth"],
    threshold=0.70  # Flag correlations above 70%
)

# View results
from finviz_weekly.research import print_correlation_analysis
print_correlation_analysis(analysis)
```

**Output:**
- Correlation matrix between all factors
- List of highly correlated pairs
- Potentially redundant factors

### Factor Decay Analysis

Measure how long signals last:

```bash
python -m finviz_weekly.research \
  --history data/history/finviz_scored_history.parquet \
  --analysis decay \
  --factor score_master
```

```python
decay_analysis = toolkit.analyze_factor_decay(
    history_df,
    factor_column="score_master",
    periods=[1, 3, 5, 7, 14, 21, 30, 60, 90]
)

print_decay_analysis(decay_analysis)
```

**Output:**
- Correlation with forward returns at different periods
- Optimal holding period (highest correlation)
- Signal half-life (when correlation drops to 50%)

### Rolling Backtest Performance

Track strategy performance over time:

```python
rolling_results = toolkit.rolling_backtest_performance(
    history_df,
    factor_column="score_master",
    window_days=90,
    step_days=30,
    top_n=20
)

print(rolling_results)
```

---

## Market Regime Detection

Identify bull/bear/sideways markets using SPY and VIX.

### Quick Start

```bash
# Detect current market regime
python -m finviz_weekly.regime
```

### Python API

```python
from finviz_weekly.regime import RegimeDetector, print_regime_analysis

detector = RegimeDetector()

# Auto-fetch data from yfinance
analysis = detector.get_regime_from_yfinance()

print_regime_analysis(analysis)
```

### Regimes

- **BULL**: Uptrending market (price > MA20 > MA50 > MA200)
- **BEAR**: Downtrending market (price < MA20 < MA50 < MA200)
- **SIDEWAYS**: Range-bound market
- **VOLATILE**: High VIX, uncertain direction

### Custom Parameters

```python
detector = RegimeDetector(
    ma_short=20,             # 20-day MA
    ma_medium=50,            # 50-day MA
    ma_long=200,             # 200-day MA
    vix_low_threshold=15.0,  # VIX < 15 = low vol
    vix_high_threshold=25.0  # VIX > 25 = high vol
)
```

### Strategy Implications

**Bull Market**:
- Favor growth and momentum strategies
- Increase equity exposure
- Look for breakout opportunities

**Bear Market**:
- Focus on quality and defensive stocks
- Reduce exposure or hedge
- Preserve capital

**Sideways**:
- Range-trading strategies
- Focus on stock-picking
- Sector rotation

**Volatile**:
- Reduce position sizes
- High-conviction only
- Consider hedging

---

## ML Factor Optimization

Automatically optimize factor weights using Bayesian optimization.

### Quick Start

```bash
python -m finviz_weekly.optimize \
  --history data/history/finviz_scored_history.parquet \
  --start-date 2024-01-01 \
  --end-date 2025-01-01 \
  --factors score_quality score_value score_growth score_momentum \
  --iterations 50 \
  --output config/optimal_weights.json
```

### Python API

```python
from finviz_weekly.optimize import FactorOptimizer, print_optimization_result
from pathlib import Path

optimizer = FactorOptimizer(
    history_path=Path("data/history/finviz_scored_history.parquet"),
    start_date="2024-01-01",
    end_date="2025-01-01",
    objective="sharpe"  # or "return" or "risk_adjusted"
)

# Optimize factor weights
result = optimizer.optimize_weights(
    factors=["score_quality", "score_value", "score_growth", "score_momentum"],
    n_iterations=50,
    n_initial_points=10
)

print_optimization_result(result)

# Save optimal weights
optimizer.save_optimal_weights(result, Path("config/optimal_weights.json"))
```

### How It Works

1. Tests different weight combinations (Bayesian optimization)
2. Backtests each combination over the specified period
3. Optimizes for Sharpe ratio (or other objective)
4. Returns optimal weights and improvement vs equal weights

### Requirements

- Requires `scikit-optimize`: `pip install scikit-optimize`
- Need sufficient historical data (3-6+ months)
- Falls back to grid search if scikit-optimize not available

---

## REST API

Programmatic access to screening results, portfolios, and backtests.

### Starting the API

```bash
# Standalone
uvicorn api.main:app --host 0.0.0.0 --port 8000

# With Docker
docker-compose up api
```

API will be available at `http://localhost:8000`

Interactive docs: `http://localhost:8000/docs`

### Endpoints

#### Get Screening Results

```bash
GET /api/v1/screen?limit=50&sort_by=score_master&min_score=70

# Response:
[
  {
    "ticker": "AAPL",
    "company": "Apple Inc.",
    "sector": "Technology",
    "price": 185.50,
    "score_master": 85.2,
    ...
  },
  ...
]
```

#### List Investment Themes

```bash
GET /api/v1/themes

# Response:
[
  {"name": "quality_value", "url": "/api/v1/themes/quality_value"},
  {"name": "compounders", "url": "/api/v1/themes/compounders"},
  ...
]
```

#### Get Stock Details

```bash
GET /api/v1/stocks/AAPL

# Response:
{
  "ticker": "AAPL",
  "company": "Apple Inc.",
  "sector": "Technology",
  "score_master": 85.2,
  "score_quality": 90.1,
  ... all columns ...
}
```

#### Build Portfolio

```bash
GET /api/v1/portfolio?num_positions=20&score_column=score_master&max_sector_weight=0.30

# Response:
{
  "positions": [...],
  "num_positions": 20,
  "num_sectors": 5,
  "sector_weights": {"Technology": 0.30, ...}
}
```

#### Run Backtest

```bash
POST /api/v1/backtest
Content-Type: application/json

{
  "score_column": "score_master",
  "start_date": "2024-01-01",
  "end_date": "2025-01-01",
  "top_n": 20,
  "rebalance_days": 7
}

# Response:
{
  "total_return": 0.25,
  "annual_return": 0.28,
  "sharpe_ratio": 1.45,
  "max_drawdown": -0.15,
  "num_trades": 45
}
```

### Python Client Example

```python
import requests

# Get top 20 stocks
response = requests.get("http://localhost:8000/api/v1/screen?limit=20&sort_by=score_master")
stocks = response.json()

for stock in stocks:
    print(f"{stock['ticker']}: {stock['score_master']:.1f}")

# Build portfolio
response = requests.get("http://localhost:8000/api/v1/portfolio?num_positions=20")
portfolio = response.json()

print(f"Portfolio: {portfolio['num_positions']} positions across {portfolio['num_sectors']} sectors")
```

---

## Docker Deployment

Complete Docker setup for production deployment.

### Services

```yaml
services:
  scraper:      # Run screening pipeline
  dashboard:    # Streamlit dashboard (port 8501)
  api:          # FastAPI backend (port 8000)
  postgres:     # PostgreSQL database (port 5432)
  test:         # Run tests
```

### Quick Start

```bash
# Start all services
docker-compose up -d dashboard api postgres

# View dashboard
open http://localhost:8501

# View API docs
open http://localhost:8000/docs

# Run scraper
docker-compose run scraper

# Run tests
docker-compose run test
```

### Building Images

```bash
# Build all images
docker-compose build

# Build specific service
docker-compose build dashboard

# Build with no cache
docker-compose build --no-cache
```

### Data Persistence

Data is persisted in volumes:
- `./data` - Screening results and history
- `postgres_data` - Database data

### Environment Variables

Create `.env` file:

```env
# Database
DATABASE_URL=postgresql://finviz_user:finviz_password@postgres:5432/finviz

# Scraping
RATE_LIMIT_PER_SEC=0.5

# Optional: API authentication
API_SECRET_KEY=your-secret-key-here
```

### Production Deployment

1. **Set up environment**:
   ```bash
   cp .env.example .env
   # Edit .env with production values
   ```

2. **Start services**:
   ```bash
   docker-compose -f docker-compose.yml up -d
   ```

3. **Set up scheduled scraping** (cron):
   ```bash
   # Add to crontab
   0 2 * * 0 cd /path/to/finviz-scraper && docker-compose run scraper
   ```

4. **Configure reverse proxy** (Nginx):
   ```nginx
   server {
       listen 80;
       server_name api.example.com;

       location / {
           proxy_pass http://localhost:8000;
       }
   }

   server {
       listen 80;
       server_name dashboard.example.com;

       location / {
           proxy_pass http://localhost:8501;
       }
   }
   ```

---

## Integration Examples

### Complete Workflow

```python
# 1. Detect market regime
from finviz_weekly.regime import RegimeDetector
detector = RegimeDetector()
regime = detector.get_regime_from_yfinance()

# 2. Load screening results
import pandas as pd
scored_df = pd.read_parquet("data/latest/finviz_scored.parquet")

# 3. Build portfolio based on regime
from finviz_weekly.portfolio import PortfolioBuilder, PortfolioConfig

if regime.current_regime.value == "bull":
    # Aggressive in bull market
    config = PortfolioConfig(num_positions=30, max_sector_weight=0.35)
    score_column = "score_growth"  # Favor growth
elif regime.current_regime.value == "bear":
    # Defensive in bear market
    config = PortfolioConfig(num_positions=15, max_sector_weight=0.25)
    score_column = "score_quality"  # Favor quality
else:
    # Balanced otherwise
    config = PortfolioConfig(num_positions=20, max_sector_weight=0.30)
    score_column = "score_master"

builder = PortfolioBuilder(config)
portfolio = builder.build_portfolio(scored_df, score_column)

# 4. Set up alerts for portfolio changes
from finviz_weekly.alerts import AlertEngine, load_alert_config
alert_config = load_alert_config()
engine = AlertEngine(alert_config)
alerts = engine.detect_alerts(scored_df)
if alerts:
    engine.send_alerts(alerts)

print(f"Built {len(portfolio.positions)}-stock portfolio for {regime.current_regime.value} market")
```

---

## Requirements

### Core Dependencies

Install with groups:

```bash
# Visualization
pip install -e .[viz]

# API
pip install -e .[api]

# Database
pip install -e .[db]

# ML optimization
pip install -e .[ml]

# Everything
pip install -e .[all]
```

### System Requirements

- Python 3.11+
- Docker (optional, for containerized deployment)
- PostgreSQL 15+ (optional, for database backend)

---

## Troubleshooting

### Alerts Not Sending

1. Check email configuration in `config/alerts_config.yaml`
2. For Gmail, use App Password (not regular password)
3. Test SMTP connection: `telnet smtp.gmail.com 587`

### Portfolio Builder Issues

- Ensure sufficient stocks pass filters (market cap, liquidity)
- Check for NaN values in score columns
- Verify sector column exists

### API Not Starting

- Check port 8000 is not in use: `lsof -i :8000`
- Verify data exists at `/app/data/latest`
- Check logs: `docker-compose logs api`

### Optimization Fails

- Requires 3-6+ months of historical data
- Install scikit-optimize: `pip install scikit-optimize`
- Reduce n_iterations if running out of memory

---

## Next Steps

1. **Set up automated data collection** (GitHub Actions already configured)
2. **Configure alerts** for your preferred channels
3. **Build and track portfolios** using the construction tools
4. **Optimize factor weights** with your historical data
5. **Deploy API** for programmatic access
6. **Monitor market regime** to adapt strategies

For more examples, see `examples/` directory.
