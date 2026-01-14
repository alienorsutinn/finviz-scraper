# Quick Action Checklist - Start Here! 🎯

Based on the comprehensive audit in `docs/AUDIT_AND_ROADMAP.md`, here are the **immediate actions** you can take today.

---

## ⚡ 5-Minute Wins (Do Right Now)

### 1. Add Async Timeout
**File:** `src/finviz_weekly/pipeline.py:138`

**Change:**
```python
# BEFORE:
asyncio.run(scrape_all(tasks, sem, http_config))

# AFTER:
try:
    asyncio.run(asyncio.wait_for(
        scrape_all(tasks, sem, http_config),
        timeout=3600  # 1 hour max
    ))
except asyncio.TimeoutError:
    LOGGER.error("Scraping timed out after 1 hour")
    raise
```

---

### 2. Fix Hardcoded Database Password
**File:** `docker-compose.yml:97`

**Change:**
```yaml
# BEFORE:
environment:
  POSTGRES_PASSWORD: finviz_password

# AFTER:
environment:
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-change_me_in_production}

# Then create .env file:
# POSTGRES_PASSWORD=your_secure_random_password_here
```

---

## 🔥 30-Minute Fixes (High Impact)

### 3. Add API Key Validation
**File:** `src/finviz_weekly/debate/llm/openai_client.py:25`

**Add this method:**
```python
def __init__(self):
    """Initialize OpenAI client with validation."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError(
            "❌ OPENAI_API_KEY environment variable required.\n"
            "Get one at: https://platform.openai.com/api-keys\n"
            "Then run: export OPENAI_API_KEY='sk-...'"
        )
    self._client = openai.OpenAI(api_key=api_key)
    LOGGER.info("✅ OpenAI client initialized successfully")
```

---

### 4. Replace Bare Exceptions in pipeline.py
**File:** `src/finviz_weekly/pipeline.py:74-75`

**Change:**
```python
# BEFORE:
except Exception as e:
    LOGGER.warning("All-screener universe failed...")

# AFTER:
except (requests.exceptions.RequestException, TimeoutError, ValueError) as e:
    LOGGER.error(f"All-screener universe failed: {e}", exc_info=True)
    # Fall back to empty universe
```

---

### 5. Add Input Validation to screen.py
**File:** `src/finviz_weekly/screen.py` - Add at start of `do_screen()`

**Add:**
```python
def do_screen(
    scored_path: Path,
    *,
    out_dir: str,
    top_n: int = 80,
    # ... other params
) -> Dict[str, pd.DataFrame]:
    """Screen stocks with validation."""

    # Validate inputs
    if top_n < 1 or top_n > 500:
        raise ValueError(f"top_n must be in [1, 500], got {top_n}")

    if not scored_path.exists():
        raise FileNotFoundError(f"Scored data not found: {scored_path}")

    # Load and validate schema
    df = pd.read_parquet(scored_path)
    required_cols = ["ticker", "company", "sector"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    LOGGER.info(f"✅ Loaded {len(df)} stocks with valid schema")

    # ... rest of function
```

---

## 🛠️ 1-Hour Tasks (Quality Improvements)

### 6. Mask API Keys in Logs
**Create new file:** `src/finviz_weekly/utils.py`

```python
"""Utility functions for the package."""
import re
import logging

def sanitize_url(url: str) -> str:
    """Mask API keys in URLs for safe logging."""
    patterns = [
        (r'(api_key=)[^&\s]+', r'\1***MASKED***'),
        (r'(key=)[^&\s]+', r'\1***MASKED***'),
        (r'(token=)[^&\s]+', r'\1***MASKED***'),
    ]

    sanitized = url
    for pattern, replacement in patterns:
        sanitized = re.sub(pattern, replacement, sanitized)

    return sanitized


class SanitizingFormatter(logging.Formatter):
    """Formatter that sanitizes sensitive data in logs."""

    def format(self, record: logging.LogRecord) -> str:
        # Sanitize message
        if isinstance(record.msg, str):
            record.msg = sanitize_url(record.msg)

        # Sanitize args
        if record.args:
            record.args = tuple(
                sanitize_url(str(arg)) if isinstance(arg, str) else arg
                for arg in record.args
            )

        return super().format(record)
```

**Then update logging setup:**
```python
# In src/finviz_weekly/__init__.py or main entry point
from finviz_weekly.utils import SanitizingFormatter

handler = logging.StreamHandler()
handler.setFormatter(SanitizingFormatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
))
logging.root.addHandler(handler)
```

---

### 7. Add Configuration Validation
**File:** `src/finviz_weekly/config.py`

**Add to each dataclass:**
```python
@dataclass
class HttpConfig:
    rate_per_sec: float = 0.5
    connect_timeout_secs: int = 5
    read_timeout_secs: int = 20
    max_retries: int = 3

    def __post_init__(self):
        """Validate configuration values."""
        if self.rate_per_sec <= 0 or self.rate_per_sec > 10:
            raise ValueError(
                f"rate_per_sec must be in (0, 10], got {self.rate_per_sec}. "
                "Higher values may trigger rate limiting."
            )

        if self.connect_timeout_secs < 1:
            raise ValueError(f"connect_timeout_secs must be >= 1, got {self.connect_timeout_secs}")

        if self.read_timeout_secs < 1:
            raise ValueError(f"read_timeout_secs must be >= 1, got {self.read_timeout_secs}")

        if self.max_retries < 0 or self.max_retries > 10:
            raise ValueError(f"max_retries must be in [0, 10], got {self.max_retries}")

        LOGGER.debug(f"✅ HttpConfig validated: rate={self.rate_per_sec}/sec, retries={self.max_retries}")


@dataclass
class RunConfig:
    mode: str
    ticker_limit: Optional[int] = None
    out_dir: str = "data"

    def __post_init__(self):
        """Validate run configuration."""
        valid_modes = ["tickers", "universe", "all-screener"]
        if self.mode not in valid_modes:
            raise ValueError(f"mode must be one of {valid_modes}, got '{self.mode}'")

        if self.ticker_limit is not None and self.ticker_limit < 1:
            raise ValueError(f"ticker_limit must be >= 1, got {self.ticker_limit}")

        # Ensure output directory is writable
        out_path = Path(self.out_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        if not os.access(out_path, os.W_OK):
            raise PermissionError(f"Output directory not writable: {out_path}")

        LOGGER.debug(f"✅ RunConfig validated: mode={self.mode}, out_dir={self.out_dir}")
```

---

### 8. Complete Missing TODO in optimize.py
**File:** `src/finviz_weekly/optimize.py:129`

**Replace:**
```python
# BEFORE:
best_return=0.0,  # TODO: Calculate actual return

# AFTER:
best_return=self._calculate_total_return(returns_array),
```

**Add method:**
```python
def _calculate_total_return(self, returns: np.ndarray) -> float:
    """
    Calculate cumulative return from period returns.

    Args:
        returns: Array of period returns (e.g., [0.02, -0.01, 0.03])

    Returns:
        Total cumulative return (e.g., 0.041 for 4.1%)
    """
    if len(returns) == 0:
        return 0.0

    # Compound returns: (1+r1)*(1+r2)*...*(1+rn) - 1
    cumulative = np.prod(1 + returns) - 1

    return float(cumulative)
```

---

## 📊 Testing Improvements (2 Hours)

### 9. Add VCR Fixtures for HTTP Tests

**Install:**
```bash
pip install vcrpy pytest-vcr
```

**Create test:**
```python
# tests/test_earnings_http.py
import pytest
import vcr

@pytest.fixture
def vcr_config():
    """VCR configuration."""
    return {
        "record_mode": "once",  # Record on first run, replay after
        "match_on": ["method", "scheme", "host", "port", "path"],
        "filter_headers": ["authorization", "api-key"],
        "cassette_library_dir": "tests/fixtures/vcr_cassettes",
    }


@vcr.use_cassette("tests/fixtures/vcr_cassettes/earnings_AAPL.yaml")
def test_scrape_earnings_reactions_real_html():
    """Test earnings scraper with real HTML from Finviz."""
    from finviz_weekly.scrapers.earnings import scrape_earnings_reactions
    from finviz_weekly.http import get_http_session
    from finviz_weekly.config import HttpConfig

    session = get_http_session(HttpConfig())
    http_config = HttpConfig()

    # This will record on first run, replay from cassette after
    results = scrape_earnings_reactions("AAPL", session, http_config)

    assert isinstance(results, list)
    if results:  # If data exists
        assert "quarter" in results[0]
        assert "avg_alpha" in results[0]
```

**Record real responses:**
```bash
# First run records HTTP responses
pytest tests/test_earnings_http.py --record-mode=once

# Subsequent runs use recordings (no network)
pytest tests/test_earnings_http.py

# To re-record (when Finviz changes):
pytest tests/test_earnings_http.py --record-mode=rewrite
```

---

### 10. Add Integration Test

**Create:**
```python
# tests/test_integration_full_pipeline.py
import subprocess
import pytest
from pathlib import Path


def test_full_scrape_to_screen_pipeline(tmp_path):
    """
    Integration test: scrape → screen → verify outputs.

    Tests the complete workflow end-to-end.
    """
    # 1. Run scraper with 3 test tickers
    scrape_result = subprocess.run(
        [
            "python", "-m", "finviz_weekly", "run",
            "--mode", "tickers",
            "--tickers", "AAPL,MSFT,GOOGL",
            "--out", str(tmp_path),
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert scrape_result.returncode == 0, f"Scrape failed: {scrape_result.stderr}"

    # Verify outputs
    raw_file = tmp_path / "finviz_raw.parquet"
    assert raw_file.exists(), "Raw data not created"

    # 2. Run screening
    screen_result = subprocess.run(
        [
            "python", "-m", "finviz_weekly", "screen",
            "--out", str(tmp_path),
            "--top", "10",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert screen_result.returncode == 0, f"Screen failed: {screen_result.stderr}"

    # Verify screening outputs
    scored_file = tmp_path / "finviz_scored.parquet"
    assert scored_file.exists(), "Scored data not created"

    report_file = tmp_path / "finviz_report.md"
    assert report_file.exists(), "Report not created"

    # 3. Verify data schema
    import pandas as pd
    scored_df = pd.read_parquet(scored_file)

    assert len(scored_df) == 3, "Should have 3 tickers"
    assert "ticker" in scored_df.columns
    assert "score_master" in scored_df.columns

    print("✅ Full pipeline integration test passed!")
```

**Run:**
```bash
pytest tests/test_integration_full_pipeline.py -v
```

---

## 📝 Documentation Quick Wins (30 Minutes)

### 11. Add Architecture Diagram

**Create:** `docs/architecture.txt`

```
┌─────────────────────────────────────────────────────────────────┐
│                         FINVIZ-SCRAPER                          │
│                    Investment Research Platform                  │
└─────────────────────────────────────────────────────────────────┘

┌───────────────────┐
│   CLI Interface   │  ← User interaction point
│   (cli.py)        │
└────────┬──────────┘
         │
         ├─→ run      (Scrape data)
         ├─→ screen   (Score & filter stocks)
         ├─→ debate   (AI research)
         └─→ backtest (Validate strategies)

┌────────▼─────────────────────────────────────────────────────────┐
│              PIPELINE (Orchestration Layer)                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │Universe  │→ │ Scraper  │→ │ Storage  │→ │ Resume   │       │
│  │Builder   │  │ (async)  │  │(Parquet) │  │ Logic    │       │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │
└──────────────────────────────────────────────────────────────────┘
         │
         ├─→ HTTP Layer (retry, rate limit, user agents)
         ├─→ Checkpoint (crash-safe writes)
         └─→ Error Handling (graceful failures)

┌────────▼─────────────────────────────────────────────────────────┐
│                    DATA SCRAPERS                                 │
│  ┌─────────────┬─────────────┬─────────────┬─────────────┐    │
│  │Fundamentals │  Earnings   │   Insider   │ Financials  │    │
│  │(parse.py)   │ (earnings.py)│ (insider.py)│(financials.py)│  │
│  │             │             │             │             │    │
│  │• Quality    │• Reactions  │• Trading    │• Margins    │    │
│  │• Value      │• Surprises  │• Net Value  │• ROE/ROA    │    │
│  │• Growth     │• Alpha      │• Sentiment  │• Liquidity  │    │
│  └─────────────┴─────────────┴─────────────┴─────────────┘    │
└──────────────────────────────────────────────────────────────────┘

┌────────▼─────────────────────────────────────────────────────────┐
│                    SCREENING ENGINE                              │
│  ┌──────────────┬──────────────┬──────────────┬─────────────┐  │
│  │   Scoring    │ Normalization│  Filtering   │  Ranking    │  │
│  │  (score.py)  │  (groups)    │  (zones)     │  (themes)   │  │
│  │              │              │              │             │  │
│  │• Quality     │• By sector   │• Aggressive  │• Quality/   │  │
│  │• Value       │• By size     │• Add         │  Value      │  │
│  │• Growth      │• Percentile  │• Starter     │• Compounders│  │
│  │• Momentum    │  ranking     │• Watch       │• GARP       │  │
│  │• Enhanced    │              │• Avoid       │• Custom     │  │
│  └──────────────┴──────────────┴──────────────┴─────────────┘  │
└──────────────────────────────────────────────────────────────────┘

┌────────▼─────────────────────────────────────────────────────────┐
│                  RESEARCH & ANALYSIS                             │
│  ┌───────────┬──────────────┬──────────────┬───────────────┐   │
│  │ Backtester│   Portfolio  │   Research   │  Regime       │   │
│  │           │   Builder    │   Tools      │  Detection    │   │
│  │• Returns  │• Sector div  │• Correlation │• Bull/Bear    │   │
│  │• Sharpe   │• Risk mgmt   │• Decay       │• VIX + SPY    │   │
│  │• Drawdown │• Rebalancing │• Attribution │• MA analysis  │   │
│  └───────────┴──────────────┴──────────────┴───────────────┘   │
└──────────────────────────────────────────────────────────────────┘

┌────────▼─────────────────────────────────────────────────────────┐
│                    OUTPUT LAYER                                  │
│  ┌──────────┬──────────┬──────────┬──────────┬──────────┐       │
│  │ Parquet  │   CSV    │ Markdown │Dashboard │   API    │       │
│  │  Files   │  Files   │  Report  │(Streamlit)│(FastAPI)│       │
│  │          │          │          │          │          │       │
│  │• History │• Top 50  │• Summary │• Filters │• REST    │       │
│  │• Latest  │• Lists   │• Insights│• Charts  │• Docs    │       │
│  └──────────┴──────────┴──────────┴──────────┴──────────┘       │
└──────────────────────────────────────────────────────────────────┘

┌────────▼─────────────────────────────────────────────────────────┐
│                  DEPLOYMENT OPTIONS                              │
│  ┌──────────────────┬──────────────────┬──────────────────┐     │
│  │  Local/CLI       │   Docker Compose │   GitHub Actions │     │
│  │  python -m ...   │  5 services      │  Automated       │     │
│  │                  │  postgres+api    │  Weekly runs     │     │
│  └──────────────────┴──────────────────┴──────────────────┘     │
└──────────────────────────────────────────────────────────────────┘

LEGEND:
→ Data flow
┌─┐ Component/Module
├─┤ Sub-component
```

---

## 🎯 Priority Order

**Do these in order for maximum impact:**

1. ⚡ **5-minute wins** (#1-2) → Immediate security improvements
2. 🔥 **30-minute fixes** (#3-5) → Critical quality improvements
3. 🛠️ **1-hour tasks** (#6-8) → Robustness enhancements
4. 📊 **Testing** (#9-10) → Confidence building
5. 📝 **Documentation** (#11) → Knowledge sharing

**Total Time:** ~6 hours to complete all quick wins

---

## ✅ Verification Checklist

After completing fixes, verify:

```bash
# 1. All tests pass
pytest tests/ -v

# 2. No linter errors
pylint src/finviz_weekly/ --fail-under=8.0

# 3. Type checking passes
mypy src/finviz_weekly/

# 4. Security scan clean
bandit -r src/finviz_weekly/ -ll

# 5. Can run full pipeline
python -m finviz_weekly run --mode tickers --tickers AAPL,MSFT --out /tmp/test
python -m finviz_weekly screen --out /tmp/test

# 6. Docker builds successfully
docker-compose build

# 7. Integration test passes
pytest tests/test_integration_full_pipeline.py -v
```

---

## 📈 Tracking Progress

Create a tracking board:

```markdown
## Quick Wins Progress

- [ ] #1: Add async timeout (5 min)
- [ ] #2: Fix hardcoded DB password (5 min)
- [ ] #3: Add API key validation (30 min)
- [ ] #4: Replace bare exceptions (30 min)
- [ ] #5: Add input validation (30 min)
- [ ] #6: Mask API keys in logs (1 hr)
- [ ] #7: Add config validation (1 hr)
- [ ] #8: Complete TODOs (1 hr)
- [ ] #9: Add VCR fixtures (2 hrs)
- [ ] #10: Add integration test (2 hrs)
- [ ] #11: Add architecture diagram (30 min)

**Progress:** 0/11 ✅
```

---

## 🚀 After Quick Wins

See full roadmap in `docs/AUDIT_AND_ROADMAP.md` for:
- Phase 2: Quality & Testing (25 hours)
- Phase 3: Optimization (15 hours)
- Phase 4: Advanced Features (40+ hours)

**Questions?** Open an issue or see the full audit report!
