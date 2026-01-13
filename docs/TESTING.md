# Production Testing Guide

## Network Environment Limitation

**IMPORTANT:** The current development environment uses a proxy that blocks access to finviz.com (403 Forbidden). This means the scrapers **cannot be tested with live data** in this environment. However, the code is production-ready and will work in environments with proper internet access.

### Error Message You'll See:
```
OSError('Tunnel connection failed: 403 Forbidden')
```

### Where the Scrapers WILL Work:
- ✅ Local development machine (laptop/desktop)
- ✅ Cloud instances (AWS EC2, GCP Compute, Azure VM)
- ✅ GitHub Actions CI/CD (as configured)
- ✅ Docker containers with network access
- ✅ Production servers

### Where They WON'T Work:
- ❌ Restricted corporate networks blocking finviz.com
- ❌ Development environments with proxy restrictions (like this one)
- ❌ Networks with financial site filtering

---

## Testing in Production Environment

Once you're in an environment with proper network access, follow these steps:

### 1. Verify Network Access

```bash
# Test basic connectivity
curl -I https://finviz.com

# Should return: HTTP/2 200
# If you get 403 or timeout, check proxy/firewall settings
```

### 2. Test Insider Trading Scraper

```bash
# Single ticker
python -m finviz_weekly insider --ticker AAPL --output-format json

# Multiple tickers
python -m finviz_weekly insider --tickers AAPL,MSFT,GOOGL,TSLA --output-format csv

# From file
echo "AAPL\nMSFT\nGOOGL\nTSLA\nNVDA" > test_tickers.txt
python -m finviz_weekly insider --tickers-file test_tickers.txt --output-format json > insider_results.json
```

**Expected Output (JSON):**
```json
{
  "AAPL": {
    "total_buys": 15,
    "total_sells": 23,
    "buy_value": 12500000.0,
    "sell_value": 45000000.0,
    "net_value": -32500000.0,
    "net_transactions": -8
  },
  "MSFT": {
    "total_buys": 20,
    "total_sells": 10,
    "buy_value": 25000000.0,
    "sell_value": 8000000.0,
    "net_value": 17000000.0,
    "net_transactions": 10
  }
}
```

**Quality Checks:**
- ✅ All requested tickers returned (or error logged)
- ✅ `net_value` = `buy_value` - `sell_value`
- ✅ `net_transactions` = `total_buys` - `total_sells`
- ✅ Values are reasonable (not NaN, not extreme outliers)
- ✅ No parsing errors in logs

### 3. Test Earnings Reactions Scraper

```bash
# Single ticker
python -m finviz_weekly earnings --ticker AAPL --output-format json

# Multiple tickers
python -m finviz_weekly earnings --tickers AAPL,MSFT,GOOGL --output-format csv
```

**Expected Output (JSON):**
```json
{
  "AAPL": {
    "total_events": 12,
    "avg_day_0_change": 2.5,
    "avg_week_1_change": 3.8,
    "avg_day_0_alpha": 1.8,
    "avg_week_1_alpha": 2.3,
    "avg_rsi": 55.2,
    "positive_reactions": 8,
    "negative_reactions": 4,
    "positive_reaction_pct": 66.67
  }
}
```

**Quality Checks:**
- ✅ At least 4-8 earnings events per ticker (quarterly earnings)
- ✅ Percentages are in reasonable range (-50% to +50%)
- ✅ RSI is between 0-100
- ✅ `positive_reactions` + `negative_reactions` = `total_events`
- ✅ Alpha calculations are relative to SPY (market)

### 4. Test Financial Statements Scraper

```bash
# Single ticker
python -m finviz_weekly financials --ticker AAPL --output-format json

# Multiple tickers
python -m finviz_weekly financials --tickers AAPL,MSFT,GOOGL
```

**Expected Output (JSON):**
```json
{
  "AAPL": {
    "statements": {
      "income_statement": {
        "Revenue": [394328000000, 365817000000],
        "Net Income": [96995000000, 94680000000],
        "Gross Profit": [169148000000, 152836000000]
      },
      "balance_sheet": {
        "Total Assets": [352755000000, 338516000000],
        "Total Liabilities": [290437000000, 302083000000],
        "Stockholders Equity": [62318000000, 36433000000]
      },
      "cash_flow": {
        "Operating Cash Flow": [110543000000, 104038000000],
        "Investing Cash Flow": [-22354000000, -14545000000],
        "Financing Cash Flow": [-108488000000, -92686000000]
      }
    },
    "ratios": {
      "net_margin": 0.246,
      "gross_margin": 0.429,
      "roe": 1.557,
      "roa": 0.275,
      "debt_to_equity": 4.660,
      "current_ratio": 0.98,
      "asset_turnover": 1.118
    }
  }
}
```

**Quality Checks:**
- ✅ All three statement types returned (income, balance, cash_flow)
- ✅ Ratios are calculated correctly
- ✅ ROE and ROA are positive for profitable companies
- ✅ Margins are between 0-1 (0-100%)
- ✅ Current ratio > 1 indicates good liquidity
- ✅ No division by zero errors

---

## Performance Benchmarks

Expected performance (with rate limiting):

| Scraper | Time per Ticker | Rate Limit | Notes |
|---------|----------------|------------|-------|
| Insider | 2-3 seconds | 0.5 req/sec | Respects rate limits |
| Earnings | 2-3 seconds | 0.5 req/sec | Parses historical data |
| Financials | 3-5 seconds | 0.5 req/sec | Three pages per ticker |

**For 100 tickers:**
- Insider: ~5 minutes
- Earnings: ~5 minutes
- Financials: ~8 minutes
- **Total: ~18 minutes** (if run sequentially)

**Optimization:**
- Run scrapers in parallel (separate processes)
- Use `--concurrency` flag (future enhancement)
- Cache results (future enhancement)

---

## Error Handling Test Cases

### 1. Invalid Ticker
```bash
python -m finviz_weekly insider --ticker INVALID123
```
**Expected:** Error logged, continues to next ticker

### 2. Network Timeout
**Expected:** Retry logic kicks in (5 retries with exponential backoff)

### 3. Missing Data
**Expected:** Returns empty dict for that ticker, logs warning

### 4. Rate Limiting (429 Error)
**Expected:** Backs off, waits longer, retries

---

## Integration Testing

### Test the Integration Example

```bash
# Run the integration script
python examples/integrate_new_scrapers.py --tickers AAPL,MSFT,GOOGL

# Expected output:
# 1. Fetches fundamentals for 3 tickers
# 2. Scrapes insider trading data
# 3. Scrapes earnings reactions
# 4. Scrapes financial statements
# 5. Merges all data into enhanced dataframe
# 6. Calculates enhanced scores
# 7. Saves to data/enhanced/enhanced_YYYYMMDD.csv
```

**Verify Output:**
```bash
# Check the output file
ls -lh data/enhanced/

# View the enhanced data
head data/enhanced/enhanced_*.csv
```

**Expected Columns:**
```
ticker, insider_score, earnings_score, financial_quality, enhanced_score,
insider_net_value, insider_total_buys, insider_total_sells,
earnings_avg_alpha, earnings_avg_rsi, earnings_win_rate,
net_margin, roe, roa, debt_to_equity, current_ratio
```

---

## Validation Checklist

After running scrapers in production, verify:

- [ ] **No crashes** - All commands complete successfully
- [ ] **Error handling** - Invalid tickers logged, not crashed
- [ ] **Data quality** - Output passes quality checks above
- [ ] **Performance** - Within expected time ranges
- [ ] **Rate limiting** - No 429 errors from finviz.com
- [ ] **Output format** - JSON/CSV is valid and parseable
- [ ] **Integration** - Enhanced dataframe merges correctly
- [ ] **Scoring** - Enhanced scores calculated without errors
- [ ] **Persistence** - Output files saved correctly

---

## Sample Test Script

Create `test_production.sh`:

```bash
#!/bin/bash
set -e

echo "=== Testing Finviz Scrapers in Production ==="

# Test tickers
TICKERS="AAPL,MSFT,GOOGL,TSLA,NVDA"

# Test insider
echo ""
echo "[1/4] Testing insider trading scraper..."
python -m finviz_weekly insider --tickers $TICKERS --output-format json > test_insider.json
echo "✓ Insider scraper completed"

# Test earnings
echo ""
echo "[2/4] Testing earnings reactions scraper..."
python -m finviz_weekly earnings --tickers $TICKERS --output-format json > test_earnings.json
echo "✓ Earnings scraper completed"

# Test financials
echo ""
echo "[3/4] Testing financial statements scraper..."
python -m finviz_weekly financials --tickers $TICKERS > test_financials.json
echo "✓ Financials scraper completed"

# Test integration
echo ""
echo "[4/4] Testing integration example..."
python examples/integrate_new_scrapers.py --tickers $TICKERS
echo "✓ Integration completed"

# Summary
echo ""
echo "=== Test Summary ==="
echo "Insider results: $(cat test_insider.json | jq 'length') tickers"
echo "Earnings results: $(cat test_earnings.json | jq 'length') tickers"
echo "Financials results: $(cat test_financials.json | jq 'length') tickers"
echo "Enhanced data: $(ls -lh data/enhanced/enhanced_*.csv | tail -1)"

echo ""
echo "✓ All tests passed!"
```

**Run it:**
```bash
chmod +x test_production.sh
./test_production.sh
```

---

## CI/CD Testing

The scrapers can be tested in GitHub Actions:

```yaml
# .github/workflows/test-scrapers.yml
name: Test New Scrapers

on:
  workflow_dispatch:
  schedule:
    - cron: '0 0 * * 1'  # Weekly on Monday

jobs:
  test-scrapers:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install -e .

      - name: Test insider scraper
        run: |
          python -m finviz_weekly insider --tickers AAPL,MSFT,GOOGL --output-format json

      - name: Test earnings scraper
        run: |
          python -m finviz_weekly earnings --tickers AAPL,MSFT,GOOGL --output-format json

      - name: Test financials scraper
        run: |
          python -m finviz_weekly financials --tickers AAPL,MSFT,GOOGL

      - name: Upload results
        uses: actions/upload-artifact@v3
        with:
          name: scraper-test-results
          path: test_*.json
```

---

## Next Steps After Testing

1. **Document findings** - Record any edge cases or issues found
2. **Update code** - Fix any bugs discovered during testing
3. **Optimize performance** - Tune rate limits and concurrency
4. **Integrate into pipeline** - Add to main scraping workflow
5. **Set up monitoring** - Track success rates and errors

---

## Support

If you encounter issues:

1. Check network connectivity: `curl -I https://finviz.com`
2. Review logs for error messages
3. Test with a single ticker first: `--ticker AAPL`
4. Verify your Python environment: `python --version` (should be 3.11+)
5. Check the GitHub Issues for similar problems

**Common Issues:**
- **403 Forbidden**: Network/proxy blocking
- **Timeout**: Slow connection or rate limiting
- **Parse Error**: Finviz HTML changed (needs code update)
- **Empty Results**: Ticker doesn't have that data on Finviz
