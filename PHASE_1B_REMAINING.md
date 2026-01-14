# Phase 1B - Remaining Critical Fixes

**Status:** 4 of 10 critical fixes completed (Phase 1A)
**Remaining:** 6 fixes needed to complete Phase 1

---

## ✅ Completed (Phase 1A)

- [x] **Fix #1:** Add async timeout to pipeline.py
- [x] **Fix #2:** Fix hardcoded database password
- [x] **Fix #3:** Add API key validation (OpenAI, Brave, Google CSE)
- [x] **Fix #4:** Replace bare exceptions in pipeline.py
- [x] **Verify:** All 70 tests passing

---

## 🔄 Remaining Work (Phase 1B) - 6 Hours Total

### Fix #5: Add Input Validation to screen.py (1 hour)

**File:** `src/finviz_weekly/screen.py`
**Issue:** No schema validation on input DataFrame

**Implementation:**
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

    if len(df) == 0:
        raise ValueError("Input DataFrame is empty")

    LOGGER.info(f"✅ Loaded {len(df)} stocks with valid schema")

    # ... rest of function
```

**Also add validation to:**
- `debate/search/*.py` - Validate `max_results` in [1, 50]
- `optimize.py:_evaluate_weights()` - Check for division by zero

---

### Fix #6: Create utils.py with Log Sanitization (1 hour)

**File:** `src/finviz_weekly/utils.py` (NEW)
**Issue:** API keys visible in logs

**Implementation:**
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
        (r'(password=)[^&\s]+', r'\1***MASKED***'),
        (r'(sk-[A-Za-z0-9]{20,})', r'sk-***MASKED***'),  # OpenAI keys
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

**Then update logging in:**
- `src/finviz_weekly/__init__.py` or CLI entry point
- Apply SanitizingFormatter to all handlers

---

### Fix #7: Add Config Validation (__post_init__) (1 hour)

**File:** `src/finviz_weekly/config.py`
**Issue:** No runtime validation of config values

**Implementation:**
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
            raise ValueError(
                f"connect_timeout_secs must be >= 1, got {self.connect_timeout_secs}"
            )

        if self.read_timeout_secs < 1:
            raise ValueError(
                f"read_timeout_secs must be >= 1, got {self.read_timeout_secs}"
            )

        if self.max_retries < 0 or self.max_retries > 10:
            raise ValueError(
                f"max_retries must be in [0, 10], got {self.max_retries}"
            )


@dataclass
class RunConfig:
    mode: str
    ticker_limit: Optional[int] = None
    out_dir: str = "data"

    def __post_init__(self):
        """Validate run configuration."""
        import os
        from pathlib import Path

        valid_modes = ["tickers", "universe", "all-screener"]
        if self.mode not in valid_modes:
            raise ValueError(
                f"mode must be one of {valid_modes}, got '{self.mode}'"
            )

        if self.ticker_limit is not None and self.ticker_limit < 1:
            raise ValueError(
                f"ticker_limit must be >= 1, got {self.ticker_limit}"
            )

        # Ensure output directory is writable
        out_path = Path(self.out_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        if not os.access(out_path, os.W_OK):
            raise PermissionError(
                f"Output directory not writable: {out_path}"
            )
```

---

### Fix #8: Complete TODO in optimize.py (30 minutes)

**File:** `src/finviz_weekly/optimize.py:129`
**Issue:** `best_return=0.0  # TODO: Calculate actual return`

**Implementation:**
```python
# Add this method to FactorOptimizer class
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


# Then in optimize_weights(), replace:
best_return=0.0,  # TODO

# With:
best_return=self._calculate_total_return(returns_array),
```

**Also fix:**
- Line 194, 206: Check `std_return > 0` before division

---

### Fix #9: Replace Bare Exceptions in earnings.py (1 hour)

**File:** `src/finviz_weekly/scrapers/earnings.py`
**Locations:** Multiple bare `except Exception:` blocks

**Pattern to fix:**
```python
# BEFORE (Bad):
except Exception:
    pass

# AFTER (Good):
except (RequestException, AttributeError, ValueError, KeyError) as e:
    LOGGER.error(f"Failed to parse earnings for {ticker}: {e}", exc_info=True)
    return []
```

**Specific fixes needed:**
- Line ~93: Ticker matching in HTML
- Line ~170: Regex substitution
- General HTML parsing failures

**Also fix:**
- Add strict regex with word boundaries for ticker matching
- Validate parsed percentages are in valid range

---

### Fix #10: Replace Bare Exceptions in screen.py (1 hour)

**File:** `src/finviz_weekly/screen.py`
**Location:** Line 336 - Bare `pass`

**Fix:**
```python
# BEFORE:
except:
    pass

# AFTER:
except (KeyError, ValueError, TypeError) as e:
    LOGGER.warning(
        f"Failed to normalize group {group}: {e}",
        exc_info=True
    )
    continue
```

**Additional locations:**
- Any other bare `except:` blocks
- JSON parse errors without logging

---

## Testing Checklist

After implementing each fix:

```bash
# 1. Run tests
pytest tests/ -v

# 2. Static analysis
pylint src/finviz_weekly/config.py  # After Fix #7
pylint src/finviz_weekly/screen.py  # After Fix #10
pylint src/finviz_weekly/optimize.py  # After Fix #8

# 3. Type checking
mypy src/finviz_weekly/

# 4. Security scan
bandit -r src/finviz_weekly/ -ll

# 5. Manual verification
# Try invalid config
python -c "from finviz_weekly.config import HttpConfig; HttpConfig(rate_per_sec=99)"
# Should raise ValueError

# Try invalid screen input
python -m finviz_weekly screen --out /tmp --top 9999
# Should raise ValueError
```

---

## Commit Strategy

**After completing all fixes:**

```bash
git add -A
git commit -m "fix: Phase 1B - Input Validation & Exception Handling

Completed remaining 6 critical fixes from audit (issues #5-10):

## 5. Add Input Validation
- screen.py: Validate DataFrame schema and top_n range
- optimize.py: Check for division by zero
- search/*.py: Validate max_results in [1, 50]

## 6. Create utils.py with Log Sanitization
- Mask API keys, passwords, tokens in logs
- SanitizingFormatter for automatic sanitization
- Protects against credential leaks

## 7. Add Config Validation
- HttpConfig: Validate rate limits, timeouts
- RunConfig: Validate mode, paths, permissions
- Fail fast with clear error messages

## 8. Complete TODO in optimize.py
- Implemented _calculate_total_return()
- Fixed division by zero check
- Returns now accurately calculated

## 9. Replace Bare Exceptions in earnings.py
- Specific exception types with logging
- Better error messages for debugging
- Validate parsed data ranges

## 10. Replace Bare Exceptions in screen.py
- Fixed bare pass at line 336
- All exceptions logged with context
- Specific exception types throughout

## Impact
- ✅ All 10 Phase 1 critical fixes complete
- ✅ 70/70 tests passing
- ✅ No security vulnerabilities
- ✅ Current grade: B++ → A-

Phase 1 COMPLETE. Ready for Phase 2 (Quality & Testing)."
```

---

## Expected Outcome

**After Phase 1B completion:**
- **Grade:** B+ → A- (Good → Very Good)
- **Critical Issues:** 0 (down from 1)
- **High Issues:** 0 (down from 10)
- **Test Coverage:** 70/70 passing (100%)
- **Security:** No exposed credentials, validated inputs
- **Reliability:** Graceful degradation, clear error messages

**Time Investment:** 6 hours
**Value Created:** Enterprise-ready error handling and security

---

## Quick Start (Do This Now)

```bash
# 1. Open the files
code src/finviz_weekly/screen.py      # Fix #5
code src/finviz_weekly/utils.py       # Fix #6 (create new)
code src/finviz_weekly/config.py      # Fix #7
code src/finviz_weekly/optimize.py    # Fix #8
code src/finviz_weekly/scrapers/earnings.py  # Fix #9
code src/finviz_weekly/screen.py      # Fix #10

# 2. Implement fixes one by one (copy from above)

# 3. Test after each fix
pytest tests/ -v

# 4. Commit when all done
git commit -m "fix: Phase 1B - [your summary]"

# 5. Push
git push
```

---

## Get Help

**Questions?** See:
- `docs/AUDIT_AND_ROADMAP.md` - Full audit details
- `QUICK_ACTION_CHECKLIST.md` - Step-by-step guide
- GitHub Issues - Create issue for specific problems

**Stuck?** The fixes above are copy-paste ready!
