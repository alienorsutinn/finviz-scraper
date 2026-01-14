# Machine Learning Improvement Plan

## Executive Summary

This document outlines a comprehensive plan to enhance the machine learning and modeling capabilities of the finviz-scraper project. The current system has solid foundations (70% maturity) with factor-based scoring and Bayesian optimization, but lacks modern ML models, proper validation, and production-ready risk management.

**Current State:** Factor-based quantitative screening with Information Coefficient learning
**Target State:** Production-ready ML platform with gradient boosting, proper validation, and risk controls

---

## Table of Contents

1. [Current ML Capabilities](#current-ml-capabilities)
2. [Identified Gaps](#identified-gaps)
3. [Improvement Roadmap](#improvement-roadmap)
4. [Implementation Phases](#implementation-phases)
5. [Success Metrics](#success-metrics)
6. [Technical Requirements](#technical-requirements)

---

## Current ML Capabilities

### ✅ What's Working Well

**1. Multi-Factor Scoring Engine** (`screen.py` - 85% mature)
- 6 core factors: Quality, Value, Risk, Growth, Momentum, Oversold
- 7 composite strategies
- Sector/industry-based normalization
- Percentile ranking system

**2. Factor Weight Optimization**
- **learn.py**: Spearman IC-based weight learning (80% mature)
- **optimize.py**: Bayesian optimization with Gaussian Process (70% mature)
- Supports global and sector-specific weights

**3. Backtesting Framework** (`backtest.py` - 60% mature)
- Event-driven portfolio simulation
- Configurable rebalancing
- Sharpe ratio, drawdown, win rate calculation

**4. Enhanced Features** (Phase 4)
- Insider trading scoring
- Earnings reaction analysis
- Financial health metrics

**5. Market Regime Detection** (`regime.py` - 75% mature)
- Moving average crossover signals
- VIX-based volatility classification

### Maturity Assessment

```
Overall ML Platform: 70% mature

Suitable for:
✅ Educational use & research
✅ Factor-based screening
✅ Historical strategy analysis

Not yet suitable for:
❌ Automated trading
❌ Production ML systems
❌ Real-time decision making
```

---

## Identified Gaps

### 🔴 Critical Gaps

**1. No Out-of-Sample Validation**
- Current: IC learned on full dataset (potential look-ahead bias)
- Risk: Overfitted weights, poor forward performance
- Impact: Strategy may fail in live trading

**2. No ML Models Beyond Factor Weighting**
- Current: Only linear factor combinations
- Missing: XGBoost, LightGBM, neural networks
- Impact: Limited predictive power

**3. Simplistic Backtest Assumptions**
- No transaction costs/slippage
- No liquidity constraints
- No dividend handling
- Impact: Unrealistically optimistic returns

### 🟡 High-Priority Gaps

**4. Limited Feature Engineering**
- Missing: Technical indicators (MACD, Bollinger Bands, ATR)
- Missing: Market microstructure features
- Missing: Sentiment indicators

**5. No Feature Importance Analysis**
- Hard to understand which factors drive returns
- No SHAP values or permutation importance
- Difficult to debug model performance

**6. No Portfolio Optimization**
- Equal-weight portfolio only
- No minimum variance optimization
- No correlation matrix usage
- Missing: CVaR, expected shortfall

### 🟢 Medium-Priority Gaps

**7. No Production Monitoring**
- No model drift detection
- No performance degradation alerts
- No automated retraining

**8. Regime Not Integrated**
- Regime detection exists but unused in portfolio
- No regime-based allocation
- No defensive positioning in bear markets

**9. LLM Not Integrated**
- Debate module separate from quant scores
- No blending optimization
- No qualitative + quantitative fusion

---

## Improvement Roadmap

### Phase 5A: Validation & Robustness (3-4 weeks)

**Goal:** Eliminate look-ahead bias and properly validate models

#### Tasks

**1. Walk-Forward Validation Framework**
```python
# New module: src/finviz_weekly/validation.py

class WalkForwardValidator:
    """
    Time-series cross-validation with expanding window.

    Example:
        Train: 2020-01-01 to 2021-12-31 (2 years)
        Test: 2022-01-01 to 2022-03-31 (3 months)

        Train: 2020-01-01 to 2022-03-31 (2.25 years)
        Test: 2022-04-01 to 2022-06-30 (3 months)
        ...
    """

    def __init__(self,
                 train_period_months: int = 24,
                 test_period_months: int = 3,
                 step_months: int = 3):
        pass

    def split(self, data: pd.DataFrame) -> Iterator[Tuple[pd.DataFrame, pd.DataFrame]]:
        """Generate train/test splits."""
        pass

    def validate_strategy(self,
                         strategy_fn: Callable,
                         data: pd.DataFrame) -> pd.DataFrame:
        """
        Run strategy through all folds.

        Returns:
            pd.DataFrame with columns:
                - fold_id
                - train_start, train_end
                - test_start, test_end
                - oos_sharpe, oos_return
                - oos_ic (factor weights)
                - degradation_pct
        """
        pass
```

**Implementation:**
- 2-year training window, 3-month test window
- Rolling forward 3 months at a time
- Track OOS Sharpe, IC, and returns
- Alert if OOS Sharpe < 50% of in-sample

**2. Baseline Comparison Framework**
```python
# Compare learned weights vs. baselines

BASELINES = {
    "equal_weight": Weights all factors equally,
    "no_momentum": Remove momentum factor,
    "value_only": Only value factor,
    "spx_benchmark": Buy & hold S&P 500
}

# Generate comparison report:
# - OOS Sharpe for each baseline
# - Statistical significance tests
# - Which baseline is hardest to beat
```

**3. Overfitting Detection**
```python
def detect_overfitting(is_sharpe: float, oos_sharpe: float) -> Dict:
    """
    Alert on significant OOS degradation.

    Returns:
        {
            "is_sharpe": 2.5,
            "oos_sharpe": 0.8,
            "degradation_pct": -68%,
            "alert": "CRITICAL" if >50% drop
        }
    """
```

**Deliverables:**
- `src/finviz_weekly/validation.py` (300+ lines)
- `tests/test_validation.py` (50+ tests)
- Validation report generator
- OOS performance tracking dashboard

---

### Phase 5B: Feature Importance & Explainability (2-3 weeks)

**Goal:** Understand which factors actually drive returns

#### Tasks

**1. Permutation Importance**
```python
# New module: src/finviz_weekly/explainability.py

def calculate_permutation_importance(
    data: pd.DataFrame,
    factors: List[str],
    target: str = "forward_return_21d",
    n_repeats: int = 10
) -> pd.DataFrame:
    """
    Shuffle each factor and measure performance drop.

    Returns:
        pd.DataFrame:
            factor | importance | importance_std
            quality | 0.45 | 0.03
            value | 0.32 | 0.04
            ...
    """
```

**2. Partial Dependence Plots**
```python
def partial_dependence_plot(
    data: pd.DataFrame,
    feature: str,
    target: str,
    ax=None
) -> plt.Figure:
    """
    Show relationship between single feature and returns.

    Example: Does higher ROE really predict returns?
    """
```

**3. Factor Contribution Analysis**
```python
def decompose_score(
    ticker: str,
    weights: Dict[str, float],
    scores: pd.DataFrame
) -> pd.DataFrame:
    """
    Break down total score into factor contributions.

    Returns:
        factor | raw_score | weight | contribution
        quality | 85 | 0.30 | 25.5
        value | 60 | 0.25 | 15.0
        ...
        TOTAL | - | 1.00 | 78.2
    """
```

**Deliverables:**
- `src/finviz_weekly/explainability.py` (250+ lines)
- `tests/test_explainability.py` (30+ tests)
- Factor importance report generator
- Visualization utilities

---

### Phase 6A: Enhanced Backtesting (3-4 weeks)

**Goal:** Realistic simulation with transaction costs

#### Tasks

**1. Transaction Cost Model**
```python
# Update backtest.py

@dataclass
class TransactionCosts:
    """Model realistic trading costs."""
    commission_pct: float = 0.001  # 0.1% per trade (e.g., $10 per $10k)
    spread_pct: float = 0.01  # 1% bid-ask spread
    market_impact_pct: float = 0.02  # 2% slippage on entry/exit
    min_commission: float = 1.0  # $1 minimum

    def calculate_cost(self, trade_value: float, volume: float) -> float:
        """Calculate total cost for a trade."""
        commission = max(trade_value * self.commission_pct, self.min_commission)
        spread = trade_value * self.spread_pct
        impact = trade_value * self.market_impact_pct * (1 - min(volume / 1e9, 1.0))
        return commission + spread + impact

# Update BacktestConfig to include costs
```

**2. Liquidity Filtering**
```python
def filter_illiquid_stocks(
    data: pd.DataFrame,
    min_dollar_volume: float = 1e6  # $1M daily
) -> pd.DataFrame:
    """
    Remove stocks with insufficient liquidity.

    Prevents backtest from selecting stocks
    that couldn't be traded in practice.
    """
```

**3. Dividend Handling**
```python
def adjust_for_dividends(
    returns: pd.Series,
    dividends: pd.Series
) -> pd.Series:
    """
    Add dividend yield to total returns.

    Total Return = Price Return + Dividend Yield
    """
```

**4. Enhanced Metrics**
```python
class BacktestResults:
    # Existing metrics
    total_return: float
    sharpe_ratio: float
    max_drawdown: float

    # NEW metrics
    sortino_ratio: float  # Downside deviation only
    calmar_ratio: float  # Return / Max Drawdown
    cvar_95: float  # Conditional Value at Risk (95th percentile)
    hit_rate: float  # % of profitable trades
    profit_factor: float  # Gross profit / Gross loss
    avg_trade_duration: int  # Days per position
    turnover: float  # % portfolio traded per period
```

**Deliverables:**
- Enhanced `backtest.py` with costs (100+ new lines)
- `tests/test_backtest_realistic.py` (40+ tests)
- Cost sensitivity analysis tool
- Turnover optimization guidance

---

### Phase 6B: Portfolio Optimization (3-4 weeks)

**Goal:** Modern portfolio theory implementation

#### Tasks

**1. Minimum Variance Portfolio**
```python
# New: src/finviz_weekly/portfolio_optimizer.py

def optimize_minimum_variance(
    returns: pd.DataFrame,  # Historical returns
    constraints: Dict
) -> Dict[str, float]:
    """
    Find portfolio weights that minimize variance.

    Uses scipy.optimize.minimize with:
    - Objective: Minimize portfolio variance
    - Constraints: Sum weights = 1, weights >= 0
    - Bounds: Min/max per stock

    Returns:
        {ticker: weight} for each stock
    """
    from scipy.optimize import minimize

    cov_matrix = returns.cov()

    def objective(weights):
        return weights @ cov_matrix @ weights

    constraints = [
        {"type": "eq", "fun": lambda w: np.sum(w) - 1.0},  # Sum = 1
    ]

    bounds = [(0.0, 0.10)] * len(returns.columns)  # 0-10% per stock

    result = minimize(objective, x0=init_weights,
                     constraints=constraints, bounds=bounds)
    return dict(zip(returns.columns, result.x))
```

**2. Mean-Variance Optimization**
```python
def optimize_mean_variance(
    expected_returns: pd.Series,
    cov_matrix: pd.DataFrame,
    risk_aversion: float = 1.0
) -> Dict[str, float]:
    """
    Maximize: Expected Return - (risk_aversion * Variance)

    risk_aversion:
        0.5 = Aggressive (prioritize returns)
        1.0 = Balanced
        2.0 = Conservative (minimize risk)
    """
```

**3. Risk Parity Portfolio**
```python
def optimize_risk_parity(
    returns: pd.DataFrame
) -> Dict[str, float]:
    """
    Equal risk contribution from each asset.

    Each stock contributes equally to total portfolio risk,
    not equal weights (which ignores correlations).
    """
```

**4. Black-Litterman Model**
```python
def black_litterman_optimization(
    market_caps: pd.Series,  # Market equilibrium
    views: Dict[str, float],  # Analyst views (e.g., "AAPL will return 15%")
    confidence: float = 0.5
) -> Dict[str, float]:
    """
    Blend market equilibrium with investor views.

    Useful for incorporating qualitative insights
    (e.g., from LLM research) into portfolio.
    """
```

**Deliverables:**
- `src/finviz_weekly/portfolio_optimizer.py` (400+ lines)
- `tests/test_portfolio_optimization.py` (50+ tests)
- Portfolio comparison tool (equal-weight vs. optimized)
- Risk decomposition visualization

---

### Phase 7: ML Model Training (4-6 weeks)

**Goal:** Modern ML models for return prediction

#### Tasks

**1. Feature Engineering**
```python
# New: src/finviz_weekly/feature_engineering.py

class FeatureEngineer:
    """Generate ML-ready features."""

    def create_technical_indicators(self, prices: pd.DataFrame) -> pd.DataFrame:
        """
        Add technical indicators:
        - RSI (14, 28 day)
        - MACD (12, 26, 9)
        - Bollinger Bands (20 day, 2 std)
        - ATR (14 day)
        - Volume ratios (current / 20-day avg)
        """

    def create_cross_sectional_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Rank-based features:
        - Percentile ranks of all factors
        - Z-scores within sector
        - Distance from sector median
        """

    def create_interaction_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Feature interactions:
        - Quality * Value (GARP)
        - Momentum * RSI (overbought momentum)
        - PE Ratio * EPS Growth (PEG ratio)
        """

    def create_lag_features(self, data: pd.DataFrame, lags: List[int]) -> pd.DataFrame:
        """
        Time-lagged features:
        - Scores from t-1, t-2, t-3 periods
        - Score trends (increasing/decreasing)
        - Score volatility
        """
```

**2. XGBoost Model Training**
```python
# New: src/finviz_weekly/models/xgboost_model.py

import xgboost as xgb
from sklearn.model_selection import TimeSeriesSplit

class XGBoostReturnPredictor:
    """
    Predict 21-day forward returns using XGBoost.
    """

    def __init__(self, params: Dict = None):
        self.params = params or {
            "objective": "reg:squarederror",
            "max_depth": 6,
            "learning_rate": 0.01,
            "n_estimators": 1000,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "reg_alpha": 0.1,  # L1 regularization
            "reg_lambda": 1.0,  # L2 regularization
        }
        self.model = None
        self.feature_importance = None

    def train(self,
              X_train: pd.DataFrame,
              y_train: pd.Series,
              X_val: pd.DataFrame = None,
              y_val: pd.Series = None):
        """
        Train XGBoost model with early stopping.

        Uses validation set to prevent overfitting.
        """
        dtrain = xgb.DMatrix(X_train, label=y_train)

        if X_val is not None:
            dval = xgb.DMatrix(X_val, label=y_val)
            evallist = [(dtrain, "train"), (dval, "val")]
            self.model = xgb.train(
                self.params,
                dtrain,
                num_boost_round=1000,
                evals=evallist,
                early_stopping_rounds=50,
                verbose_eval=100
            )
        else:
            self.model = xgb.train(self.params, dtrain, num_boost_round=500)

        self.feature_importance = self.model.get_score(importance_type="gain")

    def predict(self, X: pd.DataFrame) -> pd.Series:
        """Predict forward returns."""
        dtest = xgb.DMatrix(X)
        return pd.Series(self.model.predict(dtest), index=X.index)

    def get_feature_importance(self) -> pd.DataFrame:
        """Return feature importance sorted by gain."""
        return pd.DataFrame({
            "feature": self.feature_importance.keys(),
            "importance": self.feature_importance.values()
        }).sort_values("importance", ascending=False)
```

**3. Hyperparameter Optimization**
```python
# Use Optuna for hyperparameter tuning

import optuna

def optimize_xgboost_params(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    n_trials: int = 100
) -> Dict:
    """
    Find optimal XGBoost hyperparameters.

    Optimizes over:
    - max_depth: 3-10
    - learning_rate: 0.001-0.3 (log scale)
    - subsample: 0.5-1.0
    - colsample_bytree: 0.5-1.0
    - reg_alpha: 0-10
    - reg_lambda: 0-10

    Objective: Maximize validation Sharpe ratio
    """

    def objective(trial):
        params = {
            "max_depth": trial.suggest_int("max_depth", 3, 10),
            "learning_rate": trial.suggest_float("learning_rate", 0.001, 0.3, log=True),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
            "reg_alpha": trial.suggest_float("reg_alpha", 0, 10),
            "reg_lambda": trial.suggest_float("reg_lambda", 0, 10),
        }

        # Train model with walk-forward validation
        model = XGBoostReturnPredictor(params)
        scores = walk_forward_validate(model, X_train, y_train)

        return scores["oos_sharpe"].mean()

    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=n_trials)

    return study.best_params
```

**4. Model Ensemble**
```python
class EnsemblePredictor:
    """
    Combine multiple models for robustness.

    Models:
    - XGBoost (gradient boosting)
    - Factor weights (linear model)
    - Random Forest (ensemble trees)

    Ensemble method: Weighted average or stacking
    """

    def __init__(self, models: List, weights: List[float] = None):
        self.models = models
        self.weights = weights or [1.0 / len(models)] * len(models)

    def predict(self, X: pd.DataFrame) -> pd.Series:
        """Average predictions from all models."""
        predictions = [model.predict(X) for model in self.models]
        return np.average(predictions, axis=0, weights=self.weights)
```

**Deliverables:**
- `src/finviz_weekly/feature_engineering.py` (300+ lines)
- `src/finviz_weekly/models/xgboost_model.py` (250+ lines)
- `src/finviz_weekly/models/ensemble.py` (150+ lines)
- `tests/test_ml_models.py` (60+ tests)
- Model training pipeline
- Hyperparameter tuning script
- Model comparison dashboard

---

### Phase 8: Production Deployment (3-4 weeks)

**Goal:** Production-ready ML system

#### Tasks

**1. Model Versioning & Storage**
```python
# New: src/finviz_weekly/model_registry.py

class ModelRegistry:
    """
    Track trained models with metadata.

    Storage structure:
        models/
            xgboost_v1_20240115.pkl
            xgboost_v2_20240215.pkl
            metadata.json
    """

    def save_model(self,
                   model: Any,
                   name: str,
                   metrics: Dict,
                   features: List[str]):
        """
        Save model with metadata.

        Metadata includes:
        - Training date
        - Feature list
        - OOS Sharpe, IC
        - Hyperparameters
        - Training data period
        """

    def load_latest_model(self, name: str) -> Any:
        """Load most recent version of model."""

    def compare_models(self, name_pattern: str) -> pd.DataFrame:
        """
        Compare all models matching pattern.

        Returns:
            model_name | train_date | oos_sharpe | features_count
        """
```

**2. Model Drift Detection**
```python
def detect_model_drift(
    model: Any,
    recent_data: pd.DataFrame,
    baseline_ic: float,
    alert_threshold: float = 0.20
) -> Dict:
    """
    Alert if model performance degrades.

    Checks:
    - IC drop > 20% from baseline
    - Sharpe drop > 30%
    - Feature distribution shifts (KS test)

    Returns:
        {
            "alert": True/False,
            "current_ic": 0.04,
            "baseline_ic": 0.08,
            "degradation_pct": -50%,
            "features_shifted": ["momentum", "rsi"]
        }
    """
```

**3. Automated Retraining**
```python
class RetrainingScheduler:
    """
    Automatically retrain models on schedule.

    Triggers:
    - Monthly: Scheduled retraining
    - Performance: IC drops >20%
    - Data: New data covers >10% of training set
    """

    def should_retrain(self,
                       last_train_date: date,
                       current_performance: Dict) -> bool:
        """Check if retraining is needed."""

    def retrain_and_deploy(self, model_name: str):
        """
        Retrain model and deploy if better than current.

        Steps:
        1. Pull latest data
        2. Train new model
        3. Validate on OOS data
        4. Deploy if OOS Sharpe > current model
        5. Archive old model
        6. Send alert email
        """
```

**4. Model API**
```python
# Update api/main.py

from fastapi import FastAPI
from .models.xgboost_model import XGBoostReturnPredictor

app = FastAPI()

@app.post("/predict")
def predict_returns(tickers: List[str]) -> Dict[str, float]:
    """
    Predict 21-day forward returns for tickers.

    Request:
        POST /predict
        {"tickers": ["AAPL", "MSFT", "GOOGL"]}

    Response:
        {
            "AAPL": 0.08,  # 8% expected return
            "MSFT": 0.05,
            "GOOGL": 0.12
        }
    """
    model = load_latest_model("xgboost")
    features = fetch_features(tickers)
    predictions = model.predict(features)
    return predictions.to_dict()

@app.get("/model/status")
def model_status() -> Dict:
    """
    Get current model metadata.

    Returns:
        {
            "model_name": "xgboost_v5",
            "train_date": "2024-01-15",
            "oos_sharpe": 1.8,
            "oos_ic": 0.06,
            "features_count": 47
        }
    """
```

**Deliverables:**
- `src/finviz_weekly/model_registry.py` (200+ lines)
- `src/finviz_weekly/drift_detection.py` (150+ lines)
- `src/finviz_weekly/retraining.py` (200+ lines)
- Updated `api/main.py` with model endpoints
- Monitoring dashboard
- Automated retraining script
- Alert notification system

---

## Success Metrics

### Phase 5A Validation
- ✅ OOS Sharpe > 0.5 (acceptable)
- ✅ OOS Sharpe > 1.0 (good)
- ✅ OOS degradation < 30% vs. in-sample
- ✅ Beats equal-weight baseline 80% of time

### Phase 5B Explainability
- ✅ Identify top 5 most important factors
- ✅ Prune factors with <5% importance
- ✅ Generate factor contribution reports for all stocks

### Phase 6A Backtesting
- ✅ Transaction cost < 2% annually
- ✅ Realistic Sharpe > 1.0 after costs
- ✅ Hit rate > 55%
- ✅ CVaR-95 < 10%

### Phase 6B Portfolio Optimization
- ✅ Min-variance portfolio volatility < equal-weight by 20%
- ✅ Sharpe improvement > 0.2 vs. equal-weight
- ✅ Max single position < 10%
- ✅ Max sector exposure < 30%

### Phase 7 ML Models
- ✅ XGBoost OOS Sharpe > factor weights by 0.3
- ✅ Feature importance identifies new predictive features
- ✅ Ensemble model Sharpe > individual models
- ✅ OOS IC > 0.04 (statistically significant)

### Phase 8 Production
- ✅ Model drift detection catches 90% of degradations
- ✅ Automated retraining completes in < 1 hour
- ✅ API latency < 500ms per prediction
- ✅ Zero model deployment failures

---

## Technical Requirements

### New Dependencies

```toml
# pyproject.toml

[project]
dependencies = [
    # Existing
    "pandas>=2.1",
    "numpy>=1.24",
    "yfinance>=0.2",

    # NEW - ML Models
    "xgboost>=2.0",  # Gradient boosting
    "lightgbm>=4.0",  # Alternative gradient boosting
    "scikit-learn>=1.3",  # ML utilities
    "optuna>=3.0",  # Hyperparameter tuning

    # NEW - Optimization
    "cvxpy>=1.4",  # Convex optimization (portfolio)
    "scipy>=1.11",  # Scientific computing (already installed)

    # NEW - Visualization
    "plotly>=5.0",  # Interactive plots
    "shap>=0.44",  # SHAP values for explainability

    # NEW - Production
    "mlflow>=2.9",  # Model tracking (optional)
    "evidently>=0.4",  # Model monitoring (optional)
]

[project.optional-dependencies]
ml = [
    "xgboost>=2.0",
    "lightgbm>=4.0",
    "optuna>=3.0",
    "cvxpy>=1.4",
    "plotly>=5.0",
    "shap>=0.44",
]
```

### Hardware Requirements

**Training:**
- CPU: 4+ cores (parallel tree building)
- RAM: 16GB+ (for 10 years of daily data)
- Storage: 10GB+ (models + historical data)

**Inference:**
- CPU: 2+ cores
- RAM: 4GB+
- Latency: < 500ms for 100 stocks

---

## Implementation Timeline

```
Phase 5A: Validation Framework
Week 1-2: Walk-forward validation
Week 3: Baseline comparisons
Week 4: Overfitting detection

Phase 5B: Explainability
Week 5-6: Permutation importance
Week 7: Partial dependence plots

Phase 6A: Enhanced Backtesting
Week 8-9: Transaction costs
Week 10: Liquidity & dividends
Week 11: Enhanced metrics

Phase 6B: Portfolio Optimization
Week 12-13: Min-variance optimization
Week 14: Mean-variance & risk parity
Week 15: Comparison tools

Phase 7: ML Models
Week 16-17: Feature engineering
Week 18-19: XGBoost training
Week 20: Hyperparameter tuning
Week 21: Ensemble methods

Phase 8: Production
Week 22-23: Model registry & versioning
Week 24: Drift detection
Week 25: Automated retraining
```

**Total Duration:** ~6 months (25 weeks)

**Can be parallelized:** Phases 5B and 6A can run concurrently

---

## Risk Mitigation

### Technical Risks

**Risk:** XGBoost overfits on limited data
- **Mitigation:** Use walk-forward validation, regularization, early stopping
- **Contingency:** Fall back to factor weights

**Risk:** Transaction costs too high (>3% annually)
- **Mitigation:** Reduce turnover, optimize rebalancing frequency
- **Contingency:** Monthly rebalancing instead of weekly

**Risk:** Model drift not detected
- **Mitigation:** Multiple drift metrics (IC, Sharpe, feature distributions)
- **Contingency:** Manual monthly reviews

### Data Risks

**Risk:** Survivorship bias in historical data
- **Mitigation:** Use point-in-time data, exclude delisted stocks retrospectively
- **Contingency:** Document bias, adjust expectations

**Risk:** Data quality degrades (missing values)
- **Mitigation:** Automated quality checks, alerts on >10% missing
- **Contingency:** Use previous values, skip problematic periods

---

## Appendix: Quick Wins

### Low-Effort, High-Impact Improvements (1-2 weeks)

**1. Add Transaction Cost Estimates**
- Multiply backtest returns by 0.98 (2% annual cost)
- Add turnover calculation
- Estimate: 1 day of work

**2. Feature Importance from Current IC**
- Already have IC by factor
- Just rank and visualize
- Estimate: 2 days of work

**3. Regime-Based Position Sizing**
- If regime == BEAR: reduce positions by 30%
- If regime == BULL: full allocation
- Estimate: 3 days of work

**4. Rolling Window IC**
- Compute IC every month instead of all-time
- Track IC stability over time
- Estimate: 2 days of work

**5. Sector Rotation Strategy**
- Identify top-performing sectors each month
- Overweight by 10%
- Estimate: 3 days of work

---

## Conclusion

This plan transforms the finviz-scraper from a **factor-based screening tool** (70% mature) to a **production-ready ML platform** (95% mature) over 6 months.

**Key improvements:**
1. Proper validation (no look-ahead bias)
2. Modern ML models (XGBoost)
3. Portfolio optimization (min-variance)
4. Production deployment (drift detection, auto-retraining)

**Expected impact:**
- OOS Sharpe: 0.8 → 1.5+ (87% improvement)
- Predictive power: IC 0.04 → 0.07+ (75% improvement)
- Risk management: 30% volatility reduction
- Production readiness: 70% → 95% maturity

---

*Last updated: 2026-01-14*
*Author: Claude (AI Assistant)*
*Status: Draft - Ready for Review*
