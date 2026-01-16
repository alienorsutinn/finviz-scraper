"""
Tests for Phase 14: Production Platform

Tests covering:
- Walk-forward backtesting
- Monte Carlo simulation
- Portfolio optimization
- Rebalancing engine
- FastAPI service
- Alert bots
- Database integration
- Scheduler
- Transformer models
- LLM assistant
"""

import pytest
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


# ============================================================================
# Walk-Forward Backtesting Tests
# ============================================================================

class TestWalkForwardConfig:
    """Tests for walk-forward configuration."""

    def test_config_defaults(self):
        """Test default configuration."""
        from finviz_weekly.walkforward import WalkForwardConfig

        config = WalkForwardConfig()
        assert config.training_window_days == 252
        assert config.test_window_days == 21
        assert config.step_days == 21

    def test_config_custom(self):
        """Test custom configuration."""
        from finviz_weekly.walkforward import WalkForwardConfig

        config = WalkForwardConfig(
            training_window_days=126,
            test_window_days=10,
        )
        assert config.training_window_days == 126
        assert config.test_window_days == 10


class TestParameterSpace:
    """Tests for parameter space."""

    def test_parameter_space_creation(self):
        """Test creating parameter space."""
        from finviz_weekly.walkforward import ParameterSpace

        space = ParameterSpace()
        space.add_continuous("param1", 0.0, 1.0)
        space.add_discrete("param2", ["a", "b", "c"])
        space.add_integer("param3", 1, 10)

        assert "param1" in space.params
        assert "param2" in space.params
        assert "param3" in space.params

    def test_random_sampling(self):
        """Test random parameter sampling."""
        from finviz_weekly.walkforward import ParameterSpace

        space = ParameterSpace()
        space.add_continuous("x", 0.0, 1.0)
        space.add_integer("n", 1, 10)

        sample = space.sample_random()
        assert 0.0 <= sample["x"] <= 1.0
        assert 1 <= sample["n"] <= 10


# ============================================================================
# Monte Carlo Simulation Tests
# ============================================================================

class TestMonteCarloConfig:
    """Tests for Monte Carlo configuration."""

    def test_config_defaults(self):
        """Test default configuration."""
        from finviz_weekly.monte_carlo import MonteCarloConfig

        config = MonteCarloConfig()
        assert config.n_simulations == 10000
        assert config.n_periods == 252

    def test_return_model_enum(self):
        """Test return model enum."""
        from finviz_weekly.monte_carlo import ReturnModel

        assert ReturnModel.NORMAL.value == "normal"
        assert ReturnModel.STUDENT_T.value == "student_t"
        assert ReturnModel.HISTORICAL_BOOTSTRAP.value == "historical_bootstrap"


class TestMonteCarloSimulator:
    """Tests for Monte Carlo simulator."""

    def test_simulator_creation(self):
        """Test simulator creation."""
        from finviz_weekly.monte_carlo import MonteCarloSimulator, MonteCarloConfig

        config = MonteCarloConfig(n_simulations=100, n_periods=10)
        simulator = MonteCarloSimulator(config)
        assert simulator is not None

    def test_simple_simulation(self):
        """Test running a simple simulation."""
        from finviz_weekly.monte_carlo import MonteCarloSimulator, MonteCarloConfig

        config = MonteCarloConfig(n_simulations=100, n_periods=10)
        simulator = MonteCarloSimulator(config)

        returns = np.random.normal(0.001, 0.02, 100)
        result = simulator.simulate(returns)

        assert result is not None
        assert len(result.paths) == 100


class TestVaRCalculation:
    """Tests for VaR calculation."""

    def test_historical_var(self):
        """Test historical VaR."""
        from finviz_weekly.monte_carlo import calculate_var

        returns = np.random.normal(-0.001, 0.02, 1000)
        var = calculate_var(returns, confidence=0.95, method='historical')

        assert var > 0
        assert var < 0.1

    def test_cvar(self):
        """Test CVaR calculation."""
        from finviz_weekly.monte_carlo import calculate_cvar

        returns = np.random.normal(-0.001, 0.02, 1000)
        cvar = calculate_cvar(returns, confidence=0.95)

        assert cvar > 0


# ============================================================================
# Portfolio Optimization Tests
# ============================================================================

class TestOptimizationObjective:
    """Tests for optimization objectives."""

    def test_objective_enum(self):
        """Test optimization objective enum."""
        from finviz_weekly.portfolio_optimizer import OptimizationObjective

        assert OptimizationObjective.MAX_SHARPE.value == "max_sharpe"
        assert OptimizationObjective.MIN_VARIANCE.value == "min_variance"
        assert OptimizationObjective.RISK_PARITY.value == "risk_parity"


class TestMeanVarianceOptimizer:
    """Tests for Mean-Variance optimizer."""

    def test_optimizer_creation(self):
        """Test optimizer creation."""
        from finviz_weekly.portfolio_optimizer import MeanVarianceOptimizer

        returns = pd.DataFrame({
            'A': np.random.normal(0.001, 0.02, 100),
            'B': np.random.normal(0.0015, 0.025, 100),
            'C': np.random.normal(0.0008, 0.015, 100),
        })

        optimizer = MeanVarianceOptimizer(returns)
        assert optimizer.n_assets == 3

    def test_max_sharpe_optimization(self):
        """Test max Sharpe ratio optimization."""
        from finviz_weekly.portfolio_optimizer import MeanVarianceOptimizer, OptimizationObjective

        np.random.seed(42)
        returns = pd.DataFrame({
            'A': np.random.normal(0.001, 0.02, 100),
            'B': np.random.normal(0.0015, 0.025, 100),
        })

        optimizer = MeanVarianceOptimizer(returns)
        result = optimizer.optimize(OptimizationObjective.MAX_SHARPE)

        assert len(result.weights) == 2
        assert abs(sum(result.weights) - 1.0) < 0.01


class TestHRP:
    """Tests for Hierarchical Risk Parity."""

    def test_hrp_creation(self):
        """Test HRP creation."""
        from finviz_weekly.portfolio_optimizer import HierarchicalRiskParity

        returns = pd.DataFrame({
            'A': np.random.normal(0.001, 0.02, 100),
            'B': np.random.normal(0.0015, 0.025, 100),
            'C': np.random.normal(0.0008, 0.015, 100),
        })

        hrp = HierarchicalRiskParity(returns)
        assert hrp.n_assets == 3


# ============================================================================
# Rebalancing Engine Tests
# ============================================================================

class TestRebalanceConfig:
    """Tests for rebalance configuration."""

    def test_config_defaults(self):
        """Test default configuration."""
        from finviz_weekly.rebalancer import RebalanceConfig

        config = RebalanceConfig()
        assert config.absolute_threshold == 0.05
        assert config.relative_threshold == 0.25


class TestRebalanceStrategy:
    """Tests for rebalance strategies."""

    def test_strategy_enum(self):
        """Test strategy enum."""
        from finviz_weekly.rebalancer import RebalanceStrategy

        assert RebalanceStrategy.CALENDAR.value == "calendar"
        assert RebalanceStrategy.THRESHOLD.value == "threshold"


class TestDriftCalculator:
    """Tests for drift calculation."""

    def test_drift_calculation(self):
        """Test drift calculation."""
        from finviz_weekly.rebalancer import calculate_portfolio_drift

        current = {'AAPL': 0.30, 'MSFT': 0.30, 'GOOGL': 0.40}
        target = {'AAPL': 0.33, 'MSFT': 0.33, 'GOOGL': 0.34}

        drift = calculate_portfolio_drift(current, target)
        assert len(drift) == 3
        assert 'absolute_drift' in drift.columns


# ============================================================================
# FastAPI Service Tests
# ============================================================================

class TestAPIModels:
    """Tests for API models."""

    def test_api_available(self):
        """Test if API can be imported."""
        try:
            from finviz_weekly.api import FASTAPI_AVAILABLE
            # Just test import works
            assert True
        except ImportError:
            pytest.skip("FastAPI not installed")


class TestAPIEndpoints:
    """Tests for API endpoints."""

    def test_create_app(self):
        """Test app creation."""
        try:
            from finviz_weekly.api import create_app, FASTAPI_AVAILABLE
            if not FASTAPI_AVAILABLE:
                pytest.skip("FastAPI not installed")

            app = create_app()
            assert app is not None
        except ImportError:
            pytest.skip("FastAPI not installed")


# ============================================================================
# Alert Bot Tests
# ============================================================================

class TestAlertPriority:
    """Tests for alert priority."""

    def test_priority_enum(self):
        """Test priority enum."""
        from finviz_weekly.alerts_bot import AlertPriority

        assert AlertPriority.LOW.value == "low"
        assert AlertPriority.CRITICAL.value == "critical"


class TestAlert:
    """Tests for Alert class."""

    def test_alert_creation(self):
        """Test alert creation."""
        from finviz_weekly.alerts_bot import Alert, AlertType, AlertPriority

        alert = Alert(
            type=AlertType.SIGNAL,
            priority=AlertPriority.HIGH,
            title="Test Alert",
            message="Test message",
        )

        assert alert.title == "Test Alert"
        assert alert.priority == AlertPriority.HIGH

    def test_alert_to_text(self):
        """Test alert text conversion."""
        from finviz_weekly.alerts_bot import Alert, AlertType, AlertPriority

        alert = Alert(
            type=AlertType.INFO,
            priority=AlertPriority.MEDIUM,
            title="Info",
            message="Information",
        )

        text = alert.to_text()
        assert "Info" in text


class TestAlertFactories:
    """Tests for alert factory functions."""

    def test_signal_alert(self):
        """Test signal alert creation."""
        from finviz_weekly.alerts_bot import create_signal_alert

        alert = create_signal_alert("AAPL", "buy", 75.0, "Strong momentum")
        assert "AAPL" in alert.title
        assert alert.data["ticker"] == "AAPL"

    def test_risk_alert(self):
        """Test risk alert creation."""
        from finviz_weekly.alerts_bot import create_risk_alert

        alert = create_risk_alert("VaR", 0.05, 0.03)
        assert "VaR" in alert.title


# ============================================================================
# Database Integration Tests
# ============================================================================

class TestDatabaseConfig:
    """Tests for database configuration."""

    def test_config_defaults(self):
        """Test default configuration."""
        from finviz_weekly.database import DatabaseConfig

        config = DatabaseConfig()
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "finviz"

    def test_connection_string(self):
        """Test connection string generation."""
        from finviz_weekly.database import DatabaseConfig

        config = DatabaseConfig(host="myhost", user="myuser")
        assert "myhost" in config.connection_string
        assert "myuser" in config.connection_string


# ============================================================================
# Scheduler Tests
# ============================================================================

class TestTaskStatus:
    """Tests for task status."""

    def test_status_enum(self):
        """Test status enum."""
        from finviz_weekly.scheduler import TaskStatus

        assert TaskStatus.PENDING.value == "pending"
        assert TaskStatus.SUCCESS.value == "success"
        assert TaskStatus.FAILED.value == "failed"


class TestTaskResult:
    """Tests for task result."""

    def test_result_creation(self):
        """Test result creation."""
        from finviz_weekly.scheduler import TaskResult, TaskStatus

        result = TaskResult(
            task_id="test_task",
            status=TaskStatus.SUCCESS,
            start_time=datetime.now(),
        )

        assert result.task_id == "test_task"
        assert result.status == TaskStatus.SUCCESS


class TestSimpleScheduler:
    """Tests for simple scheduler."""

    def test_scheduler_creation(self):
        """Test scheduler creation."""
        from finviz_weekly.scheduler import SimpleScheduler

        scheduler = SimpleScheduler()
        assert scheduler is not None
        assert len(scheduler.tasks) == 0


# ============================================================================
# Transformer Model Tests
# ============================================================================

class TestTransformerConfig:
    """Tests for transformer configuration."""

    def test_config_defaults(self):
        """Test default configuration."""
        from finviz_weekly.ml_transformer import TransformerConfig

        config = TransformerConfig()
        assert config.d_model == 64
        assert config.n_heads == 4
        assert config.seq_length == 60


class TestModelType:
    """Tests for model type enum."""

    def test_model_type_enum(self):
        """Test model type enum."""
        from finviz_weekly.ml_transformer import ModelType

        assert ModelType.PRICE_PREDICTOR.value == "price_predictor"
        assert ModelType.VOLATILITY_PREDICTOR.value == "volatility_predictor"


class TestFeatureEngineering:
    """Tests for time series feature engineering."""

    def test_feature_creation(self):
        """Test feature creation."""
        from finviz_weekly.ml_transformer import create_time_series_features

        df = pd.DataFrame({
            'close': np.random.randn(100).cumsum() + 100,
            'volume': np.random.randint(1000000, 10000000, 100),
        })

        features = create_time_series_features(df)
        assert 'return_1d' in features.columns
        assert 'volatility_5d' in features.columns
        assert 'rsi' in features.columns


# ============================================================================
# LLM Assistant Tests
# ============================================================================

class TestLLMConfig:
    """Tests for LLM configuration."""

    def test_config_defaults(self):
        """Test default configuration."""
        from finviz_weekly.llm_assistant import LLMConfig

        config = LLMConfig()
        assert config.model == "gpt-4o-mini"
        assert config.temperature == 0.3


class TestResearchReport:
    """Tests for research report."""

    def test_report_creation(self):
        """Test report creation."""
        from finviz_weekly.llm_assistant import ResearchReport

        report = ResearchReport(
            ticker="AAPL",
            title="Test Report",
            summary="Test summary",
            thesis="Test thesis",
            bull_case="Test bull case",
            bear_case="Test bear case",
            key_metrics={},
            risks=["Risk 1"],
            catalysts=["Catalyst 1"],
            recommendation="Buy",
        )

        assert report.ticker == "AAPL"
        assert report.recommendation == "Buy"

    def test_report_to_markdown(self):
        """Test markdown conversion."""
        from finviz_weekly.llm_assistant import ResearchReport

        report = ResearchReport(
            ticker="AAPL",
            title="Apple Inc. Analysis",
            summary="Summary text",
            thesis="Thesis text",
            bull_case="Bull case text",
            bear_case="Bear case text",
            key_metrics={"PE": 25},
            risks=["Competition"],
            catalysts=["New products"],
            recommendation="Buy",
        )

        md = report.to_markdown()
        assert "Apple Inc. Analysis" in md
        assert "Bull Case" in md
        assert "Competition" in md


# ============================================================================
# CLI Tests
# ============================================================================

class TestCLIPhase14Commands:
    """Tests for Phase 14 CLI commands."""

    def test_cli_has_phase14_commands(self):
        """Test CLI parser includes Phase 14 commands."""
        from finviz_weekly.cli import parse_args

        # Test each command can be parsed
        for cmd in ['backtest', 'monte-carlo', 'optimize', 'rebalance', 'api', 'research', 'scheduler']:
            args = parse_args([cmd])
            assert args.command == cmd

    def test_backtest_args(self):
        """Test backtest command arguments."""
        from finviz_weekly.cli import parse_args

        args = parse_args(['backtest', '--train-window', '126', '--test-window', '10'])
        assert args.train_window == 126
        assert args.test_window == 10

    def test_monte_carlo_args(self):
        """Test monte-carlo command arguments."""
        from finviz_weekly.cli import parse_args

        args = parse_args(['monte-carlo', '--n-simulations', '5000'])
        assert args.n_simulations == 5000

    def test_optimize_args(self):
        """Test optimize command arguments."""
        from finviz_weekly.cli import parse_args

        args = parse_args(['optimize', '--opt-method', 'hrp', '--opt-objective', 'min_variance'])
        assert args.opt_method == 'hrp'
        assert args.opt_objective == 'min_variance'


# ============================================================================
# Integration Tests
# ============================================================================

class TestPhase14Integration:
    """Integration tests for Phase 14 components."""

    def test_all_modules_importable(self):
        """Test all Phase 14 modules can be imported."""
        from finviz_weekly import walkforward
        from finviz_weekly import monte_carlo
        from finviz_weekly import portfolio_optimizer
        from finviz_weekly import rebalancer
        from finviz_weekly import api
        from finviz_weekly import alerts_bot
        from finviz_weekly import database
        from finviz_weekly import scheduler
        from finviz_weekly import ml_transformer
        from finviz_weekly import llm_assistant

    def test_optimizer_with_simulation(self):
        """Test optimizer with Monte Carlo simulation."""
        from finviz_weekly.portfolio_optimizer import MeanVarianceOptimizer
        from finviz_weekly.monte_carlo import MonteCarloSimulator, MonteCarloConfig

        # Create sample returns
        np.random.seed(42)
        returns = pd.DataFrame({
            'A': np.random.normal(0.001, 0.02, 100),
            'B': np.random.normal(0.0015, 0.025, 100),
        })

        # Optimize
        optimizer = MeanVarianceOptimizer(returns)
        result = optimizer.optimize()

        # Simulate
        portfolio_returns = (returns @ result.weights).values
        config = MonteCarloConfig(n_simulations=100, n_periods=10)
        simulator = MonteCarloSimulator(config)
        sim_result = simulator.simulate(portfolio_returns)

        assert sim_result.prob_positive > 0
