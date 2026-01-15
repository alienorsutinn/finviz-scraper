"""
Tests for Phase 13: Complete Platform Enhancement

Tests covering:
- Factor analytics (PCA, correlations, decay)
- Risk engine (position sizing, limits, stop-loss)
- Broker integration (paper trading, journaling)
- Social sentiment (Reddit scraping)
- ETF flows (sector rotation)
- SEC filings (EDGAR API)
- Dashboard (Streamlit)
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import numpy as np
import pandas as pd


# ============================================================================
# Factor Analytics Tests
# ============================================================================

class TestPCAAnalyzer:
    """Tests for PCA factor analysis."""

    def test_pca_analyzer_import(self):
        """Test that PCAAnalyzer can be imported."""
        from finviz_weekly.factor_analytics import PCAAnalyzer
        analyzer = PCAAnalyzer()
        assert analyzer is not None
        assert analyzer.variance_threshold == 0.90

    def test_pca_analyzer_custom_threshold(self):
        """Test PCA with custom variance threshold."""
        from finviz_weekly.factor_analytics import PCAAnalyzer
        analyzer = PCAAnalyzer(variance_threshold=0.95)
        assert analyzer.variance_threshold == 0.95

    def test_pca_fit_transform(self):
        """Test PCA fit_transform on sample data."""
        from finviz_weekly.factor_analytics import PCAAnalyzer

        # Create sample data
        np.random.seed(42)
        n_samples = 100
        data = pd.DataFrame({
            'ticker': [f'T{i}' for i in range(n_samples)],
            'quality_score': np.random.randn(n_samples),
            'value_score': np.random.randn(n_samples),
            'momentum_score': np.random.randn(n_samples),
            'growth_score': np.random.randn(n_samples),
        })

        analyzer = PCAAnalyzer()
        factor_cols = ['quality_score', 'value_score', 'momentum_score', 'growth_score']
        result = analyzer.fit_transform(data, factor_cols)

        assert result is not None
        assert len(result.explained_variance_ratio) > 0
        assert sum(result.explained_variance_ratio) <= 1.0
        assert result.recommended_components >= 1

    def test_pca_get_redundant_factors(self):
        """Test identifying redundant factors."""
        from finviz_weekly.factor_analytics import PCAAnalyzer, PCAResult

        result = PCAResult(
            n_components=3,
            explained_variance_ratio=[0.5, 0.3, 0.2],
            cumulative_variance=[0.5, 0.8, 1.0],
            loadings=pd.DataFrame(),
            scores=pd.DataFrame(),
            feature_importance={'factor_a': 0.4, 'factor_b': 0.05, 'factor_c': 0.3},
            recommended_components=2,
        )

        analyzer = PCAAnalyzer()
        redundant = analyzer.get_redundant_factors(result, threshold=0.1)
        assert 'factor_b' in redundant
        assert 'factor_a' not in redundant


class TestRollingCorrelationTracker:
    """Tests for rolling correlation analysis."""

    def test_correlation_tracker_import(self):
        """Test that RollingCorrelationTracker can be imported."""
        from finviz_weekly.factor_analytics import RollingCorrelationTracker
        tracker = RollingCorrelationTracker()
        assert tracker is not None

    def test_correlation_config(self):
        """Test correlation configuration."""
        from finviz_weekly.factor_analytics import RollingCorrelationTracker
        tracker = RollingCorrelationTracker(window_days=30, n_clusters=5)
        assert tracker.window_days == 30
        assert tracker.n_clusters == 5


class TestFactorDecayAnalyzer:
    """Tests for factor decay analysis."""

    def test_decay_analyzer_import(self):
        """Test that FactorDecayAnalyzer can be imported."""
        from finviz_weekly.factor_analytics import FactorDecayAnalyzer
        analyzer = FactorDecayAnalyzer()
        assert analyzer is not None

    def test_decay_result_dataclass(self):
        """Test DecayResult dataclass."""
        from finviz_weekly.factor_analytics import DecayResult

        result = DecayResult(
            factor_name='momentum',
            half_life_days=15.5,
            decay_rate=0.045,
            initial_ic=0.12,
            current_ic=0.08,
            is_persistent=False,
            optimal_holding_period=21,
        )

        assert result.factor_name == 'momentum'
        assert result.half_life_days == 15.5


# ============================================================================
# Risk Engine Tests
# ============================================================================

class TestPositionLimits:
    """Tests for position limits configuration."""

    def test_position_limits_defaults(self):
        """Test default position limits."""
        from finviz_weekly.risk_engine import PositionLimits

        limits = PositionLimits()
        assert limits.max_position_pct == 0.10
        assert limits.max_sector_pct == 0.30
        assert limits.max_positions == 30
        assert limits.min_positions == 10

    def test_position_limits_custom(self):
        """Test custom position limits."""
        from finviz_weekly.risk_engine import PositionLimits

        limits = PositionLimits(
            max_position_pct=0.05,
            max_sector_pct=0.25,
            max_positions=50,
        )
        assert limits.max_position_pct == 0.05
        assert limits.max_sector_pct == 0.25
        assert limits.max_positions == 50


class TestPositionSizer:
    """Tests for position sizing methods."""

    def test_position_sizer_import(self):
        """Test PositionSizer import."""
        from finviz_weekly.risk_engine import PositionSizer, SizingMethod

        sizer = PositionSizer(method=SizingMethod.EQUAL_WEIGHT)
        assert sizer is not None
        assert sizer.method == SizingMethod.EQUAL_WEIGHT

    def test_sizing_methods(self):
        """Test all sizing methods enum values."""
        from finviz_weekly.risk_engine import SizingMethod

        assert SizingMethod.EQUAL_WEIGHT.value == "equal_weight"
        assert SizingMethod.RISK_PARITY.value == "risk_parity"
        assert SizingMethod.VOLATILITY_TARGET.value == "volatility_target"
        assert SizingMethod.KELLY_CRITERION.value == "kelly_criterion"

    def test_equal_weight_sizing(self):
        """Test equal weight position sizing."""
        from finviz_weekly.risk_engine import PositionSizer, SizingMethod

        sizer = PositionSizer(method=SizingMethod.EQUAL_WEIGHT)
        tickers = ['AAPL', 'MSFT', 'GOOGL', 'NVDA']
        weights = sizer.calculate_weights(tickers, portfolio_value=100000)

        assert len(weights) == 4
        assert all(w > 0 for w in weights.values())


class TestStopLossManager:
    """Tests for stop-loss management."""

    def test_stop_loss_types(self):
        """Test stop-loss type enum."""
        from finviz_weekly.risk_engine import StopLossType

        assert StopLossType.FIXED.value == "fixed"
        assert StopLossType.TRAILING.value == "trailing"
        assert StopLossType.ATR_BASED.value == "atr_based"
        assert StopLossType.VOLATILITY_ADJUSTED.value == "volatility_adjusted"

    def test_stop_loss_manager_import(self):
        """Test StopLossManager import."""
        from finviz_weekly.risk_engine import StopLossManager, StopLossType

        manager = StopLossManager(stop_type=StopLossType.TRAILING)
        assert manager is not None


class TestRiskEngine:
    """Tests for risk engine."""

    def test_risk_engine_import(self):
        """Test RiskEngine import."""
        from finviz_weekly.risk_engine import RiskEngine, PositionLimits

        limits = PositionLimits()
        engine = RiskEngine(limits=limits)
        assert engine is not None

    def test_risk_engine_check_new_trade(self):
        """Test new trade risk check."""
        from finviz_weekly.risk_engine import RiskEngine, PositionLimits

        limits = PositionLimits(max_position_pct=0.10)
        engine = RiskEngine(limits=limits)

        # Should pass for small position
        result = engine.check_new_trade(
            ticker='AAPL',
            trade_value=5000,
            portfolio_value=100000,
            current_positions={},
        )
        assert result.is_allowed


# ============================================================================
# Broker Integration Tests
# ============================================================================

class TestBaseBroker:
    """Tests for broker base class."""

    def test_broker_abstract_class(self):
        """Test BaseBroker is abstract."""
        from finviz_weekly.broker_integration import BaseBroker

        with pytest.raises(TypeError):
            BaseBroker()  # Can't instantiate abstract class


class TestPaperTradingBroker:
    """Tests for paper trading broker."""

    def test_paper_broker_init(self):
        """Test paper trading broker initialization."""
        from finviz_weekly.broker_integration import PaperTradingBroker

        broker = PaperTradingBroker(initial_cash=50000)
        assert broker is not None
        assert broker.initial_cash == 50000

    def test_paper_broker_connect(self):
        """Test paper broker connection."""
        from finviz_weekly.broker_integration import PaperTradingBroker

        broker = PaperTradingBroker()
        result = broker.connect()
        assert result is True

    def test_paper_broker_get_account(self):
        """Test getting account info."""
        from finviz_weekly.broker_integration import PaperTradingBroker

        broker = PaperTradingBroker(initial_cash=100000)
        broker.connect()
        account = broker.get_account_info()

        assert account is not None
        assert account.cash == 100000
        assert account.portfolio_value == 100000


class TestOrderTypes:
    """Tests for order types."""

    def test_order_side_enum(self):
        """Test OrderSide enum."""
        from finviz_weekly.broker_integration import OrderSide

        assert OrderSide.BUY.value == "buy"
        assert OrderSide.SELL.value == "sell"

    def test_order_type_enum(self):
        """Test OrderType enum."""
        from finviz_weekly.broker_integration import OrderType

        assert OrderType.MARKET.value == "market"
        assert OrderType.LIMIT.value == "limit"
        assert OrderType.STOP.value == "stop"

    def test_order_status_enum(self):
        """Test OrderStatus enum."""
        from finviz_weekly.broker_integration import OrderStatus

        assert OrderStatus.PENDING.value == "pending"
        assert OrderStatus.FILLED.value == "filled"
        assert OrderStatus.CANCELLED.value == "cancelled"


class TestTradeJournal:
    """Tests for trade journal."""

    def test_trade_journal_import(self):
        """Test TradeJournal import."""
        from finviz_weekly.broker_integration import TradeJournal

        journal = TradeJournal()
        assert journal is not None


# ============================================================================
# Social Sentiment Tests
# ============================================================================

class TestTickerExtractor:
    """Tests for ticker extraction."""

    def test_ticker_extractor_import(self):
        """Test TickerExtractor import."""
        from finviz_weekly.social_sentiment import TickerExtractor

        extractor = TickerExtractor()
        assert extractor is not None

    def test_extract_tickers(self):
        """Test ticker extraction from text."""
        from finviz_weekly.social_sentiment import TickerExtractor

        extractor = TickerExtractor()
        text = "Just bought some $AAPL and MSFT. Love NVDA too!"
        tickers = extractor.extract(text)

        assert 'AAPL' in tickers
        assert 'MSFT' in tickers
        assert 'NVDA' in tickers


class TestRedditScraper:
    """Tests for Reddit scraper."""

    def test_reddit_scraper_import(self):
        """Test RedditScraper import."""
        from finviz_weekly.social_sentiment import RedditScraper, SocialSentimentConfig

        config = SocialSentimentConfig()
        scraper = RedditScraper(config)
        assert scraper is not None

    def test_social_config(self):
        """Test social sentiment config."""
        from finviz_weekly.social_sentiment import SocialSentimentConfig

        config = SocialSentimentConfig(
            subreddits=['wallstreetbets', 'stocks'],
            post_limit=50,
        )
        assert 'wallstreetbets' in config.subreddits
        assert config.post_limit == 50


# ============================================================================
# ETF Flows Tests
# ============================================================================

class TestETFFlowTracker:
    """Tests for ETF flow tracker."""

    def test_etf_tracker_import(self):
        """Test ETFFlowTracker import."""
        from finviz_weekly.etf_flows import ETFFlowTracker

        tracker = ETFFlowTracker()
        assert tracker is not None

    def test_sector_etfs_defined(self):
        """Test sector ETF mappings exist."""
        from finviz_weekly.etf_flows import SECTOR_ETFS

        assert 'Technology' in SECTOR_ETFS
        assert 'Healthcare' in SECTOR_ETFS
        assert 'Financials' in SECTOR_ETFS
        assert 'XLK' in SECTOR_ETFS['Technology']


class TestSectorRotationSignal:
    """Tests for sector rotation signals."""

    def test_rotation_signal_import(self):
        """Test SectorRotationSignal import."""
        from finviz_weekly.etf_flows import SectorRotationSignal

        signal = SectorRotationSignal(
            rotation_type='risk_on',
            strength=75.0,
            sectors_in=['Technology', 'Financials'],
            sectors_out=['Utilities', 'Consumer Staples'],
            confidence=0.85,
        )

        assert signal.rotation_type == 'risk_on'
        assert signal.strength == 75.0
        assert 'Technology' in signal.sectors_in


# ============================================================================
# SEC Filings Tests
# ============================================================================

class TestSECEdgarClient:
    """Tests for SEC EDGAR API client."""

    def test_edgar_client_import(self):
        """Test SECEdgarClient import."""
        from finviz_weekly.sec_filings import SECEdgarClient

        client = SECEdgarClient()
        assert client is not None
        assert client.base_url is not None


class TestFilingTextAnalyzer:
    """Tests for filing text analysis."""

    def test_text_analyzer_import(self):
        """Test FilingTextAnalyzer import."""
        from finviz_weekly.sec_filings import FilingTextAnalyzer

        analyzer = FilingTextAnalyzer()
        assert analyzer is not None


class TestSECFilingsTracker:
    """Tests for SEC filings tracker."""

    def test_filings_tracker_import(self):
        """Test SECFilingsTracker import."""
        from finviz_weekly.sec_filings import SECFilingsTracker

        tracker = SECFilingsTracker()
        assert tracker is not None

    def test_filing_dataclass(self):
        """Test Filing dataclass."""
        from finviz_weekly.sec_filings import Filing

        filing = Filing(
            ticker='AAPL',
            cik='0000320193',
            filing_type='10-K',
            filed_date='2024-01-15',
            accession_number='0000320193-24-000123',
            document_url='https://sec.gov/...',
            description='Annual Report',
        )

        assert filing.ticker == 'AAPL'
        assert filing.filing_type == '10-K'


# ============================================================================
# Dashboard Tests
# ============================================================================

class TestDashboard:
    """Tests for dashboard module."""

    def test_dashboard_config_import(self):
        """Test DashboardConfig import."""
        from finviz_weekly.dashboard import DashboardConfig

        config = DashboardConfig()
        assert config.title == "Finviz Scraper Dashboard"
        assert config.refresh_interval == 300
        assert config.enable_trading is False

    def test_dashboard_functions_exist(self):
        """Test dashboard render functions exist."""
        from finviz_weekly import dashboard

        assert hasattr(dashboard, 'render_portfolio_overview')
        assert hasattr(dashboard, 'render_risk_analytics')
        assert hasattr(dashboard, 'render_factor_analysis')
        assert hasattr(dashboard, 'render_screening')
        assert hasattr(dashboard, 'render_trading')
        assert hasattr(dashboard, 'render_settings')


# ============================================================================
# CLI Tests
# ============================================================================

class TestCLIPhase13Commands:
    """Tests for Phase 13 CLI commands."""

    def test_cli_has_phase13_commands(self):
        """Test CLI parser includes Phase 13 commands."""
        from finviz_weekly.cli import parse_args

        # Test each command can be parsed
        for cmd in ['factor-analytics', 'risk-check', 'paper-trade',
                    'reddit', 'etf-flows', 'sec-filings', 'dashboard']:
            args = parse_args([cmd])
            assert args.command == cmd

    def test_factor_analytics_args(self):
        """Test factor-analytics command arguments."""
        from finviz_weekly.cli import parse_args

        args = parse_args(['factor-analytics', '--analytics-action', 'pca'])
        assert args.analytics_action == 'pca'

    def test_paper_trade_args(self):
        """Test paper-trade command arguments."""
        from finviz_weekly.cli import parse_args

        args = parse_args(['paper-trade', '--trade-action', 'buy', '--shares', '100'])
        assert args.trade_action == 'buy'
        assert args.shares == 100

    def test_reddit_args(self):
        """Test reddit command arguments."""
        from finviz_weekly.cli import parse_args

        args = parse_args(['reddit', '--subreddits', 'wallstreetbets,stocks'])
        assert 'wallstreetbets' in args.subreddits

    def test_sec_filings_args(self):
        """Test sec-filings command arguments."""
        from finviz_weekly.cli import parse_args

        args = parse_args(['sec-filings', '--filing-type', '10-K', '--filing-days', '60'])
        assert args.filing_type == '10-K'
        assert args.filing_days == 60


# ============================================================================
# Integration Tests
# ============================================================================

class TestPhase13Integration:
    """Integration tests for Phase 13 components."""

    def test_all_modules_importable(self):
        """Test all Phase 13 modules can be imported."""
        # These should not raise ImportError
        from finviz_weekly import factor_analytics
        from finviz_weekly import risk_engine
        from finviz_weekly import broker_integration
        from finviz_weekly import social_sentiment
        from finviz_weekly import etf_flows
        from finviz_weekly import sec_filings
        from finviz_weekly import dashboard

    def test_risk_engine_with_broker(self):
        """Test risk engine integration with paper broker."""
        from finviz_weekly.risk_engine import RiskEngine, PositionLimits
        from finviz_weekly.broker_integration import PaperTradingBroker

        # Create components
        limits = PositionLimits(max_position_pct=0.10)
        engine = RiskEngine(limits=limits)
        broker = PaperTradingBroker(initial_cash=100000)
        broker.connect()

        # Get account info
        account = broker.get_account_info()

        # Check if trade is allowed
        result = engine.check_new_trade(
            ticker='AAPL',
            trade_value=5000,
            portfolio_value=account.portfolio_value,
            current_positions={},
        )

        assert result.is_allowed
