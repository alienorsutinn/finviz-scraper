"""Tests for Phase 12 modules: Institutional & Macro Intelligence."""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import numpy as np


# =============================================================================
# Institutional Ownership Tests
# =============================================================================

class TestInstitutionalOwnership:
    """Tests for institutional ownership scraper."""

    def test_institutional_config_defaults(self):
        """Test InstitutionalConfig has sensible defaults."""
        from finviz_weekly.scrapers.institutional_ownership import InstitutionalConfig

        config = InstitutionalConfig()
        assert config.min_holders == 5
        assert config.significant_change_pct == 10.0
        assert config.request_delay == 0.5

    def test_institutional_data_dataclass(self):
        """Test InstitutionalData dataclass fields."""
        from finviz_weekly.scrapers.institutional_ownership import InstitutionalData

        data = InstitutionalData(
            ticker="AAPL",
            fetch_date="2024-01-15",
            institutional_pct=75.5,
            num_holders=1234,
        )

        assert data.ticker == "AAPL"
        assert data.institutional_pct == 75.5
        assert data.ownership_score == 50.0  # Default
        assert data.combined_score == 50.0

    def test_institutional_holder_dataclass(self):
        """Test InstitutionalHolder dataclass."""
        from finviz_weekly.scrapers.institutional_ownership import InstitutionalHolder

        holder = InstitutionalHolder(
            holder_name="Vanguard Group",
            shares=1000000,
            date_reported="2024-01-01",
            pct_held=5.5,
            value=150000000,
            pct_change=10.5,
        )

        assert holder.holder_name == "Vanguard Group"
        assert holder.pct_change == 10.5

    def test_scraper_cache_mechanism(self):
        """Test that scraper uses cache correctly."""
        from finviz_weekly.scrapers.institutional_ownership import (
            InstitutionalOwnershipScraper,
            InstitutionalConfig,
            InstitutionalData,
        )

        config = InstitutionalConfig(cache_hours=1)
        scraper = InstitutionalOwnershipScraper(config)

        # Manually set cache
        test_data = InstitutionalData(ticker="TEST", fetch_date="2024-01-15")
        scraper._set_cache("TEST", test_data)

        # Should return cached data
        assert scraper._is_cached("TEST") is True
        cached = scraper._get_cached("TEST")
        assert cached.ticker == "TEST"

    def test_calculate_scores(self):
        """Test score calculation logic."""
        from finviz_weekly.scrapers.institutional_ownership import (
            InstitutionalOwnershipScraper,
            InstitutionalData,
        )

        scraper = InstitutionalOwnershipScraper()

        # Test with high institutional ownership
        data = InstitutionalData(
            ticker="HIGH",
            fetch_date="2024-01-15",
            institutional_pct=60.0,
            increased_positions=8,
            decreased_positions=2,
            new_positions=3,
        )

        result = scraper._calculate_scores(data)
        assert result.ownership_score > 70  # High ownership should give good score
        assert result.momentum_score > 50  # Net positive changes

    def test_detect_accumulation(self):
        """Test accumulation detection."""
        from finviz_weekly.scrapers.institutional_ownership import (
            InstitutionalOwnershipScraper,
            InstitutionalData,
        )

        scraper = InstitutionalOwnershipScraper()

        data = {
            "ACCUM": InstitutionalData(
                ticker="ACCUM",
                fetch_date="2024-01-15",
                new_positions=5,
                increased_positions=10,
                decreased_positions=2,
                net_institutional_change=15.0,
            ),
            "NORMAL": InstitutionalData(
                ticker="NORMAL",
                fetch_date="2024-01-15",
                new_positions=0,
                increased_positions=3,
                decreased_positions=3,
            ),
        }

        accumulating = scraper.detect_accumulation(data)
        assert "ACCUM" in accumulating
        assert "NORMAL" not in accumulating

    def test_create_institutional_features(self):
        """Test feature creation for ML."""
        from finviz_weekly.scrapers.institutional_ownership import (
            InstitutionalData,
            create_institutional_features,
        )

        data = {
            "AAPL": InstitutionalData(
                ticker="AAPL",
                fetch_date="2024-01-15",
                institutional_pct=70.0,
                num_holders=100,
                momentum_score=75.0,
            ),
        }

        features = create_institutional_features(data)
        assert len(features) == 1
        assert "inst_pct" in features.columns
        assert "inst_momentum_score" in features.columns


# =============================================================================
# Macro Indicators Tests
# =============================================================================

class TestMacroIndicators:
    """Tests for macro indicators module."""

    def test_macro_regime_enum(self):
        """Test MacroRegime enum values."""
        from finviz_weekly.macro_indicators import MacroRegime

        assert MacroRegime.RISK_ON.value == "risk_on"
        assert MacroRegime.RISK_OFF.value == "risk_off"
        assert MacroRegime.RECESSION_WARNING.value == "recession_warning"

    def test_yield_curve_state_enum(self):
        """Test YieldCurveState enum values."""
        from finviz_weekly.macro_indicators import YieldCurveState

        assert YieldCurveState.NORMAL.value == "normal"
        assert YieldCurveState.INVERTED.value == "inverted"
        assert YieldCurveState.FLAT.value == "flat"

    def test_yield_curve_data_dataclass(self):
        """Test YieldCurveData dataclass."""
        from finviz_weekly.macro_indicators import YieldCurveData, YieldCurveState

        data = YieldCurveData(
            date="2024-01-15",
            yield_2y=4.5,
            yield_10y=4.0,
            spread_2y10y=-0.5,
            curve_state=YieldCurveState.INVERTED,
        )

        assert data.yield_2y == 4.5
        assert data.spread_2y10y == -0.5
        assert data.curve_state == YieldCurveState.INVERTED

    def test_vix_data_dataclass(self):
        """Test VIXData dataclass."""
        from finviz_weekly.macro_indicators import VIXData, VIXTermStructure

        data = VIXData(
            date="2024-01-15",
            vix_spot=25.5,
            vix_percentile=75.0,
            is_elevated=True,
            term_structure=VIXTermStructure.FLAT,
        )

        assert data.vix_spot == 25.5
        assert data.is_elevated is True

    def test_macro_snapshot_dataclass(self):
        """Test MacroSnapshot dataclass."""
        from finviz_weekly.macro_indicators import MacroSnapshot, MacroRegime

        snapshot = MacroSnapshot(
            date="2024-01-15",
            regime=MacroRegime.RISK_ON,
            risk_score=70.0,
        )

        assert snapshot.regime == MacroRegime.RISK_ON
        assert snapshot.risk_score == 70.0

    def test_sector_sensitivity_mapping(self):
        """Test sector sensitivity configuration."""
        from finviz_weekly.macro_indicators import MacroIndicators

        assert "Technology" in MacroIndicators.SECTOR_MACRO_SENSITIVITY
        assert "Utilities" in MacroIndicators.SECTOR_MACRO_SENSITIVITY

        tech = MacroIndicators.SECTOR_MACRO_SENSITIVITY["Technology"]
        assert tech["risk_on"] > 1.0  # Tech is risk-on sensitive

        utils = MacroIndicators.SECTOR_MACRO_SENSITIVITY["Utilities"]
        assert utils["risk_on"] < 0  # Utilities are defensive

    def test_position_size_multiplier(self):
        """Test position size multiplier calculation."""
        from finviz_weekly.macro_indicators import MacroIndicators

        indicators = MacroIndicators()

        # Mock the snapshot to test different regimes
        with patch.object(indicators, 'get_macro_snapshot') as mock_snapshot:
            from finviz_weekly.macro_indicators import MacroSnapshot, MacroRegime

            # Risk-on regime
            mock_snapshot.return_value = MacroSnapshot(
                date="2024-01-15",
                regime=MacroRegime.RISK_ON,
            )
            mult = indicators.get_position_size_multiplier()
            assert mult > 1.0

            # Risk-off regime
            mock_snapshot.return_value = MacroSnapshot(
                date="2024-01-15",
                regime=MacroRegime.RISK_OFF,
            )
            mult = indicators.get_position_size_multiplier()
            assert mult < 1.0


# =============================================================================
# News Sentiment Tests
# =============================================================================

class TestNewsSentiment:
    """Tests for news sentiment analyzer."""

    def test_sentiment_level_enum(self):
        """Test SentimentLevel enum values."""
        from finviz_weekly.news_sentiment import SentimentLevel

        assert SentimentLevel.VERY_POSITIVE.value == 2
        assert SentimentLevel.NEUTRAL.value == 0
        assert SentimentLevel.VERY_NEGATIVE.value == -2

    def test_news_event_type_enum(self):
        """Test NewsEventType enum values."""
        from finviz_weekly.news_sentiment import NewsEventType

        assert NewsEventType.EARNINGS.value == "earnings"
        assert NewsEventType.ANALYST.value == "analyst"
        assert NewsEventType.MA.value == "m&a"

    def test_news_item_dataclass(self):
        """Test NewsItem dataclass."""
        from finviz_weekly.news_sentiment import NewsItem, SentimentLevel

        item = NewsItem(
            headline="Company beats earnings expectations",
            source="Reuters",
            published=datetime.now(),
            sentiment=SentimentLevel.POSITIVE,
            sentiment_score=0.6,
        )

        assert item.headline == "Company beats earnings expectations"
        assert item.sentiment == SentimentLevel.POSITIVE

    def test_sentiment_config_defaults(self):
        """Test SentimentConfig defaults."""
        from finviz_weekly.news_sentiment import SentimentConfig

        config = SentimentConfig()
        assert config.use_llm is True
        assert config.fallback_to_rules is True
        assert config.max_headlines_per_ticker == 20

    def test_rule_based_sentiment_positive(self):
        """Test rule-based sentiment analysis for positive headlines."""
        from finviz_weekly.news_sentiment import RuleBasedSentiment

        analyzer = RuleBasedSentiment()

        # Positive headline
        score, event_type, confidence = analyzer.analyze(
            "Stock surges after strong earnings beat"
        )
        assert score > 0
        assert confidence > 0.3

    def test_rule_based_sentiment_negative(self):
        """Test rule-based sentiment analysis for negative headlines."""
        from finviz_weekly.news_sentiment import RuleBasedSentiment

        analyzer = RuleBasedSentiment()

        # Negative headline
        score, event_type, confidence = analyzer.analyze(
            "Stock plunges on revenue miss and guidance cut"
        )
        assert score < 0

    def test_rule_based_event_detection(self):
        """Test event type detection."""
        from finviz_weekly.news_sentiment import RuleBasedSentiment, NewsEventType

        analyzer = RuleBasedSentiment()

        # Earnings event
        _, event_type, _ = analyzer.analyze("Q3 earnings exceed expectations")
        assert event_type == NewsEventType.EARNINGS

        # Analyst event
        _, event_type, _ = analyzer.analyze("Analyst upgrades stock to buy rating")
        assert event_type == NewsEventType.ANALYST

        # M&A event
        _, event_type, _ = analyzer.analyze("Company announces acquisition deal")
        assert event_type == NewsEventType.MA

    def test_news_sentiment_data_aggregation(self):
        """Test NewsSentimentData aggregation."""
        from finviz_weekly.news_sentiment import NewsSentimentData

        data = NewsSentimentData(
            ticker="AAPL",
            fetch_date="2024-01-15",
            avg_sentiment=0.3,
            positive_ratio=0.7,
            has_earnings_news=True,
        )

        assert data.avg_sentiment == 0.3
        assert data.positive_ratio == 0.7
        assert data.has_earnings_news is True

    def test_create_sentiment_features(self):
        """Test feature creation for ML."""
        from finviz_weekly.news_sentiment import NewsSentimentData, create_sentiment_features

        data = {
            "AAPL": NewsSentimentData(
                ticker="AAPL",
                fetch_date="2024-01-15",
                avg_sentiment=0.5,
                news_count_7d=10,
                combined_score=70.0,
            ),
        }

        features = create_sentiment_features(data)
        assert len(features) == 1
        assert "news_avg_sentiment" in features.columns
        assert "news_combined_score" in features.columns


# =============================================================================
# Stacking Ensemble Tests
# =============================================================================

class TestStackingEnsemble:
    """Tests for stacking ensemble meta-learner."""

    def test_base_model_config(self):
        """Test BaseModelConfig dataclass."""
        from finviz_weekly.stacking_ensemble import BaseModelConfig

        config = BaseModelConfig(
            name="xgboost_main",
            model_type="xgboost",
            weight=0.35,
            params={"max_depth": 6},
        )

        assert config.name == "xgboost_main"
        assert config.model_type == "xgboost"
        assert config.weight == 0.35

    def test_stacking_config_defaults(self):
        """Test StackingConfig defaults."""
        from finviz_weekly.stacking_ensemble import StackingConfig

        config = StackingConfig()
        assert config.meta_model_type == "ridge"
        assert config.use_cv_predictions is True
        assert config.n_folds == 5
        assert config.dynamic_weighting is True

    def test_default_ensemble_creation(self):
        """Test creating default ensemble."""
        from finviz_weekly.stacking_ensemble import create_default_ensemble

        ensemble = create_default_ensemble()

        assert ensemble.config is not None
        assert len(ensemble.config.base_models) >= 3
        assert ensemble.is_fitted is False

    def test_base_model_wrapper(self):
        """Test BaseModel wrapper class."""
        from finviz_weekly.stacking_ensemble import BaseModel, BaseModelConfig

        config = BaseModelConfig(
            name="ridge_test",
            model_type="ridge",
            params={"alpha": 1.0},
        )

        model = BaseModel(config)
        assert model.is_fitted is False
        assert model.config.model_type == "ridge"

    def test_base_model_ridge_fit_predict(self):
        """Test fitting and predicting with ridge model."""
        from finviz_weekly.stacking_ensemble import BaseModel, BaseModelConfig

        config = BaseModelConfig(
            name="ridge_test",
            model_type="ridge",
            params={"alpha": 1.0},
        )

        model = BaseModel(config)

        # Create dummy data
        X = pd.DataFrame({
            "feature1": np.random.randn(100),
            "feature2": np.random.randn(100),
        })
        y = pd.Series(np.random.randn(100))

        model.fit(X, y)
        assert model.is_fitted is True

        predictions = model.predict(X)
        assert len(predictions) == 100

    def test_stacking_ensemble_initialization(self):
        """Test StackingEnsemble initialization."""
        from finviz_weekly.stacking_ensemble import StackingEnsemble, StackingConfig

        config = StackingConfig(
            meta_model_type="ridge",
            n_folds=3,
        )

        ensemble = StackingEnsemble(config)
        assert ensemble.config.n_folds == 3
        assert ensemble.is_fitted is False

    def test_model_performance_dataclass(self):
        """Test ModelPerformance dataclass."""
        from finviz_weekly.stacking_ensemble import ModelPerformance

        perf = ModelPerformance(
            model_name="xgboost",
            period="2024-01",
            ic=0.05,
            sharpe=1.5,
            hit_rate=0.55,
            mse=0.01,
        )

        assert perf.model_name == "xgboost"
        assert perf.ic == 0.05

    def test_adaptive_ensemble_regime_weights(self):
        """Test AdaptiveStackingEnsemble regime weights."""
        from finviz_weekly.stacking_ensemble import AdaptiveStackingEnsemble

        ensemble = AdaptiveStackingEnsemble()

        assert "bull" in ensemble.regime_weights
        assert "bear" in ensemble.regime_weights
        assert "high_vol" in ensemble.regime_weights


# =============================================================================
# Integration Tests
# =============================================================================

class TestPhase12Integration:
    """Integration tests for Phase 12 modules."""

    def test_all_modules_importable(self):
        """Test that all Phase 12 modules can be imported."""
        from finviz_weekly.scrapers.institutional_ownership import InstitutionalOwnershipScraper
        from finviz_weekly.macro_indicators import MacroIndicators
        from finviz_weekly.news_sentiment import NewsSentimentScraper
        from finviz_weekly.stacking_ensemble import StackingEnsemble

        assert InstitutionalOwnershipScraper is not None
        assert MacroIndicators is not None
        assert NewsSentimentScraper is not None
        assert StackingEnsemble is not None

    def test_scrapers_package_exports(self):
        """Test scrapers package exports Phase 12 modules."""
        from finviz_weekly.scrapers import (
            InstitutionalData,
            InstitutionalConfig,
            InstitutionalOwnershipScraper,
            calculate_institutional_score,
            create_institutional_features,
        )

        assert InstitutionalData is not None
        assert InstitutionalOwnershipScraper is not None

    def test_cli_commands_registered(self):
        """Test CLI commands are registered."""
        from finviz_weekly.cli import parse_args

        # Test that new commands are in choices
        args = parse_args(["institutional", "--tickers", "AAPL"])
        assert args.command == "institutional"

        args = parse_args(["macro"])
        assert args.command == "macro"

        args = parse_args(["sentiment", "--tickers", "AAPL"])
        assert args.command == "sentiment"

        args = parse_args(["stacking"])
        assert args.command == "stacking"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
