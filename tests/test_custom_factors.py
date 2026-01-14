"""Tests for custom factors module."""
from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from finviz_weekly.custom_factors import (
    CustomFactorBuilder,
    ExpressionParser,
    FactorDefinition,
    FactorTestResult,
    get_factor_templates,
)


@pytest.fixture
def mock_stock_data():
    """Create mock stock data for testing."""
    np.random.seed(42)
    n_stocks = 50

    return pd.DataFrame({
        "ticker": [f"STOCK{i}" for i in range(n_stocks)],
        "pe": np.random.uniform(5, 50, n_stocks),
        "eps_growth_next_y": np.random.uniform(-0.2, 0.5, n_stocks),
        "score_quality": np.random.uniform(0, 100, n_stocks),
        "score_value": np.random.uniform(0, 100, n_stocks),
        "score_growth": np.random.uniform(0, 100, n_stocks),
        "score_momentum": np.random.uniform(0, 100, n_stocks),
        "price": np.random.uniform(10, 500, n_stocks),
        "market_cap": np.random.uniform(1e9, 1e12, n_stocks),
    })


@pytest.fixture
def temp_factors_path():
    """Create temporary path for factors file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "custom_factors.json"


class TestFactorDefinition:
    """Tests for FactorDefinition."""

    def test_create_definition(self):
        """Test creating a factor definition."""
        factor = FactorDefinition(
            name="test_factor",
            expression="{pe} / {eps_growth_next_y}",
            description="Test PEG ratio",
            higher_is_better=False,
            category="value",
        )

        assert factor.name == "test_factor"
        assert factor.expression == "{pe} / {eps_growth_next_y}"
        assert factor.higher_is_better is False
        assert factor.category == "value"
        assert factor.created_at != ""

    def test_to_dict(self):
        """Test serialization to dict."""
        factor = FactorDefinition(
            name="test_factor",
            expression="{pe} * 2",
            description="Test factor",
        )

        data = factor.to_dict()

        assert data["name"] == "test_factor"
        assert data["expression"] == "{pe} * 2"
        assert "created_at" in data

    def test_from_dict(self):
        """Test deserialization from dict."""
        data = {
            "name": "loaded_factor",
            "expression": "{score_quality} + {score_value}",
            "description": "Combined score",
            "higher_is_better": True,
            "category": "composite",
            "weight": 1.5,
        }

        factor = FactorDefinition.from_dict(data)

        assert factor.name == "loaded_factor"
        assert factor.weight == 1.5


class TestExpressionParser:
    """Tests for ExpressionParser."""

    def test_parse_simple_expression(self, mock_stock_data):
        """Test parsing simple expression."""
        parser = ExpressionParser()

        result = parser.parse("{pe} * 2", mock_stock_data)

        assert len(result) == len(mock_stock_data)
        assert (result == mock_stock_data["pe"] * 2).all()

    def test_parse_arithmetic(self, mock_stock_data):
        """Test parsing arithmetic expressions."""
        parser = ExpressionParser()

        # Addition
        result = parser.parse("{score_quality} + {score_value}", mock_stock_data)
        expected = mock_stock_data["score_quality"] + mock_stock_data["score_value"]
        assert np.allclose(result, expected)

        # Division
        result = parser.parse("{pe} / {eps_growth_next_y}", mock_stock_data)
        expected = mock_stock_data["pe"] / mock_stock_data["eps_growth_next_y"]
        assert np.allclose(result, expected, equal_nan=True)

    def test_parse_functions(self, mock_stock_data):
        """Test parsing function calls."""
        parser = ExpressionParser()

        # abs
        result = parser.parse("abs({eps_growth_next_y})", mock_stock_data)
        expected = np.abs(mock_stock_data["eps_growth_next_y"])
        assert np.allclose(result, expected)

        # log
        result = parser.parse("log({pe})", mock_stock_data)
        expected = np.log(mock_stock_data["pe"])
        assert np.allclose(result, expected)

        # sqrt
        result = parser.parse("sqrt({price})", mock_stock_data)
        expected = np.sqrt(mock_stock_data["price"])
        assert np.allclose(result, expected)

    def test_parse_rank(self, mock_stock_data):
        """Test rank function."""
        parser = ExpressionParser()

        result = parser.parse("rank({score_quality})", mock_stock_data)

        # Rank should be between 0 and 1
        assert result.min() >= 0
        assert result.max() <= 1

    def test_parse_zscore(self, mock_stock_data):
        """Test zscore function."""
        parser = ExpressionParser()

        result = parser.parse("zscore({pe})", mock_stock_data)

        # Z-score should have mean ~0 and std ~1
        assert abs(result.mean()) < 0.01
        assert abs(result.std() - 1.0) < 0.01

    def test_parse_missing_column(self, mock_stock_data):
        """Test parsing with missing column."""
        parser = ExpressionParser()

        with pytest.raises(ValueError, match="not found"):
            parser.parse("{nonexistent_column}", mock_stock_data)

    def test_validate_expression_valid(self, mock_stock_data):
        """Test expression validation with valid expression."""
        parser = ExpressionParser()
        columns = list(mock_stock_data.columns)

        is_valid, error = parser.validate_expression("{pe} + {score_quality}", columns)

        assert is_valid is True
        assert error == ""

    def test_validate_expression_missing_column(self, mock_stock_data):
        """Test expression validation with missing column."""
        parser = ExpressionParser()
        columns = list(mock_stock_data.columns)

        is_valid, error = parser.validate_expression("{nonexistent}", columns)

        assert is_valid is False
        assert "Unknown column" in error

    def test_validate_expression_unsafe(self, mock_stock_data):
        """Test expression validation with unsafe patterns."""
        parser = ExpressionParser()
        columns = list(mock_stock_data.columns)

        is_valid, error = parser.validate_expression("import os", columns)
        assert is_valid is False

        is_valid, error = parser.validate_expression("__import__('os')", columns)
        assert is_valid is False


class TestCustomFactorBuilder:
    """Tests for CustomFactorBuilder."""

    def test_init(self, temp_factors_path):
        """Test builder initialization."""
        builder = CustomFactorBuilder(factors_path=temp_factors_path)

        assert builder.factors_path == temp_factors_path
        assert len(builder.factors) == 0

    def test_create_factor(self, temp_factors_path):
        """Test creating a factor."""
        builder = CustomFactorBuilder(factors_path=temp_factors_path)

        factor = builder.create_factor(
            name="peg_ratio",
            expression="{pe} / {eps_growth_next_y}",
            description="PEG Ratio",
            higher_is_better=False,
            category="value",
        )

        assert factor.name == "peg_ratio"
        assert "peg_ratio" in builder.factors

        # Should be persisted
        builder2 = CustomFactorBuilder(factors_path=temp_factors_path)
        assert "peg_ratio" in builder2.factors

    def test_create_duplicate_factor(self, temp_factors_path):
        """Test creating duplicate factor raises error."""
        builder = CustomFactorBuilder(factors_path=temp_factors_path)

        builder.create_factor(name="test", expression="{pe}")

        with pytest.raises(ValueError, match="already exists"):
            builder.create_factor(name="test", expression="{pe} * 2")

    def test_update_factor(self, temp_factors_path):
        """Test updating a factor."""
        builder = CustomFactorBuilder(factors_path=temp_factors_path)

        builder.create_factor(name="test", expression="{pe}")
        factor = builder.update_factor(
            name="test",
            expression="{pe} * 2",
            description="Updated",
        )

        assert factor.expression == "{pe} * 2"
        assert factor.description == "Updated"

    def test_delete_factor(self, temp_factors_path):
        """Test deleting a factor."""
        builder = CustomFactorBuilder(factors_path=temp_factors_path)

        builder.create_factor(name="test", expression="{pe}")
        assert "test" in builder.factors

        builder.delete_factor("test")
        assert "test" not in builder.factors

    def test_delete_nonexistent(self, temp_factors_path):
        """Test deleting nonexistent factor."""
        builder = CustomFactorBuilder(factors_path=temp_factors_path)

        with pytest.raises(ValueError, match="not found"):
            builder.delete_factor("nonexistent")

    def test_get_factor(self, temp_factors_path):
        """Test getting a factor."""
        builder = CustomFactorBuilder(factors_path=temp_factors_path)

        builder.create_factor(name="test", expression="{pe}")
        factor = builder.get_factor("test")

        assert factor.name == "test"

    def test_list_factors(self, temp_factors_path):
        """Test listing factors."""
        builder = CustomFactorBuilder(factors_path=temp_factors_path)

        builder.create_factor(name="f1", expression="{pe}", category="value")
        builder.create_factor(name="f2", expression="{score_quality}", category="quality")
        builder.create_factor(name="f3", expression="{score_value}", category="value")

        all_factors = builder.list_factors()
        assert len(all_factors) == 3

        value_factors = builder.list_factors(category="value")
        assert len(value_factors) == 2

    def test_test_factor(self, temp_factors_path, mock_stock_data):
        """Test testing a factor."""
        builder = CustomFactorBuilder(factors_path=temp_factors_path)

        builder.create_factor(name="test", expression="{pe} * 2")
        result = builder.test_factor("test", mock_stock_data)

        assert isinstance(result, FactorTestResult)
        assert result.success is True
        assert result.sample_size > 0
        assert result.null_count >= 0

    def test_test_factor_with_error(self, temp_factors_path, mock_stock_data):
        """Test testing a factor that causes an error."""
        builder = CustomFactorBuilder(factors_path=temp_factors_path)

        builder.create_factor(name="bad", expression="{nonexistent}")

        # Need to manually add to bypass validation
        builder.factors["bad"].expression = "{nonexistent}"

        result = builder.test_factor("bad", mock_stock_data)

        assert result.success is False
        assert result.error_message is not None

    def test_compute_factor(self, temp_factors_path, mock_stock_data):
        """Test computing factor values."""
        builder = CustomFactorBuilder(factors_path=temp_factors_path)

        builder.create_factor(name="double_pe", expression="{pe} * 2")
        values = builder.compute_factor("double_pe", mock_stock_data)

        assert len(values) == len(mock_stock_data)
        assert (values == mock_stock_data["pe"] * 2).all()

    def test_compute_all_factors(self, temp_factors_path, mock_stock_data):
        """Test computing all factors."""
        builder = CustomFactorBuilder(factors_path=temp_factors_path)

        builder.create_factor(name="f1", expression="{pe} * 2")
        builder.create_factor(name="f2", expression="{score_quality} + 10")

        result = builder.compute_all_factors(mock_stock_data)

        assert "custom_f1" in result.columns
        assert "custom_f2" in result.columns

    def test_create_composite_factor(self, temp_factors_path):
        """Test creating composite factor."""
        builder = CustomFactorBuilder(factors_path=temp_factors_path)

        builder.create_factor(name="f1", expression="{pe}")
        builder.create_factor(name="f2", expression="{score_quality}")

        composite = builder.create_composite_factor(
            name="combined",
            factor_weights={"f1": 0.6, "f2": 0.4},
        )

        assert composite.name == "combined"
        assert composite.category == "composite"


class TestFactorTemplates:
    """Tests for factor templates."""

    def test_get_templates(self):
        """Test getting factor templates."""
        templates = get_factor_templates()

        assert isinstance(templates, dict)
        assert len(templates) > 0
        assert "peg_ratio" in templates
        assert "garp_score" in templates

    def test_template_structure(self):
        """Test template structure."""
        templates = get_factor_templates()

        for name, template in templates.items():
            assert "expression" in template
            assert "description" in template
            assert "higher_is_better" in template
            assert "category" in template


class TestFactorTestResult:
    """Tests for FactorTestResult."""

    def test_to_dict(self):
        """Test result serialization."""
        result = FactorTestResult(
            factor_name="test",
            expression="{pe}",
            test_date="2024-01-15",
            success=True,
            sample_size=100,
            mean_value=25.5,
            std_value=10.2,
            min_value=5.0,
            max_value=50.0,
            null_count=5,
            null_pct=5.0,
        )

        data = result.to_dict()

        assert data["factor_name"] == "test"
        assert data["success"] is True
        assert data["statistics"]["sample_size"] == 100
