"""
Custom Factor Builder for user-defined scoring factors.

Provides:
- Expression-based factor definitions
- Factor validation and testing
- Factor persistence and management
- Composite factor creation
- Factor performance analysis
"""
from __future__ import annotations

import json
import logging
import operator
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)

# Safe operators for expression evaluation
SAFE_OPERATORS = {
    "+": operator.add,
    "-": operator.sub,
    "*": operator.mul,
    "/": operator.truediv,
    "**": operator.pow,
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
}

# Safe functions for expression evaluation
SAFE_FUNCTIONS = {
    "abs": np.abs,
    "log": np.log,
    "log10": np.log10,
    "sqrt": np.sqrt,
    "exp": np.exp,
    "min": np.minimum,
    "max": np.maximum,
    "clip": np.clip,
    "sign": np.sign,
    "rank": lambda x: x.rank(pct=True),
    "zscore": lambda x: (x - x.mean()) / x.std(),
    "winsorize": lambda x, lower=0.01, upper=0.99: x.clip(x.quantile(lower), x.quantile(upper)),
    "percentile": lambda x: x.rank(pct=True) * 100,
    "normalize": lambda x: (x - x.min()) / (x.max() - x.min()),
}


@dataclass
class FactorDefinition:
    """Definition of a custom factor."""

    name: str  # Unique factor name
    expression: str  # Mathematical expression
    description: str = ""

    # Metadata
    created_at: str = ""
    updated_at: str = ""
    author: str = ""

    # Factor properties
    higher_is_better: bool = True  # True if higher values are better
    category: str = "custom"  # Factor category (value, growth, quality, etc.)
    weight: float = 1.0  # Default weight in composite scores

    # Validation
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        if not self.updated_at:
            self.updated_at = self.created_at

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "expression": self.expression,
            "description": self.description,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "author": self.author,
            "higher_is_better": self.higher_is_better,
            "category": self.category,
            "weight": self.weight,
            "min_value": self.min_value,
            "max_value": self.max_value,
        }

    @classmethod
    def from_dict(cls, data: dict) -> FactorDefinition:
        """Create from dictionary."""
        return cls(
            name=data["name"],
            expression=data["expression"],
            description=data.get("description", ""),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
            author=data.get("author", ""),
            higher_is_better=data.get("higher_is_better", True),
            category=data.get("category", "custom"),
            weight=data.get("weight", 1.0),
            min_value=data.get("min_value"),
            max_value=data.get("max_value"),
        )


@dataclass
class FactorTestResult:
    """Result of testing a custom factor."""

    factor_name: str
    expression: str
    test_date: str

    # Computation results
    success: bool
    error_message: Optional[str] = None

    # Statistics
    sample_size: int = 0
    mean_value: float = 0.0
    std_value: float = 0.0
    min_value: float = 0.0
    max_value: float = 0.0
    null_count: int = 0
    null_pct: float = 0.0

    # Sample values
    sample_values: List[Dict[str, Any]] = field(default_factory=list)

    # Predictive power (if returns available)
    ic: Optional[float] = None  # Information coefficient with forward returns

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "factor_name": self.factor_name,
            "expression": self.expression,
            "test_date": self.test_date,
            "success": self.success,
            "error_message": self.error_message,
            "statistics": {
                "sample_size": self.sample_size,
                "mean": self.mean_value,
                "std": self.std_value,
                "min": self.min_value,
                "max": self.max_value,
                "null_count": self.null_count,
                "null_pct": self.null_pct,
            },
            "sample_values": self.sample_values,
            "ic": self.ic,
        }


class ExpressionParser:
    """
    Parse and evaluate factor expressions safely.

    Supports:
    - Column references: {column_name}
    - Arithmetic: +, -, *, /, **
    - Comparisons: >, <, >=, <=, ==, !=
    - Functions: abs, log, sqrt, rank, zscore, etc.
    - Conditional: if_else(condition, true_value, false_value)
    """

    def __init__(self):
        self.operators = SAFE_OPERATORS
        self.functions = SAFE_FUNCTIONS.copy()

        # Add if_else function
        self.functions["if_else"] = self._if_else

    def _if_else(self, condition: pd.Series, true_val: Any, false_val: Any) -> pd.Series:
        """Conditional expression."""
        result = pd.Series(index=condition.index, dtype=float)
        result[condition] = true_val if not isinstance(true_val, pd.Series) else true_val[condition]
        result[~condition] = false_val if not isinstance(false_val, pd.Series) else false_val[~condition]
        return result

    def parse(self, expression: str, data: pd.DataFrame) -> pd.Series:
        """
        Parse and evaluate an expression.

        Args:
            expression: Factor expression string
            data: DataFrame with input columns

        Returns:
            Series with computed factor values
        """
        # Replace column references with actual data
        processed_expr = expression

        # Find all column references {column_name}
        column_refs = re.findall(r"\{(\w+)\}", expression)

        # Create a safe namespace
        namespace = {
            "np": np,
            "pd": pd,
        }

        # Add functions
        namespace.update(self.functions)

        # Add data columns
        for col in column_refs:
            if col in data.columns:
                namespace[col] = data[col]
                processed_expr = processed_expr.replace(f"{{{col}}}", col)
            else:
                raise ValueError(f"Column '{col}' not found in data")

        # Evaluate expression
        try:
            result = eval(processed_expr, {"__builtins__": {}}, namespace)

            # Ensure result is a Series
            if isinstance(result, (int, float)):
                result = pd.Series([result] * len(data), index=data.index)
            elif not isinstance(result, pd.Series):
                result = pd.Series(result, index=data.index)

            return result

        except Exception as e:
            raise ValueError(f"Error evaluating expression: {e}")

    def validate_expression(self, expression: str, available_columns: List[str]) -> Tuple[bool, str]:
        """
        Validate an expression without evaluating it.

        Args:
            expression: Expression to validate
            available_columns: List of available column names

        Returns:
            (is_valid, error_message)
        """
        # Check for column references
        column_refs = re.findall(r"\{(\w+)\}", expression)

        for col in column_refs:
            if col not in available_columns:
                return False, f"Unknown column: {col}"

        # Check for unsafe patterns
        unsafe_patterns = [
            r"__",  # Dunder methods
            r"import",
            r"exec",
            r"eval",
            r"open",
            r"file",
            r"os\.",
            r"sys\.",
            r"subprocess",
        ]

        for pattern in unsafe_patterns:
            if re.search(pattern, expression, re.IGNORECASE):
                return False, f"Unsafe pattern detected: {pattern}"

        return True, ""


class CustomFactorBuilder:
    """
    Build and manage custom factors.

    Provides tools for:
    - Creating new factors from expressions
    - Testing factors on sample data
    - Saving and loading factor definitions
    - Computing factor values
    """

    def __init__(self, factors_path: Optional[Path] = None):
        """
        Initialize factor builder.

        Args:
            factors_path: Path to store factor definitions
        """
        self.factors_path = factors_path or Path("config/custom_factors.json")
        self.parser = ExpressionParser()
        self.factors: Dict[str, FactorDefinition] = {}

        # Load existing factors
        self._load_factors()

        LOGGER.info(f"Custom factor builder initialized. {len(self.factors)} factors loaded.")

    def _load_factors(self) -> None:
        """Load factors from disk."""
        if self.factors_path.exists():
            with open(self.factors_path) as f:
                data = json.load(f)
                self.factors = {
                    name: FactorDefinition.from_dict(factor_data)
                    for name, factor_data in data.items()
                }

    def _save_factors(self) -> None:
        """Save factors to disk."""
        self.factors_path.parent.mkdir(parents=True, exist_ok=True)

        data = {name: factor.to_dict() for name, factor in self.factors.items()}

        with open(self.factors_path, "w") as f:
            json.dump(data, f, indent=2)

    def create_factor(
        self,
        name: str,
        expression: str,
        description: str = "",
        higher_is_better: bool = True,
        category: str = "custom",
        weight: float = 1.0,
        author: str = "",
    ) -> FactorDefinition:
        """
        Create a new custom factor.

        Args:
            name: Unique factor name
            expression: Mathematical expression using {column_name} references
            description: Human-readable description
            higher_is_better: Whether higher values are better
            category: Factor category
            weight: Default weight
            author: Creator name

        Returns:
            Created FactorDefinition
        """
        if name in self.factors:
            raise ValueError(f"Factor '{name}' already exists. Use update_factor() to modify.")

        factor = FactorDefinition(
            name=name,
            expression=expression,
            description=description,
            higher_is_better=higher_is_better,
            category=category,
            weight=weight,
            author=author,
        )

        self.factors[name] = factor
        self._save_factors()

        LOGGER.info(f"Created factor: {name}")

        return factor

    def update_factor(
        self,
        name: str,
        expression: Optional[str] = None,
        description: Optional[str] = None,
        higher_is_better: Optional[bool] = None,
        weight: Optional[float] = None,
    ) -> FactorDefinition:
        """
        Update an existing factor.

        Args:
            name: Factor name to update
            expression: New expression (optional)
            description: New description (optional)
            higher_is_better: New value (optional)
            weight: New weight (optional)

        Returns:
            Updated FactorDefinition
        """
        if name not in self.factors:
            raise ValueError(f"Factor '{name}' not found")

        factor = self.factors[name]

        if expression is not None:
            factor.expression = expression
        if description is not None:
            factor.description = description
        if higher_is_better is not None:
            factor.higher_is_better = higher_is_better
        if weight is not None:
            factor.weight = weight

        factor.updated_at = datetime.now().isoformat()

        self._save_factors()

        LOGGER.info(f"Updated factor: {name}")

        return factor

    def delete_factor(self, name: str) -> None:
        """Delete a factor."""
        if name not in self.factors:
            raise ValueError(f"Factor '{name}' not found")

        del self.factors[name]
        self._save_factors()

        LOGGER.info(f"Deleted factor: {name}")

    def get_factor(self, name: str) -> FactorDefinition:
        """Get a factor by name."""
        if name not in self.factors:
            raise ValueError(f"Factor '{name}' not found")
        return self.factors[name]

    def list_factors(self, category: Optional[str] = None) -> List[FactorDefinition]:
        """List all factors, optionally filtered by category."""
        factors = list(self.factors.values())

        if category:
            factors = [f for f in factors if f.category == category]

        return factors

    def test_factor(
        self,
        factor_name: str,
        data: pd.DataFrame,
        forward_returns: Optional[pd.Series] = None,
        sample_size: int = 10,
    ) -> FactorTestResult:
        """
        Test a factor on sample data.

        Args:
            factor_name: Name of factor to test
            data: DataFrame with input data
            forward_returns: Optional forward returns for IC calculation
            sample_size: Number of sample values to include

        Returns:
            FactorTestResult with test results
        """
        if factor_name not in self.factors:
            raise ValueError(f"Factor '{factor_name}' not found")

        factor = self.factors[factor_name]

        try:
            # Compute factor values
            values = self.parser.parse(factor.expression, data)

            # Calculate statistics
            valid_values = values.dropna()

            result = FactorTestResult(
                factor_name=factor_name,
                expression=factor.expression,
                test_date=datetime.now().isoformat(),
                success=True,
                sample_size=len(valid_values),
                mean_value=float(valid_values.mean()),
                std_value=float(valid_values.std()),
                min_value=float(valid_values.min()),
                max_value=float(valid_values.max()),
                null_count=int(values.isna().sum()),
                null_pct=float(values.isna().mean() * 100),
            )

            # Add sample values
            if "ticker" in data.columns:
                sample_df = data[["ticker"]].copy()
                sample_df["factor_value"] = values
                sample_df = sample_df.dropna().head(sample_size)
                result.sample_values = sample_df.to_dict("records")

            # Calculate IC if returns provided
            if forward_returns is not None:
                common_idx = values.index.intersection(forward_returns.index)
                if len(common_idx) >= 20:
                    try:
                        from scipy.stats import spearmanr
                        corr, _ = spearmanr(
                            values.loc[common_idx].fillna(0),
                            forward_returns.loc[common_idx].fillna(0),
                        )
                        result.ic = float(corr) if not np.isnan(corr) else None
                    except ImportError:
                        result.ic = float(values.loc[common_idx].corr(forward_returns.loc[common_idx]))

        except Exception as e:
            result = FactorTestResult(
                factor_name=factor_name,
                expression=factor.expression,
                test_date=datetime.now().isoformat(),
                success=False,
                error_message=str(e),
            )

        return result

    def compute_factor(
        self,
        factor_name: str,
        data: pd.DataFrame,
    ) -> pd.Series:
        """
        Compute factor values for data.

        Args:
            factor_name: Factor name
            data: Input DataFrame

        Returns:
            Series with factor values
        """
        if factor_name not in self.factors:
            raise ValueError(f"Factor '{factor_name}' not found")

        factor = self.factors[factor_name]
        return self.parser.parse(factor.expression, data)

    def compute_all_factors(
        self,
        data: pd.DataFrame,
        categories: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Compute all custom factors for data.

        Args:
            data: Input DataFrame
            categories: Optional list of categories to compute

        Returns:
            DataFrame with all computed factors
        """
        result = data.copy()

        for factor in self.list_factors():
            if categories and factor.category not in categories:
                continue

            try:
                result[f"custom_{factor.name}"] = self.compute_factor(factor.name, data)
            except Exception as e:
                LOGGER.warning(f"Error computing factor {factor.name}: {e}")
                result[f"custom_{factor.name}"] = np.nan

        return result

    def create_composite_factor(
        self,
        name: str,
        factor_weights: Dict[str, float],
        description: str = "",
    ) -> FactorDefinition:
        """
        Create a composite factor from weighted combination of existing factors.

        Args:
            name: Name for composite factor
            factor_weights: Dict mapping factor names to weights
            description: Description

        Returns:
            Created FactorDefinition
        """
        # Build expression
        terms = []
        total_weight = sum(abs(w) for w in factor_weights.values())

        for factor_name, weight in factor_weights.items():
            if factor_name not in self.factors:
                raise ValueError(f"Factor '{factor_name}' not found")

            # Normalize weight
            norm_weight = weight / total_weight
            factor = self.factors[factor_name]

            # Wrap factor expression in parentheses
            terms.append(f"{norm_weight} * ({factor.expression})")

        expression = " + ".join(terms)

        return self.create_factor(
            name=name,
            expression=expression,
            description=description or f"Composite of: {', '.join(factor_weights.keys())}",
            category="composite",
        )


# Pre-defined factor templates
FACTOR_TEMPLATES = {
    "peg_ratio": {
        "expression": "{pe} / {eps_growth_next_y}",
        "description": "Price/Earnings to Growth ratio - lower is better",
        "higher_is_better": False,
        "category": "value",
    },
    "earnings_yield": {
        "expression": "1 / {pe}",
        "description": "Inverse of P/E ratio - higher is better",
        "higher_is_better": True,
        "category": "value",
    },
    "garp_score": {
        "expression": "rank({score_quality}) * 0.4 + rank({score_growth}) * 0.4 + rank({score_value}) * 0.2",
        "description": "Growth at Reasonable Price - balanced quality, growth, and value",
        "higher_is_better": True,
        "category": "composite",
    },
    "momentum_quality": {
        "expression": "rank({score_momentum}) * rank({score_quality})",
        "description": "Quality stocks with positive momentum",
        "higher_is_better": True,
        "category": "composite",
    },
    "deep_value": {
        "expression": "rank({score_value}) * if_else({pe} < 15, 1.2, 1.0)",
        "description": "Deep value with P/E boost",
        "higher_is_better": True,
        "category": "value",
    },
}


def get_factor_templates() -> Dict[str, Dict]:
    """Get available factor templates."""
    return FACTOR_TEMPLATES.copy()
