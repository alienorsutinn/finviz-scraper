"""Data quality monitoring and validation for scraped data."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

LOGGER = logging.getLogger(__name__)


@dataclass
class QualityMetrics:
    """Data quality metrics for a dataset."""

    timestamp: datetime
    total_rows: int
    total_columns: int
    completeness: float  # % of non-null values
    duplicate_rows: int
    outliers_detected: int
    validation_errors: List[str]
    field_completeness: Dict[str, float]  # % complete per field
    passed: bool


class DataQualityChecker:
    """Check data quality and detect anomalies."""

    def __init__(self, config: Optional[dict] = None):
        """
        Initialize quality checker.

        Args:
            config: Optional configuration with thresholds
        """
        self.config = config or {}
        self.min_rows = self.config.get("min_rows", 1500)
        self.min_completeness = self.config.get("min_completeness", 0.7)
        self.critical_fields = self.config.get(
            "critical_fields",
            ["ticker", "market_cap", "price", "sector", "industry"],
        )

    def check_fundamentals(self, df: pd.DataFrame) -> QualityMetrics:
        """
        Check quality of fundamentals dataframe.

        Args:
            df: Fundamentals dataframe to check

        Returns:
            QualityMetrics with validation results
        """
        errors = []
        timestamp = datetime.now()

        # Check row count
        if len(df) < self.min_rows:
            errors.append(f"Too few rows: {len(df)} < {self.min_rows} (likely blocked)")

        # Check for duplicates
        duplicates = df.duplicated(subset=["ticker"]).sum() if "ticker" in df.columns else 0
        if duplicates > 0:
            errors.append(f"Found {duplicates} duplicate tickers")

        # Check completeness
        total_values = df.size
        non_null_values = df.count().sum()
        completeness = non_null_values / total_values if total_values > 0 else 0

        if completeness < self.min_completeness:
            errors.append(f"Low completeness: {completeness:.1%} < {self.min_completeness:.1%}")

        # Check critical fields
        field_completeness = {}
        for field in self.critical_fields:
            if field in df.columns:
                field_complete = df[field].notna().mean()
                field_completeness[field] = field_complete
                if field_complete < 0.9:
                    errors.append(f"Field '{field}' only {field_complete:.1%} complete")
            else:
                errors.append(f"Missing critical field: '{field}'")
                field_completeness[field] = 0.0

        # Check for outliers
        outliers = self._detect_outliers(df)

        passed = len(errors) == 0

        return QualityMetrics(
            timestamp=timestamp,
            total_rows=len(df),
            total_columns=len(df.columns),
            completeness=completeness,
            duplicate_rows=duplicates,
            outliers_detected=outliers,
            validation_errors=errors,
            field_completeness=field_completeness,
            passed=passed,
        )

    def _detect_outliers(self, df: pd.DataFrame) -> int:
        """Detect outliers in numeric fields."""
        outliers = 0

        # Check P/E ratio outliers
        if "pe" in df.columns:
            pe = pd.to_numeric(df["pe"], errors="coerce")
            extreme_pe = ((pe > 1000) | (pe < -1000)).sum()
            outliers += extreme_pe

        # Check market cap outliers (negative or impossibly large)
        if "market_cap" in df.columns:
            mcap = pd.to_numeric(df["market_cap"], errors="coerce")
            bad_mcap = (mcap < 0).sum()
            outliers += bad_mcap

        # Check price outliers
        if "price" in df.columns:
            price = pd.to_numeric(df["price"], errors="coerce")
            bad_price = ((price <= 0) | (price > 1000000)).sum()
            outliers += bad_price

        return outliers

    def check_scored(self, df: pd.DataFrame) -> QualityMetrics:
        """
        Check quality of scored dataframe.

        Args:
            df: Scored dataframe to check

        Returns:
            QualityMetrics with validation results
        """
        errors = []
        timestamp = datetime.now()

        # Check scoring fields exist
        score_fields = ["total_score", "quality_score", "value_score"]
        for field in score_fields:
            if field not in df.columns:
                errors.append(f"Missing score field: '{field}'")

        # Check score ranges
        if "total_score" in df.columns:
            invalid_scores = ((df["total_score"] < 0) | (df["total_score"] > 100)).sum()
            if invalid_scores > 0:
                errors.append(f"{invalid_scores} scores outside 0-100 range")

        # Check zones
        if "zone" in df.columns:
            valid_zones = {"ADD", "HOLD", "REDUCE", "SELL"}
            invalid_zones = ~df["zone"].isin(valid_zones)
            if invalid_zones.any():
                errors.append(f"{invalid_zones.sum()} rows with invalid zones")

        completeness = df.notna().sum().sum() / df.size if df.size > 0 else 0
        field_completeness = {col: df[col].notna().mean() for col in df.columns}

        passed = len(errors) == 0

        return QualityMetrics(
            timestamp=timestamp,
            total_rows=len(df),
            total_columns=len(df.columns),
            completeness=completeness,
            duplicate_rows=0,
            outliers_detected=0,
            validation_errors=errors,
            field_completeness=field_completeness,
            passed=passed,
        )

    def save_report(self, metrics: QualityMetrics, output_path: Path) -> None:
        """Save quality report to file."""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        report = []
        report.append("# Data Quality Report\n")
        report.append(f"**Timestamp:** {metrics.timestamp.isoformat()}\n")
        report.append(f"**Status:** {'✅ PASSED' if metrics.passed else '❌ FAILED'}\n")
        report.append("\n## Summary\n")
        report.append(f"- **Total Rows:** {metrics.total_rows:,}\n")
        report.append(f"- **Total Columns:** {metrics.total_columns}\n")
        report.append(f"- **Completeness:** {metrics.completeness:.1%}\n")
        report.append(f"- **Duplicate Rows:** {metrics.duplicate_rows}\n")
        report.append(f"- **Outliers Detected:** {metrics.outliers_detected}\n")

        if metrics.validation_errors:
            report.append("\n## Validation Errors\n")
            for error in metrics.validation_errors:
                report.append(f"- ❌ {error}\n")

        if metrics.field_completeness:
            report.append("\n## Field Completeness\n")
            report.append("| Field | Completeness |\n")
            report.append("|-------|-------------|\n")
            for field, pct in sorted(metrics.field_completeness.items(), key=lambda x: x[1]):
                status = "✅" if pct >= 0.9 else "⚠️" if pct >= 0.7 else "❌"
                report.append(f"| {field} | {status} {pct:.1%} |\n")

        output_path.write_text("".join(report))
        LOGGER.info(f"Quality report saved to {output_path}")


def check_latest_data(data_dir: Path = Path("data/latest")) -> bool:
    """
    Check quality of latest scraped data.

    Args:
        data_dir: Directory containing latest data

    Returns:
        True if quality checks passed, False otherwise
    """
    checker = DataQualityChecker()

    # Check fundamentals
    fundamentals_path = data_dir / "finviz_fundamentals.parquet"
    if not fundamentals_path.exists():
        LOGGER.error(f"Fundamentals file not found: {fundamentals_path}")
        return False

    df_fund = pd.read_parquet(fundamentals_path)
    metrics_fund = checker.check_fundamentals(df_fund)

    LOGGER.info(f"Fundamentals quality: {metrics_fund.completeness:.1%} complete, {len(df_fund):,} rows")

    if metrics_fund.validation_errors:
        for error in metrics_fund.validation_errors:
            LOGGER.error(f"Quality issue: {error}")

    # Save report
    report_path = data_dir / "quality_report.md"
    checker.save_report(metrics_fund, report_path)

    # Check scored data if exists
    scored_path = data_dir / "finviz_scored.parquet"
    if scored_path.exists():
        df_scored = pd.read_parquet(scored_path)
        metrics_scored = checker.check_scored(df_scored)

        if not metrics_scored.passed:
            LOGGER.error("Scored data quality check failed")
            for error in metrics_scored.validation_errors:
                LOGGER.error(f"Scoring issue: {error}")

    return metrics_fund.passed


if __name__ == "__main__":
    import sys

    # Run quality check on latest data
    passed = check_latest_data()
    sys.exit(0 if passed else 1)
