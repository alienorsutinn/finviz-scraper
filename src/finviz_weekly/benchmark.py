"""
Benchmark Comparison Framework

Compare strategy performance against multiple benchmarks:
- Market indices (SPY, QQQ, IWM)
- Factor portfolios (momentum, value, quality)
- Custom benchmarks
- Rolling comparisons
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)


# =============================================================================
# Benchmark Types
# =============================================================================

class BenchmarkType(Enum):
    """Types of benchmarks."""
    INDEX = "index"
    FACTOR = "factor"
    SECTOR = "sector"
    CUSTOM = "custom"
    RISK_FREE = "risk_free"


@dataclass
class BenchmarkDefinition:
    """Definition of a benchmark."""
    name: str
    ticker: str
    benchmark_type: BenchmarkType
    description: str = ""
    weight: float = 1.0  # For composite benchmarks


# Standard benchmarks
STANDARD_BENCHMARKS = {
    'SPY': BenchmarkDefinition(
        name='S&P 500',
        ticker='SPY',
        benchmark_type=BenchmarkType.INDEX,
        description='Large cap US equities',
    ),
    'QQQ': BenchmarkDefinition(
        name='Nasdaq 100',
        ticker='QQQ',
        benchmark_type=BenchmarkType.INDEX,
        description='Large cap growth/tech',
    ),
    'IWM': BenchmarkDefinition(
        name='Russell 2000',
        ticker='IWM',
        benchmark_type=BenchmarkType.INDEX,
        description='Small cap US equities',
    ),
    'VTI': BenchmarkDefinition(
        name='Total US Market',
        ticker='VTI',
        benchmark_type=BenchmarkType.INDEX,
        description='Total US stock market',
    ),
    'EFA': BenchmarkDefinition(
        name='Developed Intl',
        ticker='EFA',
        benchmark_type=BenchmarkType.INDEX,
        description='Developed markets ex-US',
    ),
    'AGG': BenchmarkDefinition(
        name='US Aggregate Bond',
        ticker='AGG',
        benchmark_type=BenchmarkType.INDEX,
        description='US investment grade bonds',
    ),
    'MTUM': BenchmarkDefinition(
        name='Momentum Factor',
        ticker='MTUM',
        benchmark_type=BenchmarkType.FACTOR,
        description='MSCI USA Momentum Factor',
    ),
    'VLUE': BenchmarkDefinition(
        name='Value Factor',
        ticker='VLUE',
        benchmark_type=BenchmarkType.FACTOR,
        description='MSCI USA Value Factor',
    ),
    'QUAL': BenchmarkDefinition(
        name='Quality Factor',
        ticker='QUAL',
        benchmark_type=BenchmarkType.FACTOR,
        description='MSCI USA Quality Factor',
    ),
    'SIZE': BenchmarkDefinition(
        name='Size Factor',
        ticker='SIZE',
        benchmark_type=BenchmarkType.FACTOR,
        description='MSCI USA Size Factor',
    ),
    'USMV': BenchmarkDefinition(
        name='Min Volatility',
        ticker='USMV',
        benchmark_type=BenchmarkType.FACTOR,
        description='MSCI USA Min Volatility',
    ),
}


# =============================================================================
# Comparison Metrics
# =============================================================================

@dataclass
class ComparisonMetrics:
    """Metrics comparing strategy to benchmark."""
    # Absolute metrics
    strategy_return: float
    benchmark_return: float
    active_return: float  # Alpha = strategy - benchmark

    # Risk metrics
    strategy_vol: float
    benchmark_vol: float
    tracking_error: float

    # Risk-adjusted
    strategy_sharpe: float
    benchmark_sharpe: float
    information_ratio: float

    # Correlation
    correlation: float
    beta: float
    alpha: float  # Jensen's alpha

    # Relative metrics
    up_capture: float
    down_capture: float
    capture_ratio: float

    # Hit rates
    outperformance_rate: float  # % of periods strategy > benchmark
    best_relative_period: float
    worst_relative_period: float


@dataclass
class RollingComparison:
    """Rolling comparison statistics."""
    dates: pd.DatetimeIndex
    rolling_alpha: pd.Series
    rolling_beta: pd.Series
    rolling_correlation: pd.Series
    rolling_tracking_error: pd.Series
    rolling_information_ratio: pd.Series


# =============================================================================
# Benchmark Data Provider
# =============================================================================

class BenchmarkDataProvider:
    """Provides benchmark return data."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self._cache: Dict[str, pd.Series] = {}

    def get_returns(
        self,
        ticker: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.Series:
        """
        Get benchmark returns.

        Args:
            ticker: Benchmark ticker
            start_date: Start date filter
            end_date: End date filter

        Returns:
            Daily returns series
        """
        if ticker in self._cache:
            returns = self._cache[ticker]
        else:
            returns = self._fetch_returns(ticker)
            self._cache[ticker] = returns

        # Apply date filters
        if start_date:
            returns = returns[returns.index >= start_date]
        if end_date:
            returns = returns[returns.index <= end_date]

        return returns

    def _fetch_returns(self, ticker: str) -> pd.Series:
        """Fetch returns from data source."""
        try:
            import yfinance as yf

            data = yf.download(ticker, period="10y", progress=False)
            if not data.empty:
                returns = data['Adj Close'].pct_change().dropna()
                returns.name = ticker
                return returns

        except ImportError:
            LOGGER.warning("yfinance not installed, using synthetic data")

        # Return synthetic data for testing
        dates = pd.date_range(end=datetime.now(), periods=2520, freq='B')
        returns = pd.Series(
            np.random.normal(0.0004, 0.01, len(dates)),
            index=dates,
            name=ticker,
        )
        return returns

    def get_multiple(
        self,
        tickers: List[str],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """Get returns for multiple benchmarks."""
        returns_dict = {}
        for ticker in tickers:
            returns_dict[ticker] = self.get_returns(ticker, start_date, end_date)

        return pd.DataFrame(returns_dict)


# =============================================================================
# Benchmark Comparator
# =============================================================================

class BenchmarkComparator:
    """Compare strategy against benchmarks."""

    def __init__(
        self,
        data_provider: Optional[BenchmarkDataProvider] = None,
        risk_free_rate: float = 0.02,
    ):
        self.data_provider = data_provider or BenchmarkDataProvider()
        self.risk_free_rate = risk_free_rate

    def compare(
        self,
        strategy_returns: pd.Series,
        benchmark_ticker: str,
    ) -> ComparisonMetrics:
        """
        Compare strategy against a benchmark.

        Args:
            strategy_returns: Strategy daily returns
            benchmark_ticker: Benchmark ticker symbol

        Returns:
            ComparisonMetrics
        """
        # Get aligned benchmark returns
        benchmark_returns = self.data_provider.get_returns(
            benchmark_ticker,
            start_date=strategy_returns.index[0],
            end_date=strategy_returns.index[-1],
        )

        # Align indices
        aligned = pd.DataFrame({
            'strategy': strategy_returns,
            'benchmark': benchmark_returns,
        }).dropna()

        strat = aligned['strategy']
        bench = aligned['benchmark']

        # Calculate metrics
        return self._calculate_metrics(strat, bench)

    def compare_multiple(
        self,
        strategy_returns: pd.Series,
        benchmark_tickers: List[str],
    ) -> Dict[str, ComparisonMetrics]:
        """Compare strategy against multiple benchmarks."""
        results = {}
        for ticker in benchmark_tickers:
            try:
                results[ticker] = self.compare(strategy_returns, ticker)
            except Exception as e:
                LOGGER.warning(f"Failed to compare with {ticker}: {e}")

        return results

    def _calculate_metrics(
        self,
        strategy: pd.Series,
        benchmark: pd.Series,
    ) -> ComparisonMetrics:
        """Calculate all comparison metrics."""
        # Annualization factor
        ann_factor = 252

        # Total returns
        strat_total = (1 + strategy).prod() - 1
        bench_total = (1 + benchmark).prod() - 1
        active_return = strat_total - bench_total

        # Volatility
        strat_vol = strategy.std() * np.sqrt(ann_factor)
        bench_vol = benchmark.std() * np.sqrt(ann_factor)

        # Tracking error
        active_returns = strategy - benchmark
        tracking_error = active_returns.std() * np.sqrt(ann_factor)

        # Sharpe ratios
        rf_daily = self.risk_free_rate / ann_factor
        strat_sharpe = (strategy.mean() - rf_daily) / strategy.std() * np.sqrt(ann_factor) if strategy.std() > 0 else 0
        bench_sharpe = (benchmark.mean() - rf_daily) / benchmark.std() * np.sqrt(ann_factor) if benchmark.std() > 0 else 0

        # Information ratio
        info_ratio = active_returns.mean() / active_returns.std() * np.sqrt(ann_factor) if active_returns.std() > 0 else 0

        # Correlation and Beta
        correlation = strategy.corr(benchmark)
        covariance = strategy.cov(benchmark)
        bench_variance = benchmark.var()
        beta = covariance / bench_variance if bench_variance > 0 else 1

        # Jensen's Alpha (annualized)
        alpha = (strategy.mean() - (rf_daily + beta * (benchmark.mean() - rf_daily))) * ann_factor

        # Up/Down capture
        up_capture, down_capture = self._calculate_capture_ratios(strategy, benchmark)
        capture_ratio = up_capture / down_capture if down_capture != 0 else float('inf')

        # Outperformance
        outperformance = (strategy > benchmark).mean()
        relative_returns = strategy - benchmark
        best_relative = relative_returns.max()
        worst_relative = relative_returns.min()

        return ComparisonMetrics(
            strategy_return=strat_total,
            benchmark_return=bench_total,
            active_return=active_return,
            strategy_vol=strat_vol,
            benchmark_vol=bench_vol,
            tracking_error=tracking_error,
            strategy_sharpe=strat_sharpe,
            benchmark_sharpe=bench_sharpe,
            information_ratio=info_ratio,
            correlation=correlation,
            beta=beta,
            alpha=alpha,
            up_capture=up_capture,
            down_capture=down_capture,
            capture_ratio=capture_ratio,
            outperformance_rate=outperformance,
            best_relative_period=best_relative,
            worst_relative_period=worst_relative,
        )

    def _calculate_capture_ratios(
        self,
        strategy: pd.Series,
        benchmark: pd.Series,
    ) -> Tuple[float, float]:
        """Calculate up and down capture ratios."""
        up_periods = benchmark > 0
        down_periods = benchmark < 0

        if up_periods.any():
            up_capture = strategy[up_periods].mean() / benchmark[up_periods].mean()
        else:
            up_capture = 1.0

        if down_periods.any():
            down_capture = strategy[down_periods].mean() / benchmark[down_periods].mean()
        else:
            down_capture = 1.0

        return up_capture, down_capture

    def rolling_comparison(
        self,
        strategy_returns: pd.Series,
        benchmark_ticker: str,
        window: int = 252,
    ) -> RollingComparison:
        """
        Calculate rolling comparison metrics.

        Args:
            strategy_returns: Strategy returns
            benchmark_ticker: Benchmark ticker
            window: Rolling window in days

        Returns:
            RollingComparison with rolling metrics
        """
        benchmark_returns = self.data_provider.get_returns(
            benchmark_ticker,
            start_date=strategy_returns.index[0],
            end_date=strategy_returns.index[-1],
        )

        aligned = pd.DataFrame({
            'strategy': strategy_returns,
            'benchmark': benchmark_returns,
        }).dropna()

        strat = aligned['strategy']
        bench = aligned['benchmark']

        # Rolling calculations
        rolling_corr = strat.rolling(window).corr(bench)

        # Rolling beta
        rolling_cov = strat.rolling(window).cov(bench)
        rolling_var = bench.rolling(window).var()
        rolling_beta = rolling_cov / rolling_var

        # Rolling alpha
        rf_daily = self.risk_free_rate / 252
        rolling_alpha = (
            strat.rolling(window).mean() -
            (rf_daily + rolling_beta * (bench.rolling(window).mean() - rf_daily))
        ) * 252

        # Rolling tracking error
        active = strat - bench
        rolling_te = active.rolling(window).std() * np.sqrt(252)

        # Rolling IR
        rolling_ir = active.rolling(window).mean() / active.rolling(window).std() * np.sqrt(252)

        return RollingComparison(
            dates=aligned.index,
            rolling_alpha=rolling_alpha,
            rolling_beta=rolling_beta,
            rolling_correlation=rolling_corr,
            rolling_tracking_error=rolling_te,
            rolling_information_ratio=rolling_ir,
        )


# =============================================================================
# Composite Benchmark
# =============================================================================

class CompositeBenchmark:
    """Create custom composite benchmarks."""

    def __init__(
        self,
        components: Dict[str, float],
        name: str = "Custom Benchmark",
        data_provider: Optional[BenchmarkDataProvider] = None,
    ):
        """
        Initialize composite benchmark.

        Args:
            components: Dict of {ticker: weight}
            name: Name of composite benchmark
            data_provider: Data provider instance
        """
        self.components = components
        self.name = name
        self.data_provider = data_provider or BenchmarkDataProvider()

        # Normalize weights
        total_weight = sum(components.values())
        self.weights = {k: v / total_weight for k, v in components.items()}

    def get_returns(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.Series:
        """
        Calculate composite benchmark returns.

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            Composite returns series
        """
        component_returns = self.data_provider.get_multiple(
            list(self.components.keys()),
            start_date,
            end_date,
        )

        # Weight returns
        weighted_returns = pd.Series(0.0, index=component_returns.index)
        for ticker, weight in self.weights.items():
            if ticker in component_returns.columns:
                weighted_returns += component_returns[ticker].fillna(0) * weight

        weighted_returns.name = self.name
        return weighted_returns


# =============================================================================
# Benchmark Report Generator
# =============================================================================

class BenchmarkReportGenerator:
    """Generate benchmark comparison reports."""

    def __init__(self, comparator: Optional[BenchmarkComparator] = None):
        self.comparator = comparator or BenchmarkComparator()

    def generate_report(
        self,
        strategy_returns: pd.Series,
        benchmarks: List[str] = None,
        strategy_name: str = "Strategy",
    ) -> str:
        """
        Generate markdown benchmark comparison report.

        Args:
            strategy_returns: Strategy returns
            benchmarks: List of benchmark tickers
            strategy_name: Name of strategy

        Returns:
            Markdown report string
        """
        if benchmarks is None:
            benchmarks = ['SPY', 'QQQ', 'IWM']

        results = self.comparator.compare_multiple(strategy_returns, benchmarks)

        # Build report
        report = f"# Benchmark Comparison Report\n\n"
        report += f"**Strategy:** {strategy_name}\n\n"
        report += f"**Period:** {strategy_returns.index[0].strftime('%Y-%m-%d')} to "
        report += f"{strategy_returns.index[-1].strftime('%Y-%m-%d')}\n\n"

        # Summary table
        report += "## Performance Summary\n\n"
        report += "| Metric | Strategy |"
        for bm in benchmarks:
            report += f" {bm} |"
        report += "\n|--------|----------|"
        for _ in benchmarks:
            report += "------|"
        report += "\n"

        # Get strategy metrics from first comparison
        if results:
            first_result = list(results.values())[0]

            # Total Return
            report += f"| Total Return | {first_result.strategy_return*100:.1f}% |"
            for bm, metrics in results.items():
                report += f" {metrics.benchmark_return*100:.1f}% |"
            report += "\n"

            # Volatility
            report += f"| Volatility | {first_result.strategy_vol*100:.1f}% |"
            for bm, metrics in results.items():
                report += f" {metrics.benchmark_vol*100:.1f}% |"
            report += "\n"

            # Sharpe
            report += f"| Sharpe | {first_result.strategy_sharpe:.2f} |"
            for bm, metrics in results.items():
                report += f" {metrics.benchmark_sharpe:.2f} |"
            report += "\n"

        # Individual comparisons
        report += "\n## Detailed Comparisons\n\n"

        for ticker, metrics in results.items():
            bench_info = STANDARD_BENCHMARKS.get(ticker)
            bench_name = bench_info.name if bench_info else ticker

            report += f"### vs {bench_name} ({ticker})\n\n"
            report += f"- **Active Return:** {metrics.active_return*100:+.2f}%\n"
            report += f"- **Tracking Error:** {metrics.tracking_error*100:.2f}%\n"
            report += f"- **Information Ratio:** {metrics.information_ratio:.2f}\n"
            report += f"- **Beta:** {metrics.beta:.2f}\n"
            report += f"- **Alpha (annualized):** {metrics.alpha*100:+.2f}%\n"
            report += f"- **Correlation:** {metrics.correlation:.2f}\n"
            report += f"- **Up Capture:** {metrics.up_capture*100:.1f}%\n"
            report += f"- **Down Capture:** {metrics.down_capture*100:.1f}%\n"
            report += f"- **Outperformance Rate:** {metrics.outperformance_rate*100:.1f}%\n\n"

        return report

    def generate_html_section(
        self,
        strategy_returns: pd.Series,
        benchmarks: List[str] = None,
    ) -> str:
        """Generate HTML section for benchmark comparison."""
        if benchmarks is None:
            benchmarks = ['SPY', 'QQQ']

        results = self.comparator.compare_multiple(strategy_returns, benchmarks)

        html = """<div class="section">
    <h2>Benchmark Comparison</h2>
    <table class="comparison-table">
        <thead>
            <tr>
                <th>Benchmark</th>
                <th>Active Return</th>
                <th>Beta</th>
                <th>Alpha</th>
                <th>Info Ratio</th>
                <th>Correlation</th>
                <th>Up Capture</th>
                <th>Down Capture</th>
            </tr>
        </thead>
        <tbody>"""

        for ticker, metrics in results.items():
            alpha_class = "positive" if metrics.alpha > 0 else "negative"
            ar_class = "positive" if metrics.active_return > 0 else "negative"

            html += f"""<tr>
    <td><strong>{ticker}</strong></td>
    <td class="{ar_class}">{metrics.active_return*100:+.2f}%</td>
    <td>{metrics.beta:.2f}</td>
    <td class="{alpha_class}">{metrics.alpha*100:+.2f}%</td>
    <td>{metrics.information_ratio:.2f}</td>
    <td>{metrics.correlation:.2f}</td>
    <td>{metrics.up_capture*100:.1f}%</td>
    <td>{metrics.down_capture*100:.1f}%</td>
</tr>"""

        html += """</tbody></table></div>"""

        return html


# =============================================================================
# Convenience Functions
# =============================================================================

def compare_to_spy(strategy_returns: pd.Series) -> ComparisonMetrics:
    """Quick comparison to S&P 500."""
    comparator = BenchmarkComparator()
    return comparator.compare(strategy_returns, 'SPY')


def compare_to_benchmarks(
    strategy_returns: pd.Series,
    benchmarks: List[str] = None,
) -> Dict[str, ComparisonMetrics]:
    """Compare to multiple benchmarks."""
    if benchmarks is None:
        benchmarks = ['SPY', 'QQQ', 'IWM']

    comparator = BenchmarkComparator()
    return comparator.compare_multiple(strategy_returns, benchmarks)


def generate_benchmark_report(
    strategy_returns: pd.Series,
    benchmarks: List[str] = None,
    strategy_name: str = "Strategy",
) -> str:
    """Generate markdown benchmark report."""
    generator = BenchmarkReportGenerator()
    return generator.generate_report(strategy_returns, benchmarks, strategy_name)


def create_60_40_benchmark() -> CompositeBenchmark:
    """Create a standard 60/40 stock/bond benchmark."""
    return CompositeBenchmark(
        components={'SPY': 0.60, 'AGG': 0.40},
        name='60/40 Portfolio',
    )


def create_factor_benchmark() -> CompositeBenchmark:
    """Create an equal-weight factor benchmark."""
    return CompositeBenchmark(
        components={
            'MTUM': 0.25,  # Momentum
            'VLUE': 0.25,  # Value
            'QUAL': 0.25,  # Quality
            'USMV': 0.25,  # Low volatility
        },
        name='Factor Composite',
    )
