"""
HTML Backtest Report Generator

Generates professional, interactive HTML reports for backtest results including:
- Performance charts
- Drawdown analysis
- Trade analysis
- Risk metrics
- Monthly returns heatmap
"""

from __future__ import annotations

import base64
import io
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)


# =============================================================================
# Report Data Structures
# =============================================================================

@dataclass
class TradeRecord:
    """Individual trade record."""
    entry_date: datetime
    exit_date: datetime
    ticker: str
    side: str  # 'long' or 'short'
    entry_price: float
    exit_price: float
    quantity: float
    pnl: float
    pnl_pct: float
    holding_days: int
    exit_reason: str = ""


@dataclass
class BacktestMetrics:
    """Comprehensive backtest metrics."""
    # Returns
    total_return: float
    cagr: float
    annual_volatility: float
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float

    # Drawdown
    max_drawdown: float
    max_drawdown_duration: int
    avg_drawdown: float
    avg_drawdown_duration: int

    # Risk
    var_95: float
    cvar_95: float
    skewness: float
    kurtosis: float

    # Trading
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    avg_win: float
    avg_loss: float
    profit_factor: float
    avg_trade: float
    best_trade: float
    worst_trade: float

    # Exposure
    avg_exposure: float
    max_exposure: float

    # Time
    start_date: datetime = field(default_factory=datetime.now)
    end_date: datetime = field(default_factory=datetime.now)
    trading_days: int = 0


@dataclass
class BacktestResult:
    """Complete backtest result."""
    strategy_name: str
    metrics: BacktestMetrics
    equity_curve: pd.Series
    drawdown_series: pd.Series
    trades: List[TradeRecord]
    monthly_returns: pd.DataFrame
    benchmark_equity: Optional[pd.Series] = None
    parameters: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# Metrics Calculator
# =============================================================================

class MetricsCalculator:
    """Calculate backtest metrics from returns."""

    @staticmethod
    def calculate_metrics(
        returns: pd.Series,
        trades: List[TradeRecord],
        risk_free_rate: float = 0.02,
    ) -> BacktestMetrics:
        """Calculate comprehensive metrics."""
        # Basic returns
        total_return = (1 + returns).prod() - 1
        trading_days = len(returns)
        years = trading_days / 252

        cagr = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0
        annual_vol = returns.std() * np.sqrt(252)

        # Risk-adjusted
        excess_returns = returns - risk_free_rate / 252
        sharpe = excess_returns.mean() / returns.std() * np.sqrt(252) if returns.std() > 0 else 0

        downside_returns = returns[returns < 0]
        downside_std = downside_returns.std() * np.sqrt(252) if len(downside_returns) > 0 else 0.001
        sortino = (cagr - risk_free_rate) / downside_std if downside_std > 0 else 0

        # Drawdown
        equity = (1 + returns).cumprod()
        rolling_max = equity.expanding().max()
        drawdown = equity / rolling_max - 1

        max_dd = drawdown.min()
        calmar = cagr / abs(max_dd) if max_dd != 0 else 0

        # Drawdown duration
        dd_periods = []
        in_dd = False
        dd_start = 0
        for i, dd in enumerate(drawdown):
            if dd < 0 and not in_dd:
                in_dd = True
                dd_start = i
            elif dd >= 0 and in_dd:
                in_dd = False
                dd_periods.append(i - dd_start)

        max_dd_duration = max(dd_periods) if dd_periods else 0
        avg_dd_duration = np.mean(dd_periods) if dd_periods else 0
        avg_dd = drawdown[drawdown < 0].mean() if (drawdown < 0).any() else 0

        # VaR/CVaR
        var_95 = np.percentile(returns, 5)
        cvar_95 = returns[returns <= var_95].mean() if (returns <= var_95).any() else var_95

        # Higher moments
        skewness = returns.skew()
        kurtosis = returns.kurtosis()

        # Trade statistics
        total_trades = len(trades)
        winning = [t for t in trades if t.pnl > 0]
        losing = [t for t in trades if t.pnl <= 0]

        win_rate = len(winning) / total_trades if total_trades > 0 else 0
        avg_win = np.mean([t.pnl_pct for t in winning]) if winning else 0
        avg_loss = np.mean([t.pnl_pct for t in losing]) if losing else 0

        gross_profit = sum(t.pnl for t in winning)
        gross_loss = abs(sum(t.pnl for t in losing))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

        avg_trade = np.mean([t.pnl_pct for t in trades]) if trades else 0
        best_trade = max([t.pnl_pct for t in trades]) if trades else 0
        worst_trade = min([t.pnl_pct for t in trades]) if trades else 0

        return BacktestMetrics(
            total_return=total_return,
            cagr=cagr,
            annual_volatility=annual_vol,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            calmar_ratio=calmar,
            max_drawdown=max_dd,
            max_drawdown_duration=max_dd_duration,
            avg_drawdown=avg_dd,
            avg_drawdown_duration=int(avg_dd_duration),
            var_95=var_95,
            cvar_95=cvar_95,
            skewness=skewness,
            kurtosis=kurtosis,
            total_trades=total_trades,
            winning_trades=len(winning),
            losing_trades=len(losing),
            win_rate=win_rate,
            avg_win=avg_win,
            avg_loss=avg_loss,
            profit_factor=profit_factor,
            avg_trade=avg_trade,
            best_trade=best_trade,
            worst_trade=worst_trade,
            avg_exposure=0.8,  # Placeholder
            max_exposure=1.0,
            start_date=returns.index[0] if len(returns) > 0 else datetime.now(),
            end_date=returns.index[-1] if len(returns) > 0 else datetime.now(),
            trading_days=trading_days,
        )


# =============================================================================
# HTML Report Generator
# =============================================================================

class HTMLReportGenerator:
    """Generate interactive HTML backtest reports."""

    def __init__(self, include_plotly: bool = True):
        self.include_plotly = include_plotly

    def generate(
        self,
        result: BacktestResult,
        output_path: Optional[str] = None,
    ) -> str:
        """
        Generate HTML report.

        Args:
            result: Backtest result
            output_path: Optional path to save HTML file

        Returns:
            HTML string
        """
        html = self._build_html(result)

        if output_path:
            Path(output_path).write_text(html)
            LOGGER.info(f"Report saved to {output_path}")

        return html

    def _build_html(self, result: BacktestResult) -> str:
        """Build complete HTML document."""
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Backtest Report: {result.strategy_name}</title>
    {self._get_styles()}
    {self._get_plotly_cdn() if self.include_plotly else ''}
</head>
<body>
    <div class="container">
        {self._build_header(result)}
        {self._build_summary_cards(result)}
        {self._build_equity_chart(result)}
        {self._build_drawdown_chart(result)}
        {self._build_monthly_returns(result)}
        {self._build_trade_analysis(result)}
        {self._build_risk_metrics(result)}
        {self._build_trade_list(result)}
        {self._build_footer()}
    </div>
    {self._get_scripts()}
</body>
</html>"""

    def _get_styles(self) -> str:
        """Get CSS styles."""
        return """<style>
:root {
    --primary: #2563eb;
    --success: #16a34a;
    --danger: #dc2626;
    --warning: #d97706;
    --bg: #f8fafc;
    --card-bg: #ffffff;
    --text: #1e293b;
    --text-muted: #64748b;
    --border: #e2e8f0;
}

* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--bg);
    color: var(--text);
    line-height: 1.6;
}

.container {
    max-width: 1400px;
    margin: 0 auto;
    padding: 2rem;
}

.header {
    background: linear-gradient(135deg, var(--primary), #1d4ed8);
    color: white;
    padding: 2rem;
    border-radius: 12px;
    margin-bottom: 2rem;
}

.header h1 {
    font-size: 2rem;
    margin-bottom: 0.5rem;
}

.header .subtitle {
    opacity: 0.9;
    font-size: 1rem;
}

.cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
    margin-bottom: 2rem;
}

.card {
    background: var(--card-bg);
    border-radius: 8px;
    padding: 1.5rem;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}

.card .label {
    font-size: 0.85rem;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

.card .value {
    font-size: 1.75rem;
    font-weight: 600;
    margin-top: 0.25rem;
}

.card .value.positive { color: var(--success); }
.card .value.negative { color: var(--danger); }

.section {
    background: var(--card-bg);
    border-radius: 8px;
    padding: 1.5rem;
    margin-bottom: 2rem;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}

.section h2 {
    font-size: 1.25rem;
    margin-bottom: 1rem;
    padding-bottom: 0.5rem;
    border-bottom: 2px solid var(--border);
}

.chart-container {
    height: 400px;
    margin-top: 1rem;
}

.metrics-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 1rem;
}

.metric {
    text-align: center;
    padding: 1rem;
    background: var(--bg);
    border-radius: 6px;
}

.metric .label {
    font-size: 0.8rem;
    color: var(--text-muted);
}

.metric .value {
    font-size: 1.25rem;
    font-weight: 600;
}

.heatmap {
    overflow-x: auto;
}

.heatmap table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9rem;
}

.heatmap th, .heatmap td {
    padding: 0.5rem;
    text-align: center;
    border: 1px solid var(--border);
}

.heatmap th {
    background: var(--bg);
    font-weight: 600;
}

.heatmap .positive { background: rgba(22, 163, 74, var(--intensity)); }
.heatmap .negative { background: rgba(220, 38, 38, var(--intensity)); }

.trade-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.85rem;
}

.trade-table th, .trade-table td {
    padding: 0.75rem;
    text-align: left;
    border-bottom: 1px solid var(--border);
}

.trade-table th {
    background: var(--bg);
    font-weight: 600;
}

.trade-table tr:hover {
    background: var(--bg);
}

.badge {
    display: inline-block;
    padding: 0.25rem 0.5rem;
    border-radius: 4px;
    font-size: 0.75rem;
    font-weight: 500;
}

.badge.long { background: #dbeafe; color: #1d4ed8; }
.badge.short { background: #fce7f3; color: #be185d; }

.footer {
    text-align: center;
    color: var(--text-muted);
    font-size: 0.85rem;
    margin-top: 2rem;
    padding-top: 1rem;
    border-top: 1px solid var(--border);
}

@media (max-width: 768px) {
    .container { padding: 1rem; }
    .cards { grid-template-columns: repeat(2, 1fr); }
}
</style>"""

    def _get_plotly_cdn(self) -> str:
        """Get Plotly CDN link."""
        return '<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>'

    def _build_header(self, result: BacktestResult) -> str:
        """Build header section."""
        m = result.metrics
        period = f"{m.start_date.strftime('%Y-%m-%d')} to {m.end_date.strftime('%Y-%m-%d')}"

        return f"""<div class="header">
    <h1>{result.strategy_name}</h1>
    <div class="subtitle">Backtest Report | {period} | {m.trading_days} Trading Days</div>
</div>"""

    def _build_summary_cards(self, result: BacktestResult) -> str:
        """Build summary metric cards."""
        m = result.metrics

        def fmt_pct(v: float, sign: bool = True) -> str:
            s = "+" if v > 0 and sign else ""
            return f"{s}{v*100:.2f}%"

        def get_class(v: float) -> str:
            return "positive" if v > 0 else "negative" if v < 0 else ""

        cards = [
            ("Total Return", fmt_pct(m.total_return), get_class(m.total_return)),
            ("CAGR", fmt_pct(m.cagr), get_class(m.cagr)),
            ("Sharpe Ratio", f"{m.sharpe_ratio:.2f}", get_class(m.sharpe_ratio)),
            ("Max Drawdown", fmt_pct(m.max_drawdown), "negative"),
            ("Win Rate", f"{m.win_rate*100:.1f}%", get_class(m.win_rate - 0.5)),
            ("Profit Factor", f"{m.profit_factor:.2f}", get_class(m.profit_factor - 1)),
            ("Total Trades", str(m.total_trades), ""),
            ("Sortino Ratio", f"{m.sortino_ratio:.2f}", get_class(m.sortino_ratio)),
        ]

        html = '<div class="cards">'
        for label, value, cls in cards:
            html += f"""<div class="card">
    <div class="label">{label}</div>
    <div class="value {cls}">{value}</div>
</div>"""
        html += '</div>'

        return html

    def _build_equity_chart(self, result: BacktestResult) -> str:
        """Build equity curve chart."""
        if not self.include_plotly:
            return ""

        equity = result.equity_curve
        dates = [d.strftime('%Y-%m-%d') for d in equity.index]
        values = equity.tolist()

        benchmark_trace = ""
        if result.benchmark_equity is not None:
            bench_values = result.benchmark_equity.tolist()
            benchmark_trace = f"""{{
                x: {dates},
                y: {bench_values},
                name: 'Benchmark',
                type: 'scatter',
                line: {{color: '#94a3b8', dash: 'dash'}}
            }},"""

        return f"""<div class="section">
    <h2>Equity Curve</h2>
    <div id="equity-chart" class="chart-container"></div>
    <script>
        Plotly.newPlot('equity-chart', [
            {benchmark_trace}
            {{
                x: {dates},
                y: {values},
                name: 'Strategy',
                type: 'scatter',
                fill: 'tozeroy',
                line: {{color: '#2563eb'}}
            }}
        ], {{
            margin: {{t: 20, r: 20, b: 40, l: 60}},
            xaxis: {{title: ''}},
            yaxis: {{title: 'Portfolio Value', tickformat: '$,.0f'}},
            hovermode: 'x unified',
            legend: {{orientation: 'h', y: 1.1}}
        }}, {{responsive: true}});
    </script>
</div>"""

    def _build_drawdown_chart(self, result: BacktestResult) -> str:
        """Build drawdown chart."""
        if not self.include_plotly:
            return ""

        dd = result.drawdown_series
        dates = [d.strftime('%Y-%m-%d') for d in dd.index]
        values = (dd * 100).tolist()

        return f"""<div class="section">
    <h2>Drawdown</h2>
    <div id="drawdown-chart" class="chart-container"></div>
    <script>
        Plotly.newPlot('drawdown-chart', [{{
            x: {dates},
            y: {values},
            type: 'scatter',
            fill: 'tozeroy',
            line: {{color: '#dc2626'}},
            fillcolor: 'rgba(220, 38, 38, 0.2)'
        }}], {{
            margin: {{t: 20, r: 20, b: 40, l: 60}},
            xaxis: {{title: ''}},
            yaxis: {{title: 'Drawdown %', ticksuffix: '%'}},
            hovermode: 'x unified'
        }}, {{responsive: true}});
    </script>
</div>"""

    def _build_monthly_returns(self, result: BacktestResult) -> str:
        """Build monthly returns heatmap."""
        df = result.monthly_returns

        if df.empty:
            return ""

        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        html = """<div class="section">
    <h2>Monthly Returns</h2>
    <div class="heatmap">
        <table>
            <tr><th>Year</th>"""

        for m in months:
            html += f"<th>{m}</th>"
        html += "<th>Year</th></tr>"

        for year in df.index:
            html += f"<tr><td><strong>{year}</strong></td>"
            year_total = 0
            for month in range(1, 13):
                if month in df.columns:
                    val = df.loc[year, month]
                    if pd.notna(val):
                        year_total += val
                        intensity = min(abs(val) * 5, 0.8)
                        cls = "positive" if val > 0 else "negative"
                        html += f'<td class="{cls}" style="--intensity: {intensity}">{val*100:.1f}%</td>'
                    else:
                        html += "<td>-</td>"
                else:
                    html += "<td>-</td>"

            intensity = min(abs(year_total) * 2, 0.8)
            cls = "positive" if year_total > 0 else "negative"
            html += f'<td class="{cls}" style="--intensity: {intensity}"><strong>{year_total*100:.1f}%</strong></td>'
            html += "</tr>"

        html += "</table></div></div>"

        return html

    def _build_trade_analysis(self, result: BacktestResult) -> str:
        """Build trade analysis section."""
        m = result.metrics

        return f"""<div class="section">
    <h2>Trade Analysis</h2>
    <div class="metrics-grid">
        <div class="metric">
            <div class="label">Total Trades</div>
            <div class="value">{m.total_trades}</div>
        </div>
        <div class="metric">
            <div class="label">Winning Trades</div>
            <div class="value" style="color: var(--success)">{m.winning_trades}</div>
        </div>
        <div class="metric">
            <div class="label">Losing Trades</div>
            <div class="value" style="color: var(--danger)">{m.losing_trades}</div>
        </div>
        <div class="metric">
            <div class="label">Win Rate</div>
            <div class="value">{m.win_rate*100:.1f}%</div>
        </div>
        <div class="metric">
            <div class="label">Avg Win</div>
            <div class="value" style="color: var(--success)">+{m.avg_win*100:.2f}%</div>
        </div>
        <div class="metric">
            <div class="label">Avg Loss</div>
            <div class="value" style="color: var(--danger)">{m.avg_loss*100:.2f}%</div>
        </div>
        <div class="metric">
            <div class="label">Best Trade</div>
            <div class="value" style="color: var(--success)">+{m.best_trade*100:.2f}%</div>
        </div>
        <div class="metric">
            <div class="label">Worst Trade</div>
            <div class="value" style="color: var(--danger)">{m.worst_trade*100:.2f}%</div>
        </div>
        <div class="metric">
            <div class="label">Profit Factor</div>
            <div class="value">{m.profit_factor:.2f}</div>
        </div>
        <div class="metric">
            <div class="label">Avg Trade</div>
            <div class="value">{m.avg_trade*100:.2f}%</div>
        </div>
    </div>
</div>"""

    def _build_risk_metrics(self, result: BacktestResult) -> str:
        """Build risk metrics section."""
        m = result.metrics

        return f"""<div class="section">
    <h2>Risk Metrics</h2>
    <div class="metrics-grid">
        <div class="metric">
            <div class="label">Annual Volatility</div>
            <div class="value">{m.annual_volatility*100:.1f}%</div>
        </div>
        <div class="metric">
            <div class="label">Max Drawdown</div>
            <div class="value" style="color: var(--danger)">{m.max_drawdown*100:.1f}%</div>
        </div>
        <div class="metric">
            <div class="label">Max DD Duration</div>
            <div class="value">{m.max_drawdown_duration} days</div>
        </div>
        <div class="metric">
            <div class="label">Avg Drawdown</div>
            <div class="value">{m.avg_drawdown*100:.1f}%</div>
        </div>
        <div class="metric">
            <div class="label">VaR (95%)</div>
            <div class="value" style="color: var(--danger)">{m.var_95*100:.2f}%</div>
        </div>
        <div class="metric">
            <div class="label">CVaR (95%)</div>
            <div class="value" style="color: var(--danger)">{m.cvar_95*100:.2f}%</div>
        </div>
        <div class="metric">
            <div class="label">Sharpe Ratio</div>
            <div class="value">{m.sharpe_ratio:.2f}</div>
        </div>
        <div class="metric">
            <div class="label">Sortino Ratio</div>
            <div class="value">{m.sortino_ratio:.2f}</div>
        </div>
        <div class="metric">
            <div class="label">Calmar Ratio</div>
            <div class="value">{m.calmar_ratio:.2f}</div>
        </div>
        <div class="metric">
            <div class="label">Skewness</div>
            <div class="value">{m.skewness:.2f}</div>
        </div>
        <div class="metric">
            <div class="label">Kurtosis</div>
            <div class="value">{m.kurtosis:.2f}</div>
        </div>
    </div>
</div>"""

    def _build_trade_list(self, result: BacktestResult, max_trades: int = 50) -> str:
        """Build trade list table."""
        trades = result.trades[:max_trades]

        if not trades:
            return ""

        html = """<div class="section">
    <h2>Recent Trades</h2>
    <table class="trade-table">
        <thead>
            <tr>
                <th>Entry Date</th>
                <th>Exit Date</th>
                <th>Ticker</th>
                <th>Side</th>
                <th>Entry</th>
                <th>Exit</th>
                <th>P&L</th>
                <th>P&L %</th>
                <th>Days</th>
            </tr>
        </thead>
        <tbody>"""

        for t in trades:
            pnl_class = "positive" if t.pnl > 0 else "negative"
            pnl_sign = "+" if t.pnl > 0 else ""

            html += f"""<tr>
    <td>{t.entry_date.strftime('%Y-%m-%d')}</td>
    <td>{t.exit_date.strftime('%Y-%m-%d')}</td>
    <td><strong>{t.ticker}</strong></td>
    <td><span class="badge {t.side}">{t.side.upper()}</span></td>
    <td>${t.entry_price:.2f}</td>
    <td>${t.exit_price:.2f}</td>
    <td class="{pnl_class}">{pnl_sign}${t.pnl:,.2f}</td>
    <td class="{pnl_class}">{pnl_sign}{t.pnl_pct*100:.2f}%</td>
    <td>{t.holding_days}</td>
</tr>"""

        html += "</tbody></table></div>"

        if len(result.trades) > max_trades:
            html = html.replace("</div>", f"<p style='margin-top: 1rem; color: var(--text-muted);'>Showing {max_trades} of {len(result.trades)} trades</p></div>")

        return html

    def _build_footer(self) -> str:
        """Build footer section."""
        return f"""<div class="footer">
    <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Finviz Weekly Backtest Report</p>
</div>"""

    def _get_scripts(self) -> str:
        """Get JavaScript for interactivity."""
        return """<script>
// Add any additional interactivity here
document.querySelectorAll('.trade-table tbody tr').forEach(row => {
    row.style.cursor = 'pointer';
    row.addEventListener('click', function() {
        this.classList.toggle('selected');
    });
});
</script>"""


# =============================================================================
# Report Utilities
# =============================================================================

def calculate_monthly_returns(equity_curve: pd.Series) -> pd.DataFrame:
    """Calculate monthly returns from equity curve."""
    returns = equity_curve.pct_change().dropna()

    # Group by year and month
    monthly = returns.groupby([
        returns.index.year,
        returns.index.month
    ]).apply(lambda x: (1 + x).prod() - 1)

    # Pivot to year x month format
    if len(monthly) > 0:
        df = monthly.unstack(level=1)
        df.index.name = 'Year'
        return df

    return pd.DataFrame()


def generate_backtest_report(
    strategy_name: str,
    returns: pd.Series,
    trades: List[TradeRecord],
    initial_capital: float = 100000,
    benchmark_returns: Optional[pd.Series] = None,
    parameters: Optional[Dict[str, Any]] = None,
    output_path: Optional[str] = None,
) -> str:
    """
    Generate a complete backtest report.

    Args:
        strategy_name: Name of the strategy
        returns: Daily returns series
        trades: List of trade records
        initial_capital: Starting capital
        benchmark_returns: Optional benchmark returns
        parameters: Strategy parameters
        output_path: Path to save HTML file

    Returns:
        HTML report string
    """
    # Calculate metrics
    metrics = MetricsCalculator.calculate_metrics(returns, trades)

    # Build equity curve
    equity = initial_capital * (1 + returns).cumprod()

    # Calculate drawdown
    rolling_max = equity.expanding().max()
    drawdown = equity / rolling_max - 1

    # Monthly returns
    monthly = calculate_monthly_returns(equity)

    # Benchmark equity
    benchmark_equity = None
    if benchmark_returns is not None:
        benchmark_equity = initial_capital * (1 + benchmark_returns).cumprod()

    # Create result
    result = BacktestResult(
        strategy_name=strategy_name,
        metrics=metrics,
        equity_curve=equity,
        drawdown_series=drawdown,
        trades=trades,
        monthly_returns=monthly,
        benchmark_equity=benchmark_equity,
        parameters=parameters or {},
    )

    # Generate report
    generator = HTMLReportGenerator()
    return generator.generate(result, output_path)


# =============================================================================
# Example Usage
# =============================================================================

def create_sample_report(output_path: str = "backtest_report.html") -> str:
    """Create a sample backtest report for demonstration."""
    import numpy as np

    # Generate sample data
    np.random.seed(42)
    dates = pd.date_range('2020-01-01', '2023-12-31', freq='B')
    returns = pd.Series(
        np.random.normal(0.0005, 0.02, len(dates)),
        index=dates
    )

    # Sample trades
    trades = []
    for i in range(30):
        entry_idx = np.random.randint(0, len(dates) - 20)
        exit_idx = entry_idx + np.random.randint(5, 20)
        pnl_pct = np.random.normal(0.02, 0.05)

        trades.append(TradeRecord(
            entry_date=dates[entry_idx],
            exit_date=dates[exit_idx],
            ticker=['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'NVDA'][i % 5],
            side='long',
            entry_price=100 + np.random.uniform(-20, 20),
            exit_price=100 * (1 + pnl_pct),
            quantity=100,
            pnl=10000 * pnl_pct,
            pnl_pct=pnl_pct,
            holding_days=exit_idx - entry_idx,
        ))

    # Benchmark
    benchmark = pd.Series(
        np.random.normal(0.0003, 0.015, len(dates)),
        index=dates
    )

    return generate_backtest_report(
        strategy_name="Momentum Factor Strategy",
        returns=returns,
        trades=trades,
        benchmark_returns=benchmark,
        parameters={'lookback': 12, 'top_n': 10, 'rebalance': 'monthly'},
        output_path=output_path,
    )
