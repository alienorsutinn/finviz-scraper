"""
LLM Research Assistant

Provides AI-powered capabilities for:
- Natural language data queries
- Stock analysis summarization
- Research report generation
- Investment thesis generation
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import pandas as pd

LOGGER = logging.getLogger(__name__)


class LLMProvider(Enum):
    """Supported LLM providers."""
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    LOCAL = "local"


@dataclass
class LLMConfig:
    """Configuration for LLM assistant."""
    provider: LLMProvider = LLMProvider.OPENAI
    model: str = "gpt-4o-mini"
    temperature: float = 0.3
    max_tokens: int = 2000

    # API keys (from environment)
    openai_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None

    # Local model settings
    local_model_path: Optional[str] = None

    @classmethod
    def from_env(cls) -> 'LLMConfig':
        """Create config from environment."""
        provider = LLMProvider.OPENAI
        if os.getenv('ANTHROPIC_API_KEY'):
            provider = LLMProvider.ANTHROPIC

        return cls(
            provider=provider,
            model=os.getenv('LLM_MODEL', 'gpt-4o-mini'),
            openai_api_key=os.getenv('OPENAI_API_KEY'),
            anthropic_api_key=os.getenv('ANTHROPIC_API_KEY'),
        )


@dataclass
class QueryResult:
    """Result from a query."""
    query: str
    response: str
    data_used: Optional[pd.DataFrame] = None
    confidence: float = 0.0
    sources: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ResearchReport:
    """Generated research report."""
    ticker: str
    title: str
    summary: str
    thesis: str
    bull_case: str
    bear_case: str
    key_metrics: Dict[str, Any]
    risks: List[str]
    catalysts: List[str]
    recommendation: str
    target_price: Optional[float] = None
    generated_at: datetime = field(default_factory=datetime.now)

    def to_markdown(self) -> str:
        """Convert to markdown format."""
        md = f"""# {self.title}

## Summary
{self.summary}

## Investment Thesis
{self.thesis}

## Bull Case
{self.bull_case}

## Bear Case
{self.bear_case}

## Key Metrics
"""
        for key, value in self.key_metrics.items():
            md += f"- **{key}**: {value}\n"

        md += "\n## Risks\n"
        for risk in self.risks:
            md += f"- {risk}\n"

        md += "\n## Catalysts\n"
        for catalyst in self.catalysts:
            md += f"- {catalyst}\n"

        md += f"""
## Recommendation
**{self.recommendation}**
"""
        if self.target_price:
            md += f"Target Price: ${self.target_price:.2f}\n"

        return md


# =============================================================================
# LLM Client
# =============================================================================

class LLMClient:
    """Client for LLM API calls."""

    def __init__(self, config: Optional[LLMConfig] = None):
        self.config = config or LLMConfig.from_env()

    def _call_openai(self, messages: List[Dict[str, str]]) -> str:
        """Call OpenAI API."""
        try:
            import openai

            client = openai.OpenAI(api_key=self.config.openai_api_key)

            response = client.chat.completions.create(
                model=self.config.model,
                messages=messages,
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens,
            )

            return response.choices[0].message.content

        except ImportError:
            raise ImportError("openai not installed. Install with: pip install openai")

    def _call_anthropic(self, messages: List[Dict[str, str]]) -> str:
        """Call Anthropic API."""
        try:
            import anthropic

            client = anthropic.Anthropic(api_key=self.config.anthropic_api_key)

            # Convert messages format
            system_msg = ""
            user_msgs = []
            for msg in messages:
                if msg["role"] == "system":
                    system_msg = msg["content"]
                else:
                    user_msgs.append(msg)

            response = client.messages.create(
                model=self.config.model or "claude-3-haiku-20240307",
                max_tokens=self.config.max_tokens,
                system=system_msg,
                messages=user_msgs,
            )

            return response.content[0].text

        except ImportError:
            raise ImportError("anthropic not installed. Install with: pip install anthropic")

    def complete(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
    ) -> str:
        """
        Get completion from LLM.

        Args:
            prompt: User prompt
            system_prompt: Optional system prompt

        Returns:
            LLM response
        """
        messages = []

        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})

        messages.append({"role": "user", "content": prompt})

        if self.config.provider == LLMProvider.OPENAI:
            return self._call_openai(messages)
        elif self.config.provider == LLMProvider.ANTHROPIC:
            return self._call_anthropic(messages)
        else:
            raise ValueError(f"Unsupported provider: {self.config.provider}")


# =============================================================================
# Research Assistant
# =============================================================================

class ResearchAssistant:
    """AI-powered research assistant."""

    def __init__(self, config: Optional[LLMConfig] = None, data_dir: str = "data"):
        self.config = config or LLMConfig.from_env()
        self.client = LLMClient(self.config)
        self.data_dir = data_dir
        self._data_cache: Optional[pd.DataFrame] = None

    def _load_data(self) -> pd.DataFrame:
        """Load fundamentals data."""
        if self._data_cache is not None:
            return self._data_cache

        from pathlib import Path

        data_path = Path(self.data_dir) / "latest" / "finviz_fundamentals.parquet"

        if data_path.exists():
            self._data_cache = pd.read_parquet(data_path)
        else:
            self._data_cache = pd.DataFrame()

        return self._data_cache

    def query(self, question: str) -> QueryResult:
        """
        Answer a natural language question about the data.

        Args:
            question: User's question

        Returns:
            QueryResult with answer
        """
        data = self._load_data()

        # Build context from data
        context = self._build_data_context(data, question)

        system_prompt = """You are a financial research assistant with access to stock screening data.
Answer questions based on the provided data context. Be specific and cite numbers when relevant.
If you don't have enough information to answer, say so clearly."""

        prompt = f"""Data Context:
{context}

Question: {question}

Please provide a clear, data-driven answer."""

        try:
            response = self.client.complete(prompt, system_prompt)

            return QueryResult(
                query=question,
                response=response,
                confidence=0.8,
                sources=["finviz_fundamentals"],
            )

        except Exception as e:
            LOGGER.error(f"Query failed: {e}")
            return QueryResult(
                query=question,
                response=f"Error processing query: {str(e)}",
                confidence=0.0,
            )

    def _build_data_context(
        self,
        data: pd.DataFrame,
        question: str,
    ) -> str:
        """Build relevant data context for the question."""
        if data.empty:
            return "No data available."

        # Basic stats
        context_parts = [
            f"Total stocks: {len(data)}",
        ]

        # Sector breakdown
        if 'sector' in data.columns:
            sectors = data['sector'].value_counts().head(5).to_dict()
            context_parts.append(f"Top sectors: {sectors}")

        # Score distribution
        if 'total_score' in data.columns:
            context_parts.append(
                f"Score stats - Mean: {data['total_score'].mean():.1f}, "
                f"Max: {data['total_score'].max():.1f}, "
                f"Min: {data['total_score'].min():.1f}"
            )

        # Top stocks
        if 'total_score' in data.columns and 'ticker' in data.columns:
            top_5 = data.nlargest(5, 'total_score')[['ticker', 'total_score', 'sector']].to_dict('records')
            context_parts.append(f"Top 5 stocks: {top_5}")

        # Check for specific ticker mention
        question_upper = question.upper()
        for ticker in data.get('ticker', []):
            if ticker in question_upper:
                ticker_data = data[data['ticker'] == ticker].iloc[0].to_dict()
                # Filter out NaN values
                ticker_data = {k: v for k, v in ticker_data.items() if pd.notna(v)}
                context_parts.append(f"Data for {ticker}: {ticker_data}")
                break

        return "\n".join(context_parts)

    def analyze_ticker(self, ticker: str) -> Dict[str, Any]:
        """
        Get detailed analysis for a ticker.

        Args:
            ticker: Stock ticker

        Returns:
            Analysis dict
        """
        data = self._load_data()

        if data.empty or 'ticker' not in data.columns:
            return {"error": "No data available"}

        ticker_data = data[data['ticker'] == ticker.upper()]

        if ticker_data.empty:
            return {"error": f"Ticker {ticker} not found"}

        row = ticker_data.iloc[0]

        # Build analysis
        analysis = {
            "ticker": ticker.upper(),
            "company": row.get('company', 'N/A'),
            "sector": row.get('sector', 'N/A'),
            "industry": row.get('industry', 'N/A'),
        }

        # Scores
        score_cols = [c for c in row.index if 'score' in c.lower()]
        analysis['scores'] = {col: row[col] for col in score_cols if pd.notna(row[col])}

        # Valuation
        val_cols = ['pe', 'forward_pe', 'peg', 'ps', 'pb']
        analysis['valuation'] = {col: row[col] for col in val_cols if col in row and pd.notna(row[col])}

        # Growth
        growth_cols = [c for c in row.index if 'growth' in c.lower()]
        analysis['growth'] = {col: row[col] for col in growth_cols if pd.notna(row[col])}

        # Quality
        quality_cols = ['roe', 'roa', 'profit_margin', 'operating_margin']
        analysis['quality'] = {col: row[col] for col in quality_cols if col in row and pd.notna(row[col])}

        return analysis

    def generate_report(self, ticker: str) -> ResearchReport:
        """
        Generate a research report for a ticker.

        Args:
            ticker: Stock ticker

        Returns:
            ResearchReport
        """
        analysis = self.analyze_ticker(ticker)

        if "error" in analysis:
            return ResearchReport(
                ticker=ticker,
                title=f"Report for {ticker}",
                summary=analysis["error"],
                thesis="N/A",
                bull_case="N/A",
                bear_case="N/A",
                key_metrics={},
                risks=[],
                catalysts=[],
                recommendation="N/A",
            )

        # Use LLM to generate report
        prompt = f"""Generate a professional investment research report for {ticker}.

Company: {analysis.get('company', 'N/A')}
Sector: {analysis.get('sector', 'N/A')}
Industry: {analysis.get('industry', 'N/A')}

Scores: {json.dumps(analysis.get('scores', {}), indent=2)}
Valuation: {json.dumps(analysis.get('valuation', {}), indent=2)}
Growth: {json.dumps(analysis.get('growth', {}), indent=2)}
Quality: {json.dumps(analysis.get('quality', {}), indent=2)}

Please provide:
1. A 2-3 sentence summary
2. Investment thesis (1 paragraph)
3. Bull case (key upside drivers)
4. Bear case (key risks)
5. Top 3 risks
6. Top 3 catalysts
7. Recommendation (Strong Buy/Buy/Hold/Sell/Strong Sell)

Format as JSON with keys: summary, thesis, bull_case, bear_case, risks, catalysts, recommendation"""

        system_prompt = "You are a senior equity research analyst. Provide objective, data-driven analysis."

        try:
            response = self.client.complete(prompt, system_prompt)

            # Parse response
            try:
                # Try to extract JSON from response
                import re
                json_match = re.search(r'\{.*\}', response, re.DOTALL)
                if json_match:
                    report_data = json.loads(json_match.group())
                else:
                    report_data = {}
            except json.JSONDecodeError:
                report_data = {
                    "summary": response[:500],
                    "thesis": "See summary",
                    "bull_case": "N/A",
                    "bear_case": "N/A",
                    "risks": [],
                    "catalysts": [],
                    "recommendation": "Hold",
                }

            return ResearchReport(
                ticker=ticker.upper(),
                title=f"Research Report: {ticker.upper()} - {analysis.get('company', 'N/A')}",
                summary=report_data.get("summary", ""),
                thesis=report_data.get("thesis", ""),
                bull_case=report_data.get("bull_case", ""),
                bear_case=report_data.get("bear_case", ""),
                key_metrics=analysis.get("scores", {}),
                risks=report_data.get("risks", []),
                catalysts=report_data.get("catalysts", []),
                recommendation=report_data.get("recommendation", "Hold"),
            )

        except Exception as e:
            LOGGER.error(f"Report generation failed: {e}")
            return ResearchReport(
                ticker=ticker,
                title=f"Report for {ticker}",
                summary=f"Error generating report: {str(e)}",
                thesis="N/A",
                bull_case="N/A",
                bear_case="N/A",
                key_metrics=analysis.get("scores", {}),
                risks=[],
                catalysts=[],
                recommendation="N/A",
            )

    def compare_stocks(self, tickers: List[str]) -> str:
        """
        Compare multiple stocks.

        Args:
            tickers: List of tickers to compare

        Returns:
            Comparison summary
        """
        data = self._load_data()

        if data.empty:
            return "No data available."

        comparisons = []
        for ticker in tickers:
            analysis = self.analyze_ticker(ticker)
            if "error" not in analysis:
                comparisons.append(analysis)

        if not comparisons:
            return "None of the tickers found in data."

        prompt = f"""Compare these stocks and provide a recommendation on which is the best investment:

{json.dumps(comparisons, indent=2)}

Provide:
1. Key differences in valuation
2. Key differences in quality/growth
3. Risk comparison
4. Top pick and reasoning"""

        system_prompt = "You are a senior equity research analyst comparing investment opportunities."

        try:
            return self.client.complete(prompt, system_prompt)
        except Exception as e:
            return f"Comparison failed: {str(e)}"

    def screen_stocks(self, criteria: str) -> List[str]:
        """
        Screen stocks based on natural language criteria.

        Args:
            criteria: Natural language screening criteria

        Returns:
            List of matching tickers
        """
        data = self._load_data()

        if data.empty:
            return []

        # Build data summary
        columns = list(data.columns)
        sample = data.head(3).to_dict('records')

        prompt = f"""Given this stock screening data with columns: {columns}

Sample data: {json.dumps(sample, indent=2)}

User criteria: {criteria}

Please provide the Python pandas query expression to filter stocks matching this criteria.
Only return the filter expression, nothing else. Example: "pe < 20 and roe > 15"
"""

        try:
            query_expr = self.client.complete(prompt).strip()

            # Clean up response
            query_expr = query_expr.replace('```python', '').replace('```', '').strip()

            # Try to apply filter
            try:
                filtered = data.query(query_expr)
                return filtered['ticker'].tolist() if 'ticker' in filtered.columns else []
            except Exception as e:
                LOGGER.warning(f"Filter failed: {e}")
                return []

        except Exception as e:
            LOGGER.error(f"Screening failed: {e}")
            return []


# =============================================================================
# Convenience Functions
# =============================================================================

def query_data(question: str, data_dir: str = "data") -> str:
    """Query data with natural language."""
    assistant = ResearchAssistant(data_dir=data_dir)
    result = assistant.query(question)
    return result.response


def analyze_stock(ticker: str, data_dir: str = "data") -> Dict[str, Any]:
    """Get analysis for a stock."""
    assistant = ResearchAssistant(data_dir=data_dir)
    return assistant.analyze_ticker(ticker)


def generate_report(ticker: str, data_dir: str = "data") -> str:
    """Generate markdown report for a stock."""
    assistant = ResearchAssistant(data_dir=data_dir)
    report = assistant.generate_report(ticker)
    return report.to_markdown()
