"""
News Sentiment Analyzer

Analyzes news headlines and articles for sentiment signals:
- LLM-based sentiment analysis using Claude API
- Headline parsing and scoring
- News momentum (sentiment trend over time)
- Event detection (earnings, M&A, analyst actions)
"""

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from enum import Enum
import time
import json

import pandas as pd

logger = logging.getLogger(__name__)


class SentimentLevel(Enum):
    """Sentiment classification levels."""
    VERY_NEGATIVE = -2
    NEGATIVE = -1
    NEUTRAL = 0
    POSITIVE = 1
    VERY_POSITIVE = 2


class NewsEventType(Enum):
    """Types of news events."""
    EARNINGS = "earnings"
    GUIDANCE = "guidance"
    ANALYST = "analyst"
    INSIDER = "insider"
    MA = "m&a"  # Mergers & Acquisitions
    REGULATORY = "regulatory"
    PRODUCT = "product"
    MANAGEMENT = "management"
    LEGAL = "legal"
    GENERAL = "general"


@dataclass
class NewsItem:
    """Single news item."""
    headline: str
    source: str
    published: datetime
    url: Optional[str] = None

    # Analysis results
    sentiment: SentimentLevel = SentimentLevel.NEUTRAL
    sentiment_score: float = 0.0  # -1.0 to 1.0
    event_type: NewsEventType = NewsEventType.GENERAL
    confidence: float = 0.5
    key_entities: List[str] = field(default_factory=list)
    summary: Optional[str] = None


@dataclass
class NewsSentimentData:
    """Aggregated news sentiment for a ticker."""
    ticker: str
    fetch_date: str

    # News items
    news_items: List[NewsItem] = field(default_factory=list)
    news_count_24h: int = 0
    news_count_7d: int = 0

    # Sentiment aggregates
    avg_sentiment: float = 0.0  # -1.0 to 1.0
    sentiment_std: float = 0.0  # Volatility of sentiment
    positive_ratio: float = 0.5  # % of positive news
    negative_ratio: float = 0.5  # % of negative news

    # Momentum
    sentiment_momentum: float = 0.0  # Change in sentiment over time
    news_volume_trend: str = "stable"  # increasing, decreasing, stable

    # Events
    has_earnings_news: bool = False
    has_analyst_news: bool = False
    has_ma_news: bool = False

    # Scores
    sentiment_score: float = 50.0  # 0-100 normalized score
    momentum_score: float = 50.0
    combined_score: float = 50.0


@dataclass
class SentimentConfig:
    """Configuration for sentiment analysis."""
    use_llm: bool = True
    llm_model: str = "claude-3-haiku-20240307"
    fallback_to_rules: bool = True
    max_headlines_per_ticker: int = 20
    lookback_days: int = 7
    request_delay: float = 0.5
    cache_hours: int = 6


class RuleBasedSentiment:
    """Rule-based sentiment analysis fallback."""

    POSITIVE_KEYWORDS = {
        'high': ['surge', 'soar', 'jump', 'rally', 'beat', 'exceed', 'upgrade',
                 'buy', 'outperform', 'breakthrough', 'record', 'strong', 'growth',
                 'profit', 'gain', 'rise', 'boost', 'bullish', 'optimistic'],
        'medium': ['increase', 'improve', 'positive', 'upside', 'higher', 'advance',
                   'recover', 'expand', 'opportunity', 'momentum', 'confident']
    }

    NEGATIVE_KEYWORDS = {
        'high': ['crash', 'plunge', 'collapse', 'miss', 'downgrade', 'sell',
                 'underperform', 'loss', 'decline', 'tumble', 'warning', 'cut',
                 'bearish', 'concern', 'risk', 'lawsuit', 'investigation'],
        'medium': ['decrease', 'lower', 'negative', 'downside', 'weak', 'fall',
                   'drop', 'slow', 'challenge', 'pressure', 'uncertain']
    }

    EVENT_PATTERNS = {
        NewsEventType.EARNINGS: r'\b(earnings|eps|revenue|quarter|q[1-4]|fiscal|profit|income)\b',
        NewsEventType.GUIDANCE: r'\b(guidance|outlook|forecast|expect|raise|lower|cut)\b',
        NewsEventType.ANALYST: r'\b(analyst|rating|upgrade|downgrade|price target|buy|sell|hold)\b',
        NewsEventType.INSIDER: r'\b(insider|ceo|cfo|executive|director|bought|sold|purchase)\b',
        NewsEventType.MA: r'\b(acquire|acquisition|merger|deal|buyout|takeover)\b',
        NewsEventType.REGULATORY: r'\b(fda|sec|regulatory|approval|investigation|fine)\b',
        NewsEventType.PRODUCT: r'\b(launch|product|release|announce|unveil|introduce)\b',
        NewsEventType.MANAGEMENT: r'\b(appoint|resign|hire|fire|ceo|cfo|departure)\b',
        NewsEventType.LEGAL: r'\b(lawsuit|sue|legal|court|settlement|verdict)\b',
    }

    def analyze(self, headline: str) -> Tuple[float, NewsEventType, float]:
        """Analyze headline using rules. Returns (score, event_type, confidence)."""
        headline_lower = headline.lower()

        # Calculate sentiment score
        score = 0.0
        matches = 0

        for word in self.POSITIVE_KEYWORDS['high']:
            if word in headline_lower:
                score += 0.4
                matches += 1
        for word in self.POSITIVE_KEYWORDS['medium']:
            if word in headline_lower:
                score += 0.2
                matches += 1
        for word in self.NEGATIVE_KEYWORDS['high']:
            if word in headline_lower:
                score -= 0.4
                matches += 1
        for word in self.NEGATIVE_KEYWORDS['medium']:
            if word in headline_lower:
                score -= 0.2
                matches += 1

        # Clamp score
        score = max(-1.0, min(1.0, score))

        # Detect event type
        event_type = NewsEventType.GENERAL
        for evt_type, pattern in self.EVENT_PATTERNS.items():
            if re.search(pattern, headline_lower):
                event_type = evt_type
                break

        # Confidence based on matches
        confidence = min(0.9, 0.3 + matches * 0.15)

        return score, event_type, confidence


class LLMSentimentAnalyzer:
    """LLM-based sentiment analysis using Claude API."""

    SYSTEM_PROMPT = """You are a financial news sentiment analyzer. Analyze the given stock news headline and provide:
1. sentiment_score: A number from -1.0 (very negative) to 1.0 (very positive)
2. event_type: One of [earnings, guidance, analyst, insider, m&a, regulatory, product, management, legal, general]
3. confidence: A number from 0.0 to 1.0 indicating your confidence
4. key_entities: List of important entities mentioned
5. brief_summary: One sentence summary of the news impact

Respond in JSON format only."""

    def __init__(self, api_key: Optional[str] = None, model: str = "claude-3-haiku-20240307"):
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        self.model = model
        self._client = None

    def _get_client(self):
        """Lazy-load Anthropic client."""
        if self._client is None:
            try:
                import anthropic
                self._client = anthropic.Anthropic(api_key=self.api_key)
            except ImportError:
                logger.warning("anthropic package not installed. Install with: pip install anthropic")
                return None
            except Exception as e:
                logger.warning(f"Could not initialize Anthropic client: {e}")
                return None
        return self._client

    def analyze(self, headline: str, ticker: str) -> Optional[Dict]:
        """Analyze headline using Claude API."""
        client = self._get_client()
        if not client:
            return None

        try:
            message = client.messages.create(
                model=self.model,
                max_tokens=256,
                system=self.SYSTEM_PROMPT,
                messages=[
                    {
                        "role": "user",
                        "content": f"Analyze this news headline for {ticker}:\n\n\"{headline}\""
                    }
                ]
            )

            # Parse response
            response_text = message.content[0].text
            # Extract JSON from response
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())

        except Exception as e:
            logger.warning(f"LLM analysis failed for headline: {e}")

        return None

    def analyze_batch(self, headlines: List[Tuple[str, str]],
                     delay: float = 0.5) -> List[Optional[Dict]]:
        """Analyze multiple headlines. Each item is (headline, ticker)."""
        results = []
        for headline, ticker in headlines:
            result = self.analyze(headline, ticker)
            results.append(result)
            if delay > 0:
                time.sleep(delay)
        return results


class NewsSentimentScraper:
    """Scrape and analyze news sentiment for stocks."""

    def __init__(self, config: Optional[SentimentConfig] = None):
        self.config = config or SentimentConfig()
        self.rule_analyzer = RuleBasedSentiment()
        self.llm_analyzer = LLMSentimentAnalyzer(model=self.config.llm_model) if self.config.use_llm else None
        self._cache: Dict[str, Tuple[datetime, NewsSentimentData]] = {}

    def _fetch_news(self, ticker: str) -> List[Dict]:
        """Fetch news for a ticker using yfinance."""
        try:
            import yfinance as yf

            stock = yf.Ticker(ticker)
            news = stock.news

            if not news:
                return []

            return news[:self.config.max_headlines_per_ticker]

        except Exception as e:
            logger.error(f"Error fetching news for {ticker}: {e}")
            return []

    def _analyze_headline(self, headline: str, ticker: str) -> NewsItem:
        """Analyze a single headline."""
        item = NewsItem(
            headline=headline,
            source="yfinance",
            published=datetime.now()
        )

        # Try LLM first
        if self.llm_analyzer and self.config.use_llm:
            llm_result = self.llm_analyzer.analyze(headline, ticker)
            if llm_result:
                item.sentiment_score = llm_result.get('sentiment_score', 0.0)
                item.confidence = llm_result.get('confidence', 0.7)
                item.key_entities = llm_result.get('key_entities', [])
                item.summary = llm_result.get('brief_summary')

                # Map event type
                event_str = llm_result.get('event_type', 'general')
                try:
                    item.event_type = NewsEventType(event_str)
                except ValueError:
                    item.event_type = NewsEventType.GENERAL

                # Map sentiment level
                if item.sentiment_score >= 0.5:
                    item.sentiment = SentimentLevel.VERY_POSITIVE
                elif item.sentiment_score >= 0.2:
                    item.sentiment = SentimentLevel.POSITIVE
                elif item.sentiment_score <= -0.5:
                    item.sentiment = SentimentLevel.VERY_NEGATIVE
                elif item.sentiment_score <= -0.2:
                    item.sentiment = SentimentLevel.NEGATIVE
                else:
                    item.sentiment = SentimentLevel.NEUTRAL

                return item

        # Fallback to rule-based
        if self.config.fallback_to_rules:
            score, event_type, confidence = self.rule_analyzer.analyze(headline)
            item.sentiment_score = score
            item.event_type = event_type
            item.confidence = confidence

            if score >= 0.5:
                item.sentiment = SentimentLevel.VERY_POSITIVE
            elif score >= 0.2:
                item.sentiment = SentimentLevel.POSITIVE
            elif score <= -0.5:
                item.sentiment = SentimentLevel.VERY_NEGATIVE
            elif score <= -0.2:
                item.sentiment = SentimentLevel.NEGATIVE
            else:
                item.sentiment = SentimentLevel.NEUTRAL

        return item

    def fetch_sentiment(self, ticker: str) -> NewsSentimentData:
        """Fetch and analyze news sentiment for a ticker."""
        # Check cache
        if ticker in self._cache:
            cache_time, cached_data = self._cache[ticker]
            if datetime.now() - cache_time < timedelta(hours=self.config.cache_hours):
                return cached_data

        data = NewsSentimentData(
            ticker=ticker,
            fetch_date=datetime.now().strftime("%Y-%m-%d")
        )

        # Fetch news
        news_items = self._fetch_news(ticker)

        if not news_items:
            self._cache[ticker] = (datetime.now(), data)
            return data

        # Analyze each headline
        for news in news_items:
            headline = news.get('title', '')
            if not headline:
                continue

            item = self._analyze_headline(headline, ticker)

            # Parse publish time
            pub_time = news.get('providerPublishTime')
            if pub_time:
                item.published = datetime.fromtimestamp(pub_time)

            item.source = news.get('publisher', 'Unknown')
            item.url = news.get('link')

            data.news_items.append(item)

        # Calculate aggregates
        if data.news_items:
            scores = [item.sentiment_score for item in data.news_items]
            data.avg_sentiment = sum(scores) / len(scores)
            data.sentiment_std = pd.Series(scores).std() if len(scores) > 1 else 0.0

            positive = sum(1 for s in scores if s > 0.1)
            negative = sum(1 for s in scores if s < -0.1)
            total = len(scores)
            data.positive_ratio = positive / total if total > 0 else 0.5
            data.negative_ratio = negative / total if total > 0 else 0.5

            # Count by time period
            now = datetime.now()
            data.news_count_24h = sum(
                1 for item in data.news_items
                if (now - item.published).days < 1
            )
            data.news_count_7d = len(data.news_items)

            # Event detection
            data.has_earnings_news = any(
                item.event_type == NewsEventType.EARNINGS for item in data.news_items
            )
            data.has_analyst_news = any(
                item.event_type == NewsEventType.ANALYST for item in data.news_items
            )
            data.has_ma_news = any(
                item.event_type == NewsEventType.MA for item in data.news_items
            )

            # Calculate sentiment momentum (recent vs older)
            if len(data.news_items) >= 4:
                recent = data.news_items[:len(data.news_items)//2]
                older = data.news_items[len(data.news_items)//2:]
                recent_avg = sum(i.sentiment_score for i in recent) / len(recent)
                older_avg = sum(i.sentiment_score for i in older) / len(older)
                data.sentiment_momentum = recent_avg - older_avg

        # Calculate normalized scores
        data = self._calculate_scores(data)

        # Cache
        self._cache[ticker] = (datetime.now(), data)

        time.sleep(self.config.request_delay)
        return data

    def _calculate_scores(self, data: NewsSentimentData) -> NewsSentimentData:
        """Calculate normalized sentiment scores."""
        # Sentiment score: map -1 to 1 range to 0-100
        data.sentiment_score = 50 + (data.avg_sentiment * 50)

        # Momentum score
        data.momentum_score = 50 + (data.sentiment_momentum * 100)
        data.momentum_score = max(0, min(100, data.momentum_score))

        # Combined score
        data.combined_score = (
            data.sentiment_score * 0.6 +
            data.momentum_score * 0.4
        )

        # Boost for significant events
        if data.has_earnings_news and data.avg_sentiment > 0.2:
            data.combined_score += 5
        if data.has_analyst_news and data.avg_sentiment > 0.2:
            data.combined_score += 3

        data.combined_score = max(0, min(100, data.combined_score))

        return data

    def fetch_batch(self, tickers: List[str],
                   show_progress: bool = True) -> Dict[str, NewsSentimentData]:
        """Fetch sentiment for multiple tickers."""
        results = {}

        for i, ticker in enumerate(tickers):
            if show_progress and i % 10 == 0:
                logger.info(f"Fetching news sentiment: {i}/{len(tickers)}")

            results[ticker] = self.fetch_sentiment(ticker)

        return results

    def get_top_sentiment(self,
                         data: Dict[str, NewsSentimentData],
                         top_n: int = 20) -> pd.DataFrame:
        """Get stocks with best sentiment."""
        rows = []
        for ticker, sent_data in data.items():
            if sent_data.news_count_7d > 0:
                rows.append({
                    'ticker': ticker,
                    'avg_sentiment': sent_data.avg_sentiment,
                    'news_count': sent_data.news_count_7d,
                    'positive_ratio': sent_data.positive_ratio,
                    'momentum': sent_data.sentiment_momentum,
                    'has_earnings': sent_data.has_earnings_news,
                    'has_analyst': sent_data.has_analyst_news,
                    'sentiment_score': sent_data.sentiment_score,
                    'combined_score': sent_data.combined_score
                })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        return df.nlargest(top_n, 'combined_score')

    def get_worst_sentiment(self,
                           data: Dict[str, NewsSentimentData],
                           top_n: int = 20) -> pd.DataFrame:
        """Get stocks with worst sentiment (potential shorts or avoid)."""
        rows = []
        for ticker, sent_data in data.items():
            if sent_data.news_count_7d > 0:
                rows.append({
                    'ticker': ticker,
                    'avg_sentiment': sent_data.avg_sentiment,
                    'news_count': sent_data.news_count_7d,
                    'negative_ratio': sent_data.negative_ratio,
                    'momentum': sent_data.sentiment_momentum,
                    'combined_score': sent_data.combined_score
                })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        return df.nsmallest(top_n, 'combined_score')


def create_sentiment_features(data: Dict[str, NewsSentimentData]) -> pd.DataFrame:
    """Create features for ML model from sentiment data."""
    rows = []

    for ticker, sent_data in data.items():
        rows.append({
            'ticker': ticker,
            'news_avg_sentiment': sent_data.avg_sentiment,
            'news_sentiment_std': sent_data.sentiment_std,
            'news_positive_ratio': sent_data.positive_ratio,
            'news_negative_ratio': sent_data.negative_ratio,
            'news_momentum': sent_data.sentiment_momentum,
            'news_count_24h': sent_data.news_count_24h,
            'news_count_7d': sent_data.news_count_7d,
            'news_has_earnings': 1 if sent_data.has_earnings_news else 0,
            'news_has_analyst': 1 if sent_data.has_analyst_news else 0,
            'news_has_ma': 1 if sent_data.has_ma_news else 0,
            'news_sentiment_score': sent_data.sentiment_score,
            'news_combined_score': sent_data.combined_score
        })

    return pd.DataFrame(rows).set_index('ticker') if rows else pd.DataFrame()


def quick_sentiment_check(ticker: str, use_llm: bool = False) -> Dict:
    """Quick sentiment check for a single ticker."""
    config = SentimentConfig(use_llm=use_llm)
    scraper = NewsSentimentScraper(config)
    data = scraper.fetch_sentiment(ticker)

    return {
        'ticker': ticker,
        'avg_sentiment': data.avg_sentiment,
        'sentiment_label': 'positive' if data.avg_sentiment > 0.1 else (
            'negative' if data.avg_sentiment < -0.1 else 'neutral'
        ),
        'news_count': data.news_count_7d,
        'positive_ratio': data.positive_ratio,
        'momentum': data.sentiment_momentum,
        'combined_score': data.combined_score,
        'top_headlines': [
            {'headline': item.headline, 'sentiment': item.sentiment_score}
            for item in data.news_items[:5]
        ]
    }
