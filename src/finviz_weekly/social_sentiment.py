"""
Social Media Sentiment Analysis

Analyze sentiment from social platforms:
- Reddit (r/wallstreetbets, r/stocks, r/investing)
- Mention frequency and sentiment tracking
- Momentum detection (trending tickers)
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import Counter
import time

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class RedditPost:
    """Reddit post data."""
    id: str
    title: str
    subreddit: str
    score: int
    num_comments: int
    created_utc: float
    url: str
    selftext: str = ""
    tickers_mentioned: List[str] = field(default_factory=list)
    sentiment_score: float = 0.0


@dataclass
class TickerMention:
    """Aggregated ticker mentions."""
    ticker: str
    mention_count: int
    avg_sentiment: float
    total_score: int  # Combined Reddit score
    total_comments: int
    subreddits: List[str] = field(default_factory=list)
    momentum: float = 0.0  # Change vs previous period
    posts: List[RedditPost] = field(default_factory=list)


@dataclass
class SocialSentimentConfig:
    """Configuration for social sentiment scraping."""
    subreddits: List[str] = field(default_factory=lambda: [
        "wallstreetbets", "stocks", "investing", "stockmarket", "options"
    ])
    lookback_hours: int = 24
    min_score: int = 10  # Minimum Reddit score
    min_comments: int = 5
    max_posts_per_sub: int = 100


class TickerExtractor:
    """Extract stock tickers from text."""

    # Common words that look like tickers but aren't
    BLACKLIST = {
        'A', 'I', 'AM', 'PM', 'CEO', 'CFO', 'IPO', 'ETF', 'USA', 'GDP',
        'ATH', 'ATL', 'DD', 'DRS', 'EPS', 'FUD', 'GME', 'IMO', 'ITM',
        'IV', 'OTM', 'PE', 'PT', 'QE', 'RH', 'SEC', 'SI', 'TD', 'UK',
        'US', 'WSB', 'YOY', 'EOD', 'AH', 'PM', 'EST', 'PST', 'UTC',
        'ARE', 'FOR', 'THE', 'AND', 'NOT', 'YOU', 'ALL', 'CAN', 'HAD',
        'HER', 'WAS', 'ONE', 'OUR', 'OUT', 'DAY', 'GET', 'HAS', 'HIM',
        'HIS', 'HOW', 'ITS', 'MAY', 'NEW', 'NOW', 'OLD', 'SEE', 'WAY',
        'WHO', 'BOY', 'DID', 'SAY', 'SHE', 'TOO', 'USE', 'IT', 'OR',
        'BE', 'BY', 'DO', 'GO', 'HE', 'IF', 'IN', 'IS', 'ME', 'MY',
        'NO', 'OF', 'ON', 'SO', 'TO', 'UP', 'WE', 'AN', 'AS', 'AT',
        'YOLO', 'HODL', 'FOMO', 'BTFD', 'TLDR', 'IMO', 'IMHO',
        'LOL', 'WTF', 'FYI', 'BTW', 'EDIT', 'UPDATE', 'TL', 'DR',
        'PT', 'SP', 'EV', 'AI', 'AR', 'VR', 'PR', 'ER', 'FDA',
    }

    # Pattern for cashtags ($AAPL) and standalone tickers
    CASHTAG_PATTERN = re.compile(r'\$([A-Z]{1,5})\b')
    TICKER_PATTERN = re.compile(r'\b([A-Z]{2,5})\b')

    def extract_tickers(self, text: str) -> List[str]:
        """Extract stock tickers from text."""
        tickers = set()

        # Extract cashtags (higher confidence)
        for match in self.CASHTAG_PATTERN.finditer(text):
            ticker = match.group(1)
            if ticker not in self.BLACKLIST and len(ticker) <= 5:
                tickers.add(ticker)

        # Extract potential tickers (lower confidence, need context)
        for match in self.TICKER_PATTERN.finditer(text):
            ticker = match.group(1)
            if ticker not in self.BLACKLIST and len(ticker) >= 2:
                # Additional validation - should be near financial context
                context = text[max(0, match.start()-50):match.end()+50].lower()
                if any(word in context for word in [
                    'stock', 'buy', 'sell', 'call', 'put', 'share',
                    'price', 'target', 'earnings', 'bullish', 'bearish',
                    'moon', 'rocket', 'yolo', 'long', 'short'
                ]):
                    tickers.add(ticker)

        return list(tickers)


class RedditSentimentAnalyzer:
    """Analyze sentiment from Reddit posts."""

    BULLISH_KEYWORDS = [
        'buy', 'call', 'calls', 'long', 'bullish', 'moon', 'rocket',
        'undervalued', 'squeeze', 'breakout', 'upside', 'tendies',
        'gain', 'profit', 'winner', 'green', 'pump', 'yolo',
        '🚀', '💎', '🙌', '📈', '💰', '🔥'
    ]

    BEARISH_KEYWORDS = [
        'sell', 'put', 'puts', 'short', 'bearish', 'crash', 'dump',
        'overvalued', 'tank', 'downside', 'loss', 'loser', 'red',
        'bag', 'bagholder', 'rip', 'rekt', 'worthless',
        '📉', '💀', '🩸', '🔻'
    ]

    def analyze_sentiment(self, text: str) -> float:
        """
        Analyze sentiment of text.

        Returns:
            Score from -1.0 (bearish) to 1.0 (bullish)
        """
        text_lower = text.lower()

        bullish_count = sum(1 for word in self.BULLISH_KEYWORDS if word in text_lower)
        bearish_count = sum(1 for word in self.BEARISH_KEYWORDS if word in text_lower)

        total = bullish_count + bearish_count
        if total == 0:
            return 0.0

        return (bullish_count - bearish_count) / total


class RedditScraper:
    """Scrape Reddit for stock discussions."""

    def __init__(self, config: Optional[SocialSentimentConfig] = None):
        self.config = config or SocialSentimentConfig()
        self.ticker_extractor = TickerExtractor()
        self.sentiment_analyzer = RedditSentimentAnalyzer()
        self._reddit = None

    def _get_reddit_client(self):
        """Get Reddit client (using PRAW if available, else public JSON API)."""
        if self._reddit is None:
            try:
                import praw
                import os

                client_id = os.environ.get('REDDIT_CLIENT_ID')
                client_secret = os.environ.get('REDDIT_CLIENT_SECRET')
                user_agent = os.environ.get('REDDIT_USER_AGENT', 'finviz-scraper/1.0')

                if client_id and client_secret:
                    self._reddit = praw.Reddit(
                        client_id=client_id,
                        client_secret=client_secret,
                        user_agent=user_agent
                    )
                    logger.info("Using PRAW Reddit client")
                else:
                    self._reddit = "json"  # Fallback to JSON API
                    logger.info("Using public Reddit JSON API")
            except ImportError:
                self._reddit = "json"
                logger.info("PRAW not installed, using public Reddit JSON API")

        return self._reddit

    def _fetch_subreddit_posts_json(self, subreddit: str,
                                    limit: int = 100) -> List[Dict]:
        """Fetch posts using Reddit's public JSON API."""
        import requests

        url = f"https://www.reddit.com/r/{subreddit}/hot.json"
        headers = {'User-Agent': 'finviz-scraper/1.0'}
        params = {'limit': limit}

        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            posts = []
            for child in data.get('data', {}).get('children', []):
                post = child.get('data', {})
                posts.append({
                    'id': post.get('id'),
                    'title': post.get('title', ''),
                    'selftext': post.get('selftext', ''),
                    'score': post.get('score', 0),
                    'num_comments': post.get('num_comments', 0),
                    'created_utc': post.get('created_utc', 0),
                    'url': f"https://reddit.com{post.get('permalink', '')}",
                    'subreddit': subreddit,
                })

            return posts

        except Exception as e:
            logger.error(f"Failed to fetch r/{subreddit}: {e}")
            return []

    def _fetch_subreddit_posts_praw(self, subreddit: str,
                                    limit: int = 100) -> List[Dict]:
        """Fetch posts using PRAW."""
        try:
            sub = self._reddit.subreddit(subreddit)
            posts = []

            for post in sub.hot(limit=limit):
                posts.append({
                    'id': post.id,
                    'title': post.title,
                    'selftext': post.selftext,
                    'score': post.score,
                    'num_comments': post.num_comments,
                    'created_utc': post.created_utc,
                    'url': f"https://reddit.com{post.permalink}",
                    'subreddit': subreddit,
                })

            return posts

        except Exception as e:
            logger.error(f"Failed to fetch r/{subreddit} with PRAW: {e}")
            return []

    def fetch_posts(self) -> List[RedditPost]:
        """Fetch posts from configured subreddits."""
        client = self._get_reddit_client()
        all_posts = []

        cutoff_time = datetime.now() - timedelta(hours=self.config.lookback_hours)
        cutoff_utc = cutoff_time.timestamp()

        for subreddit in self.config.subreddits:
            logger.info(f"Fetching r/{subreddit}...")

            if client == "json":
                raw_posts = self._fetch_subreddit_posts_json(
                    subreddit, self.config.max_posts_per_sub
                )
            else:
                raw_posts = self._fetch_subreddit_posts_praw(
                    subreddit, self.config.max_posts_per_sub
                )

            for raw in raw_posts:
                # Filter by time
                if raw['created_utc'] < cutoff_utc:
                    continue

                # Filter by score/comments
                if raw['score'] < self.config.min_score:
                    continue
                if raw['num_comments'] < self.config.min_comments:
                    continue

                # Extract tickers and sentiment
                full_text = f"{raw['title']} {raw['selftext']}"
                tickers = self.ticker_extractor.extract_tickers(full_text)
                sentiment = self.sentiment_analyzer.analyze_sentiment(full_text)

                post = RedditPost(
                    id=raw['id'],
                    title=raw['title'],
                    subreddit=subreddit,
                    score=raw['score'],
                    num_comments=raw['num_comments'],
                    created_utc=raw['created_utc'],
                    url=raw['url'],
                    selftext=raw['selftext'][:500],  # Truncate
                    tickers_mentioned=tickers,
                    sentiment_score=sentiment
                )

                all_posts.append(post)

            # Rate limiting
            time.sleep(1)

        logger.info(f"Fetched {len(all_posts)} posts total")
        return all_posts

    def aggregate_by_ticker(self, posts: List[RedditPost]) -> Dict[str, TickerMention]:
        """Aggregate mentions by ticker."""
        ticker_data: Dict[str, TickerMention] = {}

        for post in posts:
            for ticker in post.tickers_mentioned:
                if ticker not in ticker_data:
                    ticker_data[ticker] = TickerMention(
                        ticker=ticker,
                        mention_count=0,
                        avg_sentiment=0.0,
                        total_score=0,
                        total_comments=0,
                        subreddits=[],
                        posts=[]
                    )

                mention = ticker_data[ticker]
                mention.mention_count += 1
                mention.total_score += post.score
                mention.total_comments += post.num_comments
                mention.posts.append(post)

                if post.subreddit not in mention.subreddits:
                    mention.subreddits.append(post.subreddit)

        # Calculate average sentiment
        for ticker, mention in ticker_data.items():
            if mention.posts:
                mention.avg_sentiment = sum(p.sentiment_score for p in mention.posts) / len(mention.posts)

        return ticker_data

    def get_trending_tickers(self, limit: int = 20) -> pd.DataFrame:
        """Get trending tickers from Reddit."""
        posts = self.fetch_posts()
        mentions = self.aggregate_by_ticker(posts)

        rows = []
        for ticker, data in mentions.items():
            rows.append({
                'ticker': ticker,
                'mentions': data.mention_count,
                'sentiment': data.avg_sentiment,
                'total_score': data.total_score,
                'total_comments': data.total_comments,
                'subreddits': ', '.join(data.subreddits),
                'bullish': data.avg_sentiment > 0.2,
                'bearish': data.avg_sentiment < -0.2,
            })

        df = pd.DataFrame(rows)
        if not df.empty:
            # Score = mentions * sentiment * log(total_score)
            import numpy as np
            df['trend_score'] = (
                df['mentions'] *
                (1 + df['sentiment']) *
                np.log1p(df['total_score'])
            )
            df = df.nlargest(limit, 'trend_score')

        return df


def get_reddit_sentiment(tickers: Optional[List[str]] = None,
                        config: Optional[SocialSentimentConfig] = None) -> pd.DataFrame:
    """
    Get Reddit sentiment for tickers.

    Args:
        tickers: Filter to specific tickers (None = all trending)
        config: Scraping configuration
    """
    scraper = RedditScraper(config)
    df = scraper.get_trending_tickers(limit=100)

    if tickers and not df.empty:
        df = df[df['ticker'].isin(tickers)]

    return df


def create_social_features(sentiment_data: pd.DataFrame) -> pd.DataFrame:
    """Create features for ML model from social sentiment."""
    if sentiment_data.empty:
        return pd.DataFrame()

    features = sentiment_data[['ticker', 'mentions', 'sentiment', 'total_score']].copy()
    features = features.rename(columns={
        'mentions': 'reddit_mentions',
        'sentiment': 'reddit_sentiment',
        'total_score': 'reddit_score'
    })
    features = features.set_index('ticker')

    return features
