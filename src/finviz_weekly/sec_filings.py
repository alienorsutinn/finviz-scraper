"""
SEC Filings Parser Module

Parse and analyze SEC filings:
- 8-K filings (material events)
- 10-Q/10-K filings (quarterly/annual reports)
- 13F filings (institutional holdings)
- Form 4 (insider transactions)
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import time

import pandas as pd

logger = logging.getLogger(__name__)


# SEC filing types
FILING_TYPES = {
    '8-K': 'Material Event',
    '10-Q': 'Quarterly Report',
    '10-K': 'Annual Report',
    '13F-HR': 'Institutional Holdings',
    '4': 'Insider Transaction',
    'S-1': 'IPO Registration',
    'DEF 14A': 'Proxy Statement',
}

# 8-K item numbers and their meanings
FORM_8K_ITEMS = {
    '1.01': 'Entry into Material Agreement',
    '1.02': 'Termination of Material Agreement',
    '1.03': 'Bankruptcy',
    '2.01': 'Acquisition/Disposition of Assets',
    '2.02': 'Results of Operations',
    '2.03': 'Creation of Obligation',
    '2.04': 'Triggering Events',
    '2.05': 'Costs for Exit Activities',
    '2.06': 'Material Impairments',
    '3.01': 'Delisting Notice',
    '3.02': 'Unregistered Equity Sales',
    '3.03': 'Material Modification to Rights',
    '4.01': 'Auditor Changes',
    '4.02': 'Non-Reliance on Financial Statements',
    '5.01': 'Director/Officer Changes',
    '5.02': 'Departure/Election of Directors/Officers',
    '5.03': 'Amendments to Articles/Bylaws',
    '5.07': 'Shareholder Vote Results',
    '7.01': 'Regulation FD Disclosure',
    '8.01': 'Other Events',
    '9.01': 'Financial Statements and Exhibits',
}


@dataclass
class SECFiling:
    """SEC filing data."""
    ticker: str
    filing_type: str
    filed_date: str
    accepted_date: str
    accession_number: str
    url: str
    description: str = ""

    # Analysis results
    items: List[str] = field(default_factory=list)  # For 8-K
    sentiment_score: float = 0.0
    is_material: bool = False
    key_topics: List[str] = field(default_factory=list)


@dataclass
class FilingAnalysis:
    """Analysis of filing content."""
    filing_id: str
    filing_type: str
    sentiment_score: float
    key_phrases: List[str]
    risk_factors: List[str]
    financial_metrics: Dict[str, float]
    management_discussion_summary: str = ""


class SECEdgarClient:
    """Client for SEC EDGAR API."""

    BASE_URL = "https://data.sec.gov"
    COMPANY_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"

    def __init__(self, user_agent: str = "finviz-scraper contact@example.com"):
        """
        Initialize EDGAR client.

        Note: SEC requires a user agent with contact info.
        """
        self.user_agent = user_agent
        self.headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
        }

    def _get_cik(self, ticker: str) -> Optional[str]:
        """Get CIK (Central Index Key) for a ticker."""
        try:
            import requests

            # SEC's company tickers JSON
            url = f"{self.BASE_URL}/submissions/CIK{ticker.upper()}.json"

            # Try ticker lookup
            tickers_url = "https://www.sec.gov/files/company_tickers.json"
            response = requests.get(tickers_url, headers=self.headers, timeout=10)
            response.raise_for_status()

            data = response.json()
            for _, company in data.items():
                if company.get('ticker', '').upper() == ticker.upper():
                    cik = str(company.get('cik_str', ''))
                    return cik.zfill(10)  # Pad to 10 digits

            return None

        except Exception as e:
            logger.error(f"Failed to get CIK for {ticker}: {e}")
            return None

    def get_filings(self, ticker: str,
                   filing_type: Optional[str] = None,
                   limit: int = 20) -> List[SECFiling]:
        """Get recent filings for a company."""
        try:
            import requests

            cik = self._get_cik(ticker)
            if not cik:
                logger.warning(f"Could not find CIK for {ticker}")
                return []

            # Get company submissions
            url = f"{self.BASE_URL}/submissions/CIK{cik}.json"
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()

            data = response.json()
            filings_data = data.get('filings', {}).get('recent', {})

            filings = []
            forms = filings_data.get('form', [])
            dates = filings_data.get('filingDate', [])
            accessions = filings_data.get('accessionNumber', [])
            descriptions = filings_data.get('primaryDocument', [])

            for i in range(min(limit, len(forms))):
                form = forms[i]

                # Filter by type if specified
                if filing_type and form != filing_type:
                    continue

                accession = accessions[i].replace('-', '')
                filing_url = (
                    f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/"
                    f"{descriptions[i]}"
                )

                filing = SECFiling(
                    ticker=ticker,
                    filing_type=form,
                    filed_date=dates[i],
                    accepted_date=dates[i],
                    accession_number=accessions[i],
                    url=filing_url,
                    description=FILING_TYPES.get(form, form),
                )

                filings.append(filing)

                if len(filings) >= limit:
                    break

            return filings

        except Exception as e:
            logger.error(f"Failed to get filings for {ticker}: {e}")
            return []

    def get_8k_filings(self, ticker: str, limit: int = 10) -> List[SECFiling]:
        """Get recent 8-K filings (material events)."""
        return self.get_filings(ticker, filing_type='8-K', limit=limit)


class FilingTextAnalyzer:
    """Analyze filing text for sentiment and key information."""

    # Positive keywords
    POSITIVE_KEYWORDS = [
        'growth', 'increase', 'improved', 'strong', 'exceeded',
        'record', 'successful', 'profitable', 'expansion', 'beat',
        'outperformed', 'raised', 'upgraded', 'positive', 'momentum',
    ]

    # Negative keywords
    NEGATIVE_KEYWORDS = [
        'decline', 'decrease', 'weak', 'miss', 'below',
        'loss', 'impairment', 'restructuring', 'layoff', 'investigation',
        'litigation', 'default', 'downgrade', 'warning', 'concern',
        'uncertainty', 'challenging', 'headwind', 'risk', 'adverse',
    ]

    # Risk-related phrases
    RISK_PHRASES = [
        'material adverse effect',
        'going concern',
        'significant uncertainty',
        'we may not be able',
        'there is no assurance',
        'we cannot guarantee',
        'subject to risks',
        'regulatory action',
        'class action',
    ]

    def analyze_text(self, text: str) -> FilingAnalysis:
        """Analyze filing text."""
        text_lower = text.lower()

        # Sentiment analysis
        positive_count = sum(1 for word in self.POSITIVE_KEYWORDS if word in text_lower)
        negative_count = sum(1 for word in self.NEGATIVE_KEYWORDS if word in text_lower)
        total = positive_count + negative_count

        if total > 0:
            sentiment = (positive_count - negative_count) / total
        else:
            sentiment = 0.0

        # Extract key phrases (simplified)
        key_phrases = []
        for word in self.POSITIVE_KEYWORDS + self.NEGATIVE_KEYWORDS:
            if word in text_lower:
                key_phrases.append(word)

        # Find risk factors
        risk_factors = []
        for phrase in self.RISK_PHRASES:
            if phrase in text_lower:
                risk_factors.append(phrase)

        # Extract financial metrics (simplified regex)
        metrics = {}
        revenue_match = re.search(r'revenue[s]?\s+(?:of\s+)?\$?([\d,.]+)\s*(million|billion)?', text_lower)
        if revenue_match:
            value = float(revenue_match.group(1).replace(',', ''))
            if revenue_match.group(2) == 'billion':
                value *= 1000
            metrics['revenue_mentioned'] = value

        return FilingAnalysis(
            filing_id="",
            filing_type="",
            sentiment_score=sentiment,
            key_phrases=key_phrases[:10],
            risk_factors=risk_factors,
            financial_metrics=metrics,
        )


class SECFilingsTracker:
    """Track and analyze SEC filings."""

    def __init__(self):
        self.edgar_client = SECEdgarClient()
        self.text_analyzer = FilingTextAnalyzer()

    def get_recent_filings(self, ticker: str, days: int = 30) -> List[SECFiling]:
        """Get filings from the last N days."""
        all_filings = self.edgar_client.get_filings(ticker, limit=50)

        cutoff = datetime.now() - timedelta(days=days)
        recent = []

        for filing in all_filings:
            try:
                filed_date = datetime.strptime(filing.filed_date, "%Y-%m-%d")
                if filed_date >= cutoff:
                    recent.append(filing)
            except ValueError:
                continue

        return recent

    def get_material_events(self, ticker: str, days: int = 90) -> List[SECFiling]:
        """Get material events (8-K filings)."""
        filings = self.edgar_client.get_8k_filings(ticker, limit=20)

        cutoff = datetime.now() - timedelta(days=days)
        material = []

        for filing in filings:
            try:
                filed_date = datetime.strptime(filing.filed_date, "%Y-%m-%d")
                if filed_date >= cutoff:
                    filing.is_material = True
                    material.append(filing)
            except ValueError:
                continue

        return material

    def screen_for_events(self, tickers: List[str],
                         event_types: List[str] = None) -> pd.DataFrame:
        """Screen multiple tickers for recent filings."""
        event_types = event_types or ['8-K', '4']

        rows = []
        for ticker in tickers:
            filings = self.get_recent_filings(ticker, days=7)

            for filing in filings:
                if filing.filing_type in event_types:
                    rows.append({
                        'ticker': ticker,
                        'filing_type': filing.filing_type,
                        'filed_date': filing.filed_date,
                        'description': filing.description,
                        'url': filing.url,
                    })

            time.sleep(0.2)  # Rate limiting

        return pd.DataFrame(rows)

    def get_filing_sentiment_summary(self, ticker: str) -> Dict:
        """Get sentiment summary from recent filings."""
        filings = self.get_recent_filings(ticker, days=90)

        if not filings:
            return {
                'ticker': ticker,
                'filings_count': 0,
                'avg_sentiment': 0.0,
                'material_events': 0,
            }

        # Count by type
        type_counts = {}
        for f in filings:
            type_counts[f.filing_type] = type_counts.get(f.filing_type, 0) + 1

        # Count material events (8-K)
        material_count = type_counts.get('8-K', 0)

        return {
            'ticker': ticker,
            'filings_count': len(filings),
            'filing_types': type_counts,
            'material_events': material_count,
            'most_recent': filings[0].filed_date if filings else None,
            'most_recent_type': filings[0].filing_type if filings else None,
        }


def get_sec_filings_summary(tickers: List[str]) -> pd.DataFrame:
    """Get SEC filings summary for multiple tickers."""
    tracker = SECFilingsTracker()

    rows = []
    for ticker in tickers:
        summary = tracker.get_filing_sentiment_summary(ticker)
        rows.append(summary)
        time.sleep(0.3)

    return pd.DataFrame(rows)


def create_sec_features(tickers: List[str]) -> pd.DataFrame:
    """Create features for ML model from SEC filings."""
    tracker = SECFilingsTracker()

    rows = []
    for ticker in tickers:
        filings = tracker.get_recent_filings(ticker, days=30)

        row = {
            'ticker': ticker,
            'sec_filings_30d': len(filings),
            'sec_8k_count': sum(1 for f in filings if f.filing_type == '8-K'),
            'sec_4_count': sum(1 for f in filings if f.filing_type == '4'),
            'sec_has_material_event': any(f.filing_type == '8-K' for f in filings),
        }
        rows.append(row)
        time.sleep(0.2)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.set_index('ticker')

    return df
