"""
ETF Flow Tracking Module

Track money flows into/out of sector ETFs for rotation signals:
- Sector ETF flow analysis
- Smart money flow detection
- Sector rotation momentum
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import time

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# Major sector ETFs
SECTOR_ETFS = {
    'Technology': ['XLK', 'VGT', 'QQQ'],
    'Healthcare': ['XLV', 'VHT', 'IBB'],
    'Financials': ['XLF', 'VFH', 'KBE'],
    'Consumer Discretionary': ['XLY', 'VCR'],
    'Consumer Staples': ['XLP', 'VDC'],
    'Energy': ['XLE', 'VDE', 'OIH'],
    'Industrials': ['XLI', 'VIS'],
    'Materials': ['XLB', 'VAW'],
    'Utilities': ['XLU', 'VPU'],
    'Real Estate': ['XLRE', 'VNQ', 'IYR'],
    'Communication Services': ['XLC', 'VOX'],
}

# Factor/Style ETFs
FACTOR_ETFS = {
    'Value': ['VTV', 'IWD', 'VLUE'],
    'Growth': ['VUG', 'IWF', 'MTUM'],
    'Momentum': ['MTUM', 'PDP'],
    'Quality': ['QUAL', 'SPHQ'],
    'Low Volatility': ['SPLV', 'USMV'],
    'Small Cap': ['IWM', 'VB', 'IJR'],
    'Large Cap': ['SPY', 'IVV', 'VOO'],
    'Dividend': ['VIG', 'DVY', 'SCHD'],
}

# Thematic ETFs
THEMATIC_ETFS = {
    'AI/Tech': ['BOTZ', 'ROBO', 'AIQ'],
    'Clean Energy': ['ICLN', 'TAN', 'QCLN'],
    'Cybersecurity': ['HACK', 'CIBR', 'BUG'],
    'Semiconductors': ['SMH', 'SOXX'],
    'Cloud Computing': ['SKYY', 'WCLD'],
    'EV/Batteries': ['LIT', 'DRIV'],
    'Cannabis': ['MJ', 'MSOS'],
    'China': ['FXI', 'KWEB', 'MCHI'],
}


@dataclass
class ETFFlowData:
    """ETF flow data."""
    ticker: str
    name: str
    category: str  # sector, factor, thematic
    theme: str  # e.g., 'Technology', 'Value'

    # Price data
    current_price: float = 0.0
    price_change_1d: float = 0.0
    price_change_5d: float = 0.0
    price_change_21d: float = 0.0

    # Volume/Flow data
    avg_volume_20d: float = 0.0
    current_volume: float = 0.0
    volume_ratio: float = 1.0  # Current / Average

    # Estimated flows (volume * price change direction)
    estimated_flow_1d: float = 0.0
    estimated_flow_5d: float = 0.0
    estimated_flow_21d: float = 0.0

    # Momentum
    momentum_score: float = 50.0
    relative_strength: float = 0.0  # vs SPY


@dataclass
class SectorRotationSignal:
    """Sector rotation signal."""
    date: str
    from_sectors: List[str]  # Money flowing out
    to_sectors: List[str]  # Money flowing in
    signal_strength: float  # 0-100
    rotation_type: str  # 'risk_on', 'risk_off', 'neutral'


class ETFFlowTracker:
    """Track ETF flows for sector rotation signals."""

    def __init__(self):
        self._cache: Dict[str, Tuple[datetime, ETFFlowData]] = {}
        self._cache_hours = 4

    def _get_etf_data(self, ticker: str, category: str, theme: str) -> ETFFlowData:
        """Fetch ETF data from yfinance."""
        try:
            import yfinance as yf

            etf = yf.Ticker(ticker)

            # Get historical data
            hist = etf.history(period="3mo")
            if hist.empty:
                return ETFFlowData(ticker=ticker, name=ticker, category=category, theme=theme)

            current_price = hist['Close'].iloc[-1]

            # Calculate returns
            price_1d = (hist['Close'].iloc[-1] / hist['Close'].iloc[-2] - 1) * 100 if len(hist) > 1 else 0
            price_5d = (hist['Close'].iloc[-1] / hist['Close'].iloc[-5] - 1) * 100 if len(hist) > 5 else 0
            price_21d = (hist['Close'].iloc[-1] / hist['Close'].iloc[-21] - 1) * 100 if len(hist) > 21 else 0

            # Volume analysis
            avg_volume = hist['Volume'].tail(20).mean()
            current_volume = hist['Volume'].iloc[-1]
            volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1.0

            # Estimate flows (simplified: volume * direction)
            flow_1d = current_volume * (1 if price_1d > 0 else -1) * abs(price_1d) / 100
            flow_5d = hist['Volume'].tail(5).sum() * (1 if price_5d > 0 else -1) * abs(price_5d) / 100
            flow_21d = hist['Volume'].tail(21).sum() * (1 if price_21d > 0 else -1) * abs(price_21d) / 100

            # Get ETF info
            info = etf.info
            name = info.get('shortName', ticker)

            return ETFFlowData(
                ticker=ticker,
                name=name,
                category=category,
                theme=theme,
                current_price=current_price,
                price_change_1d=price_1d,
                price_change_5d=price_5d,
                price_change_21d=price_21d,
                avg_volume_20d=avg_volume,
                current_volume=current_volume,
                volume_ratio=volume_ratio,
                estimated_flow_1d=flow_1d,
                estimated_flow_5d=flow_5d,
                estimated_flow_21d=flow_21d,
            )

        except Exception as e:
            logger.error(f"Error fetching {ticker}: {e}")
            return ETFFlowData(ticker=ticker, name=ticker, category=category, theme=theme)

    def fetch_sector_flows(self) -> Dict[str, List[ETFFlowData]]:
        """Fetch flow data for all sector ETFs."""
        results = {}

        for sector, etfs in SECTOR_ETFS.items():
            sector_data = []
            for ticker in etfs:
                logger.debug(f"Fetching {ticker} ({sector})")
                data = self._get_etf_data(ticker, 'sector', sector)
                sector_data.append(data)
                time.sleep(0.3)

            results[sector] = sector_data

        return results

    def fetch_factor_flows(self) -> Dict[str, List[ETFFlowData]]:
        """Fetch flow data for factor ETFs."""
        results = {}

        for factor, etfs in FACTOR_ETFS.items():
            factor_data = []
            for ticker in etfs:
                logger.debug(f"Fetching {ticker} ({factor})")
                data = self._get_etf_data(ticker, 'factor', factor)
                factor_data.append(data)
                time.sleep(0.3)

            results[factor] = factor_data

        return results

    def get_sector_ranking(self, period: str = '21d') -> pd.DataFrame:
        """Rank sectors by flow momentum."""
        sector_flows = self.fetch_sector_flows()

        rows = []
        for sector, etf_list in sector_flows.items():
            if not etf_list:
                continue

            # Use primary ETF (first in list)
            primary = etf_list[0]

            if period == '1d':
                flow = primary.estimated_flow_1d
                price_chg = primary.price_change_1d
            elif period == '5d':
                flow = primary.estimated_flow_5d
                price_chg = primary.price_change_5d
            else:  # 21d
                flow = primary.estimated_flow_21d
                price_chg = primary.price_change_21d

            rows.append({
                'sector': sector,
                'etf': primary.ticker,
                'price_change': price_chg,
                'volume_ratio': primary.volume_ratio,
                'estimated_flow': flow,
                'momentum_score': 50 + price_chg * 2,  # Simple momentum
            })

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values('momentum_score', ascending=False)
            df['rank'] = range(1, len(df) + 1)

        return df

    def detect_rotation(self) -> SectorRotationSignal:
        """Detect sector rotation patterns."""
        ranking = self.get_sector_ranking('21d')

        if ranking.empty:
            return SectorRotationSignal(
                date=datetime.now().strftime("%Y-%m-%d"),
                from_sectors=[],
                to_sectors=[],
                signal_strength=0,
                rotation_type='neutral'
            )

        # Top 3 and bottom 3 sectors
        top_sectors = ranking.head(3)['sector'].tolist()
        bottom_sectors = ranking.tail(3)['sector'].tolist()

        # Classify rotation type
        defensive_sectors = {'Utilities', 'Consumer Staples', 'Healthcare', 'Real Estate'}
        cyclical_sectors = {'Technology', 'Consumer Discretionary', 'Financials', 'Industrials'}

        top_set = set(top_sectors)
        if top_set & defensive_sectors and not (top_set & cyclical_sectors):
            rotation_type = 'risk_off'
        elif top_set & cyclical_sectors and not (top_set & defensive_sectors):
            rotation_type = 'risk_on'
        else:
            rotation_type = 'neutral'

        # Signal strength based on spread between top and bottom
        if len(ranking) >= 6:
            top_avg = ranking.head(3)['price_change'].mean()
            bottom_avg = ranking.tail(3)['price_change'].mean()
            spread = top_avg - bottom_avg
            signal_strength = min(100, abs(spread) * 5)
        else:
            signal_strength = 50

        return SectorRotationSignal(
            date=datetime.now().strftime("%Y-%m-%d"),
            from_sectors=bottom_sectors,
            to_sectors=top_sectors,
            signal_strength=signal_strength,
            rotation_type=rotation_type
        )

    def get_factor_rotation(self) -> Dict[str, float]:
        """Analyze factor rotation (value vs growth, etc.)."""
        factor_flows = self.fetch_factor_flows()

        factor_scores = {}
        for factor, etf_list in factor_flows.items():
            if etf_list:
                primary = etf_list[0]
                factor_scores[factor] = primary.price_change_21d

        return factor_scores

    def get_recommended_sector_tilts(self) -> Dict[str, float]:
        """Get recommended sector tilts based on flows."""
        ranking = self.get_sector_ranking('21d')

        if ranking.empty:
            return {}

        tilts = {}
        n_sectors = len(ranking)

        for i, row in ranking.iterrows():
            # Convert rank to tilt (-0.2 to +0.2)
            rank = row['rank']
            tilt = 0.2 * (1 - 2 * (rank - 1) / (n_sectors - 1)) if n_sectors > 1 else 0
            tilts[row['sector']] = round(tilt, 2)

        return tilts


def get_etf_flow_summary() -> Dict:
    """Get summary of ETF flows and rotation signals."""
    tracker = ETFFlowTracker()

    # Get sector ranking
    sector_ranking = tracker.get_sector_ranking('21d')

    # Detect rotation
    rotation = tracker.detect_rotation()

    # Get factor scores
    factor_scores = tracker.get_factor_rotation()

    # Get tilts
    tilts = tracker.get_recommended_sector_tilts()

    return {
        'date': datetime.now().strftime("%Y-%m-%d"),
        'sector_ranking': sector_ranking.to_dict('records') if not sector_ranking.empty else [],
        'rotation_signal': {
            'type': rotation.rotation_type,
            'strength': rotation.signal_strength,
            'flowing_into': rotation.to_sectors,
            'flowing_out': rotation.from_sectors,
        },
        'factor_scores': factor_scores,
        'recommended_tilts': tilts,
    }


def create_etf_flow_features() -> pd.DataFrame:
    """Create features for ML model from ETF flows."""
    tracker = ETFFlowTracker()
    ranking = tracker.get_sector_ranking('21d')

    if ranking.empty:
        return pd.DataFrame()

    # Create sector momentum features
    features = {}
    for _, row in ranking.iterrows():
        sector_key = row['sector'].lower().replace(' ', '_')
        features[f'sector_{sector_key}_momentum'] = row['momentum_score']
        features[f'sector_{sector_key}_flow'] = row['estimated_flow']

    return pd.DataFrame([features])
