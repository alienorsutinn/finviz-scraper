"""
Macro Economic Indicators Module

Provides macro-level signals for market timing and regime detection:
- Treasury yield curve (2Y-10Y spread, inversion detection)
- VIX term structure (contango/backwardation)
- Fed Funds Rate and expectations
- Credit spreads (investment grade vs high yield)
- Dollar strength (DXY)
- Sector rotation signals
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from enum import Enum

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class MacroRegime(Enum):
    """Macro economic regime classification."""
    RISK_ON = "risk_on"  # Favorable for equities
    RISK_OFF = "risk_off"  # Defensive positioning
    TRANSITION = "transition"  # Mixed signals
    RECESSION_WARNING = "recession_warning"  # Yield curve inversion


class YieldCurveState(Enum):
    """Yield curve shape classification."""
    NORMAL = "normal"  # Upward sloping
    FLAT = "flat"  # Near zero spread
    INVERTED = "inverted"  # Downward sloping (recession signal)
    STEEPENING = "steepening"  # Spread increasing
    FLATTENING = "flattening"  # Spread decreasing


class VIXTermStructure(Enum):
    """VIX futures term structure."""
    CONTANGO = "contango"  # Normal, futures > spot
    BACKWARDATION = "backwardation"  # Stress, spot > futures
    FLAT = "flat"  # Neutral


@dataclass
class YieldCurveData:
    """Treasury yield curve data."""
    date: str
    yield_3m: Optional[float] = None
    yield_2y: Optional[float] = None
    yield_5y: Optional[float] = None
    yield_10y: Optional[float] = None
    yield_30y: Optional[float] = None

    # Calculated spreads
    spread_2y10y: Optional[float] = None  # Most watched recession indicator
    spread_3m10y: Optional[float] = None
    spread_5y30y: Optional[float] = None

    # State
    curve_state: YieldCurveState = YieldCurveState.NORMAL
    inversion_days: int = 0  # Consecutive days of inversion


@dataclass
class VIXData:
    """VIX and volatility data."""
    date: str
    vix_spot: Optional[float] = None
    vix_1m: Optional[float] = None  # 1-month VIX future
    vix_3m: Optional[float] = None  # 3-month VIX future

    # Term structure
    term_structure: VIXTermStructure = VIXTermStructure.CONTANGO
    contango_pct: float = 0.0  # % difference between futures and spot

    # Levels
    vix_percentile: float = 50.0  # Historical percentile (0-100)
    is_elevated: bool = False  # VIX > 25
    is_extreme: bool = False  # VIX > 35


@dataclass
class CreditData:
    """Credit spread data."""
    date: str
    ig_spread: Optional[float] = None  # Investment grade spread over treasuries
    hy_spread: Optional[float] = None  # High yield spread
    ig_hy_ratio: Optional[float] = None  # Ratio of spreads

    # State
    credit_stress: bool = False  # Spreads widening significantly
    spread_percentile: float = 50.0


@dataclass
class MacroSnapshot:
    """Complete macro environment snapshot."""
    date: str

    # Components
    yield_curve: YieldCurveData = field(default_factory=lambda: YieldCurveData(date=""))
    vix_data: VIXData = field(default_factory=lambda: VIXData(date=""))
    credit_data: CreditData = field(default_factory=lambda: CreditData(date=""))

    # Dollar
    dxy: Optional[float] = None  # Dollar index
    dxy_trend: str = "neutral"  # up, down, neutral

    # Aggregate signals
    regime: MacroRegime = MacroRegime.TRANSITION
    risk_score: float = 50.0  # 0 = max risk-off, 100 = max risk-on
    recession_probability: float = 0.0  # 0-100%

    # Sector tilts based on macro
    sector_tilts: Dict[str, float] = field(default_factory=dict)


class MacroIndicators:
    """Fetch and analyze macro economic indicators."""

    # Historical VIX percentiles for context
    VIX_PERCENTILES = {
        10: 12.0,
        25: 14.0,
        50: 17.0,
        75: 22.0,
        90: 30.0,
        95: 35.0
    }

    # Sector sensitivity to macro factors
    SECTOR_MACRO_SENSITIVITY = {
        # Risk-on sectors
        'Technology': {'risk_on': 1.3, 'low_rates': 1.2, 'strong_dollar': -0.1},
        'Consumer Cyclical': {'risk_on': 1.2, 'low_rates': 1.1, 'strong_dollar': -0.2},
        'Financial': {'risk_on': 1.1, 'low_rates': -0.3, 'strong_dollar': 0.1},
        'Industrials': {'risk_on': 1.1, 'low_rates': 1.0, 'strong_dollar': -0.2},

        # Defensive sectors
        'Utilities': {'risk_on': -0.3, 'low_rates': 1.3, 'strong_dollar': 0.0},
        'Consumer Defensive': {'risk_on': -0.2, 'low_rates': 0.5, 'strong_dollar': -0.1},
        'Healthcare': {'risk_on': 0.0, 'low_rates': 0.3, 'strong_dollar': -0.1},
        'Real Estate': {'risk_on': 0.2, 'low_rates': 1.4, 'strong_dollar': 0.0},

        # Commodity-linked
        'Energy': {'risk_on': 0.8, 'low_rates': 0.2, 'strong_dollar': -0.3},
        'Basic Materials': {'risk_on': 1.0, 'low_rates': 0.3, 'strong_dollar': -0.4},

        # Communication
        'Communication Services': {'risk_on': 0.7, 'low_rates': 0.8, 'strong_dollar': -0.1},
    }

    def __init__(self):
        self._cache: Dict[str, Tuple[datetime, any]] = {}
        self._cache_hours = 4  # Macro data updates less frequently

    def _fetch_treasury_yields(self) -> YieldCurveData:
        """Fetch treasury yields from yfinance."""
        try:
            import yfinance as yf

            # Treasury ETFs as proxies for yields
            tickers = {
                '3m': '^IRX',  # 13-week T-bill
                '2y': None,  # No direct ticker, estimate from TLT/SHY
                '5y': None,
                '10y': '^TNX',  # 10-year yield
                '30y': '^TYX',  # 30-year yield
            }

            data = YieldCurveData(date=datetime.now().strftime("%Y-%m-%d"))

            # Fetch available yields
            for maturity, ticker in tickers.items():
                if ticker:
                    try:
                        yield_data = yf.Ticker(ticker)
                        hist = yield_data.history(period="5d")
                        if not hist.empty:
                            value = hist['Close'].iloc[-1]
                            if maturity == '3m':
                                data.yield_3m = value
                            elif maturity == '10y':
                                data.yield_10y = value
                            elif maturity == '30y':
                                data.yield_30y = value
                    except Exception as e:
                        logger.debug(f"Error fetching {maturity} yield: {e}")

            # Estimate 2Y from short-term ETF (SHY)
            try:
                shy = yf.Ticker("SHY")
                shy_info = shy.info
                if shy_info and 'yield' in shy_info:
                    data.yield_2y = shy_info['yield'] * 100
            except Exception:
                # Fallback: estimate as midpoint between 3m and 10y
                if data.yield_3m and data.yield_10y:
                    data.yield_2y = (data.yield_3m + data.yield_10y) / 2

            # Calculate spreads
            if data.yield_2y and data.yield_10y:
                data.spread_2y10y = data.yield_10y - data.yield_2y

            if data.yield_3m and data.yield_10y:
                data.spread_3m10y = data.yield_10y - data.yield_3m

            if data.yield_5y and data.yield_30y:
                data.spread_5y30y = data.yield_30y - data.yield_5y

            # Determine curve state
            if data.spread_2y10y is not None:
                if data.spread_2y10y < -0.1:
                    data.curve_state = YieldCurveState.INVERTED
                elif data.spread_2y10y < 0.25:
                    data.curve_state = YieldCurveState.FLAT
                else:
                    data.curve_state = YieldCurveState.NORMAL

            return data

        except Exception as e:
            logger.error(f"Error fetching treasury yields: {e}")
            return YieldCurveData(date=datetime.now().strftime("%Y-%m-%d"))

    def _fetch_vix_data(self) -> VIXData:
        """Fetch VIX and volatility data."""
        try:
            import yfinance as yf

            data = VIXData(date=datetime.now().strftime("%Y-%m-%d"))

            # VIX spot
            vix = yf.Ticker("^VIX")
            vix_hist = vix.history(period="1y")

            if not vix_hist.empty:
                data.vix_spot = vix_hist['Close'].iloc[-1]

                # Calculate percentile
                current = data.vix_spot
                historical = vix_hist['Close'].values
                data.vix_percentile = (historical < current).sum() / len(historical) * 100

                # Determine if elevated/extreme
                data.is_elevated = current > 25
                data.is_extreme = current > 35

            # VIX futures proxies (VIXY for short-term, VXZ for mid-term)
            try:
                vixy = yf.Ticker("VIXY")
                vixy_hist = vixy.history(period="5d")
                if not vixy_hist.empty:
                    # Use price changes as proxy for term structure
                    pass
            except Exception:
                pass

            # Estimate term structure from VIX level
            if data.vix_spot:
                # In normal markets, VIX futures are ~5-10% above spot (contango)
                # In stressed markets, spot > futures (backwardation)
                if data.vix_spot > 30:
                    data.term_structure = VIXTermStructure.BACKWARDATION
                    data.contango_pct = -5.0  # Estimate
                elif data.vix_spot > 20:
                    data.term_structure = VIXTermStructure.FLAT
                    data.contango_pct = 2.0
                else:
                    data.term_structure = VIXTermStructure.CONTANGO
                    data.contango_pct = 7.0  # Normal contango

            return data

        except Exception as e:
            logger.error(f"Error fetching VIX data: {e}")
            return VIXData(date=datetime.now().strftime("%Y-%m-%d"))

    def _fetch_credit_data(self) -> CreditData:
        """Fetch credit spread data from ETF proxies."""
        try:
            import yfinance as yf

            data = CreditData(date=datetime.now().strftime("%Y-%m-%d"))

            # Use ETF yields as proxies
            # LQD = Investment Grade Corporate
            # HYG = High Yield Corporate
            # TLT = Long-term Treasury

            try:
                lqd = yf.Ticker("LQD")
                hyg = yf.Ticker("HYG")
                tlt = yf.Ticker("TLT")

                lqd_info = lqd.info
                hyg_info = hyg.info
                tlt_info = tlt.info

                # Get yields
                lqd_yield = lqd_info.get('yield', 0) * 100 if lqd_info else 0
                hyg_yield = hyg_info.get('yield', 0) * 100 if hyg_info else 0
                tlt_yield = tlt_info.get('yield', 0) * 100 if tlt_info else 0

                # Calculate spreads
                if lqd_yield and tlt_yield:
                    data.ig_spread = lqd_yield - tlt_yield

                if hyg_yield and tlt_yield:
                    data.hy_spread = hyg_yield - tlt_yield

                if data.ig_spread and data.hy_spread and data.ig_spread > 0:
                    data.ig_hy_ratio = data.hy_spread / data.ig_spread

                # Detect credit stress (HY spread > 5% typically signals stress)
                if data.hy_spread and data.hy_spread > 5:
                    data.credit_stress = True

            except Exception as e:
                logger.debug(f"Error fetching credit data: {e}")

            return data

        except Exception as e:
            logger.error(f"Error fetching credit data: {e}")
            return CreditData(date=datetime.now().strftime("%Y-%m-%d"))

    def _fetch_dollar_data(self) -> Tuple[Optional[float], str]:
        """Fetch dollar index data."""
        try:
            import yfinance as yf

            uup = yf.Ticker("UUP")  # Dollar bull ETF as proxy
            hist = uup.history(period="3mo")

            if hist.empty:
                return None, "neutral"

            current = hist['Close'].iloc[-1]
            sma_20 = hist['Close'].rolling(20).mean().iloc[-1]
            sma_50 = hist['Close'].rolling(50).mean().iloc[-1]

            # Determine trend
            if current > sma_20 > sma_50:
                trend = "up"
            elif current < sma_20 < sma_50:
                trend = "down"
            else:
                trend = "neutral"

            return current, trend

        except Exception as e:
            logger.error(f"Error fetching dollar data: {e}")
            return None, "neutral"

    def get_macro_snapshot(self, force_refresh: bool = False) -> MacroSnapshot:
        """Get complete macro environment snapshot."""
        cache_key = "macro_snapshot"

        # Check cache
        if not force_refresh and cache_key in self._cache:
            cache_time, cached_data = self._cache[cache_key]
            if datetime.now() - cache_time < timedelta(hours=self._cache_hours):
                return cached_data

        # Fetch all components
        yield_curve = self._fetch_treasury_yields()
        vix_data = self._fetch_vix_data()
        credit_data = self._fetch_credit_data()
        dxy, dxy_trend = self._fetch_dollar_data()

        snapshot = MacroSnapshot(
            date=datetime.now().strftime("%Y-%m-%d"),
            yield_curve=yield_curve,
            vix_data=vix_data,
            credit_data=credit_data,
            dxy=dxy,
            dxy_trend=dxy_trend
        )

        # Calculate aggregate regime and scores
        snapshot = self._calculate_regime(snapshot)
        snapshot = self._calculate_sector_tilts(snapshot)

        # Cache
        self._cache[cache_key] = (datetime.now(), snapshot)

        return snapshot

    def _calculate_regime(self, snapshot: MacroSnapshot) -> MacroSnapshot:
        """Calculate overall macro regime and risk score."""
        risk_score = 50.0  # Start neutral
        recession_signals = 0

        # Yield curve contribution
        if snapshot.yield_curve.curve_state == YieldCurveState.INVERTED:
            risk_score -= 20
            recession_signals += 2
        elif snapshot.yield_curve.curve_state == YieldCurveState.FLAT:
            risk_score -= 10
            recession_signals += 1
        elif snapshot.yield_curve.curve_state == YieldCurveState.NORMAL:
            if snapshot.yield_curve.spread_2y10y and snapshot.yield_curve.spread_2y10y > 1.0:
                risk_score += 10  # Healthy steep curve

        # VIX contribution
        if snapshot.vix_data.is_extreme:
            risk_score -= 25
        elif snapshot.vix_data.is_elevated:
            risk_score -= 15
        elif snapshot.vix_data.vix_spot and snapshot.vix_data.vix_spot < 15:
            risk_score += 15  # Low volatility = complacency or calm

        if snapshot.vix_data.term_structure == VIXTermStructure.BACKWARDATION:
            risk_score -= 10  # Stress signal

        # Credit contribution
        if snapshot.credit_data.credit_stress:
            risk_score -= 15
            recession_signals += 1

        # Dollar contribution (strong dollar can hurt multinationals)
        if snapshot.dxy_trend == "up":
            risk_score -= 5
        elif snapshot.dxy_trend == "down":
            risk_score += 5

        # Clamp score
        snapshot.risk_score = max(0, min(100, risk_score))

        # Calculate recession probability (simple heuristic)
        snapshot.recession_probability = min(100, recession_signals * 25)

        # Determine regime
        if snapshot.risk_score >= 65:
            snapshot.regime = MacroRegime.RISK_ON
        elif snapshot.risk_score <= 35:
            if recession_signals >= 2:
                snapshot.regime = MacroRegime.RECESSION_WARNING
            else:
                snapshot.regime = MacroRegime.RISK_OFF
        else:
            snapshot.regime = MacroRegime.TRANSITION

        return snapshot

    def _calculate_sector_tilts(self, snapshot: MacroSnapshot) -> MacroSnapshot:
        """Calculate sector tilts based on macro environment."""
        tilts = {}

        for sector, sensitivities in self.SECTOR_MACRO_SENSITIVITY.items():
            tilt = 0.0

            # Risk-on/off contribution
            risk_factor = (snapshot.risk_score - 50) / 50  # -1 to 1
            tilt += sensitivities['risk_on'] * risk_factor * 10

            # Interest rate contribution (using VIX as proxy for risk-free rate expectations)
            if snapshot.yield_curve.yield_10y:
                rate_factor = -1 if snapshot.yield_curve.yield_10y > 4.5 else (
                    1 if snapshot.yield_curve.yield_10y < 3.0 else 0
                )
                tilt += sensitivities['low_rates'] * rate_factor * 5

            # Dollar contribution
            dollar_factor = 1 if snapshot.dxy_trend == "up" else (
                -1 if snapshot.dxy_trend == "down" else 0
            )
            tilt += sensitivities['strong_dollar'] * dollar_factor * 5

            tilts[sector] = round(tilt, 1)

        snapshot.sector_tilts = tilts
        return snapshot

    def get_macro_features(self) -> pd.DataFrame:
        """Create features for ML model from macro data."""
        snapshot = self.get_macro_snapshot()

        features = {
            'yield_2y10y_spread': snapshot.yield_curve.spread_2y10y,
            'yield_curve_inverted': 1 if snapshot.yield_curve.curve_state == YieldCurveState.INVERTED else 0,
            'vix_level': snapshot.vix_data.vix_spot,
            'vix_percentile': snapshot.vix_data.vix_percentile,
            'vix_elevated': 1 if snapshot.vix_data.is_elevated else 0,
            'vix_backwardation': 1 if snapshot.vix_data.term_structure == VIXTermStructure.BACKWARDATION else 0,
            'credit_stress': 1 if snapshot.credit_data.credit_stress else 0,
            'hy_spread': snapshot.credit_data.hy_spread,
            'dollar_trend_up': 1 if snapshot.dxy_trend == "up" else 0,
            'dollar_trend_down': 1 if snapshot.dxy_trend == "down" else 0,
            'macro_risk_score': snapshot.risk_score,
            'recession_probability': snapshot.recession_probability,
            'regime_risk_on': 1 if snapshot.regime == MacroRegime.RISK_ON else 0,
            'regime_risk_off': 1 if snapshot.regime == MacroRegime.RISK_OFF else 0,
        }

        return pd.DataFrame([features])

    def get_position_size_multiplier(self) -> float:
        """Get position size multiplier based on macro conditions."""
        snapshot = self.get_macro_snapshot()

        if snapshot.regime == MacroRegime.RECESSION_WARNING:
            return 0.5  # Half positions
        elif snapshot.regime == MacroRegime.RISK_OFF:
            return 0.7  # Reduced positions
        elif snapshot.regime == MacroRegime.RISK_ON:
            return 1.1  # Slightly increased
        else:
            return 1.0  # Normal


def get_macro_summary() -> Dict:
    """Get a summary of current macro conditions."""
    indicators = MacroIndicators()
    snapshot = indicators.get_macro_snapshot()

    return {
        'date': snapshot.date,
        'regime': snapshot.regime.value,
        'risk_score': snapshot.risk_score,
        'recession_probability': snapshot.recession_probability,
        'yield_curve': {
            'state': snapshot.yield_curve.curve_state.value,
            'spread_2y10y': snapshot.yield_curve.spread_2y10y,
            '10y_yield': snapshot.yield_curve.yield_10y,
        },
        'volatility': {
            'vix': snapshot.vix_data.vix_spot,
            'percentile': snapshot.vix_data.vix_percentile,
            'term_structure': snapshot.vix_data.term_structure.value,
        },
        'credit': {
            'hy_spread': snapshot.credit_data.hy_spread,
            'stress': snapshot.credit_data.credit_stress,
        },
        'dollar': {
            'trend': snapshot.dxy_trend,
        },
        'sector_tilts': snapshot.sector_tilts,
        'position_size_mult': indicators.get_position_size_multiplier(),
    }
