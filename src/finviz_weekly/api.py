"""
FastAPI REST Service for Finviz Scraper

Provides REST API endpoints for:
- Stock screening
- Portfolio optimization
- Risk analysis
- Trading signals
- Data retrieval
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

# FastAPI imports (with graceful fallback)
try:
    from fastapi import FastAPI, HTTPException, Query, Depends, BackgroundTasks
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel, Field
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    # Create dummy classes for type hints
    class BaseModel:
        pass
    class FastAPI:
        pass

LOGGER = logging.getLogger(__name__)


# =============================================================================
# Pydantic Models
# =============================================================================

if FASTAPI_AVAILABLE:
    class ScreeningRequest(BaseModel):
        """Request model for stock screening."""
        top_n: int = Field(default=50, ge=1, le=500)
        min_market_cap: float = Field(default=300_000_000)
        min_price: float = Field(default=1.0)
        theme: str = Field(default="enhanced_master")
        sectors: Optional[List[str]] = None
        exclude_tickers: Optional[List[str]] = None


    class ScreeningResponse(BaseModel):
        """Response model for screening results."""
        timestamp: str
        theme: str
        count: int
        stocks: List[Dict[str, Any]]


    class OptimizationRequest(BaseModel):
        """Request model for portfolio optimization."""
        tickers: List[str]
        method: str = Field(default="mean_variance")
        objective: str = Field(default="max_sharpe")
        constraints: Optional[Dict[str, Any]] = None
        lookback_days: int = Field(default=252)


    class OptimizationResponse(BaseModel):
        """Response model for optimization results."""
        weights: Dict[str, float]
        expected_return: float
        expected_volatility: float
        sharpe_ratio: float
        effective_n: float


    class RiskAnalysisRequest(BaseModel):
        """Request model for risk analysis."""
        portfolio: Dict[str, float]  # ticker -> weight
        analysis_type: str = Field(default="var")  # var, monte_carlo, stress
        confidence: float = Field(default=0.95)
        horizon_days: int = Field(default=1)


    class RiskAnalysisResponse(BaseModel):
        """Response model for risk analysis."""
        var: float
        cvar: float
        volatility: float
        max_drawdown: Optional[float] = None
        details: Dict[str, Any]


    class SignalRequest(BaseModel):
        """Request model for trading signals."""
        tickers: List[str]
        signal_type: str = Field(default="composite")  # composite, momentum, value, etc.


    class SignalResponse(BaseModel):
        """Response model for signals."""
        signals: Dict[str, Dict[str, Any]]
        timestamp: str


    class RebalanceRequest(BaseModel):
        """Request model for rebalancing."""
        current_positions: Dict[str, Dict[str, float]]  # ticker -> {shares, price}
        target_weights: Dict[str, float]
        portfolio_value: float


    class RebalanceResponse(BaseModel):
        """Response model for rebalancing."""
        needs_rebalance: bool
        trades: List[Dict[str, Any]]
        turnover: float
        estimated_cost: float


    class HealthResponse(BaseModel):
        """Response model for health check."""
        status: str
        version: str
        timestamp: str


# =============================================================================
# API Application
# =============================================================================

def create_app(data_dir: str = "data") -> FastAPI:
    """
    Create and configure FastAPI application.

    Args:
        data_dir: Path to data directory

    Returns:
        Configured FastAPI app
    """
    if not FASTAPI_AVAILABLE:
        raise ImportError(
            "FastAPI not installed. Install with: pip install fastapi uvicorn"
        )

    app = FastAPI(
        title="Finviz Scraper API",
        description="REST API for quantitative stock screening and portfolio management",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Store data directory in app state
    app.state.data_dir = Path(data_dir)

    # Register routes
    _register_routes(app)

    return app


def _register_routes(app: FastAPI) -> None:
    """Register all API routes."""

    @app.get("/", response_model=HealthResponse)
    async def root():
        """Root endpoint with health check."""
        return HealthResponse(
            status="healthy",
            version="1.0.0",
            timestamp=datetime.now().isoformat(),
        )

    @app.get("/health", response_model=HealthResponse)
    async def health_check():
        """Health check endpoint."""
        return HealthResponse(
            status="healthy",
            version="1.0.0",
            timestamp=datetime.now().isoformat(),
        )

    # -------------------------------------------------------------------------
    # Screening Endpoints
    # -------------------------------------------------------------------------

    @app.post("/api/v1/screen", response_model=ScreeningResponse)
    async def screen_stocks(request: ScreeningRequest):
        """
        Screen stocks based on criteria.

        Returns top stocks matching the screening parameters.
        """
        try:
            data_path = app.state.data_dir / "latest" / "finviz_fundamentals.parquet"

            if not data_path.exists():
                raise HTTPException(
                    status_code=404,
                    detail="No screening data available. Run the scraper first.",
                )

            df = pd.read_parquet(data_path)

            # Apply filters
            if request.min_market_cap:
                if 'market_cap' in df.columns:
                    df = df[df['market_cap'] >= request.min_market_cap]

            if request.min_price:
                if 'price' in df.columns:
                    df = df[df['price'] >= request.min_price]

            if request.sectors:
                if 'sector' in df.columns:
                    df = df[df['sector'].isin(request.sectors)]

            if request.exclude_tickers:
                if 'ticker' in df.columns:
                    df = df[~df['ticker'].isin(request.exclude_tickers)]

            # Sort by score
            score_col = f"{request.theme}_score" if f"{request.theme}_score" in df.columns else 'total_score'
            if score_col not in df.columns:
                score_col = df.select_dtypes(include=[np.number]).columns[0]

            df = df.sort_values(score_col, ascending=False).head(request.top_n)

            # Convert to response
            stocks = df.to_dict('records')

            return ScreeningResponse(
                timestamp=datetime.now().isoformat(),
                theme=request.theme,
                count=len(stocks),
                stocks=stocks,
            )

        except Exception as e:
            LOGGER.error(f"Screening error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/api/v1/screen/themes")
    async def get_available_themes():
        """Get list of available screening themes."""
        return {
            "themes": [
                "enhanced_master",
                "insider_momentum",
                "earnings_surprise",
                "quality_growth_enhanced",
                "value",
                "momentum",
                "quality",
                "growth",
            ]
        }

    # -------------------------------------------------------------------------
    # Optimization Endpoints
    # -------------------------------------------------------------------------

    @app.post("/api/v1/optimize", response_model=OptimizationResponse)
    async def optimize_portfolio(request: OptimizationRequest):
        """
        Optimize portfolio weights.

        Supports multiple optimization methods.
        """
        try:
            from .portfolio_optimizer import optimize_portfolio, OptimizationObjective

            # Get price data
            prices_path = app.state.data_dir / "history" / "prices.parquet"

            if not prices_path.exists():
                raise HTTPException(
                    status_code=404,
                    detail="No price history available.",
                )

            prices_df = pd.read_parquet(prices_path)

            # Filter to requested tickers
            available_tickers = [t for t in request.tickers if t in prices_df.columns]

            if len(available_tickers) < 2:
                raise HTTPException(
                    status_code=400,
                    detail="Need at least 2 valid tickers for optimization.",
                )

            # Calculate returns
            returns = prices_df[available_tickers].pct_change().dropna()

            # Limit lookback
            if len(returns) > request.lookback_days:
                returns = returns.tail(request.lookback_days)

            # Map objective string to enum
            objective_map = {
                "max_sharpe": OptimizationObjective.MAX_SHARPE,
                "min_variance": OptimizationObjective.MIN_VARIANCE,
                "risk_parity": OptimizationObjective.RISK_PARITY,
                "max_return": OptimizationObjective.MAX_RETURN,
            }
            objective = objective_map.get(
                request.objective,
                OptimizationObjective.MAX_SHARPE,
            )

            # Run optimization
            result = optimize_portfolio(
                returns,
                method=request.method,
                objective=objective,
            )

            return OptimizationResponse(
                weights=dict(zip(result.tickers, result.weights.tolist())),
                expected_return=result.expected_return,
                expected_volatility=result.expected_volatility,
                sharpe_ratio=result.sharpe_ratio,
                effective_n=result.effective_n,
            )

        except Exception as e:
            LOGGER.error(f"Optimization error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/api/v1/optimize/methods")
    async def get_optimization_methods():
        """Get list of available optimization methods."""
        return {
            "methods": [
                {"name": "mean_variance", "description": "Classic Markowitz Mean-Variance"},
                {"name": "hrp", "description": "Hierarchical Risk Parity"},
                {"name": "risk_parity", "description": "Equal Risk Contribution"},
                {"name": "black_litterman", "description": "Black-Litterman with views"},
            ],
            "objectives": [
                {"name": "max_sharpe", "description": "Maximize Sharpe Ratio"},
                {"name": "min_variance", "description": "Minimum Variance"},
                {"name": "risk_parity", "description": "Risk Parity"},
                {"name": "max_return", "description": "Maximum Return"},
            ],
        }

    # -------------------------------------------------------------------------
    # Risk Analysis Endpoints
    # -------------------------------------------------------------------------

    @app.post("/api/v1/risk", response_model=RiskAnalysisResponse)
    async def analyze_risk(request: RiskAnalysisRequest):
        """
        Analyze portfolio risk.

        Calculates VaR, CVaR, and other risk metrics.
        """
        try:
            from .monte_carlo import calculate_var, calculate_cvar

            # Get price data
            prices_path = app.state.data_dir / "history" / "prices.parquet"

            if not prices_path.exists():
                raise HTTPException(
                    status_code=404,
                    detail="No price history available.",
                )

            prices_df = pd.read_parquet(prices_path)

            # Get tickers from portfolio
            tickers = list(request.portfolio.keys())
            weights = np.array(list(request.portfolio.values()))
            weights = weights / weights.sum()  # Normalize

            available = [t for t in tickers if t in prices_df.columns]
            if len(available) < 1:
                raise HTTPException(
                    status_code=400,
                    detail="No valid tickers in portfolio.",
                )

            # Calculate portfolio returns
            returns = prices_df[available].pct_change().dropna()
            port_weights = np.array([
                request.portfolio.get(t, 0) for t in available
            ])
            port_weights = port_weights / port_weights.sum()

            portfolio_returns = (returns @ port_weights).values

            # Calculate risk metrics
            var = calculate_var(portfolio_returns, request.confidence)
            cvar = calculate_cvar(portfolio_returns, request.confidence)
            volatility = np.std(portfolio_returns) * np.sqrt(252)

            # Max drawdown
            cumulative = (1 + pd.Series(portfolio_returns)).cumprod()
            rolling_max = cumulative.expanding().max()
            drawdown = (cumulative - rolling_max) / rolling_max
            max_dd = abs(drawdown.min())

            return RiskAnalysisResponse(
                var=var,
                cvar=cvar,
                volatility=volatility,
                max_drawdown=max_dd,
                details={
                    "confidence": request.confidence,
                    "horizon_days": request.horizon_days,
                    "num_observations": len(portfolio_returns),
                },
            )

        except Exception as e:
            LOGGER.error(f"Risk analysis error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    # -------------------------------------------------------------------------
    # Signal Endpoints
    # -------------------------------------------------------------------------

    @app.post("/api/v1/signals", response_model=SignalResponse)
    async def get_signals(request: SignalRequest):
        """
        Get trading signals for tickers.
        """
        try:
            data_path = app.state.data_dir / "latest" / "finviz_fundamentals.parquet"

            if not data_path.exists():
                raise HTTPException(
                    status_code=404,
                    detail="No data available.",
                )

            df = pd.read_parquet(data_path)

            if 'ticker' not in df.columns:
                raise HTTPException(
                    status_code=500,
                    detail="Invalid data format.",
                )

            signals = {}
            for ticker in request.tickers:
                ticker_data = df[df['ticker'] == ticker]

                if ticker_data.empty:
                    signals[ticker] = {"error": "Not found"}
                    continue

                row = ticker_data.iloc[0]

                # Build signal response
                signal_data = {
                    "ticker": ticker,
                    "signal": "neutral",
                    "strength": 50,
                }

                # Check for score columns
                if 'total_score' in row:
                    score = row['total_score']
                    signal_data['strength'] = int(score)
                    if score >= 70:
                        signal_data['signal'] = "strong_buy"
                    elif score >= 60:
                        signal_data['signal'] = "buy"
                    elif score <= 30:
                        signal_data['signal'] = "strong_sell"
                    elif score <= 40:
                        signal_data['signal'] = "sell"

                # Add component signals
                for col in ['momentum_score', 'value_score', 'quality_score', 'growth_score']:
                    if col in row:
                        signal_data[col.replace('_score', '')] = float(row[col])

                signals[ticker] = signal_data

            return SignalResponse(
                signals=signals,
                timestamp=datetime.now().isoformat(),
            )

        except Exception as e:
            LOGGER.error(f"Signal error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    # -------------------------------------------------------------------------
    # Rebalancing Endpoints
    # -------------------------------------------------------------------------

    @app.post("/api/v1/rebalance", response_model=RebalanceResponse)
    async def calculate_rebalance(request: RebalanceRequest):
        """
        Calculate rebalancing trades.
        """
        try:
            from .rebalancer import generate_rebalance_trades

            # Calculate current weights
            positions = request.current_positions
            total_value = request.portfolio_value

            current_weights = {}
            prices = {}
            for ticker, pos in positions.items():
                value = pos['shares'] * pos['price']
                current_weights[ticker] = value / total_value if total_value > 0 else 0
                prices[ticker] = pos['price']

            # Generate trades
            trades = generate_rebalance_trades(
                current_weights,
                request.target_weights,
                total_value,
                prices,
            )

            # Calculate turnover
            turnover = sum(t['value'] for t in trades) / total_value if total_value > 0 else 0

            return RebalanceResponse(
                needs_rebalance=len(trades) > 0,
                trades=trades,
                turnover=turnover,
                estimated_cost=turnover * 0.001,  # 10bps
            )

        except Exception as e:
            LOGGER.error(f"Rebalance error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    # -------------------------------------------------------------------------
    # Data Endpoints
    # -------------------------------------------------------------------------

    @app.get("/api/v1/tickers")
    async def get_tickers():
        """Get list of available tickers."""
        try:
            data_path = app.state.data_dir / "latest" / "finviz_fundamentals.parquet"

            if not data_path.exists():
                return {"tickers": []}

            df = pd.read_parquet(data_path)

            if 'ticker' in df.columns:
                tickers = df['ticker'].tolist()
            else:
                tickers = []

            return {"tickers": tickers, "count": len(tickers)}

        except Exception as e:
            LOGGER.error(f"Error getting tickers: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/api/v1/ticker/{ticker}")
    async def get_ticker_data(ticker: str):
        """Get data for a specific ticker."""
        try:
            data_path = app.state.data_dir / "latest" / "finviz_fundamentals.parquet"

            if not data_path.exists():
                raise HTTPException(status_code=404, detail="No data available.")

            df = pd.read_parquet(data_path)

            if 'ticker' not in df.columns:
                raise HTTPException(status_code=500, detail="Invalid data format.")

            ticker_data = df[df['ticker'] == ticker.upper()]

            if ticker_data.empty:
                raise HTTPException(status_code=404, detail=f"Ticker {ticker} not found.")

            return ticker_data.iloc[0].to_dict()

        except HTTPException:
            raise
        except Exception as e:
            LOGGER.error(f"Error getting ticker data: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/api/v1/sectors")
    async def get_sectors():
        """Get list of sectors with stock counts."""
        try:
            data_path = app.state.data_dir / "latest" / "finviz_fundamentals.parquet"

            if not data_path.exists():
                return {"sectors": []}

            df = pd.read_parquet(data_path)

            if 'sector' not in df.columns:
                return {"sectors": []}

            sector_counts = df['sector'].value_counts().to_dict()

            return {
                "sectors": [
                    {"name": sector, "count": count}
                    for sector, count in sector_counts.items()
                ]
            }

        except Exception as e:
            LOGGER.error(f"Error getting sectors: {e}")
            raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Server Runner
# =============================================================================

def run_server(
    host: str = "0.0.0.0",
    port: int = 8000,
    data_dir: str = "data",
    reload: bool = False,
):
    """
    Run the API server.

    Args:
        host: Host to bind to
        port: Port to listen on
        data_dir: Path to data directory
        reload: Enable auto-reload for development
    """
    try:
        import uvicorn
    except ImportError:
        raise ImportError(
            "uvicorn not installed. Install with: pip install uvicorn"
        )

    app = create_app(data_dir)

    uvicorn.run(
        app,
        host=host,
        port=port,
        reload=reload,
    )


# Application instance for direct import
if FASTAPI_AVAILABLE:
    app = create_app()
