"""
FastAPI backend for Finviz Weekly screening results.

Provides RESTful API for programmatic access to screening data, alerts, and portfolios.
"""
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Initialize FastAPI app
app = FastAPI(
    title="Finviz Weekly API",
    description="RESTful API for stock screening and investment analysis",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data directory
DATA_DIR = Path("/app/data/latest")


# ============================================================================
# Models
# ============================================================================


class Stock(BaseModel):
    """Stock screening result."""

    ticker: str
    company: Optional[str] = None
    sector: Optional[str] = None
    price: Optional[float] = None
    market_cap: Optional[str] = None
    score_master: Optional[float] = None
    score_quality_value: Optional[float] = None
    zone_label: Optional[str] = None


class Theme(BaseModel):
    """Investment theme with top stocks."""

    name: str
    description: str
    stocks: List[Stock]


class Portfolio(BaseModel):
    """Portfolio construction result."""

    positions: List[dict]
    num_positions: int
    num_sectors: int
    sector_weights: dict
    metadata: Optional[dict] = None


class AlertConfig(BaseModel):
    """Alert configuration."""

    enabled: bool = True
    master_score_threshold: float = Field(80.0, ge=0, le=100)
    insider_buying_min: float = Field(1000000.0, ge=0)
    dedup_days: int = Field(7, ge=1, le=30)


class BacktestRequest(BaseModel):
    """Backtest request parameters."""

    score_column: str = "score_master"
    start_date: str
    end_date: str
    top_n: int = Field(20, ge=5, le=100)
    rebalance_days: int = Field(7, ge=1, le=30)


class BacktestResult(BaseModel):
    """Backtest result."""

    total_return: float
    annual_return: float
    sharpe_ratio: float
    max_drawdown: float
    num_trades: int


# ============================================================================
# Helper Functions
# ============================================================================


def load_scored_data() -> pd.DataFrame:
    """Load latest scored screening results."""
    scored_path = DATA_DIR / "finviz_scored.parquet"
    if not scored_path.exists():
        raise HTTPException(status_code=404, detail="Scored data not found. Run screening first.")
    return pd.read_parquet(scored_path)


def load_theme_data(theme_name: str) -> pd.DataFrame:
    """Load specific theme results."""
    theme_path = DATA_DIR / f"top50_{theme_name}.csv"
    if not theme_path.exists():
        raise HTTPException(status_code=404, detail=f"Theme '{theme_name}' not found")
    return pd.read_csv(theme_path)


# ============================================================================
# Endpoints
# ============================================================================


@app.get("/", tags=["Root"])
async def root():
    """API root endpoint."""
    return {
        "name": "Finviz Weekly API",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": {
            "screening": "/api/v1/screen",
            "themes": "/api/v1/themes",
            "stocks": "/api/v1/stocks/{ticker}",
            "portfolio": "/api/v1/portfolio",
            "alerts": "/api/v1/alerts",
        },
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "data_available": (DATA_DIR / "finviz_scored.parquet").exists(),
    }


@app.get("/api/v1/screen", response_model=List[Stock], tags=["Screening"])
async def get_screening_results(
    limit: int = Query(50, ge=1, le=500, description="Number of results to return"),
    sort_by: str = Query("score_master", description="Column to sort by"),
    sector: Optional[str] = Query(None, description="Filter by sector"),
    min_score: Optional[float] = Query(None, description="Minimum score filter"),
):
    """
    Get latest screening results.

    Returns scored stocks sorted by specified column.
    """
    df = load_scored_data()

    # Apply filters
    if sector:
        df = df[df["sector"] == sector]

    if min_score and sort_by in df.columns:
        df = df[df[sort_by] >= min_score]

    # Sort and limit
    if sort_by in df.columns:
        df = df.sort_values(sort_by, ascending=False)

    df = df.head(limit)

    # Convert to Stock models
    stocks = []
    for _, row in df.iterrows():
        stocks.append(
            Stock(
                ticker=str(row["ticker"]),
                company=str(row.get("company", "")),
                sector=str(row.get("sector", "")),
                price=float(row.get("price", 0)) if pd.notna(row.get("price")) else None,
                market_cap=str(row.get("market_cap", "")),
                score_master=float(row.get("score_master", 0)) if pd.notna(row.get("score_master")) else None,
                score_quality_value=(
                    float(row.get("score_quality_value", 0))
                    if pd.notna(row.get("score_quality_value"))
                    else None
                ),
                zone_label=str(row.get("zone_label", "")),
            )
        )

    return stocks


@app.get("/api/v1/themes", tags=["Screening"])
async def list_themes():
    """List available investment themes."""
    theme_files = list(DATA_DIR.glob("top50_*.csv"))

    themes = []
    for f in theme_files:
        theme_name = f.stem.replace("top50_", "")
        themes.append(
            {
                "name": theme_name,
                "display_name": theme_name.replace("_", " ").title(),
                "url": f"/api/v1/themes/{theme_name}",
            }
        )

    return themes


@app.get("/api/v1/themes/{theme_name}", tags=["Screening"])
async def get_theme_results(
    theme_name: str,
    limit: int = Query(20, ge=1, le=50, description="Number of stocks to return"),
):
    """Get stocks for a specific investment theme."""
    df = load_theme_data(theme_name)
    df = df.head(limit)

    stocks = []
    for _, row in df.iterrows():
        stocks.append(
            {
                "ticker": str(row.get("ticker", "")),
                "company": str(row.get("company", "")),
                "sector": str(row.get("sector", "")),
                "score": float(row.get("score", 0)) if pd.notna(row.get("score")) else None,
            }
        )

    return {"theme": theme_name, "stocks": stocks}


@app.get("/api/v1/stocks/{ticker}", tags=["Stocks"])
async def get_stock_details(ticker: str):
    """Get detailed information for a specific stock."""
    df = load_scored_data()

    stock_data = df[df["ticker"].str.upper() == ticker.upper()]

    if stock_data.empty:
        raise HTTPException(status_code=404, detail=f"Stock '{ticker}' not found")

    row = stock_data.iloc[0]

    # Get all columns for this stock
    details = {}
    for col in df.columns:
        val = row[col]
        if pd.notna(val):
            details[col] = float(val) if isinstance(val, (int, float)) else str(val)

    return details


@app.get("/api/v1/sectors", tags=["Stocks"])
async def get_sectors():
    """Get list of available sectors with stock counts."""
    df = load_scored_data()

    if "sector" not in df.columns:
        return []

    sector_counts = df["sector"].value_counts()

    sectors = [{"sector": sector, "count": int(count)} for sector, count in sector_counts.items()]

    return sorted(sectors, key=lambda x: -x["count"])


@app.get("/api/v1/portfolio", tags=["Portfolio"])
async def build_portfolio(
    num_positions: int = Query(20, ge=5, le=50, description="Number of positions"),
    score_column: str = Query("score_master", description="Score column to use"),
    max_sector_weight: float = Query(0.30, ge=0.1, le=1.0, description="Max sector weight"),
):
    """Build a diversified portfolio from screening results."""
    try:
        from finviz_weekly.portfolio import PortfolioBuilder, PortfolioConfig

        df = load_scored_data()

        config = PortfolioConfig(
            num_positions=num_positions,
            max_sector_weight=max_sector_weight,
        )

        builder = PortfolioBuilder(config)
        portfolio = builder.build_portfolio(df, score_column)

        # Convert to dict
        return {
            "positions": [
                {
                    "ticker": p.ticker,
                    "company": p.company,
                    "sector": p.sector,
                    "weight": p.weight,
                    "score": p.score,
                    "reason": p.reason,
                }
                for p in portfolio.positions
            ],
            "num_positions": len(portfolio.positions),
            "num_sectors": portfolio.num_sectors,
            "sector_weights": portfolio.sector_weights,
            "total_weight": portfolio.total_weight,
        }

    except ImportError:
        raise HTTPException(
            status_code=500, detail="Portfolio module not available. Install required dependencies."
        )


@app.get("/api/v1/stats", tags=["Statistics"])
async def get_statistics():
    """Get screening statistics and summary metrics."""
    df = load_scored_data()

    stats = {
        "total_stocks": len(df),
        "avg_master_score": float(df["score_master"].mean()) if "score_master" in df.columns else None,
        "sectors": int(df["sector"].nunique()) if "sector" in df.columns else None,
        "enhanced_data": {
            "insider": "insider_net_value" in df.columns,
            "earnings": "earnings_avg_alpha" in df.columns,
            "financials": "net_margin" in df.columns and "roe" in df.columns,
        },
    }

    return stats


@app.post("/api/v1/backtest", response_model=BacktestResult, tags=["Backtesting"])
async def run_backtest(request: BacktestRequest):
    """
    Run a backtest on historical data.

    Requires historical data to be available.
    """
    history_path = Path("/app/data/history/finviz_scored_history.parquet")

    if not history_path.exists():
        raise HTTPException(
            status_code=404, detail="Historical data not found. Collect data over time to enable backtesting."
        )

    try:
        from finviz_weekly.backtest import Backtester, BacktestConfig

        backtester = Backtester(history_path)

        config = BacktestConfig(
            start_date=request.start_date,
            end_date=request.end_date,
            score_column=request.score_column,
            top_n=request.top_n,
            rebalance_days=request.rebalance_days,
        )

        results = backtester.run(config)

        return BacktestResult(
            total_return=results.total_return,
            annual_return=results.annual_return,
            sharpe_ratio=results.sharpe_ratio,
            max_drawdown=results.max_drawdown,
            num_trades=len(results.trades),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Backtest failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
