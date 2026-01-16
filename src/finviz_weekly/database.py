"""
PostgreSQL/TimescaleDB Integration

Provides database storage for:
- Historical fundamentals data
- Price data (time-series optimized)
- Screening results
- Portfolio positions
- Trade history
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, Generator, List, Optional, Tuple

import pandas as pd

LOGGER = logging.getLogger(__name__)

# Optional database imports
try:
    import psycopg2
    from psycopg2 import pool, sql
    from psycopg2.extras import execute_values, RealDictCursor
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

try:
    from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Date, Text
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker, Session
    SQLALCHEMY_AVAILABLE = True
    Base = declarative_base()
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    Base = None


@dataclass
class DatabaseConfig:
    """Database configuration."""
    host: str = "localhost"
    port: int = 5432
    database: str = "finviz"
    user: str = "finviz"
    password: str = ""
    pool_min: int = 1
    pool_max: int = 10

    # TimescaleDB settings
    use_timescale: bool = True
    chunk_interval: str = "7 days"

    @classmethod
    def from_env(cls) -> 'DatabaseConfig':
        """Create config from environment variables."""
        return cls(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '5432')),
            database=os.getenv('DB_NAME', 'finviz'),
            user=os.getenv('DB_USER', 'finviz'),
            password=os.getenv('DB_PASSWORD', ''),
            use_timescale=os.getenv('USE_TIMESCALE', 'true').lower() == 'true',
        )

    @property
    def connection_string(self) -> str:
        """Get psycopg2 connection string."""
        return f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"

    @property
    def sqlalchemy_url(self) -> str:
        """Get SQLAlchemy URL."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


# =============================================================================
# Schema Definitions
# =============================================================================

SCHEMA_SQL = """
-- Fundamentals table
CREATE TABLE IF NOT EXISTS fundamentals (
    id SERIAL,
    ticker VARCHAR(10) NOT NULL,
    as_of_date DATE NOT NULL,
    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Basic info
    company VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    country VARCHAR(50),

    -- Valuation
    market_cap BIGINT,
    price NUMERIC(12, 4),
    pe NUMERIC(10, 2),
    forward_pe NUMERIC(10, 2),
    peg NUMERIC(10, 2),
    ps NUMERIC(10, 2),
    pb NUMERIC(10, 2),
    pc NUMERIC(10, 2),
    pfcf NUMERIC(10, 2),

    -- Dividends
    dividend_yield NUMERIC(8, 4),
    payout_ratio NUMERIC(8, 4),

    -- Profitability
    profit_margin NUMERIC(8, 4),
    operating_margin NUMERIC(8, 4),
    gross_margin NUMERIC(8, 4),
    roa NUMERIC(8, 4),
    roe NUMERIC(8, 4),
    roi NUMERIC(8, 4),

    -- Growth
    eps_growth_this_y NUMERIC(10, 4),
    eps_growth_next_y NUMERIC(10, 4),
    eps_growth_next_5y NUMERIC(10, 4),
    sales_growth_past_5y NUMERIC(10, 4),

    -- Financial health
    current_ratio NUMERIC(8, 2),
    quick_ratio NUMERIC(8, 2),
    debt_equity NUMERIC(10, 2),
    lt_debt_equity NUMERIC(10, 2),

    -- Technical
    sma20 NUMERIC(10, 4),
    sma50 NUMERIC(10, 4),
    sma200 NUMERIC(10, 4),
    rsi NUMERIC(8, 2),
    volatility NUMERIC(8, 4),
    beta NUMERIC(8, 4),
    atr NUMERIC(10, 4),

    -- Volume
    volume BIGINT,
    avg_volume BIGINT,
    relative_volume NUMERIC(8, 2),

    -- Scores
    total_score NUMERIC(8, 2),
    momentum_score NUMERIC(8, 2),
    value_score NUMERIC(8, 2),
    quality_score NUMERIC(8, 2),
    growth_score NUMERIC(8, 2),

    -- Constraints
    PRIMARY KEY (ticker, as_of_date)
);

-- Create index for common queries
CREATE INDEX IF NOT EXISTS idx_fundamentals_date ON fundamentals(as_of_date);
CREATE INDEX IF NOT EXISTS idx_fundamentals_sector ON fundamentals(sector);

-- Prices table (time-series optimized)
CREATE TABLE IF NOT EXISTS prices (
    time TIMESTAMP WITH TIME ZONE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    open NUMERIC(12, 4),
    high NUMERIC(12, 4),
    low NUMERIC(12, 4),
    close NUMERIC(12, 4),
    adj_close NUMERIC(12, 4),
    volume BIGINT,
    PRIMARY KEY (ticker, time)
);

CREATE INDEX IF NOT EXISTS idx_prices_time ON prices(time DESC);

-- Screening results table
CREATE TABLE IF NOT EXISTS screening_results (
    id SERIAL PRIMARY KEY,
    screen_date DATE NOT NULL,
    theme VARCHAR(50) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    rank INTEGER,
    score NUMERIC(8, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_screening_date ON screening_results(screen_date);

-- Portfolio positions table
CREATE TABLE IF NOT EXISTS positions (
    id SERIAL PRIMARY KEY,
    portfolio_id VARCHAR(50) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    shares NUMERIC(18, 6) NOT NULL,
    cost_basis NUMERIC(12, 4),
    purchase_date DATE,
    sector VARCHAR(100),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(portfolio_id, ticker)
);

-- Trade history table
CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    portfolio_id VARCHAR(50) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    action VARCHAR(10) NOT NULL,
    shares NUMERIC(18, 6) NOT NULL,
    price NUMERIC(12, 4) NOT NULL,
    commission NUMERIC(10, 4) DEFAULT 0,
    executed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_trades_portfolio ON trades(portfolio_id);
CREATE INDEX IF NOT EXISTS idx_trades_ticker ON trades(ticker);

-- Alerts table
CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL,
    priority VARCHAR(20) NOT NULL,
    title VARCHAR(255) NOT NULL,
    message TEXT,
    data JSONB,
    sent_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    acknowledged BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_alerts_type ON alerts(alert_type);
"""

TIMESCALE_SQL = """
-- Convert prices to hypertable for time-series optimization
SELECT create_hypertable('prices', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '7 days');

-- Add compression policy
ALTER TABLE prices SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'ticker'
);

-- Continuous aggregates for common queries
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_price_stats
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS day,
    ticker,
    first(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, time) AS close,
    sum(volume) AS volume
FROM prices
GROUP BY time_bucket('1 day', time), ticker;
"""


# =============================================================================
# Database Connection Pool
# =============================================================================

class DatabasePool:
    """Connection pool manager."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._pool: Optional[Any] = None

    def initialize(self) -> None:
        """Initialize connection pool."""
        if not PSYCOPG2_AVAILABLE:
            raise ImportError("psycopg2 not installed. Install with: pip install psycopg2-binary")

        self._pool = pool.ThreadedConnectionPool(
            self.config.pool_min,
            self.config.pool_max,
            self.config.connection_string,
        )
        LOGGER.info("Database connection pool initialized")

    def close(self) -> None:
        """Close connection pool."""
        if self._pool:
            self._pool.closeall()
            self._pool = None

    @contextmanager
    def get_connection(self) -> Generator:
        """Get connection from pool."""
        if not self._pool:
            self.initialize()

        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    @contextmanager
    def get_cursor(self, cursor_factory=None) -> Generator:
        """Get cursor from pool."""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=cursor_factory or RealDictCursor)
            try:
                yield cursor
            finally:
                cursor.close()


# =============================================================================
# Database Operations
# =============================================================================

class Database:
    """Main database interface."""

    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig.from_env()
        self._pool = DatabasePool(self.config)

    def initialize(self) -> None:
        """Initialize database with schema."""
        self._pool.initialize()

        with self._pool.get_cursor() as cursor:
            cursor.execute(SCHEMA_SQL)

            if self.config.use_timescale:
                try:
                    cursor.execute(TIMESCALE_SQL)
                    LOGGER.info("TimescaleDB extensions enabled")
                except Exception as e:
                    LOGGER.warning(f"TimescaleDB not available: {e}")

    def close(self) -> None:
        """Close database connections."""
        self._pool.close()

    # -------------------------------------------------------------------------
    # Fundamentals Operations
    # -------------------------------------------------------------------------

    def upsert_fundamentals(self, df: pd.DataFrame) -> int:
        """
        Upsert fundamentals data.

        Args:
            df: DataFrame with fundamentals data

        Returns:
            Number of rows upserted
        """
        if df.empty:
            return 0

        # Prepare columns
        columns = [
            'ticker', 'as_of_date', 'company', 'sector', 'industry',
            'market_cap', 'price', 'pe', 'forward_pe', 'peg',
            'profit_margin', 'roe', 'roa', 'debt_equity',
            'eps_growth_this_y', 'eps_growth_next_y',
            'sma20', 'sma50', 'sma200', 'rsi', 'beta',
            'total_score', 'momentum_score', 'value_score', 'quality_score', 'growth_score',
        ]

        available_cols = [c for c in columns if c in df.columns]

        # Prepare data
        records = df[available_cols].to_dict('records')

        with self._pool.get_cursor() as cursor:
            # Build upsert query
            cols_str = ', '.join(available_cols)
            placeholders = ', '.join(['%s'] * len(available_cols))
            update_str = ', '.join([f"{c} = EXCLUDED.{c}" for c in available_cols if c not in ('ticker', 'as_of_date')])

            query = f"""
                INSERT INTO fundamentals ({cols_str})
                VALUES ({placeholders})
                ON CONFLICT (ticker, as_of_date)
                DO UPDATE SET {update_str}
            """

            for record in records:
                values = [record.get(c) for c in available_cols]
                cursor.execute(query, values)

        return len(records)

    def get_fundamentals(
        self,
        tickers: Optional[List[str]] = None,
        as_of_date: Optional[date] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> pd.DataFrame:
        """
        Get fundamentals data.

        Args:
            tickers: Filter by tickers
            as_of_date: Get data for specific date
            start_date: Start of date range
            end_date: End of date range

        Returns:
            DataFrame with fundamentals
        """
        conditions = []
        params = []

        if tickers:
            conditions.append("ticker = ANY(%s)")
            params.append(tickers)

        if as_of_date:
            conditions.append("as_of_date = %s")
            params.append(as_of_date)

        if start_date:
            conditions.append("as_of_date >= %s")
            params.append(start_date)

        if end_date:
            conditions.append("as_of_date <= %s")
            params.append(end_date)

        where_clause = " AND ".join(conditions) if conditions else "TRUE"

        query = f"SELECT * FROM fundamentals WHERE {where_clause} ORDER BY as_of_date DESC, ticker"

        with self._pool.get_cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

        return pd.DataFrame(rows)

    # -------------------------------------------------------------------------
    # Price Operations
    # -------------------------------------------------------------------------

    def insert_prices(self, df: pd.DataFrame, ticker: str) -> int:
        """
        Insert price data.

        Args:
            df: DataFrame with OHLCV data
            ticker: Ticker symbol

        Returns:
            Number of rows inserted
        """
        if df.empty:
            return 0

        records = []
        for idx, row in df.iterrows():
            records.append((
                idx if isinstance(idx, datetime) else datetime.combine(idx, datetime.min.time()),
                ticker,
                row.get('Open'),
                row.get('High'),
                row.get('Low'),
                row.get('Close'),
                row.get('Adj Close', row.get('Close')),
                row.get('Volume'),
            ))

        with self._pool.get_cursor() as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO prices (time, ticker, open, high, low, close, adj_close, volume)
                VALUES %s
                ON CONFLICT (ticker, time) DO NOTHING
                """,
                records,
            )

        return len(records)

    def get_prices(
        self,
        tickers: List[str],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Get price data.

        Args:
            tickers: List of tickers
            start_date: Start date
            end_date: End date

        Returns:
            DataFrame with prices (pivoted by ticker)
        """
        conditions = ["ticker = ANY(%s)"]
        params = [tickers]

        if start_date:
            conditions.append("time >= %s")
            params.append(start_date)

        if end_date:
            conditions.append("time <= %s")
            params.append(end_date)

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT time, ticker, adj_close
            FROM prices
            WHERE {where_clause}
            ORDER BY time
        """

        with self._pool.get_cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

        df = pd.DataFrame(rows)
        if df.empty:
            return df

        # Pivot to have tickers as columns
        df = df.pivot(index='time', columns='ticker', values='adj_close')
        return df

    # -------------------------------------------------------------------------
    # Screening Operations
    # -------------------------------------------------------------------------

    def save_screening_results(
        self,
        theme: str,
        results: pd.DataFrame,
        screen_date: Optional[date] = None,
    ) -> int:
        """Save screening results."""
        if results.empty:
            return 0

        screen_date = screen_date or date.today()

        records = []
        for rank, (_, row) in enumerate(results.iterrows(), 1):
            records.append((
                screen_date,
                theme,
                row.get('ticker', row.name),
                rank,
                row.get('total_score', row.get('score')),
            ))

        with self._pool.get_cursor() as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO screening_results (screen_date, theme, ticker, rank, score)
                VALUES %s
                """,
                records,
            )

        return len(records)

    def get_screening_history(
        self,
        theme: str,
        ticker: Optional[str] = None,
        days: int = 30,
    ) -> pd.DataFrame:
        """Get historical screening results."""
        conditions = ["theme = %s", "screen_date >= CURRENT_DATE - %s"]
        params = [theme, days]

        if ticker:
            conditions.append("ticker = %s")
            params.append(ticker)

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT screen_date, ticker, rank, score
            FROM screening_results
            WHERE {where_clause}
            ORDER BY screen_date DESC, rank
        """

        with self._pool.get_cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

        return pd.DataFrame(rows)

    # -------------------------------------------------------------------------
    # Portfolio Operations
    # -------------------------------------------------------------------------

    def save_position(
        self,
        portfolio_id: str,
        ticker: str,
        shares: float,
        cost_basis: Optional[float] = None,
        purchase_date: Optional[date] = None,
        sector: Optional[str] = None,
    ) -> None:
        """Save or update portfolio position."""
        with self._pool.get_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO positions (portfolio_id, ticker, shares, cost_basis, purchase_date, sector)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (portfolio_id, ticker)
                DO UPDATE SET
                    shares = EXCLUDED.shares,
                    cost_basis = COALESCE(EXCLUDED.cost_basis, positions.cost_basis),
                    updated_at = CURRENT_TIMESTAMP
                """,
                (portfolio_id, ticker, shares, cost_basis, purchase_date, sector),
            )

    def get_positions(self, portfolio_id: str) -> pd.DataFrame:
        """Get portfolio positions."""
        with self._pool.get_cursor() as cursor:
            cursor.execute(
                """
                SELECT ticker, shares, cost_basis, purchase_date, sector, updated_at
                FROM positions
                WHERE portfolio_id = %s
                ORDER BY ticker
                """,
                (portfolio_id,),
            )
            rows = cursor.fetchall()

        return pd.DataFrame(rows)

    def delete_position(self, portfolio_id: str, ticker: str) -> None:
        """Delete a position."""
        with self._pool.get_cursor() as cursor:
            cursor.execute(
                "DELETE FROM positions WHERE portfolio_id = %s AND ticker = %s",
                (portfolio_id, ticker),
            )

    # -------------------------------------------------------------------------
    # Trade Operations
    # -------------------------------------------------------------------------

    def record_trade(
        self,
        portfolio_id: str,
        ticker: str,
        action: str,
        shares: float,
        price: float,
        commission: float = 0,
        notes: Optional[str] = None,
    ) -> int:
        """Record a trade."""
        with self._pool.get_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO trades (portfolio_id, ticker, action, shares, price, commission, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (portfolio_id, ticker, action, shares, price, commission, notes),
            )
            result = cursor.fetchone()
            return result['id'] if result else 0

    def get_trade_history(
        self,
        portfolio_id: str,
        ticker: Optional[str] = None,
        days: int = 365,
    ) -> pd.DataFrame:
        """Get trade history."""
        conditions = ["portfolio_id = %s", "executed_at >= CURRENT_TIMESTAMP - INTERVAL '%s days'"]
        params = [portfolio_id, days]

        if ticker:
            conditions.append("ticker = %s")
            params.append(ticker)

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT id, ticker, action, shares, price, commission, executed_at, notes
            FROM trades
            WHERE {where_clause}
            ORDER BY executed_at DESC
        """

        with self._pool.get_cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

        return pd.DataFrame(rows)


# =============================================================================
# Convenience Functions
# =============================================================================

_default_db: Optional[Database] = None


def get_database() -> Database:
    """Get or create default database instance."""
    global _default_db
    if _default_db is None:
        _default_db = Database()
        _default_db.initialize()
    return _default_db


def store_fundamentals(df: pd.DataFrame) -> int:
    """Store fundamentals data to database."""
    return get_database().upsert_fundamentals(df)


def load_fundamentals(
    tickers: Optional[List[str]] = None,
    as_of_date: Optional[date] = None,
) -> pd.DataFrame:
    """Load fundamentals from database."""
    return get_database().get_fundamentals(tickers, as_of_date)


def store_prices(df: pd.DataFrame, ticker: str) -> int:
    """Store price data to database."""
    return get_database().insert_prices(df, ticker)


def load_prices(
    tickers: List[str],
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """Load price data from database."""
    return get_database().get_prices(tickers, start_date, end_date)
