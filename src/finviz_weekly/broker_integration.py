"""
Broker Integration Module

Connect to brokers for live/paper trading:
- Alpaca API integration
- Interactive Brokers (IBKR) integration
- Paper trading simulation
- Trade journaling and analytics
"""

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import json
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class OrderStatus(Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class TradingMode(Enum):
    LIVE = "live"
    PAPER = "paper"


@dataclass
class Order:
    """Trading order."""
    id: str
    ticker: str
    side: OrderSide
    order_type: OrderType
    quantity: int
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: int = 0
    filled_avg_price: float = 0.0
    submitted_at: Optional[str] = None
    filled_at: Optional[str] = None
    notes: str = ""

    def to_dict(self) -> Dict:
        return {
            'id': self.id,
            'ticker': self.ticker,
            'side': self.side.value,
            'order_type': self.order_type.value,
            'quantity': self.quantity,
            'limit_price': self.limit_price,
            'stop_price': self.stop_price,
            'status': self.status.value,
            'filled_quantity': self.filled_quantity,
            'filled_avg_price': self.filled_avg_price,
            'submitted_at': self.submitted_at,
            'filled_at': self.filled_at,
            'notes': self.notes,
        }


@dataclass
class BrokerPosition:
    """Position from broker."""
    ticker: str
    quantity: int
    avg_entry_price: float
    current_price: float
    market_value: float
    unrealized_pnl: float
    unrealized_pnl_pct: float


@dataclass
class AccountInfo:
    """Broker account information."""
    account_id: str
    cash: float
    portfolio_value: float
    buying_power: float
    equity: float
    positions_count: int
    is_paper: bool


@dataclass
class TradeRecord:
    """Record of executed trade for journaling."""
    id: str
    ticker: str
    side: str
    quantity: int
    price: float
    total_value: float
    commission: float
    executed_at: str
    strategy: str = ""
    signal_score: float = 0.0
    notes: str = ""
    tags: List[str] = field(default_factory=list)

    # Performance tracking (filled after exit)
    exit_price: Optional[float] = None
    exit_date: Optional[str] = None
    pnl: Optional[float] = None
    pnl_pct: Optional[float] = None
    holding_days: Optional[int] = None


class BaseBroker(ABC):
    """Abstract base class for broker integrations."""

    @abstractmethod
    def connect(self) -> bool:
        """Connect to broker."""
        pass

    @abstractmethod
    def get_account_info(self) -> AccountInfo:
        """Get account information."""
        pass

    @abstractmethod
    def get_positions(self) -> List[BrokerPosition]:
        """Get current positions."""
        pass

    @abstractmethod
    def submit_order(self, order: Order) -> Order:
        """Submit an order."""
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        pass

    @abstractmethod
    def get_order_status(self, order_id: str) -> Order:
        """Get order status."""
        pass

    @abstractmethod
    def get_quote(self, ticker: str) -> Dict[str, float]:
        """Get current quote for ticker."""
        pass


class AlpacaBroker(BaseBroker):
    """Alpaca broker integration."""

    def __init__(self, api_key: Optional[str] = None,
                 secret_key: Optional[str] = None,
                 paper: bool = True):
        """
        Initialize Alpaca broker.

        Args:
            api_key: Alpaca API key (or ALPACA_API_KEY env var)
            secret_key: Alpaca secret key (or ALPACA_SECRET_KEY env var)
            paper: Use paper trading (default True)
        """
        self.api_key = api_key or os.environ.get('ALPACA_API_KEY')
        self.secret_key = secret_key or os.environ.get('ALPACA_SECRET_KEY')
        self.paper = paper
        self._api = None

    def connect(self) -> bool:
        """Connect to Alpaca API."""
        if not self.api_key or not self.secret_key:
            logger.error("Alpaca API credentials not provided")
            return False

        try:
            from alpaca.trading.client import TradingClient
            self._api = TradingClient(
                api_key=self.api_key,
                secret_key=self.secret_key,
                paper=self.paper
            )
            # Test connection
            self._api.get_account()
            logger.info(f"Connected to Alpaca ({'paper' if self.paper else 'live'})")
            return True
        except ImportError:
            logger.error("alpaca-py not installed. Install with: pip install alpaca-py")
            return False
        except Exception as e:
            logger.error(f"Failed to connect to Alpaca: {e}")
            return False

    def get_account_info(self) -> AccountInfo:
        """Get Alpaca account information."""
        if not self._api:
            raise RuntimeError("Not connected to Alpaca")

        account = self._api.get_account()
        return AccountInfo(
            account_id=account.account_number,
            cash=float(account.cash),
            portfolio_value=float(account.portfolio_value),
            buying_power=float(account.buying_power),
            equity=float(account.equity),
            positions_count=0,  # Would need separate call
            is_paper=self.paper
        )

    def get_positions(self) -> List[BrokerPosition]:
        """Get current Alpaca positions."""
        if not self._api:
            raise RuntimeError("Not connected to Alpaca")

        positions = self._api.get_all_positions()
        return [
            BrokerPosition(
                ticker=pos.symbol,
                quantity=int(pos.qty),
                avg_entry_price=float(pos.avg_entry_price),
                current_price=float(pos.current_price),
                market_value=float(pos.market_value),
                unrealized_pnl=float(pos.unrealized_pl),
                unrealized_pnl_pct=float(pos.unrealized_plpc) * 100
            )
            for pos in positions
        ]

    def submit_order(self, order: Order) -> Order:
        """Submit order to Alpaca."""
        if not self._api:
            raise RuntimeError("Not connected to Alpaca")

        try:
            from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
            from alpaca.trading.enums import OrderSide as AlpacaSide, TimeInForce

            side = AlpacaSide.BUY if order.side == OrderSide.BUY else AlpacaSide.SELL

            if order.order_type == OrderType.MARKET:
                request = MarketOrderRequest(
                    symbol=order.ticker,
                    qty=order.quantity,
                    side=side,
                    time_in_force=TimeInForce.DAY
                )
            elif order.order_type == OrderType.LIMIT:
                request = LimitOrderRequest(
                    symbol=order.ticker,
                    qty=order.quantity,
                    side=side,
                    time_in_force=TimeInForce.DAY,
                    limit_price=order.limit_price
                )
            else:
                raise ValueError(f"Unsupported order type: {order.order_type}")

            result = self._api.submit_order(request)

            order.id = result.id
            order.status = OrderStatus.SUBMITTED
            order.submitted_at = datetime.now().isoformat()

            return order

        except Exception as e:
            logger.error(f"Failed to submit order: {e}")
            order.status = OrderStatus.REJECTED
            order.notes = str(e)
            return order

    def cancel_order(self, order_id: str) -> bool:
        """Cancel Alpaca order."""
        if not self._api:
            raise RuntimeError("Not connected to Alpaca")

        try:
            self._api.cancel_order_by_id(order_id)
            return True
        except Exception as e:
            logger.error(f"Failed to cancel order: {e}")
            return False

    def get_order_status(self, order_id: str) -> Order:
        """Get Alpaca order status."""
        if not self._api:
            raise RuntimeError("Not connected to Alpaca")

        result = self._api.get_order_by_id(order_id)

        status_map = {
            'new': OrderStatus.SUBMITTED,
            'filled': OrderStatus.FILLED,
            'partially_filled': OrderStatus.PARTIALLY_FILLED,
            'canceled': OrderStatus.CANCELLED,
            'rejected': OrderStatus.REJECTED,
        }

        return Order(
            id=result.id,
            ticker=result.symbol,
            side=OrderSide.BUY if result.side == 'buy' else OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=int(result.qty),
            status=status_map.get(result.status, OrderStatus.PENDING),
            filled_quantity=int(result.filled_qty or 0),
            filled_avg_price=float(result.filled_avg_price or 0),
        )

    def get_quote(self, ticker: str) -> Dict[str, float]:
        """Get current quote."""
        try:
            from alpaca.data.historical import StockHistoricalDataClient
            from alpaca.data.requests import StockLatestQuoteRequest

            data_client = StockHistoricalDataClient(self.api_key, self.secret_key)
            request = StockLatestQuoteRequest(symbol_or_symbols=ticker)
            quote = data_client.get_stock_latest_quote(request)[ticker]

            return {
                'bid': float(quote.bid_price),
                'ask': float(quote.ask_price),
                'mid': (float(quote.bid_price) + float(quote.ask_price)) / 2,
            }
        except Exception as e:
            logger.error(f"Failed to get quote for {ticker}: {e}")
            return {'bid': 0, 'ask': 0, 'mid': 0}


class PaperTradingBroker(BaseBroker):
    """Paper trading simulator."""

    def __init__(self, initial_cash: float = 100000.0,
                 data_source: str = "yfinance"):
        """
        Initialize paper trading broker.

        Args:
            initial_cash: Starting cash amount
            data_source: Source for price data ('yfinance')
        """
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.data_source = data_source
        self.positions: Dict[str, BrokerPosition] = {}
        self.orders: Dict[str, Order] = {}
        self.order_counter = 0
        self.connected = False

    def connect(self) -> bool:
        """Initialize paper trading."""
        self.connected = True
        logger.info(f"Paper trading initialized with ${self.initial_cash:,.2f}")
        return True

    def get_account_info(self) -> AccountInfo:
        """Get paper account information."""
        portfolio_value = self.cash + sum(p.market_value for p in self.positions.values())

        return AccountInfo(
            account_id="PAPER_ACCOUNT",
            cash=self.cash,
            portfolio_value=portfolio_value,
            buying_power=self.cash,
            equity=portfolio_value,
            positions_count=len(self.positions),
            is_paper=True
        )

    def get_positions(self) -> List[BrokerPosition]:
        """Get paper positions."""
        # Update current prices
        for ticker in self.positions:
            quote = self.get_quote(ticker)
            if quote['mid'] > 0:
                pos = self.positions[ticker]
                pos.current_price = quote['mid']
                pos.market_value = pos.quantity * pos.current_price
                pos.unrealized_pnl = pos.market_value - (pos.quantity * pos.avg_entry_price)
                pos.unrealized_pnl_pct = (pos.unrealized_pnl / (pos.quantity * pos.avg_entry_price)) * 100

        return list(self.positions.values())

    def submit_order(self, order: Order) -> Order:
        """Submit paper order (instant fill at market)."""
        self.order_counter += 1
        order.id = f"PAPER_{self.order_counter}"
        order.submitted_at = datetime.now().isoformat()

        # Get current price
        quote = self.get_quote(order.ticker)
        if quote['mid'] <= 0:
            order.status = OrderStatus.REJECTED
            order.notes = "Could not get quote"
            return order

        # Determine fill price
        if order.order_type == OrderType.MARKET:
            if order.side == OrderSide.BUY:
                fill_price = quote['ask'] if quote['ask'] > 0 else quote['mid']
            else:
                fill_price = quote['bid'] if quote['bid'] > 0 else quote['mid']
        elif order.order_type == OrderType.LIMIT:
            fill_price = order.limit_price
        else:
            fill_price = quote['mid']

        total_value = order.quantity * fill_price

        # Check buying power for buys
        if order.side == OrderSide.BUY:
            if total_value > self.cash:
                order.status = OrderStatus.REJECTED
                order.notes = "Insufficient buying power"
                return order

            # Execute buy
            self.cash -= total_value

            if order.ticker in self.positions:
                # Add to existing position
                pos = self.positions[order.ticker]
                total_shares = pos.quantity + order.quantity
                pos.avg_entry_price = (
                    (pos.quantity * pos.avg_entry_price + order.quantity * fill_price)
                    / total_shares
                )
                pos.quantity = total_shares
            else:
                # New position
                self.positions[order.ticker] = BrokerPosition(
                    ticker=order.ticker,
                    quantity=order.quantity,
                    avg_entry_price=fill_price,
                    current_price=fill_price,
                    market_value=total_value,
                    unrealized_pnl=0,
                    unrealized_pnl_pct=0
                )

        else:  # SELL
            if order.ticker not in self.positions:
                order.status = OrderStatus.REJECTED
                order.notes = "No position to sell"
                return order

            pos = self.positions[order.ticker]
            if order.quantity > pos.quantity:
                order.status = OrderStatus.REJECTED
                order.notes = "Insufficient shares"
                return order

            # Execute sell
            self.cash += total_value
            pos.quantity -= order.quantity

            if pos.quantity == 0:
                del self.positions[order.ticker]

        # Mark as filled
        order.status = OrderStatus.FILLED
        order.filled_quantity = order.quantity
        order.filled_avg_price = fill_price
        order.filled_at = datetime.now().isoformat()

        self.orders[order.id] = order
        return order

    def cancel_order(self, order_id: str) -> bool:
        """Cancel paper order (only if pending)."""
        if order_id in self.orders:
            order = self.orders[order_id]
            if order.status == OrderStatus.PENDING:
                order.status = OrderStatus.CANCELLED
                return True
        return False

    def get_order_status(self, order_id: str) -> Order:
        """Get paper order status."""
        if order_id in self.orders:
            return self.orders[order_id]
        raise ValueError(f"Order not found: {order_id}")

    def get_quote(self, ticker: str) -> Dict[str, float]:
        """Get current quote using yfinance."""
        try:
            import yfinance as yf
            stock = yf.Ticker(ticker)
            info = stock.info

            bid = info.get('bid', 0) or 0
            ask = info.get('ask', 0) or 0
            price = info.get('regularMarketPrice', 0) or info.get('previousClose', 0) or 0

            if bid == 0 and ask == 0 and price > 0:
                # Simulate spread
                bid = price * 0.999
                ask = price * 1.001

            return {
                'bid': float(bid),
                'ask': float(ask),
                'mid': float(price) if price > 0 else (bid + ask) / 2,
            }
        except Exception as e:
            logger.error(f"Failed to get quote for {ticker}: {e}")
            return {'bid': 0, 'ask': 0, 'mid': 0}


class TradeJournal:
    """Trade journaling and analytics."""

    def __init__(self, journal_path: str = "data/trade_journal.json"):
        self.journal_path = Path(journal_path)
        self.trades: List[TradeRecord] = []
        self._load()

    def _load(self):
        """Load journal from disk."""
        if self.journal_path.exists():
            try:
                with open(self.journal_path) as f:
                    data = json.load(f)
                    self.trades = [TradeRecord(**t) for t in data]
            except Exception as e:
                logger.error(f"Failed to load journal: {e}")

    def _save(self):
        """Save journal to disk."""
        self.journal_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.journal_path, 'w') as f:
            json.dump([self._trade_to_dict(t) for t in self.trades], f, indent=2)

    def _trade_to_dict(self, trade: TradeRecord) -> Dict:
        """Convert trade to dict."""
        return {
            'id': trade.id,
            'ticker': trade.ticker,
            'side': trade.side,
            'quantity': trade.quantity,
            'price': trade.price,
            'total_value': trade.total_value,
            'commission': trade.commission,
            'executed_at': trade.executed_at,
            'strategy': trade.strategy,
            'signal_score': trade.signal_score,
            'notes': trade.notes,
            'tags': trade.tags,
            'exit_price': trade.exit_price,
            'exit_date': trade.exit_date,
            'pnl': trade.pnl,
            'pnl_pct': trade.pnl_pct,
            'holding_days': trade.holding_days,
        }

    def record_trade(self, order: Order, strategy: str = "",
                    signal_score: float = 0.0, notes: str = "",
                    tags: List[str] = None) -> TradeRecord:
        """Record an executed trade."""
        trade = TradeRecord(
            id=order.id,
            ticker=order.ticker,
            side=order.side.value,
            quantity=order.filled_quantity,
            price=order.filled_avg_price,
            total_value=order.filled_quantity * order.filled_avg_price,
            commission=0,  # Would need from execution result
            executed_at=order.filled_at or datetime.now().isoformat(),
            strategy=strategy,
            signal_score=signal_score,
            notes=notes,
            tags=tags or []
        )

        self.trades.append(trade)
        self._save()
        return trade

    def record_exit(self, entry_id: str, exit_price: float,
                   exit_date: Optional[str] = None):
        """Record exit for a trade."""
        for trade in self.trades:
            if trade.id == entry_id and trade.side == 'buy':
                trade.exit_price = exit_price
                trade.exit_date = exit_date or datetime.now().isoformat()
                trade.pnl = (exit_price - trade.price) * trade.quantity
                trade.pnl_pct = ((exit_price / trade.price) - 1) * 100

                # Calculate holding days
                entry = datetime.fromisoformat(trade.executed_at.replace('Z', '+00:00'))
                exit = datetime.fromisoformat(trade.exit_date.replace('Z', '+00:00'))
                trade.holding_days = (exit - entry).days

                self._save()
                return

    def get_performance_summary(self) -> Dict:
        """Get trading performance summary."""
        closed_trades = [t for t in self.trades if t.pnl is not None]

        if not closed_trades:
            return {
                'total_trades': len(self.trades),
                'closed_trades': 0,
                'open_trades': len(self.trades),
                'win_rate': 0,
                'total_pnl': 0,
                'avg_pnl': 0,
                'avg_holding_days': 0,
            }

        winners = [t for t in closed_trades if t.pnl > 0]
        losers = [t for t in closed_trades if t.pnl < 0]

        return {
            'total_trades': len(self.trades),
            'closed_trades': len(closed_trades),
            'open_trades': len(self.trades) - len(closed_trades),
            'win_rate': len(winners) / len(closed_trades) * 100,
            'total_pnl': sum(t.pnl for t in closed_trades),
            'avg_pnl': sum(t.pnl for t in closed_trades) / len(closed_trades),
            'avg_pnl_pct': sum(t.pnl_pct for t in closed_trades) / len(closed_trades),
            'avg_holding_days': sum(t.holding_days or 0 for t in closed_trades) / len(closed_trades),
            'best_trade': max(t.pnl for t in closed_trades),
            'worst_trade': min(t.pnl for t in closed_trades),
            'avg_winner': sum(t.pnl for t in winners) / len(winners) if winners else 0,
            'avg_loser': sum(t.pnl for t in losers) / len(losers) if losers else 0,
            'profit_factor': (
                abs(sum(t.pnl for t in winners) / sum(t.pnl for t in losers))
                if losers and sum(t.pnl for t in losers) != 0 else float('inf')
            ),
        }

    def get_trades_by_strategy(self, strategy: str) -> List[TradeRecord]:
        """Get trades for a specific strategy."""
        return [t for t in self.trades if t.strategy == strategy]

    def get_trades_by_tag(self, tag: str) -> List[TradeRecord]:
        """Get trades with a specific tag."""
        return [t for t in self.trades if tag in t.tags]

    def to_dataframe(self) -> pd.DataFrame:
        """Convert journal to DataFrame."""
        return pd.DataFrame([self._trade_to_dict(t) for t in self.trades])


def create_broker(broker_type: str = "paper", **kwargs) -> BaseBroker:
    """
    Factory function to create broker instance.

    Args:
        broker_type: 'paper', 'alpaca'
        **kwargs: Broker-specific arguments
    """
    if broker_type == "paper":
        return PaperTradingBroker(**kwargs)
    elif broker_type == "alpaca":
        return AlpacaBroker(**kwargs)
    else:
        raise ValueError(f"Unknown broker type: {broker_type}")
