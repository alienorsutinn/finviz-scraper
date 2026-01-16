"""
Telegram and Discord Bot Alerts

Provides notification capabilities for:
- Trading signals
- Rebalancing triggers
- Risk alerts
- Screening results
"""

from __future__ import annotations

import asyncio
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

LOGGER = logging.getLogger(__name__)


class AlertPriority(Enum):
    """Alert priority levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertType(Enum):
    """Types of alerts."""
    SIGNAL = "signal"
    REBALANCE = "rebalance"
    RISK = "risk"
    SCREENING = "screening"
    ERROR = "error"
    INFO = "info"


@dataclass
class Alert:
    """Alert message."""
    type: AlertType
    priority: AlertPriority
    title: str
    message: str
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

    def to_text(self) -> str:
        """Convert to plain text format."""
        priority_emoji = {
            AlertPriority.LOW: "",
            AlertPriority.MEDIUM: "",
            AlertPriority.HIGH: "[!]",
            AlertPriority.CRITICAL: "[!!!]",
        }
        emoji = priority_emoji.get(self.priority, "")
        return f"{emoji} {self.title}\n\n{self.message}"

    def to_markdown(self) -> str:
        """Convert to markdown format."""
        priority_prefix = {
            AlertPriority.LOW: "",
            AlertPriority.MEDIUM: "",
            AlertPriority.HIGH: "**[HIGH]** ",
            AlertPriority.CRITICAL: "**[CRITICAL]** ",
        }
        prefix = priority_prefix.get(self.priority, "")
        return f"{prefix}**{self.title}**\n\n{self.message}"


@dataclass
class BotConfig:
    """Configuration for alert bots."""
    # Telegram settings
    telegram_token: Optional[str] = None
    telegram_chat_ids: List[str] = field(default_factory=list)

    # Discord settings
    discord_webhook_url: Optional[str] = None
    discord_bot_token: Optional[str] = None
    discord_channel_ids: List[str] = field(default_factory=list)

    # Alert settings
    min_priority: AlertPriority = AlertPriority.MEDIUM
    alert_types: List[AlertType] = field(
        default_factory=lambda: list(AlertType)
    )

    # Rate limiting
    max_alerts_per_hour: int = 30
    cooldown_seconds: int = 60

    @classmethod
    def from_env(cls) -> 'BotConfig':
        """Create config from environment variables."""
        return cls(
            telegram_token=os.getenv('TELEGRAM_BOT_TOKEN'),
            telegram_chat_ids=os.getenv('TELEGRAM_CHAT_IDS', '').split(','),
            discord_webhook_url=os.getenv('DISCORD_WEBHOOK_URL'),
            discord_bot_token=os.getenv('DISCORD_BOT_TOKEN'),
            discord_channel_ids=os.getenv('DISCORD_CHANNEL_IDS', '').split(','),
        )


class AlertBot(ABC):
    """Abstract base class for alert bots."""

    @abstractmethod
    async def send_alert(self, alert: Alert) -> bool:
        """Send an alert. Returns True if successful."""
        pass

    @abstractmethod
    def is_configured(self) -> bool:
        """Check if bot is properly configured."""
        pass


class TelegramBot(AlertBot):
    """Telegram bot for alerts."""

    def __init__(self, config: BotConfig):
        self.config = config
        self.token = config.telegram_token
        self.chat_ids = [c for c in config.telegram_chat_ids if c]
        self._client = None

    def is_configured(self) -> bool:
        return bool(self.token and self.chat_ids)

    async def send_alert(self, alert: Alert) -> bool:
        """Send alert to Telegram."""
        if not self.is_configured():
            LOGGER.warning("Telegram bot not configured")
            return False

        try:
            import httpx
        except ImportError:
            LOGGER.error("httpx not installed for Telegram bot")
            return False

        message = alert.to_text()
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        success = True
        async with httpx.AsyncClient() as client:
            for chat_id in self.chat_ids:
                try:
                    response = await client.post(
                        url,
                        json={
                            "chat_id": chat_id,
                            "text": message,
                            "parse_mode": "Markdown",
                        },
                        timeout=10.0,
                    )
                    if response.status_code != 200:
                        LOGGER.error(f"Telegram error: {response.text}")
                        success = False
                except Exception as e:
                    LOGGER.error(f"Telegram send failed: {e}")
                    success = False

        return success

    async def send_image(self, chat_id: str, image_path: str, caption: str = "") -> bool:
        """Send image to Telegram."""
        if not self.is_configured():
            return False

        try:
            import httpx
        except ImportError:
            return False

        url = f"https://api.telegram.org/bot{self.token}/sendPhoto"

        try:
            async with httpx.AsyncClient() as client:
                with open(image_path, 'rb') as f:
                    response = await client.post(
                        url,
                        data={"chat_id": chat_id, "caption": caption},
                        files={"photo": f},
                        timeout=30.0,
                    )
                    return response.status_code == 200
        except Exception as e:
            LOGGER.error(f"Telegram image send failed: {e}")
            return False


class DiscordBot(AlertBot):
    """Discord bot for alerts using webhooks."""

    def __init__(self, config: BotConfig):
        self.config = config
        self.webhook_url = config.discord_webhook_url

    def is_configured(self) -> bool:
        return bool(self.webhook_url)

    async def send_alert(self, alert: Alert) -> bool:
        """Send alert to Discord via webhook."""
        if not self.is_configured():
            LOGGER.warning("Discord bot not configured")
            return False

        try:
            import httpx
        except ImportError:
            LOGGER.error("httpx not installed for Discord bot")
            return False

        # Build Discord embed
        color_map = {
            AlertPriority.LOW: 0x808080,  # Gray
            AlertPriority.MEDIUM: 0x3498db,  # Blue
            AlertPriority.HIGH: 0xf39c12,  # Orange
            AlertPriority.CRITICAL: 0xe74c3c,  # Red
        }

        embed = {
            "title": alert.title,
            "description": alert.message,
            "color": color_map.get(alert.priority, 0x3498db),
            "timestamp": alert.timestamp.isoformat(),
            "footer": {"text": f"Type: {alert.type.value}"},
        }

        # Add fields from data
        if alert.data:
            fields = []
            for key, value in list(alert.data.items())[:10]:
                fields.append({
                    "name": key,
                    "value": str(value)[:100],
                    "inline": True,
                })
            embed["fields"] = fields

        payload = {"embeds": [embed]}

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.webhook_url,
                    json=payload,
                    timeout=10.0,
                )
                if response.status_code not in [200, 204]:
                    LOGGER.error(f"Discord error: {response.text}")
                    return False
                return True
        except Exception as e:
            LOGGER.error(f"Discord send failed: {e}")
            return False


class AlertManager:
    """Manages alert delivery across multiple channels."""

    def __init__(self, config: Optional[BotConfig] = None):
        self.config = config or BotConfig.from_env()
        self.bots: List[AlertBot] = []
        self._alert_history: List[Alert] = []
        self._last_alert_time: Dict[str, datetime] = {}

        # Initialize bots
        telegram = TelegramBot(self.config)
        if telegram.is_configured():
            self.bots.append(telegram)

        discord = DiscordBot(self.config)
        if discord.is_configured():
            self.bots.append(discord)

    def _should_send(self, alert: Alert) -> bool:
        """Check if alert should be sent based on filters."""
        # Priority filter
        priority_order = [
            AlertPriority.LOW,
            AlertPriority.MEDIUM,
            AlertPriority.HIGH,
            AlertPriority.CRITICAL,
        ]
        if priority_order.index(alert.priority) < priority_order.index(self.config.min_priority):
            return False

        # Type filter
        if alert.type not in self.config.alert_types:
            return False

        # Rate limiting
        alert_key = f"{alert.type.value}:{alert.title}"
        last_time = self._last_alert_time.get(alert_key)
        if last_time:
            seconds_since = (datetime.now() - last_time).total_seconds()
            if seconds_since < self.config.cooldown_seconds:
                LOGGER.debug(f"Alert rate limited: {alert_key}")
                return False

        return True

    async def send_alert(self, alert: Alert) -> bool:
        """Send alert to all configured channels."""
        if not self._should_send(alert):
            return False

        if not self.bots:
            LOGGER.warning("No alert bots configured")
            return False

        # Update rate limit tracking
        alert_key = f"{alert.type.value}:{alert.title}"
        self._last_alert_time[alert_key] = datetime.now()

        # Send to all bots
        results = await asyncio.gather(
            *[bot.send_alert(alert) for bot in self.bots],
            return_exceptions=True,
        )

        success = any(r is True for r in results)
        if success:
            self._alert_history.append(alert)

        return success

    def send_alert_sync(self, alert: Alert) -> bool:
        """Synchronous wrapper for send_alert."""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        return loop.run_until_complete(self.send_alert(alert))


# =============================================================================
# Alert Factory Functions
# =============================================================================

def create_signal_alert(
    ticker: str,
    signal: str,
    score: float,
    reason: str = "",
) -> Alert:
    """Create a trading signal alert."""
    priority = AlertPriority.HIGH if signal in ['strong_buy', 'strong_sell'] else AlertPriority.MEDIUM

    return Alert(
        type=AlertType.SIGNAL,
        priority=priority,
        title=f"Trading Signal: {ticker} - {signal.upper()}",
        message=f"Signal: {signal}\nScore: {score:.1f}\n{reason}",
        data={
            "ticker": ticker,
            "signal": signal,
            "score": score,
        },
    )


def create_rebalance_alert(
    trades: List[Dict[str, Any]],
    turnover: float,
) -> Alert:
    """Create a rebalancing alert."""
    n_trades = len(trades)
    buys = sum(1 for t in trades if t.get('action') == 'buy')
    sells = n_trades - buys

    trade_list = "\n".join([
        f"  {t['action'].upper()} {t['ticker']}: ${t['value']:,.0f}"
        for t in trades[:10]
    ])

    return Alert(
        type=AlertType.REBALANCE,
        priority=AlertPriority.MEDIUM,
        title=f"Rebalancing Required - {n_trades} Trades",
        message=f"Turnover: {turnover:.1%}\nBuys: {buys}, Sells: {sells}\n\nTrades:\n{trade_list}",
        data={
            "n_trades": n_trades,
            "turnover": turnover,
            "buys": buys,
            "sells": sells,
        },
    )


def create_risk_alert(
    metric: str,
    current_value: float,
    threshold: float,
    ticker: Optional[str] = None,
) -> Alert:
    """Create a risk alert."""
    title = f"Risk Alert: {metric}"
    if ticker:
        title = f"Risk Alert: {ticker} - {metric}"

    return Alert(
        type=AlertType.RISK,
        priority=AlertPriority.HIGH,
        title=title,
        message=f"Current: {current_value:.2%}\nThreshold: {threshold:.2%}\nAction may be required.",
        data={
            "metric": metric,
            "current": current_value,
            "threshold": threshold,
            "ticker": ticker,
        },
    )


def create_screening_alert(
    theme: str,
    top_picks: List[Dict[str, Any]],
) -> Alert:
    """Create a screening results alert."""
    picks_list = "\n".join([
        f"  {p['ticker']}: Score {p.get('score', 'N/A')}"
        for p in top_picks[:10]
    ])

    return Alert(
        type=AlertType.SCREENING,
        priority=AlertPriority.LOW,
        title=f"Screening Results: {theme}",
        message=f"Top {len(top_picks)} picks:\n{picks_list}",
        data={
            "theme": theme,
            "count": len(top_picks),
            "tickers": [p['ticker'] for p in top_picks],
        },
    )


def create_error_alert(
    error_type: str,
    error_message: str,
    details: Optional[Dict[str, Any]] = None,
) -> Alert:
    """Create an error alert."""
    return Alert(
        type=AlertType.ERROR,
        priority=AlertPriority.CRITICAL,
        title=f"Error: {error_type}",
        message=error_message,
        data=details or {},
    )


# =============================================================================
# Convenience Functions
# =============================================================================

_default_manager: Optional[AlertManager] = None


def get_alert_manager() -> AlertManager:
    """Get or create default alert manager."""
    global _default_manager
    if _default_manager is None:
        _default_manager = AlertManager()
    return _default_manager


def send_alert(
    title: str,
    message: str,
    priority: AlertPriority = AlertPriority.MEDIUM,
    alert_type: AlertType = AlertType.INFO,
) -> bool:
    """Send a simple alert."""
    alert = Alert(
        type=alert_type,
        priority=priority,
        title=title,
        message=message,
    )
    return get_alert_manager().send_alert_sync(alert)


def send_signal_alert(
    ticker: str,
    signal: str,
    score: float,
    reason: str = "",
) -> bool:
    """Send a trading signal alert."""
    alert = create_signal_alert(ticker, signal, score, reason)
    return get_alert_manager().send_alert_sync(alert)


def send_risk_alert(
    metric: str,
    current: float,
    threshold: float,
) -> bool:
    """Send a risk alert."""
    alert = create_risk_alert(metric, current, threshold)
    return get_alert_manager().send_alert_sync(alert)
