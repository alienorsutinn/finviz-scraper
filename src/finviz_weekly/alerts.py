"""
Real-time alert system for stock screening opportunities.

Supports Email and Slack/Discord notifications based on configurable triggers.
"""
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import json

import pandas as pd

LOGGER = logging.getLogger(__name__)


@dataclass
class AlertConfig:
    """Configuration for alert triggers."""

    enabled: bool = True

    # Score-based triggers
    master_score_threshold: float = 80.0
    quality_value_score_threshold: float = 75.0

    # Enhanced data triggers
    insider_buying_min: float = 1_000_000.0  # $1M+ net buying
    earnings_alpha_min: float = 0.05  # 5%+ avg alpha
    earnings_win_rate_min: float = 0.75  # 75%+ win rate

    # Financial health triggers
    net_margin_min: float = 0.20  # 20%+ net margin
    roe_min: float = 0.20  # 20%+ ROE
    current_ratio_min: float = 2.0

    # Conviction triggers
    min_theme_families: int = 3  # Appears in 3+ theme families

    # Deduplication (don't re-alert on same ticker within N days)
    dedup_days: int = 7

    # Notification settings
    email_enabled: bool = False
    email_to: Optional[str] = None
    email_from: Optional[str] = None
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None

    slack_enabled: bool = False
    slack_webhook_url: Optional[str] = None

    discord_enabled: bool = False
    discord_webhook_url: Optional[str] = None


@dataclass
class Alert:
    """Represents a single alert."""

    ticker: str
    company: str
    trigger_type: str
    reason: str
    score: float
    sector: str
    timestamp: datetime
    data: Dict  # Additional data for the alert


class AlertEngine:
    """Alert engine for detecting and sending notifications."""

    def __init__(self, config: AlertConfig, alert_history_path: Optional[Path] = None):
        """
        Initialize alert engine.

        Args:
            config: Alert configuration
            alert_history_path: Path to alert history file (for deduplication)
        """
        self.config = config
        self.alert_history_path = alert_history_path or Path("data/alerts_history.json")
        self.alert_history = self._load_alert_history()

    def _load_alert_history(self) -> Dict:
        """Load alert history from disk."""
        if self.alert_history_path.exists():
            try:
                return json.loads(self.alert_history_path.read_text())
            except Exception as e:
                LOGGER.warning(f"Failed to load alert history: {e}")
        return {}

    def _save_alert_history(self):
        """Save alert history to disk."""
        try:
            self.alert_history_path.parent.mkdir(parents=True, exist_ok=True)
            self.alert_history_path.write_text(json.dumps(self.alert_history, indent=2))
        except Exception as e:
            LOGGER.error(f"Failed to save alert history: {e}")

    def _should_alert(self, ticker: str, trigger_type: str) -> bool:
        """Check if we should alert on this ticker (deduplication)."""
        key = f"{ticker}:{trigger_type}"
        if key in self.alert_history:
            last_alert_str = self.alert_history[key]
            last_alert = datetime.fromisoformat(last_alert_str)
            if datetime.now() - last_alert < timedelta(days=self.config.dedup_days):
                return False
        return True

    def _record_alert(self, ticker: str, trigger_type: str):
        """Record that we alerted on this ticker."""
        key = f"{ticker}:{trigger_type}"
        self.alert_history[key] = datetime.now().isoformat()
        self._save_alert_history()

    def detect_alerts(self, scored_df: pd.DataFrame, conviction_df: Optional[pd.DataFrame] = None) -> List[Alert]:
        """
        Detect alerts from screening results.

        Args:
            scored_df: Scored screening results
            conviction_df: Optional conviction list (stocks in multiple themes)

        Returns:
            List of Alert objects
        """
        if not self.config.enabled:
            return []

        alerts = []

        # 1. High master score alerts
        if "score_master" in scored_df.columns:
            high_scores = scored_df[scored_df["score_master"] >= self.config.master_score_threshold]
            for _, row in high_scores.iterrows():
                ticker = str(row["ticker"])
                if self._should_alert(ticker, "high_master_score"):
                    alerts.append(
                        Alert(
                            ticker=ticker,
                            company=str(row.get("company", "")),
                            trigger_type="high_master_score",
                            reason=f"Master score {row['score_master']:.1f} (threshold: {self.config.master_score_threshold})",
                            score=row["score_master"],
                            sector=str(row.get("sector", "")),
                            timestamp=datetime.now(),
                            data={"score": row["score_master"]},
                        )
                    )

        # 2. Insider buying alerts
        if "insider_net_value" in scored_df.columns:
            insider_buying = scored_df[scored_df["insider_net_value"] >= self.config.insider_buying_min]
            for _, row in insider_buying.iterrows():
                ticker = str(row["ticker"])
                if self._should_alert(ticker, "insider_buying"):
                    net_value = row["insider_net_value"]
                    alerts.append(
                        Alert(
                            ticker=ticker,
                            company=str(row.get("company", "")),
                            trigger_type="insider_buying",
                            reason=f"${net_value:,.0f} net insider buying",
                            score=row.get("score_insider_momentum", row.get("score_master", 0)),
                            sector=str(row.get("sector", "")),
                            timestamp=datetime.now(),
                            data={
                                "net_value": net_value,
                                "buys": row.get("insider_total_buys", 0),
                                "sells": row.get("insider_total_sells", 0),
                            },
                        )
                    )

        # 3. Earnings quality alerts
        if "earnings_avg_alpha" in scored_df.columns and "earnings_win_rate" in scored_df.columns:
            earnings_quality = scored_df[
                (scored_df["earnings_avg_alpha"] >= self.config.earnings_alpha_min)
                & (scored_df["earnings_win_rate"] >= self.config.earnings_win_rate_min)
            ]
            for _, row in earnings_quality.iterrows():
                ticker = str(row["ticker"])
                if self._should_alert(ticker, "earnings_quality"):
                    avg_alpha = row["earnings_avg_alpha"] * 100
                    win_rate = row["earnings_win_rate"] * 100
                    alerts.append(
                        Alert(
                            ticker=ticker,
                            company=str(row.get("company", "")),
                            trigger_type="earnings_quality",
                            reason=f"{avg_alpha:.1f}% avg alpha, {win_rate:.0f}% win rate",
                            score=row.get("score_earnings_surprise", row.get("score_master", 0)),
                            sector=str(row.get("sector", "")),
                            timestamp=datetime.now(),
                            data={
                                "avg_alpha": avg_alpha,
                                "win_rate": win_rate,
                                "events": row.get("earnings_total_events", 0),
                            },
                        )
                    )

        # 4. Financial health alerts
        if all(col in scored_df.columns for col in ["net_margin", "roe", "current_ratio"]):
            financial_health = scored_df[
                (scored_df["net_margin"] >= self.config.net_margin_min)
                & (scored_df["roe"] >= self.config.roe_min)
                & (scored_df["current_ratio"] >= self.config.current_ratio_min)
            ]
            for _, row in financial_health.iterrows():
                ticker = str(row["ticker"])
                if self._should_alert(ticker, "financial_health"):
                    net_margin = row["net_margin"] * 100
                    roe = row["roe"] * 100
                    current_ratio = row["current_ratio"]
                    alerts.append(
                        Alert(
                            ticker=ticker,
                            company=str(row.get("company", "")),
                            trigger_type="financial_health",
                            reason=f"{net_margin:.1f}% margin, {roe:.1f}% ROE, {current_ratio:.2f}x current ratio",
                            score=row.get("score_master", 0),
                            sector=str(row.get("sector", "")),
                            timestamp=datetime.now(),
                            data={
                                "net_margin": net_margin,
                                "roe": roe,
                                "current_ratio": current_ratio,
                            },
                        )
                    )

        # 5. High conviction alerts (appears in multiple theme families)
        if conviction_df is not None and "count_families" in conviction_df.columns:
            high_conviction = conviction_df[conviction_df["count_families"] >= self.config.min_theme_families]
            for _, row in high_conviction.iterrows():
                ticker = str(row["ticker"])
                if self._should_alert(ticker, "high_conviction"):
                    count = row["count_families"]
                    families = row.get("families", "")
                    alerts.append(
                        Alert(
                            ticker=ticker,
                            company=str(row.get("company", "")),
                            trigger_type="high_conviction",
                            reason=f"Appears in {count} theme families: {families}",
                            score=row.get("score_master", 0),
                            sector=str(row.get("sector", "")),
                            timestamp=datetime.now(),
                            data={"count": count, "families": families},
                        )
                    )

        # Record all alerts
        for alert in alerts:
            self._record_alert(alert.ticker, alert.trigger_type)

        return alerts

    def send_alerts(self, alerts: List[Alert]):
        """Send alerts via configured channels."""
        if not alerts:
            LOGGER.info("No alerts to send")
            return

        LOGGER.info(f"Sending {len(alerts)} alerts")

        # Format alerts for notification
        message = self._format_alerts_message(alerts)

        # Send via configured channels
        if self.config.email_enabled:
            self._send_email(message, alerts)

        if self.config.slack_enabled:
            self._send_slack(message, alerts)

        if self.config.discord_enabled:
            self._send_discord(message, alerts)

    def _format_alerts_message(self, alerts: List[Alert]) -> str:
        """Format alerts into a readable message."""
        lines = [
            f"🚨 **Finviz Weekly Alerts** - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"\n{len(alerts)} new opportunities detected:\n",
        ]

        # Group by trigger type
        by_type = {}
        for alert in alerts:
            by_type.setdefault(alert.trigger_type, []).append(alert)

        for trigger_type, trigger_alerts in by_type.items():
            lines.append(f"\n### {trigger_type.replace('_', ' ').title()} ({len(trigger_alerts)})")
            for alert in trigger_alerts[:5]:  # Top 5 per type
                lines.append(
                    f"- **{alert.ticker}** ({alert.company[:30]}) - {alert.reason} | Score: {alert.score:.1f}"
                )
            if len(trigger_alerts) > 5:
                lines.append(f"  ... and {len(trigger_alerts) - 5} more")

        return "\n".join(lines)

    def _send_email(self, message: str, alerts: List[Alert]):
        """Send email notification."""
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart

            if not all(
                [
                    self.config.email_to,
                    self.config.email_from,
                    self.config.smtp_host,
                    self.config.smtp_username,
                    self.config.smtp_password,
                ]
            ):
                LOGGER.warning("Email config incomplete, skipping")
                return

            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"Finviz Weekly: {len(alerts)} New Alerts"
            msg["From"] = self.config.email_from
            msg["To"] = self.config.email_to

            # Plain text version
            text_part = MIMEText(message, "plain")
            msg.attach(text_part)

            # Send
            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as server:
                server.starttls()
                server.login(self.config.smtp_username, self.config.smtp_password)
                server.send_message(msg)

            LOGGER.info(f"Sent email alert to {self.config.email_to}")

        except Exception as e:
            LOGGER.error(f"Failed to send email: {e}")

    def _send_slack(self, message: str, alerts: List[Alert]):
        """Send Slack notification."""
        try:
            import requests

            if not self.config.slack_webhook_url:
                LOGGER.warning("Slack webhook URL not configured")
                return

            payload = {"text": message}

            response = requests.post(self.config.slack_webhook_url, json=payload, timeout=10)
            response.raise_for_status()

            LOGGER.info("Sent Slack alert")

        except Exception as e:
            LOGGER.error(f"Failed to send Slack alert: {e}")

    def _send_discord(self, message: str, alerts: List[Alert]):
        """Send Discord notification."""
        try:
            import requests

            if not self.config.discord_webhook_url:
                LOGGER.warning("Discord webhook URL not configured")
                return

            # Discord has a 2000 char limit
            if len(message) > 2000:
                message = message[:1997] + "..."

            payload = {"content": message}

            response = requests.post(self.config.discord_webhook_url, json=payload, timeout=10)
            response.raise_for_status()

            LOGGER.info("Sent Discord alert")

        except Exception as e:
            LOGGER.error(f"Failed to send Discord alert: {e}")


def load_alert_config(config_path: Optional[Path] = None) -> AlertConfig:
    """Load alert configuration from YAML file."""
    import yaml

    if config_path is None:
        config_path = Path("config/alerts_config.yaml")

    if not config_path.exists():
        LOGGER.info(f"No alert config found at {config_path}, using defaults")
        return AlertConfig()

    try:
        config_dict = yaml.safe_load(config_path.read_text())
        return AlertConfig(**config_dict)
    except Exception as e:
        LOGGER.error(f"Failed to load alert config: {e}")
        return AlertConfig()


def run_alerts(data_dir: str = "data/latest", config_path: Optional[str] = None):
    """
    Run alert detection on latest screening results.

    Args:
        data_dir: Directory containing screening results
        config_path: Path to alerts config file
    """
    data_path = Path(data_dir)
    scored_path = data_path / "finviz_scored.parquet"
    conviction_path = data_path / "conviction_2plus.csv"

    if not scored_path.exists():
        LOGGER.error(f"Scored data not found at {scored_path}")
        return

    # Load data
    scored_df = pd.read_parquet(scored_path)
    conviction_df = pd.read_csv(conviction_path) if conviction_path.exists() else None

    # Load config and create engine
    config = load_alert_config(Path(config_path) if config_path else None)
    engine = AlertEngine(config)

    # Detect and send alerts
    alerts = engine.detect_alerts(scored_df, conviction_df)

    if alerts:
        LOGGER.info(f"Detected {len(alerts)} alerts")
        engine.send_alerts(alerts)

        # Print summary
        print(f"\n🚨 {len(alerts)} alerts detected:")
        for alert in alerts[:10]:  # Show first 10
            print(f"  - {alert.ticker}: {alert.reason}")
        if len(alerts) > 10:
            print(f"  ... and {len(alerts) - 10} more")
    else:
        print("No alerts detected")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run alert detection")
    parser.add_argument("--data-dir", default="data/latest", help="Data directory")
    parser.add_argument("--config", help="Path to alerts config file")

    args = parser.parse_args()
    run_alerts(args.data_dir, args.config)
