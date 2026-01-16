"""
Task Scheduling with Airflow/Prefect

Provides DAGs and flows for:
- Daily data scraping
- Weekly screening
- Monthly rebalancing
- Continuous monitoring
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

LOGGER = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class TaskResult:
    """Result from task execution."""
    task_id: str
    status: TaskStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    output: Any = None
    error: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScheduleConfig:
    """Schedule configuration."""
    enabled: bool = True
    cron: Optional[str] = None  # Cron expression
    interval_minutes: Optional[int] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    max_retries: int = 3
    retry_delay_minutes: int = 5
    timeout_minutes: int = 60


# =============================================================================
# Task Definitions
# =============================================================================

def task_scrape_fundamentals(
    mode: str = "universe",
    ticker_limit: Optional[int] = None,
    output_dir: str = "data",
) -> TaskResult:
    """
    Task: Scrape fundamentals data.

    Args:
        mode: Scraping mode ('universe' or 'tickers')
        ticker_limit: Limit number of tickers
        output_dir: Output directory

    Returns:
        TaskResult
    """
    task_id = f"scrape_fundamentals_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    start_time = datetime.now()

    try:
        from .config import env_config
        from .http import create_session
        from .pipeline import execute

        config = env_config(
            mode=mode,
            ticker_limit=ticker_limit,
            out_dir=output_dir,
        )

        session = create_session(config.http)
        result = execute(session, config)

        return TaskResult(
            task_id=task_id,
            status=TaskStatus.SUCCESS,
            start_time=start_time,
            end_time=datetime.now(),
            output=result,
            metrics={"tickers_scraped": result.get("count", 0) if isinstance(result, dict) else 0},
        )

    except Exception as e:
        LOGGER.error(f"Scrape fundamentals failed: {e}")
        return TaskResult(
            task_id=task_id,
            status=TaskStatus.FAILED,
            start_time=start_time,
            end_time=datetime.now(),
            error=str(e),
        )


def task_run_screening(
    output_dir: str = "data",
    top_n: int = 50,
    themes: Optional[List[str]] = None,
) -> TaskResult:
    """
    Task: Run stock screening.

    Args:
        output_dir: Data directory
        top_n: Top N stocks per screen
        themes: Specific themes to run

    Returns:
        TaskResult
    """
    task_id = f"screening_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    start_time = datetime.now()

    try:
        from .screen import run_screening

        result = run_screening(
            out_dir=output_dir,
            top_n=top_n,
        )

        return TaskResult(
            task_id=task_id,
            status=TaskStatus.SUCCESS,
            start_time=start_time,
            end_time=datetime.now(),
            output=result,
        )

    except Exception as e:
        LOGGER.error(f"Screening failed: {e}")
        return TaskResult(
            task_id=task_id,
            status=TaskStatus.FAILED,
            start_time=start_time,
            end_time=datetime.now(),
            error=str(e),
        )


def task_update_prices(
    tickers: List[str],
    period: str = "1mo",
) -> TaskResult:
    """
    Task: Update price data.

    Args:
        tickers: List of tickers to update
        period: Data period

    Returns:
        TaskResult
    """
    task_id = f"update_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    start_time = datetime.now()

    try:
        import yfinance as yf
        import pandas as pd

        all_data = {}
        for ticker in tickers:
            try:
                data = yf.download(ticker, period=period, progress=False)
                if not data.empty:
                    all_data[ticker] = data
            except Exception as e:
                LOGGER.warning(f"Failed to get {ticker}: {e}")

        return TaskResult(
            task_id=task_id,
            status=TaskStatus.SUCCESS,
            start_time=start_time,
            end_time=datetime.now(),
            output=all_data,
            metrics={"tickers_updated": len(all_data)},
        )

    except Exception as e:
        LOGGER.error(f"Update prices failed: {e}")
        return TaskResult(
            task_id=task_id,
            status=TaskStatus.FAILED,
            start_time=start_time,
            end_time=datetime.now(),
            error=str(e),
        )


def task_check_drift(
    portfolio_id: str = "default",
    threshold: float = 0.05,
) -> TaskResult:
    """
    Task: Check portfolio drift.

    Args:
        portfolio_id: Portfolio identifier
        threshold: Drift threshold

    Returns:
        TaskResult
    """
    task_id = f"check_drift_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    start_time = datetime.now()

    try:
        # Placeholder - would integrate with rebalancer
        drift_detected = False
        max_drift = 0.0

        return TaskResult(
            task_id=task_id,
            status=TaskStatus.SUCCESS,
            start_time=start_time,
            end_time=datetime.now(),
            output={"drift_detected": drift_detected, "max_drift": max_drift},
            metrics={"max_drift": max_drift},
        )

    except Exception as e:
        LOGGER.error(f"Drift check failed: {e}")
        return TaskResult(
            task_id=task_id,
            status=TaskStatus.FAILED,
            start_time=start_time,
            end_time=datetime.now(),
            error=str(e),
        )


def task_send_daily_report(
    recipients: Optional[List[str]] = None,
) -> TaskResult:
    """
    Task: Generate and send daily report.

    Args:
        recipients: Email/notification recipients

    Returns:
        TaskResult
    """
    task_id = f"daily_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    start_time = datetime.now()

    try:
        from .report import write_report_from_latest

        write_report_from_latest(out_dir="data")

        return TaskResult(
            task_id=task_id,
            status=TaskStatus.SUCCESS,
            start_time=start_time,
            end_time=datetime.now(),
            output="Report generated",
        )

    except Exception as e:
        LOGGER.error(f"Daily report failed: {e}")
        return TaskResult(
            task_id=task_id,
            status=TaskStatus.FAILED,
            start_time=start_time,
            end_time=datetime.now(),
            error=str(e),
        )


# =============================================================================
# Airflow DAG Definitions (Template)
# =============================================================================

AIRFLOW_DAG_TEMPLATE = '''
"""
Airflow DAG for Finviz Scraper

To use:
1. Copy this file to your Airflow dags/ directory
2. Configure connection and variables
3. Enable the DAG
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'finviz',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'finviz_daily_scrape',
    default_args=default_args,
    description='Daily fundamentals scraping',
    schedule_interval='0 6 * * 1-5',  # 6 AM Mon-Fri
    start_date=days_ago(1),
    catchup=False,
    tags=['finviz', 'scraping'],
) as dag:

    start = DummyOperator(task_id='start')

    def _scrape_fundamentals(**context):
        from finviz_weekly.scheduler import task_scrape_fundamentals
        result = task_scrape_fundamentals(mode='universe', ticker_limit=None)
        if result.status.value == 'failed':
            raise Exception(result.error)
        return result.metrics

    scrape = PythonOperator(
        task_id='scrape_fundamentals',
        python_callable=_scrape_fundamentals,
        execution_timeout=timedelta(hours=4),
    )

    def _run_screening(**context):
        from finviz_weekly.scheduler import task_run_screening
        result = task_run_screening()
        if result.status.value == 'failed':
            raise Exception(result.error)

    screen = PythonOperator(
        task_id='run_screening',
        python_callable=_run_screening,
        execution_timeout=timedelta(minutes=30),
    )

    def _generate_report(**context):
        from finviz_weekly.scheduler import task_send_daily_report
        result = task_send_daily_report()
        if result.status.value == 'failed':
            raise Exception(result.error)

    report = PythonOperator(
        task_id='generate_report',
        python_callable=_generate_report,
        execution_timeout=timedelta(minutes=10),
    )

    end = DummyOperator(task_id='end')

    start >> scrape >> screen >> report >> end


# Weekly rebalancing DAG
with DAG(
    'finviz_weekly_rebalance',
    default_args=default_args,
    description='Weekly portfolio rebalancing check',
    schedule_interval='0 7 * * 6',  # 7 AM Saturday
    start_date=days_ago(1),
    catchup=False,
    tags=['finviz', 'rebalancing'],
) as rebalance_dag:

    start = DummyOperator(task_id='start')

    def _check_drift(**context):
        from finviz_weekly.scheduler import task_check_drift
        result = task_check_drift()
        if result.status.value == 'failed':
            raise Exception(result.error)
        return result.output

    check = PythonOperator(
        task_id='check_drift',
        python_callable=_check_drift,
    )

    end = DummyOperator(task_id='end')

    start >> check >> end
'''


# =============================================================================
# Prefect Flow Definitions (Template)
# =============================================================================

PREFECT_FLOW_TEMPLATE = '''
"""
Prefect flows for Finviz Scraper

To use:
1. pip install prefect
2. Configure Prefect server/cloud
3. Deploy this flow
"""

from datetime import timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash


@task(
    retries=3,
    retry_delay_seconds=300,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=12),
)
def scrape_fundamentals(mode: str = "universe", ticker_limit: int = None):
    """Scrape fundamentals data."""
    from finviz_weekly.scheduler import task_scrape_fundamentals
    logger = get_run_logger()

    result = task_scrape_fundamentals(mode=mode, ticker_limit=ticker_limit)

    if result.status.value == "failed":
        raise Exception(result.error)

    logger.info(f"Scraped {result.metrics.get('tickers_scraped', 0)} tickers")
    return result


@task(retries=2)
def run_screening(output_dir: str = "data"):
    """Run stock screening."""
    from finviz_weekly.scheduler import task_run_screening
    logger = get_run_logger()

    result = task_run_screening(output_dir=output_dir)

    if result.status.value == "failed":
        raise Exception(result.error)

    logger.info("Screening completed")
    return result


@task
def generate_report():
    """Generate daily report."""
    from finviz_weekly.scheduler import task_send_daily_report

    result = task_send_daily_report()

    if result.status.value == "failed":
        raise Exception(result.error)

    return result


@task
def check_portfolio_drift(portfolio_id: str = "default"):
    """Check portfolio drift."""
    from finviz_weekly.scheduler import task_check_drift

    result = task_check_drift(portfolio_id=portfolio_id)
    return result


@flow(name="Daily Finviz Pipeline")
def daily_pipeline(
    mode: str = "universe",
    ticker_limit: int = None,
    run_screening: bool = True,
):
    """
    Daily data pipeline.

    Scrapes fundamentals, runs screening, and generates report.
    """
    # Scrape
    scrape_result = scrape_fundamentals(mode=mode, ticker_limit=ticker_limit)

    # Screen
    if run_screening:
        screen_result = run_screening()

    # Report
    report_result = generate_report()

    return {
        "scrape": scrape_result,
        "screen": screen_result if run_screening else None,
        "report": report_result,
    }


@flow(name="Weekly Rebalance Check")
def weekly_rebalance_flow(portfolio_id: str = "default"):
    """
    Weekly rebalancing check.

    Checks drift and triggers rebalancing if needed.
    """
    drift_result = check_portfolio_drift(portfolio_id=portfolio_id)

    if drift_result.output.get("drift_detected"):
        # Could trigger rebalancing here
        pass

    return drift_result


if __name__ == "__main__":
    # Run locally for testing
    daily_pipeline(mode="universe", ticker_limit=100)
'''


# =============================================================================
# Simple Scheduler (No dependencies)
# =============================================================================

class SimpleScheduler:
    """
    Simple task scheduler without external dependencies.

    For production, use Airflow or Prefect.
    """

    def __init__(self):
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self.history: List[TaskResult] = []
        self._running = False

    def register_task(
        self,
        name: str,
        func: Callable,
        schedule: ScheduleConfig,
        **kwargs,
    ) -> None:
        """Register a task."""
        self.tasks[name] = {
            "func": func,
            "schedule": schedule,
            "kwargs": kwargs,
            "last_run": None,
            "next_run": schedule.start_date or datetime.now(),
        }

    def _should_run(self, task_name: str) -> bool:
        """Check if task should run."""
        task = self.tasks.get(task_name)
        if not task:
            return False

        schedule = task["schedule"]
        if not schedule.enabled:
            return False

        now = datetime.now()

        if schedule.end_date and now > schedule.end_date:
            return False

        if task["next_run"] and now < task["next_run"]:
            return False

        return True

    def _update_next_run(self, task_name: str) -> None:
        """Update next run time for task."""
        task = self.tasks.get(task_name)
        if not task:
            return

        schedule = task["schedule"]
        now = datetime.now()

        if schedule.interval_minutes:
            task["next_run"] = now + timedelta(minutes=schedule.interval_minutes)
        elif schedule.cron:
            # Would need croniter for proper cron parsing
            # Fallback to daily
            task["next_run"] = now + timedelta(days=1)
        else:
            task["next_run"] = now + timedelta(days=1)

        task["last_run"] = now

    def run_task(self, task_name: str) -> Optional[TaskResult]:
        """Run a specific task."""
        task = self.tasks.get(task_name)
        if not task:
            LOGGER.error(f"Task not found: {task_name}")
            return None

        LOGGER.info(f"Running task: {task_name}")

        try:
            result = task["func"](**task["kwargs"])
            self.history.append(result)
            self._update_next_run(task_name)
            return result

        except Exception as e:
            LOGGER.error(f"Task {task_name} failed: {e}")
            result = TaskResult(
                task_id=task_name,
                status=TaskStatus.FAILED,
                start_time=datetime.now(),
                error=str(e),
            )
            self.history.append(result)
            return result

    def run_due_tasks(self) -> List[TaskResult]:
        """Run all due tasks."""
        results = []
        for task_name in self.tasks:
            if self._should_run(task_name):
                result = self.run_task(task_name)
                if result:
                    results.append(result)
        return results

    def get_status(self) -> Dict[str, Any]:
        """Get scheduler status."""
        return {
            "tasks": {
                name: {
                    "enabled": task["schedule"].enabled,
                    "last_run": task["last_run"].isoformat() if task["last_run"] else None,
                    "next_run": task["next_run"].isoformat() if task["next_run"] else None,
                }
                for name, task in self.tasks.items()
            },
            "history_count": len(self.history),
            "recent_failures": sum(
                1 for r in self.history[-10:]
                if r.status == TaskStatus.FAILED
            ),
        }


# =============================================================================
# Export DAG Templates
# =============================================================================

def export_airflow_dag(output_path: str = "dags/finviz_dag.py") -> None:
    """Export Airflow DAG template."""
    from pathlib import Path

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(AIRFLOW_DAG_TEMPLATE)
    LOGGER.info(f"Exported Airflow DAG to {output_path}")


def export_prefect_flow(output_path: str = "flows/finviz_flow.py") -> None:
    """Export Prefect flow template."""
    from pathlib import Path

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(PREFECT_FLOW_TEMPLATE)
    LOGGER.info(f"Exported Prefect flow to {output_path}")
