"""Command line interface for finviz_weekly."""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import List

import pandas as pd

from .config import env_config
from .http import create_session
from .pipeline import execute
from .screen import run_screening
from .learn import train_weights
from .report import write_report_from_latest
from .debate import run_debate
from .insider import scrape_insider_trading, aggregate_insider_by_ticker
from .earnings import scrape_earnings_reactions, aggregate_earnings_stats
from .financials import scrape_financial_statements, calculate_financial_ratios


LOGGER = logging.getLogger(__name__)


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finviz weekly scraper")

    parser.add_argument(
        "command",
        nargs="?",
        default="run",
        choices=[
            "run", "screen", "train", "report", "debate",
            "insider", "earnings", "financials",
            "model-registry", "drift-check", "retrain",
            "correlation", "custom-factors",
            "options-flow", "short-interest",
            "analyst-estimates", "regime-status",
            "institutional", "macro", "sentiment", "stacking",
            "factor-analytics", "risk-check", "paper-trade",
            "reddit", "etf-flows", "sec-filings", "dashboard",
        ],
        help="Command to execute (default: run).",
    )

    # run args
    parser.add_argument("--mode", choices=["universe", "tickers"], required=False)
    parser.add_argument("--tickers", help="Comma separated list of tickers")
    parser.add_argument("--tickers-file", help="Path to file with tickers")
    parser.add_argument("--industry-limit", type=int)
    parser.add_argument("--ticker-limit", type=int)

    parser.add_argument("--rate-per-sec", type=float, default=0.5)
    parser.add_argument("--page-sleep-min", type=float, default=0.8)
    parser.add_argument("--page-sleep-max", type=float, default=1.8)
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--checkpoint-every", type=int, default=10)
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resume from today's partial checkpoint (default: enabled).",
    )
    parser.add_argument("--out", default="data")
    parser.add_argument("--formats", default="parquet,csv")
    parser.add_argument("--log-level", default="INFO")

    # latest snapshot options (run)
    parser.add_argument(
        "--latest-only-ok",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Only write ok rows to data/latest (default: enabled).",
    )
    parser.add_argument(
        "--latest-include-as-of-date",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include as_of_date in data/latest (default: enabled).",
    )

    # screening args (screen)
    parser.add_argument("--top", type=int, default=50, help="Top-N tickers per screen.")
    parser.add_argument("--min-market-cap", type=float, default=300_000_000)
    parser.add_argument("--min-price", type=float, default=1.0)
    parser.add_argument("--candidates-max", type=int, default=100)
    parser.add_argument("--use-learned", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--learned-min-unique-dates", type=int, default=12)
    parser.add_argument("--learned-min-forward-rows", type=int, default=200)
    parser.add_argument("--watchlist-file")
    parser.add_argument("--normalize-by", choices=["none", "sector", "industry"], default="none")

    # debate args
    parser.add_argument("--input", choices=["candidates", "conviction2", "tickers"], default="candidates")
    parser.add_argument("--max-tickers", type=int, default=30)
    parser.add_argument("--research", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--recency-days", type=int, default=30)
    parser.add_argument("--max-queries-per-ticker", type=int, default=12)
    parser.add_argument("--max-results-per-query", type=int, default=3)
    parser.add_argument("--evidence-max", type=int, default=20)
    parser.add_argument("--cache-days", type=int, default=14)
    parser.add_argument("--timeout-seconds", type=int, default=15)
    parser.add_argument("--as-of")
    parser.add_argument("--provider", choices=["openai", "mock"], default="openai")
    parser.add_argument("--model", help="LLM model name (default env OPENAI_MODEL or gpt-5-mini)")
    parser.add_argument("--verbose", action=argparse.BooleanOptionalAction, default=False)

    # training args (train)
    parser.add_argument("--min-rows-per-group", type=int, default=250)
    parser.add_argument("--group-col", type=str, default="sector")

    # new scraper args (insider, earnings, financials)
    parser.add_argument("--ticker", help="Single ticker to scrape")
    parser.add_argument("--output-format", choices=["json", "csv"], default="json")

    # enhanced pipeline args (run command)
    parser.add_argument(
        "--include-insider",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Include insider trading data in main pipeline (default: disabled).",
    )
    parser.add_argument(
        "--include-earnings",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Include earnings reaction data in main pipeline (default: disabled).",
    )
    parser.add_argument(
        "--include-financials",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Include financial statement data in main pipeline (default: disabled).",
    )
    parser.add_argument(
        "--enhanced-scoring",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Use enhanced scoring with new data sources (default: disabled).",
    )

    # Model registry args (model-registry command)
    parser.add_argument(
        "--registry-path",
        default="models/registry",
        help="Path to model registry directory (default: models/registry).",
    )
    parser.add_argument(
        "--model-id",
        help="Model ID for registry operations.",
    )
    parser.add_argument(
        "--registry-action",
        choices=["list", "compare", "deploy", "status", "cleanup"],
        default="list",
        help="Registry action to perform (default: list).",
    )
    parser.add_argument(
        "--model-type",
        help="Filter by model type (e.g., xgboost, ensemble).",
    )
    parser.add_argument(
        "--keep-last-n",
        type=int,
        default=5,
        help="Number of models to keep during cleanup (default: 5).",
    )

    # Drift detection args (drift-check command)
    parser.add_argument(
        "--ic-threshold",
        type=float,
        default=0.20,
        help="IC drop threshold for drift alert (default: 0.20).",
    )
    parser.add_argument(
        "--sharpe-threshold",
        type=float,
        default=0.30,
        help="Sharpe drop threshold for drift alert (default: 0.30).",
    )
    parser.add_argument(
        "--drift-output",
        help="Path to save drift report JSON.",
    )

    # Retraining args (retrain command)
    parser.add_argument(
        "--force-retrain",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Force retraining even if not scheduled (default: disabled).",
    )
    parser.add_argument(
        "--retrain-interval",
        type=int,
        default=30,
        help="Days between scheduled retrains (default: 30).",
    )
    parser.add_argument(
        "--training-lookback",
        type=int,
        default=730,
        help="Days of training data to use (default: 730).",
    )
    parser.add_argument(
        "--tune-hyperparams",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable hyperparameter tuning during retraining (default: disabled).",
    )

    # Correlation analysis args (correlation command)
    parser.add_argument(
        "--correlation-threshold",
        type=float,
        default=0.7,
        help="Threshold for flagging high correlations (default: 0.7).",
    )
    parser.add_argument(
        "--rolling-window",
        type=int,
        default=90,
        help="Rolling window size for correlation analysis (default: 90 days).",
    )
    parser.add_argument(
        "--n-clusters",
        type=int,
        default=3,
        help="Number of factor clusters (default: 3).",
    )
    parser.add_argument(
        "--correlation-output",
        help="Path to save correlation analysis results JSON.",
    )
    parser.add_argument(
        "--plot-output",
        help="Path to save correlation heatmap plot.",
    )

    # Custom factors args (custom-factors command)
    parser.add_argument(
        "--factors-action",
        choices=["list", "create", "test", "delete", "compute"],
        default="list",
        help="Custom factors action (default: list).",
    )
    parser.add_argument(
        "--factor-name",
        help="Name for custom factor.",
    )
    parser.add_argument(
        "--factor-expression",
        help="Expression for custom factor (e.g., '{pe} / {eps_growth_next_y}').",
    )
    parser.add_argument(
        "--factor-description",
        help="Description for custom factor.",
    )
    parser.add_argument(
        "--factor-category",
        default="custom",
        help="Category for custom factor (default: custom).",
    )
    parser.add_argument(
        "--higher-is-better",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Whether higher factor values are better (default: True).",
    )

    # Options flow args (options-flow command)
    parser.add_argument(
        "--volume-multiplier",
        type=float,
        default=2.0,
        help="Volume multiplier for unusual activity detection (default: 2.0).",
    )
    parser.add_argument(
        "--min-dte",
        type=int,
        default=7,
        help="Minimum days to expiration for options (default: 7).",
    )
    parser.add_argument(
        "--max-dte",
        type=int,
        default=90,
        help="Maximum days to expiration for options (default: 90).",
    )
    parser.add_argument(
        "--options-output",
        help="Path to save options flow data (JSON or CSV).",
    )

    # Short interest args (short-interest command)
    parser.add_argument(
        "--high-short-pct",
        type=float,
        default=15.0,
        help="Threshold for high short interest percentage (default: 15.0).",
    )
    parser.add_argument(
        "--squeeze-score-min",
        type=float,
        default=50.0,
        help="Minimum squeeze score to flag as candidate (default: 50.0).",
    )
    parser.add_argument(
        "--short-output",
        help="Path to save short interest data (JSON or CSV).",
    )

    # Analyst estimates args (analyst-estimates command)
    parser.add_argument(
        "--min-revision-pct",
        type=float,
        default=5.0,
        help="Minimum EPS revision percentage to flag (default: 5.0).",
    )
    parser.add_argument(
        "--min-analysts",
        type=int,
        default=5,
        help="Minimum number of analyst ratings (default: 5).",
    )
    parser.add_argument(
        "--analyst-output",
        help="Path to save analyst estimates data (JSON or CSV).",
    )

    # Institutional ownership args (institutional command)
    parser.add_argument(
        "--min-holders",
        type=int,
        default=5,
        help="Minimum institutional holders for analysis (default: 5).",
    )
    parser.add_argument(
        "--institutional-output",
        help="Path to save institutional ownership data (JSON or CSV).",
    )

    # Macro indicators args (macro command)
    parser.add_argument(
        "--macro-output",
        help="Path to save macro indicators data (JSON).",
    )

    # News sentiment args (sentiment command)
    parser.add_argument(
        "--use-llm-sentiment",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Use LLM for sentiment analysis (requires ANTHROPIC_API_KEY).",
    )
    parser.add_argument(
        "--sentiment-output",
        help="Path to save sentiment data (JSON or CSV).",
    )

    # Stacking ensemble args (stacking command)
    parser.add_argument(
        "--stacking-action",
        choices=["train", "predict", "evaluate", "weights"],
        default="weights",
        help="Stacking ensemble action (default: weights).",
    )
    parser.add_argument(
        "--ensemble-path",
        default="models/ensemble",
        help="Path to save/load stacking ensemble (default: models/ensemble).",
    )

    # Factor analytics args (factor-analytics command)
    parser.add_argument(
        "--analytics-action",
        choices=["pca", "correlation", "decay", "all"],
        default="all",
        help="Factor analytics action (default: all).",
    )
    parser.add_argument(
        "--variance-threshold",
        type=float,
        default=0.90,
        help="PCA variance threshold (default: 0.90).",
    )

    # Paper trading args (paper-trade command)
    parser.add_argument(
        "--initial-cash",
        type=float,
        default=100000.0,
        help="Initial paper trading cash (default: $100,000).",
    )
    parser.add_argument(
        "--trade-action",
        choices=["status", "buy", "sell", "history", "reset"],
        default="status",
        help="Paper trading action (default: status).",
    )
    parser.add_argument(
        "--shares",
        type=int,
        help="Number of shares for trade.",
    )

    # Reddit sentiment args (reddit command)
    parser.add_argument(
        "--subreddits",
        default="wallstreetbets,stocks,investing",
        help="Comma-separated subreddits to scrape (default: wsb,stocks,investing).",
    )
    parser.add_argument(
        "--reddit-output",
        help="Path to save Reddit sentiment data.",
    )

    # ETF flows args (etf-flows command)
    parser.add_argument(
        "--flow-period",
        choices=["1d", "5d", "21d"],
        default="21d",
        help="ETF flow analysis period (default: 21d).",
    )

    # SEC filings args (sec-filings command)
    parser.add_argument(
        "--filing-type",
        choices=["8-K", "10-Q", "10-K", "4", "all"],
        default="all",
        help="SEC filing type to fetch (default: all).",
    )
    parser.add_argument(
        "--filing-days",
        type=int,
        default=30,
        help="Days of filings to fetch (default: 30).",
    )

    return parser.parse_args(argv)


def _load_tickers(args: argparse.Namespace) -> list[str]:
    tickers: list[str] = []
    if args.tickers:
        tickers.extend([t.strip().upper() for t in args.tickers.split(",") if t.strip()])
    if args.tickers_file:
        path = Path(args.tickers_file)
        if path.exists():
            tickers.extend([t.strip().upper() for t in path.read_text().splitlines() if t.strip()])
    return tickers


def _run_insider_command(args: argparse.Namespace) -> None:
    """Run insider trading scraper command."""
    import json
    import pandas as pd
    from .config import HttpConfig

    tickers = []
    if args.ticker:
        tickers = [args.ticker.upper()]
    elif args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    elif args.tickers_file:
        path = Path(args.tickers_file)
        if path.exists():
            tickers = [t.strip().upper() for t in path.read_text().splitlines() if t.strip()]

    if not tickers:
        raise SystemExit("Error: Must provide --ticker, --tickers, or --tickers-file")

    LOGGER.info("Scraping insider trading for %d tickers", len(tickers))
    http_config = HttpConfig(proxy=None)
    session = create_session(http_config)

    all_transactions = []
    for ticker in tickers:
        try:
            transactions = scrape_insider_trading(ticker, session, http_config)
            all_transactions.extend(transactions)
            LOGGER.info("  %s: %d transactions", ticker, len(transactions))
        except Exception as e:
            LOGGER.error("  %s: %s", ticker, e)

    if not all_transactions:
        LOGGER.warning("No insider transactions found")
        return

    # Aggregate
    stats = aggregate_insider_by_ticker(all_transactions)

    # Output
    if args.output_format == "json":
        print(json.dumps(stats, indent=2))
    else:
        df = pd.DataFrame(stats).T
        print(df.to_csv())


def _run_earnings_command(args: argparse.Namespace) -> None:
    """Run earnings reactions scraper command."""
    import json
    import pandas as pd
    from .config import HttpConfig

    tickers = []
    if args.ticker:
        tickers = [args.ticker.upper()]
    elif args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    elif args.tickers_file:
        path = Path(args.tickers_file)
        if path.exists():
            tickers = [t.strip().upper() for t in path.read_text().splitlines() if t.strip()]

    if not tickers:
        raise SystemExit("Error: Must provide --ticker, --tickers, or --tickers-file")

    LOGGER.info("Scraping earnings reactions for %d tickers", len(tickers))
    http_config = HttpConfig(proxy=None)
    session = create_session(http_config)

    all_earnings = []
    for ticker in tickers:
        try:
            earnings = scrape_earnings_reactions(ticker, session, http_config)
            all_earnings.extend(earnings)
            LOGGER.info("  %s: %d earnings events", ticker, len(earnings))
        except Exception as e:
            LOGGER.error("  %s: %s", ticker, e)

    if not all_earnings:
        LOGGER.warning("No earnings events found")
        return

    # Aggregate
    stats = aggregate_earnings_stats(all_earnings)

    # Output
    if args.output_format == "json":
        print(json.dumps(stats, indent=2))
    else:
        df = pd.DataFrame(stats).T
        print(df.to_csv())


def _run_financials_command(args: argparse.Namespace) -> None:
    """Run financial statements scraper command."""
    from .config import HttpConfig

    tickers = []
    if args.ticker:
        tickers = [args.ticker.upper()]
    elif args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    elif args.tickers_file:
        path = Path(args.tickers_file)
        if path.exists():
            tickers = [t.strip().upper() for t in path.read_text().splitlines() if t.strip()]

    if not tickers:
        raise SystemExit("Error: Must provide --ticker, --tickers, or --tickers-file")

    LOGGER.info("Scraping financial statements for %d tickers", len(tickers))
    http_config = HttpConfig(proxy=None)
    session = create_session(http_config)

    results = {}
    for ticker in tickers:
        try:
            statements = scrape_financial_statements(ticker, session, http_config)
            ratios = calculate_financial_ratios(statements)
            results[ticker] = {"statements": statements, "ratios": ratios}
            LOGGER.info("  %s: %d ratios calculated", ticker, len(ratios))
        except Exception as e:
            LOGGER.error("  %s: %s", ticker, e)

    if not results:
        LOGGER.warning("No financial data found")
        return

    # Output (JSON only for complex nested data)
    print(json.dumps(results, indent=2))


def _run_model_registry_command(args: argparse.Namespace) -> None:
    """Run model registry command."""
    from .model_registry import ModelRegistry

    registry_path = Path(args.registry_path)
    registry = ModelRegistry(registry_path)

    action = args.registry_action

    if action == "list":
        models = registry.list_models(model_type=args.model_type)
        if not models:
            print("No models registered.")
            return

        print(f"\nRegistered Models ({len(models)} total):")
        print("-" * 80)
        for m in models:
            prod_marker = " [PRODUCTION]" if m.is_production else ""
            print(f"  {m.model_id}{prod_marker}")
            print(f"    Type: {m.model_type} | Version: {m.version}")
            print(f"    Trained: {m.trained_at[:10]} | Samples: {m.training_samples}")
            print(f"    Val IC: {m.val_ic:.4f} | Features: {m.feature_count}")
            print()

    elif action == "compare":
        df = registry.compare_models(model_type=args.model_type)
        if df.empty:
            print("No models to compare.")
            return

        print("\nModel Comparison:")
        print("-" * 100)
        print(df.to_string(index=False))

    elif action == "deploy":
        if not args.model_id:
            raise SystemExit("Error: --model-id required for deploy action")

        metadata = registry.deploy_to_production(args.model_id)
        print(f"Deployed model to production: {metadata.model_id}")

    elif action == "status":
        stats = registry.get_model_stats()
        print("\nModel Registry Status:")
        print("-" * 40)
        print(f"  Total models: {stats['total_models']}")
        print(f"  Production model: {stats.get('production_model', 'None')}")
        if stats['total_models'] > 0:
            print(f"  Latest model: {stats.get('latest_model', 'N/A')}")
            print(f"  Best val IC: {stats.get('best_val_ic', 'N/A')}")
        print(f"  Model types: {stats.get('model_types', {})}")

    elif action == "cleanup":
        keep_n = args.keep_last_n
        deleted = registry.cleanup_old_models(
            keep_last_n=keep_n,
            model_type=args.model_type,
            archive=True,
        )
        print(f"Cleaned up {len(deleted)} old models (kept last {keep_n})")
        for model_id in deleted:
            print(f"  Archived: {model_id}")


def _run_drift_check_command(args: argparse.Namespace) -> None:
    """Run drift detection command."""
    from .drift_detection import DriftDetector, DriftThresholds
    from .model_registry import ModelRegistry

    registry_path = Path(args.registry_path)
    registry = ModelRegistry(registry_path)

    # Get model to check
    model_id = args.model_id
    if not model_id:
        model_id = registry.get_production_model_id()
        if not model_id:
            raise SystemExit("Error: No production model deployed. Use --model-id to specify a model.")

    # Load data
    data_dir = Path(args.out)
    history_path = data_dir / "history" / "finviz_fundamentals_history.parquet"
    prices_path = data_dir / "history" / "prices.parquet"

    if not history_path.exists():
        raise SystemExit(f"Error: History file not found: {history_path}")
    if not prices_path.exists():
        raise SystemExit(f"Error: Prices file not found: {prices_path}")

    # Configure thresholds
    thresholds = DriftThresholds(
        ic_drop_pct=args.ic_threshold,
        sharpe_drop_pct=args.sharpe_threshold,
    )

    # Run drift check
    from .drift_detection import run_drift_check

    output_path = Path(args.drift_output) if args.drift_output else None

    report = run_drift_check(
        model_id=model_id,
        data_path=history_path,
        prices_path=prices_path,
        registry_path=registry_path,
        output_path=output_path,
    )

    # Print summary
    print("\nDrift Detection Report")
    print("=" * 60)
    print(f"Model: {report.model_id}")
    print(f"Analysis Date: {report.analysis_date[:10]}")
    print()

    print("Performance Drift:")
    print(f"  Baseline IC: {report.baseline_ic:.4f}")
    print(f"  Current IC:  {report.current_ic:.4f}")
    print(f"  Change:      {report.ic_change_pct:+.1%} {'[ALERT]' if report.ic_alert else ''}")
    print()

    print("Feature Drift:")
    print(f"  Total features:   {report.total_features}")
    print(f"  Shifted features: {report.shifted_features} ({report.shifted_features_pct:.1%})")
    if report.shifted_feature_names:
        print(f"  Shifted: {', '.join(report.shifted_feature_names[:5])}")
    print()

    print("Assessment:")
    print(f"  Severity: {report.severity.upper()}")
    print(f"  Drift Detected: {report.overall_drift_detected}")
    print(f"  Recommendation: {report.recommended_action}")

    if output_path:
        print(f"\nFull report saved to: {output_path}")


def _run_retrain_command(args: argparse.Namespace) -> None:
    """Run automated retraining command."""
    from .ml_train import ModelConfig
    from .model_registry import ModelRegistry
    from .retraining import RetrainingConfig, RetrainingScheduler, run_automated_retraining

    registry_path = Path(args.registry_path)
    data_dir = Path(args.out)

    history_path = data_dir / "history" / "finviz_fundamentals_history.parquet"
    prices_path = data_dir / "history" / "prices.parquet"

    if not history_path.exists():
        raise SystemExit(f"Error: History file not found: {history_path}")
    if not prices_path.exists():
        raise SystemExit(f"Error: Prices file not found: {prices_path}")

    # Configure model training
    model_config = ModelConfig(
        tune_hyperparams=args.tune_hyperparams,
    )

    # Configure retraining
    retrain_config = RetrainingConfig(
        retrain_interval_days=args.retrain_interval,
        training_lookback_days=args.training_lookback,
        model_config=model_config,
    )

    # Run retraining
    result = run_automated_retraining(
        history_path=history_path,
        prices_path=prices_path,
        registry_path=registry_path,
        config=retrain_config,
        force=args.force_retrain,
    )

    # Print summary
    print("\nRetraining Result")
    print("=" * 60)
    print(f"Retrain ID: {result.retrain_id}")
    print(f"Trigger: {result.trigger}")
    print(f"Training Success: {result.training_success}")
    print()

    if result.training_success:
        print("Performance Comparison:")
        print(f"  Old IC: {result.old_ic:.4f}")
        print(f"  New IC: {result.new_ic:.4f}")
        print(f"  Improvement: {result.ic_improvement:+.4f}")
        print()

    print("Deployment:")
    print(f"  Decision: {result.deployment_decision}")
    print(f"  Reason: {result.deployment_reason}")

    if result.new_model_id:
        print(f"\nNew Model ID: {result.new_model_id}")

    if result.error_message:
        print(f"\nError: {result.error_message}")


def _run_correlation_command(args: argparse.Namespace) -> None:
    """Run correlation analysis command."""
    from .correlation_analysis import CorrelationAnalyzer, CorrelationConfig
    from .visualization import FactorVisualizer

    data_dir = Path(args.out)
    history_path = data_dir / "history" / "finviz_fundamentals_history.parquet"

    if not history_path.exists():
        raise SystemExit(f"Error: History file not found: {history_path}")

    # Load data
    import pandas as pd
    data = pd.read_parquet(history_path)

    # Configure analysis
    config = CorrelationConfig(
        rolling_window_days=args.rolling_window,
        n_clusters=args.n_clusters,
    )

    # Run analysis
    analyzer = CorrelationAnalyzer(config)
    result = analyzer.analyze(data, correlation_threshold=args.correlation_threshold)

    # Print summary
    print("\nCorrelation Analysis Results")
    print("=" * 60)
    print(f"Factors analyzed: {len(result.factors_analyzed)}")
    print(f"High correlation pairs (>{args.correlation_threshold}): {len(result.high_correlation_pairs)}")
    print()

    if result.high_correlation_pairs:
        print("Highly Correlated Pairs:")
        for f1, f2, corr in result.high_correlation_pairs[:10]:
            print(f"  {f1} <-> {f2}: {corr:.3f}")
        print()

    print("Factor Clusters:")
    for cluster_id, factors in result.cluster_centroids.items():
        print(f"  Cluster {cluster_id}: {', '.join(factors)}")
    print()

    if result.pca_explained_variance:
        print("PCA Explained Variance:")
        cumulative = 0
        for i, var in enumerate(result.pca_explained_variance):
            cumulative += var
            print(f"  PC{i+1}: {var:.1%} (cumulative: {cumulative:.1%})")
        print()

    # Get redundant factors
    redundant = analyzer.get_redundant_factors(result)
    if redundant:
        print(f"Potentially Redundant Factors: {', '.join(redundant)}")

    # Save results if output path provided
    if args.correlation_output:
        output_path = Path(args.correlation_output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(result.to_dict(), f, indent=2, default=str)
        print(f"\nResults saved to: {output_path}")

    # Save plot if output path provided
    if args.plot_output:
        visualizer = FactorVisualizer()
        visualizer.plot_correlation_heatmap(
            result.correlation_matrix,
            title="Factor Correlation Matrix",
            output_path=Path(args.plot_output),
        )
        print(f"Heatmap saved to: {args.plot_output}")


def _run_options_flow_command(args: argparse.Namespace) -> None:
    """Run options flow scraper command."""
    from .scrapers.options_flow import OptionsFlowConfig, OptionsFlowScraper

    tickers = []
    if args.ticker:
        tickers = [args.ticker.upper()]
    elif args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    elif args.tickers_file:
        path = Path(args.tickers_file)
        if path.exists():
            tickers = [t.strip().upper() for t in path.read_text().splitlines() if t.strip()]

    if not tickers:
        raise SystemExit("Error: Must provide --ticker, --tickers, or --tickers-file")

    LOGGER.info("Fetching options flow for %d tickers", len(tickers))

    # Configure scraper
    config = OptionsFlowConfig(
        volume_multiplier=args.volume_multiplier,
        min_dte=args.min_dte,
        max_dte=args.max_dte,
    )

    scraper = OptionsFlowScraper(config)

    # Progress callback
    def progress(current, total, ticker):
        print(f"  [{current}/{total}] {ticker}", end="\r")

    # Fetch data
    options_data = scraper.fetch_batch(tickers, progress_callback=progress)
    print()  # Clear progress line

    if not options_data:
        LOGGER.warning("No options data found")
        return

    # Print summary
    print("\nOptions Flow Summary")
    print("=" * 70)
    print(f"Tickers fetched: {len(options_data)}/{len(tickers)}")
    print()

    # Get unusual activity
    unusual = scraper.get_unusual_activity(options_data, min_volume_spike=args.volume_multiplier)
    if not unusual.empty:
        print(f"Unusual Activity ({len(unusual)} tickers):")
        print("-" * 70)
        for _, row in unusual.head(10).iterrows():
            signal = row['signal']
            signal_color = "BULL" if signal == "BULLISH" else ("BEAR" if signal == "BEARISH" else "NEUT")
            print(f"  {row['ticker']:6} | Volume Spike: {row['volume_spike']:.1f}x | "
                  f"P/C Ratio: {row['put_call_ratio']:.2f} | Sentiment: {row['sentiment_score']:.0f} | {signal_color}")
        print()

    # Sentiment distribution
    df = scraper.to_dataframe(options_data)
    bullish = (df["sentiment_score"] > 60).sum()
    bearish = (df["sentiment_score"] < 40).sum()
    neutral = len(df) - bullish - bearish

    print("Sentiment Distribution:")
    print(f"  Bullish (>60): {bullish} ({bullish/len(df)*100:.1f}%)")
    print(f"  Neutral (40-60): {neutral} ({neutral/len(df)*100:.1f}%)")
    print(f"  Bearish (<40): {bearish} ({bearish/len(df)*100:.1f}%)")

    # Save output
    if args.options_output:
        output_path = Path(args.options_output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_path.suffix == ".csv":
            df.to_csv(output_path, index=False)
        else:
            with open(output_path, "w") as f:
                json.dump([d.to_dict() for d in options_data.values()], f, indent=2)

        print(f"\nData saved to: {output_path}")


def _run_short_interest_command(args: argparse.Namespace) -> None:
    """Run short interest tracker command."""
    from .scrapers.short_interest import ShortInterestConfig, ShortInterestTracker

    tickers = []
    if args.ticker:
        tickers = [args.ticker.upper()]
    elif args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    elif args.tickers_file:
        path = Path(args.tickers_file)
        if path.exists():
            tickers = [t.strip().upper() for t in path.read_text().splitlines() if t.strip()]

    if not tickers:
        raise SystemExit("Error: Must provide --ticker, --tickers, or --tickers-file")

    LOGGER.info("Fetching short interest for %d tickers", len(tickers))

    # Configure tracker
    config = ShortInterestConfig(
        high_short_interest_pct=args.high_short_pct,
    )

    tracker = ShortInterestTracker(config)

    # Progress callback
    def progress(current, total, ticker):
        print(f"  [{current}/{total}] {ticker}", end="\r")

    # Fetch data
    short_data = tracker.fetch_batch(tickers, progress_callback=progress)
    print()  # Clear progress line

    if not short_data:
        LOGGER.warning("No short interest data found")
        return

    # Print summary
    print("\nShort Interest Summary")
    print("=" * 70)
    print(f"Tickers fetched: {len(short_data)}/{len(tickers)}")
    print()

    # Get high short interest
    high_short = tracker.get_high_short_interest(short_data, min_short_pct=args.high_short_pct)
    if not high_short.empty:
        print(f"High Short Interest (>{args.high_short_pct}% of float):")
        print("-" * 70)
        for _, row in high_short.head(10).iterrows():
            print(f"  {row['ticker']:6} | SI/Float: {row['short_percent_float']:.1f}% | "
                  f"Days to Cover: {row['days_to_cover']:.1f} | Trend: {row['trend']:10} | "
                  f"Score: {row['contrarian_score']:.0f}")
        print()

    # Get squeeze candidates
    squeeze = tracker.get_squeeze_candidates(short_data, min_squeeze_score=args.squeeze_score_min)
    if not squeeze.empty:
        print(f"Squeeze Candidates (score >= {args.squeeze_score_min}):")
        print("-" * 70)
        for _, row in squeeze.head(10).iterrows():
            print(f"  {row['ticker']:6} | SI/Float: {row['short_percent_float']:.1f}% | "
                  f"Days to Cover: {row['days_to_cover']:.1f} | "
                  f"Squeeze Score: {row['squeeze_score']:.0f}")
        print()

    # Overall statistics
    df = tracker.to_dataframe(short_data)
    avg_short = df["short_percent_float"].mean()
    max_short = df["short_percent_float"].max()
    squeeze_count = (df["squeeze_score"] >= args.squeeze_score_min).sum()

    print("Statistics:")
    print(f"  Average SI/Float: {avg_short:.1f}%")
    print(f"  Max SI/Float: {max_short:.1f}%")
    print(f"  Squeeze Candidates: {squeeze_count}")

    # Save output
    if args.short_output:
        output_path = Path(args.short_output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_path.suffix == ".csv":
            df.to_csv(output_path, index=False)
        else:
            with open(output_path, "w") as f:
                json.dump([d.to_dict() for d in short_data.values()], f, indent=2)

        print(f"\nData saved to: {output_path}")


def _run_analyst_estimates_command(args: argparse.Namespace) -> None:
    """Run analyst estimates scraper command."""
    from .scrapers.analyst_estimates import AnalystEstimatesScraper

    tickers = []
    if args.ticker:
        tickers = [args.ticker.upper()]
    elif args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    elif args.tickers_file:
        path = Path(args.tickers_file)
        if path.exists():
            tickers = [t.strip().upper() for t in path.read_text().splitlines() if t.strip()]

    if not tickers:
        raise SystemExit("Error: Must provide --ticker, --tickers, or --tickers-file")

    LOGGER.info("Fetching analyst estimates for %d tickers", len(tickers))

    scraper = AnalystEstimatesScraper()

    # Progress callback
    def progress(current, total, ticker):
        print(f"  [{current}/{total}] {ticker}", end="\r")

    # Fetch data
    analyst_data = scraper.fetch_batch(tickers, progress_callback=progress)
    print()  # Clear progress line

    if not analyst_data:
        LOGGER.warning("No analyst data found")
        return

    # Print summary
    print("\nAnalyst Estimates Summary")
    print("=" * 70)
    print(f"Tickers fetched: {len(analyst_data)}/{len(tickers)}")
    print()

    # Get revision leaders
    leaders = scraper.get_revision_leaders(analyst_data, min_revision_pct=args.min_revision_pct)
    if not leaders.empty:
        print(f"EPS Revision Leaders (>{args.min_revision_pct}% revision):")
        print("-" * 70)
        for _, row in leaders.head(10).iterrows():
            r7 = f"{row['revision_7d']:.1f}%" if row['revision_7d'] else "N/A"
            r30 = f"{row['revision_30d']:.1f}%" if row['revision_30d'] else "N/A"
            print(f"  {row['ticker']:6} | 7d: {r7:>8} | 30d: {r30:>8} | Score: {row['revision_score']:.0f}")
        print()

    # Get highly rated
    rated = scraper.get_highly_rated(analyst_data, min_rating_score=70.0, min_analysts=args.min_analysts)
    if not rated.empty:
        print(f"Highly Rated (score >= 70, {args.min_analysts}+ analysts):")
        print("-" * 70)
        for _, row in rated.head(10).iterrows():
            upside = f"{row['upside_pct']:.1f}%" if row['upside_pct'] else "N/A"
            print(f"  {row['ticker']:6} | Rating: {row['rating_score']:.0f} | "
                  f"Buy: {row['strong_buy']+row['buy']} | Hold: {row['hold']} | "
                  f"Upside: {upside:>8}")
        print()

    # Overall statistics
    df = scraper.to_dataframe(analyst_data)
    avg_rating = df["rating_score"].mean()
    avg_upside = df["upside_to_target"].dropna().mean()

    print("Statistics:")
    print(f"  Average Rating Score: {avg_rating:.1f}")
    print(f"  Average Upside to Target: {avg_upside:.1f}%" if not pd.isna(avg_upside) else "  Average Upside: N/A")

    # Save output
    if args.analyst_output:
        output_path = Path(args.analyst_output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_path.suffix == ".csv":
            df.to_csv(output_path, index=False)
        else:
            with open(output_path, "w") as f:
                json.dump([d.to_dict() for d in analyst_data.values()], f, indent=2)

        print(f"\nData saved to: {output_path}")


def _run_institutional_command(args: argparse.Namespace) -> None:
    """Run institutional ownership scraper command."""
    from .scrapers.institutional_ownership import (
        InstitutionalOwnershipScraper,
        InstitutionalConfig,
    )

    tickers = []
    if args.ticker:
        tickers = [args.ticker.upper()]
    elif args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    elif args.tickers_file:
        path = Path(args.tickers_file)
        if path.exists():
            tickers = [t.strip().upper() for t in path.read_text().splitlines() if t.strip()]

    if not tickers:
        raise SystemExit("Error: Must provide --ticker, --tickers, or --tickers-file")

    LOGGER.info("Fetching institutional ownership for %d tickers", len(tickers))

    config = InstitutionalConfig(min_holders=args.min_holders)
    scraper = InstitutionalOwnershipScraper(config)

    # Fetch data
    inst_data = scraper.fetch_batch(tickers, show_progress=True)

    if not inst_data:
        LOGGER.warning("No institutional data found")
        return

    # Print summary
    print("\nInstitutional Ownership Summary")
    print("=" * 70)
    print(f"Tickers fetched: {len(inst_data)}/{len(tickers)}")
    print()

    # Get top picks
    top_picks = scraper.get_top_institutional_picks(inst_data, top_n=15)
    if not top_picks.empty:
        print("Top Institutional Picks (by momentum score):")
        print("-" * 70)
        for _, row in top_picks.head(10).iterrows():
            print(f"  {row['ticker']:6} | Holders: {row['num_holders']:3} | "
                  f"New Pos: {row['new_positions']:2} | "
                  f"Increased: {row['increased']:2} | "
                  f"Score: {row['combined_score']:.0f}")
        print()

    # Detect accumulation/distribution
    accumulating = scraper.detect_accumulation(inst_data)
    distributing = scraper.detect_distribution(inst_data)

    if accumulating:
        print(f"Accumulation Detected ({len(accumulating)} stocks):")
        print(f"  {', '.join(accumulating[:10])}")
        print()

    if distributing:
        print(f"Distribution Detected ({len(distributing)} stocks):")
        print(f"  {', '.join(distributing[:10])}")
        print()

    # Save output
    if args.institutional_output:
        from .scrapers.institutional_ownership import create_institutional_features
        output_path = Path(args.institutional_output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df = create_institutional_features(inst_data)
        if output_path.suffix == ".csv":
            df.to_csv(output_path)
        else:
            df.to_json(output_path, orient="records", indent=2)

        print(f"Data saved to: {output_path}")


def _run_macro_command(args: argparse.Namespace) -> None:
    """Run macro indicators command."""
    from .macro_indicators import MacroIndicators, get_macro_summary

    print("\nFetching macro indicators...")

    summary = get_macro_summary()

    print("\nMacro Environment Summary")
    print("=" * 60)
    print(f"Date: {summary['date']}")
    print(f"Regime: {summary['regime'].upper()}")
    print(f"Risk Score: {summary['risk_score']:.0f}/100")
    print(f"Recession Probability: {summary['recession_probability']:.0f}%")
    print()

    print("Yield Curve:")
    yc = summary['yield_curve']
    print(f"  State: {yc['state'].upper()}")
    print(f"  2Y-10Y Spread: {yc['spread_2y10y']:.2f}%" if yc['spread_2y10y'] else "  2Y-10Y Spread: N/A")
    print(f"  10Y Yield: {yc['10y_yield']:.2f}%" if yc['10y_yield'] else "  10Y Yield: N/A")
    print()

    print("Volatility:")
    vol = summary['volatility']
    print(f"  VIX: {vol['vix']:.1f}" if vol['vix'] else "  VIX: N/A")
    print(f"  Percentile: {vol['percentile']:.0f}%" if vol['percentile'] else "  Percentile: N/A")
    print(f"  Term Structure: {vol['term_structure'].upper()}")
    print()

    print("Credit:")
    credit = summary['credit']
    print(f"  HY Spread: {credit['hy_spread']:.2f}%" if credit['hy_spread'] else "  HY Spread: N/A")
    print(f"  Stress: {'YES' if credit['stress'] else 'No'}")
    print()

    print("Dollar:")
    print(f"  Trend: {summary['dollar']['trend'].upper()}")
    print()

    print(f"Position Size Multiplier: {summary['position_size_mult']:.2f}")
    print()

    if summary['sector_tilts']:
        print("Sector Tilts (based on macro):")
        for sector, tilt in sorted(summary['sector_tilts'].items(), key=lambda x: -x[1]):
            direction = "+" if tilt >= 0 else ""
            print(f"  {sector:25} {direction}{tilt:.1f}")

    # Save output
    if args.macro_output:
        output_path = Path(args.macro_output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)

        print(f"\nData saved to: {output_path}")


def _run_sentiment_command(args: argparse.Namespace) -> None:
    """Run news sentiment analyzer command."""
    from .news_sentiment import NewsSentimentScraper, SentimentConfig, create_sentiment_features

    tickers = []
    if args.ticker:
        tickers = [args.ticker.upper()]
    elif args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    elif args.tickers_file:
        path = Path(args.tickers_file)
        if path.exists():
            tickers = [t.strip().upper() for t in path.read_text().splitlines() if t.strip()]

    if not tickers:
        raise SystemExit("Error: Must provide --ticker, --tickers, or --tickers-file")

    LOGGER.info("Fetching news sentiment for %d tickers", len(tickers))

    config = SentimentConfig(use_llm=args.use_llm_sentiment)
    scraper = NewsSentimentScraper(config)

    # Fetch data
    sentiment_data = scraper.fetch_batch(tickers, show_progress=True)

    if not sentiment_data:
        LOGGER.warning("No sentiment data found")
        return

    # Print summary
    print("\nNews Sentiment Summary")
    print("=" * 70)
    print(f"Tickers analyzed: {len(sentiment_data)}/{len(tickers)}")
    print(f"Using LLM: {args.use_llm_sentiment}")
    print()

    # Get top sentiment
    top_sent = scraper.get_top_sentiment(sentiment_data, top_n=15)
    if not top_sent.empty:
        print("Most Positive Sentiment:")
        print("-" * 70)
        for _, row in top_sent.head(10).iterrows():
            label = "POS" if row['avg_sentiment'] > 0.1 else ("NEG" if row['avg_sentiment'] < -0.1 else "NEU")
            print(f"  {row['ticker']:6} | Sent: {row['avg_sentiment']:+.2f} ({label}) | "
                  f"News: {row['news_count']:2} | "
                  f"Pos%: {row['positive_ratio']*100:.0f}% | "
                  f"Score: {row['combined_score']:.0f}")
        print()

    # Get worst sentiment
    worst_sent = scraper.get_worst_sentiment(sentiment_data, top_n=10)
    if not worst_sent.empty:
        print("Most Negative Sentiment:")
        print("-" * 70)
        for _, row in worst_sent.head(5).iterrows():
            print(f"  {row['ticker']:6} | Sent: {row['avg_sentiment']:+.2f} | "
                  f"News: {row['news_count']:2} | "
                  f"Neg%: {row['negative_ratio']*100:.0f}%")
        print()

    # Save output
    if args.sentiment_output:
        output_path = Path(args.sentiment_output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df = create_sentiment_features(sentiment_data)
        if output_path.suffix == ".csv":
            df.to_csv(output_path)
        else:
            df.to_json(output_path, orient="records", indent=2)

        print(f"Data saved to: {output_path}")


def _run_stacking_command(args: argparse.Namespace) -> None:
    """Run stacking ensemble command."""
    from .stacking_ensemble import StackingEnsemble, create_default_ensemble

    action = args.stacking_action
    ensemble_path = Path(args.ensemble_path)

    if action == "weights":
        # Show default configuration
        ensemble = create_default_ensemble()
        print("\nStacking Ensemble Configuration")
        print("=" * 60)
        print(f"Meta-model type: {ensemble.config.meta_model_type}")
        print(f"Use CV predictions: {ensemble.config.use_cv_predictions}")
        print(f"N folds: {ensemble.config.n_folds}")
        print(f"Dynamic weighting: {ensemble.config.dynamic_weighting}")
        print()

        print("Base Models:")
        print("-" * 60)
        for model_cfg in ensemble.config.base_models:
            print(f"  {model_cfg.name}")
            print(f"    Type: {model_cfg.model_type}")
            print(f"    Initial Weight: {model_cfg.weight:.2f}")
            if model_cfg.params:
                print(f"    Params: {model_cfg.params}")
            print()

    elif action == "train":
        # Load training data
        data_dir = Path(args.out)
        history_path = data_dir / "history" / "finviz_fundamentals_history.parquet"
        prices_path = data_dir / "history" / "prices.parquet"

        if not history_path.exists():
            raise SystemExit(f"Error: History file not found: {history_path}")

        print("Loading training data...")
        features_df = pd.read_parquet(history_path)
        prices_df = pd.read_parquet(prices_path) if prices_path.exists() else None

        # Create target variable (forward returns)
        # This is a simplified example - would need proper implementation
        print("Preparing features and target...")

        # For now, just show what would happen
        print(f"Features shape: {features_df.shape}")
        print("\nNote: Full training requires proper target calculation.")
        print("Use the 'train' command with ML pipeline for complete training.")

    elif action == "evaluate":
        if not ensemble_path.exists():
            raise SystemExit(f"Error: Ensemble not found at {ensemble_path}")

        ensemble = StackingEnsemble.load(str(ensemble_path))
        print(f"Loaded ensemble from {ensemble_path}")
        print(f"Is fitted: {ensemble.is_fitted}")
        print(f"Base models: {list(ensemble.base_models.keys())}")

        if ensemble.dynamic_weights:
            print("\nDynamic Weights:")
            for name, weight in ensemble.dynamic_weights.items():
                print(f"  {name}: {weight:.3f}")

        if ensemble.performance_history:
            print(f"\nPerformance history: {len(ensemble.performance_history)} records")

    elif action == "predict":
        raise SystemExit("Error: predict action requires input data. Use programmatic API.")


def _run_factor_analytics_command(args: argparse.Namespace) -> None:
    """Run factor analytics command."""
    from .factor_analytics import PCAAnalyzer, RollingCorrelationTracker, run_factor_analytics

    data_dir = Path(args.out)
    latest_path = data_dir / "latest" / "finviz_fundamentals.parquet"

    if not latest_path.exists():
        raise SystemExit(f"Error: Data not found: {latest_path}")

    print("Loading data...")
    data = pd.read_parquet(latest_path)

    # Define factor columns
    factor_cols = [col for col in data.columns if any(
        x in col.lower() for x in ['score', 'quality', 'value', 'momentum', 'growth', 'risk']
    )]

    if not factor_cols:
        raise SystemExit("Error: No factor columns found in data")

    print(f"Found {len(factor_cols)} factor columns")

    action = args.analytics_action

    if action in ["pca", "all"]:
        print("\n" + "=" * 60)
        print("PCA ANALYSIS")
        print("=" * 60)

        analyzer = PCAAnalyzer(variance_threshold=args.variance_threshold)
        try:
            result = analyzer.fit_transform(data, factor_cols)

            print(f"\nExplained Variance:")
            for i, (var, cum) in enumerate(zip(result.explained_variance_ratio, result.cumulative_variance)):
                print(f"  PC{i+1}: {var:.1%} (cumulative: {cum:.1%})")

            print(f"\nRecommended components: {result.recommended_components}")

            print(f"\nTop Feature Importance:")
            sorted_imp = sorted(result.feature_importance.items(), key=lambda x: -x[1])
            for feat, imp in sorted_imp[:10]:
                print(f"  {feat}: {imp:.3f}")

            redundant = analyzer.get_redundant_factors(result)
            if redundant:
                print(f"\nRedundant factors (low importance): {', '.join(redundant)}")

        except Exception as e:
            print(f"PCA failed: {e}")

    if action in ["correlation", "all"]:
        print("\n" + "=" * 60)
        print("CORRELATION ANALYSIS")
        print("=" * 60)

        tracker = RollingCorrelationTracker()
        try:
            result = tracker.calculate_rolling_correlations(data, factor_cols[:10])

            print(f"\nFactor Clusters:")
            for factor, cluster in result.cluster_assignments.items():
                print(f"  Cluster {cluster}: {factor}")

            if result.unstable_pairs:
                print(f"\nUnstable Pairs (high correlation volatility):")
                for f1, f2 in result.unstable_pairs[:5]:
                    print(f"  {f1} <-> {f2}")

        except Exception as e:
            print(f"Correlation analysis failed: {e}")

    print("\nAnalysis complete.")


def _run_risk_check_command(args: argparse.Namespace) -> None:
    """Run risk check command."""
    from .risk_engine import RiskEngine, PositionLimits

    print("\nRisk Engine Configuration")
    print("=" * 60)

    limits = PositionLimits()
    print(f"Max Position Size: {limits.max_position_pct:.0%}")
    print(f"Max Sector Exposure: {limits.max_sector_pct:.0%}")
    print(f"Max Correlated Exposure: {limits.max_correlated_pct:.0%}")
    print(f"Correlation Threshold: {limits.correlation_threshold}")
    print(f"Max Positions: {limits.max_positions}")
    print(f"Min Positions: {limits.min_positions}")


def _run_paper_trade_command(args: argparse.Namespace) -> None:
    """Run paper trading command."""
    from .broker_integration import PaperTradingBroker, Order, OrderSide, OrderType, TradeJournal

    action = args.trade_action
    broker = PaperTradingBroker(initial_cash=args.initial_cash)
    broker.connect()

    journal_path = Path(args.out) / "trade_journal.json"
    journal = TradeJournal(str(journal_path))

    if action == "status":
        account = broker.get_account_info()
        positions = broker.get_positions()

        print("\nPaper Trading Account Status")
        print("=" * 60)
        print(f"Account: {account.account_id}")
        print(f"Portfolio Value: ${account.portfolio_value:,.2f}")
        print(f"Cash: ${account.cash:,.2f}")
        print(f"Positions: {account.positions_count}")

        if positions:
            print("\nPositions:")
            for pos in positions:
                print(f"  {pos.ticker}: {pos.quantity} shares @ ${pos.current_price:.2f}")
                print(f"    P&L: ${pos.unrealized_pnl:+,.2f} ({pos.unrealized_pnl_pct:+.1f}%)")

    elif action == "buy":
        if not args.ticker:
            raise SystemExit("Error: --ticker required for buy")
        if not args.shares:
            raise SystemExit("Error: --shares required for buy")

        order = Order(
            id="",
            ticker=args.ticker.upper(),
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=args.shares,
        )
        result = broker.submit_order(order)
        print(f"Order {result.status.value}: {result.side.value} {result.quantity} {result.ticker}")
        if result.filled_avg_price:
            print(f"Filled at ${result.filled_avg_price:.2f}")

    elif action == "sell":
        if not args.ticker:
            raise SystemExit("Error: --ticker required for sell")
        if not args.shares:
            raise SystemExit("Error: --shares required for sell")

        order = Order(
            id="",
            ticker=args.ticker.upper(),
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=args.shares,
        )
        result = broker.submit_order(order)
        print(f"Order {result.status.value}: {result.side.value} {result.quantity} {result.ticker}")

    elif action == "history":
        summary = journal.get_performance_summary()
        print("\nTrading Performance Summary")
        print("=" * 60)
        print(f"Total Trades: {summary['total_trades']}")
        print(f"Closed Trades: {summary['closed_trades']}")
        print(f"Win Rate: {summary.get('win_rate', 0):.1f}%")
        print(f"Total P&L: ${summary.get('total_pnl', 0):,.2f}")


def _run_reddit_command(args: argparse.Namespace) -> None:
    """Run Reddit sentiment command."""
    from .social_sentiment import RedditScraper, SocialSentimentConfig

    subreddits = [s.strip() for s in args.subreddits.split(",")]
    config = SocialSentimentConfig(subreddits=subreddits)
    scraper = RedditScraper(config)

    print(f"\nFetching Reddit sentiment from: {', '.join(subreddits)}")
    print("=" * 60)

    df = scraper.get_trending_tickers(limit=20)

    if df.empty:
        print("No trending tickers found")
        return

    print(f"\nTop Trending Tickers:")
    print("-" * 60)
    for _, row in df.head(15).iterrows():
        sentiment_label = "BULLISH" if row['bullish'] else ("BEARISH" if row['bearish'] else "NEUTRAL")
        print(f"  {row['ticker']:6} | Mentions: {row['mentions']:3} | "
              f"Sentiment: {row['sentiment']:+.2f} ({sentiment_label}) | "
              f"Score: {row['total_score']:,}")

    if args.reddit_output:
        output_path = Path(args.reddit_output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.suffix == ".csv":
            df.to_csv(output_path, index=False)
        else:
            df.to_json(output_path, orient="records", indent=2)
        print(f"\nData saved to: {output_path}")


def _run_etf_flows_command(args: argparse.Namespace) -> None:
    """Run ETF flows command."""
    from .etf_flows import ETFFlowTracker, get_etf_flow_summary

    print("\nFetching ETF flow data...")
    print("=" * 60)

    summary = get_etf_flow_summary()

    print(f"\nSector Rotation Signal")
    print("-" * 40)
    rotation = summary['rotation_signal']
    print(f"Type: {rotation['type'].upper()}")
    print(f"Strength: {rotation['strength']:.0f}/100")
    print(f"Money flowing INTO: {', '.join(rotation['flowing_into'])}")
    print(f"Money flowing OUT: {', '.join(rotation['flowing_out'])}")

    print(f"\nSector Rankings ({args.flow_period}):")
    print("-" * 60)
    for i, sector in enumerate(summary['sector_ranking'][:10], 1):
        print(f"  {i:2}. {sector['sector']:25} | "
              f"Price: {sector['price_change']:+.1f}% | "
              f"Momentum: {sector['momentum_score']:.0f}")

    print(f"\nFactor Scores:")
    for factor, score in summary['factor_scores'].items():
        print(f"  {factor}: {score:+.1f}%")

    print(f"\nRecommended Sector Tilts:")
    for sector, tilt in sorted(summary['recommended_tilts'].items(), key=lambda x: -x[1]):
        direction = "+" if tilt >= 0 else ""
        print(f"  {sector:25} {direction}{tilt:.2f}")


def _run_sec_filings_command(args: argparse.Namespace) -> None:
    """Run SEC filings command."""
    from .sec_filings import SECFilingsTracker, get_sec_filings_summary

    tickers = []
    if args.ticker:
        tickers = [args.ticker.upper()]
    elif args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    if not tickers:
        raise SystemExit("Error: Must provide --ticker or --tickers")

    print(f"\nFetching SEC filings for {len(tickers)} tickers...")
    print("=" * 60)

    tracker = SECFilingsTracker()

    for ticker in tickers:
        print(f"\n{ticker}:")
        print("-" * 40)

        filings = tracker.get_recent_filings(ticker, days=args.filing_days)

        if not filings:
            print("  No recent filings found")
            continue

        for filing in filings[:5]:
            print(f"  [{filing.filed_date}] {filing.filing_type}: {filing.description}")


def _run_dashboard_command(args: argparse.Namespace) -> None:
    """Run dashboard command."""
    print("\nStarting Finviz Scraper Dashboard...")
    print("=" * 60)
    print("\nTo run the dashboard, use:")
    print("  streamlit run -m finviz_weekly.dashboard")
    print("\nOr run directly:")
    print("  python -m streamlit run src/finviz_weekly/dashboard.py")


def _run_regime_status_command(args: argparse.Namespace) -> None:
    """Run regime status command."""
    import yfinance as yf
    from .regime_ensemble import RegimeAwareEnsemble

    print("\nFetching market data...")

    # Fetch SPY and VIX data
    spy = yf.download("SPY", period="1y", progress=False)
    vix = yf.download("^VIX", period="1y", progress=False)

    if spy.empty:
        raise SystemExit("Error: Could not fetch SPY data")

    spy_prices = spy["Adj Close"]
    vix_prices = vix["Adj Close"] if not vix.empty else None

    # Detect regime
    ensemble = RegimeAwareEnsemble()
    regime = ensemble.detect_regime(spy_prices, vix_prices)

    # Get summary
    summary = ensemble.get_regime_summary()

    # Print results
    print("\nMarket Regime Status")
    print("=" * 60)
    print(f"Trend Regime:      {regime.trend_regime.upper()}")
    print(f"Volatility Regime: {regime.volatility_regime.upper()}")
    print()

    print("Probabilities:")
    print(f"  Bull:     {regime.bull_probability:.1%}")
    print(f"  Bear:     {regime.bear_probability:.1%}")
    print(f"  High Vol: {regime.high_vol_probability:.1%}")
    print()

    print("Market Metrics:")
    print(f"  SPY 20-day return: {regime.spy_return_20d:+.1f}%")
    print(f"  SPY 50-day return: {regime.spy_return_50d:+.1f}%")
    print(f"  VIX Level: {regime.vix_level:.1f}")
    print(f"  VIX Percentile: {regime.vix_percentile:.0f}%")
    print()

    weights = ensemble.get_regime_weights()
    print("Recommended Settings:")
    print(f"  Position Size Mult: {weights.position_size_mult:.2f}")
    print()

    print("Factor Weights:")
    print(f"  Momentum: {weights.momentum_weight:.2f}")
    print(f"  Value:    {weights.value_weight:.2f}")
    print(f"  Quality:  {weights.quality_weight:.2f}")
    print(f"  Growth:   {weights.growth_weight:.2f}")
    print()

    print("Horizon Weights:")
    for horizon, weight in weights.horizon_weights.items():
        print(f"  {horizon:>3}d: {weight:.2f}")

    if weights.sector_tilts:
        print()
        print("Sector Tilts:")
        for sector, tilt in sorted(weights.sector_tilts.items(), key=lambda x: -x[1]):
            direction = "+" if tilt >= 0 else ""
            print(f"  {sector:25} {direction}{tilt:.2f}")


def _run_custom_factors_command(args: argparse.Namespace) -> None:
    """Run custom factors command."""
    from .custom_factors import CustomFactorBuilder, get_factor_templates

    builder = CustomFactorBuilder()
    action = args.factors_action

    if action == "list":
        factors = builder.list_factors()
        if not factors:
            print("No custom factors defined.")
            print("\nAvailable templates:")
            for name, template in get_factor_templates().items():
                print(f"  {name}: {template['description']}")
            return

        print(f"\nCustom Factors ({len(factors)} total):")
        print("-" * 60)
        for f in factors:
            print(f"  {f.name}")
            print(f"    Expression: {f.expression}")
            print(f"    Category: {f.category} | Higher is better: {f.higher_is_better}")
            if f.description:
                print(f"    Description: {f.description}")
            print()

    elif action == "create":
        if not args.factor_name:
            raise SystemExit("Error: --factor-name required for create action")
        if not args.factor_expression:
            raise SystemExit("Error: --factor-expression required for create action")

        factor = builder.create_factor(
            name=args.factor_name,
            expression=args.factor_expression,
            description=args.factor_description or "",
            category=args.factor_category,
            higher_is_better=args.higher_is_better,
        )

        print(f"Created factor: {factor.name}")
        print(f"  Expression: {factor.expression}")

    elif action == "test":
        if not args.factor_name:
            raise SystemExit("Error: --factor-name required for test action")

        data_dir = Path(args.out)
        latest_path = data_dir / "latest" / "finviz_fundamentals.parquet"

        if not latest_path.exists():
            raise SystemExit(f"Error: Latest data not found: {latest_path}")

        import pandas as pd
        data = pd.read_parquet(latest_path)

        result = builder.test_factor(args.factor_name, data)

        print(f"\nFactor Test: {result.factor_name}")
        print("=" * 40)
        print(f"Success: {result.success}")

        if result.success:
            print(f"Sample size: {result.sample_size}")
            print(f"Mean: {result.mean_value:.4f}")
            print(f"Std: {result.std_value:.4f}")
            print(f"Min: {result.min_value:.4f}")
            print(f"Max: {result.max_value:.4f}")
            print(f"Null %: {result.null_pct:.1f}%")

            if result.sample_values:
                print("\nSample Values:")
                for row in result.sample_values[:5]:
                    print(f"  {row.get('ticker', 'N/A')}: {row.get('factor_value', 'N/A'):.4f}")
        else:
            print(f"Error: {result.error_message}")

    elif action == "delete":
        if not args.factor_name:
            raise SystemExit("Error: --factor-name required for delete action")

        builder.delete_factor(args.factor_name)
        print(f"Deleted factor: {args.factor_name}")

    elif action == "compute":
        data_dir = Path(args.out)
        latest_path = data_dir / "latest" / "finviz_fundamentals.parquet"

        if not latest_path.exists():
            raise SystemExit(f"Error: Latest data not found: {latest_path}")

        import pandas as pd
        data = pd.read_parquet(latest_path)

        result = builder.compute_all_factors(data)

        # Show computed factors
        custom_cols = [c for c in result.columns if c.startswith("custom_")]
        if custom_cols:
            print(f"Computed {len(custom_cols)} custom factors")
            print("\nSample (first 10 rows):")
            print(result[["ticker"] + custom_cols].head(10).to_string())
        else:
            print("No custom factors to compute")


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    if args.command == "screen":
        run_screening(
            out_dir=args.out,
            top_n=int(args.top),
            min_market_cap=float(args.min_market_cap),
            min_price=float(args.min_price),
            candidates_max=int(args.candidates_max),
            use_learned=bool(args.use_learned),
            learned_min_unique_dates=int(args.learned_min_unique_dates),
            learned_min_forward_rows=int(args.learned_min_forward_rows),
            watchlist_file=args.watchlist_file,
            normalize_by=args.normalize_by,
        )
        return

    if args.command == "train":
        train_weights(
            out_dir=args.out,
            min_rows_per_group=int(args.min_rows_per_group),
            group_col=str(args.group_col),
        )
        return

    if args.command == "report":
        write_report_from_latest(out_dir=args.out)
        return

    if args.command == "debate":
        run_debate(
            out_dir=args.out,
            input_mode=args.input,
            tickers=args.tickers,
            max_tickers=args.max_tickers,
            research=bool(args.research),
            recency_days=args.recency_days,
            max_queries_per_ticker=args.max_queries_per_ticker,
            max_results_per_query=args.max_results_per_query,
            evidence_max=args.evidence_max,
            cache_days=args.cache_days,
            timeout_seconds=args.timeout_seconds,
            as_of=args.as_of,
            provider=args.provider,
            model=args.model,
            verbose=bool(args.verbose),
        )
        return

    if args.command == "insider":
        _run_insider_command(args)
        return

    if args.command == "earnings":
        _run_earnings_command(args)
        return

    if args.command == "financials":
        _run_financials_command(args)
        return

    if args.command == "model-registry":
        _run_model_registry_command(args)
        return

    if args.command == "drift-check":
        _run_drift_check_command(args)
        return

    if args.command == "retrain":
        _run_retrain_command(args)
        return

    if args.command == "correlation":
        _run_correlation_command(args)
        return

    if args.command == "custom-factors":
        _run_custom_factors_command(args)
        return

    if args.command == "options-flow":
        _run_options_flow_command(args)
        return

    if args.command == "short-interest":
        _run_short_interest_command(args)
        return

    if args.command == "analyst-estimates":
        _run_analyst_estimates_command(args)
        return

    if args.command == "regime-status":
        _run_regime_status_command(args)
        return

    if args.command == "institutional":
        _run_institutional_command(args)
        return

    if args.command == "macro":
        _run_macro_command(args)
        return

    if args.command == "sentiment":
        _run_sentiment_command(args)
        return

    if args.command == "stacking":
        _run_stacking_command(args)
        return

    if args.command == "factor-analytics":
        _run_factor_analytics_command(args)
        return

    if args.command == "risk-check":
        _run_risk_check_command(args)
        return

    if args.command == "paper-trade":
        _run_paper_trade_command(args)
        return

    if args.command == "reddit":
        _run_reddit_command(args)
        return

    if args.command == "etf-flows":
        _run_etf_flows_command(args)
        return

    if args.command == "sec-filings":
        _run_sec_filings_command(args)
        return

    if args.command == "dashboard":
        _run_dashboard_command(args)
        return

    # run
    if not args.mode:
        raise SystemExit("--mode is required for the 'run' command")

    tickers = _load_tickers(args)

    config = env_config(
        mode=args.mode,
        tickers=tickers,
        industry_limit=args.industry_limit,
        ticker_limit=args.ticker_limit,
        out_dir=args.out,
        formats=[f.strip() for f in args.formats.split(",") if f.strip()],
        log_level=args.log_level,
        rate_per_sec=args.rate_per_sec,
        page_sleep_min=args.page_sleep_min,
        page_sleep_max=args.page_sleep_max,
        resume=args.resume,
        concurrency=args.concurrency,
        checkpoint_every=args.checkpoint_every,
        latest_only_ok=bool(args.latest_only_ok),
        latest_include_as_of_date=bool(args.latest_include_as_of_date),
        include_insider=bool(args.include_insider),
        include_earnings=bool(args.include_earnings),
        include_financials=bool(args.include_financials),
        enhanced_scoring=bool(args.enhanced_scoring),
    )

    session = create_session(config.http)
    execute(session, config)


if __name__ == "__main__":
    main()
