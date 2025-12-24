# Finviz Weekly Scraper

This repository converts the original `finvizdataipynb.ipynb` notebook logic into a reusable Python package and GitHub Actions workflow that scrapes Finviz fundamentals on a weekly schedule.

## Features
- Discover industries and tickers from the Finviz screener (no Selenium).
- Scrape fundamentals for each ticker using `finvizfinance` with polite rate limiting, retries, and random user agents.
- Store outputs as Parquet and CSV under dated run folders plus rolling latest and append-only history.
- Weekly GitHub Actions workflow (Monday 01:00 UTC) with optional commit of generated data when `PERSIST_RESULTS=true`.

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

## Usage

Run the pipeline locally (example tickers mode):

```bash
python -m finviz_weekly run --mode tickers --tickers "BLK,FTNT" --out data
```

Discover the universe, limited to the first 500 tickers, at a gentle rate:

```bash
python -m finviz_weekly run --mode universe --ticker-limit 500 --rate-per-sec 0.5 --out data
```

Key CLI options:
- `--mode [universe|tickers]`
- `--tickers "AAPL,MSFT"` or `--tickers-file config/tickers.txt`
- `--industry-limit N` and `--ticker-limit N`
- `--rate-per-sec`, `--page-sleep-min`, `--page-sleep-max`
- `--out data` and `--formats parquet,csv`
- `--log-level INFO`

Outputs are written to:
- `data/runs/YYYY-MM-DD/finviz_fundamentals.parquet`
- `data/runs/YYYY-MM-DD/finviz_fundamentals.csv`
- `data/runs/YYYY-MM-DD/meta.json`
- `data/latest/finviz_fundamentals.parquet`
- `data/runs/YYYY-MM-DD/finviz_scored.parquet` (canonical scored snapshot)
- `data/runs/YYYY-MM-DD/finviz_scored.csv.gz` (Excel-friendly mirror of the scored snapshot)
- `data/latest/finviz_scored.parquet`
- `data/latest/finviz_scored.csv.gz`
- `data/history/finviz_fundamentals_history.parquet` (append-only with `as_of_date`)

## Weekly workflow
1. Run scraping (`python -m finviz_weekly run ...`).
2. Run screening (`python -m finviz_weekly screen --out data`). This writes `finviz_scored.parquet`/`csv.gz` plus candidates and conviction lists.
3. Run the debate layer (optional, uses mocks by default): `python -m finviz_weekly debate --out data --input candidates --max-tickers 20 --research off`.
   - To use Brave web search for real research, set `BRAVE_API_KEY` and pass `--research on`. Optional LLM calls use `OPENAI_API_KEY` (otherwise a deterministic mock is used).
   - Debate outputs: `data/debate/YYYY-MM-DD/{ticker}.json`, `{ticker}_evidence.json`, `debate_results.csv`, `debate_report.md`.

## GitHub Actions

The workflow in `.github/workflows/weekly.yml` runs every Monday at 01:00 UTC (and on manual dispatch). It scrapes up to 500 tickers at a low rate and uploads artifacts. When the environment variable `PERSIST_RESULTS` is set to `true`, the workflow will commit the `data/` directory back to the repository using the message `chore(data): weekly finviz snapshot YYYY-MM-DD`.

## Notes
- Scraping is subject to website changes; use conservative rate limits.
- Tests avoid network calls by using fixtures and monkeypatching.
- Python 3.11 is required.
