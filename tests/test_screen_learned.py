import json
from pathlib import Path

import pandas as pd

from finviz_weekly.screen import run_screening


def _write_snapshot(tmp_path: Path) -> Path:
    data_dir = tmp_path / "data"
    latest = data_dir / "latest"
    latest.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "ticker": "AAA",
            "company": "AAA Corp",
            "sector": "Technology",
            "industry": "Software",
            "price": 25.0,
            "market_cap": 800_000_000,
            "p_e": 10.0,
            "forward_p_e": 12.0,
            "p_s": 2.0,
            "p_b": 3.0,
            "p_fcf": 15.0,
            "ev_ebitda": 8.0,
            "roi": 12.0,
            "roe": 15.0,
            "roa": 8.0,
            "gross_margin": 40.0,
            "oper_margin": 20.0,
            "profit_margin": 18.0,
            "current_ratio": 2.0,
            "quick_ratio": 1.5,
            "debt_eq": 0.4,
            "lt_debt_eq": 0.3,
            "beta": 1.0,
            "sales_growth_past_5y": 10.0,
            "eps_growth_past_5y": 8.0,
            "sales_growth_qoq": 5.0,
            "eps_growth_qoq": 6.0,
            "perf_quarter": 5.0,
            "perf_half_y": 8.0,
            "perf_year": 12.0,
            "rsi_14": 40.0,
            "perf_month": -2.0,
            "perf_week": -1.0,
            "as_of_date": "2024-01-01",
        },
        {
            "ticker": "BBB",
            "company": "BBB Corp",
            "sector": "Technology",
            "industry": "Software",
            "price": 30.0,
            "market_cap": 900_000_000,
            "p_e": 15.0,
            "forward_p_e": 16.0,
            "p_s": 3.0,
            "p_b": 4.0,
            "p_fcf": 18.0,
            "ev_ebitda": 10.0,
            "roi": 10.0,
            "roe": 12.0,
            "roa": 7.0,
            "gross_margin": 35.0,
            "oper_margin": 18.0,
            "profit_margin": 15.0,
            "current_ratio": 1.8,
            "quick_ratio": 1.3,
            "debt_eq": 0.5,
            "lt_debt_eq": 0.4,
            "beta": 1.1,
            "sales_growth_past_5y": 9.0,
            "eps_growth_past_5y": 7.5,
            "sales_growth_qoq": 4.0,
            "eps_growth_qoq": 5.0,
            "perf_quarter": 4.0,
            "perf_half_y": 6.0,
            "perf_year": 9.0,
            "rsi_14": 45.0,
            "perf_month": -1.0,
            "perf_week": 0.5,
            "as_of_date": "2024-01-01",
        },
        {
            "ticker": "CCC",
            "company": "CCC Corp",
            "sector": "Technology",
            "industry": "Software",
            "price": 35.0,
            "market_cap": 1_100_000_000,
            "p_e": 20.0,
            "forward_p_e": 22.0,
            "p_s": 4.0,
            "p_b": 5.0,
            "p_fcf": 20.0,
            "ev_ebitda": 12.0,
            "roi": 8.0,
            "roe": 9.0,
            "roa": 6.0,
            "gross_margin": 32.0,
            "oper_margin": 15.0,
            "profit_margin": 12.0,
            "current_ratio": 1.6,
            "quick_ratio": 1.2,
            "debt_eq": 0.6,
            "lt_debt_eq": 0.5,
            "beta": 1.2,
            "sales_growth_past_5y": 8.0,
            "eps_growth_past_5y": 6.5,
            "sales_growth_qoq": 3.0,
            "eps_growth_qoq": 4.0,
            "perf_quarter": 3.0,
            "perf_half_y": 5.0,
            "perf_year": 8.0,
            "rsi_14": 55.0,
            "perf_month": 0.5,
            "perf_week": 1.0,
            "as_of_date": "2024-01-01",
        },
    ]
    df = pd.DataFrame(rows)
    df.to_parquet(latest / "finviz_fundamentals.parquet", index=False)
    return data_dir


def test_old_learned_schema_disables_learned(tmp_path: Path):
    data_dir = _write_snapshot(tmp_path)
    learned_payload = {
        "mode": "learned",
        "global": {"weights": {}},
        "by_group": {"Technology": {"weights": {"score_quality": 0.6}}},
        "n_unique_dates": 5,
        "n_forward_rows": 50,
    }
    (data_dir / "latest" / "learned_weights.json").write_text(json.dumps(learned_payload))

    run_screening(out_dir=str(data_dir), use_learned=True, top_n=2, candidates_max=10)

    scored = pd.read_parquet(data_dir / "latest" / "finviz_scored.parquet")
    assert scored["score_learned"].isna().all()
    assert not (data_dir / "latest" / "top2_learned.csv").exists()


def test_valid_learned_ignored_without_flag(tmp_path: Path):
    data_dir = _write_snapshot(tmp_path)
    learned_payload = {
        "version": 1,
        "mode": "learned",
        "features": ["score_quality"],
        "global": {"weights": {"score_quality": 1.0}},
        "groups": {},
        "group_col": None,
        "n_unique_dates": 20,
        "n_forward_rows": 300,
    }
    (data_dir / "latest" / "learned_weights.json").write_text(json.dumps(learned_payload))

    run_screening(out_dir=str(data_dir), top_n=2, candidates_max=10)

    scored = pd.read_parquet(data_dir / "latest" / "finviz_scored.parquet")
    assert scored["score_learned"].isna().all()
    assert not (data_dir / "latest" / "top2_learned.csv").exists()
    status = json.loads((data_dir / "latest" / "learned_status.json").read_text())
    assert "flag_off" in status.get("reasons", [])


def test_learned_gated_by_history(tmp_path: Path):
    data_dir = _write_snapshot(tmp_path)
    learned_payload = {
        "version": 1,
        "mode": "learned",
        "features": ["score_quality"],
        "global": {"weights": {"score_quality": 1.0}},
        "groups": {},
        "group_col": None,
        "n_unique_dates": 5,
        "n_forward_rows": 500,
    }
    (data_dir / "latest" / "learned_weights.json").write_text(json.dumps(learned_payload))

    run_screening(out_dir=str(data_dir), use_learned=True, top_n=2, candidates_max=10)

    status = json.loads((data_dir / "latest" / "learned_status.json").read_text())
    assert status["enabled"] is False
    assert "insufficient_unique_dates" in status.get("reasons", [])
    assert not (data_dir / "latest" / "top2_learned.csv").exists()


def test_conviction_family_counts(tmp_path: Path):
    data_dir = _write_snapshot(tmp_path)
    run_screening(out_dir=str(data_dir), top_n=2, candidates_max=10)

    conv2 = pd.read_csv(data_dir / "latest" / "conviction_2plus.csv")
    assert {"count_lists", "count_families", "families"}.issubset(conv2.columns)
    assert (conv2["count_lists"] >= conv2["count_families"]).all()
    assert (conv2["count_lists"] > conv2["count_families"]).any()


def test_watchlist_appended_and_exported(tmp_path: Path):
    data_dir = _write_snapshot(tmp_path)
    watch_file = data_dir / "watchlist.txt"
    watch_file.write_text("WLIST\n#commented\n", encoding="utf-8")

    run_screening(out_dir=str(data_dir), top_n=2, candidates_max=5, watchlist_file=str(watch_file))

    candidates = (data_dir / "latest" / "candidates.txt").read_text().splitlines()
    assert "WLIST" in candidates
    watch_csv = pd.read_csv(data_dir / "latest" / "watchlist.csv")
    assert "ticker" in watch_csv.columns
    assert "WLIST" in watch_csv["ticker"].astype(str).tolist()
