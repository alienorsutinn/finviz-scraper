import pandas as pd

from finviz_weekly import pipeline
from finviz_weekly.config import env_config
from finviz_weekly.storage import HISTORY_DIR


class DummySession:
    pass


def test_execute_appends_history_without_duplicates(monkeypatch, tmp_path):
    # deterministic scrape result
    def fake_scrape(ticker: str):
        return {"Ticker": ticker, "Market Cap": "1.5M", "Change": "7.25%", "Range": "1 - 2"}

    monkeypatch.setattr(pipeline, "scrape_fundamentals", fake_scrape)

    config = env_config(
        mode="tickers",
        tickers=["ABC"],
        out_dir=str(tmp_path),
        rate_per_sec=100.0,
        page_sleep_min=0,
        page_sleep_max=0,
    )
    # first run
    df1 = pipeline.execute(DummySession(), config)
    assert not df1.empty

    # second run should not duplicate history for same date
    df2 = pipeline.execute(DummySession(), config)
    history_path = tmp_path / HISTORY_DIR / "finviz_fundamentals_history.parquet"
    history = pd.read_parquet(history_path)
    assert len(history) == 1
    assert history.iloc[0]["ticker"] == "ABC"
