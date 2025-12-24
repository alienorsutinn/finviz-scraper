from pathlib import Path
import sys
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from finviz_weekly.debate.runner import run_debate  # type: ignore


def test_debate_mock(tmp_path: Path, monkeypatch):
    data_dir = tmp_path / "data"
    latest = data_dir / "latest"
    latest.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        [
            {
                "ticker": "TEST",
                "company": "Test Corp",
                "sector": "Tech",
                "industry": "Software",
                "price": 10.0,
                "wfv": 12.0,
                "zone_label": "ADD",
                "upside_pct": 20.0,
                "score_master": 0.6,
            }
        ]
    )
    df.to_parquet(latest / "finviz_scored.parquet", index=False)
    (latest / "candidates.txt").write_text("TEST\n", encoding="utf-8")
    (latest / "conviction_2plus.csv").write_text("ticker\nTEST\n", encoding="utf-8")

    run_debate(out_dir=str(data_dir), input_mode="candidates", research=False, max_tickers=5, provider="mock", model="mock")

    debate_dir = data_dir / "debate"
    # ensure at least one dated folder created
    folders = list(debate_dir.iterdir())
    assert folders, "debate output folder missing"
    out_folder = folders[0]
    assert (out_folder / "TEST.json").exists()
    assert (out_folder / "TEST_evidence.json").exists()
    assert (out_folder / "debate_results.csv").exists()
