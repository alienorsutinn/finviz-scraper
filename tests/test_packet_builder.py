from pathlib import Path
import sys
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from finviz_weekly.debate.packet import build_packet  # type: ignore


def test_build_packet_minimal():
    df = pd.DataFrame([{"ticker": "ABC", "company": "ABC Inc", "price": 10.0, "score_master": 0.5}])
    pkt = build_packet(df, "ABC")
    assert pkt["ticker"] == "ABC"
    assert pkt["company"] == "ABC Inc"
    assert "score_master" in pkt
