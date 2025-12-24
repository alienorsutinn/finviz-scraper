from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict


@dataclass
class EvidencePack:
    ticker: str
    cards: List[Dict]


@dataclass
class DebateOutcome:
    ticker: str
    packet: Dict
    evidence: EvidencePack
    judge: Dict
    scores: Dict[str, float]
    agents: Dict[str, Dict]
