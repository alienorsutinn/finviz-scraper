from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd


def _md_table(rows: List[List[str]], headers: List[str]) -> str:
    if not rows:
        return "_(no rows)_\n"
    widths = [len(h) for h in headers]
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))
    def fmt_row(r: List[str]) -> str:
        return "| " + " | ".join(cell.ljust(widths[i]) for i, cell in enumerate(r)) + " |"
    sep = "| " + " | ".join(("-" * w) for w in widths) + " |"
    out = [fmt_row(headers), sep]
    out += [fmt_row(r) for r in rows]
    return "\n".join(out) + "\n"


def _top_sector_counts(df: pd.DataFrame, n: int = 10) -> List[Tuple[str, int]]:
    if "sector" not in df.columns:
        return []
    vc = df["sector"].astype(str).value_counts().head(n)
    return list(zip(vc.index.tolist(), vc.values.tolist()))


def _read_weights_mode(weights_path: Path) -> str:
    if not weights_path.exists():
        return "missing"
    try:
        j = json.loads(weights_path.read_text(encoding="utf-8"))
        return str(j.get("mode", "unknown"))
    except Exception:
        return "unknown"


def write_report(
    *,
    out_dir: str,
    as_of: str,
    scored: pd.DataFrame,
    lists: Dict[str, pd.DataFrame],
    candidates_by_group: Dict[str, List[str]],
) -> None:
    latest_dir = Path(out_dir) / "latest"
    runs_dir = Path(out_dir) / "runs" / as_of
    latest_dir.mkdir(parents=True, exist_ok=True)
    runs_dir.mkdir(parents=True, exist_ok=True)

    weights_path = latest_dir / "learned_weights.json"
    mode = _read_weights_mode(weights_path)

    # counts
    total = int(len(scored))
    ok = int((scored["__status"] == "ok").sum()) if "__status" in scored.columns else total
    asset_counts = []
    if "asset_type" in scored.columns:
        vc = scored["asset_type"].astype(str).value_counts()
        asset_counts = list(zip(vc.index.tolist(), vc.values.tolist()))

    # repeated tickers across lists
    list_to_tickers: Dict[str, List[str]] = {}
    for k, df in lists.items():
        if "ticker" in df.columns:
            list_to_tickers[k] = [str(t).strip().upper() for t in df["ticker"].tolist() if str(t).strip()]
        else:
            list_to_tickers[k] = []

    all_ticks = []
    for v in list_to_tickers.values():
        all_ticks.extend(v)
    counts = Counter(all_ticks)

    repeated_2 = [(t, c) for t, c in counts.items() if c >= 2]
    repeated_3 = [(t, c) for t, c in counts.items() if c >= 3]
    repeated_2.sort(key=lambda x: (-x[1], x[0]))
    repeated_3.sort(key=lambda x: (-x[1], x[0]))

    # sector summary
    sector_counts = _top_sector_counts(scored, n=10)

    def top10_rows(df: pd.DataFrame) -> List[List[str]]:
        cols = ["ticker", "company", "sector", "industry", "market_cap", "price"]
        cols = [c for c in cols if c in df.columns]
        out = []
        for _, r in df.head(10).iterrows():
            row = []
            for c in cols:
                v = r.get(c, "")
                if c in ("market_cap", "price"):
                    try:
                        row.append(f"{float(v):,.2f}")
                    except Exception:
                        row.append(str(v))
                else:
                    row.append(str(v))
            out.append(row)
        return out, cols

    md = []
    md.append(f"# Finviz Weekly Report — {as_of}\n")
    md.append(f"- Weights mode: **{mode}**\n")
    md.append(f"- Rows (total): **{total}**\n")
    md.append(f"- Rows (__status==ok): **{ok}**\n")

    if asset_counts:
        md.append("\n## Asset type mix\n")
        rows = [[a, str(n)] for a, n in asset_counts]
        md.append(_md_table(rows, headers=["asset_type", "count"]))

    if sector_counts:
        md.append("\n## Top sectors (count)\n")
        rows = [[s, str(n)] for s, n in sector_counts]
        md.append(_md_table(rows, headers=["sector", "count"]))

    md.append("\n## Screen top 10s\n")
    for name in sorted(lists.keys()):
        df = lists[name]
        md.append(f"\n### {name}\n")
        rows, headers = top10_rows(df)
        md.append(_md_table(rows, headers=headers))

    md.append("\n## Candidates\n")
    for g in ["operating", "asset_manager", "bdc", "union"]:
        ticks = candidates_by_group.get(g, [])
        md.append(f"\n### {g} ({len(ticks)})\n")
        if ticks:
            md.append("`" + ", ".join(ticks[:150]) + (" …" if len(ticks) > 150 else "") + "`\n")
        else:
            md.append("_(none)_\n")

    md.append("\n## Most repeated tickers\n")
    md.append("\n### Appears in 3+ lists\n")
    if repeated_3:
        rows = [[t, str(c)] for t, c in repeated_3[:50]]
        md.append(_md_table(rows, headers=["ticker", "lists_count"]))
    else:
        md.append("_(none)_\n")

    md.append("\n### Appears in 2+ lists\n")
    if repeated_2:
        rows = [[t, str(c)] for t, c in repeated_2[:80]]
        md.append(_md_table(rows, headers=["ticker", "lists_count"]))
    else:
        md.append("_(none)_\n")

    content = "\n".join(md).strip() + "\n"

    (latest_dir / "report.md").write_text(content, encoding="utf-8")
    (runs_dir / "report.md").write_text(content, encoding="utf-8")


def write_report_from_latest(*, out_dir: str = "data") -> None:
    latest = Path(out_dir) / "latest"
    scored_path = latest / "finviz_scored.parquet"
    if not scored_path.exists():
        raise FileNotFoundError(f"Missing {scored_path}")

    scored = pd.read_parquet(scored_path)
    as_of = str(scored["as_of_date"].iloc[0]) if "as_of_date" in scored.columns else "latest"

    # load known lists if they exist
    lists = {}
    for fname in latest.glob("top*.csv"):
        try:
            lists[fname.name] = pd.read_csv(fname)
        except Exception:
            pass

    candidates_by_group = {}
    for fname, key in [
        ("candidates_operating.txt", "operating"),
        ("candidates_asset_managers.txt", "asset_manager"),
        ("candidates_bdc.txt", "bdc"),
        ("candidates.txt", "union"),
    ]:
        p = latest / fname
        if p.exists():
            candidates_by_group[key] = [t.strip().upper() for t in p.read_text().splitlines() if t.strip()]

    write_report(out_dir=out_dir, as_of=as_of, scored=scored, lists=lists, candidates_by_group=candidates_by_group)
