"""Deterministic JSON and CSV report writers."""

from __future__ import annotations

import csv
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from newcoin_trader.reports.schemas import to_jsonable


def write_json(path: Path, payload: Mapping[str, Any] | Sequence[Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(to_jsonable(payload), indent=2, sort_keys=True) + "\n"
    path.write_text(text, encoding="utf-8")
    return path


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]], *, fieldnames: Sequence[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: to_jsonable(row.get(key)) for key in fieldnames})
    return path
