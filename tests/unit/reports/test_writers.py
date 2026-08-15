"""Report writer reproducibility."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from newcoin_trader.reports.writers import write_csv, write_json


def test_write_json_and_csv(tmp_path: Path) -> None:
    json_path = write_json(tmp_path / "out.json", {"price": Decimal("1.25"), "n": 2})
    csv_path = write_csv(
        tmp_path / "out.csv",
        [{"window": "5m", "simple_return": Decimal("0.1")}],
        fieldnames=["window", "simple_return"],
    )
    assert '"1.25"' in json_path.read_text(encoding="utf-8")
    text = csv_path.read_text(encoding="utf-8")
    assert "window,simple_return" in text
    assert "5m,0.1" in text
