"""Offline smoke path tests."""

from __future__ import annotations

import inspect
from pathlib import Path

from newcoin_trader import demo
from newcoin_trader.demo import run_offline_smoke


def test_demo_module_has_no_network_client() -> None:
    source = inspect.getsource(demo)
    assert "httpx" not in source
    assert "AsyncHttpClient" not in source
    assert "create_engine" not in source


def test_offline_smoke_writes_artifacts(tmp_path: Path) -> None:
    code = run_offline_smoke(output_dir=tmp_path)
    assert code == 0
    assert (tmp_path / "analysis.json").exists()
    assert (tmp_path / "paper_run.json").exists()
    assert (tmp_path / "window_stats.csv").exists()
