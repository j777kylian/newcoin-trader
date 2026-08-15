"""Acceptance repairs: deterministic smoke, packaged fixtures, stronger idempotency keys."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import venv
from datetime import UTC, datetime
from decimal import Decimal
from importlib.resources import files
from pathlib import Path

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from newcoin_trader.database.models import PaperTrade, StrategyResult
from newcoin_trader.database.repositories.paper import paper_trade_upsert_statement
from newcoin_trader.database.repositories.strategy import strategy_result_upsert_statement
from newcoin_trader.demo import run_offline_smoke


def _constraint_names(table: object) -> set[str]:
    return {c.name for c in table.constraints if c.name}  # type: ignore[attr-defined]


def test_offline_smoke_two_runs_byte_identical(tmp_path: Path) -> None:
    a = tmp_path / "a"
    b = tmp_path / "b"
    assert run_offline_smoke(output_dir=a) == 0
    assert run_offline_smoke(output_dir=b) == 0
    for name in ("paper_run.json", "analysis.json", "window_stats.csv"):
        assert (a / name).read_bytes() == (b / name).read_bytes()


def test_offline_smoke_accepts_explicit_run_id(tmp_path: Path) -> None:
    rid = "11111111-1111-4111-8111-111111111111"
    assert run_offline_smoke(output_dir=tmp_path, run_id=rid) == 0
    payload = json.loads((tmp_path / "paper_run.json").read_text(encoding="utf-8"))
    assert payload["run_id"] == rid


def test_packaged_demo_fixtures_via_importlib_resources() -> None:
    root = files("newcoin_trader.resources.demo_run")
    meta = json.loads(root.joinpath("meta.json").read_text(encoding="utf-8"))
    snaps = json.loads(root.joinpath("snapshots.json").read_text(encoding="utf-8"))
    assert meta["token_address"]
    assert isinstance(snaps, list) and len(snaps) >= 2


def test_strategy_result_unique_includes_windows_and_nulls_not_distinct() -> None:
    names = _constraint_names(StrategyResult.__table__)
    assert "uq_strategy_results_run_strategy_token_window" in names
    uq = next(
        c for c in StrategyResult.__table__.constraints if c.name == "uq_strategy_results_run_strategy_token_window"
    )
    assert uq.dialect_kwargs.get("postgresql_nulls_not_distinct") is True
    cols = [col.name for col in uq.columns]
    assert cols == [
        "run_id",
        "strategy_name",
        "strategy_version",
        "token_id",
        "window_start",
        "window_end",
    ]
    ddl = str(CreateTable(StrategyResult.__table__).compile(dialect=postgresql.dialect())).lower()
    assert "nulls not distinct" in ddl
    sql = str(
        strategy_result_upsert_statement(
            run_id="00000000-0000-0000-0000-000000000001",
            strategy_name="listing_momentum",
            strategy_version="1.0.0",
            token_id=None,
            params={},
            metrics={},
            signals=None,
            window_start=datetime(2024, 1, 1, tzinfo=UTC),
            window_end=datetime(2024, 1, 2, tzinfo=UTC),
        ).compile(dialect=postgresql.dialect())
    ).lower()
    assert "on conflict" in sql
    assert "uq_strategy_results_run_strategy_token_window" in sql


def test_paper_trade_unique_includes_requested_price() -> None:
    names = _constraint_names(PaperTrade.__table__)
    assert "uq_paper_trades_run_order" in names
    assert "requested_price" in {c.name for c in PaperTrade.__table__.columns}
    uq = next(c for c in PaperTrade.__table__.constraints if c.name == "uq_paper_trades_run_order")
    cols = [col.name for col in uq.columns]
    assert "requested_price" in cols
    assert "requested_qty" in cols
    sql = str(
        paper_trade_upsert_statement(
            run_id="00000000-0000-0000-0000-000000000001",
            token_id=1,
            signal_ts=datetime(2024, 1, 1, tzinfo=UTC),
            side="buy",
            requested_qty=Decimal("1"),
            requested_price=Decimal("1.25"),
            fill_price=Decimal("1"),
            fill_qty=Decimal("1"),
            fee=Decimal("0"),
            slippage_bps=Decimal("0"),
            status="filled",
            reject_reason=None,
        ).compile(dialect=postgresql.dialect())
    ).lower()
    assert "on conflict" in sql
    assert "requested_price" in sql


def test_wheel_install_exposes_demo_fixtures_and_smoke(tmp_path: Path) -> None:
    """Prove packaged resources work after a real wheel install (not editable)."""
    repo = Path(__file__).resolve().parents[2]
    build_dir = tmp_path / "dist"
    build_dir.mkdir()
    env = {**os.environ, "UV_LINK_MODE": "copy"}
    built = subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(build_dir)],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
    )
    assert built.returncode == 0, built.stderr or built.stdout
    wheels = list(build_dir.glob("newcoin_trader-*.whl"))
    assert wheels, "uv build did not produce a wheel"
    wheel = wheels[0]
    venv_dir = tmp_path / "venv"
    venv.create(venv_dir, with_pip=True, clear=True)
    py = venv_dir / ("Scripts" if sys.platform == "win32" else "bin") / "python"
    installed = subprocess.run(
        [str(py), "-m", "pip", "install", "--quiet", str(wheel)],
        capture_output=True,
        text=True,
    )
    assert installed.returncode == 0, installed.stderr or installed.stdout
    probe = subprocess.run(
        [
            str(py),
            "-c",
            "from importlib.resources import files; "
            "print(files('newcoin_trader.resources.demo_run').joinpath('meta.json').read_text())",
        ],
        capture_output=True,
        text=True,
    )
    assert probe.returncode == 0, probe.stderr
    assert "DemoMint" in probe.stdout
    out = tmp_path / "smoke_out"
    cli = venv_dir / ("Scripts" if sys.platform == "win32" else "bin") / "newcoin-trader"
    result = subprocess.run(
        [str(cli), "smoke-offline", "--output-dir", str(out)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    assert (out / "paper_run.json").is_file()
    digest = hashlib.sha256((out / "paper_run.json").read_bytes()).hexdigest()
    assert len(digest) == 64
