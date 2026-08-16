"""CLI subcommand wiring — Typer must not collapse documented commands."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from newcoin_trader.cli.main import app

runner = CliRunner()


def test_smoke_offline_subcommand_with_output_dir(tmp_path: Path) -> None:
    result = runner.invoke(app, ["smoke-offline", "--output-dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "analysis.json").exists()
    assert (tmp_path / "paper_run.json").exists()
    assert (tmp_path / "window_stats.csv").exists()


def test_root_help_lists_smoke_offline_subcommand() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "ingest-market-history" in result.output
    assert "smoke-offline" in result.output
    assert "collect-once" in result.output
    assert "poll" in result.output
    assert "event-study" in result.output
    assert "feature-research" in result.output
    assert "executable-backtest" in result.output


def test_collect_once_help_is_available() -> None:
    result = runner.invoke(app, ["collect-once", "--help"])
    assert result.exit_code == 0
    assert "collect-once" in result.output.lower() or "Discover" in result.output


def test_poll_help_is_available() -> None:
    result = runner.invoke(app, ["poll", "--help"])
    assert result.exit_code == 0
    assert "interval" in result.output.lower()


def test_ingest_market_history_help_documents_control_bounds() -> None:
    result = runner.invoke(app, ["ingest-market-history", "--help"])
    assert result.exit_code == 0
    output = result.output
    assert "1" in output and "1000" in output
    assert "binance-limit" in output
    assert "raydium-page-size" in output
    assert "gecko-ohlcv-limit" in output


@pytest.mark.parametrize(
    "args",
    [
        ["--binance-limit", "0"],
        ["--binance-limit", "-1"],
        ["--binance-limit", "1001"],
        ["--raydium-page", "0"],
        ["--raydium-page-size", "0"],
        ["--raydium-page-size", "101"],
        ["--gecko-ohlcv-limit", "0"],
        ["--gecko-ohlcv-limit", "1001"],
    ],
)
def test_ingest_market_history_cli_rejects_invalid_bounds_before_network(args: list[str]) -> None:
    result = runner.invoke(app, ["ingest-market-history", *args])
    assert result.exit_code == 2, result.output
    assert "must be" in result.output.lower() or "bound" in result.output.lower() or "invalid" in result.output.lower()
