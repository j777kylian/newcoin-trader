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
    assert "listing-cohort-pilot" in result.output


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


def test_listing_cohort_pilot_help_documents_bounded_controls() -> None:
    result = runner.invoke(app, ["listing-cohort-pilot", "--help"])
    assert result.exit_code == 0, result.output
    output = result.output
    assert "max-articles" in output
    assert "max-pages" in output
    assert "requested-start" in output
    assert "requested-end" in output
    assert "max-probe-days" in output
    assert "binance-limit" in output
    assert "output-dir" in output
    assert "1000" in output


def test_listing_cohort_pilot_help_documents_fifty_target_and_three_year_lookback() -> None:
    result = runner.invoke(app, ["listing-cohort-pilot", "--help"])
    assert result.exit_code == 0, result.output
    text = " ".join(result.output.split())
    compact = " ".join("".join(ch.lower() if ch.isalnum() else " " for ch in result.output).split())
    assert "50 most" in text or "target 50" in text or "50 valid" in text
    assert "1095" in text
    assert "3-year" in text
    assert "12-month" not in text
    assert "20 most" not in text
    assert "1 2000" in compact
    assert "1 100" in compact
    assert "default 100" in compact
    assert "default 2000" in compact
    # Avoid matching binance-limit's 1–1000 as the pages cap.
    assert "pages" in compact and "1 100" in compact
    assert "articles" in compact and "1 2000" in compact


@pytest.mark.parametrize(
    "args",
    [
        ["--binance-limit", "0"],
        ["--binance-limit", "1001"],
        ["--max-articles", "0"],
        ["--max-articles", "2001"],
        ["--max-pages", "0"],
        ["--max-pages", "101"],
        ["--max-probe-days", "0"],
        ["--max-probe-days", "32"],
        [
            "--requested-start",
            "2024-02-01T00:00:00+00:00",
            "--requested-end",
            "2024-01-01T00:00:00+00:00",
        ],
    ],
)
def test_listing_cohort_pilot_cli_rejects_invalid_bounds_before_network(args: list[str]) -> None:
    result = runner.invoke(
        app,
        [
            "listing-cohort-pilot",
            "--requested-start",
            "2024-01-01T00:00:00+00:00",
            "--requested-end",
            "2024-02-01T00:00:00+00:00",
            "--output-dir",
            "artifacts/listing_cohort_pilot",
            *args,
        ],
    )
    assert result.exit_code == 2, result.output
    text = result.output.lower()
    assert "must be" in text or "after start" in text or "bound" in text
