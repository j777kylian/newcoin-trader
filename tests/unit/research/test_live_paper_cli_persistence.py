"""CLI runtime persistence wiring: DATABASE_URL only, never the test-database env."""

from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

from newcoin_trader.cli.main import app, live_paper
from newcoin_trader.domain.enums import Chain, Venue
from newcoin_trader.domain.event_study import ObservationResolution, TokenListingEvent
from newcoin_trader.domain.live_paper import ReplayMarketEvent

runner = CliRunner()

T0 = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)


def _replay_json(path: Path) -> Path:
    listing = TokenListingEvent(
        event_id="e1",
        venue=Venue.BINANCE,
        chain=Chain.BINANCE,
        token_address="TOKEN",
        pair_address="PAIR",
        symbol="TOK",
        source="binance",
        source_event_time=T0,
        first_seen_time=T0,
        first_market_data_time=T0,
        decision_available_time=T0,
        provenance={"token_id": "1"},
    )
    decision = T0 + timedelta(minutes=1)
    events = [
        ReplayMarketEvent(
            event_id="e1",
            kind="listing",
            venue=Venue.BINANCE,
            token_address="TOKEN",
            chain="binance",
            source_timestamp=T0,
            received_timestamp=T0,
            source="binance",
            listing=listing,
            provenance=dict(listing.provenance),
        ),
        ReplayMarketEvent(
            event_id="e1",
            kind="market",
            venue=Venue.BINANCE,
            token_address="TOKEN",
            chain="binance",
            source_timestamp=decision,
            received_timestamp=decision,
            price=Decimal("10"),
            liquidity=Decimal("100000"),
            volume=Decimal("1000"),
            resolution=ObservationResolution.POINT,
            source="binance:trade",
            provenance={"kind": "trade"},
        ),
    ]
    path.write_text(json.dumps({"events": [e.model_dump(mode="json") for e in events]}), encoding="utf-8")
    return path


def test_live_paper_cli_source_wires_application_session_not_test_env() -> None:
    src = inspect.getsource(live_paper)
    compact = "".join(src.split())
    assert "LivePaperService()" not in compact
    assert "LivePaperService(session=" in compact
    assert "open_research_db_stack" in src
    assert "NEWCOIN_TEST_DATABASE_URL" not in src
    cli_path = Path(inspect.getfile(live_paper))
    assert "NEWCOIN_TEST_DATABASE_URL" not in cli_path.read_text(encoding="utf-8")


def test_live_paper_cli_fails_without_database_url_and_ignores_test_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("NEWCOIN_TEST_DATABASE_URL", "postgresql+asyncpg://test-db/newcoin_test")
    replay = _replay_json(tmp_path / "replay.json")
    result = runner.invoke(
        app,
        [
            "live-paper",
            "--venue",
            "binance",
            "--duration",
            "1h",
            "--max-events",
            "50",
            "--max-signals",
            "20",
            "--max-trades",
            "20",
            "--queue-capacity",
            "100",
            "--frozen-rule-id",
            "frozen-rule-persist",
            "--phase4-config-id",
            "cfg-phase4",
            "--paper-starting-cash",
            "10000",
            "--output-dir",
            str(tmp_path / "out"),
            "--replay-path",
            str(replay),
            "--session-start",
            T0.isoformat(),
        ],
    )
    assert result.exit_code == 2, result.output
    assert "DATABASE_URL" in result.output
    assert "NEWCOIN_TEST_DATABASE_URL" not in result.output
