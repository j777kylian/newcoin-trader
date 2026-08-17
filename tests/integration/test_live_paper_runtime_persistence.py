"""Phase 6.5 CLI/service runtime persistence: application DATABASE_URL only."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from typer.testing import CliRunner

from newcoin_trader.cli.main import app
from newcoin_trader.database.base import Base
from newcoin_trader.database.models import LivePaperPosition, LivePaperSession, LivePaperSignal
from newcoin_trader.domain.enums import Chain, Venue
from newcoin_trader.domain.event_study import ObservationResolution, TokenListingEvent
from newcoin_trader.domain.executable_backtest import FrozenCandidateIdentity
from newcoin_trader.domain.feature_research import RuleCondition
from newcoin_trader.domain.live_paper import LivePaperStatus, PositionLifecycle, ReplayMarketEvent
from newcoin_trader.research.prospective_feed import ProspectiveFeedResult, ProspectiveFeedStatus
from newcoin_trader.services.live_paper import LivePaperService
from tests.integration._postgres import get_test_database_url

pytestmark = pytest.mark.integration

runner = CliRunner()
TEST_DATABASE_URL = get_test_database_url()
T0 = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)


def _postgres_available() -> bool:
    return TEST_DATABASE_URL.startswith("postgresql")


@pytest.fixture
async def session() -> AsyncIterator[AsyncSession]:
    if not _postgres_available():
        pytest.skip("PostgreSQL not configured")
    engine = create_async_engine(TEST_DATABASE_URL)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as sess:
            yield sess
            await sess.rollback()
    except (OSError, OperationalError, ConnectionRefusedError):
        pytest.skip("PostgreSQL is not reachable")
    finally:
        await engine.dispose()


def _unique_rule_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _identity(*, rule_id: str) -> FrozenCandidateIdentity:
    cond = RuleCondition(feature_name="age_source_event_seconds", op="gte", threshold=Decimal("0"))
    return FrozenCandidateIdentity(
        rule_id=rule_id,
        conditions=(cond,),
        human_readable="age_source_event_seconds gte 0",
        phase4_config_id="cfg-phase4",
        split_label="test",
        fold_index=0,
        provenance={"source": "frozen_phase4"},
    )


def _listing(*, event_id: str = "e1") -> TokenListingEvent:
    return TokenListingEvent(
        event_id=event_id,
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


def _listing_event(item: TokenListingEvent) -> ReplayMarketEvent:
    return ReplayMarketEvent(
        event_id=item.event_id,
        kind="listing",
        venue=item.venue,
        token_address=item.token_address,
        chain=item.chain.value,
        source_timestamp=item.source_event_time,
        received_timestamp=item.source_event_time,
        source=item.source,
        listing=item,
        provenance=dict(item.provenance),
    )


def _market(*, ts: datetime, price: str, event_id: str = "e1") -> ReplayMarketEvent:
    return ReplayMarketEvent(
        event_id=event_id,
        kind="market",
        venue=Venue.BINANCE,
        token_address="TOKEN",
        chain="binance",
        source_timestamp=ts,
        received_timestamp=ts,
        price=Decimal(price),
        liquidity=Decimal("100000"),
        volume=Decimal("1000"),
        resolution=ObservationResolution.POINT,
        source="binance:trade",
        provenance={"kind": "trade"},
    )


def _fill_events() -> list[ReplayMarketEvent]:
    decision = T0 + timedelta(minutes=1)
    return [
        _listing_event(_listing()),
        _market(ts=decision, price="10"),
        _market(ts=decision + timedelta(minutes=5), price="12"),
    ]


def _open_or_failed_exit_events() -> list[ReplayMarketEvent]:
    decision = T0 + timedelta(minutes=1)
    return [
        _listing_event(_listing()),
        _market(ts=decision, price="10"),
    ]


def _write_replay(path: Path, events: list[ReplayMarketEvent]) -> Path:
    path.write_text(json.dumps({"events": [e.model_dump(mode="json") for e in events]}), encoding="utf-8")
    return path


def _cli_args(
    *,
    output_dir: Path,
    session_start: datetime,
    rule_id: str,
    replay_path: Path | None = None,
    mode: str = "replay",
) -> list[str]:
    args = [
        "live-paper",
        "--mode",
        mode,
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
        rule_id,
        "--phase4-config-id",
        "cfg-phase4",
        "--paper-starting-cash",
        "10000",
        "--output-dir",
        str(output_dir),
        "--session-start",
        session_start.isoformat(),
        "--holding-period",
        "5m",
        "--position-notional",
        "100",
    ]
    if mode == "replay":
        assert replay_path is not None
        args.extend(["--replay-path", str(replay_path)])
    else:
        args.extend(
            [
                "--poll-interval",
                "1s",
                "--symbol",
                "NEWUSDT",
                "--max-polls",
                "1",
                "--max-observations-per-token",
                "10",
                "--max-total-observations",
                "20",
            ]
        )
    return args


def _install_static_prospective_feed(monkeypatch: pytest.MonkeyPatch, events: list[ReplayMarketEvent]) -> None:
    class _StaticFeed:
        async def collect_bounded(self) -> ProspectiveFeedResult:
            return ProspectiveFeedResult(
                events=tuple(events),
                status=ProspectiveFeedStatus.OK,
                poll_count=1,
                overflow_count=0,
                rejected_count=0,
                duplicate_suppressed_count=0,
                source_errors=(),
                observations_emitted=len(events),
            )

    def _build(**_kwargs: object) -> _StaticFeed:
        return _StaticFeed()

    class _FakeHttp:
        async def aclose(self) -> None:
            return None

    monkeypatch.setattr("newcoin_trader.research.prospective_feed.build_prospective_feed", _build)
    monkeypatch.setattr("newcoin_trader.collectors.http.AsyncHttpClient", lambda **_k: _FakeHttp())


async def _count_sessions(session: AsyncSession, session_id: str) -> int:
    value = await session.scalar(
        select(func.count()).select_from(LivePaperSession).where(LivePaperSession.session_id == session_id)
    )
    return int(value or 0)


async def _ensure_schema() -> None:
    engine = create_async_engine(TEST_DATABASE_URL)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    finally:
        await engine.dispose()


def _assert_artifacts(output_dir: Path) -> dict[str, object]:
    json_path = output_dir / "live_paper_summary.json"
    csv_path = output_dir / "live_paper_signals.csv"
    md_path = output_dir / "live_paper_summary.md"
    assert json_path.exists()
    assert csv_path.exists()
    assert md_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload
    assert payload.get("meta", {}).get("session_id")
    assert "signals" in payload
    assert "portfolio" in payload
    return payload


@pytest.mark.asyncio
async def test_prospective_service_persists_one_session_by_session_id(
    session: AsyncSession,
    tmp_path: Path,
) -> None:
    identity = _identity(rule_id=_unique_rule_id("persist-svc-prospective"))
    events = _fill_events()

    class _StaticFeed:
        async def collect_bounded(self) -> ProspectiveFeedResult:
            return ProspectiveFeedResult(
                events=tuple(events),
                status=ProspectiveFeedStatus.OK,
                poll_count=1,
                overflow_count=0,
                rejected_count=0,
                duplicate_suppressed_count=0,
                source_errors=(),
            )

    service = LivePaperService(session=session)
    report, paths = await service.run_prospective(
        feed=_StaticFeed(),
        identity=identity,
        venue="binance",
        duration=timedelta(hours=1),
        max_events=50,
        max_signals=20,
        max_trades=20,
        queue_capacity=100,
        starting_cash=Decimal("10000"),
        output_dir=tmp_path,
        session_start=T0,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
    )
    await session.commit()
    assert paths["json"].exists()
    assert report.meta.session_id
    assert await _count_sessions(session, report.meta.session_id) == 1
    row = await session.scalar(select(LivePaperSession).where(LivePaperSession.session_id == report.meta.session_id))
    assert row is not None
    assert row.session_id == report.meta.session_id
    assert row.venue
    assert row.frozen_rule_id == identity.rule_id
    assert row.state_json


def test_prospective_cli_persists_one_session_by_session_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if not _postgres_available():
        pytest.skip("PostgreSQL not configured")
    asyncio.run(_ensure_schema())
    monkeypatch.setenv("DATABASE_URL", TEST_DATABASE_URL)
    monkeypatch.delenv("NEWCOIN_TEST_DATABASE_URL", raising=False)
    _install_static_prospective_feed(monkeypatch, _fill_events())
    out = tmp_path / "prospective"
    rule_id = _unique_rule_id("persist-cli-prospective")
    result = runner.invoke(
        app,
        _cli_args(
            output_dir=out,
            session_start=T0,
            rule_id=rule_id,
            mode="prospective",
        ),
    )
    assert result.exit_code == 0, result.output
    payload = _assert_artifacts(out)
    session_id = str(payload["meta"]["session_id"])

    async def _assert_row() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory() as sess:
                assert await _count_sessions(sess, session_id) == 1
                row = await sess.scalar(select(LivePaperSession).where(LivePaperSession.session_id == session_id))
                assert row is not None
                assert row.session_id == session_id
                assert row.venue
                assert row.frozen_rule_id == rule_id
                assert row.state_json
        finally:
            await engine.dispose()

    asyncio.run(_assert_row())


def test_replay_cli_accepted_signal_persists_live_paper_signals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if not _postgres_available():
        pytest.skip("PostgreSQL not configured")
    asyncio.run(_ensure_schema())
    monkeypatch.setenv("DATABASE_URL", TEST_DATABASE_URL)
    replay = _write_replay(tmp_path / "replay.json", _fill_events())
    out = tmp_path / "signals"
    result = runner.invoke(
        app,
        _cli_args(
            output_dir=out,
            session_start=T0,
            rule_id=_unique_rule_id("persist-cli-signals"),
            replay_path=replay,
        ),
    )
    assert result.exit_code == 0, result.output
    payload = _assert_artifacts(out)
    session_id = str(payload["meta"]["session_id"])
    accepted = [s for s in payload["signals"] if s["status"] == LivePaperStatus.SIGNAL_ACCEPTED.value]
    assert accepted

    async def _assert_row() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory() as sess:
                rows = list(
                    (await sess.scalars(select(LivePaperSignal).where(LivePaperSignal.session_id == session_id))).all()
                )
                assert rows
                assert any(row.status == LivePaperStatus.SIGNAL_ACCEPTED.value for row in rows)
                assert all(row.signal_id and row.session_id == session_id for row in rows)
        finally:
            await engine.dispose()

    asyncio.run(_assert_row())


def test_replay_cli_open_or_failed_exit_position_persists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if not _postgres_available():
        pytest.skip("PostgreSQL not configured")
    asyncio.run(_ensure_schema())
    monkeypatch.setenv("DATABASE_URL", TEST_DATABASE_URL)
    replay = _write_replay(tmp_path / "replay.json", _open_or_failed_exit_events())
    out = tmp_path / "positions"
    result = runner.invoke(
        app,
        _cli_args(
            output_dir=out,
            session_start=T0,
            rule_id=_unique_rule_id("persist-cli-positions"),
            replay_path=replay,
        ),
    )
    assert result.exit_code == 0, result.output
    payload = _assert_artifacts(out)
    session_id = str(payload["meta"]["session_id"])
    open_or_failed = {
        PositionLifecycle.OPEN.value,
        PositionLifecycle.FAILED_EXIT.value,
    }
    assert any(p["lifecycle"] in open_or_failed for p in payload["positions"])

    async def _assert_row() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory() as sess:
                rows = list(
                    (
                        await sess.scalars(select(LivePaperPosition).where(LivePaperPosition.session_id == session_id))
                    ).all()
                )
                assert rows
                assert any(row.lifecycle in open_or_failed for row in rows)
                assert all(row.position_id and row.session_id == session_id for row in rows)
        finally:
            await engine.dispose()

    asyncio.run(_assert_row())


def test_replay_cli_three_run_restart_idempotency_is_monotonic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if not _postgres_available():
        pytest.skip("PostgreSQL not configured")
    asyncio.run(_ensure_schema())
    monkeypatch.setenv("DATABASE_URL", TEST_DATABASE_URL)
    replay = _write_replay(tmp_path / "replay.json", _fill_events())
    rule_id = _unique_rule_id("persist-cli-idempotent")
    session_ids: list[str] = []
    seen: list[set[str]] = []
    for idx in range(3):
        out = tmp_path / f"run{idx}"
        result = runner.invoke(
            app,
            _cli_args(
                output_dir=out,
                session_start=T0,
                rule_id=rule_id,
                replay_path=replay,
            ),
        )
        assert result.exit_code == 0, result.output
        payload = _assert_artifacts(out)
        session_ids.append(str(payload["meta"]["session_id"]))

        async def _load_state(session_id: str) -> dict[str, object]:
            engine = create_async_engine(TEST_DATABASE_URL)
            try:
                factory = async_sessionmaker(engine, expire_on_commit=False)
                async with factory() as sess:
                    assert await _count_sessions(sess, session_id) == 1
                    row = await sess.scalar(select(LivePaperSession).where(LivePaperSession.session_id == session_id))
                    assert row is not None
                    assert row.state_json
                    return dict(row.state_json)
            finally:
                await engine.dispose()

        state = asyncio.run(_load_state(session_ids[-1]))
        seen.append({str(x) for x in state.get("seen_signals", ())})

    assert len(set(session_ids)) == 1
    assert seen[0]
    assert seen[1] >= seen[0]
    assert seen[2] >= seen[1]
