"""Phase 8A.2 early-market-event persistence (dedicated test DB only)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from inspect import signature

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from newcoin_trader.database.base import Base
from newcoin_trader.database.models import (
    EarlyMarketEventEvidence,
    EarlyMarketEventRecord,
    EarlyMarketObservation,
    Market,
)
from newcoin_trader.database.repositories.early_market_events import EarlyMarketEventRepository
from newcoin_trader.database.repositories.tokens import TokenRepository
from newcoin_trader.domain.early_market_events import (
    AssetIdentity,
    EarlyMarketEvent,
    EarlyMarketEventKind,
    EventClockQuality,
    EventQualityStatus,
    EventTimeSemantics,
    MarketIdentity,
)
from tests.integration._postgres import get_test_database_url

pytestmark = pytest.mark.integration

TEST_DATABASE_URL = get_test_database_url()
T0 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


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


async def _token(session: AsyncSession, *, address: str = "MintAAA", symbol: str = "AAA"):
    return await TokenRepository(session).upsert(
        chain="solana",
        token_address=address,
        symbol=symbol,
        created_time=T0,
        first_seen_time=T0,
        source="fixture",
    )


def _domain_event(**overrides: object) -> EarlyMarketEvent:
    payload: dict[str, object] = {
        "event_id": "eme-native-1",
        "event_kind": EarlyMarketEventKind.DEX_FIRST_LIQUIDITY,
        "event_definition_version": "8a.1.0",
        "source": "fixture",
        "venue_or_protocol": "raydium",
        "chain": "solana",
        "asset_identity": AssetIdentity(
            chain="solana",
            asset_key="MintAAA",
            symbol="AAA",
        ),
        "market_identity": MarketIdentity(
            chain="solana",
            venue_or_protocol="raydium",
            market_key="pool:pool-a",
            pool_or_pair_address="pool-a",
            base_asset_key="MintAAA",
            symbol="AAA/USDC",
        ),
        "source_event_time": T0,
        "received_time": T0 + timedelta(seconds=1),
        "decision_available_time": T0 + timedelta(seconds=2),
        "first_market_data_time": T0 + timedelta(seconds=5),
        "first_liquidity_time": T0 + timedelta(seconds=3),
        "first_trade_time": T0 + timedelta(seconds=4),
        "event_time_semantics": EventTimeSemantics.OBSERVED,
        "event_quality_status": EventQualityStatus.ACCEPTED,
        "event_clock_quality": EventClockQuality.EXACT,
        "provenance_ref": "prov://fixture/eme-native-1",
    }
    payload.update(overrides)
    return EarlyMarketEvent.model_validate(payload)


def test_persistence_tests_never_fall_back_to_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://application-db/newcoin")
    monkeypatch.delenv("NEWCOIN_TEST_DATABASE_URL", raising=False)
    assert get_test_database_url() == ""


@pytest.mark.asyncio
async def test_duplicate_native_event_id_dedupes(session: AsyncSession) -> None:
    token = await _token(session)
    repo = EarlyMarketEventRepository(session)
    event = _domain_event()
    first = await repo.insert_event(
        event,
        source_native_event_id="native-evt-1",
        asset_token=token,
    )
    second = await repo.insert_event(
        event,
        source_native_event_id="native-evt-1",
        asset_token=token,
    )
    count = await session.scalar(
        select(func.count())
        .select_from(EarlyMarketEventRecord)
        .where(EarlyMarketEventRecord.source_native_event_id == "native-evt-1")
    )
    assert first.id == second.id
    assert count == 1


@pytest.mark.asyncio
async def test_repeated_event_insert_does_not_rewrite_source_event_time_or_provenance(
    session: AsyncSession,
) -> None:
    token = await _token(session)
    repo = EarlyMarketEventRepository(session)
    original = _domain_event(
        source_event_time=T0,
        provenance_ref="prov://original",
    )
    first = await repo.insert_event(
        original,
        source_native_event_id="native-evt-rewrite",
        asset_token=token,
    )
    mutated = _domain_event(
        event_id="eme-native-rewrite",
        source_event_time=T0 + timedelta(hours=1),
        received_time=T0 + timedelta(hours=1, seconds=1),
        decision_available_time=T0 + timedelta(hours=1, seconds=2),
        provenance_ref="prov://mutated",
    )
    second = await repo.insert_event(
        mutated,
        source_native_event_id="native-evt-rewrite",
        asset_token=token,
    )
    assert second.id == first.id
    assert second.source_event_time == T0
    assert second.provenance_ref == "prov://original"


@pytest.mark.asyncio
async def test_two_pools_same_asset_are_distinct_markets(session: AsyncSession) -> None:
    token = await _token(session)
    repo = EarlyMarketEventRepository(session)
    market_a = await repo.insert_or_get_market(
        market_key="pool:pool-a",
        base_token=token,
        pool_or_pair_address="pool-a",
        venue="raydium",
        symbol="AAA/USDC",
        market_kind="pool",
        identity_status="resolved",
        source="fixture",
    )
    market_b = await repo.insert_or_get_market(
        market_key="pool:pool-b",
        base_token=token,
        pool_or_pair_address="pool-b",
        venue="raydium",
        symbol="AAA/USDC",
        market_kind="pool",
        identity_status="resolved",
        source="fixture",
    )
    assert market_a.id != market_b.id
    assert market_a.base_token_id == market_b.base_token_id == token.id
    count = await session.scalar(
        select(func.count()).select_from(Market).where(Market.market_key.in_(("pool:pool-a", "pool:pool-b")))
    )
    assert count == 2


@pytest.mark.asyncio
async def test_market_insert_requires_explicit_token_linkage_not_symbol_only(
    session: AsyncSession,
) -> None:
    repo = EarlyMarketEventRepository(session)
    params = signature(repo.insert_or_get_market).parameters
    assert "base_token" in params
    assert params["base_token"].default is params["base_token"].empty
    with pytest.raises(TypeError):
        await repo.insert_or_get_market(  # type: ignore[call-arg]
            market_key="pool:symbol-only",
            symbol="AAA/USDC",
            venue="raydium",
            market_kind="pool",
            identity_status="resolved",
            source="fixture",
        )


@pytest.mark.asyncio
async def test_evidence_is_append_only_and_does_not_mutate_event(session: AsyncSession) -> None:
    token = await _token(session)
    repo = EarlyMarketEventRepository(session)
    event_row = await repo.insert_event(
        _domain_event(),
        source_native_event_id="native-evt-evidence",
        asset_token=token,
    )
    original_time = event_row.source_event_time
    original_prov = event_row.provenance_ref
    evidence_a = await repo.append_evidence(
        event_id=event_row.id,
        evidence_kind="announcement",
        source="fixture",
        source_native_evidence_id="ev-1",
        observed_time=T0,
        received_time=T0 + timedelta(seconds=1),
        endpoint="https://example.test/a",
        dataset="announcements",
        stable_locator="loc://a",
        status="accepted",
        payload_metadata={"n": 1},
    )
    evidence_b = await repo.append_evidence(
        event_id=event_row.id,
        evidence_kind="announcement",
        source="fixture",
        source_native_evidence_id="ev-1",
        observed_time=T0,
        received_time=T0 + timedelta(seconds=1),
        endpoint="https://example.test/a",
        dataset="announcements",
        stable_locator="loc://a",
        status="accepted",
        payload_metadata={"n": 1},
    )
    await session.refresh(event_row)
    assert evidence_a.id != evidence_b.id
    assert event_row.source_event_time == original_time
    assert event_row.provenance_ref == original_prov
    count = await session.scalar(
        select(func.count())
        .select_from(EarlyMarketEventEvidence)
        .where(EarlyMarketEventEvidence.event_id == event_row.id)
    )
    assert count == 2


@pytest.mark.asyncio
async def test_duplicate_source_native_observation_dedupes(session: AsyncSession) -> None:
    token = await _token(session)
    repo = EarlyMarketEventRepository(session)
    market = await repo.insert_or_get_market(
        market_key="pool:obs-dedupe",
        base_token=token,
        pool_or_pair_address="pool-obs",
        venue="raydium",
        market_kind="pool",
        identity_status="resolved",
        source="fixture",
    )
    kwargs = {
        "market_id": market.id,
        "source_native_observation_id": "obs-1",
        "source": "fixture",
        "source_time": T0,
        "received_time": T0 + timedelta(seconds=1),
        "availability_status": "received",
        "price": Decimal("1.25"),
        "quantity": Decimal("10"),
        "liquidity": Decimal("1000"),
        "base_reserve": Decimal("500"),
        "quote_reserve": Decimal("625"),
        "side": "buy",
        "resolution": "point",
        "provenance_json": {"k": "v"},
    }
    first = await repo.insert_observation(**kwargs)
    second = await repo.insert_observation(**kwargs)
    count = await session.scalar(
        select(func.count())
        .select_from(EarlyMarketObservation)
        .where(EarlyMarketObservation.source_native_observation_id == "obs-1")
    )
    assert first.id == second.id
    assert count == 1


@pytest.mark.asyncio
async def test_source_time_only_observation_persists_distinctly(session: AsyncSession) -> None:
    token = await _token(session)
    repo = EarlyMarketEventRepository(session)
    market = await repo.insert_or_get_market(
        market_key="pool:source-time-only",
        base_token=token,
        venue="raydium",
        market_kind="pool",
        identity_status="resolved",
        source="fixture",
    )
    row = await repo.insert_observation(
        market_id=market.id,
        source_native_observation_id="obs-source-time-only",
        source="fixture",
        source_time=T0,
        received_time=None,
        availability_status="source_time_only",
        price=Decimal("2"),
    )
    assert row.received_time is None
    assert row.availability_status == "source_time_only"
    with pytest.raises(ValueError, match="receipt"):
        await repo.insert_observation(
            market_id=market.id,
            source_native_observation_id="obs-bad-receipt",
            source="fixture",
            source_time=T0,
            received_time=None,
            availability_status="receipt_verified",
            price=Decimal("2"),
        )


@pytest.mark.asyncio
async def test_list_events_and_observations_are_bounded_and_deterministic(
    session: AsyncSession,
) -> None:
    token = await _token(session)
    repo = EarlyMarketEventRepository(session)
    market = await repo.insert_or_get_market(
        market_key="pool:chrono",
        base_token=token,
        venue="raydium",
        market_kind="pool",
        identity_status="resolved",
        source="fixture",
    )
    for i, offset in enumerate((2, 0, 1)):
        ts = T0 + timedelta(minutes=offset)
        await repo.insert_event(
            _domain_event(
                event_id=f"eme-chrono-{i}",
                source_event_time=ts,
                received_time=ts + timedelta(seconds=1),
                decision_available_time=ts + timedelta(seconds=2),
                provenance_ref=f"prov://chrono/{i}",
            ),
            source_native_event_id=f"native-chrono-{i}",
            asset_token=token,
            market=market,
        )
        await repo.insert_observation(
            market_id=market.id,
            source_native_observation_id=f"obs-chrono-{i}",
            source="fixture",
            source_time=ts,
            received_time=ts + timedelta(seconds=1),
            availability_status="received",
            price=Decimal(str(i + 1)),
        )

    events = await repo.list_events(limit=1000)
    chrono_events = [row for row in events if row.source_native_event_id.startswith("native-chrono-")]
    assert [row.source_native_event_id for row in chrono_events] == [
        "native-chrono-1",
        "native-chrono-2",
        "native-chrono-0",
    ]
    assert [(row.source_event_time, row.id) for row in events] == sorted(
        (row.source_event_time, row.id) for row in events
    )

    observations = await repo.list_observations(limit=1000)
    chrono_obs = [row for row in observations if row.source_native_observation_id.startswith("obs-chrono-")]
    assert [row.source_native_observation_id for row in chrono_obs] == [
        "obs-chrono-1",
        "obs-chrono-2",
        "obs-chrono-0",
    ]
    assert [(row.source_time, row.id) for row in observations] == sorted(
        (row.source_time, row.id) for row in observations
    )

    bounded = await repo.list_events(limit=2)
    assert len(bounded) == 2
    with pytest.raises(ValueError, match="limit"):
        await repo.list_events(limit=0)
    with pytest.raises(ValueError, match="limit"):
        await repo.list_observations(limit=1001)
