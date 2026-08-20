"""Phase 8A.3 early-market projection repository reads (dedicated test DB only)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from newcoin_trader.database.base import Base
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
from newcoin_trader.research.early_market_event_projection import (
    project_early_market_event,
    project_early_market_observations,
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
        venue="raydium",
    )


def _domain_event(**overrides: object) -> EarlyMarketEvent:
    payload: dict[str, object] = {
        "event_id": "eme-native-1",
        "event_kind": EarlyMarketEventKind.DEX_FIRST_LIQUIDITY,
        "event_definition_version": "8a.1.0",
        "source": "fixture",
        "venue_or_protocol": "raydium",
        "chain": "solana",
        "asset_identity": AssetIdentity(chain="solana", asset_key="MintAAA", symbol="AAA"),
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


def test_projection_repository_tests_never_fall_back_to_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://application-db/newcoin")
    monkeypatch.delenv("NEWCOIN_TEST_DATABASE_URL", raising=False)
    assert get_test_database_url() == ""


@pytest.mark.asyncio
async def test_list_associated_events_is_chronological_deterministic_and_exact(
    session: AsyncSession,
) -> None:
    token = await _token(session)
    repo = EarlyMarketEventRepository(session)
    market_a = await repo.insert_or_get_market(
        market_key="pool:assoc-a",
        base_token=token,
        pool_or_pair_address="pool-a",
        venue="raydium",
        market_kind="pool",
        identity_status="resolved",
        source="fixture",
    )
    market_b = await repo.insert_or_get_market(
        market_key="pool:assoc-b",
        base_token=token,
        pool_or_pair_address="pool-b",
        venue="raydium",
        market_kind="pool",
        identity_status="resolved",
        source="fixture",
    )
    # Insert out of chronological order.
    for i, offset in enumerate((2, 0, 1)):
        ts = T0 + timedelta(minutes=offset)
        market = market_a if i != 1 else market_b
        await repo.insert_event(
            _domain_event(
                event_id=f"eme-assoc-{i}",
                source_event_time=ts,
                received_time=ts + timedelta(seconds=1),
                decision_available_time=ts + timedelta(seconds=2),
                provenance_ref=f"prov://assoc/{i}",
            ),
            source_native_event_id=f"native-assoc-{i}",
            asset_token=token,
            market=market,
        )

    rows = await repo.list_associated_events(
        start=T0,
        end=T0 + timedelta(hours=1),
        limit=100,
    )
    assoc = [row for row in rows if row.event.source_native_event_id.startswith("native-assoc-")]
    assert [row.event.source_native_event_id for row in assoc] == [
        "native-assoc-1",
        "native-assoc-2",
        "native-assoc-0",
    ]
    assert all(row.token.id == token.id for row in assoc)
    assert assoc[0].market is not None and assoc[0].market.id == market_b.id
    assert assoc[1].market is not None and assoc[1].market.id == market_a.id
    assert assoc[2].market is not None and assoc[2].market.id == market_a.id

    projected = [project_early_market_event(row.event, token=row.token, market=row.market) for row in assoc]
    assert [p.event_id for p in projected] == [
        "fixture:native-assoc-1",
        "fixture:native-assoc-2",
        "fixture:native-assoc-0",
    ]
    assert all(p.provenance["market_association_reason"] == "exact_event_market_id" for p in projected)


@pytest.mark.asyncio
async def test_list_associated_events_rejects_overflow_and_bad_limits(
    session: AsyncSession,
) -> None:
    token = await _token(session, address="MintOverflow", symbol="OVF")
    repo = EarlyMarketEventRepository(session)
    market = await repo.insert_or_get_market(
        market_key="pool:overflow",
        base_token=token,
        venue="raydium",
        market_kind="pool",
        identity_status="resolved",
        source="fixture",
    )
    for i in range(3):
        ts = T0 + timedelta(minutes=i)
        await repo.insert_event(
            _domain_event(
                event_id=f"eme-ovf-{i}",
                source_event_time=ts,
                received_time=ts + timedelta(seconds=1),
                decision_available_time=ts + timedelta(seconds=2),
                provenance_ref=f"prov://ovf/{i}",
            ),
            source_native_event_id=f"native-ovf-{i}",
            asset_token=token,
            market=market,
        )

    with pytest.raises(ValueError, match="overflow"):
        await repo.list_associated_events(start=T0, end=T0 + timedelta(hours=1), limit=2)

    with pytest.raises(ValueError, match="limit"):
        await repo.list_associated_events(start=T0, end=T0 + timedelta(hours=1), limit=0)
    with pytest.raises(ValueError, match="limit"):
        await repo.list_associated_events(start=T0, end=T0 + timedelta(hours=1), limit=1001)
    with pytest.raises(ValueError, match="limit"):
        await repo.list_associated_events(start=T0, end=T0 + timedelta(hours=1), limit=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="end"):
        await repo.list_associated_events(start=T0, end=T0, limit=10)


@pytest.mark.asyncio
async def test_list_observations_for_market_is_market_specific_chronological_no_cross_market(
    session: AsyncSession,
) -> None:
    token = await _token(session, address="MintObs", symbol="OBS")
    repo = EarlyMarketEventRepository(session)
    market_a = await repo.insert_or_get_market(
        market_key="pool:obs-a",
        base_token=token,
        pool_or_pair_address="pool-obs-a",
        venue="raydium",
        market_kind="pool",
        identity_status="resolved",
        source="fixture",
    )
    market_b = await repo.insert_or_get_market(
        market_key="pool:obs-b",
        base_token=token,
        pool_or_pair_address="pool-obs-b",
        venue="raydium",
        market_kind="pool",
        identity_status="resolved",
        source="fixture",
    )
    event = await repo.insert_event(
        _domain_event(event_id="eme-obs", provenance_ref="prov://obs"),
        source_native_event_id="native-obs",
        asset_token=token,
        market=market_a,
    )

    # Out-of-order inserts; market B must never appear in market A reads.
    for i, offset in enumerate((2, 0, 1)):
        ts = T0 + timedelta(minutes=offset)
        await repo.insert_observation(
            market_id=market_a.id,
            event_id=event.id,
            source_native_observation_id=f"obs-a-{i}",
            source="fixture",
            source_time=ts,
            received_time=ts + timedelta(seconds=1),
            availability_status="received",
            price=Decimal(str(i + 1)),
            provenance_json={"kind": "trade"},
        )
        await repo.insert_observation(
            market_id=market_b.id,
            source_native_observation_id=f"obs-b-{i}",
            source="fixture",
            source_time=ts,
            received_time=ts + timedelta(seconds=1),
            availability_status="received",
            price=Decimal("99"),
            provenance_json={"kind": "trade"},
        )

    rows = await repo.list_observations_for_market(
        market_id=market_a.id,
        start=T0,
        end=T0 + timedelta(hours=1),
        limit=100,
    )
    assert [row.source_native_observation_id for row in rows] == [
        "obs-a-1",
        "obs-a-2",
        "obs-a-0",
    ]
    assert all(row.market_id == market_a.id for row in rows)
    assert all(not row.source_native_observation_id.startswith("obs-b-") for row in rows)

    projected = project_early_market_observations(rows, token=token, market=market_a)
    assert len(projected) == 3
    assert [o.timestamp for o in projected] == [T0, T0 + timedelta(minutes=1), T0 + timedelta(minutes=2)]


@pytest.mark.asyncio
async def test_list_observations_for_market_rejects_overflow_and_bad_args(
    session: AsyncSession,
) -> None:
    token = await _token(session, address="MintObsOvf", symbol="O2")
    repo = EarlyMarketEventRepository(session)
    market = await repo.insert_or_get_market(
        market_key="pool:obs-ovf",
        base_token=token,
        venue="raydium",
        market_kind="pool",
        identity_status="resolved",
        source="fixture",
    )
    for i in range(3):
        ts = T0 + timedelta(minutes=i)
        await repo.insert_observation(
            market_id=market.id,
            source_native_observation_id=f"obs-ovf-{i}",
            source="fixture",
            source_time=ts,
            received_time=ts + timedelta(seconds=1),
            availability_status="received",
            price=Decimal("1"),
        )

    with pytest.raises(ValueError, match="overflow"):
        await repo.list_observations_for_market(
            market_id=market.id,
            start=T0,
            end=T0 + timedelta(hours=1),
            limit=2,
        )
    with pytest.raises(ValueError, match="limit"):
        await repo.list_observations_for_market(
            market_id=market.id,
            start=T0,
            end=T0 + timedelta(hours=1),
            limit=0,
        )
    with pytest.raises(ValueError, match="market_id"):
        await repo.list_observations_for_market(
            market_id=0,
            start=T0,
            end=T0 + timedelta(hours=1),
            limit=10,
        )


@pytest.mark.asyncio
async def test_associated_legacy_spot_listing_without_market_projects_token_only(
    session: AsyncSession,
) -> None:
    token = await TokenRepository(session).upsert(
        chain="binance",
        token_address="NEWUSDT",
        symbol="NEWUSDT",
        created_time=T0,
        first_seen_time=T0,
        source="binance",
        venue="binance",
    )
    repo = EarlyMarketEventRepository(session)
    await repo.insert_event(
        _domain_event(
            event_id="legacy-spot",
            event_kind=EarlyMarketEventKind.BINANCE_SPOT_LISTING,
            source="binance",
            venue_or_protocol="binance",
            chain="binance",
            asset_identity=AssetIdentity(chain="binance", asset_key="NEWUSDT", symbol="NEWUSDT"),
            market_identity=None,
            provenance_ref="prov://binance/legacy-spot",
        ),
        source_native_event_id="ann-frozen-1",
        asset_token=token,
        market=None,
    )
    rows = await repo.list_associated_events(
        start=T0,
        end=T0 + timedelta(hours=1),
        limit=100,
    )
    legacy = [row for row in rows if row.event.source_native_event_id == "ann-frozen-1"]
    assert len(legacy) == 1
    assert legacy[0].market is None
    projected = project_early_market_event(
        legacy[0].event,
        token=legacy[0].token,
        market=legacy[0].market,
    )
    assert projected.pair_address is None
    assert projected.provenance["market_association_reason"] == "legacy_binance_spot_listing_token_only"
    assert projected.event_id == "binance:binance:NEWUSDT:ann-frozen-1"
    assert projected.source == "binance:cms:catalog48"
    assert projected.provenance["event_clock_field"] == "announced_spot_trading_start"
