"""PostgreSQL repository integration tests (skipped without test DB URL)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from newcoin_trader.database.base import Base
from newcoin_trader.database.repositories.market import MarketRepository
from newcoin_trader.database.repositories.paper import PaperTradeRepository
from newcoin_trader.database.repositories.strategy import StrategyResultRepository
from newcoin_trader.database.repositories.tokens import TokenRepository
from newcoin_trader.domain.enums import PaperStatus, Side
from tests.integration._postgres import get_test_database_url

pytestmark = pytest.mark.integration

DATABASE_URL = get_test_database_url()


def _postgres_available() -> bool:
    return DATABASE_URL.startswith("postgresql")


@pytest.fixture
async def session() -> AsyncIterator[AsyncSession]:
    if not _postgres_available():
        pytest.skip("PostgreSQL not configured")
    engine = create_async_engine(DATABASE_URL)
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


@pytest.mark.asyncio
async def test_token_upsert_preserves_earliest_first_seen(session: AsyncSession) -> None:
    repo = TokenRepository(session)
    early = datetime(2024, 1, 1, tzinfo=UTC)
    late = datetime(2024, 2, 1, tzinfo=UTC)
    first = await repo.upsert(
        chain="solana",
        token_address="Mint111",
        symbol="MEME",
        created_time=early,
        first_seen_time=early,
        source="birdeye",
    )
    second = await repo.upsert(
        chain="solana",
        token_address="Mint111",
        symbol="MEME2",
        created_time=late,
        first_seen_time=late,
        source="birdeye",
    )
    await session.commit()
    assert first.id == second.id
    assert second.first_seen_time == early
    assert second.symbol == "MEME2"


@pytest.mark.asyncio
async def test_snapshot_insert_is_idempotent(session: AsyncSession) -> None:
    tokens = TokenRepository(session)
    token = await tokens.upsert(
        chain="binance",
        token_address="NEWUSDT",
        symbol="NEWUSDT",
        created_time=datetime(2024, 1, 1, tzinfo=UTC),
        first_seen_time=datetime(2024, 1, 1, tzinfo=UTC),
        source="binance",
    )
    market = MarketRepository(session)
    ts = datetime(2024, 1, 1, 1, tzinfo=UTC)
    kwargs = {
        "token_id": token.id,
        "timestamp": ts,
        "price": Decimal("1.1"),
        "volume": Decimal("10"),
        "liquidity": Decimal("1000"),
        "market_cap": Decimal("5000"),
        "buy_count": 3,
        "sell_count": 2,
        "source": "binance",
    }
    a = await market.upsert_snapshot(**kwargs)
    b = await market.upsert_snapshot(**kwargs)
    await session.commit()
    assert a.id == b.id


@pytest.mark.asyncio
async def test_paper_and_strategy_insert_are_idempotent(session: AsyncSession) -> None:
    tokens = TokenRepository(session)
    token = await tokens.upsert(
        chain="solana",
        token_address="Mint222",
        symbol="PEPE",
        created_time=None,
        first_seen_time=datetime(2024, 1, 1, tzinfo=UTC),
        source="demo",
    )
    paper = PaperTradeRepository(session)
    kwargs = {
        "run_id": "00000000-0000-0000-0000-000000000001",
        "token_id": token.id,
        "signal_ts": datetime(2024, 1, 1, tzinfo=UTC),
        "side": Side.BUY,
        "requested_qty": Decimal("1"),
        "requested_price": Decimal("1.01"),
        "fill_price": Decimal("1.01"),
        "fill_qty": Decimal("1"),
        "fee": Decimal("0.01"),
        "slippage_bps": Decimal("25"),
        "status": PaperStatus.FILLED,
        "reject_reason": None,
    }
    row_a = await paper.insert(**kwargs)
    row_b = await paper.insert(**kwargs)
    # Different limit price must not collide with the prior natural key.
    row_c = await paper.insert(
        **{
            **kwargs,
            "requested_price": Decimal("1.50"),
            "fill_price": Decimal("1.50"),
        }
    )
    results = StrategyResultRepository(session)
    result_kwargs = {
        "run_id": "00000000-0000-0000-0000-000000000001",
        "strategy_name": "listing_momentum",
        "strategy_version": "1.0.0",
        "token_id": token.id,
        "params": {"lookback_minutes": 15},
        "metrics": {"return": "0.1"},
        "signals": [{"kind": "buy"}],
        "window_start": datetime(2024, 1, 1, tzinfo=UTC),
        "window_end": datetime(2024, 1, 2, tzinfo=UTC),
    }
    saved_a = await results.insert(**result_kwargs)
    saved_b = await results.insert(**result_kwargs)
    # Nullable token_id duplicates must collide under NULLS NOT DISTINCT.
    null_kwargs = {
        **result_kwargs,
        "token_id": None,
        "window_start": None,
        "window_end": None,
    }
    null_a = await results.insert(**null_kwargs)
    null_b = await results.insert(**null_kwargs)
    await session.commit()
    assert row_a.id == row_b.id
    assert row_c.id != row_a.id
    assert row_a.mode == "paper"
    assert saved_a.id == saved_b.id
    assert null_a.id == null_b.id
    assert saved_a.strategy_name == "listing_momentum"


@pytest.mark.asyncio
async def test_live_paper_repository_three_run_durable_idempotency(session: AsyncSession) -> None:
    """PG-backed Run1/Run2/Run3: empty current report must not wipe seen IDs."""
    import tempfile
    from datetime import timedelta
    from pathlib import Path

    from newcoin_trader.database.repositories.live_paper import LivePaperRepository
    from newcoin_trader.domain.enums import Chain, Side, Venue
    from newcoin_trader.domain.event_study import ObservationResolution, TokenListingEvent
    from newcoin_trader.domain.executable_backtest import (
        DepthLevel,
        FrozenCandidateIdentity,
        HistoricalDepthBook,
    )
    from newcoin_trader.domain.feature_research import RuleCondition
    from newcoin_trader.domain.live_paper import LivePaperStatus, ReplayMarketEvent
    from newcoin_trader.research.live_paper_engine import process_live_paper_session
    from newcoin_trader.services.live_paper import LivePaperService

    t0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

    def listing() -> TokenListingEvent:
        return TokenListingEvent(
            event_id="e1",
            venue=Venue.BINANCE,
            chain=Chain.BINANCE,
            token_address="TOKEN",
            pair_address="PAIR",
            symbol="TOK",
            source="binance",
            source_event_time=t0,
            first_seen_time=t0,
            first_market_data_time=t0,
            decision_available_time=t0,
            provenance={"token_id": "1"},
        )

    def listing_event(item: TokenListingEvent) -> ReplayMarketEvent:
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

    def market(*, ts: datetime, price: str, depth: HistoricalDepthBook | None = None) -> ReplayMarketEvent:
        return ReplayMarketEvent(
            event_id="e1",
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
            depth=depth,
            provenance={"kind": "trade"},
        )

    decision = t0 + timedelta(minutes=1)
    exit_ts = decision + timedelta(minutes=5)
    lst = listing()
    events = [
        listing_event(lst),
        market(ts=decision, price="10"),
        market(ts=exit_ts, price="12"),
    ]
    identity = FrozenCandidateIdentity(
        rule_id="frozen-rule-1",
        conditions=(RuleCondition(feature_name="age_source_event_seconds", op="gte", threshold=Decimal("0")),),
        human_readable="age_source_event_seconds gte 0",
        phase4_config_id="cfg-phase4",
        split_label="test",
        fold_index=0,
        provenance={"source": "frozen_phase4"},
    )
    repo = LivePaperRepository(session)
    service = LivePaperService(session=session)

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        kwargs = dict(
            events=events,
            identity=identity,
            venue="binance",
            duration=timedelta(hours=1),
            max_events=50,
            max_signals=20,
            max_trades=20,
            queue_capacity=100,
            starting_cash=Decimal("10000"),
            session_start=t0,
            position_notional=Decimal("100"),
            holding_period=timedelta(minutes=5),
        )
        r1, _ = await service.run_replay(**kwargs, output_dir=out / "r1")
        await session.commit()
        state1 = await repo.load_session_state(
            venue=Venue.BINANCE.value,
            rule_id=identity.rule_id,
            phase4_config_id=identity.phase4_config_id,
            session_start=t0,
        )
        assert state1.get("seen_signals")
        assert state1.get("seen_fills")
        pnl1 = r1.portfolio.realized_pnl

        r2, _ = await service.run_replay(**kwargs, output_dir=out / "r2")
        await session.commit()
        state2 = await repo.load_session_state(
            venue=Venue.BINANCE.value,
            rule_id=identity.rule_id,
            phase4_config_id=identity.phase4_config_id,
            session_start=t0,
        )
        assert set(state2.get("seen_signals", [])) >= set(state1.get("seen_signals", []))
        assert set(state2.get("seen_fills", [])) >= set(state1.get("seen_fills", []))
        assert r2.portfolio.realized_pnl == pnl1

        r3, _ = await service.run_replay(**kwargs, output_dir=out / "r3")
        await session.commit()
        state3 = await repo.load_session_state(
            venue=Venue.BINANCE.value,
            rule_id=identity.rule_id,
            phase4_config_id=identity.phase4_config_id,
            session_start=t0,
        )
        assert set(state3.get("seen_signals", [])) >= set(state2.get("seen_signals", []))
        assert r3.portfolio.realized_pnl == pnl1
        assert len([s for s in r3.signals if s.status is LivePaperStatus.SIGNAL_ACCEPTED]) == 0

    book_entry = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=decision,
        bids=(DepthLevel(price=Decimal("9.9"), quantity=Decimal("100")),),
        asks=(DepthLevel(price=Decimal("10"), quantity=Decimal("10")),),
        source="binance:depth",
    )
    book_partial = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=exit_ts,
        bids=(DepthLevel(price=Decimal("11"), quantity=Decimal("5")),),
        asks=(DepthLevel(price=Decimal("11.1"), quantity=Decimal("100")),),
        source="binance:depth",
    )
    partial_report = process_live_paper_session(
        events=[
            listing_event(lst),
            market(ts=decision, price="10", depth=book_entry),
            market(ts=exit_ts, price="11", depth=book_partial),
        ],
        venue=Venue.BINANCE,
        session_start=t0,
        duration=timedelta(hours=1),
        max_events=50,
        max_signals=20,
        max_trades=20,
        queue_capacity=100,
        starting_cash=Decimal("10000"),
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
        identity=identity,
        max_token_exposure=Decimal("5000"),
    )
    await repo.persist_report(partial_report)
    await session.commit()
    assert partial_report.positions
    assert partial_report.positions[-1].remaining_qty is not None
    assert partial_report.positions[-1].remaining_qty > 0
    assert any(f.side is Side.SELL for f in partial_report.fills)
