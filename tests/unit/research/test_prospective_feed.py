"""Phase 6.5 bounded prospective read-only feed: Binance Spot, mock transport only."""

from __future__ import annotations

import ast
import inspect
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from newcoin_trader.cli.main import app
from newcoin_trader.domain.enums import Chain, Side, Venue
from newcoin_trader.domain.event_study import ObservationResolution, TokenListingEvent
from newcoin_trader.domain.executable_backtest import (
    DepthLevel,
    ExecutionConfidence,
    FrozenCandidateIdentity,
    HistoricalDepthBook,
    SimulatedFillMode,
)
from newcoin_trader.domain.feature_research import RuleCondition
from newcoin_trader.domain.live_paper import (
    WARNING_MODELED,
    LivePaperRejectReason,
    LivePaperStatus,
    ReplayMarketEvent,
)
from newcoin_trader.domain.market import OrderBookL2, OrderBookLevel, Ticker24h, TradeTick
from newcoin_trader.domain.tokens import NewListingEvent
from newcoin_trader.errors import CollectorError, ConfigError, ParseError
from newcoin_trader.research.live_paper_engine import process_live_paper_session
from newcoin_trader.research.prospective_binance import (
    BinanceProspectiveFeed,
    listing_event_id,
    trade_event_id,
)
from newcoin_trader.research.prospective_feed import (
    ProspectiveFeedResult,
    ProspectiveFeedStatus,
    build_prospective_feed,
    validate_prospective_feed_bounds,
)
from newcoin_trader.services.live_paper import LivePaperService

runner = CliRunner()

T0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
SOURCE_T = datetime(2024, 1, 1, 11, 0, 0, tzinfo=UTC)
SYMBOL = "NEWUSDT"


def _identity() -> FrozenCandidateIdentity:
    cond = RuleCondition(feature_name="age_source_event_seconds", op="gte", threshold=Decimal("0"))
    return FrozenCandidateIdentity(
        rule_id="frozen-rule-1",
        conditions=(cond,),
        human_readable="age_source_event_seconds gte 0",
        phase4_config_id="cfg-phase4",
        split_label="test",
        fold_index=0,
        provenance={"source": "frozen_phase4"},
    )


def _listing_event(
    *,
    symbol: str = SYMBOL,
    onboard: datetime | None = SOURCE_T,
    first_seen: datetime = SOURCE_T,
) -> NewListingEvent:
    return NewListingEvent(
        token_address=symbol,
        chain=Chain.BINANCE,
        symbol=symbol,
        name="NEW",
        created_time=onboard,
        first_seen_time=first_seen,
        source="binance",
        venue=Venue.BINANCE,
        provenance={"endpoint": "/api/v3/exchangeInfo"},
    )


def _trade(
    *,
    trade_id: str,
    ts: datetime,
    price: str = "1.10",
    qty: str = "2.0",
    symbol: str = SYMBOL,
) -> TradeTick:
    return TradeTick(
        token_address=symbol,
        chain=Chain.BINANCE.value,
        timestamp=ts,
        side=Side.SELL,
        amount=Decimal(qty),
        price=Decimal(price),
        external_trade_id=trade_id,
        source="binance:trades",
        provenance={"kind": "trade", "endpoint": "/api/v3/trades"},
    )


def _book(*, symbol: str = SYMBOL, ts: datetime = T0) -> OrderBookL2:
    return OrderBookL2(
        token_address=symbol,
        chain=Chain.BINANCE.value,
        timestamp=ts,
        bids=(OrderBookLevel(price=Decimal("1.09"), quantity=Decimal("20")),),
        asks=(OrderBookLevel(price=Decimal("1.11"), quantity=Decimal("15")),),
        last_update_id=1027024,
        source="binance",
    )


def _ticker(*, symbol: str = SYMBOL, ts: datetime = SOURCE_T) -> Ticker24h:
    return Ticker24h(
        token_address=symbol,
        chain=Chain.BINANCE.value,
        timestamp=ts,
        last_price=Decimal("1.10"),
        volume=Decimal("1000"),
        quote_volume=Decimal("1100"),
        source="binance",
    )


class FakeClock:
    def __init__(self, start: datetime = T0, step: timedelta = timedelta(seconds=1)) -> None:
        self._now = start
        self._step = step
        self.reads: list[datetime] = []

    def __call__(self) -> datetime:
        self.reads.append(self._now)
        current = self._now
        self._now = self._now + self._step
        return current

    def peek(self) -> datetime:
        return self._now


class FakeBinanceClient:
    """Injected collector stand-in: only public GET-shaped methods."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.listings: list[NewListingEvent] = [_listing_event()]
        self.trades: list[TradeTick] = [_trade(trade_id="28457", ts=SOURCE_T)]
        self.ticker: Ticker24h = _ticker()
        self.book: OrderBookL2 = _book()
        self.fail_exchange = False
        self.fail_trades: Exception | None = None
        self.trade_sequences: list[list[TradeTick]] | None = None
        self._trade_poll = 0

    async def exchange_info(self) -> list[NewListingEvent]:
        self.calls.append("exchange_info")
        if self.fail_exchange:
            raise CollectorError("exchange_info unavailable")
        return list(self.listings)

    async def recent_trades(self, symbol: str, *, limit: int = 500) -> list[TradeTick]:
        self.calls.append(f"recent_trades:{symbol}:{limit}")
        if self.fail_trades is not None:
            raise self.fail_trades
        if self.trade_sequences is not None:
            idx = min(self._trade_poll, len(self.trade_sequences) - 1)
            self._trade_poll += 1
            return list(self.trade_sequences[idx])
        return list(self.trades)

    async def ticker_24h(self, symbol: str) -> Ticker24h:
        self.calls.append(f"ticker_24h:{symbol}")
        return self.ticker

    async def order_book(self, symbol: str, *, limit: int = 100) -> OrderBookL2:
        self.calls.append(f"order_book:{symbol}:{limit}")
        return self.book


def _bounds(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "symbol": SYMBOL,
        "poll_interval": timedelta(seconds=0),
        "duration": timedelta(seconds=5),
        "max_polls": 2,
        "max_events": 50,
        "max_observations_per_token": 20,
        "max_total_observations": 40,
        "queue_capacity": 100,
    }
    base.update(overrides)
    return base


def _feed(
    client: FakeBinanceClient | None = None,
    clock: FakeClock | None = None,
    **bound_overrides: Any,
) -> BinanceProspectiveFeed:
    return BinanceProspectiveFeed(
        client=client or FakeBinanceClient(),
        now=clock or FakeClock(),
        sleep=_async_noop_sleep,
        **_bounds(**bound_overrides),
    )


async def _async_noop_sleep(_seconds: float) -> None:
    return None


# ---------------------------------------------------------------------------
# Structural safety
# ---------------------------------------------------------------------------


def test_prospective_modules_are_get_only_no_write_trading_wallet() -> None:
    root = Path(__file__).resolve().parents[3] / "src" / "newcoin_trader" / "research"
    paths = [
        root / "prospective_feed.py",
        root / "prospective_binance.py",
        root / "prospective_capabilities.py",
    ]
    forbidden_substrings = (
        "private_key",
        "mnemonic",
        "secret_key",
        "api_secret",
        "wallet",
        "withdraw",
        "transfer",
        "sign_transaction",
        "live_trading",
        "place_order",
        "create_order",
        "x-mbx-apikey",
        ".post(",
        ".put(",
        ".patch(",
        ".delete(",
    )
    for path in paths:
        text = path.read_text(encoding="utf-8")
        lower = text.lower()
        for token in forbidden_substrings:
            assert token not in lower, f"{path.name} contains {token}"
        tree = ast.parse(text)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                assert node.func.attr not in {"post", "put", "patch", "delete", "request"}
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                assert node.value.upper() not in {"POST", "PUT", "PATCH", "DELETE"}


def test_prospective_feed_uses_binance_client_public_get_methods_only() -> None:
    src = Path(__file__).resolve().parents[3] / "src" / "newcoin_trader" / "research" / "prospective_binance.py"
    text = src.read_text(encoding="utf-8")
    assert "exchange_info" in text
    assert "recent_trades" in text
    assert "ticker_24h" in text
    assert "order_book" in text
    assert "agg_trades" not in text or "recent_trades" in text
    assert "/api/v3/order" not in text
    assert "apiKey" not in text
    assert "X-MBX-APIKEY" not in text


def test_unsupported_prospective_venue_raises_config_error_no_replay_fallback() -> None:
    with pytest.raises(ConfigError, match="unsupported prospective venue"):
        build_prospective_feed(
            venue="raydium",
            client=FakeBinanceClient(),
            now=FakeClock(),
            **_bounds(),
        )
    with pytest.raises(ConfigError, match="unsupported prospective venue"):
        build_prospective_feed(
            venue="birdeye",
            client=FakeBinanceClient(),
            now=FakeClock(),
            **_bounds(),
        )
    with pytest.raises(ConfigError, match="unsupported prospective venue"):
        build_prospective_feed(
            venue="geckoterminal",
            client=FakeBinanceClient(),
            now=FakeClock(),
            **_bounds(),
        )


def test_validate_prospective_bounds_require_explicit_caps() -> None:
    validate_prospective_feed_bounds(
        poll_interval=timedelta(seconds=1),
        duration=timedelta(minutes=5),
        max_polls=10,
        max_events=50,
        max_observations_per_token=20,
        max_total_observations=40,
        queue_capacity=100,
    )
    with pytest.raises(ConfigError):
        validate_prospective_feed_bounds(
            poll_interval=timedelta(seconds=-1),
            duration=timedelta(minutes=5),
            max_polls=10,
            max_events=50,
            max_observations_per_token=20,
            max_total_observations=40,
            queue_capacity=100,
        )
    with pytest.raises(ConfigError):
        validate_prospective_feed_bounds(
            poll_interval=timedelta(seconds=1),
            duration=timedelta(0),
            max_polls=10,
            max_events=50,
            max_observations_per_token=20,
            max_total_observations=40,
            queue_capacity=100,
        )


# ---------------------------------------------------------------------------
# Receipt clock vs source clock
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_received_timestamp_is_local_clock_not_source() -> None:
    # Local receipt clock is deliberately offset from Binance source trade time.
    clock = FakeClock(start=T0 + timedelta(hours=2), step=timedelta(seconds=1))
    client = FakeBinanceClient()
    client.trades = [_trade(trade_id="1", ts=SOURCE_T)]
    result = await _feed(client=client, clock=clock, max_polls=1).collect_bounded()
    assert result.status in {
        ProspectiveFeedStatus.OK,
        ProspectiveFeedStatus.BOUNDS_REACHED,
    }
    markets = [e for e in result.events if e.kind == "market"]
    assert markets
    for event in markets:
        assert event.source_timestamp == SOURCE_T
        assert event.received_timestamp != event.source_timestamp
        assert event.received_timestamp >= T0 + timedelta(hours=2)
    listings = [e for e in result.events if e.kind == "listing"]
    assert listings
    assert listings[0].received_timestamp != listings[0].source_timestamp


# ---------------------------------------------------------------------------
# Stable IDs / duplicate suppression / same-T distinct
# ---------------------------------------------------------------------------


def test_listing_and_trade_ids_are_deterministic_and_stable() -> None:
    onboard = datetime(2024, 1, 1, tzinfo=UTC)
    a = listing_event_id(source="binance", symbol=SYMBOL, onboard=onboard, venue=Venue.BINANCE)
    b = listing_event_id(source="binance", symbol=SYMBOL, onboard=onboard, venue=Venue.BINANCE)
    assert a == b
    assert SYMBOL in a
    assert "binance" in a
    t1 = trade_event_id(source="binance", trade_id="28457")
    t2 = trade_event_id(source="binance", trade_id="28457")
    assert t1 == t2
    assert "28457" in t1
    assert t1 != trade_event_id(source="binance", trade_id="28458")


@pytest.mark.asyncio
async def test_duplicate_poll_same_trade_keeps_same_id_and_suppresses_extra() -> None:
    client = FakeBinanceClient()
    trade = _trade(trade_id="99", ts=SOURCE_T)
    client.trade_sequences = [[trade], [trade]]
    # Non-advancing clock so duration/max_polls alone bound the loop.
    clock = FakeClock(start=T0, step=timedelta(0))
    feed = _feed(client=client, clock=clock, max_polls=2, poll_interval=timedelta(0), duration=timedelta(hours=1))
    result = await feed.collect_bounded()
    markets = [e for e in result.events if e.kind == "market"]
    ids = [e.event_id for e in markets]
    assert ids.count(trade_event_id(source="binance", trade_id="99")) == 1
    assert result.duplicate_suppressed_count >= 1
    assert result.poll_count == 2


@pytest.mark.asyncio
async def test_same_timestamp_distinct_trade_ids_remain_distinct() -> None:
    client = FakeBinanceClient()
    ts = SOURCE_T
    client.trades = [
        _trade(trade_id="100", ts=ts, price="1.10"),
        _trade(trade_id="101", ts=ts, price="1.11"),
    ]
    result = await _feed(client=client, max_polls=1).collect_bounded()
    markets = [e for e in result.events if e.kind == "market"]
    assert len(markets) == 2
    assert markets[0].event_id != markets[1].event_id
    assert markets[0].source_timestamp == markets[1].source_timestamp


# ---------------------------------------------------------------------------
# Bounds / overflow
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bounded_polling_respects_max_polls_and_duration() -> None:
    client = FakeBinanceClient()
    clock = FakeClock(start=T0, step=timedelta(seconds=2))
    feed = _feed(
        client=client,
        clock=clock,
        max_polls=3,
        duration=timedelta(seconds=3),
        poll_interval=timedelta(0),
    )
    result = await feed.collect_bounded()
    assert result.poll_count <= 3
    # duration ~3s with 2s clock steps ⇒ stops early
    assert result.poll_count <= 2
    assert "exchange_info" in client.calls


@pytest.mark.asyncio
async def test_max_events_and_observation_caps_are_auditable() -> None:
    client = FakeBinanceClient()
    client.trades = [_trade(trade_id=str(i), ts=SOURCE_T + timedelta(milliseconds=i)) for i in range(10)]
    result = await _feed(
        client=client,
        max_polls=1,
        max_events=3,
        max_total_observations=2,
        max_observations_per_token=2,
    ).collect_bounded()
    assert len(result.events) <= 3
    market_count = sum(1 for e in result.events if e.kind == "market")
    assert market_count <= 2
    assert result.rejected_count >= 1 or result.status is ProspectiveFeedStatus.BOUNDS_REACHED


@pytest.mark.asyncio
async def test_queue_overflow_is_explicit_not_silent() -> None:
    client = FakeBinanceClient()
    client.trades = [_trade(trade_id=str(i), ts=SOURCE_T + timedelta(milliseconds=i)) for i in range(5)]
    clock = FakeClock(start=T0, step=timedelta(0))
    result = await _feed(
        client=client,
        clock=clock,
        max_polls=1,
        queue_capacity=2,
        max_events=50,
        max_total_observations=50,
        max_observations_per_token=50,
        duration=timedelta(hours=1),
    ).collect_bounded()
    assert result.overflow_count >= 1
    assert len(result.events) <= 2
    assert result.status in {
        ProspectiveFeedStatus.QUEUE_OVERFLOW,
        ProspectiveFeedStatus.BOUNDS_REACHED,
        ProspectiveFeedStatus.OK,
    }


# ---------------------------------------------------------------------------
# Failure / malformed — no fabricated input
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_source_failure_is_explicit_source_unavailable_no_fake_price() -> None:
    client = FakeBinanceClient()
    client.fail_exchange = True
    result = await _feed(client=client, max_polls=1).collect_bounded()
    assert result.status is ProspectiveFeedStatus.SOURCE_UNAVAILABLE
    assert result.events == ()
    assert result.source_errors
    assert all(e.price is None or e.kind != "market" for e in result.events)


@pytest.mark.asyncio
async def test_malformed_trade_payload_does_not_fabricate_market_events() -> None:
    client = FakeBinanceClient()
    client.fail_trades = ParseError("trade malformed")
    result = await _feed(client=client, max_polls=1).collect_bounded()
    assert result.status is ProspectiveFeedStatus.SOURCE_UNAVAILABLE
    assert not any(e.kind == "market" for e in result.events)
    assert result.source_errors


@pytest.mark.asyncio
async def test_missing_configured_symbol_is_source_unavailable() -> None:
    client = FakeBinanceClient()
    client.listings = [_listing_event(symbol="OTHERUSDT")]
    result = await _feed(client=client, max_polls=1, symbol=SYMBOL).collect_bounded()
    assert result.status is ProspectiveFeedStatus.SOURCE_UNAVAILABLE
    assert result.events == ()


# ---------------------------------------------------------------------------
# Normalization / depth / provenance
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_normalizes_to_replay_market_event_with_point_resolution_and_depth() -> None:
    result = await _feed(max_polls=1).collect_bounded()
    assert result.events
    listing = next(e for e in result.events if e.kind == "listing")
    assert isinstance(listing, ReplayMarketEvent)
    assert listing.listing is not None
    assert listing.venue is Venue.BINANCE
    assert listing.resolution is ObservationResolution.POINT or listing.kind == "listing"
    market = next(e for e in result.events if e.kind == "market")
    assert market.resolution is ObservationResolution.POINT
    assert market.price == Decimal("1.10")
    assert market.depth is not None
    assert market.depth.token_address == SYMBOL
    assert market.provenance.get("external_trade_id") == "28457"
    assert "binance" in market.source


# ---------------------------------------------------------------------------
# Parity + service same engine
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prospective_replay_parity_identical_normalized_sequence(tmp_path: Path) -> None:
    client = FakeBinanceClient()
    client.trades = [
        _trade(trade_id="1", ts=T0 + timedelta(minutes=1), price="10"),
        _trade(trade_id="2", ts=T0 + timedelta(minutes=6), price="11"),
    ]
    client.book = OrderBookL2(
        token_address=SYMBOL,
        chain=Chain.BINANCE.value,
        timestamp=T0,
        bids=(OrderBookLevel(price=Decimal("9.9"), quantity=Decimal("100000")),),
        asks=(OrderBookLevel(price=Decimal("10.1"), quantity=Decimal("100000")),),
        last_update_id=1,
        source="binance",
    )
    # Align listing clocks so Phase 6 decision_delay can see trades.
    client.listings = [_listing_event(onboard=T0, first_seen=T0)]
    clock = FakeClock(start=T0, step=timedelta(0))
    feed = _feed(client=client, clock=clock, max_polls=1)
    feed_result = await feed.collect_bounded()
    events = feed_result.events
    assert events

    kwargs = dict(
        venue=Venue.BINANCE,
        session_start=T0,
        duration=timedelta(hours=1),
        max_events=50,
        max_signals=20,
        max_trades=20,
        queue_capacity=100,
        starting_cash=Decimal("10000"),
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
        identity=_identity(),
        decision_delay=timedelta(minutes=1),
        assumed_fee_bps=Decimal("10"),
    )
    direct = process_live_paper_session(events=events, **kwargs)
    via_replay = process_live_paper_session(events=events, **kwargs)
    assert direct.model_dump() == via_replay.model_dump()

    service = LivePaperService()

    class _StaticFeed:
        async def collect_bounded(self) -> ProspectiveFeedResult:
            return feed_result

    report, _paths = await service.run_prospective(
        feed=_StaticFeed(),
        identity=_identity(),
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
        assumed_fee_bps=Decimal("10"),
    )
    assert report.meta.phase == "phase_6_live_paper"
    assert report.extras.get("prospective_feed", {}).get("status") == feed_result.status.value


@pytest.mark.asyncio
async def test_run_prospective_delegates_to_run_replay_same_engine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = LivePaperService()
    calls: list[str] = []
    events = (
        ReplayMarketEvent(
            event_id="listing-1",
            kind="listing",
            venue=Venue.BINANCE,
            token_address=SYMBOL,
            chain="binance",
            source_timestamp=T0,
            received_timestamp=T0,
            source="binance",
            listing=None,
        ),
    )

    class _Feed:
        async def collect_bounded(self) -> ProspectiveFeedResult:
            calls.append("collect")
            return ProspectiveFeedResult(
                events=events,
                status=ProspectiveFeedStatus.OK,
                poll_count=1,
                overflow_count=0,
                rejected_count=0,
                duplicate_suppressed_count=0,
                source_errors=(),
            )

    original = service.run_replay

    async def _wrapped(**kwargs: Any) -> Any:
        calls.append("run_replay")
        assert kwargs["events"] == events
        return await original(**kwargs)

    monkeypatch.setattr(service, "run_replay", _wrapped)
    await service.run_prospective(
        feed=_Feed(),
        identity=_identity(),
        venue="binance",
        duration=timedelta(hours=1),
        max_events=50,
        max_signals=20,
        max_trades=20,
        queue_capacity=100,
        starting_cash=Decimal("10000"),
        output_dir=tmp_path,
        session_start=T0,
    )
    assert calls == ["collect", "run_replay"]


def test_live_paper_service_run_prospective_signature_has_no_duplicate_engine() -> None:
    src = inspect.getsource(LivePaperService.run_prospective)
    assert "run_replay" in src
    assert "process_live_paper_session" not in src


# ---------------------------------------------------------------------------
# CLI bounds / mutual exclusion / unsupported venue
# ---------------------------------------------------------------------------


def test_cli_prospective_and_replay_are_mutually_exclusive(tmp_path: Path) -> None:
    both = runner.invoke(
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
            "r1",
            "--phase4-config-id",
            "c1",
            "--paper-starting-cash",
            "10000",
            "--output-dir",
            str(tmp_path),
            "--session-start",
            T0.isoformat(),
            "--replay-path",
            str(tmp_path / "replay.json"),
            "--mode",
            "prospective",
            "--poll-interval",
            "1s",
            "--symbol",
            SYMBOL,
        ],
    )
    assert both.exit_code != 0

    neither = runner.invoke(
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
            "r1",
            "--phase4-config-id",
            "c1",
            "--paper-starting-cash",
            "10000",
            "--output-dir",
            str(tmp_path),
            "--session-start",
            T0.isoformat(),
        ],
    )
    assert neither.exit_code != 0


def test_cli_prospective_requires_poll_interval_and_rejects_unsupported_venue(tmp_path: Path) -> None:
    missing_poll = runner.invoke(
        app,
        [
            "live-paper",
            "--mode",
            "prospective",
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
            "r1",
            "--phase4-config-id",
            "c1",
            "--paper-starting-cash",
            "10000",
            "--output-dir",
            str(tmp_path),
            "--session-start",
            T0.isoformat(),
            "--symbol",
            SYMBOL,
            "--max-polls",
            "2",
            "--max-observations-per-token",
            "10",
            "--max-total-observations",
            "20",
        ],
    )
    assert missing_poll.exit_code != 0

    bad_venue = runner.invoke(
        app,
        [
            "live-paper",
            "--mode",
            "prospective",
            "--venue",
            "raydium",
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
            "r1",
            "--phase4-config-id",
            "c1",
            "--paper-starting-cash",
            "10000",
            "--output-dir",
            str(tmp_path),
            "--session-start",
            T0.isoformat(),
            "--poll-interval",
            "1s",
            "--symbol",
            SYMBOL,
            "--max-polls",
            "2",
            "--max-observations-per-token",
            "10",
            "--max-total-observations",
            "20",
        ],
    )
    assert bad_venue.exit_code != 0
    assert "unsupported" in bad_venue.output.lower() or bad_venue.exit_code == 2


# ---------------------------------------------------------------------------
# Phase 6.5 depth PIT: attached later L2 must not exact_depth before its clocks
# ---------------------------------------------------------------------------


def _token_listing(*, source: datetime = T0) -> TokenListingEvent:
    return TokenListingEvent(
        event_id="listing-newusdt",
        venue=Venue.BINANCE,
        chain=Chain.BINANCE,
        token_address=SYMBOL,
        pair_address="PAIR",
        symbol=SYMBOL,
        source="binance",
        source_event_time=source,
        first_seen_time=source,
        first_market_data_time=source,
        decision_available_time=source,
        provenance={"token_id": "1"},
    )


def _replay_listing(listing: TokenListingEvent, *, received: datetime | None = None) -> ReplayMarketEvent:
    return ReplayMarketEvent(
        event_id=listing.event_id,
        kind="listing",
        venue=listing.venue,
        token_address=listing.token_address,
        chain=listing.chain.value,
        source_timestamp=listing.source_event_time,
        received_timestamp=received or listing.source_event_time,
        source=listing.source,
        listing=listing,
        provenance=dict(listing.provenance),
    )


def _replay_market(
    *,
    event_id: str,
    ts: datetime,
    received: datetime | None = None,
    price: str = "10",
    liquidity: str = "100000",
    depth: HistoricalDepthBook | None = None,
    provenance: dict[str, str] | None = None,
) -> ReplayMarketEvent:
    return ReplayMarketEvent(
        event_id=event_id,
        kind="market",
        venue=Venue.BINANCE,
        token_address=SYMBOL,
        chain=Chain.BINANCE.value,
        source_timestamp=ts,
        received_timestamp=received or ts,
        price=Decimal(price),
        liquidity=Decimal(liquidity),
        volume=Decimal("1000"),
        resolution=ObservationResolution.POINT,
        source="binance:trades",
        depth=depth,
        provenance=provenance or {"kind": "trade"},
    )


def _attached_depth(
    *,
    received: datetime,
    source: datetime | None = None,
    ask: str = "99",
    bid: str = "1",
    qty: str = "100000",
) -> HistoricalDepthBook:
    provenance: dict[str, str] = {
        "endpoint": "/api/v3/depth",
        "last_update_id": "7",
        "depth_received_timestamp": received.isoformat(),
    }
    if source is not None:
        provenance["depth_source_timestamp"] = source.isoformat()
    return HistoricalDepthBook(
        token_address=SYMBOL,
        chain=Chain.BINANCE.value,
        venue=Venue.BINANCE,
        timestamp=received,
        bids=(DepthLevel(price=Decimal(bid), quantity=Decimal(qty)),),
        asks=(DepthLevel(price=Decimal(ask), quantity=Decimal(qty)),),
        source="binance:depth",
        provenance=provenance,
    )


def _pit_paper_kwargs(**overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = dict(
        venue=Venue.BINANCE,
        session_start=T0,
        duration=timedelta(hours=1),
        max_events=50,
        max_signals=20,
        max_trades=20,
        queue_capacity=100,
        starting_cash=Decimal("10000"),
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
        identity=_identity(),
        decision_delay=timedelta(minutes=1),
        assumed_fee_bps=Decimal("10"),
        freshness_max_age=timedelta(minutes=5),
        min_liquidity=Decimal("1000"),
        max_token_exposure=Decimal("5000"),
        max_venue_exposure=Decimal("5000"),
    )
    kwargs.update(overrides)
    return kwargs


def _run_pit_session(
    *,
    entry_depth: HistoricalDepthBook | None,
    exit_depth: HistoricalDepthBook | None,
    entry_ts: datetime | None = None,
    exit_ts: datetime | None = None,
    entry_received: datetime | None = None,
    exit_received: datetime | None = None,
    entry_provenance: dict[str, str] | None = None,
    **kwargs: Any,
) -> Any:
    listing = _token_listing()
    decision = T0 + timedelta(minutes=1)
    entry_at = entry_ts or decision
    exit_at = exit_ts or (decision + timedelta(minutes=5))
    events = [
        _replay_listing(listing),
        _replay_market(
            event_id="m-entry",
            ts=entry_at,
            received=entry_received or entry_at,
            depth=entry_depth,
            provenance=entry_provenance,
        ),
        _replay_market(
            event_id="m-exit",
            ts=exit_at,
            received=exit_received or exit_at,
            price="11",
            depth=exit_depth,
        ),
    ]
    return process_live_paper_session(events=events, **_pit_paper_kwargs(**kwargs))


@pytest.mark.asyncio
async def test_binance_depth_records_received_clock_and_does_not_invent_source() -> None:
    clock = FakeClock(start=T0, step=timedelta(seconds=1))
    client = FakeBinanceClient()
    client.book = _book(ts=SOURCE_T)
    result = await _feed(client=client, clock=clock, max_polls=1).collect_bounded()
    market = next(e for e in result.events if e.kind == "market")
    assert market.depth is not None
    prov = market.depth.provenance or {}
    assert "depth_received_timestamp" in prov
    assert "depth_source_timestamp" not in prov
    received = datetime.fromisoformat(prov["depth_received_timestamp"])
    assert market.depth.timestamp == received
    assert received == market.depth.timestamp
    assert received > market.received_timestamp
    assert received != SOURCE_T
    assert received != client.book.timestamp
    assert prov.get("last_update_id") == "1027024"
    assert prov.get("endpoint") == "/api/v3/depth"


@pytest.mark.asyncio
async def test_same_poll_attaches_later_depth_to_earlier_trade_without_invented_source() -> None:
    clock = FakeClock(start=T0, step=timedelta(seconds=1))
    result = await _feed(clock=clock, max_polls=1).collect_bounded()
    market = next(e for e in result.events if e.kind == "market")
    assert market.depth is not None
    assert market.depth.timestamp > market.received_timestamp
    assert "depth_source_timestamp" not in (market.depth.provenance or {})


def test_depth_received_after_entry_clock_is_not_exact_depth() -> None:
    decision = T0 + timedelta(minutes=1)
    future_book = _attached_depth(received=decision + timedelta(seconds=5), ask="99")
    report = _run_pit_session(entry_depth=future_book, exit_depth=None)
    buys = [f for f in report.fills if f.side is Side.BUY]
    assert buys
    assert all(f.mode is not SimulatedFillMode.EXACT_DEPTH for f in buys)
    assert all(f.confidence is not ExecutionConfidence.EXACT_DEPTH for f in buys)
    assert all(f.fill_price != Decimal("99") for f in buys)
    assert any("modeled" in f.label.lower() or f.confidence.value.startswith("modeled") for f in buys)
    assert any("depth_pit_rejected" in f.label for f in buys)
    assert report.data_quality.get("future_rejections", 0) >= 1


def test_depth_received_after_exit_clock_is_not_exact_depth() -> None:
    decision = T0 + timedelta(minutes=1)
    exit_at = decision + timedelta(minutes=5)
    entry_book = _attached_depth(received=decision, ask="10.1", bid="9.9")
    future_exit = _attached_depth(received=exit_at + timedelta(seconds=5), ask="50", bid="0.5")
    report = _run_pit_session(entry_depth=entry_book, exit_depth=future_exit)
    sells = [f for f in report.fills if f.side is Side.SELL]
    assert sells
    assert all(f.mode is not SimulatedFillMode.EXACT_DEPTH for f in sells)
    assert all(f.fill_price != Decimal("0.5") for f in sells)
    assert any("depth_pit_rejected" in f.label for f in sells)


def test_depth_received_equal_or_before_eligible_clock_may_be_exact_depth() -> None:
    decision = T0 + timedelta(minutes=1)
    exit_at = decision + timedelta(minutes=5)
    entry_book = _attached_depth(received=decision, ask="10.1", bid="9.9")
    exit_book = _attached_depth(received=exit_at, ask="11.1", bid="10.9")
    equal = _run_pit_session(entry_depth=entry_book, exit_depth=exit_book)
    assert equal.fills
    assert any(f.side is Side.BUY and f.mode is SimulatedFillMode.EXACT_DEPTH for f in equal.fills)
    assert any(f.side is Side.SELL and f.mode is SimulatedFillMode.EXACT_DEPTH for f in equal.fills)

    before = _run_pit_session(
        entry_depth=_attached_depth(received=decision - timedelta(seconds=1), ask="10.1", bid="9.9"),
        exit_depth=_attached_depth(received=exit_at - timedelta(seconds=1), ask="11.1", bid="10.9"),
    )
    assert any(f.mode is SimulatedFillMode.EXACT_DEPTH for f in before.fills)


def test_future_depth_attached_to_earlier_trade_cannot_affect_features_signal_or_fill() -> None:
    listing = _token_listing()
    decision = T0 + timedelta(minutes=1)
    exit_at = decision + timedelta(minutes=5)
    future_book = _attached_depth(received=decision + timedelta(minutes=2), ask="99", bid="0.01")
    poisoned = [
        _replay_listing(listing),
        _replay_market(event_id="m-entry", ts=decision, depth=future_book, liquidity="100000"),
        _replay_market(event_id="m-exit", ts=exit_at, price="11"),
    ]
    clean = [
        _replay_listing(listing),
        _replay_market(event_id="m-entry", ts=decision, depth=None, liquidity="100000"),
        _replay_market(event_id="m-exit", ts=exit_at, price="11"),
    ]
    poisoned_report = process_live_paper_session(events=poisoned, **_pit_paper_kwargs())
    clean_report = process_live_paper_session(events=clean, **_pit_paper_kwargs())
    assert poisoned_report.fills
    assert clean_report.fills
    assert all(f.mode is not SimulatedFillMode.EXACT_DEPTH for f in poisoned_report.fills)
    poison_buys = [f for f in poisoned_report.fills if f.side is Side.BUY]
    clean_buys = [f for f in clean_report.fills if f.side is Side.BUY]
    assert poison_buys and clean_buys
    assert poison_buys[0].fill_price == clean_buys[0].fill_price
    assert poison_buys[0].mode == clean_buys[0].mode
    poison_status = [s.status for s in poisoned_report.signals if s.status is LivePaperStatus.SIGNAL_ACCEPTED]
    clean_status = [s.status for s in clean_report.signals if s.status is LivePaperStatus.SIGNAL_ACCEPTED]
    assert poison_status == clean_status
    assert all(f.fill_price != Decimal("99") for f in poisoned_report.fills)


def test_session_outside_and_stale_depth_rejected_from_exact_depth() -> None:
    outside = _attached_depth(received=T0 + timedelta(hours=2), ask="99")
    outside_report = _run_pit_session(entry_depth=outside, exit_depth=None)
    buys = [f for f in outside_report.fills if f.side is Side.BUY]
    if buys:
        assert all(f.mode is not SimulatedFillMode.EXACT_DEPTH for f in buys)
        assert any("depth_pit_rejected" in f.label for f in buys)
    else:
        assert any(
            r.reason
            in {
                LivePaperRejectReason.SESSION_EXPIRED,
                LivePaperRejectReason.MIN_LIQUIDITY,
                LivePaperRejectReason.INSUFFICIENT_LIQUIDITY,
            }
            for r in outside_report.rejections
        )

    stale = _attached_depth(received=T0 - timedelta(minutes=20), ask="99")
    stale_report = _run_pit_session(
        entry_depth=stale,
        exit_depth=None,
        session_start=T0 - timedelta(minutes=30),
        duration=timedelta(hours=2),
    )
    stale_buys = [f for f in stale_report.fills if f.side is Side.BUY]
    assert stale_buys
    assert all(f.mode is not SimulatedFillMode.EXACT_DEPTH for f in stale_buys)
    assert any("depth_pit_rejected" in f.label for f in stale_buys)
    assert stale_report.data_quality.get("stale_rejections", 0) >= 1


def test_reliable_depth_source_timestamp_is_enforced_separately_from_received() -> None:
    decision = T0 + timedelta(minutes=1)
    # Received is eligible; source update is after the entry clock.
    leaked_source = _attached_depth(
        received=decision,
        source=decision + timedelta(seconds=30),
        ask="99",
    )
    report = _run_pit_session(entry_depth=leaked_source, exit_depth=None)
    buys = [f for f in report.fills if f.side is Side.BUY]
    assert buys
    assert all(f.mode is not SimulatedFillMode.EXACT_DEPTH for f in buys)
    assert all(f.fill_price != Decimal("99") for f in buys)


def test_later_exit_valid_depth_is_not_entry_valid_and_entry_depth_not_reused_stale_at_exit() -> None:
    listing = _token_listing()
    decision = T0 + timedelta(minutes=1)
    exit_at = decision + timedelta(minutes=5)
    future_at_entry = _attached_depth(received=exit_at, ask="99", bid="0.5")
    valid_at_exit = _attached_depth(received=exit_at, ask="11.1", bid="10.9")
    events = [
        _replay_listing(listing),
        _replay_market(event_id="m-entry", ts=decision, depth=future_at_entry),
        _replay_market(event_id="m-exit", ts=exit_at, price="11", depth=valid_at_exit),
    ]
    report = process_live_paper_session(events=events, **_pit_paper_kwargs())
    buys = [f for f in report.fills if f.side is Side.BUY]
    sells = [f for f in report.fills if f.side is Side.SELL]
    assert buys
    assert all(f.mode is not SimulatedFillMode.EXACT_DEPTH for f in buys)
    if sells:
        assert any(f.mode is SimulatedFillMode.EXACT_DEPTH for f in sells)

    # Entry-valid book that is stale at exit must not be reused on the exit event.
    entry_book = _attached_depth(received=decision, ask="10.1", bid="9.9")
    stale_exit_reuse = _attached_depth(received=decision, ask="11.1", bid="0.25")
    reused = process_live_paper_session(
        events=[
            _replay_listing(listing),
            _replay_market(event_id="m-entry", ts=decision, depth=entry_book),
            _replay_market(event_id="m-exit", ts=exit_at, price="11", depth=stale_exit_reuse),
        ],
        **_pit_paper_kwargs(freshness_max_age=timedelta(minutes=2)),
    )
    reused_sells = [f for f in reused.fills if f.side is Side.SELL]
    assert reused_sells
    assert all(f.mode is not SimulatedFillMode.EXACT_DEPTH for f in reused_sells)
    assert all(f.fill_price != Decimal("0.25") for f in reused_sells)


def test_modeled_fallback_retains_modeled_confidence_and_does_not_silent_downgrade_exact() -> None:
    decision = T0 + timedelta(minutes=1)
    future_book = _attached_depth(received=decision + timedelta(seconds=5), ask="99")
    report = _run_pit_session(entry_depth=future_book, exit_depth=None)
    buys = [f for f in report.fills if f.side is Side.BUY]
    assert buys
    entry = buys[0]
    assert entry.mode is SimulatedFillMode.MODELED_PRICE
    assert entry.confidence is ExecutionConfidence.MODELED_PRICE
    assert WARNING_MODELED in entry.label or "modeled" in entry.label.lower()
    assert "depth_pit_rejected" in entry.label
    assert "exact_depth" not in entry.label
    accepted = next(s for s in report.signals if s.status is LivePaperStatus.SIGNAL_ACCEPTED)
    assert accepted.provenance.get("fill_mode") == SimulatedFillMode.MODELED_PRICE.value
    assert accepted.provenance.get("depth_pit_rejected")


def test_depth_pit_preserves_phase6_partial_and_idempotent_replay_parity() -> None:
    listing = _token_listing()
    decision = T0 + timedelta(minutes=1)
    exit1 = decision + timedelta(minutes=5)
    exit2 = decision + timedelta(minutes=6)
    thin = _attached_depth(received=exit1, ask="11.1", bid="10.9", qty="1")
    rest = _attached_depth(received=exit2, ask="11.1", bid="10.9", qty="100000")
    entry_book = _attached_depth(received=decision, ask="10.1", bid="9.9")
    events = [
        _replay_listing(listing),
        _replay_market(event_id="m-entry", ts=decision, depth=entry_book),
        _replay_market(event_id="m-exit", ts=exit1, price="11", depth=thin),
        _replay_market(event_id="m-exit-2", ts=exit2, price="11", depth=rest),
    ]
    kwargs = _pit_paper_kwargs(position_notional=Decimal("100"), holding_period=timedelta(minutes=5))
    first_store: dict[str, Any] = {}
    first = process_live_paper_session(events=events, state_store=first_store, **kwargs)
    second = process_live_paper_session(events=events, state_store=first_store, **kwargs)
    replay = process_live_paper_session(events=events, **kwargs)
    assert first.fills
    assert {f.fill_id for f in second.fills}.isdisjoint({f.fill_id for f in first.fills}) or second.fills == ()
    assert replay.model_dump() == process_live_paper_session(events=events, **kwargs).model_dump()
    sells = [f for f in first.fills if f.side is Side.SELL]
    assert sells
    statuses = {f.status for f in sells}
    assert LivePaperStatus.EXIT_PARTIAL in statuses or LivePaperStatus.EXIT_FILLED in statuses


@pytest.mark.asyncio
async def test_prospective_normalized_depth_pit_matches_replay_of_same_events() -> None:
    client = FakeBinanceClient()
    client.listings = [_listing_event(onboard=T0, first_seen=T0)]
    client.trades = [
        _trade(trade_id="1", ts=T0 + timedelta(minutes=1), price="10"),
        _trade(trade_id="2", ts=T0 + timedelta(minutes=6), price="11"),
    ]
    client.book = OrderBookL2(
        token_address=SYMBOL,
        chain=Chain.BINANCE.value,
        timestamp=SOURCE_T,
        bids=(OrderBookLevel(price=Decimal("9.9"), quantity=Decimal("100000")),),
        asks=(OrderBookLevel(price=Decimal("10.1"), quantity=Decimal("100000")),),
        last_update_id=1,
        source="binance",
    )
    clock = FakeClock(start=T0, step=timedelta(seconds=1))
    feed_result = await _feed(client=client, clock=clock, max_polls=1).collect_bounded()
    events = feed_result.events
    kwargs = _pit_paper_kwargs()
    direct = process_live_paper_session(events=events, **kwargs)
    replay = process_live_paper_session(events=events, **kwargs)
    assert direct.model_dump() == replay.model_dump()
    later_than_entry = T0 + timedelta(minutes=1)
    if any(e.depth is not None and e.depth.timestamp > later_than_entry for e in events if e.kind == "market"):
        buys = [f for f in direct.fills if f.side is Side.BUY]
        assert all(f.mode is not SimulatedFillMode.EXACT_DEPTH for f in buys) or not buys
