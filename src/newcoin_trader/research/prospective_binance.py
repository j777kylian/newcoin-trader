"""Binance public Spot prospective feed (GET-only, one configured symbol)."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol

from newcoin_trader.domain.enums import Chain, Venue
from newcoin_trader.domain.event_study import ObservationResolution, TokenListingEvent
from newcoin_trader.domain.executable_backtest import DepthLevel, HistoricalDepthBook
from newcoin_trader.domain.live_paper import ReplayMarketEvent
from newcoin_trader.domain.market import OrderBookL2, Ticker24h, TradeTick
from newcoin_trader.domain.tokens import NewListingEvent
from newcoin_trader.errors import CollectorError, ConfigError, ParseError
from newcoin_trader.research.prospective_feed import (
    ProspectiveFeedResult,
    ProspectiveFeedStatus,
    validate_prospective_feed_bounds,
)

NowFn = Callable[[], datetime]
SleepFn = Callable[[float], Awaitable[None]]


class BinanceSpotReader(Protocol):
    """Subset of BinanceClient used by the prospective adapter (public GET only)."""

    async def exchange_info(self) -> list[NewListingEvent]: ...

    async def recent_trades(self, symbol: str, *, limit: int = 500) -> list[TradeTick]: ...

    async def ticker_24h(self, symbol: str) -> Ticker24h: ...

    async def order_book(self, symbol: str, *, limit: int = 100) -> OrderBookL2: ...


def listing_event_id(
    *,
    source: str,
    symbol: str,
    onboard: datetime | None,
    venue: Venue,
) -> str:
    onboard_key = onboard.astimezone(UTC).isoformat() if onboard is not None else "none"
    return f"{source}:listing:{venue.value}:{symbol}:{onboard_key}"


def trade_event_id(*, source: str, trade_id: str) -> str:
    return f"{source}:trade:{trade_id}"


def _book_liquidity(book: OrderBookL2) -> Decimal | None:
    total = Decimal("0")
    for level in book.bids:
        total += level.price * level.quantity
    for level in book.asks:
        total += level.price * level.quantity
    return total if total > 0 else None


def _to_depth_book(book: OrderBookL2, *, received: datetime) -> HistoricalDepthBook:
    # Binance /depth has lastUpdateId but no venue source update time — do not invent one.
    # Local receipt is the only reliable depth clock; keep it distinct in provenance.
    return HistoricalDepthBook(
        token_address=book.token_address,
        chain=book.chain,
        venue=Venue.BINANCE,
        timestamp=received,
        bids=tuple(DepthLevel(price=level.price, quantity=level.quantity) for level in book.bids),
        asks=tuple(DepthLevel(price=level.price, quantity=level.quantity) for level in book.asks),
        source="binance:depth",
        provenance={
            "endpoint": "/api/v3/depth",
            "last_update_id": str(book.last_update_id or ""),
            "depth_received_timestamp": received.isoformat(),
        },
    )


def _to_listing_replay(
    listing: NewListingEvent,
    *,
    received: datetime,
) -> ReplayMarketEvent:
    source_event = listing.created_time or listing.first_seen_time
    event_id = listing_event_id(
        source=listing.source,
        symbol=listing.symbol,
        onboard=listing.created_time,
        venue=listing.venue or Venue.BINANCE,
    )
    token_listing = TokenListingEvent(
        event_id=event_id,
        venue=listing.venue or Venue.BINANCE,
        chain=listing.chain,
        token_address=listing.token_address,
        pair_address=listing.pair_address,
        symbol=listing.symbol,
        source=listing.source,
        source_event_time=source_event,
        first_seen_time=listing.first_seen_time,
        first_market_data_time=None,
        decision_available_time=listing.first_seen_time,
        provenance=dict(listing.provenance or {}),
    )
    return ReplayMarketEvent(
        event_id=event_id,
        kind="listing",
        venue=token_listing.venue,
        token_address=token_listing.token_address,
        chain=token_listing.chain.value,
        source_timestamp=source_event,
        received_timestamp=received,
        resolution=ObservationResolution.POINT,
        source=listing.source,
        listing=token_listing,
        provenance=dict(token_listing.provenance),
    )


def _to_trade_replay(
    trade: TradeTick,
    *,
    received: datetime,
    depth: HistoricalDepthBook | None,
    liquidity: Decimal | None,
) -> ReplayMarketEvent:
    if not trade.external_trade_id:
        raise ParseError("binance trade missing external_trade_id")
    event_id = trade_event_id(source="binance", trade_id=trade.external_trade_id)
    provenance = dict(trade.provenance or {})
    provenance["external_trade_id"] = trade.external_trade_id
    if depth is not None and liquidity is not None:
        provenance["liquidity_from_depth"] = "true"
    return ReplayMarketEvent(
        event_id=event_id,
        kind="market",
        venue=Venue.BINANCE,
        token_address=trade.token_address,
        chain=trade.chain,
        source_timestamp=trade.timestamp,
        received_timestamp=received,
        price=trade.price,
        liquidity=liquidity,
        volume=trade.amount,
        resolution=ObservationResolution.POINT,
        source=trade.source,
        depth=depth,
        provenance=provenance,
    )


class BinanceProspectiveFeed:
    """Bounded polling adapter over Binance public Spot GET endpoints."""

    def __init__(
        self,
        *,
        client: BinanceSpotReader,
        now: NowFn,
        symbol: str,
        poll_interval: timedelta,
        duration: timedelta,
        max_polls: int,
        max_events: int,
        max_observations_per_token: int,
        max_total_observations: int,
        queue_capacity: int,
        sleep: SleepFn = asyncio.sleep,
        trade_limit: int = 100,
        depth_limit: int = 100,
    ) -> None:
        if not symbol.strip():
            raise ConfigError("prospective Binance symbol is required")
        validate_prospective_feed_bounds(
            poll_interval=poll_interval,
            duration=duration,
            max_polls=max_polls,
            max_events=max_events,
            max_observations_per_token=max_observations_per_token,
            max_total_observations=max_total_observations,
            queue_capacity=queue_capacity,
        )
        self._client = client
        self._now = now
        self._sleep = sleep
        self._symbol = symbol.strip().upper()
        self._poll_interval = poll_interval
        self._duration = duration
        self._max_polls = max_polls
        self._max_events = max_events
        self._max_obs_per_token = max_observations_per_token
        self._max_total_obs = max_total_observations
        self._queue_capacity = queue_capacity
        self._trade_limit = trade_limit
        self._depth_limit = depth_limit

    async def collect_bounded(self) -> ProspectiveFeedResult:
        started = self._now()
        deadline = started + self._duration
        events: list[ReplayMarketEvent] = []
        seen_event_ids: set[str] = set()
        obs_per_token: dict[str, int] = {}
        total_obs = 0
        poll_count = 0
        overflow_count = 0
        rejected_count = 0
        duplicate_suppressed = 0
        source_errors: list[str] = []
        listing_emitted = False
        status = ProspectiveFeedStatus.OK

        def _can_poll_more() -> bool:
            return len(events) < self._max_events

        def _push(event: ReplayMarketEvent, *, counts_as_observation: bool) -> bool:
            nonlocal overflow_count, rejected_count, total_obs, duplicate_suppressed, status
            if event.event_id in seen_event_ids:
                duplicate_suppressed += 1
                return False
            if len(events) >= self._queue_capacity:
                overflow_count += 1
                rejected_count += 1
                status = ProspectiveFeedStatus.QUEUE_OVERFLOW
                return False
            if len(events) >= self._max_events:
                rejected_count += 1
                status = ProspectiveFeedStatus.BOUNDS_REACHED
                return False
            if counts_as_observation:
                token = event.token_address
                if obs_per_token.get(token, 0) >= self._max_obs_per_token:
                    rejected_count += 1
                    status = ProspectiveFeedStatus.BOUNDS_REACHED
                    return False
                if total_obs >= self._max_total_obs:
                    rejected_count += 1
                    status = ProspectiveFeedStatus.BOUNDS_REACHED
                    return False
            events.append(event)
            seen_event_ids.add(event.event_id)
            if counts_as_observation:
                obs_per_token[event.token_address] = obs_per_token.get(event.token_address, 0) + 1
                total_obs += 1
            return True

        while poll_count < self._max_polls and self._now() < deadline and _can_poll_more():
            if status is ProspectiveFeedStatus.SOURCE_UNAVAILABLE:
                break
            poll_count += 1
            try:
                listings = await self._client.exchange_info()
                listing_received = self._now()
            except (CollectorError, ParseError) as exc:
                source_errors.append(f"exchange_info: {exc}")
                status = ProspectiveFeedStatus.SOURCE_UNAVAILABLE
                break

            selected = next((item for item in listings if item.symbol == self._symbol), None)
            if selected is None:
                source_errors.append(f"configured symbol {self._symbol!r} not trading on exchangeInfo")
                status = ProspectiveFeedStatus.SOURCE_UNAVAILABLE
                break

            if not listing_emitted:
                listing_event = _to_listing_replay(selected, received=listing_received)
                if _push(listing_event, counts_as_observation=False):
                    listing_emitted = True

            try:
                trades = await self._client.recent_trades(self._symbol, limit=self._trade_limit)
                trades_received = self._now()
            except (CollectorError, ParseError) as exc:
                source_errors.append(f"recent_trades: {exc}")
                status = ProspectiveFeedStatus.SOURCE_UNAVAILABLE
                break

            depth_book: HistoricalDepthBook | None = None
            liquidity: Decimal | None = None
            try:
                raw_book = await self._client.order_book(self._symbol, limit=self._depth_limit)
                book_received = self._now()
                if raw_book.token_address == self._symbol:
                    depth_book = _to_depth_book(raw_book, received=book_received)
                    liquidity = _book_liquidity(raw_book)
            except (CollectorError, ParseError) as exc:
                source_errors.append(f"order_book: {exc}")
                # Depth is optional for trade emission; do not fabricate liquidity/price.

            try:
                await self._client.ticker_24h(self._symbol)
                _ = self._now()  # receipt clock after ticker acceptance (unused for trades)
            except (CollectorError, ParseError) as exc:
                source_errors.append(f"ticker_24h: {exc}")
                # Ticker failure does not fabricate trade inputs; trades may still emit.

            for trade in trades:
                if trade.token_address != self._symbol:
                    rejected_count += 1
                    continue
                attach_depth = None
                if depth_book is not None and depth_book.token_address == trade.token_address:
                    attach_depth = depth_book
                try:
                    market_event = _to_trade_replay(
                        trade,
                        received=trades_received,
                        depth=attach_depth,
                        liquidity=liquidity,
                    )
                except ParseError as exc:
                    source_errors.append(f"trade_normalize: {exc}")
                    status = ProspectiveFeedStatus.SOURCE_UNAVAILABLE
                    continue
                _push(market_event, counts_as_observation=True)

            if poll_count >= self._max_polls or not _can_poll_more() or self._now() >= deadline:
                break
            if self._poll_interval.total_seconds() > 0:
                await self._sleep(self._poll_interval.total_seconds())

        if status is ProspectiveFeedStatus.OK and (
            poll_count >= self._max_polls
            or len(events) >= self._max_events
            or total_obs >= self._max_total_obs
            or any(v >= self._max_obs_per_token for v in obs_per_token.values())
        ):
            status = ProspectiveFeedStatus.BOUNDS_REACHED

        if status is ProspectiveFeedStatus.OK and overflow_count:
            status = ProspectiveFeedStatus.QUEUE_OVERFLOW

        return ProspectiveFeedResult(
            events=tuple(events),
            status=status,
            poll_count=poll_count,
            overflow_count=overflow_count,
            rejected_count=rejected_count,
            duplicate_suppressed_count=duplicate_suppressed,
            source_errors=tuple(source_errors),
            observations_emitted=total_obs,
            extras={"symbol": self._symbol, "venue": Venue.BINANCE.value, "chain": Chain.BINANCE.value},
        )


__all__ = [
    "BinanceProspectiveFeed",
    "BinanceSpotReader",
    "listing_event_id",
    "trade_event_id",
]
