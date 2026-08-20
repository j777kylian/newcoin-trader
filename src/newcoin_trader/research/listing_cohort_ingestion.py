"""Cohort-driven historical Binance Spot kline/aggTrade ingestion (research only)."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Protocol

from newcoin_trader.domain.enums import Venue
from newcoin_trader.domain.event_study import ObservationResolution
from newcoin_trader.domain.feature_research import FeatureMarketInput, FeatureTradeInput
from newcoin_trader.domain.listing_cohort import CohortListing
from newcoin_trader.domain.market import Kline, TradeTick
from newcoin_trader.domain.types import require_utc
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.event_study_config import DEFAULT_ENTRY_DELAYS, DEFAULT_HOLDING_PERIODS

INGEST_WINDOW_BUFFER = timedelta(minutes=1)
KLINE_INTERVAL = "1m"
KLINE_SOURCE = "binance:kline:1m"
AGG_TRADE_SOURCE = "binance:agg_trade"
BINANCE_LIMIT_MIN = 1
BINANCE_LIMIT_MAX = 1000
MAX_INGEST_PAGES = 100
_AGGTRADE_MAX_SPAN_MS = 60 * 60 * 1000


class BinanceMarketHistory(Protocol):
    """Subset of Binance Spot REST used for listing-cohort historical series."""

    async def klines(
        self,
        symbol: str,
        *,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 500,
    ) -> list[Kline]: ...

    async def agg_trades(
        self,
        symbol: str,
        *,
        from_id: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 500,
    ) -> list[TradeTick]: ...


def datetime_to_millis(value: datetime) -> int:
    return int(require_utc(value).timestamp() * 1000)


def validate_binance_limit(limit: int) -> int:
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise ConfigError(f"binance_limit must be an integer in [{BINANCE_LIMIT_MIN}, {BINANCE_LIMIT_MAX}]")
    if limit < BINANCE_LIMIT_MIN or limit > BINANCE_LIMIT_MAX:
        raise ConfigError(
            f"binance_limit must be in [{BINANCE_LIMIT_MIN}, {BINANCE_LIMIT_MAX}] "
            f"(strictly positive, conservative upper bound), got {limit}"
        )
    return limit


def compute_listing_ingest_window(
    listing: CohortListing,
    *,
    buffer: timedelta = INGEST_WINDOW_BUFFER,
    now_utc: datetime | None = None,
) -> tuple[datetime, datetime] | None:
    """Window from min listing clocks minus buffer through delay+hold plus buffer.

    The window end is clamped to ``now_utc`` so collection never extends into the future.
    """
    resolved_now = require_utc(now_utc) if now_utc is not None else datetime.now(UTC)
    start_candidates = [
        ts
        for ts in (
            listing.source_event_time,
            listing.first_market_data_time,
            listing.first_trade_time,
            listing.first_kline_time,
        )
        if ts is not None
    ]
    end_candidates = [ts for ts in (listing.source_event_time, listing.first_market_data_time) if ts is not None]
    if not start_candidates or not end_candidates:
        return None
    start = min(start_candidates) - buffer
    end = max(end_candidates) + max(DEFAULT_ENTRY_DELAYS) + max(DEFAULT_HOLDING_PERIODS) + buffer
    end = min(end, resolved_now)
    if end <= start:
        return None
    return start, end


def observation_passes_listing_clocks(timestamp: datetime, listing: CohortListing) -> bool:
    """Drop pre-listing points. ``source_event_time`` and ``first_market_data_time`` stay distinct."""
    if listing.source_event_time is not None and timestamp < listing.source_event_time:
        return False
    if listing.first_market_data_time is not None and timestamp < listing.first_market_data_time:
        return False
    return True


def kline_to_market_input(kline: Kline) -> FeatureMarketInput:
    return FeatureMarketInput(
        token_address=kline.token_address,
        chain=kline.chain,
        venue=kline.venue or Venue.BINANCE,
        timestamp=kline.close_time,
        price=kline.close,
        volume=kline.volume,
        resolution=ObservationResolution.MINUTE,
        source=KLINE_SOURCE,
        provenance={"kind": "kline", "interval": kline.interval or KLINE_INTERVAL},
    )


def trade_to_market_input(trade: TradeTick) -> FeatureMarketInput:
    provenance = dict(trade.provenance) if trade.provenance else {"kind": "aggTrade"}
    return FeatureMarketInput(
        token_address=trade.token_address,
        chain=trade.chain,
        venue=Venue.BINANCE,
        timestamp=trade.timestamp,
        price=trade.price,
        resolution=ObservationResolution.POINT,
        source=AGG_TRADE_SOURCE,
        provenance=provenance,
    )


def trade_to_trade_input(trade: TradeTick) -> FeatureTradeInput:
    provenance = dict(trade.provenance) if trade.provenance else {"kind": "aggTrade"}
    return FeatureTradeInput(
        token_address=trade.token_address,
        chain=trade.chain,
        venue=Venue.BINANCE,
        timestamp=trade.timestamp,
        side=str(trade.side),
        amount=trade.amount,
        price=trade.price,
        source=AGG_TRADE_SOURCE,
        provenance=provenance,
    )


async def _fetch_klines_window(
    source: BinanceMarketHistory,
    symbol: str,
    *,
    start: datetime,
    end: datetime,
    limit: int,
) -> list[Kline]:
    collected: list[Kline] = []
    cursor = datetime_to_millis(start)
    end_ms = datetime_to_millis(end)
    seen: set[datetime] = set()
    for _page in range(MAX_INGEST_PAGES):
        if cursor > end_ms:
            break
        batch = await source.klines(
            symbol,
            interval=KLINE_INTERVAL,
            start_time=cursor,
            end_time=end_ms,
            limit=limit,
        )
        if not batch:
            break
        for kline in batch:
            if kline.open_time in seen:
                continue
            seen.add(kline.open_time)
            collected.append(kline)
        next_cursor = datetime_to_millis(batch[-1].open_time) + 1
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if len(batch) < limit:
            break
    return collected


async def _fetch_agg_trades_window(
    source: BinanceMarketHistory,
    symbol: str,
    *,
    start: datetime,
    end: datetime,
    limit: int,
) -> list[TradeTick]:
    collected: list[TradeTick] = []
    start_ms = datetime_to_millis(start)
    end_ms = datetime_to_millis(end)
    cursor = start_ms
    from_id: int | None = None
    seen_ids: set[str] = set()
    for _page in range(MAX_INGEST_PAGES):
        if from_id is None and cursor > end_ms:
            break
        if from_id is None:
            chunk_end = min(end_ms, cursor + _AGGTRADE_MAX_SPAN_MS - 1)
            batch = await source.agg_trades(
                symbol,
                start_time=cursor,
                end_time=chunk_end,
                limit=limit,
            )
        else:
            batch = await source.agg_trades(symbol, from_id=from_id, limit=limit)
        if not batch:
            if from_id is None:
                cursor += _AGGTRADE_MAX_SPAN_MS
                continue
            break
        past_window = False
        for trade in batch:
            ts_ms = datetime_to_millis(trade.timestamp)
            if ts_ms > end_ms:
                past_window = True
                break
            trade_id = trade.external_trade_id or ""
            if trade_id and trade_id in seen_ids:
                continue
            if trade_id:
                seen_ids.add(trade_id)
            if ts_ms >= start_ms:
                collected.append(trade)
        if past_window:
            break
        last_id = batch[-1].external_trade_id
        if last_id is None:
            break
        next_from_id = int(last_id) + 1
        if len(batch) < limit:
            if from_id is None:
                cursor += _AGGTRADE_MAX_SPAN_MS
                from_id = None
                continue
            break
        from_id = next_from_id
    return collected


async def ingest_cohort_market_history(
    listings: Sequence[CohortListing],
    source: BinanceMarketHistory,
    *,
    limit: int = 500,
    buffer: timedelta = INGEST_WINDOW_BUFFER,
    now_utc: datetime | None = None,
) -> tuple[tuple[FeatureMarketInput, ...], tuple[FeatureTradeInput, ...]]:
    """Fetch 1m klines + aggTrades per listing window; drop pre-listing observations."""
    validate_binance_limit(limit)
    market_inputs: list[FeatureMarketInput] = []
    trade_inputs: list[FeatureTradeInput] = []
    for listing in listings:
        window = compute_listing_ingest_window(listing, buffer=buffer, now_utc=now_utc)
        if window is None:
            continue
        start, end = window
        klines = await _fetch_klines_window(source, listing.symbol, start=start, end=end, limit=limit)
        ticks = await _fetch_agg_trades_window(source, listing.symbol, start=start, end=end, limit=limit)
        for kline in klines:
            inp = kline_to_market_input(kline)
            if observation_passes_listing_clocks(inp.timestamp, listing):
                market_inputs.append(inp)
        for tick in ticks:
            if not observation_passes_listing_clocks(tick.timestamp, listing):
                continue
            market_inputs.append(trade_to_market_input(tick))
            trade_inputs.append(trade_to_trade_input(tick))
    market_inputs.sort(key=lambda row: (row.timestamp, row.token_address, row.source, row.resolution.value))
    trade_inputs.sort(key=lambda row: (row.timestamp, row.token_address, row.source))
    return tuple(market_inputs), tuple(trade_inputs)
