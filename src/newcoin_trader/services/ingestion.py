"""Async discovery/ingestion orchestration. Collectors are GET/read-only."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol

from newcoin_trader.domain.market import Kline, PoolSnapshot, PriceSnapshot, TradeTick
from newcoin_trader.domain.tokens import NewListingEvent
from newcoin_trader.errors import ConfigError

CollectFn = Callable[[], Awaitable["CollectOnceResult"]]


class BinanceDiscovery(Protocol):
    async def exchange_info(self) -> list[NewListingEvent]: ...


class BirdeyeDiscovery(Protocol):
    async def discover_new_tokens(
        self,
        *,
        limit: int = 10,
        time_to: int | None = None,
        meme_platform_enabled: bool = False,
    ) -> list[NewListingEvent]: ...

    async def discover_new_pairs(
        self,
        *,
        limit: int = 10,
        offset: int = 0,
    ) -> list[NewListingEvent]: ...


class TokenWriter(Protocol):
    async def upsert(
        self,
        *,
        chain: str,
        token_address: str,
        symbol: str,
        created_time: datetime | None,
        first_seen_time: datetime,
        source: str,
        venue: str | None = None,
        metadata_json: dict[str, Any] | None = None,
    ) -> Any: ...


class MarketWriter(Protocol):
    async def upsert_snapshot(
        self,
        *,
        token_id: int,
        timestamp: datetime,
        price: Decimal,
        volume: Decimal | None,
        liquidity: Decimal | None,
        market_cap: Decimal | None,
        buy_count: int | None,
        sell_count: int | None,
        source: str,
        provenance: dict[str, Any] | None = None,
    ) -> Any: ...

    async def upsert_trade(
        self,
        *,
        token_id: int,
        timestamp: datetime,
        side: str,
        amount: Decimal,
        price: Decimal,
        source: str,
        external_trade_id: str | None = None,
        provenance: dict[str, Any] | None = None,
    ) -> Any: ...


@dataclass(frozen=True)
class CollectOnceResult:
    discovered: int
    upserted: int
    binance_count: int
    birdeye_token_count: int
    birdeye_pair_count: int


class IngestionService:
    """Bounded discovery + idempotent persistence via injected protocols."""

    def __init__(
        self,
        *,
        binance: BinanceDiscovery,
        birdeye: BirdeyeDiscovery,
        tokens: TokenWriter,
        market: MarketWriter,
    ) -> None:
        self._binance = binance
        self._birdeye = birdeye
        self._tokens = tokens
        self._market = market

    async def collect_once(
        self,
        *,
        birdeye_limit: int = 10,
        meme_platform_enabled: bool = True,
    ) -> CollectOnceResult:
        binance_events = await self._binance.exchange_info()
        birdeye_tokens = await self._birdeye.discover_new_tokens(
            limit=birdeye_limit,
            meme_platform_enabled=meme_platform_enabled,
        )
        birdeye_pairs = await self._birdeye.discover_new_pairs(limit=birdeye_limit)
        events = [*binance_events, *birdeye_tokens, *birdeye_pairs]
        upserted = 0
        for event in events:
            await self._upsert_listing(event)
            upserted += 1
        return CollectOnceResult(
            discovered=len(events),
            upserted=upserted,
            binance_count=len(binance_events),
            birdeye_token_count=len(birdeye_tokens),
            birdeye_pair_count=len(birdeye_pairs),
        )

    async def ingest_snapshots(
        self,
        *,
        token_id: int,
        snapshots: Sequence[PriceSnapshot],
    ) -> int:
        for snap in snapshots:
            await self._market.upsert_snapshot(
                token_id=token_id,
                timestamp=snap.timestamp,
                price=snap.price,
                volume=snap.volume,
                liquidity=snap.liquidity,
                market_cap=snap.market_cap,
                buy_count=snap.buy_count,
                sell_count=snap.sell_count,
                source=snap.source,
                provenance=snap.provenance,
            )
        return len(snapshots)

    async def ingest_trades(
        self,
        *,
        token_id: int,
        trades: Sequence[TradeTick],
    ) -> int:
        for trade in trades:
            await self._market.upsert_trade(
                token_id=token_id,
                timestamp=trade.timestamp,
                side=str(trade.side),
                amount=trade.amount,
                price=trade.price,
                source=trade.source,
                external_trade_id=trade.external_trade_id,
                provenance=trade.provenance,
            )
        return len(trades)

    async def _upsert_listing(self, event: NewListingEvent) -> Any:
        return await self._tokens.upsert(
            chain=str(event.chain),
            token_address=event.token_address,
            symbol=event.symbol,
            created_time=event.created_time,
            first_seen_time=event.first_seen_time,
            source=event.source,
            venue=str(event.venue) if event.venue is not None else None,
            metadata_json=dict(event.provenance) if event.provenance else None,
        )


class PollController:
    """Safe polling loop with interval and external/max-iteration stop control."""

    def __init__(
        self,
        *,
        interval_seconds: float,
        stop_event: asyncio.Event | None = None,
        max_iterations: int | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self._interval = interval_seconds
        self._stop = stop_event or asyncio.Event()
        self._max_iterations = max_iterations
        self.stopped = False

    def stop(self) -> None:
        self._stop.set()

    async def run(self, collect: CollectFn | IngestionService) -> list[CollectOnceResult]:
        worker: CollectFn
        if isinstance(collect, IngestionService):
            worker = collect.collect_once
        else:
            worker = collect

        results: list[CollectOnceResult] = []
        iterations = 0
        while not self._stop.is_set():
            results.append(await worker())
            iterations += 1
            if self._max_iterations is not None and iterations >= self._max_iterations:
                break
            if self._stop.is_set():
                break
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval)
            except TimeoutError:
                continue
        self.stopped = True
        return results


class BinanceMarketHistory(Protocol):
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

    async def recent_trades(self, symbol: str, *, limit: int = 500) -> list[TradeTick]: ...


class RaydiumMarketHistory(Protocol):
    async def list_pools(
        self,
        *,
        page: int = 1,
        page_size: int = 100,
        pool_type: str = "all",
    ) -> list[PoolSnapshot]: ...


class GeckoMarketHistory(Protocol):
    async def get_pool(self, network: str, pool_address: str) -> PoolSnapshot: ...

    async def pool_ohlcv(
        self,
        network: str,
        pool_address: str,
        *,
        timeframe: str = "minute",
        aggregate: int = 1,
        limit: int = 100,
        before_timestamp: int | None = None,
        currency: str = "usd",
    ) -> list[Kline]: ...


@dataclass(frozen=True)
class MarketHistoryResult:
    snapshots: int
    trades: int
    pools: int
    by_source: dict[str, int]


# Public ingest-market-history request/page/record control bounds (inclusive).
# Values must be strictly positive integers. Invalid values raise ConfigError
# at the service and CLI boundaries before any collector, HTTP, or persistence work.
# Caps are conservative (at or below documented venue maxima).
INGEST_CONTROL_MIN = 1
INGEST_BINANCE_LIMIT_MAX = 1000  # Binance klines / aggTrades / trades max
INGEST_RAYDIUM_PAGE_MAX = 100
INGEST_RAYDIUM_PAGE_SIZE_MAX = 100  # below typical Raydium pageSize cap of 1000
INGEST_GECKO_OHLCV_LIMIT_MAX = 1000  # GeckoTerminal OHLCV max


def _require_bounded_int(
    value: Any,
    *,
    name: str,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{name} must be an integer in [{minimum}, {maximum}]")
    if value < minimum or value > maximum:
        raise ConfigError(
            f"{name} must be in [{minimum}, {maximum}] (strictly positive, conservative upper bound), got {value}"
        )
    return value


def validate_ingest_market_history_controls(
    *,
    binance_limit: int,
    raydium_page: int,
    raydium_page_size: int | None,
    gecko_ohlcv_limit: int,
) -> None:
    """Reject zero/negative/excessive page/limit controls before any network work."""
    _require_bounded_int(
        binance_limit,
        name="binance_limit",
        minimum=INGEST_CONTROL_MIN,
        maximum=INGEST_BINANCE_LIMIT_MAX,
    )
    _require_bounded_int(
        raydium_page,
        name="raydium_page",
        minimum=INGEST_CONTROL_MIN,
        maximum=INGEST_RAYDIUM_PAGE_MAX,
    )
    if raydium_page_size is not None:
        _require_bounded_int(
            raydium_page_size,
            name="raydium_page_size",
            minimum=INGEST_CONTROL_MIN,
            maximum=INGEST_RAYDIUM_PAGE_SIZE_MAX,
        )
    _require_bounded_int(
        gecko_ohlcv_limit,
        name="gecko_ohlcv_limit",
        minimum=INGEST_CONTROL_MIN,
        maximum=INGEST_GECKO_OHLCV_LIMIT_MAX,
    )


def _row_id(row: Any) -> int:
    if isinstance(row, dict):
        return int(row["id"])
    return int(row.id)


def _kline_to_snapshot(kline: Kline) -> PriceSnapshot:
    return PriceSnapshot(
        token_address=kline.token_address,
        chain=kline.chain,
        timestamp=kline.close_time,
        price=kline.close,
        volume=kline.volume,
        source=kline.source,
        provenance={"kind": "kline", "interval": kline.interval},
    )


def _pool_to_snapshot(pool: PoolSnapshot) -> PriceSnapshot | None:
    if pool.price is None:
        return None
    return PriceSnapshot(
        token_address=pool.pool_address,
        chain=pool.chain,
        timestamp=pool.timestamp,
        price=pool.price,
        volume=pool.volume_24h,
        liquidity=pool.liquidity,
        source=pool.source,
        provenance={"kind": "pool"},
    )


class MarketHistoryService:
    """Bounded research-only market history fetch + persist (GET collectors only)."""

    def __init__(
        self,
        *,
        binance: BinanceMarketHistory,
        raydium: RaydiumMarketHistory,
        gecko: GeckoMarketHistory,
        tokens: TokenWriter,
        market: MarketWriter,
    ) -> None:
        self._binance = binance
        self._raydium = raydium
        self._gecko = gecko
        self._tokens = tokens
        self._market = market

    async def _persist_snapshot(self, *, token_id: int, snap: PriceSnapshot) -> None:
        await self._market.upsert_snapshot(
            token_id=token_id,
            timestamp=snap.timestamp,
            price=snap.price,
            volume=snap.volume,
            liquidity=snap.liquidity,
            market_cap=snap.market_cap,
            buy_count=snap.buy_count,
            sell_count=snap.sell_count,
            source=snap.source,
            provenance=snap.provenance,
        )

    async def _persist_trade(self, *, token_id: int, trade: TradeTick) -> None:
        await self._market.upsert_trade(
            token_id=token_id,
            timestamp=trade.timestamp,
            side=str(trade.side),
            amount=trade.amount,
            price=trade.price,
            source=trade.source,
            external_trade_id=trade.external_trade_id,
            provenance=trade.provenance,
        )

    async def ingest_market_history(
        self,
        *,
        binance_symbol: str | None = None,
        binance_interval: str = "1h",
        binance_start_ms: int | None = None,
        binance_end_ms: int | None = None,
        binance_limit: int = 100,
        include_binance_recent_trades: bool = False,
        raydium_page: int = 1,
        raydium_page_size: int | None = None,
        gecko_network: str | None = None,
        gecko_pool: str | None = None,
        gecko_ohlcv_limit: int = 100,
        gecko_timeframe: str = "minute",
    ) -> MarketHistoryResult:
        validate_ingest_market_history_controls(
            binance_limit=binance_limit,
            raydium_page=raydium_page,
            raydium_page_size=raydium_page_size,
            gecko_ohlcv_limit=gecko_ohlcv_limit,
        )
        snapshots = 0
        trades = 0
        pools = 0
        by_source: dict[str, int] = {}

        def _bump(source: str, n: int = 1) -> None:
            by_source[source] = by_source.get(source, 0) + n

        if binance_symbol:
            token = await self._tokens.upsert(
                chain="binance",
                token_address=binance_symbol,
                symbol=binance_symbol,
                created_time=None,
                first_seen_time=datetime.now(UTC),
                source="binance",
                venue="binance",
            )
            token_id = _row_id(token)
            klines = await self._binance.klines(
                binance_symbol,
                interval=binance_interval,
                start_time=binance_start_ms,
                end_time=binance_end_ms,
                limit=binance_limit,
            )
            for kline in klines:
                await self._persist_snapshot(token_id=token_id, snap=_kline_to_snapshot(kline))
                snapshots += 1
                _bump("binance")
            agg = await self._binance.agg_trades(
                binance_symbol,
                start_time=binance_start_ms,
                end_time=binance_end_ms,
                limit=binance_limit,
            )
            for trade in agg:
                await self._persist_trade(token_id=token_id, trade=trade)
                trades += 1
                _bump("binance")
            if include_binance_recent_trades:
                for trade in await self._binance.recent_trades(binance_symbol, limit=binance_limit):
                    await self._persist_trade(token_id=token_id, trade=trade)
                    trades += 1
                    _bump("binance")

        if raydium_page_size is not None:
            pool_rows = await self._raydium.list_pools(
                page=raydium_page,
                page_size=raydium_page_size,
            )
            for pool in pool_rows:
                pools += 1
                _bump("raydium")
                token = await self._tokens.upsert(
                    chain=pool.chain,
                    token_address=pool.pool_address,
                    symbol=pool.name or pool.pool_address[:12],
                    created_time=None,
                    first_seen_time=pool.timestamp,
                    source=pool.source,
                    venue="raydium",
                )
                snap = _pool_to_snapshot(pool)
                if snap is None:
                    continue
                await self._persist_snapshot(token_id=_row_id(token), snap=snap)
                snapshots += 1

        if gecko_network and gecko_pool:
            pool = await self._gecko.get_pool(gecko_network, gecko_pool)
            pools += 1
            _bump("geckoterminal")
            token = await self._tokens.upsert(
                chain=pool.chain,
                token_address=pool.pool_address,
                symbol=pool.name or pool.pool_address[:12],
                created_time=None,
                first_seen_time=pool.timestamp,
                source=pool.source,
                venue="geckoterminal",
            )
            token_id = _row_id(token)
            snap = _pool_to_snapshot(pool)
            if snap is not None:
                pool_snap = snap.model_copy(update={"source": f"{snap.source}:pool"})
                await self._persist_snapshot(token_id=token_id, snap=pool_snap)
                snapshots += 1
            for kline in await self._gecko.pool_ohlcv(
                gecko_network,
                gecko_pool,
                timeframe=gecko_timeframe,
                limit=gecko_ohlcv_limit,
            ):
                kline_snap = _kline_to_snapshot(kline).model_copy(
                    update={"source": f"{kline.source}:ohlcv:{kline.interval}"}
                )
                await self._persist_snapshot(token_id=token_id, snap=kline_snap)
                snapshots += 1
                _bump("geckoterminal")

        return MarketHistoryResult(
            snapshots=snapshots,
            trades=trades,
            pools=pools,
            by_source=by_source,
        )
