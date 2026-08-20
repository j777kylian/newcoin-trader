"""Phase 8A.2 early-market-event persistence repository."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from newcoin_trader.database.models import (
    EarlyMarketEventEvidence,
    EarlyMarketEventRecord,
    EarlyMarketObservation,
    Market,
    Token,
)
from newcoin_trader.domain.early_market_events import EarlyMarketEvent
from newcoin_trader.domain.types import require_utc

_LIST_LIMIT_MIN = 1
_LIST_LIMIT_MAX = 1000
_RECEIPT_CLAIMING_STATUSES = frozenset({"receipt_verified"})


def _token_id(token: Token | int) -> int:
    if isinstance(token, Token):
        return token.id
    return token


def _optional_token_id(token: Token | int | None) -> int | None:
    if token is None:
        return None
    return _token_id(token)


def _market_id(market: Market | int | None) -> int | None:
    if market is None:
        return None
    if isinstance(market, Market):
        return market.id
    return market


def _require_bound(limit: int) -> int:
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < _LIST_LIMIT_MIN or limit > _LIST_LIMIT_MAX:
        raise ValueError(f"limit must be an integer in {_LIST_LIMIT_MIN}..{_LIST_LIMIT_MAX}")
    return limit


def _require_native_id(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required and must be a non-empty source-native id")
    return value


class EarlyMarketEventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_or_get_market(
        self,
        *,
        market_key: str,
        base_token: Token | int,
        quote_token: Token | int | None = None,
        pool_or_pair_address: str | None = None,
        venue: str,
        symbol: str | None = None,
        source_native_market_id: str | None = None,
        market_kind: str,
        identity_status: str,
        source: str,
        metadata_json: dict[str, Any] | None = None,
        provenance_json: dict[str, Any] | None = None,
    ) -> Market:
        base_token_id = _token_id(base_token)
        quote_token_id = _optional_token_id(quote_token)
        stmt = (
            insert(Market)
            .values(
                market_key=market_key,
                base_token_id=base_token_id,
                quote_token_id=quote_token_id,
                pool_or_pair_address=pool_or_pair_address,
                venue=venue,
                symbol=symbol,
                source_native_market_id=source_native_market_id,
                market_kind=market_kind,
                identity_status=identity_status,
                source=source,
                metadata_json=metadata_json,
                provenance_json=provenance_json,
            )
            .on_conflict_do_nothing(constraint="uq_markets_market_key")
            .returning(Market)
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            existing = await self._session.scalar(select(Market).where(Market.market_key == market_key))
            assert existing is not None
            return existing
        await self._session.flush()
        return row

    async def insert_event(
        self,
        event: EarlyMarketEvent,
        *,
        source_native_event_id: str,
        asset_token: Token | int,
        market: Market | int | None = None,
    ) -> EarlyMarketEventRecord:
        # Domain EarlyMarketEvent already enforces UTC + clock order.
        native_id = _require_native_id(source_native_event_id, field="source_native_event_id")
        asset_token_id = _token_id(asset_token)
        market_id = _market_id(market)

        stmt = (
            insert(EarlyMarketEventRecord)
            .values(
                source_native_event_id=native_id,
                source=event.source,
                event_kind=event.event_kind.value,
                event_definition_version=event.event_definition_version,
                venue_or_protocol=event.venue_or_protocol,
                chain=event.chain,
                asset_token_id=asset_token_id,
                market_id=market_id,
                source_event_time=event.source_event_time,
                received_time=event.received_time,
                decision_available_time=event.decision_available_time,
                first_market_data_time=event.first_market_data_time,
                first_liquidity_time=event.first_liquidity_time,
                first_trade_time=event.first_trade_time,
                event_time_semantics=event.event_time_semantics.value,
                event_quality_status=event.event_quality_status.value,
                event_clock_quality=event.event_clock_quality.value,
                provenance_ref=event.provenance_ref,
            )
            .on_conflict_do_nothing(constraint="uq_early_market_events_source_native_id")
            .returning(EarlyMarketEventRecord)
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            existing = await self._session.scalar(
                select(EarlyMarketEventRecord).where(
                    EarlyMarketEventRecord.source == event.source,
                    EarlyMarketEventRecord.source_native_event_id == native_id,
                )
            )
            assert existing is not None
            return existing
        await self._session.flush()
        return row

    async def append_evidence(
        self,
        *,
        event_id: int,
        evidence_kind: str,
        source: str,
        observed_time: datetime,
        status: str,
        source_native_evidence_id: str | None = None,
        received_time: datetime | None = None,
        endpoint: str | None = None,
        dataset: str | None = None,
        stable_locator: str | None = None,
        payload_metadata: dict[str, Any] | None = None,
    ) -> EarlyMarketEventEvidence:
        observed = require_utc(observed_time)
        received = require_utc(received_time) if received_time is not None else None
        row = EarlyMarketEventEvidence(
            event_id=event_id,
            evidence_kind=evidence_kind,
            source=source,
            source_native_evidence_id=source_native_evidence_id,
            observed_time=observed,
            received_time=received,
            endpoint=endpoint,
            dataset=dataset,
            stable_locator=stable_locator,
            status=status,
            payload_metadata=payload_metadata,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def insert_observation(
        self,
        *,
        market_id: int,
        source_native_observation_id: str,
        source: str,
        source_time: datetime,
        availability_status: str,
        received_time: datetime | None = None,
        event_id: int | None = None,
        price: Decimal | None = None,
        quantity: Decimal | None = None,
        liquidity: Decimal | None = None,
        base_reserve: Decimal | None = None,
        quote_reserve: Decimal | None = None,
        side: str | None = None,
        resolution: str | None = None,
        provenance_json: dict[str, Any] | None = None,
    ) -> EarlyMarketObservation:
        native_id = _require_native_id(source_native_observation_id, field="source_native_observation_id")
        if not isinstance(availability_status, str) or not availability_status:
            raise ValueError("availability_status must be an exact declared non-empty value")
        source_ts = require_utc(source_time)
        received_ts = require_utc(received_time) if received_time is not None else None
        if received_ts is None and availability_status in _RECEIPT_CLAIMING_STATUSES:
            raise ValueError("cannot claim receipt_verified when received_time is missing")

        stmt = (
            insert(EarlyMarketObservation)
            .values(
                market_id=market_id,
                event_id=event_id,
                source_native_observation_id=native_id,
                source=source,
                source_time=source_ts,
                received_time=received_ts,
                availability_status=availability_status,
                price=price,
                quantity=quantity,
                liquidity=liquidity,
                base_reserve=base_reserve,
                quote_reserve=quote_reserve,
                side=side,
                resolution=resolution,
                provenance_json=provenance_json,
            )
            .on_conflict_do_nothing(constraint="uq_early_market_observations_source_native_id")
            .returning(EarlyMarketObservation)
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            existing = await self._session.scalar(
                select(EarlyMarketObservation).where(
                    EarlyMarketObservation.source == source,
                    EarlyMarketObservation.source_native_observation_id == native_id,
                )
            )
            assert existing is not None
            return existing
        await self._session.flush()
        return row

    async def list_events(self, *, limit: int) -> list[EarlyMarketEventRecord]:
        bound = _require_bound(limit)
        result = await self._session.scalars(
            select(EarlyMarketEventRecord)
            .order_by(EarlyMarketEventRecord.source_event_time, EarlyMarketEventRecord.id)
            .limit(bound)
        )
        return list(result.all())

    async def list_observations(self, *, limit: int) -> list[EarlyMarketObservation]:
        bound = _require_bound(limit)
        result = await self._session.scalars(
            select(EarlyMarketObservation)
            .order_by(EarlyMarketObservation.source_time, EarlyMarketObservation.id)
            .limit(bound)
        )
        return list(result.all())
