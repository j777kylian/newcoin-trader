"""Idempotent market snapshot and trade writes."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.dml import Insert

from newcoin_trader.database.models import PriceSnapshot, Trade


def trade_upsert_statement(
    *,
    token_id: int,
    timestamp: datetime,
    side: str,
    amount: Decimal,
    price: Decimal,
    source: str,
    external_trade_id: str | None = None,
    provenance: dict[str, Any] | None = None,
) -> Insert:
    values = {
        "token_id": token_id,
        "timestamp": timestamp,
        "side": side,
        "amount": amount,
        "price": price,
        "source": source,
        "external_trade_id": external_trade_id,
        "provenance": provenance,
    }
    stmt = insert(Trade).values(**values)
    if external_trade_id is not None:
        return stmt.on_conflict_do_nothing(
            index_elements=["token_id", "source", "external_trade_id"],
            index_where=Trade.external_trade_id.is_not(None),
        )
    return stmt.on_conflict_do_nothing(
        index_elements=["token_id", "timestamp", "side", "amount", "price", "source"],
        index_where=Trade.external_trade_id.is_(None),
    )


class MarketRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

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
    ) -> PriceSnapshot:
        stmt = (
            insert(PriceSnapshot)
            .values(
                token_id=token_id,
                timestamp=timestamp,
                price=price,
                volume=volume,
                liquidity=liquidity,
                market_cap=market_cap,
                buy_count=buy_count,
                sell_count=sell_count,
                source=source,
                provenance=provenance,
            )
            .on_conflict_do_nothing(constraint="uq_price_snapshots_token_ts_source")
            .returning(PriceSnapshot)
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            existing = await self._session.scalar(
                select(PriceSnapshot).where(
                    PriceSnapshot.token_id == token_id,
                    PriceSnapshot.timestamp == timestamp,
                    PriceSnapshot.source == source,
                )
            )
            assert existing is not None
            return existing
        await self._session.flush()
        return row

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
    ) -> Trade:
        stmt = trade_upsert_statement(
            token_id=token_id,
            timestamp=timestamp,
            side=side,
            amount=amount,
            price=price,
            source=source,
            external_trade_id=external_trade_id,
            provenance=provenance,
        ).returning(Trade)
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            if external_trade_id is not None:
                query = select(Trade).where(
                    Trade.token_id == token_id,
                    Trade.source == source,
                    Trade.external_trade_id == external_trade_id,
                )
            else:
                query = select(Trade).where(
                    Trade.token_id == token_id,
                    Trade.timestamp == timestamp,
                    Trade.side == side,
                    Trade.amount == amount,
                    Trade.price == price,
                    Trade.source == source,
                )
            existing = await self._session.scalar(query)
            assert existing is not None
            return existing
        await self._session.flush()
        return row
