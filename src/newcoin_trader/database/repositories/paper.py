"""Paper-trade persistence with replay-safe idempotency."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.dml import Insert

from newcoin_trader.database.models import PaperTrade
from newcoin_trader.domain.enums import PaperStatus, RejectReason, Side


def paper_trade_upsert_statement(
    *,
    run_id: str | UUID,
    token_id: int,
    signal_ts: datetime,
    side: str,
    requested_qty: Decimal,
    requested_price: Decimal,
    fill_price: Decimal | None,
    fill_qty: Decimal | None,
    fee: Decimal | None,
    slippage_bps: Decimal | None,
    status: str,
    reject_reason: str | None,
    meta_json: dict[str, Any] | None = None,
) -> Insert:
    stmt = insert(PaperTrade).values(
        run_id=UUID(str(run_id)),
        token_id=token_id,
        signal_ts=signal_ts,
        side=side,
        requested_qty=requested_qty,
        requested_price=requested_price,
        fill_price=fill_price,
        fill_qty=fill_qty,
        fee=fee,
        slippage_bps=slippage_bps,
        status=status,
        reject_reason=reject_reason,
        mode="paper",
        meta_json=meta_json,
    )
    return stmt.on_conflict_do_nothing(constraint="uq_paper_trades_run_order")


class PaperTradeRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(
        self,
        *,
        run_id: str | UUID,
        token_id: int,
        signal_ts: datetime,
        side: Side,
        requested_qty: Decimal,
        requested_price: Decimal,
        fill_price: Decimal | None,
        fill_qty: Decimal | None,
        fee: Decimal | None,
        slippage_bps: Decimal | None,
        status: PaperStatus,
        reject_reason: RejectReason | str | None,
        meta_json: dict[str, Any] | None = None,
    ) -> PaperTrade:
        reason = str(reject_reason) if reject_reason is not None else None
        stmt = paper_trade_upsert_statement(
            run_id=run_id,
            token_id=token_id,
            signal_ts=signal_ts,
            side=str(side),
            requested_qty=requested_qty,
            requested_price=requested_price,
            fill_price=fill_price,
            fill_qty=fill_qty,
            fee=fee,
            slippage_bps=slippage_bps,
            status=str(status),
            reject_reason=reason,
            meta_json=meta_json,
        ).returning(PaperTrade)
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            existing = await self._session.scalar(
                select(PaperTrade).where(
                    PaperTrade.run_id == UUID(str(run_id)),
                    PaperTrade.token_id == token_id,
                    PaperTrade.signal_ts == signal_ts,
                    PaperTrade.side == str(side),
                    PaperTrade.requested_qty == requested_qty,
                    PaperTrade.requested_price == requested_price,
                )
            )
            assert existing is not None
            return existing
        await self._session.flush()
        return row
