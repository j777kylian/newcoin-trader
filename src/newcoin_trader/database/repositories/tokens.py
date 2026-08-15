"""Idempotent token upserts. first_seen_time is never moved later."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.dml import Insert

from newcoin_trader.database.models import Token


def token_upsert_statement(
    *,
    chain: str,
    token_address: str,
    symbol: str,
    created_time: datetime | None,
    first_seen_time: datetime,
    source: str,
    venue: str | None = None,
    metadata_json: dict[str, Any] | None = None,
) -> Insert:
    stmt = insert(Token).values(
        chain=chain,
        token_address=token_address,
        symbol=symbol,
        created_time=created_time,
        first_seen_time=first_seen_time,
        source=source,
        venue=venue,
        metadata_json=metadata_json,
    )
    return stmt.on_conflict_do_update(
        constraint="uq_tokens_chain_address",
        set_={
            "symbol": stmt.excluded.symbol,
            "created_time": func.coalesce(Token.created_time, stmt.excluded.created_time),
            "first_seen_time": func.least(Token.first_seen_time, stmt.excluded.first_seen_time),
            "venue": func.coalesce(stmt.excluded.venue, Token.venue),
            "metadata_json": func.coalesce(stmt.excluded.metadata_json, Token.metadata_json),
            "updated_at": func.now(),
        },
    )


class TokenRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

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
    ) -> Token:
        stmt = (
            token_upsert_statement(
                chain=chain,
                token_address=token_address,
                symbol=symbol,
                created_time=created_time,
                first_seen_time=first_seen_time,
                source=source,
                venue=venue,
                metadata_json=metadata_json,
            )
            .returning(Token)
            .execution_options(populate_existing=True)
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one()
        await self._session.flush()
        return row
