"""Strategy research-result persistence with replay-safe idempotency."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.dml import Insert

from newcoin_trader.database.models import StrategyResult


def _eq_nullable(column: Any, value: Any) -> Any:
    if value is None:
        return column.is_(None)
    return column == value


def strategy_result_upsert_statement(
    *,
    run_id: str | UUID,
    strategy_name: str,
    strategy_version: str,
    token_id: int | None,
    params: dict[str, Any],
    metrics: dict[str, Any],
    signals: list[Any] | None,
    window_start: datetime | None,
    window_end: datetime | None,
) -> Insert:
    stmt = insert(StrategyResult).values(
        run_id=UUID(str(run_id)),
        strategy_name=strategy_name,
        strategy_version=strategy_version,
        token_id=token_id,
        params_json=params,
        metrics_json=metrics,
        signals_json=signals,
        window_start=window_start,
        window_end=window_end,
    )
    return stmt.on_conflict_do_nothing(constraint="uq_strategy_results_run_strategy_token_window")


class StrategyResultRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(
        self,
        *,
        run_id: str | UUID,
        strategy_name: str,
        strategy_version: str,
        token_id: int | None,
        params: dict[str, Any],
        metrics: dict[str, Any],
        signals: list[Any] | None,
        window_start: datetime | None,
        window_end: datetime | None,
    ) -> StrategyResult:
        stmt = strategy_result_upsert_statement(
            run_id=run_id,
            strategy_name=strategy_name,
            strategy_version=strategy_version,
            token_id=token_id,
            params=params,
            metrics=metrics,
            signals=signals,
            window_start=window_start,
            window_end=window_end,
        ).returning(StrategyResult)
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            existing = await self._session.scalar(
                select(StrategyResult).where(
                    StrategyResult.run_id == UUID(str(run_id)),
                    StrategyResult.strategy_name == strategy_name,
                    StrategyResult.strategy_version == strategy_version,
                    _eq_nullable(StrategyResult.token_id, token_id),
                    _eq_nullable(StrategyResult.window_start, window_start),
                    _eq_nullable(StrategyResult.window_end, window_end),
                )
            )
            assert existing is not None
            return existing
        await self._session.flush()
        return row
