"""Bounded read-only Phase 5 executable-backtest repository (no schema writes)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from newcoin_trader.database.models import PriceSnapshot, Token, Trade
from newcoin_trader.database.repositories.event_study import EventStudyRepository
from newcoin_trader.domain.event_study import TokenListingEvent
from newcoin_trader.domain.executable_backtest import (
    ExecutionMarketObservation,
    ExecutionTradeTick,
    HistoricalDepthBook,
)
from newcoin_trader.domain.feature_research import DecisionFeatureRecord
from newcoin_trader.domain.types import require_utc
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.event_study_normalize import parse_venue
from newcoin_trader.research.event_study_resolution import resolution_from_provenance


def _as_str_dict(value: dict[str, Any] | None) -> dict[str, str] | None:
    if not value:
        return None
    return {str(k): str(v) for k, v in value.items() if v is not None}


class ExecutableBacktestRepository:
    """Application/research reads only; never writes. No historical depth table exists."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._events = EventStudyRepository(session)

    async def list_listing_events(
        self,
        *,
        venue: str,
        start: datetime,
        end: datetime,
        max_events: int,
    ) -> list[TokenListingEvent]:
        return await self._events.list_listing_events(
            venue=venue,
            start=start,
            end=end,
            max_events=max_events,
        )

    def _observations_query(
        self,
        *,
        token_ids: list[int],
        venue: str,
        start: datetime,
        end: datetime,
        max_execution_inputs: int,
    ) -> Select[tuple[PriceSnapshot, Token]]:
        start_utc = require_utc(start)
        end_utc = require_utc(end)
        return (
            select(PriceSnapshot, Token)
            .join(Token, Token.id == PriceSnapshot.token_id)
            .where(
                PriceSnapshot.token_id.in_(token_ids),
                Token.venue == venue,
                PriceSnapshot.timestamp >= start_utc,
                PriceSnapshot.timestamp <= end_utc,
            )
            .order_by(PriceSnapshot.timestamp.asc(), PriceSnapshot.id.asc())
            .limit(max_execution_inputs + 1)
        )

    async def list_execution_observations(
        self,
        *,
        token_ids: list[int],
        venue: str,
        start: datetime,
        end: datetime,
        max_execution_inputs: int,
    ) -> list[ExecutionMarketObservation]:
        if not token_ids:
            return []
        result = await self._session.execute(
            self._observations_query(
                token_ids=token_ids,
                venue=venue,
                start=start,
                end=end,
                max_execution_inputs=max_execution_inputs,
            )
        )
        rows = result.all()
        if len(rows) > max_execution_inputs:
            raise ConfigError(
                f"execution input read budget exceeded: more than {max_execution_inputs} "
                "snapshots in the event+latency+holding bounds; raise max_execution_inputs "
                "or narrow the study bounds"
            )
        observations: list[ExecutionMarketObservation] = []
        for snap, token in rows:
            resolved_venue = parse_venue(token.venue, fallback_source=snap.source)
            observations.append(
                ExecutionMarketObservation(
                    token_address=token.token_address,
                    chain=token.chain,
                    venue=resolved_venue,
                    timestamp=snap.timestamp,
                    price=snap.price,
                    liquidity=snap.liquidity,
                    volume=snap.volume,
                    resolution=resolution_from_provenance(snap.provenance, source=snap.source),
                    source=snap.source,
                    provenance=_as_str_dict(snap.provenance),
                )
            )
        return observations

    def _trades_query(
        self,
        *,
        token_ids: list[int],
        venue: str,
        start: datetime,
        end: datetime,
        max_trades: int,
    ) -> Select[tuple[Trade, Token]]:
        start_utc = require_utc(start)
        end_utc = require_utc(end)
        return (
            select(Trade, Token)
            .join(Token, Token.id == Trade.token_id)
            .where(
                Trade.token_id.in_(token_ids),
                Token.venue == venue,
                Trade.timestamp >= start_utc,
                Trade.timestamp <= end_utc,
            )
            .order_by(Trade.timestamp.asc(), Trade.id.asc())
            .limit(max_trades + 1)
        )

    async def list_execution_trades(
        self,
        *,
        token_ids: list[int],
        venue: str,
        start: datetime,
        end: datetime,
        max_trades: int,
    ) -> list[ExecutionTradeTick]:
        if not token_ids:
            return []
        result = await self._session.execute(
            self._trades_query(
                token_ids=token_ids,
                venue=venue,
                start=start,
                end=end,
                max_trades=max_trades,
            )
        )
        rows = result.all()
        if len(rows) > max_trades:
            raise ConfigError(
                f"trade read budget exceeded: more than {max_trades} trades "
                "in the execution window; raise max_trades or narrow bounds"
            )
        trades: list[ExecutionTradeTick] = []
        for trade, token in rows:
            resolved_venue = parse_venue(token.venue, fallback_source=trade.source)
            trades.append(
                ExecutionTradeTick(
                    token_address=token.token_address,
                    chain=token.chain,
                    venue=resolved_venue,
                    timestamp=trade.timestamp,
                    side=trade.side,
                    amount=trade.amount,
                    price=trade.price,
                    source=trade.source,
                    provenance=_as_str_dict(trade.provenance),
                )
            )
        return trades

    async def list_historical_depth(
        self,
        *,
        token_ids: list[int],
        venue: str,
        start: datetime,
        end: datetime,
        max_execution_inputs: int,
    ) -> list[HistoricalDepthBook]:
        """Explicit no-op: the database has no historical depth table.

        Callers may still supply HistoricalDepthBook inputs in-memory for exact
        depth walking. This method documents the unsupported persisted capability
        and always returns an empty list without fabricating books.
        """
        _ = (token_ids, venue, start, end, max_execution_inputs)
        return []

    async def list_decision_records(
        self,
        *,
        events: list[TokenListingEvent],
        records: list[DecisionFeatureRecord] | None = None,
    ) -> list[DecisionFeatureRecord]:
        """Phase 5 does not recompute Phase 4 features.

        When precomputed records are supplied (tests/service wiring), return the
        subset matching selected events. Production callers must pass frozen
        records from a prior Phase 4 artifact — never rediscover rules here.
        """
        if records is None:
            return []
        event_ids = {e.event_id for e in events}
        return [r for r in records if r.event_id in event_ids]
