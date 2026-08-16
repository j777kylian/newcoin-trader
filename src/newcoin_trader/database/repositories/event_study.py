"""Bounded read queries for Phase 3 event-study research (no schema changes)."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from newcoin_trader.database.models import PriceSnapshot, Token
from newcoin_trader.domain.event_study import MarketObservation, TokenListingEvent
from newcoin_trader.domain.types import require_utc
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.event_study_normalize import build_listing_event, build_market_observation


class EventStudyRepository:
    """Application/research reads only; never writes."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def _token_query(
        self,
        *,
        venue: str,
        start: datetime,
        end: datetime,
        max_events: int,
    ) -> Select[tuple[Token]]:
        start_utc = require_utc(start)
        end_utc = require_utc(end)
        # Prefer source_event ordering via COALESCE(created_time, first_seen_time).
        event_time = func.coalesce(Token.created_time, Token.first_seen_time)
        return (
            select(Token)
            .where(
                Token.venue == venue,
                event_time >= start_utc,
                event_time < end_utc,
            )
            .order_by(event_time.asc(), Token.id.asc())
            .limit(max_events)
        )

    async def list_listing_events(
        self,
        *,
        venue: str,
        start: datetime,
        end: datetime,
        max_events: int,
    ) -> list[TokenListingEvent]:
        result = await self._session.execute(
            self._token_query(venue=venue, start=start, end=end, max_events=max_events)
        )
        tokens = list(result.scalars().all())
        if not tokens:
            return []

        token_ids = [t.id for t in tokens]
        first_md_rows = await self._session.execute(
            select(PriceSnapshot.token_id, func.min(PriceSnapshot.timestamp))
            .where(PriceSnapshot.token_id.in_(token_ids))
            .group_by(PriceSnapshot.token_id)
        )
        first_md = {int(tid): ts for tid, ts in first_md_rows.all()}

        events: list[TokenListingEvent] = []
        for token in tokens:
            events.append(
                build_listing_event(
                    token_id=token.id,
                    token_address=token.token_address,
                    chain=token.chain,
                    symbol=token.symbol,
                    source=token.source,
                    venue=token.venue,
                    created_time=token.created_time,
                    first_seen_time=token.first_seen_time,
                    first_market_data_time=first_md.get(token.id),
                    metadata_json=token.metadata_json,
                )
            )
        return events

    def _observations_query(
        self,
        *,
        token_ids: list[int],
        venue: str,
        start: datetime,
        end: datetime,
        max_observations: int,
    ) -> Select[tuple[PriceSnapshot, Token]]:
        start_utc = require_utc(start)
        end_utc = require_utc(end)
        # Fetch budget+1 so callers can detect overflow without unbounded materialization.
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
            .limit(max_observations + 1)
        )

    async def list_observations_for_tokens(
        self,
        *,
        token_ids: list[int],
        venue: str,
        start: datetime,
        end: datetime,
        max_observations: int,
    ) -> list[MarketObservation]:
        if not token_ids:
            return []
        result = await self._session.execute(
            self._observations_query(
                token_ids=token_ids,
                venue=venue,
                start=start,
                end=end,
                max_observations=max_observations,
            )
        )
        rows = result.all()
        if len(rows) > max_observations:
            raise ConfigError(
                f"observation read budget exceeded: more than {max_observations} snapshots "
                "in the event+grid window; raise max_observations or narrow the study bounds"
            )
        observations: list[MarketObservation] = []
        for snap, token in rows:
            observations.append(
                build_market_observation(
                    token_address=token.token_address,
                    chain=token.chain,
                    venue=token.venue,
                    timestamp=snap.timestamp,
                    price=snap.price,
                    source=snap.source,
                    provenance=snap.provenance,
                )
            )
        return observations
