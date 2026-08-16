"""Bounded read-only Phase 4 feature-research repository (no schema writes)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from newcoin_trader.database.models import PriceSnapshot, Token, Trade
from newcoin_trader.database.repositories.event_study import EventStudyRepository
from newcoin_trader.domain.event_study import MarketObservation, TokenListingEvent
from newcoin_trader.domain.feature_research import FeatureMarketInput, FeatureTradeInput
from newcoin_trader.domain.types import require_utc
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.event_study_normalize import parse_venue
from newcoin_trader.research.event_study_resolution import resolution_from_provenance


def _as_str_dict(value: dict[str, Any] | None) -> dict[str, str] | None:
    if not value:
        return None
    return {str(k): str(v) for k, v in value.items() if v is not None}


class FeatureResearchRepository:
    """Application/research reads only; never writes. Reuses Phase 3 event listing."""

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

    def _feature_inputs_query(
        self,
        *,
        token_ids: list[int],
        venue: str,
        start: datetime,
        end: datetime,
        max_feature_inputs: int,
    ) -> Select[tuple[PriceSnapshot, Token]]:
        start_utc = require_utc(start)
        end_utc = require_utc(end)
        # budget+1 so callers detect overflow without unbounded materialization.
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
            .limit(max_feature_inputs + 1)
        )

    async def list_feature_inputs(
        self,
        *,
        token_ids: list[int],
        venue: str,
        start: datetime,
        end: datetime,
        max_feature_inputs: int,
    ) -> list[FeatureMarketInput]:
        if not token_ids:
            return []
        result = await self._session.execute(
            self._feature_inputs_query(
                token_ids=token_ids,
                venue=venue,
                start=start,
                end=end,
                max_feature_inputs=max_feature_inputs,
            )
        )
        rows = result.all()
        if len(rows) > max_feature_inputs:
            raise ConfigError(
                f"feature input read budget exceeded: more than {max_feature_inputs} snapshots "
                "in the event+window bounds; raise max_feature_inputs or narrow the study bounds"
            )
        inputs: list[FeatureMarketInput] = []
        for snap, token in rows:
            resolved_venue = parse_venue(token.venue, fallback_source=snap.source)
            prov = _as_str_dict(snap.provenance)
            resolution = resolution_from_provenance(snap.provenance, source=snap.source)
            inputs.append(
                FeatureMarketInput(
                    token_address=token.token_address,
                    chain=token.chain,
                    venue=resolved_venue,
                    timestamp=snap.timestamp,
                    price=snap.price,
                    volume=snap.volume,
                    liquidity=snap.liquidity,
                    resolution=resolution,
                    source=snap.source,
                    provenance=prov,
                )
            )
        return inputs

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

    async def list_feature_trades(
        self,
        *,
        token_ids: list[int],
        venue: str,
        start: datetime,
        end: datetime,
        max_trades: int,
    ) -> list[FeatureTradeInput]:
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
                "in the feature-input window; raise max_trades or narrow bounds"
            )
        trades: list[FeatureTradeInput] = []
        for trade, token in rows:
            resolved_venue = parse_venue(token.venue, fallback_source=trade.source)
            trades.append(
                FeatureTradeInput(
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

    async def list_label_observations(
        self,
        *,
        token_ids: list[int],
        venue: str,
        start: datetime,
        end: datetime,
        max_observations: int,
    ) -> list[MarketObservation]:
        """Separate Phase-3-compatible future-label price path (never mixed into features)."""
        return await self._events.list_observations_for_tokens(
            token_ids=token_ids,
            venue=venue,
            start=start,
            end=end,
            max_observations=max_observations,
        )
