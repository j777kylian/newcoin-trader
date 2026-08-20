"""Phase 8A.3 source-agnostic projection into Phase 3 event-study inputs.

Adapts persisted early-market event + explicit token/market association and
observations into ``TokenListingEvent`` / ``MarketObservation``. Does not alter
the Phase 3 resolution engine.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from newcoin_trader.database.models import EarlyMarketEventRecord, EarlyMarketObservation, Market, Token
from newcoin_trader.domain.event_study import MarketObservation, TokenListingEvent
from newcoin_trader.research.event_study_normalize import _parse_chain, parse_venue
from newcoin_trader.research.event_study_resolution import resolution_from_provenance

_ASSOCIATION_EXACT = "exact_event_market_id"
_ASSOCIATION_LEGACY_TOKEN_ONLY = "legacy_binance_spot_listing_token_only"
_LEGACY_TOKEN_ONLY_KIND = "BINANCE_SPOT_LISTING"
_LEGACY_BINANCE_PHASE3_SOURCE = "binance:cms:catalog48"


def projected_event_id(source: str, source_native_event_id: str) -> str:
    """Deterministic Phase 3 event id from source + source-native identity only."""
    if not isinstance(source, str) or not source:
        raise ValueError("source is required for projected event id")
    if not isinstance(source_native_event_id, str) or not source_native_event_id:
        raise ValueError("source_native_event_id is required for projected event id")
    return f"{source}:{source_native_event_id}"


def _as_str_dict(value: Mapping[str, Any] | None) -> dict[str, str]:
    if not value:
        return {}
    return {str(k): str(v) for k, v in value.items() if v is not None}


def _legacy_binance_announcement_code(source_native_event_id: str) -> str:
    """Require bare nonempty announcement code; never parse/guess packed compound ids."""
    if not isinstance(source_native_event_id, str):
        raise ValueError("BINANCE_SPOT_LISTING source_native_event_id must be a nonempty announcement code")
    code = source_native_event_id.strip()
    if not code:
        raise ValueError("BINANCE_SPOT_LISTING source_native_event_id must be a nonempty announcement code")
    if ":" in code:
        raise ValueError(
            "BINANCE_SPOT_LISTING source_native_event_id must be the bare announcement code; "
            "refusing packed/compound native identity"
        )
    return code


def _resolve_market_association(
    event: EarlyMarketEventRecord,
    market: Market | None,
) -> tuple[Market | None, str]:
    if event.market_id is not None:
        if market is None:
            raise ValueError(
                "exact event.market_id requires a matching Market row; refusing missing market association"
            )
        if market.id != event.market_id:
            raise ValueError(
                f"market id mismatch: event.market_id={event.market_id} "
                f"!= market.id={market.id}; refusing ambiguous/mismatched linkage"
            )
        return market, _ASSOCIATION_EXACT

    if market is not None:
        raise ValueError(
            "refusing market binding without exact event.market_id (no symbol lookup / no first-of-many pools)"
        )

    if event.event_kind == _LEGACY_TOKEN_ONLY_KIND:
        return None, _ASSOCIATION_LEGACY_TOKEN_ONLY

    raise ValueError(
        f"non-legacy event_kind={event.event_kind!r} requires exact event.market_id "
        "with matching Market row; refusing token-only / missing market projection"
    )


def project_early_market_event(
    event: EarlyMarketEventRecord,
    *,
    token: Token,
    market: Market | None = None,
) -> TokenListingEvent:
    """Project a persisted early-market event into a Phase 3 ``TokenListingEvent``."""
    if token.id != event.asset_token_id:
        raise ValueError(f"token id mismatch: event.asset_token_id={event.asset_token_id} != token.id={token.id}")

    resolved_market, association_reason = _resolve_market_association(event, market)
    venue = parse_venue(event.venue_or_protocol, fallback_source=event.source)
    chain = _parse_chain(event.chain)

    pair_address = None if resolved_market is None else resolved_market.pool_or_pair_address
    provenance: dict[str, str] = {
        "source_native_event_id": event.source_native_event_id,
        "event_kind": event.event_kind,
        "event_definition_version": event.event_definition_version,
        "event_time_semantics": event.event_time_semantics,
        "event_quality_status": event.event_quality_status,
        "event_clock_quality": event.event_clock_quality,
        "provenance_ref": event.provenance_ref,
        "received_time": event.received_time.isoformat(),
        "market_association_reason": association_reason,
        "asset_token_id": str(event.asset_token_id),
    }
    if resolved_market is not None:
        provenance["market_id"] = str(resolved_market.id)
        provenance["market_key"] = resolved_market.market_key
        if resolved_market.source_native_market_id is not None:
            provenance["source_native_market_id"] = resolved_market.source_native_market_id

    if event.event_kind == _LEGACY_TOKEN_ONLY_KIND:
        announcement_code = _legacy_binance_announcement_code(event.source_native_event_id)
        if not isinstance(token.symbol, str) or not token.symbol.strip():
            raise ValueError("BINANCE_SPOT_LISTING requires nonempty token.symbol for legacy event id")
        # Phase 8.1 identity/source: do not use generic event.source for Phase 3 id/source.
        event_id = f"binance:binance:{token.symbol.strip()}:{announcement_code}"
        phase3_source = _LEGACY_BINANCE_PHASE3_SOURCE
        provenance["source_native_event_id"] = announcement_code
        # Concrete legacy event-clock provenance under current schema (no arbitrary provenance JSON).
        if event.source_event_time is not None:
            provenance["event_clock_field"] = "announced_spot_trading_start"
    else:
        event_id = projected_event_id(event.source, event.source_native_event_id)
        phase3_source = event.source

    return TokenListingEvent(
        event_id=event_id,
        venue=venue,
        chain=chain,
        token_address=token.token_address,
        pair_address=pair_address,
        symbol=token.symbol,
        source=phase3_source,
        source_event_time=event.source_event_time,
        first_seen_time=event.received_time,
        first_market_data_time=event.first_market_data_time,
        decision_available_time=event.decision_available_time,
        provenance=provenance,
    )


def project_early_market_observations(
    observations: Sequence[EarlyMarketObservation],
    *,
    token: Token,
    market: Market | None = None,
    venue_or_protocol: str | None = None,
    chain: str | None = None,
) -> list[MarketObservation]:
    """Project priced early-market observations into Phase 3 ``MarketObservation`` rows.

    Price-null rows are dropped (never fabricated as exact). Resolution is derived only
    via ``resolution_from_provenance`` from persisted provenance/source.
    """
    if market is not None:
        venue_raw = market.venue
        chain_raw = token.chain
    else:
        if venue_or_protocol is None or chain is None:
            raise ValueError("token-only observation projection requires explicit venue_or_protocol and chain")
        venue_raw = venue_or_protocol
        chain_raw = chain

    venue = parse_venue(venue_raw, fallback_source=venue_raw)
    # Validate chain against frozen enum (MarketObservation.chain remains str).
    _parse_chain(chain_raw)

    projected: list[MarketObservation] = []
    for row in observations:
        if market is not None and row.market_id != market.id:
            raise ValueError(f"observation market_id={row.market_id} does not match bound market.id={market.id}")
        if row.price is None:
            continue
        prov = _as_str_dict(row.provenance_json)
        prov["source_native_observation_id"] = row.source_native_observation_id
        prov["availability_status"] = row.availability_status
        prov["market_id"] = str(row.market_id)
        if row.event_id is not None:
            prov["event_id"] = str(row.event_id)
        resolution = resolution_from_provenance(row.provenance_json, source=row.source)
        projected.append(
            MarketObservation(
                token_address=token.token_address,
                chain=chain_raw,
                venue=venue,
                timestamp=row.source_time,
                price=row.price,
                resolution=resolution,
                source=row.source,
                provenance=prov,
            )
        )
    return projected
