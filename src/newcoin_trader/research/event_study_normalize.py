"""Normalize stored Token/PriceSnapshot rows into Phase 3 event-study records."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any

from newcoin_trader.domain.enums import Chain, Venue
from newcoin_trader.domain.event_study import MarketObservation, TokenListingEvent
from newcoin_trader.domain.types import require_utc
from newcoin_trader.research.event_study_resolution import resolution_from_provenance


def _as_str_dict(value: Mapping[str, Any] | None) -> dict[str, str]:
    if not value:
        return {}
    return {str(k): str(v) for k, v in value.items() if v is not None}


def _parse_venue(raw: str | None, *, fallback_source: str) -> Venue:
    candidate = (raw or fallback_source or "").strip().lower()
    # Sources may be namespaced (binance:aggTrades); take the head token.
    head = candidate.split(":", 1)[0]
    for venue in Venue:
        if venue.value == head or venue.value == candidate:
            return venue
    # Map common aliases
    if head in {"geckoterminal", "gecko"}:
        return Venue.GECKO
    raise ValueError(f"unsupported venue for event-study: {raw!r} / {fallback_source!r}")


def _parse_chain(raw: str) -> Chain:
    text = raw.strip().lower()
    for chain in Chain:
        if chain.value == text:
            return chain
    raise ValueError(f"unsupported chain for event-study: {raw!r}")


def build_listing_event(
    *,
    token_id: int,
    token_address: str,
    chain: str,
    symbol: str,
    source: str,
    venue: str | None,
    created_time: datetime | None,
    first_seen_time: datetime,
    first_market_data_time: datetime | None,
    metadata_json: Mapping[str, Any] | None = None,
) -> TokenListingEvent:
    """Build a listing event without claiming universal launch semantics.

    ``source_event_time`` uses ``created_time`` when available; otherwise falls
    back to ``first_seen_time`` and records that fallback in provenance.
    ``decision_available_time`` is ``first_seen_time`` (discovery clock).
    """
    first_seen = require_utc(first_seen_time)
    created = require_utc(created_time) if created_time is not None else None
    first_md = require_utc(first_market_data_time) if first_market_data_time is not None else None
    resolved_venue = _parse_venue(venue, fallback_source=source)
    resolved_chain = _parse_chain(chain)

    provenance = _as_str_dict(metadata_json)
    provenance["token_id"] = str(token_id)
    provenance["normalization"] = "token_listing_event_v1"
    if created is not None:
        source_event_time = created
        provenance["source_event_time_field"] = "created_time"
    else:
        source_event_time = first_seen
        provenance["source_event_time_field"] = "first_seen_time_fallback"
        provenance["source_event_time_note"] = "created_time_absent_not_universal_launch"

    pair_address = provenance.get("pair_address") or provenance.get("pairAddress")
    event_id = f"{resolved_venue.value}:{resolved_chain.value}:{token_address}:{token_id}"
    return TokenListingEvent(
        event_id=event_id,
        venue=resolved_venue,
        chain=resolved_chain,
        token_address=token_address,
        pair_address=pair_address,
        symbol=symbol,
        source=source,
        source_event_time=source_event_time,
        first_seen_time=first_seen,
        first_market_data_time=first_md,
        decision_available_time=first_seen,
        provenance=provenance,
    )


def build_market_observation(
    *,
    token_address: str,
    chain: str,
    venue: str | None,
    timestamp: datetime,
    price: Decimal,
    source: str,
    provenance: Mapping[str, Any] | None = None,
) -> MarketObservation:
    resolved_venue = _parse_venue(venue, fallback_source=source)
    prov = _as_str_dict(provenance)
    resolution = resolution_from_provenance(provenance, source=source)
    return MarketObservation(
        token_address=token_address,
        chain=chain,
        venue=resolved_venue,
        timestamp=require_utc(timestamp),
        price=price,
        resolution=resolution,
        source=source,
        provenance=prov or None,
    )
