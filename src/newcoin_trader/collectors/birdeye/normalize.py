"""Normalize Birdeye discovery payloads into domain records."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from newcoin_trader.collectors.normalization import parse_venue_time, require_list, require_mapping
from newcoin_trader.domain.enums import Chain, Venue
from newcoin_trader.domain.tokens import NewListingEvent
from newcoin_trader.domain.types import maybe_decimal


def _chain(value: str) -> Chain:
    if value.lower() == "solana":
        return Chain.SOLANA
    return Chain.SOLANA


def normalize_new_tokens(payload: Any, *, chain: str, seen_at: datetime | None = None) -> list[NewListingEvent]:
    data = require_mapping(payload, context="birdeye.new_tokens")
    inner = data.get("data", data)
    items_raw = inner.get("items") if isinstance(inner, dict) else inner
    items = require_list(items_raw, context="birdeye.new_tokens.items")
    now = seen_at or datetime.now(UTC)
    events: list[NewListingEvent] = []
    for raw in items:
        item = require_mapping(raw, context="birdeye.token")
        address = str(item["address"])
        listed = item.get("liquidityAddedAt") or item.get("createdAt")
        created = parse_venue_time(listed) if listed is not None else None
        events.append(
            NewListingEvent(
                token_address=address,
                chain=_chain(chain),
                symbol=str(item.get("symbol") or address[:6]),
                name=str(item["name"]) if item.get("name") else None,
                created_time=created,
                first_seen_time=now,
                source="birdeye",
                venue=Venue.BIRDEYE,
                liquidity=maybe_decimal(item.get("liquidity")),
                provenance={"endpoint": "/defi/v2/tokens/new_listing"},
            )
        )
    return events


def normalize_new_pairs(payload: Any, *, chain: str, seen_at: datetime | None = None) -> list[NewListingEvent]:
    data = require_mapping(payload, context="birdeye.new_pairs")
    inner = data.get("data", data)
    events: list[NewListingEvent] = []
    now = seen_at or datetime.now(UTC)
    items: list[Any] = []
    if isinstance(inner, dict) and "items" in inner:
        for group in require_list(inner.get("items"), context="birdeye.search.items"):
            group_map = require_mapping(group, context="birdeye.search.group")
            items.extend(require_list(group_map.get("result") or [], context="birdeye.search.result"))
    elif isinstance(inner, dict) and "pairs" in inner:
        items = require_list(inner.get("pairs"), context="birdeye.pairs")
    else:
        items = require_list(inner if isinstance(inner, list) else [], context="birdeye.pairs.fallback")
    for raw in items:
        item = require_mapping(raw, context="birdeye.pair")
        base_raw = item.get("base")
        base: dict[str, Any] = base_raw if isinstance(base_raw, dict) else {}
        address = str(base.get("address") or item.get("address") or item.get("base_address") or "")
        if not address:
            continue
        created_raw = item.get("created_at") or item.get("blockTime") or item.get("createdAt")
        created = parse_venue_time(created_raw) if created_raw is not None else None
        symbol = str(base.get("symbol") or item.get("symbol") or item.get("name") or address[:6])
        events.append(
            NewListingEvent(
                token_address=address,
                chain=_chain(chain),
                symbol=symbol,
                name=str(item.get("name")) if item.get("name") else None,
                created_time=created,
                first_seen_time=now,
                source="birdeye",
                venue=Venue.BIRDEYE,
                pair_address=str(item.get("address") or item.get("pair_address") or ""),
                provenance={"endpoint": "/defi/v3/search", "target": "pair"},
            )
        )
    return events
