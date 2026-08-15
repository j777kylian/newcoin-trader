"""Normalize GeckoTerminal JSON:API pool and OHLCV payloads."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from newcoin_trader.collectors.normalization import parse_venue_time, require_list, require_mapping
from newcoin_trader.domain.enums import Venue
from newcoin_trader.domain.market import Kline, PoolSnapshot
from newcoin_trader.domain.types import as_decimal, maybe_decimal, utc_from_seconds
from newcoin_trader.errors import ParseError


def normalize_pool(payload: Any, *, network: str) -> PoolSnapshot:
    data = require_mapping(payload, context="gecko.pool")
    node = data.get("data", data)
    node = require_mapping(node, context="gecko.pool.data")
    attrs = require_mapping(node.get("attributes"), context="gecko.pool.attributes")
    included = {item.get("id"): item for item in data.get("included") or [] if isinstance(item, dict)}
    rel = node.get("relationships") if isinstance(node.get("relationships"), dict) else {}
    base_rel = ((rel.get("base_token") or {}).get("data") or {}) if isinstance(rel, dict) else {}
    quote_rel = ((rel.get("quote_token") or {}).get("data") or {}) if isinstance(rel, dict) else {}
    base = included.get(base_rel.get("id"), {})
    quote = included.get(quote_rel.get("id"), {})
    base_obj: dict[str, Any] = base if isinstance(base, dict) else {}
    quote_obj: dict[str, Any] = quote if isinstance(quote, dict) else {}
    base_attrs_raw = base_obj.get("attributes")
    quote_attrs_raw = quote_obj.get("attributes")
    base_attrs: dict[str, Any] = base_attrs_raw if isinstance(base_attrs_raw, dict) else {}
    quote_attrs: dict[str, Any] = quote_attrs_raw if isinstance(quote_attrs_raw, dict) else {}
    created = attrs.get("pool_created_at")
    ts = parse_venue_time(created) if created else datetime.now(UTC)
    volume = attrs.get("volume_usd")
    volume_24h = None
    if isinstance(volume, dict):
        volume_24h = maybe_decimal(volume.get("h24"))
    return PoolSnapshot(
        pool_address=str(attrs.get("address") or node.get("id")),
        chain=network,
        base_mint=str(base_attrs.get("address") or ""),
        quote_mint=str(quote_attrs.get("address") or ""),
        timestamp=ts,
        price=maybe_decimal(attrs.get("base_token_price_usd")),
        liquidity=maybe_decimal(attrs.get("reserve_in_usd")),
        volume_24h=volume_24h,
        name=str(attrs["name"]) if attrs.get("name") else None,
        source="geckoterminal",
        provenance={"endpoint": f"/networks/{network}/pools"},
    )


def normalize_ohlcv(
    payload: Any,
    *,
    pool_address: str,
    network: str,
    interval: str,
) -> list[Kline]:
    data = require_mapping(payload, context="gecko.ohlcv")
    node = data.get("data", data)
    node = require_mapping(node, context="gecko.ohlcv.data")
    attrs = require_mapping(node.get("attributes"), context="gecko.ohlcv.attributes")
    rows = require_list(attrs.get("ohlcv_list"), context="gecko.ohlcv.list")
    klines: list[Kline] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 6:
            raise ParseError("ohlcv row must be [ts, o, h, l, c, v]")
        open_time = utc_from_seconds(int(row[0]))
        klines.append(
            Kline(
                token_address=pool_address,
                chain=network,
                open_time=open_time,
                close_time=open_time,
                open=as_decimal(row[1]),
                high=as_decimal(row[2]),
                low=as_decimal(row[3]),
                close=as_decimal(row[4]),
                volume=as_decimal(row[5]),
                interval=interval,
                source="geckoterminal",
                venue=Venue.GECKO,
            )
        )
    return klines
