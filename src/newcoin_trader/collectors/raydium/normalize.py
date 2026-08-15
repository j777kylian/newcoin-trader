"""Normalize Raydium pool and read-only quote payloads."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from newcoin_trader.collectors.normalization import parse_venue_time, require_list, require_mapping
from newcoin_trader.domain.market import PoolQuote, PoolSnapshot
from newcoin_trader.domain.types import as_decimal, maybe_decimal, utc_from_millis
from newcoin_trader.errors import ParseError

# Prefer stable remote observation fields when the venue provides them.
_REMOTE_OBSERVATION_KEYS = ("openTime", "updateTime", "timestamp", "slot", "blockTime")


def _canonical_response_identity(item: dict[str, Any]) -> str:
    day_raw = item.get("day")
    day: dict[str, Any] = day_raw if isinstance(day_raw, dict) else {}
    mint_a_raw = item.get("mintA")
    mint_b_raw = item.get("mintB")
    mint_a: dict[str, Any] = mint_a_raw if isinstance(mint_a_raw, dict) else {}
    mint_b: dict[str, Any] = mint_b_raw if isinstance(mint_b_raw, dict) else {}
    canonical = {
        "id": item.get("id") or item.get("address"),
        "price": item.get("price"),
        "tvl": item.get("tvl") or item.get("liquidity"),
        "volume": day.get("volume") if day else item.get("volume24h"),
        "mintA": mint_a.get("address") or item.get("baseMint"),
        "mintB": mint_b.get("address") or item.get("quoteMint"),
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _observation_timestamp(item: dict[str, Any]) -> tuple[datetime, str]:
    """Remote observation time, or a deterministic synthetic time from payload identity."""
    for key in _REMOTE_OBSERVATION_KEYS:
        if key in item and item[key] is not None:
            return parse_venue_time(item[key]), f"remote:{key}"
    identity = _canonical_response_identity(item)
    # Stable positive millis derived only from payload bytes — never local wall clock.
    ms = 1_700_000_000_000 + (int(identity[:12], 16) % 10_000_000_000)
    return utc_from_millis(ms), f"canonical:{identity}"


def normalize_pool_list(payload: Any, *, timestamp: datetime | None = None) -> list[PoolSnapshot]:
    data = require_mapping(payload, context="raydium.pools")
    inner = data.get("data", data)
    rows = inner.get("data") if isinstance(inner, dict) else inner
    rows = require_list(rows, context="raydium.pools.data")
    pools: list[PoolSnapshot] = []
    for raw in rows:
        item = require_mapping(raw, context="raydium.pool")
        mint_a_raw = item.get("mintA")
        mint_b_raw = item.get("mintB")
        mint_a: dict[str, Any] = mint_a_raw if isinstance(mint_a_raw, dict) else {}
        mint_b: dict[str, Any] = mint_b_raw if isinstance(mint_b_raw, dict) else {}
        day_raw = item.get("day")
        day: dict[str, Any] = day_raw if isinstance(day_raw, dict) else {}
        # Explicit caller timestamp remains an override for tests; default is never now().
        if timestamp is not None:
            ts = timestamp
            identity = _canonical_response_identity(item)
            obs_kind = "caller_override"
        else:
            ts, obs_kind = _observation_timestamp(item)
            identity = (
                obs_kind.removeprefix("canonical:")
                if obs_kind.startswith("canonical:")
                else _canonical_response_identity(item)
            )
        pools.append(
            PoolSnapshot(
                pool_address=str(item.get("id") or item.get("address")),
                chain="solana",
                base_mint=str(mint_a.get("address") or item.get("baseMint") or ""),
                quote_mint=str(mint_b.get("address") or item.get("quoteMint") or ""),
                timestamp=ts,
                price=maybe_decimal(item.get("price")),
                liquidity=maybe_decimal(item.get("tvl") or item.get("liquidity")),
                volume_24h=maybe_decimal(day.get("volume") or item.get("volume24h")),
                name=f"{mint_a.get('symbol', '')}/{mint_b.get('symbol', '')}".strip("/") or None,
                source="raydium",
                provenance={
                    "endpoint": "/pools/info/list",
                    "response_identity": identity,
                    "observation": obs_kind,
                },
            )
        )
    return pools


def normalize_quote(payload: Any) -> PoolQuote:
    data = require_mapping(payload, context="raydium.quote")
    inner = data.get("data", data)
    item = require_mapping(inner, context="raydium.quote.data")
    if "outputAmount" not in item:
        raise ParseError("raydium quote missing outputAmount")
    return PoolQuote(
        input_mint=str(item["inputMint"]),
        output_mint=str(item["outputMint"]),
        input_amount=as_decimal(item["inputAmount"]),
        output_amount=as_decimal(item["outputAmount"]),
        other_amount_threshold=maybe_decimal(item.get("otherAmountThreshold")),
        slippage_bps=int(item.get("slippageBps") or 0),
        price_impact_pct=maybe_decimal(item.get("priceImpactPct")),
        source="raydium",
        quote_id=str(data["id"]) if data.get("id") else None,
        provenance={"endpoint": "/compute/swap-base-in", "read_only": "true"},
    )
