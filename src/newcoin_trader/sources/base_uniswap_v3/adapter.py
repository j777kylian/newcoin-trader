"""In-memory Base Uniswap V3 adapter: verified evidence → historical facts only."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any, Self, cast

from pydantic import BaseModel, ConfigDict, field_validator

from newcoin_trader.domain.early_market_events import (
    AssetIdentity,
    EarlyMarketEventKind,
    EventAvailability,
    EventAvailabilityStatus,
    EventClockQuality,
    EventQualityStatus,
    EventTimeSemantics,
    HistoricalEarlyMarketEventFact,
    MarketIdentity,
)
from newcoin_trader.domain.types import require_utc
from newcoin_trader.sources.base_uniswap_v3.models import (
    CanonicalPoolCreatedEvidence,
    FactoryPoolCreatedRecord,
    FinalityBoundary,
    SwapLogRecord,
    VerifiedExactPoolSwapScanResult,
    VerifiedFactoryUniverse,
    assert_finality_for_block,
    normalize_address,
    require_non_bool_int,
    strict_reconstruct_model,
    validated_model_copy,
)

_CHAIN = "base"
_VENUE = "uniswap_v3"
_SOURCE = "base_uniswap_v3"
_EVENT_DEFINITION_VERSION = "8c.4.0"
_AVAILABILITY_POLICY_VERSION = "8c.4.0"


def validate_factory_universe(universe: VerifiedFactoryUniverse) -> tuple[FactoryPoolCreatedRecord, ...]:
    """Revalidate complete canonical factory evidence before exposing eligible pools."""
    validated = strict_reconstruct_model(VerifiedFactoryUniverse, universe)
    return tuple(item.creation for item in validated.candidates)


def build_eligible_scope(
    factory_universe: VerifiedFactoryUniverse,
    *,
    explicit_pool_allowlist: Sequence[str] | None = None,
) -> tuple[FactoryPoolCreatedRecord, ...]:
    universe = validate_factory_universe(factory_universe)
    if explicit_pool_allowlist is None:
        return universe
    if not isinstance(explicit_pool_allowlist, Sequence) or isinstance(explicit_pool_allowlist, (str, bytes)):
        raise ValueError("explicit_pool_allowlist must be a sequence of addresses")
    if not explicit_pool_allowlist:
        raise ValueError("explicit_pool_allowlist must be non-empty")

    allow: list[str] = []
    seen: set[str] = set()
    for raw in explicit_pool_allowlist:
        addr = normalize_address(raw, field_name="allowlist pool")
        if addr in seen:
            raise ValueError("duplicate allowlist pool address")
        seen.add(addr)
        allow.append(addr)

    by_pool = {record.pool_address: record for record in universe}
    missing = [addr for addr in allow if addr not in by_pool]
    if missing:
        raise ValueError(f"explicit allowlisted pool missing from factory universe: {missing[0]}")

    return tuple(by_pool[addr] for addr in allow)


def select_earliest_valid_swap(
    swaps: Sequence[SwapLogRecord],
    *,
    creation: FactoryPoolCreatedRecord,
) -> SwapLogRecord:
    if not isinstance(creation, FactoryPoolCreatedRecord):
        raise ValueError("creation record required")
    if not isinstance(swaps, Sequence) or isinstance(swaps, (str, bytes)):
        raise ValueError("swaps must be a sequence")

    creation_key = creation.order_key
    eligible: list[SwapLogRecord] = []
    for swap in swaps:
        if not isinstance(swap, SwapLogRecord):
            raise ValueError("swap entries must be SwapLogRecord")
        if swap.pool_address != creation.pool_address:
            continue
        if swap.order_key <= creation_key:
            raise ValueError("swap must be strictly after creation (lexicographic block/tx/log)")
        eligible.append(swap)
    if not eligible:
        raise ValueError("no eligible swap after creation")
    return min(eligible, key=lambda item: item.order_key)


def _require_decimals(value: object, *, field_name: str) -> int:
    decimals = require_non_bool_int(value, field_name=field_name, minimum=0)
    if decimals > 255:
        raise ValueError(f"{field_name} must be <= 255")
    return decimals


def compute_realized_execution_price(
    swap: SwapLogRecord,
    *,
    token0: str,
    token1: str,
    quote_allowlist: Sequence[str],
    decimals_by_address: Mapping[str, object],
) -> tuple[Decimal, str, str]:
    """Return (quote_per_base, base_address, quote_address). Never uses sqrtPriceX96."""
    if not isinstance(swap, SwapLogRecord):
        raise ValueError("swap required")
    t0 = normalize_address(token0, field_name="token0")
    t1 = normalize_address(token1, field_name="token1")
    if t0 == t1:
        raise ValueError("token0 and token1 must differ")

    if not isinstance(quote_allowlist, Sequence) or isinstance(quote_allowlist, (str, bytes)):
        raise ValueError("quote_allowlist must be a sequence")
    quotes = [normalize_address(item, field_name="quote allowlist") for item in quote_allowlist]
    if len(quotes) != len(set(quotes)):
        raise ValueError("quote_allowlist has duplicates")

    matches = [addr for addr in (t0, t1) if addr in quotes]
    if len(matches) != 1:
        raise ValueError("quote allowlist must match exactly one pool token")
    quote = matches[0]
    base = t1 if quote == t0 else t0

    if not isinstance(decimals_by_address, Mapping):
        raise ValueError("decimals_by_address must be a mapping")
    if base not in decimals_by_address or quote not in decimals_by_address:
        raise ValueError("missing decimals for base/quote")
    base_decimals = _require_decimals(decimals_by_address[base], field_name="base decimals")
    quote_decimals = _require_decimals(decimals_by_address[quote], field_name="quote decimals")

    amount0 = Decimal(swap.amount0)
    amount1 = Decimal(swap.amount1)
    if amount0 == 0 or amount1 == 0:
        raise ValueError("zero amount0/amount1 delta rejected")
    # Require opposite signs before abs/normalization (Uniswap V3 pool deltas).
    if (amount0 > 0) == (amount1 > 0):
        raise ValueError("amount0 and amount1 must have opposite signs")

    amount_base = amount0 if base == t0 else amount1
    amount_quote = amount0 if quote == t0 else amount1

    base_qty = abs(amount_base) / (Decimal(10) ** base_decimals)
    quote_qty = abs(amount_quote) / (Decimal(10) ** quote_decimals)
    if base_qty == 0 or quote_qty == 0:
        raise ValueError("zero scaled base/quote quantity rejected")
    return quote_qty / base_qty, base, quote


def _orient_base_quote(
    *,
    token0: str,
    token1: str,
    quote_allowlist: Sequence[str],
) -> tuple[str, str]:
    t0 = normalize_address(token0, field_name="token0")
    t1 = normalize_address(token1, field_name="token1")
    if t0 == t1:
        raise ValueError("token0 and token1 must differ")
    if not isinstance(quote_allowlist, Sequence) or isinstance(quote_allowlist, (str, bytes)):
        raise ValueError("quote_allowlist must be a sequence")
    quotes = [normalize_address(item, field_name="quote allowlist") for item in quote_allowlist]
    if len(quotes) != len(set(quotes)):
        raise ValueError("quote_allowlist has duplicates")
    matches = [addr for addr in (t0, t1) if addr in quotes]
    if len(matches) != 1:
        raise ValueError("quote allowlist must match exactly one pool token")
    quote = matches[0]
    base = t1 if quote == t0 else t0
    return base, quote


class AdapterRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    creation_evidence: CanonicalPoolCreatedEvidence
    exact_pool_swap_scan_result: VerifiedExactPoolSwapScanResult
    factory_universe: VerifiedFactoryUniverse
    explicit_pool_allowlist: tuple[str, ...]
    finality: FinalityBoundary
    quote_allowlist: tuple[str, ...]
    event_id: str
    provenance_ref: str

    @field_validator("explicit_pool_allowlist")
    @classmethod
    def _allowlist(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("explicit_pool_allowlist must be non-empty")
        out: list[str] = []
        seen: set[str] = set()
        for raw in value:
            addr = normalize_address(raw, field_name="allowlist pool")
            if addr in seen:
                raise ValueError("duplicate allowlist pool address")
            seen.add(addr)
            out.append(addr)
        return tuple(out)

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        """Copy through validation so nested scan/proof updates cannot bypass bindings."""
        return cast(Self, validated_model_copy(self, update=update, deep=deep))


def adapt_dex_first_trade(request: AdapterRequest) -> HistoricalEarlyMarketEventFact:
    """Accept already-verified evidence only; produce SOURCE_TIME_ONLY historical fact."""
    # Reconstruct before any business logic so model_construct cannot bypass nested
    # PoolCreated / Swap / scan digest / ledger / finality validators.
    validated = strict_reconstruct_model(AdapterRequest, request)

    creation = validated.creation_evidence.creation
    if creation.pool_address not in validated.explicit_pool_allowlist:
        raise ValueError("creation pool missing from explicit allowlist")

    scan_result = validated.exact_pool_swap_scan_result
    pool_proof = scan_result.pool_scan_proof

    factory_proof = validated.factory_universe.factory_scan_proof
    if validated.finality != factory_proof.finality:
        raise ValueError("factory scan proof finality must equal request finality")
    if validated.finality != pool_proof.finality:
        raise ValueError("pool scan proof finality must equal request finality")
    if scan_result.pool_address != creation.pool_address:
        raise ValueError("scan result pool mismatch")
    if pool_proof.pool_address != creation.pool_address:
        raise ValueError("pool scan proof pool mismatch")
    if pool_proof.creation_block != creation.block_number:
        raise ValueError("pool scan proof lower boundary must equal creation block")

    verified_factory_records = validate_factory_universe(validated.factory_universe)
    if creation not in verified_factory_records:
        raise ValueError("creation evidence missing from verified factory universe")
    factory_proof.assert_covers_block(creation.block_number)
    assert_finality_for_block(validated.creation_evidence.block, validated.finality)

    ordered = scan_result.ordered_candidates
    if not ordered:
        raise ValueError("scan result candidates must be non-empty for first trade")

    selected_swap = select_earliest_valid_swap(
        tuple(item.swap for item in ordered),
        creation=creation,
    )
    selected = next(item for item in ordered if item.swap.order_key == selected_swap.order_key)
    if selected.swap.pool_address != creation.pool_address:
        raise ValueError("swap pool must match creation pool")

    pool_proof.assert_covers_block(selected.swap.block_number)
    assert_finality_for_block(selected.block, validated.finality)

    base_address, quote_address = _orient_base_quote(
        token0=creation.token0,
        token1=creation.token1,
        quote_allowlist=validated.quote_allowlist,
    )
    source_event_time = require_utc(selected.block.timestamp)

    asset = AssetIdentity(chain=_CHAIN, asset_key=base_address, symbol=None)
    market = MarketIdentity(
        chain=_CHAIN,
        venue_or_protocol=_VENUE,
        market_key=creation.pool_address,
        pool_or_pair_address=creation.pool_address,
        base_asset_key=base_address,
        quote_asset_key=quote_address,
        symbol=None,
    )
    availability = EventAvailability.model_validate(
        {
            "status": EventAvailabilityStatus.SOURCE_TIME_ONLY,
            "source_event_time": source_event_time,
            "received_time": None,
            "decision_available_time": None,
            "availability_policy_version": _AVAILABILITY_POLICY_VERSION,
            "availability_provenance_ref": validated.provenance_ref,
        }
    )
    return HistoricalEarlyMarketEventFact.model_validate(
        {
            "event_id": validated.event_id,
            "event_kind": EarlyMarketEventKind.DEX_FIRST_TRADE,
            "event_definition_version": _EVENT_DEFINITION_VERSION,
            "source": _SOURCE,
            "venue_or_protocol": _VENUE,
            "chain": _CHAIN,
            "asset_identity": asset,
            "market_identity": market,
            "source_event_time": source_event_time,
            "availability": availability,
            "first_market_data_time": None,
            "first_liquidity_time": None,
            "first_trade_time": source_event_time,
            "event_time_semantics": EventTimeSemantics.OBSERVED,
            "event_quality_status": EventQualityStatus.ACCEPTED,
            "event_clock_quality": EventClockQuality.EXACT,
            "provenance_ref": validated.provenance_ref,
        }
    )


__all__ = [
    "AdapterRequest",
    "adapt_dex_first_trade",
    "build_eligible_scope",
    "compute_realized_execution_price",
    "select_earliest_valid_swap",
    "validate_factory_universe",
]
