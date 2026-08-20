"""Phase 8A.1 early-market-event domain contracts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from newcoin_trader.domain.early_market_events import (
    AssetIdentity,
    CapabilityProfile,
    EarlyMarketEvent,
    EarlyMarketEventKind,
    EventClockQuality,
    EventQualityStatus,
    EventTimeSemantics,
    MarketIdentity,
)


def _utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, second, tzinfo=UTC)


def _asset(**overrides: object) -> AssetIdentity:
    payload: dict[str, object] = {
        "chain": "solana",
        "asset_key": "So11111111111111111111111111111111111111112",
        "symbol": "SOL",
    }
    payload.update(overrides)
    return AssetIdentity.model_validate(payload)


def _market(**overrides: object) -> MarketIdentity:
    payload: dict[str, object] = {
        "chain": "solana",
        "venue_or_protocol": "raydium",
        "market_key": "pool:58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2",
        "pool_or_pair_address": "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2",
        "base_asset_key": "So11111111111111111111111111111111111111112",
        "quote_asset_key": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "symbol": "SOL/USDC",
    }
    payload.update(overrides)
    return MarketIdentity.model_validate(payload)


def _event(**overrides: object) -> EarlyMarketEvent:
    t0 = _utc(2024, 6, 1, 12, 0, 0)
    payload: dict[str, object] = {
        "event_id": "eme-1",
        "event_kind": EarlyMarketEventKind.DEX_FIRST_LIQUIDITY,
        "event_definition_version": "8a.1.0",
        "source": "fixture",
        "venue_or_protocol": "raydium",
        "chain": "solana",
        "asset_identity": _asset(),
        "market_identity": _market(),
        "source_event_time": t0,
        "received_time": t0 + timedelta(seconds=1),
        "decision_available_time": t0 + timedelta(seconds=2),
        "first_market_data_time": t0 + timedelta(seconds=5),
        "first_liquidity_time": t0 + timedelta(seconds=3),
        "first_trade_time": t0 + timedelta(seconds=4),
        "event_time_semantics": EventTimeSemantics.OBSERVED,
        "event_quality_status": EventQualityStatus.ACCEPTED,
        "event_clock_quality": EventClockQuality.EXACT,
        "provenance_ref": "prov://fixture/eme-1",
    }
    payload.update(overrides)
    return EarlyMarketEvent.model_validate(payload)


def test_early_market_event_kind_values() -> None:
    assert list(EarlyMarketEventKind) == [
        EarlyMarketEventKind.BINANCE_ALPHA_AVAILABLE,
        EarlyMarketEventKind.DEX_FIRST_LIQUIDITY,
        EarlyMarketEventKind.DEX_FIRST_TRADE,
        EarlyMarketEventKind.BINANCE_SPOT_LISTING,
    ]
    assert EarlyMarketEventKind.BINANCE_ALPHA_AVAILABLE.value == "BINANCE_ALPHA_AVAILABLE"
    assert EarlyMarketEventKind.DEX_FIRST_LIQUIDITY.value == "DEX_FIRST_LIQUIDITY"
    assert EarlyMarketEventKind.DEX_FIRST_TRADE.value == "DEX_FIRST_TRADE"
    assert EarlyMarketEventKind.BINANCE_SPOT_LISTING.value == "BINANCE_SPOT_LISTING"


def test_supporting_enum_values() -> None:
    assert EventTimeSemantics.ANNOUNCED.value == "announced"
    assert EventTimeSemantics.OBSERVED.value == "observed"
    assert EventTimeSemantics.EFFECTIVE.value == "effective"
    assert EventTimeSemantics.UNKNOWN.value == "unknown"

    assert EventQualityStatus.ACCEPTED.value == "accepted"
    assert EventQualityStatus.DEGRADED.value == "degraded"
    assert EventQualityStatus.REJECTED.value == "rejected"
    assert EventQualityStatus.UNKNOWN.value == "unknown"

    assert EventClockQuality.EXACT.value == "exact"
    assert EventClockQuality.BOUNDED.value == "bounded"
    assert EventClockQuality.ESTIMATED.value == "estimated"
    assert EventClockQuality.UNKNOWN.value == "unknown"


def test_asset_identity_is_structurally_distinct_from_market_identity() -> None:
    asset = _asset()
    market = _market()
    assert type(asset) is not type(market)
    assert not isinstance(asset, MarketIdentity)
    assert not isinstance(market, AssetIdentity)
    assert asset.asset_key != market.market_key
    assert market.pool_or_pair_address is not None
    # Symbol alone is never a complete identity: both types require non-symbol keys.
    assert asset.model_fields["asset_key"].is_required()
    assert market.model_fields["market_key"].is_required()
    assert market.model_fields["pool_or_pair_address"].is_required() is False


def test_asset_identity_rejects_symbol_only_construction() -> None:
    with pytest.raises(ValidationError):
        AssetIdentity.model_validate({"chain": "solana", "symbol": "SOL"})


def test_market_identity_carries_pool_or_pair_identity() -> None:
    market = _market(pool_or_pair_address="PoolAddrXYZ", market_key="pool:PoolAddrXYZ")
    assert market.pool_or_pair_address == "PoolAddrXYZ"
    assert "PoolAddrXYZ" in market.market_key


def test_timestamps_require_utc_aware() -> None:
    naive = datetime(2024, 6, 1, 12, 0, 0)
    aware = _utc(2024, 6, 1, 12, 0, 1)
    with pytest.raises(ValueError, match="timezone-aware"):
        _event(source_event_time=naive, received_time=aware, decision_available_time=aware)
    with pytest.raises(ValueError, match="timezone-aware"):
        _event(
            source_event_time=aware,
            received_time=naive,
            decision_available_time=aware,
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        _event(
            source_event_time=aware,
            received_time=aware,
            decision_available_time=naive,
        )


def test_optional_first_clocks_reject_naive_timestamps() -> None:
    aware = _utc(2024, 6, 1, 12, 0, 0)
    naive = datetime(2024, 6, 1, 12, 0, 5)
    with pytest.raises(ValueError, match="timezone-aware"):
        _event(first_market_data_time=naive)
    with pytest.raises(ValueError, match="timezone-aware"):
        _event(
            source_event_time=aware,
            received_time=aware,
            decision_available_time=aware,
            first_liquidity_time=naive,
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        _event(
            source_event_time=aware,
            received_time=aware,
            decision_available_time=aware,
            first_trade_time=naive,
        )


def test_non_utc_aware_timestamps_normalized_to_utc() -> None:
    offset = timezone(timedelta(hours=2))
    local = datetime(2024, 6, 1, 14, 0, 0, tzinfo=offset)
    event = _event(
        source_event_time=local,
        received_time=local,
        decision_available_time=local,
        first_market_data_time=local,
        first_liquidity_time=local,
        first_trade_time=local,
    )
    assert event.source_event_time == _utc(2024, 6, 1, 12, 0, 0)
    assert event.source_event_time.tzinfo == UTC


def test_rejects_received_time_before_source_event_time() -> None:
    t0 = _utc(2024, 6, 1, 12, 0, 0)
    with pytest.raises(ValidationError, match="received_time"):
        _event(
            source_event_time=t0,
            received_time=t0 - timedelta(seconds=1),
            decision_available_time=t0,
        )


def test_rejects_decision_available_time_before_received_time() -> None:
    t0 = _utc(2024, 6, 1, 12, 0, 0)
    with pytest.raises(ValidationError, match="decision_available_time"):
        _event(
            source_event_time=t0,
            received_time=t0 + timedelta(seconds=2),
            decision_available_time=t0 + timedelta(seconds=1),
        )


def test_equal_event_clocks_are_allowed() -> None:
    t0 = _utc(2024, 6, 1, 12, 0, 0)
    event = _event(
        source_event_time=t0,
        received_time=t0,
        decision_available_time=t0,
    )
    assert event.source_event_time == event.received_time == event.decision_available_time


def test_distinct_timestamps_are_preserved_exactly() -> None:
    source = _utc(2024, 6, 1, 12, 0, 0)
    received = _utc(2024, 6, 1, 12, 0, 1)
    decision = _utc(2024, 6, 1, 12, 0, 2)
    first_md = _utc(2024, 6, 1, 12, 0, 10)
    first_liq = _utc(2024, 6, 1, 11, 59, 50)  # may precede event clocks by kind
    first_trade = _utc(2024, 6, 1, 12, 5, 0)
    event = _event(
        source_event_time=source,
        received_time=received,
        decision_available_time=decision,
        first_market_data_time=first_md,
        first_liquidity_time=first_liq,
        first_trade_time=first_trade,
    )
    assert event.source_event_time == source
    assert event.received_time == received
    assert event.decision_available_time == decision
    assert event.first_market_data_time == first_md
    assert event.first_liquidity_time == first_liq
    assert event.first_trade_time == first_trade
    clocks = (
        event.source_event_time,
        event.received_time,
        event.decision_available_time,
        event.first_market_data_time,
        event.first_liquidity_time,
        event.first_trade_time,
    )
    assert len(set(clocks)) == 6


def test_no_ordering_imposed_between_first_clocks_and_event_clocks() -> None:
    t0 = _utc(2024, 6, 1, 12, 0, 0)
    # First-observation clocks may lie anywhere relative to event clocks.
    event = _event(
        source_event_time=t0,
        received_time=t0,
        decision_available_time=t0,
        first_market_data_time=t0 - timedelta(hours=1),
        first_liquidity_time=t0 + timedelta(hours=2),
        first_trade_time=t0 - timedelta(minutes=30),
    )
    assert event.first_market_data_time < event.source_event_time
    assert event.first_liquidity_time > event.decision_available_time


def test_deterministic_json_serialization() -> None:
    event = _event()
    first = event.model_dump_json()
    second = event.model_dump_json()
    assert first == second
    round_trip = EarlyMarketEvent.model_validate_json(first)
    assert round_trip == event
    assert round_trip.model_dump_json() == first


def test_models_are_frozen() -> None:
    event = _event()
    with pytest.raises(ValidationError):
        event.event_id = "mutated"  # type: ignore[misc]
    with pytest.raises(ValidationError):
        event.asset_identity.symbol = "X"  # type: ignore[misc]
    assert event.market_identity is not None
    with pytest.raises(ValidationError):
        event.market_identity.market_key = "mutated"  # type: ignore[misc]


def test_capability_profile_skeleton_construction() -> None:
    profile = CapabilityProfile(
        profile_id="cap-8a1-skeleton",
        venue_or_protocol="binance",
        chain="binance",
        supported_event_kinds=(
            EarlyMarketEventKind.BINANCE_ALPHA_AVAILABLE,
            EarlyMarketEventKind.BINANCE_SPOT_LISTING,
        ),
        event_definition_version="8a.1.0",
        notes="types/data only",
    )
    assert profile.profile_id == "cap-8a1-skeleton"
    assert EarlyMarketEventKind.BINANCE_SPOT_LISTING in profile.supported_event_kinds
    with pytest.raises(ValidationError):
        profile.notes = "mutated"  # type: ignore[misc]
