"""Phase 8A.3 early-market-event → Phase 3 projection boundary."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from newcoin_trader.database.models import EarlyMarketEventRecord, EarlyMarketObservation, Market, Token
from newcoin_trader.domain.enums import Chain, Venue
from newcoin_trader.domain.event_study import CellOutcomeStatus, ObservationResolution
from newcoin_trader.domain.listing_cohort import (
    CohortListing,
    CompletenessStatus,
    SourceEventTimeStatus,
    SpotClass,
)
from newcoin_trader.research.early_market_event_projection import (
    project_early_market_event,
    project_early_market_observations,
    projected_event_id,
)
from newcoin_trader.research.event_study_config import DEFAULT_ENTRY_DELAYS, DEFAULT_HOLDING_PERIODS
from newcoin_trader.research.event_study_engine import evaluate_cell, run_event_study
from newcoin_trader.research.listing_cohort_run import _to_event

T0 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
LEGACY_PHASE3_EVENT_ID = "binance:binance:NEWUSDT:ann-frozen-1"
LEGACY_PHASE3_SOURCE = "binance:cms:catalog48"
LEGACY_ANNOUNCEMENT_CODE = "ann-frozen-1"


def _token(
    *,
    token_id: int = 7,
    address: str = "MintAAA",
    chain: str = "solana",
    symbol: str = "AAA",
    venue: str | None = "raydium",
) -> Token:
    return Token(
        id=token_id,
        token_address=address,
        chain=chain,
        symbol=symbol,
        created_time=T0 - timedelta(hours=1),
        first_seen_time=T0 - timedelta(minutes=30),
        venue=venue,
        source="fixture-token",
        metadata_json=None,
    )


def _market(
    *,
    market_id: int = 11,
    base_token_id: int = 7,
    pool: str | None = "pool-a",
    venue: str = "raydium",
    market_key: str = "pool:pool-a",
) -> Market:
    return Market(
        id=market_id,
        market_key=market_key,
        base_token_id=base_token_id,
        quote_token_id=None,
        pool_or_pair_address=pool,
        venue=venue,
        symbol="AAA/USDC",
        source_native_market_id="mkt-a",
        market_kind="pool",
        identity_status="resolved",
        source="fixture",
        metadata_json=None,
        provenance_json=None,
    )


def _event_row(**overrides: object) -> EarlyMarketEventRecord:
    payload: dict[str, object] = {
        "id": 101,
        "source_native_event_id": "native-evt-1",
        "source": "fixture",
        "event_kind": "DEX_FIRST_LIQUIDITY",
        "event_definition_version": "8a.1.0",
        "venue_or_protocol": "raydium",
        "chain": "solana",
        "asset_token_id": 7,
        "market_id": 11,
        "source_event_time": T0,
        "received_time": T0 + timedelta(seconds=1),
        "decision_available_time": T0 + timedelta(seconds=2),
        "first_market_data_time": T0 + timedelta(seconds=5),
        "first_liquidity_time": T0 + timedelta(seconds=3),
        "first_trade_time": T0 + timedelta(seconds=4),
        "event_time_semantics": "observed",
        "event_quality_status": "accepted",
        "event_clock_quality": "exact",
        "provenance_ref": "prov://fixture/eme-native-1",
    }
    payload.update(overrides)
    return EarlyMarketEventRecord(**payload)


def _obs_row(**overrides: object) -> EarlyMarketObservation:
    payload: dict[str, object] = {
        "id": 201,
        "market_id": 11,
        "event_id": 101,
        "source_native_observation_id": "obs-1",
        "source": "fixture",
        "source_time": T0 + timedelta(seconds=10),
        "received_time": T0 + timedelta(seconds=11),
        "availability_status": "received",
        "price": Decimal("1.25"),
        "quantity": Decimal("10"),
        "liquidity": None,
        "base_reserve": None,
        "quote_reserve": None,
        "side": "buy",
        "resolution": None,
        "provenance_json": {"kind": "trade"},
    }
    payload.update(overrides)
    return EarlyMarketObservation(**payload)


def test_projected_event_id_is_deterministic_from_source_and_native_only() -> None:
    assert projected_event_id("fixture", "native-evt-1") == "fixture:native-evt-1"
    assert projected_event_id("binance", "binance:NEWUSDT:ann-frozen-1") == LEGACY_PHASE3_EVENT_ID


def test_project_preserves_identity_clocks_venue_asset_market_and_provenance() -> None:
    token = _token()
    market = _market()
    event = _event_row()
    projected = project_early_market_event(event, token=token, market=market)

    assert projected.event_id == "fixture:native-evt-1"
    assert projected.source == "fixture"
    assert projected.venue is Venue.RAYDIUM
    assert projected.chain is Chain.SOLANA
    assert projected.token_address == "MintAAA"
    assert projected.pair_address == "pool-a"
    assert projected.symbol == "AAA"
    assert projected.source_event_time == T0
    assert projected.first_seen_time == T0 + timedelta(seconds=1)
    assert projected.decision_available_time == T0 + timedelta(seconds=2)
    assert projected.first_market_data_time == T0 + timedelta(seconds=5)
    assert projected.provenance["source_native_event_id"] == "native-evt-1"
    assert projected.provenance["event_kind"] == "DEX_FIRST_LIQUIDITY"
    assert projected.provenance["event_definition_version"] == "8a.1.0"
    assert projected.provenance["event_time_semantics"] == "observed"
    assert projected.provenance["event_quality_status"] == "accepted"
    assert projected.provenance["event_clock_quality"] == "exact"
    assert projected.provenance["provenance_ref"] == "prov://fixture/eme-native-1"
    assert projected.provenance["received_time"] == (T0 + timedelta(seconds=1)).isoformat()
    assert projected.provenance["market_association_reason"] == "exact_event_market_id"
    assert projected.provenance["market_id"] == "11"
    assert projected.provenance["market_key"] == "pool:pool-a"
    assert "created_time" not in projected.provenance.get("source_event_time_field", "")


def test_missing_first_market_data_time_stays_none_and_is_not_fabricated() -> None:
    projected = project_early_market_event(
        _event_row(first_market_data_time=None),
        token=_token(),
        market=_market(),
    )
    assert projected.first_market_data_time is None
    # Must not substitute token/publication/observation clocks.
    assert projected.first_seen_time == T0 + timedelta(seconds=1)
    assert projected.source_event_time == T0


def test_projection_is_deterministic_across_repeats() -> None:
    token = _token()
    market = _market()
    event = _event_row()
    a = project_early_market_event(event, token=token, market=market)
    b = project_early_market_event(event, token=token, market=market)
    assert a == b
    assert a.model_dump() == b.model_dump()


def test_legacy_binance_spot_listing_without_market_projects_token_only() -> None:
    token = _token(address="NEWUSDT", chain="binance", symbol="NEWUSDT", venue="binance")
    event = _event_row(
        source="binance",
        source_native_event_id=LEGACY_ANNOUNCEMENT_CODE,
        event_kind="BINANCE_SPOT_LISTING",
        venue_or_protocol="binance",
        chain="binance",
        market_id=None,
        first_market_data_time=T0 + timedelta(minutes=1),
    )
    projected = project_early_market_event(event, token=token, market=None)
    assert projected.event_id == LEGACY_PHASE3_EVENT_ID
    assert projected.source == LEGACY_PHASE3_SOURCE
    assert projected.pair_address is None
    assert projected.venue is Venue.BINANCE
    assert projected.chain is Chain.BINANCE
    assert projected.provenance["market_association_reason"] == "legacy_binance_spot_listing_token_only"
    assert projected.provenance["event_clock_field"] == "announced_spot_trading_start"
    assert projected.provenance["source_native_event_id"] == LEGACY_ANNOUNCEMENT_CODE


def test_legacy_binance_spot_listing_parity_versus_phase81_to_event() -> None:
    """Parity against genuine Phase 8.1 ``_to_event`` (not a hand-built TokenListingEvent)."""
    source_event = T0
    first_seen = T0 + timedelta(seconds=3)
    decision = T0 + timedelta(seconds=5)
    first_md = T0 + timedelta(minutes=1)
    cohort = CohortListing(
        announcement_code=LEGACY_ANNOUNCEMENT_CODE,
        announcement_id="101",
        title="Binance Will List NEWUSDT",
        classification=SpotClass.SPOT_LISTING,
        symbol="NEWUSDT",
        release_date=first_seen,
        source_event_time=source_event,
        source_event_time_status=SourceEventTimeStatus.EXTRACTED,
        first_seen_time=first_seen,
        first_kline_time=first_md,
        first_trade_time=first_md,
        first_market_data_time=first_md,
        decision_available_time=decision,
        completeness=CompletenessStatus.COMPLETE,
        # Research-visible cohort provenance; Phase 8A.2 has no arbitrary provenance JSON column.
        provenance={"source_event_time": source_event.isoformat()},
    )
    legacy = _to_event(cohort)
    assert legacy is not None
    assert legacy.event_id == LEGACY_PHASE3_EVENT_ID
    assert legacy.source == LEGACY_PHASE3_SOURCE
    assert legacy.provenance["event_clock_field"] == "announced_spot_trading_start"

    token = _token(address="NEWUSDT", chain="binance", symbol="NEWUSDT", venue="binance")
    event = _event_row(
        source="binance",
        source_native_event_id=LEGACY_ANNOUNCEMENT_CODE,
        event_kind="BINANCE_SPOT_LISTING",
        venue_or_protocol="binance",
        chain="binance",
        market_id=None,
        source_event_time=source_event,
        received_time=first_seen,
        decision_available_time=decision,
        first_market_data_time=first_md,
        provenance_ref="prov://binance/ann-frozen-1",
    )
    projected = project_early_market_event(event, token=token, market=None)

    assert projected.event_id == legacy.event_id
    assert projected.source == legacy.source
    assert projected.source_event_time == legacy.source_event_time
    assert projected.first_seen_time == legacy.first_seen_time
    assert projected.decision_available_time == legacy.decision_available_time
    assert projected.first_market_data_time == legacy.first_market_data_time
    assert projected.venue == legacy.venue
    assert projected.chain == legacy.chain
    assert projected.pair_address == legacy.pair_address
    assert projected.token_address == legacy.token_address
    assert projected.symbol == legacy.symbol
    assert projected.provenance["event_clock_field"] == legacy.provenance["event_clock_field"]
    # Persisted audit fields under current schema (not full unknown cohort provenance dict).
    for key in (
        "source_native_event_id",
        "event_kind",
        "event_definition_version",
        "event_time_semantics",
        "event_quality_status",
        "event_clock_quality",
        "provenance_ref",
        "received_time",
        "market_association_reason",
    ):
        assert key in projected.provenance
    assert projected.provenance["source_native_event_id"] == LEGACY_ANNOUNCEMENT_CODE
    assert projected.provenance["market_association_reason"] == "legacy_binance_spot_listing_token_only"
    assert projected.provenance["event_clock_field"] == "announced_spot_trading_start"

    entry = source_event + timedelta(seconds=10)
    exit_ = entry + timedelta(minutes=1)
    obs = project_early_market_observations(
        [
            _obs_row(
                source_time=entry,
                price=Decimal("100"),
                provenance_json={"kind": "trade"},
                source="binance:trades",
                market_id=0,
            ),
            _obs_row(
                id=202,
                source_native_observation_id="obs-2",
                source_time=exit_,
                price=Decimal("110"),
                provenance_json={"kind": "trade"},
                source="binance:trades",
                market_id=0,
            ),
        ],
        token=token,
        market=None,
        venue_or_protocol="binance",
        chain="binance",
    )
    projected_cells = run_event_study(
        [projected],
        obs,
        entry_delays=[timedelta(seconds=10)],
        holding_periods=[timedelta(minutes=1)],
    )
    legacy_cells = run_event_study(
        [legacy],
        obs,
        entry_delays=[timedelta(seconds=10)],
        holding_periods=[timedelta(minutes=1)],
    )
    assert [c.status for c in projected_cells] == [c.status for c in legacy_cells]
    assert projected_cells[0].status is CellOutcomeStatus.COMPLETE
    assert projected_cells[0].simple_return == legacy_cells[0].simple_return

    default_cells = run_event_study(
        [projected],
        obs,
        entry_delays=DEFAULT_ENTRY_DELAYS,
        holding_periods=(timedelta(minutes=1),),
    )
    assert default_cells[0].entry_delay == DEFAULT_ENTRY_DELAYS[0]
    assert default_cells[0].status is CellOutcomeStatus.COMPLETE


@pytest.mark.parametrize(
    "native_id",
    ["", "   ", "binance:NEWUSDT:ann-frozen-1"],
)
def test_legacy_binance_spot_listing_rejects_malformed_native_identity(native_id: str) -> None:
    token = _token(address="NEWUSDT", chain="binance", symbol="NEWUSDT", venue="binance")
    with pytest.raises(ValueError, match="announcement"):
        project_early_market_event(
            _event_row(
                source="binance",
                source_native_event_id=native_id,
                event_kind="BINANCE_SPOT_LISTING",
                venue_or_protocol="binance",
                chain="binance",
                market_id=None,
            ),
            token=token,
            market=None,
        )


def test_nonlegacy_missing_market_fails_closed() -> None:
    with pytest.raises(ValueError, match="market"):
        project_early_market_event(
            _event_row(market_id=None, event_kind="DEX_FIRST_LIQUIDITY"),
            token=_token(),
            market=None,
        )


def test_mismatched_market_id_fails_closed() -> None:
    with pytest.raises(ValueError, match="market"):
        project_early_market_event(
            _event_row(market_id=11),
            token=_token(),
            market=_market(market_id=99),
        )


def test_same_token_two_pools_without_exact_market_id_does_not_pick_first() -> None:
    # Ambiguous linkage: event has no market_id; presence of multiple pools is irrelevant —
    # projection must fail closed rather than bind by symbol or first pool.
    with pytest.raises(ValueError, match="market"):
        project_early_market_event(
            _event_row(market_id=None, event_kind="DEX_FIRST_TRADE"),
            token=_token(),
            market=_market(market_id=11),
        )


def test_unsupported_venue_or_chain_fails_closed() -> None:
    with pytest.raises(ValueError, match="venue"):
        project_early_market_event(
            _event_row(venue_or_protocol="unknown-venue"),
            token=_token(),
            market=_market(),
        )
    with pytest.raises(ValueError, match="chain"):
        project_early_market_event(
            _event_row(chain="ethereum"),
            token=_token(),
            market=_market(),
        )


def test_primary_pit_strict_and_equality_boundaries_do_not_shift_decision() -> None:
    token = _token(address="NEWUSDT", chain="binance", symbol="NEWUSDT", venue="binance")
    decision = T0 + timedelta(seconds=10)
    event = project_early_market_event(
        _event_row(
            source="binance",
            source_native_event_id="pit-ann",
            event_kind="BINANCE_SPOT_LISTING",
            venue_or_protocol="binance",
            chain="binance",
            market_id=None,
            source_event_time=T0,
            received_time=T0 + timedelta(seconds=1),
            decision_available_time=decision,
            first_market_data_time=None,
        ),
        token=token,
        market=None,
    )
    entry = T0 + timedelta(seconds=10)
    exit_ = entry + timedelta(minutes=1)
    obs = project_early_market_observations(
        [
            _obs_row(source_time=entry, price=Decimal("1"), provenance_json={"kind": "trade"}, source="binance"),
            _obs_row(
                id=202,
                source_native_observation_id="obs-exit",
                source_time=exit_,
                price=Decimal("1.1"),
                provenance_json={"kind": "trade"},
                source="binance",
            ),
        ],
        token=token,
        market=None,
        venue_or_protocol="binance",
        chain="binance",
    )
    # Equality: entry_time == decision_available_time is eligible (no decision shift).
    equal = evaluate_cell(
        event,
        obs,
        entry_delay=timedelta(seconds=10),
        holding_period=timedelta(minutes=1),
    )
    assert equal.status is CellOutcomeStatus.COMPLETE

    strict = evaluate_cell(
        event,
        obs,
        entry_delay=timedelta(seconds=5),
        holding_period=timedelta(minutes=1),
    )
    assert strict.status is CellOutcomeStatus.NOT_DECISION_AVAILABLE


def test_point_observations_yield_point_and_permit_10s_cell() -> None:
    token = _token()
    market = _market()
    event = project_early_market_event(_event_row(decision_available_time=T0), token=token, market=market)
    entry = T0 + timedelta(seconds=10)
    exit_ = entry + timedelta(minutes=1)
    obs = project_early_market_observations(
        [
            _obs_row(source_time=entry, price=Decimal("2"), provenance_json={"kind": "trade"}),
            _obs_row(
                id=202,
                source_native_observation_id="obs-exit",
                source_time=exit_,
                price=Decimal("2.2"),
                provenance_json={"kind": "aggTrade"},
            ),
        ],
        token=token,
        market=market,
    )
    assert all(o.resolution is ObservationResolution.POINT for o in obs)
    cell = evaluate_cell(
        event,
        obs,
        entry_delay=timedelta(seconds=10),
        holding_period=timedelta(minutes=1),
    )
    assert cell.status is CellOutcomeStatus.COMPLETE
    assert cell.simple_return == Decimal("0.1")


def test_1m_kline_provenance_stays_minute_and_engine_rejects_10s() -> None:
    token = _token()
    market = _market()
    event = project_early_market_event(_event_row(decision_available_time=T0), token=token, market=market)
    entry = T0 + timedelta(seconds=10)
    exit_ = entry + timedelta(minutes=1)
    obs = project_early_market_observations(
        [
            _obs_row(
                source_time=entry,
                price=Decimal("2"),
                provenance_json={"kind": "kline", "interval": "1m"},
                source="binance:kline",
            ),
            _obs_row(
                id=202,
                source_native_observation_id="obs-exit",
                source_time=exit_,
                price=Decimal("2.2"),
                provenance_json={"kind": "kline", "interval": "1m"},
                source="binance:kline",
            ),
        ],
        token=token,
        market=market,
    )
    assert all(o.resolution is ObservationResolution.MINUTE for o in obs)
    cell = evaluate_cell(
        event,
        obs,
        entry_delay=timedelta(seconds=10),
        holding_period=timedelta(minutes=1),
    )
    assert cell.status is CellOutcomeStatus.UNSUPPORTED_RESOLUTION


def test_price_null_observations_are_not_projected_as_exact() -> None:
    token = _token()
    market = _market()
    obs = project_early_market_observations(
        [
            _obs_row(price=None, provenance_json={"kind": "trade"}),
            _obs_row(
                id=202,
                source_native_observation_id="obs-priced",
                price=Decimal("3"),
                provenance_json={"kind": "trade"},
            ),
        ],
        token=token,
        market=market,
    )
    assert len(obs) == 1
    assert obs[0].price == Decimal("3")


def test_observation_provenance_preserves_identities_and_availability() -> None:
    token = _token()
    market = _market()
    obs = project_early_market_observations(
        [
            _obs_row(
                event_id=101,
                availability_status="receipt_verified",
                provenance_json={"kind": "trade", "extra": "keep"},
            )
        ],
        token=token,
        market=market,
    )
    assert obs[0].provenance is not None
    assert obs[0].provenance["kind"] == "trade"
    assert obs[0].provenance["extra"] == "keep"
    assert obs[0].provenance["source_native_observation_id"] == "obs-1"
    assert obs[0].provenance["market_id"] == "11"
    assert obs[0].provenance["event_id"] == "101"
    assert obs[0].provenance["availability_status"] == "receipt_verified"


def test_missing_exact_entry_remains_missing_entry_no_forward_fill() -> None:
    token = _token()
    market = _market()
    event = project_early_market_event(_event_row(decision_available_time=T0), token=token, market=market)
    entry = T0 + timedelta(minutes=1)
    exit_ = entry + timedelta(minutes=1)
    # Observation only after entry — must not forward-fill.
    obs = project_early_market_observations(
        [
            _obs_row(
                source_time=exit_,
                price=Decimal("1.1"),
                provenance_json={"kind": "trade"},
            )
        ],
        token=token,
        market=market,
    )
    cell = evaluate_cell(
        event,
        obs,
        entry_delay=timedelta(minutes=1),
        holding_period=timedelta(minutes=1),
    )
    assert cell.status is CellOutcomeStatus.MISSING_ENTRY
    assert cell.simple_return is None


def test_default_holding_grid_constant_is_existing_phase3() -> None:
    assert DEFAULT_HOLDING_PERIODS[0] == timedelta(minutes=1)
    assert DEFAULT_ENTRY_DELAYS[0] == timedelta(seconds=10)
