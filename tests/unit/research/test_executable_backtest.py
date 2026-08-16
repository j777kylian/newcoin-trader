"""Phase 5 executable historical backtest: clocks, venue fills, fees, frozen identity."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from typer.testing import CliRunner

from newcoin_trader.cli.main import app
from newcoin_trader.domain.enums import Chain, Side, Venue
from newcoin_trader.domain.event_study import CellOutcomeStatus, ObservationResolution, TokenListingEvent
from newcoin_trader.domain.executable_backtest import (
    DISCLAIMER,
    WARNING_RESEARCH_ONLY,
    DepthLevel,
    ExecutableBacktestStatus,
    ExecutionConfidence,
    ExecutionMarketObservation,
    FrozenCandidateIdentity,
    HistoricalDepthBook,
    SimulatedFillMode,
)
from newcoin_trader.domain.feature_research import (
    CandidateRule,
    DecisionFeatureRecord,
    FeatureValue,
    FeatureValueState,
    FutureLabel,
    RuleCondition,
)
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.executable_backtest_aggregate import (
    aggregate_trades,
    edge_retention,
)
from newcoin_trader.research.executable_backtest_capabilities import (
    capability_matrix_str,
    classify_execution_component,
)
from newcoin_trader.research.executable_backtest_config import (
    DEFAULT_ASSUMED_FEE_BPS,
    DEFAULT_MAX_PARTICIPATION,
    DEFAULT_POSITION_NOTIONALS,
    FEE_BPS_MAX,
    MAX_LATENCY_GRID,
    MAX_PARTICIPATION_MAX,
    MAX_PARTICIPATION_MIN,
    validate_executable_backtest_bounds,
)
from newcoin_trader.research.executable_backtest_engine import (
    evaluate_executable_trade,
    simulate_cex_depth_fill,
    simulate_dex_liquidity_fill,
    simulate_modeled_price_fill,
)
from newcoin_trader.research.executable_backtest_run import (
    build_executable_backtest_report,
    emit_executable_backtest_artifacts,
)
from newcoin_trader.research.feature_research_rules import discover_candidate_rules
from newcoin_trader.services.executable_backtest import ExecutableBacktestService

runner = CliRunner()

T0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


def _event(
    *,
    source: datetime = T0,
    available: datetime | None = None,
    venue: Venue = Venue.BINANCE,
    token: str = "TOKEN",
    event_id: str = "e1",
) -> TokenListingEvent:
    avail = available or source
    return TokenListingEvent(
        event_id=event_id,
        venue=venue,
        chain=Chain.BINANCE if venue is Venue.BINANCE else Chain.SOLANA,
        token_address=token,
        pair_address="PAIR",
        symbol="TOK",
        source=venue.value,
        source_event_time=source,
        first_seen_time=source,
        first_market_data_time=source,
        decision_available_time=avail,
        provenance={"token_id": "1"},
    )


def _obs(
    ts: datetime,
    price: str,
    *,
    liquidity: str | None = None,
    resolution: ObservationResolution = ObservationResolution.POINT,
    venue: Venue = Venue.BINANCE,
    token: str = "TOKEN",
    source: str = "binance:trade",
) -> ExecutionMarketObservation:
    return ExecutionMarketObservation(
        token_address=token,
        chain="binance" if venue is Venue.BINANCE else "solana",
        venue=venue,
        timestamp=ts,
        price=Decimal(price),
        liquidity=Decimal(liquidity) if liquidity is not None else None,
        resolution=resolution,
        source=source,
        provenance={"kind": "trade"},
    )


def _rule(*, rule_id: str = "frozen-rule-1") -> CandidateRule:
    return CandidateRule(
        rule_id=rule_id,
        conditions=(RuleCondition(feature_name="age_seconds", op="gte", threshold=Decimal("0")),),
        human_readable="age_seconds gte 0",
        selected=True,
    )


def _identity(*, rule: CandidateRule | None = None, fold: int | None = None) -> FrozenCandidateIdentity:
    r = rule or _rule()
    return FrozenCandidateIdentity(
        rule_id=r.rule_id,
        conditions=r.conditions,
        human_readable=r.human_readable,
        phase4_config_id="cfg-phase4",
        split_label="test",
        fold_index=fold,
        provenance={"source": "frozen_phase4"},
    )


def _record(
    event: TokenListingEvent,
    *,
    decision_time: datetime | None = None,
    gross: str = "0.10",
) -> DecisionFeatureRecord:
    dt = decision_time or (event.source_event_time + timedelta(minutes=1))
    return DecisionFeatureRecord(
        event_id=event.event_id,
        venue=event.venue,
        chain=event.chain,
        token_address=event.token_address,
        pair_address=event.pair_address,
        source_event_time=event.source_event_time,
        first_seen_time=event.first_seen_time,
        first_market_data_time=event.first_market_data_time,
        decision_available_time=event.decision_available_time,
        decision_time=dt,
        feature_cutoff=dt,
        features=(
            FeatureValue(
                name="age_seconds",
                family="age",
                value=Decimal("60"),
                state=FeatureValueState.AVAILABLE,
            ),
        ),
        labels=(
            FutureLabel(
                entry_delay=timedelta(minutes=1),
                holding_period=timedelta(minutes=5),
                status=CellOutcomeStatus.COMPLETE,
                simple_return=Decimal(gross),
            ),
        ),
        config_id="cfg-phase4",
        computation_id="comp-1",
    )


# ---------------------------------------------------------------------------
# Config / capabilities
# ---------------------------------------------------------------------------


def test_validate_bounds_require_positive_windows_and_caps() -> None:
    with pytest.raises(ConfigError):
        validate_executable_backtest_bounds(
            start=T0,
            end=T0,
            max_events=10,
            max_trades=10,
            max_execution_inputs=10,
            latencies=(timedelta(seconds=10),),
            holding_periods=(timedelta(minutes=5),),
            position_notionals=(Decimal("100"),),
            max_participation=Decimal("0.1"),
            assumed_fee_bps=Decimal("10"),
        )


def test_capability_matrix_labels_depth_unsupported_in_db() -> None:
    matrix = capability_matrix_str()
    assert matrix["binance"]["historical_depth"] == "unsupported"
    assert matrix["binance"]["historical_trades"] == "supported"
    assert matrix["raydium"]["pool_reserves"] == "unsupported"
    assert matrix["geckoterminal"]["historical_fees"] == "unsupported"
    assert classify_execution_component(Venue.BINANCE, "historical_depth").value == "unsupported"


# ---------------------------------------------------------------------------
# Clocks / availability / latency / future data
# ---------------------------------------------------------------------------


def test_no_fill_before_decision_available() -> None:
    event = _event(source=T0, available=T0 + timedelta(minutes=5))
    decision = T0 + timedelta(minutes=1)  # before availability
    fill_ts = decision + timedelta(seconds=10)
    result = evaluate_executable_trade(
        event=event,
        record=_record(event, decision_time=decision),
        identity=_identity(),
        observations=(_obs(fill_ts, "1.0"),),
        side=Side.BUY,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
        entry_request_latency=timedelta(0),
        entry_fill_latency=timedelta(seconds=10),
        exit_request_latency=timedelta(0),
        exit_fill_latency=timedelta(0),
        assumed_fee_bps=Decimal("10"),
        max_participation=Decimal("0.1"),
    )
    assert result.status is ExecutableBacktestStatus.NOT_DECISION_AVAILABLE
    assert result.entry_fill is None


def test_latency_forward_only_fill_after_signal() -> None:
    event = _event()
    decision = T0 + timedelta(minutes=1)
    signal = decision
    fill_ts = signal + timedelta(seconds=30)
    exit_fill = fill_ts + timedelta(minutes=5)
    obs = (
        _obs(fill_ts, "1.00", liquidity="10000"),
        _obs(exit_fill, "1.10", liquidity="10000"),
    )
    result = evaluate_executable_trade(
        event=event,
        record=_record(event, decision_time=decision, gross="0.10"),
        identity=_identity(),
        observations=obs,
        side=Side.BUY,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
        entry_request_latency=timedelta(0),
        entry_fill_latency=timedelta(seconds=30),
        exit_request_latency=timedelta(0),
        exit_fill_latency=timedelta(0),
        assumed_fee_bps=Decimal("0"),
        max_participation=Decimal("0.5"),
    )
    assert result.status is ExecutableBacktestStatus.FULLY_FILLED
    assert result.signal_time == signal
    assert result.request_time == signal
    assert result.fill_time == fill_ts
    assert result.exit_signal_time == fill_ts + timedelta(minutes=5)
    assert result.exit_fill_time == exit_fill
    assert result.fill_time >= result.signal_time
    assert result.fill_time >= event.decision_available_time


def test_future_observation_never_used_at_fill() -> None:
    event = _event()
    decision = T0 + timedelta(minutes=1)
    fill_ts = decision + timedelta(seconds=10)
    # Only a future observation exists — must not fill from it.
    future = _obs(fill_ts + timedelta(seconds=1), "9.99", liquidity="99999")
    result = evaluate_executable_trade(
        event=event,
        record=_record(event, decision_time=decision),
        identity=_identity(),
        observations=(future,),
        side=Side.BUY,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
        entry_request_latency=timedelta(0),
        entry_fill_latency=timedelta(seconds=10),
        exit_request_latency=timedelta(0),
        exit_fill_latency=timedelta(0),
        assumed_fee_bps=Decimal("10"),
        max_participation=Decimal("0.1"),
    )
    assert result.status is ExecutableBacktestStatus.NO_ENTRY
    assert result.entry_fill is None


def test_subminute_latency_unsupported_on_minute_resolution() -> None:
    event = _event()
    decision = T0 + timedelta(minutes=1)
    fill_ts = decision + timedelta(seconds=10)
    obs = (
        _obs(fill_ts, "1.0", resolution=ObservationResolution.MINUTE, source="binance:kline"),
        _obs(fill_ts + timedelta(minutes=5), "1.1", resolution=ObservationResolution.MINUTE),
    )
    result = evaluate_executable_trade(
        event=event,
        record=_record(event, decision_time=decision),
        identity=_identity(),
        observations=obs,
        side=Side.BUY,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
        entry_request_latency=timedelta(0),
        entry_fill_latency=timedelta(seconds=10),
        exit_request_latency=timedelta(0),
        exit_fill_latency=timedelta(0),
        assumed_fee_bps=Decimal("10"),
        max_participation=Decimal("0.1"),
    )
    assert result.status is ExecutableBacktestStatus.UNSUPPORTED_RESOLUTION


# ---------------------------------------------------------------------------
# Binance / CEX depth walking
# ---------------------------------------------------------------------------


def test_cex_buy_walks_asks_weighted_average() -> None:
    book = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=T0,
        bids=(DepthLevel(price=Decimal("9.9"), quantity=Decimal("50")),),
        asks=(
            DepthLevel(price=Decimal("10.0"), quantity=Decimal("5")),
            DepthLevel(price=Decimal("10.2"), quantity=Decimal("5")),
        ),
        source="supplied_historical_depth",
    )
    fill = simulate_cex_depth_fill(
        book=book,
        side=Side.BUY,
        requested_qty=Decimal("8"),
        assumed_fee_bps=Decimal("0"),
    )
    assert fill is not None
    assert fill.status is ExecutableBacktestStatus.FULLY_FILLED
    assert fill.mode is SimulatedFillMode.EXACT_DEPTH
    assert fill.confidence is ExecutionConfidence.EXACT_DEPTH
    # 5@10 + 3@10.2 = 80.6 / 8 = 10.075
    assert fill.fill_price == Decimal("10.075")
    assert fill.fill_qty == Decimal("8")
    assert fill.spread_cost is not None
    assert fill.impact_cost is not None
    assert fill.fee_cost == Decimal("0")


def test_cex_sell_walks_bids_not_asks() -> None:
    book = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=T0,
        bids=(
            DepthLevel(price=Decimal("10.0"), quantity=Decimal("4")),
            DepthLevel(price=Decimal("9.5"), quantity=Decimal("10")),
        ),
        asks=(DepthLevel(price=Decimal("10.5"), quantity=Decimal("100")),),
        source="supplied_historical_depth",
    )
    fill = simulate_cex_depth_fill(
        book=book,
        side=Side.SELL,
        requested_qty=Decimal("5"),
        assumed_fee_bps=Decimal("0"),
    )
    assert fill is not None
    assert fill.fill_qty == Decimal("5")
    # 4@10 + 1@9.5 = 49.5 / 5 = 9.9
    assert fill.fill_price == Decimal("9.9")


def test_cex_partial_and_unfilled_and_spread_distinct() -> None:
    thin = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=T0,
        bids=(DepthLevel(price=Decimal("9"), quantity=Decimal("1")),),
        asks=(DepthLevel(price=Decimal("11"), quantity=Decimal("2")),),
        source="supplied_historical_depth",
    )
    partial = simulate_cex_depth_fill(
        book=thin,
        side=Side.BUY,
        requested_qty=Decimal("10"),
        assumed_fee_bps=Decimal("0"),
    )
    assert partial is not None
    assert partial.status is ExecutableBacktestStatus.PARTIAL
    assert partial.fill_qty == Decimal("2")
    assert partial.spread_cost is not None
    assert partial.slippage_cost is not None or partial.impact_cost is not None

    empty = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=T0,
        bids=(),
        asks=(),
        source="supplied_historical_depth",
    )
    unfilled = simulate_cex_depth_fill(
        book=empty,
        side=Side.BUY,
        requested_qty=Decimal("1"),
        assumed_fee_bps=Decimal("0"),
    )
    assert unfilled is not None
    assert unfilled.status is ExecutableBacktestStatus.UNFILLED
    assert unfilled.fill_qty == Decimal("0")


def test_cex_multilevel_depth_and_modeled_fallback_without_depth() -> None:
    event = _event()
    decision = T0 + timedelta(minutes=1)
    fill_ts = decision
    exit_ts = fill_ts + timedelta(minutes=5)
    book = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=fill_ts,
        bids=(
            DepthLevel(price=Decimal("0.99"), quantity=Decimal("100")),
            DepthLevel(price=Decimal("0.98"), quantity=Decimal("100")),
        ),
        asks=(
            DepthLevel(price=Decimal("1.00"), quantity=Decimal("40")),
            DepthLevel(price=Decimal("1.01"), quantity=Decimal("40")),
            DepthLevel(price=Decimal("1.02"), quantity=Decimal("40")),
        ),
        source="supplied_historical_depth",
    )
    with_depth = evaluate_executable_trade(
        event=event,
        record=_record(event, decision_time=decision),
        identity=_identity(),
        observations=(
            _obs(fill_ts, "1.00", liquidity="100000"),
            _obs(exit_ts, "1.05", liquidity="100000"),
        ),
        depth_books=(book,),
        side=Side.BUY,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
        entry_request_latency=timedelta(0),
        entry_fill_latency=timedelta(0),
        exit_request_latency=timedelta(0),
        exit_fill_latency=timedelta(0),
        assumed_fee_bps=Decimal("0"),
        max_participation=Decimal("1"),
    )
    assert with_depth.entry_fill is not None
    assert with_depth.entry_fill.mode is SimulatedFillMode.EXACT_DEPTH

    modeled = evaluate_executable_trade(
        event=event,
        record=_record(event, decision_time=decision),
        identity=_identity(),
        observations=(
            _obs(fill_ts, "1.00", liquidity="100000"),
            _obs(exit_ts, "1.05", liquidity="100000"),
        ),
        depth_books=(),
        side=Side.BUY,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
        entry_request_latency=timedelta(0),
        entry_fill_latency=timedelta(0),
        exit_request_latency=timedelta(0),
        exit_fill_latency=timedelta(0),
        assumed_fee_bps=Decimal("0"),
        max_participation=Decimal("0.1"),
    )
    assert modeled.entry_fill is not None
    assert modeled.entry_fill.mode in {
        SimulatedFillMode.MODELED_PRICE,
        SimulatedFillMode.MODELED_LIQUIDITY,
        SimulatedFillMode.EXACT_TRADE,
    }
    assert (
        "modeled" in modeled.entry_fill.confidence.value
        or modeled.entry_fill.confidence is ExecutionConfidence.EXACT_TRADE
    )


# ---------------------------------------------------------------------------
# DEX liquidity / impact / modeled labels
# ---------------------------------------------------------------------------


def test_dex_liquidity_participation_impact_is_modeled_not_amm() -> None:
    fill = simulate_dex_liquidity_fill(
        observation=_obs(
            T0,
            "1.0",
            liquidity="1000",
            venue=Venue.RAYDIUM,
            source="raydium:pool",
        ),
        side=Side.BUY,
        position_notional=Decimal("100"),
        max_participation=Decimal("0.1"),
        assumed_fee_bps=Decimal("30"),
        impact_coefficient=Decimal("1"),
    )
    assert fill is not None
    assert fill.mode is SimulatedFillMode.MODELED_LIQUIDITY
    assert fill.confidence is ExecutionConfidence.MODELED_LIQUIDITY_IMPACT
    assert fill.impact_cost is not None and fill.impact_cost > 0
    assert "amm" not in (fill.label or "").lower()
    assert "modeled" in fill.confidence.value


def test_dex_insufficient_liquidity_capped_participation() -> None:
    fill = simulate_dex_liquidity_fill(
        observation=_obs(T0, "1.0", liquidity="50", venue=Venue.GECKO, source="geckoterminal"),
        side=Side.BUY,
        position_notional=Decimal("100"),
        max_participation=Decimal("0.1"),
        assumed_fee_bps=Decimal("30"),
        impact_coefficient=Decimal("1"),
    )
    assert fill is not None
    assert fill.status in {
        ExecutableBacktestStatus.PARTIAL,
        ExecutableBacktestStatus.INSUFFICIENT_LIQUIDITY,
    }
    # Cap: max notional = 50 * 0.1 = 5
    assert fill.fill_qty * fill.fill_price <= Decimal("5") + Decimal("0.0000001")


def _dex_obs() -> ExecutionMarketObservation:
    return _obs(T0, "1.0", liquidity="1000", venue=Venue.RAYDIUM, source="raydium:pool")


def test_dex_numerical_safety_rejects_nan_inf_zero_negative_notional() -> None:
    obs = _dex_obs()
    for notional in (
        Decimal("NaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
        Decimal("0"),
        Decimal("-1"),
    ):
        fill = simulate_dex_liquidity_fill(
            observation=obs,
            side=Side.BUY,
            position_notional=notional,
            max_participation=Decimal("0.1"),
            assumed_fee_bps=Decimal("30"),
            impact_coefficient=Decimal("1"),
        )
        assert fill is None


def test_dex_numerical_safety_rejects_nonfinite_and_out_of_range_participation_fee_impact() -> None:
    obs = _dex_obs()
    base = {
        "observation": obs,
        "side": Side.BUY,
        "position_notional": Decimal("100"),
        "max_participation": Decimal("0.1"),
        "assumed_fee_bps": Decimal("30"),
        "impact_coefficient": Decimal("1"),
    }
    overrides: list[dict[str, Decimal]] = [
        {"max_participation": Decimal("NaN")},
        {"max_participation": Decimal("Infinity")},
        {"max_participation": Decimal("-Infinity")},
        {"max_participation": Decimal("0")},
        {"max_participation": MAX_PARTICIPATION_MIN / Decimal("10")},
        {"max_participation": MAX_PARTICIPATION_MAX + Decimal("0.1")},
        {"assumed_fee_bps": Decimal("NaN")},
        {"assumed_fee_bps": Decimal("Infinity")},
        {"assumed_fee_bps": Decimal("-Infinity")},
        {"assumed_fee_bps": Decimal("-1")},
        {"assumed_fee_bps": FEE_BPS_MAX + Decimal("1")},
        {"impact_coefficient": Decimal("NaN")},
        {"impact_coefficient": Decimal("Infinity")},
        {"impact_coefficient": Decimal("-Infinity")},
        {"impact_coefficient": Decimal("-1")},
    ]
    for override in overrides:
        fill = simulate_dex_liquidity_fill(**(base | override))
        assert fill is None


def test_dex_numerical_safety_valid_fill_unchanged_and_finite() -> None:
    fill = simulate_dex_liquidity_fill(
        observation=_dex_obs(),
        side=Side.BUY,
        position_notional=Decimal("100"),
        max_participation=Decimal("0.1"),
        assumed_fee_bps=Decimal("30"),
        impact_coefficient=Decimal("1"),
    )
    assert fill is not None
    assert fill.status is ExecutableBacktestStatus.FULLY_FILLED
    assert fill.mode is SimulatedFillMode.MODELED_LIQUIDITY
    assert fill.fill_price == Decimal("1.10")
    assert fill.notional == Decimal("100")
    for value in (
        fill.fill_qty,
        fill.fill_price,
        fill.notional,
        fill.fee_cost,
        fill.impact_cost,
        fill.requested_qty,
    ):
        assert value.is_finite()
        assert not value.is_nan()


def test_dex_numerical_safety_no_raw_decimal_exception() -> None:
    obs = _dex_obs()
    try:
        fill = simulate_dex_liquidity_fill(
            observation=obs,
            side=Side.BUY,
            position_notional=Decimal("NaN"),
            max_participation=Decimal("NaN"),
            assumed_fee_bps=Decimal("NaN"),
            impact_coefficient=Decimal("NaN"),
        )
    except Exception as exc:  # noqa: BLE001 — must never escape as raw Decimal failure
        pytest.fail(f"raw exception escaped simulate_dex_liquidity_fill: {type(exc).__name__}: {exc}")
    assert fill is None


def test_dex_numerical_safety_evaluate_maps_invalid_market_data() -> None:
    event = _event(venue=Venue.RAYDIUM)
    decision = T0 + timedelta(minutes=1)
    fill_ts = decision
    exit_ts = fill_ts + timedelta(minutes=5)
    result = evaluate_executable_trade(
        event=event,
        record=_record(event, decision_time=decision),
        identity=_identity(),
        observations=(
            _obs(fill_ts, "1.0", liquidity="1000", venue=Venue.RAYDIUM, source="raydium:pool"),
            _obs(exit_ts, "1.1", liquidity="1000", venue=Venue.RAYDIUM, source="raydium:pool"),
        ),
        side=Side.BUY,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
        entry_request_latency=timedelta(0),
        entry_fill_latency=timedelta(0),
        exit_request_latency=timedelta(0),
        exit_fill_latency=timedelta(0),
        assumed_fee_bps=Decimal("30"),
        max_participation=Decimal("NaN"),
        impact_coefficient=Decimal("1"),
    )
    assert result.status is ExecutableBacktestStatus.INVALID_MARKET_DATA


# ---------------------------------------------------------------------------
# Fees / net / caps / failures
# ---------------------------------------------------------------------------


def test_fee_spread_slippage_impact_reported_separately_and_net() -> None:
    event = _event()
    decision = T0 + timedelta(minutes=1)
    fill_ts = decision
    exit_ts = fill_ts + timedelta(minutes=5)
    result = evaluate_executable_trade(
        event=event,
        record=_record(event, decision_time=decision, gross="0.10"),
        identity=_identity(),
        observations=(
            _obs(fill_ts, "1.00", liquidity="100000"),
            _obs(exit_ts, "1.10", liquidity="100000"),
        ),
        side=Side.BUY,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
        entry_request_latency=timedelta(0),
        entry_fill_latency=timedelta(0),
        exit_request_latency=timedelta(0),
        exit_fill_latency=timedelta(0),
        assumed_fee_bps=Decimal("10"),
        max_participation=Decimal("0.1"),
    )
    assert result.status is ExecutableBacktestStatus.FULLY_FILLED
    assert result.gross_return is not None
    assert result.net_return is not None
    assert result.total_fee_cost is not None and result.total_fee_cost > 0
    assert result.total_spread_cost is not None
    assert result.total_slippage_cost is not None
    assert result.total_impact_cost is not None
    assert result.net_return < result.gross_return


def test_position_participation_grid_capped() -> None:
    for notional in DEFAULT_POSITION_NOTIONALS:
        assert notional > 0
    assert DEFAULT_MAX_PARTICIPATION <= Decimal("1")
    assert DEFAULT_MAX_PARTICIPATION > 0
    fill = simulate_modeled_price_fill(
        observation=_obs(T0, "1.0", liquidity="1000"),
        side=Side.BUY,
        position_notional=Decimal("10000"),
        max_participation=Decimal("0.05"),
        assumed_fee_bps=Decimal("0"),
    )
    assert fill is not None
    assert fill.fill_qty * fill.fill_price <= Decimal("50") + Decimal("0.0001")


def test_missing_exit_not_silently_dropped() -> None:
    event = _event()
    decision = T0 + timedelta(minutes=1)
    fill_ts = decision
    result = evaluate_executable_trade(
        event=event,
        record=_record(event, decision_time=decision),
        identity=_identity(),
        observations=(_obs(fill_ts, "1.00", liquidity="100000"),),
        side=Side.BUY,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
        entry_request_latency=timedelta(0),
        entry_fill_latency=timedelta(0),
        exit_request_latency=timedelta(0),
        exit_fill_latency=timedelta(0),
        assumed_fee_bps=Decimal("0"),
        max_participation=Decimal("0.1"),
    )
    assert result.status is ExecutableBacktestStatus.NO_EXIT
    assert result.entry_fill is not None
    assert result.exit_fill is None


def test_invalid_market_data_controlled_status() -> None:
    event = _event()
    decision = T0 + timedelta(minutes=1)
    fill_ts = decision
    result = evaluate_executable_trade(
        event=event,
        record=_record(event, decision_time=decision),
        identity=_identity(),
        observations=(_obs(fill_ts, "0", liquidity="1000"),),
        side=Side.BUY,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
        entry_request_latency=timedelta(0),
        entry_fill_latency=timedelta(0),
        exit_request_latency=timedelta(0),
        exit_fill_latency=timedelta(0),
        assumed_fee_bps=Decimal("10"),
        max_participation=Decimal("0.1"),
    )
    assert result.status is ExecutableBacktestStatus.INVALID_MARKET_DATA


def test_extreme_decimal_no_raw_exception_nan_inf() -> None:
    event = _event()
    decision = T0 + timedelta(minutes=1)
    fill_ts = decision
    exit_ts = fill_ts + timedelta(minutes=5)
    huge = "1" + "0" * 80
    result = evaluate_executable_trade(
        event=event,
        record=_record(event, decision_time=decision),
        identity=_identity(),
        observations=(
            _obs(fill_ts, huge, liquidity="1"),
            _obs(exit_ts, "1", liquidity="1"),
        ),
        side=Side.BUY,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
        entry_request_latency=timedelta(0),
        entry_fill_latency=timedelta(0),
        exit_request_latency=timedelta(0),
        exit_fill_latency=timedelta(0),
        assumed_fee_bps=Decimal("10"),
        max_participation=Decimal("0.1"),
    )
    assert result.status in {
        ExecutableBacktestStatus.INVALID_MARKET_DATA,
        ExecutableBacktestStatus.INSUFFICIENT_LIQUIDITY,
        ExecutableBacktestStatus.UNFILLED,
        ExecutableBacktestStatus.PARTIAL,
        ExecutableBacktestStatus.NO_EXIT,
        ExecutableBacktestStatus.FULLY_FILLED,
    }
    for value in (
        result.gross_return,
        result.net_return,
        result.total_fee_cost,
        result.entry_fill.fill_price if result.entry_fill else None,
    ):
        if value is not None:
            assert value.is_finite()
            assert not value.is_nan()


# ---------------------------------------------------------------------------
# Edge retention / aggregate
# ---------------------------------------------------------------------------


def test_edge_retention_positive_zero_negative_gross() -> None:
    assert edge_retention(gross=Decimal("0.10"), net=Decimal("0.05")) == Decimal("0.5")
    zero = edge_retention(gross=Decimal("0"), net=Decimal("-0.01"))
    assert zero["semantics"] == "zero_gross"
    neg = edge_retention(gross=Decimal("-0.10"), net=Decimal("-0.12"))
    assert neg["semantics"] == "negative_gross"


# ---------------------------------------------------------------------------
# Frozen Phase4 identity / no discovery / no test tuning
# ---------------------------------------------------------------------------


def test_frozen_identity_preserved_no_discovery_invocation(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"n": 0}

    def _boom(*_a: object, **_k: object) -> object:
        called["n"] += 1
        raise AssertionError("discovery must not be invoked")

    monkeypatch.setattr(
        "newcoin_trader.research.feature_research_rules.discover_candidate_rules",
        _boom,
    )
    event = _event()
    decision = T0 + timedelta(minutes=1)
    fill_ts = decision
    exit_ts = fill_ts + timedelta(minutes=5)
    identity = _identity(fold=2)
    result = evaluate_executable_trade(
        event=event,
        record=_record(event, decision_time=decision),
        identity=identity,
        observations=(
            _obs(fill_ts, "1.00", liquidity="100000"),
            _obs(exit_ts, "1.05", liquidity="100000"),
        ),
        side=Side.BUY,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
        entry_request_latency=timedelta(0),
        entry_fill_latency=timedelta(0),
        exit_request_latency=timedelta(0),
        exit_fill_latency=timedelta(0),
        assumed_fee_bps=Decimal("0"),
        max_participation=Decimal("0.1"),
    )
    assert result.frozen_rule_id == identity.rule_id
    assert result.phase4_config_id == "cfg-phase4"
    assert result.fold_index == 2
    assert result.split_label == "test"
    assert called["n"] == 0
    # Ensure discover still exists but was not used by Phase5 path
    assert callable(discover_candidate_rules)


def test_service_rejects_missing_frozen_rule_identity() -> None:
    service = ExecutableBacktestService(session=MagicMock())
    with pytest.raises(ConfigError, match="frozen"):
        # sync validation path exposed for unit test
        service.validate_run_args(
            venue="binance",
            start=T0,
            end=T0 + timedelta(days=1),
            max_events=10,
            max_trades=10,
            max_execution_inputs=10,
            output_dir=Path("/tmp"),
            frozen_rules=(),
        )


# ---------------------------------------------------------------------------
# Deterministic artifacts / budgets
# ---------------------------------------------------------------------------


def test_deterministic_artifacts_compare_gross_and_net(tmp_path: Path) -> None:
    event = _event()
    decision = T0 + timedelta(minutes=1)
    fill_ts = decision
    exit_ts = fill_ts + timedelta(minutes=5)
    trade = evaluate_executable_trade(
        event=event,
        record=_record(event, decision_time=decision, gross="0.10"),
        identity=_identity(),
        observations=(
            _obs(fill_ts, "1.00", liquidity="100000"),
            _obs(exit_ts, "1.08", liquidity="100000"),
        ),
        side=Side.BUY,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
        entry_request_latency=timedelta(0),
        entry_fill_latency=timedelta(0),
        exit_request_latency=timedelta(0),
        exit_fill_latency=timedelta(0),
        assumed_fee_bps=Decimal("10"),
        max_participation=Decimal("0.1"),
    )
    report = build_executable_backtest_report(
        trades=(trade,),
        venue="binance",
        start=T0,
        end=T0 + timedelta(days=1),
        max_events=10,
        max_trades=100,
        max_execution_inputs=1000,
        latencies=(timedelta(0),),
        holding_periods=(timedelta(minutes=5),),
        position_notionals=(Decimal("100"),),
        max_participation=DEFAULT_MAX_PARTICIPATION,
        assumed_fee_bps=DEFAULT_ASSUMED_FEE_BPS[Venue.BINANCE],
        frozen_identities=(_identity(),),
    )
    assert report.meta.phase == "phase_5_executable_backtest"
    assert DISCLAIMER in report.meta.warnings
    assert WARNING_RESEARCH_ONLY in report.meta.warnings
    paths1 = emit_executable_backtest_artifacts(report, tmp_path / "a")
    paths2 = emit_executable_backtest_artifacts(report, tmp_path / "b")
    assert paths1["json"].read_text() == paths2["json"].read_text()
    assert paths1["csv"].read_text() == paths2["csv"].read_text()
    assert paths1["markdown"].read_text() == paths2["markdown"].read_text()
    payload = json.loads(paths1["json"].read_text())
    assert "aggregates" in payload
    assert "phase4_gross" in payload["aggregates"] or "mean_phase4_gross_return" in payload["aggregates"]
    md = paths1["markdown"].read_text()
    assert "net" in md.lower()
    assert "modeled" in md.lower() or "assumed" in md.lower()


def test_repository_budget_plus_one_overflow() -> None:
    import asyncio

    from newcoin_trader.database.repositories.executable_backtest import ExecutableBacktestRepository

    session = MagicMock()
    repo = ExecutableBacktestRepository(session)

    async def _run() -> None:
        result = MagicMock()
        result.all.return_value = [("row",)] * 3
        session.execute = AsyncMock(return_value=result)
        with pytest.raises(ConfigError, match="budget"):
            await repo.list_execution_observations(
                token_ids=[1],
                venue="binance",
                start=T0,
                end=T0 + timedelta(days=1),
                max_execution_inputs=2,
            )

    asyncio.run(_run())


def test_cli_requires_bounds_and_lists_command() -> None:
    help_result = runner.invoke(app, ["--help"])
    assert help_result.exit_code == 0
    assert "executable-backtest" in help_result.output

    missing = runner.invoke(app, ["executable-backtest"])
    assert missing.exit_code != 0

    bad = runner.invoke(
        app,
        [
            "executable-backtest",
            "--venue",
            "binance",
            "--start",
            "2024-01-01T00:00:00+00:00",
            "--end",
            "2024-01-02T00:00:00+00:00",
            "--max-events",
            "10",
            "--max-trades",
            "10",
            "--max-execution-inputs",
            "10",
            "--output-dir",
            "/tmp/x",
        ],
    )
    # Missing frozen rule identity should fail closed before DB
    assert bad.exit_code == 2
    assert "frozen" in bad.output.lower() or "rule" in bad.output.lower()


def test_aggregate_summaries_by_venue_rule_fold_confidence() -> None:
    event = _event()
    decision = T0 + timedelta(minutes=1)
    fill_ts = decision
    exit_ts = fill_ts + timedelta(minutes=5)
    trades = []
    for fold, gross in ((0, "0.10"), (1, "0.00"), (2, "-0.05")):
        trades.append(
            evaluate_executable_trade(
                event=event,
                record=_record(event, decision_time=decision, gross=gross),
                identity=_identity(fold=fold),
                observations=(
                    _obs(fill_ts, "1.00", liquidity="100000"),
                    _obs(exit_ts, "1.05" if Decimal(gross) >= 0 else "0.95", liquidity="100000"),
                ),
                side=Side.BUY,
                position_notional=Decimal("100"),
                holding_period=timedelta(minutes=5),
                entry_request_latency=timedelta(0),
                entry_fill_latency=timedelta(0),
                exit_request_latency=timedelta(0),
                exit_fill_latency=timedelta(0),
                assumed_fee_bps=Decimal("10"),
                max_participation=Decimal("0.1"),
            )
        )
    summary = aggregate_trades(trades)
    assert "by_venue" in summary
    assert "by_rule" in summary
    assert "by_fold" in summary
    assert "by_confidence" in summary
    assert summary["fill_coverage"] is not None
    assert MAX_LATENCY_GRID >= 1


@pytest.mark.asyncio
async def test_service_run_does_not_call_discovery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(*_a: object, **_k: object) -> object:
        raise AssertionError("Phase5 must not rediscover rules")

    monkeypatch.setattr(
        "newcoin_trader.research.feature_research_rules.discover_candidate_rules",
        _boom,
    )
    monkeypatch.setattr(
        "newcoin_trader.research.feature_research_rules.select_and_test_rules",
        _boom,
    )

    session = MagicMock()
    service = ExecutableBacktestService(session)
    identity = _identity()
    event = _event()
    decision = T0 + timedelta(minutes=1)
    fill_ts = decision
    exit_ts = fill_ts + timedelta(minutes=5)

    with (
        patch.object(
            service._repo,
            "list_listing_events",
            new=AsyncMock(return_value=[event]),
        ),
        patch.object(
            service._repo,
            "list_decision_records",
            new=AsyncMock(return_value=[_record(event, decision_time=decision)]),
        ),
        patch.object(
            service._repo,
            "list_execution_observations",
            new=AsyncMock(
                return_value=[
                    _obs(fill_ts, "1.00", liquidity="100000"),
                    _obs(exit_ts, "1.05", liquidity="100000"),
                ]
            ),
        ),
        patch.object(
            service._repo,
            "list_execution_trades",
            new=AsyncMock(return_value=[]),
        ),
        patch.object(
            service._repo,
            "list_historical_depth",
            new=AsyncMock(return_value=[]),
        ),
    ):
        report, paths = await service.run(
            venue="binance",
            start=T0,
            end=T0 + timedelta(days=1),
            max_events=10,
            max_trades=100,
            max_execution_inputs=1000,
            output_dir=tmp_path,
            frozen_rules=(identity,),
            latency_specs=["0s"],
            holding_specs=["5m"],
            position_notionals=(Decimal("100"),),
        )
    assert report.meta.event_count == 1
    assert paths["json"].exists()
    assert report.meta.phase == "phase_5_executable_backtest"
