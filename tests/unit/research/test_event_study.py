"""Phase 3 event-study: times, resolution, returns, censoring, determinism."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal, Overflow
from math import isclose
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from typer.testing import CliRunner

from newcoin_trader.cli.main import app
from newcoin_trader.database.repositories.event_study import EventStudyRepository
from newcoin_trader.domain.enums import Chain, Venue
from newcoin_trader.domain.event_study import (
    CellOutcomeStatus,
    EventStudyCellResult,
    MarketObservation,
    ObservationResolution,
    TokenListingEvent,
)
from newcoin_trader.errors import ConfigError, ResearchError
from newcoin_trader.research.event_study_aggregate import aggregate_results, deterministic_quantile
from newcoin_trader.research.event_study_config import (
    DEFAULT_ENTRY_DELAYS,
    DEFAULT_HOLDING_PERIODS,
    DEFAULT_MAX_OBSERVATIONS,
    format_duration,
    observation_snapshot_bounds,
    parse_duration,
    parse_duration_list,
    validate_event_study_bounds,
)
from newcoin_trader.research.event_study_engine import (
    _path_stats,
    evaluate_cell,
    prepare_observations,
    run_event_study,
)
from newcoin_trader.research.event_study_normalize import build_listing_event
from newcoin_trader.research.event_study_resolution import (
    resolution_from_provenance,
    supports_entry_delay,
)
from newcoin_trader.research.event_study_run import build_report, emit_event_study_artifacts

runner = CliRunner()


def _event(
    *,
    source_event_time: datetime,
    decision_available_time: datetime | None = None,
    first_seen_time: datetime | None = None,
    venue: Venue = Venue.BINANCE,
    token: str = "TOKEN",
    event_id: str = "e1",
    source: str | None = None,
    provenance: dict[str, str] | None = None,
) -> TokenListingEvent:
    seen = first_seen_time or source_event_time
    decision = decision_available_time or seen
    return TokenListingEvent(
        event_id=event_id,
        venue=venue,
        chain=Chain.BINANCE if venue is Venue.BINANCE else Chain.SOLANA,
        token_address=token,
        symbol="TOK",
        source=source if source is not None else venue.value,
        source_event_time=source_event_time,
        first_seen_time=seen,
        first_market_data_time=source_event_time,
        decision_available_time=decision,
        provenance=dict(provenance or {}),
    )


def _obs(
    ts: datetime,
    price: str,
    *,
    resolution: ObservationResolution = ObservationResolution.POINT,
    venue: Venue = Venue.BINANCE,
    token: str = "TOKEN",
    source: str = "binance:trades",
    provenance: dict[str, str] | None = None,
) -> MarketObservation:
    return MarketObservation(
        token_address=token,
        chain="binance" if venue is Venue.BINANCE else "solana",
        venue=venue,
        timestamp=ts,
        price=Decimal(price),
        resolution=resolution,
        source=source,
        provenance=provenance,
    )


# --- time construction / timezones ---


def test_token_listing_event_requires_utc() -> None:
    naive = datetime(2024, 1, 1, 12, 0, 0)
    with pytest.raises(ValueError, match="timezone-aware"):
        _event(source_event_time=naive)  # type: ignore[arg-type]


def test_token_listing_event_converts_non_utc_offset_to_utc() -> None:
    eastern = timezone(timedelta(hours=-5))
    local = datetime(2024, 1, 1, 7, 0, tzinfo=eastern)
    event = _event(source_event_time=local)
    assert event.source_event_time == datetime(2024, 1, 1, 12, 0, tzinfo=UTC)


def test_build_listing_event_uses_created_time_and_distinct_clocks() -> None:
    created = datetime(2024, 1, 1, tzinfo=UTC)
    seen = datetime(2024, 1, 1, 0, 5, tzinfo=UTC)
    first_md = datetime(2024, 1, 1, 0, 1, tzinfo=UTC)
    event = build_listing_event(
        token_id=7,
        token_address="ABC",
        chain="solana",
        symbol="ABC",
        source="birdeye",
        venue="birdeye",
        created_time=created,
        first_seen_time=seen,
        first_market_data_time=first_md,
        metadata_json={"pair_address": "PAIR"},
    )
    assert event.source_event_time == created
    assert event.first_seen_time == seen
    assert event.first_market_data_time == first_md
    assert event.decision_available_time == seen
    assert event.pair_address == "PAIR"
    assert event.provenance["source_event_time_field"] == "created_time"


def test_build_listing_event_falls_back_when_created_time_absent() -> None:
    seen = datetime(2024, 1, 1, tzinfo=UTC)
    event = build_listing_event(
        token_id=1,
        token_address="ABC",
        chain="binance",
        symbol="ABC",
        source="binance",
        venue="binance",
        created_time=None,
        first_seen_time=seen,
        first_market_data_time=None,
    )
    assert event.source_event_time == seen
    assert event.provenance["source_event_time_field"] == "first_seen_time_fallback"


# --- decision latency / no pre-discovery entries ---


def test_not_decision_available_when_entry_before_decision() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    event = _event(
        source_event_time=t0,
        decision_available_time=t0 + timedelta(minutes=2),
    )
    obs = [
        _obs(t0 + timedelta(seconds=10), "1.0"),
        _obs(t0 + timedelta(seconds=10) + timedelta(minutes=1), "1.1"),
    ]
    cell = evaluate_cell(
        event,
        obs,
        entry_delay=timedelta(seconds=10),
        holding_period=timedelta(minutes=1),
    )
    assert cell.status is CellOutcomeStatus.NOT_DECISION_AVAILABLE
    assert cell.simple_return is None


# --- resolution ---


def test_minute_ohlcv_rejects_subminute_entry_delay() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    event = _event(source_event_time=t0)
    obs = [
        _obs(
            t0 + timedelta(seconds=10),
            "1.0",
            resolution=ObservationResolution.MINUTE,
            source="binance:kline",
        ),
        _obs(
            t0 + timedelta(seconds=70),
            "1.1",
            resolution=ObservationResolution.MINUTE,
            source="binance:kline",
        ),
    ]
    cell = evaluate_cell(
        event,
        obs,
        entry_delay=timedelta(seconds=10),
        holding_period=timedelta(minutes=1),
    )
    assert cell.status is CellOutcomeStatus.UNSUPPORTED_RESOLUTION


def test_point_data_supports_10s_and_30s() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    event = _event(source_event_time=t0)
    for delay in (timedelta(seconds=10), timedelta(seconds=30)):
        entry = t0 + delay
        exit_ = entry + timedelta(minutes=1)
        obs = [_obs(entry, "100"), _obs(exit_, "110")]
        cell = evaluate_cell(event, obs, entry_delay=delay, holding_period=timedelta(minutes=1))
        assert cell.status is CellOutcomeStatus.COMPLETE
        assert cell.simple_return == Decimal("0.1")


def test_resolution_from_provenance_kline_1m_is_minute() -> None:
    assert resolution_from_provenance({"kind": "kline", "interval": "1m"}) is ObservationResolution.MINUTE
    assert resolution_from_provenance({"kind": "trade"}) is ObservationResolution.POINT
    assert not supports_entry_delay(ObservationResolution.MINUTE, timedelta(seconds=10))
    assert supports_entry_delay(ObservationResolution.POINT, timedelta(seconds=10))


# --- return arithmetic ---


def test_positive_negative_zero_and_nonpositive_returns() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    event = _event(source_event_time=t0)
    delay = timedelta(minutes=1)
    hold = timedelta(minutes=1)
    entry = t0 + delay
    exit_ = entry + hold

    up = evaluate_cell(
        event,
        [_obs(entry, "100"), _obs(exit_, "110")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert up.simple_return == Decimal("0.1")
    assert up.log_return is not None
    assert isclose(float(up.log_return), 0.09531017980432493, rel_tol=1e-9)

    down = evaluate_cell(
        event,
        [_obs(entry, "100"), _obs(exit_, "90")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert down.simple_return == Decimal("-0.1")

    flat = evaluate_cell(
        event,
        [_obs(entry, "100"), _obs(exit_, "100")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert flat.simple_return == Decimal("0")

    bad = evaluate_cell(
        event,
        [_obs(entry, "0"), _obs(exit_, "1")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert bad.status is CellOutcomeStatus.INVALID_MARKET_DATA

    neg = evaluate_cell(
        event,
        [_obs(entry, "1"), _obs(exit_, "-1")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert neg.status is CellOutcomeStatus.INVALID_MARKET_DATA


# --- missing / censoring ---


def test_missing_entry_exit_and_right_censoring() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    event = _event(source_event_time=t0)
    delay = timedelta(minutes=1)
    hold = timedelta(minutes=5)
    entry = t0 + delay
    exit_ = entry + hold

    missing_entry = evaluate_cell(
        event,
        [_obs(exit_, "1.1")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert missing_entry.status is CellOutcomeStatus.MISSING_ENTRY

    missing_exit = evaluate_cell(
        event,
        [_obs(entry, "1.0"), _obs(exit_ + timedelta(minutes=1), "1.2")],
        entry_delay=delay,
        holding_period=hold,
        data_horizon_end=exit_ + timedelta(minutes=10),
    )
    assert missing_exit.status is CellOutcomeStatus.MISSING_EXIT
    assert missing_exit.entry_price == Decimal("1.0")
    assert missing_exit.simple_return is None

    censored = evaluate_cell(
        event,
        [_obs(entry, "1.0")],
        entry_delay=delay,
        holding_period=hold,
        data_horizon_end=entry + timedelta(minutes=1),
    )
    assert censored.status is CellOutcomeStatus.RIGHT_CENSORED
    assert censored.simple_return is None


# --- lookahead adversarial ---


def test_future_data_does_not_change_entry_eligibility() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    event = _event(source_event_time=t0)
    delay = timedelta(minutes=1)
    hold = timedelta(minutes=1)
    entry = t0 + delay
    exit_ = entry + hold
    base = [_obs(entry, "1.0"), _obs(exit_, "1.1")]
    adversarial = [
        *base,
        _obs(entry + timedelta(seconds=1), "999"),  # after entry; must not create/alter entry
        _obs(exit_ + timedelta(hours=1), "0.01"),
    ]
    a = evaluate_cell(event, base, entry_delay=delay, holding_period=hold)
    b = evaluate_cell(event, adversarial, entry_delay=delay, holding_period=hold)
    assert a.status is CellOutcomeStatus.COMPLETE
    assert b.status is CellOutcomeStatus.COMPLETE
    assert a.entry_price == b.entry_price == Decimal("1.0")
    assert a.simple_return == b.simple_return

    # Future-only observation must not manufacture an entry.
    future_only = [_obs(entry + timedelta(minutes=10), "5.0")]
    cell = evaluate_cell(event, future_only, entry_delay=delay, holding_period=hold)
    assert cell.status is CellOutcomeStatus.MISSING_ENTRY


# --- unsorted / duplicates ---


def test_unsorted_input_and_duplicate_observations_are_deterministic() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    event = _event(source_event_time=t0)
    delay = timedelta(minutes=1)
    hold = timedelta(minutes=1)
    entry = t0 + delay
    exit_ = entry + hold
    messy = [
        _obs(exit_, "1.1", source="b"),
        _obs(entry, "1.0", source="a"),
        _obs(entry, "1.0", source="a"),  # duplicate
        _obs(exit_, "1.1", source="a"),  # same ts; finer/lex prefer
    ]
    prepared = prepare_observations(messy)
    assert len([o for o in prepared if o.timestamp == entry]) == 1
    cells_a = run_event_study([event], messy, entry_delays=[delay], holding_periods=[hold])
    cells_b = run_event_study([event], list(reversed(messy)), entry_delays=[delay], holding_periods=[hold])
    assert cells_a == cells_b


# --- venue separation ---


def test_venues_are_never_pooled() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    delay = timedelta(minutes=1)
    hold = timedelta(minutes=1)
    entry = t0 + delay
    exit_ = entry + hold
    events = [
        _event(source_event_time=t0, venue=Venue.BINANCE, token="A", event_id="bin"),
        _event(source_event_time=t0, venue=Venue.RAYDIUM, token="A", event_id="ray"),
    ]
    obs = [
        _obs(entry, "1.0", venue=Venue.BINANCE, token="A"),
        _obs(exit_, "1.2", venue=Venue.BINANCE, token="A"),
        _obs(entry, "1.0", venue=Venue.RAYDIUM, token="A", source="raydium"),
        _obs(exit_, "0.8", venue=Venue.RAYDIUM, token="A", source="raydium"),
    ]
    cells = run_event_study(events, obs, entry_delays=[delay], holding_periods=[hold])
    by_venue = {c.venue: c for c in cells}
    assert by_venue[Venue.BINANCE].simple_return == Decimal("0.2")
    assert by_venue[Venue.RAYDIUM].simple_return == Decimal("-0.2")
    aggs = aggregate_results(cells)
    assert len(aggs) == 2
    assert {a.venue for a in aggs} == {Venue.BINANCE, Venue.RAYDIUM}


# --- path / MFE MAE ---


def test_path_stats_use_observed_points_only() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    event = _event(source_event_time=t0)
    delay = timedelta(minutes=1)
    hold = timedelta(minutes=3)
    entry = t0 + delay
    mid = entry + timedelta(minutes=1)
    exit_ = entry + hold
    cell = evaluate_cell(
        event,
        [_obs(entry, "100"), _obs(mid, "150"), _obs(exit_, "120")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert cell.path.path_available is True
    assert cell.path.mfe == Decimal("0.5")
    assert cell.path.mae == Decimal("0")
    assert cell.path.time_to_peak == timedelta(minutes=1)


# --- config / bounds ---


def test_duration_parsing_and_defaults() -> None:
    assert parse_duration("10s") == timedelta(seconds=10)
    assert parse_duration("1m") == timedelta(minutes=1)
    assert parse_duration("2h") == timedelta(hours=2)
    assert format_duration(timedelta(seconds=30)) == "30s"
    assert DEFAULT_ENTRY_DELAYS[0] == timedelta(seconds=10)
    assert DEFAULT_HOLDING_PERIODS[-1] == timedelta(hours=24)
    with pytest.raises(ConfigError):
        parse_duration("1w")
    with pytest.raises(ConfigError):
        validate_event_study_bounds(
            start=datetime(2024, 1, 2, tzinfo=UTC),
            end=datetime(2024, 1, 1, tzinfo=UTC),
            max_events=10,
            entry_delays=DEFAULT_ENTRY_DELAYS,
            holding_periods=DEFAULT_HOLDING_PERIODS,
        )
    with pytest.raises(ConfigError):
        validate_event_study_bounds(
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=datetime(2024, 1, 2, tzinfo=UTC),
            max_events=0,
            entry_delays=DEFAULT_ENTRY_DELAYS,
            holding_periods=DEFAULT_HOLDING_PERIODS,
        )


def test_parse_duration_list_dedupes_deterministically() -> None:
    assert parse_duration_list(["1m", "1m", "5m"], default=()) == (
        timedelta(minutes=1),
        timedelta(minutes=5),
    )


def test_deterministic_quantile() -> None:
    values = [Decimal("0"), Decimal("1"), Decimal("2"), Decimal("3"), Decimal("4")]
    assert deterministic_quantile(values, Decimal("0.5")) == Decimal("2")
    assert deterministic_quantile(values, Decimal("0")) == Decimal("0")
    assert deterministic_quantile([], Decimal("0.5")) is None


# --- blocker: selection end ≠ data horizon end ---


def test_selection_end_does_not_censor_exit_after_selection() -> None:
    """Edge listing near selection end: valid exit after selection end must complete."""
    source = datetime(2024, 1, 1, 23, 59, 30, tzinfo=UTC)
    selection_end = datetime(2024, 1, 2, 0, 0, tzinfo=UTC)
    delay = timedelta(minutes=1)
    hold = timedelta(minutes=1)
    entry = source + delay  # 2024-01-02T00:00:30Z
    exit_ = entry + hold  # 2024-01-02T00:01:30Z
    event = _event(source_event_time=source)
    start = datetime(2024, 1, 1, tzinfo=UTC)

    complete = build_report(
        events=[event],
        observations=[_obs(entry, "1"), _obs(exit_, "2")],
        venue="binance",
        start=start,
        end=selection_end,
        max_events=10,
        entry_delays=[delay],
        holding_periods=[hold],
    )
    assert len(complete.cell_results) == 1
    assert complete.cell_results[0].status is CellOutcomeStatus.COMPLETE
    assert complete.cell_results[0].simple_return == Decimal("1")
    assert complete.meta.end == selection_end
    assert complete.meta.data_horizon_end == exit_
    assert complete.meta.data_horizon_end != selection_end

    censored = build_report(
        events=[event],
        observations=[_obs(entry, "1")],
        venue="binance",
        start=start,
        end=selection_end,
        max_events=10,
        entry_delays=[delay],
        holding_periods=[hold],
    )
    assert censored.cell_results[0].status is CellOutcomeStatus.RIGHT_CENSORED
    assert censored.cell_results[0].simple_return is None

    # Interior event (exit before selection end) unchanged.
    interior_source = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    interior_entry = interior_source + delay
    interior_exit = interior_entry + hold
    interior = build_report(
        events=[_event(source_event_time=interior_source, event_id="interior")],
        observations=[_obs(interior_entry, "1"), _obs(interior_exit, "2")],
        venue="binance",
        start=start,
        end=selection_end,
        max_events=10,
        entry_delays=[delay],
        holding_periods=[hold],
    )
    assert interior.cell_results[0].status is CellOutcomeStatus.COMPLETE
    assert interior.cell_results[0].simple_return == Decimal("1")


def test_build_report_carries_explicit_data_horizon_end() -> None:
    source = datetime(2024, 1, 1, 23, 59, 30, tzinfo=UTC)
    selection_end = datetime(2024, 1, 2, 0, 0, tzinfo=UTC)
    delay = timedelta(minutes=1)
    hold = timedelta(minutes=1)
    entry = source + delay
    exit_ = entry + hold
    event = _event(source_event_time=source)
    # Query horizon reaches exit; exact exit present → complete.
    report = build_report(
        events=[event],
        observations=[_obs(entry, "1"), _obs(exit_, "2")],
        venue="binance",
        start=datetime(2024, 1, 1, tzinfo=UTC),
        end=selection_end,
        max_events=10,
        entry_delays=[delay],
        holding_periods=[hold],
        data_horizon_end=exit_,
    )
    assert report.cell_results[0].status is CellOutcomeStatus.COMPLETE
    assert report.meta.data_horizon_end == exit_
    # Insufficient query horizon → right_censored even if we somehow had an exit stamp.
    short = build_report(
        events=[event],
        observations=[_obs(entry, "1"), _obs(exit_, "2")],
        venue="binance",
        start=datetime(2024, 1, 1, tzinfo=UTC),
        end=selection_end,
        max_events=10,
        entry_delays=[delay],
        holding_periods=[hold],
        data_horizon_end=entry,
    )
    assert short.cell_results[0].status is CellOutcomeStatus.RIGHT_CENSORED


# --- blocker: four event clocks retained in cell outputs ---


def test_four_event_clocks_survive_evaluate_cell_json_and_csv(tmp_path: Path) -> None:
    source = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    seen = datetime(2024, 1, 1, 0, 5, tzinfo=UTC)
    first_md = datetime(2024, 1, 1, 0, 2, tzinfo=UTC)
    decision = datetime(2024, 1, 1, 0, 7, tzinfo=UTC)
    delay = timedelta(minutes=10)
    hold = timedelta(minutes=1)
    entry = source + delay
    exit_ = entry + hold
    event = TokenListingEvent(
        event_id="clocks",
        venue=Venue.BINANCE,
        chain=Chain.BINANCE,
        token_address="TOKEN",
        symbol="TOK",
        source="listing-feed",
        source_event_time=source,
        first_seen_time=seen,
        first_market_data_time=first_md,
        decision_available_time=decision,
        provenance={"token_id": "3"},
    )
    assert len({source, seen, first_md, decision}) == 4
    obs = [_obs(entry, "10"), _obs(exit_, "11")]
    cell = evaluate_cell(event, obs, entry_delay=delay, holding_period=hold)
    assert cell.status is CellOutcomeStatus.COMPLETE
    assert cell.source_event_time == source
    assert cell.first_seen_time == seen
    assert cell.first_market_data_time == first_md
    assert cell.decision_available_time == decision

    report = build_report(
        events=[event],
        observations=obs,
        venue="binance",
        start=source,
        end=source + timedelta(days=1),
        max_events=10,
        entry_delays=[delay],
        holding_periods=[hold],
    )
    paths = emit_event_study_artifacts(report, tmp_path)
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    row = payload["cell_results"][0]
    assert row["source_event_time"] == source.isoformat()
    assert row["first_seen_time"] == seen.isoformat()
    assert row["first_market_data_time"] == first_md.isoformat()
    assert row["decision_available_time"] == decision.isoformat()
    assert row["event_source"] == "listing-feed"
    assert row["event_provenance"]["token_id"] == "3"

    csv_text = paths["csv"].read_text(encoding="utf-8")
    header = csv_text.splitlines()[0]
    for field in (
        "source_event_time",
        "first_seen_time",
        "first_market_data_time",
        "decision_available_time",
        "event_source",
        "event_provenance",
        "entry_source",
        "exit_source",
    ):
        assert field in header
    assert source.isoformat() in csv_text
    assert seen.isoformat() in csv_text
    assert first_md.isoformat() in csv_text
    assert decision.isoformat() in csv_text


# --- blocker: Decimal-native aggregation (no float conversion) ---


def test_aggregate_results_are_decimal_native_and_finite() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    delay = timedelta(minutes=1)
    hold = timedelta(minutes=1)
    entry = t0 + delay
    exit_ = entry + hold

    def _complete(event_id: str, entry_px: str, exit_px: str) -> EventStudyCellResult:
        return evaluate_cell(
            _event(source_event_time=t0, event_id=event_id),
            [_obs(entry, entry_px), _obs(exit_, exit_px)],
            entry_delay=delay,
            holding_period=hold,
        )

    ordinary = aggregate_results(
        [
            _complete("a", "100", "110"),  # +0.1
            _complete("b", "100", "90"),  # -0.1
            _complete("c", "100", "100"),  # 0
        ]
    )
    assert len(ordinary) == 1
    agg = ordinary[0]
    assert agg.mean_simple_return == Decimal("0")
    assert agg.median_simple_return == Decimal("0")
    assert agg.min_simple_return == Decimal("-0.1")
    assert agg.max_simple_return == Decimal("0.1")
    assert agg.win_rate == Decimal("1") / Decimal("3")
    assert agg.std_simple_return is not None and agg.std_simple_return.is_finite()
    for value in (
        agg.mean_simple_return,
        agg.median_simple_return,
        agg.std_simple_return,
        agg.win_rate,
        agg.p10,
        agg.p25,
        agg.p75,
        agg.p90,
        agg.min_simple_return,
        agg.max_simple_return,
    ):
        assert value is None or (isinstance(value, Decimal) and value.is_finite())

    # Deterministic type-7 quantiles on Decimal inputs.
    q_vals = [Decimal("-0.1"), Decimal("0"), Decimal("0.1")]
    assert deterministic_quantile(q_vals, Decimal("0.5")) == Decimal("0")
    assert deterministic_quantile(q_vals, Decimal("0.25")) == Decimal("-0.05")

    # Mixed positive/negative ordinary distribution stays finite Decimals.
    mixed_signs = aggregate_results(
        [
            _complete("p1", "100", "120"),
            _complete("n1", "100", "80"),
            _complete("p2", "50", "55"),
            _complete("n2", "50", "40"),
        ]
    )
    assert mixed_signs[0].mean_simple_return is not None
    assert mixed_signs[0].mean_simple_return.is_finite()
    assert mixed_signs[0].min_simple_return is not None and mixed_signs[0].min_simple_return < 0
    assert mixed_signs[0].max_simple_return is not None and mixed_signs[0].max_simple_return > 0
    assert mixed_signs[0].win_rate == Decimal("2") / Decimal("4")

    # Huge / tiny finite returns must not become NaN/Infinity via float.
    huge_cell = _complete("huge", "1", "1e999999")
    assert huge_cell.status is CellOutcomeStatus.COMPLETE
    assert huge_cell.simple_return is not None and huge_cell.simple_return.is_finite()
    tiny_cell = _complete("tiny", "1e999999", "1")
    assert tiny_cell.status is CellOutcomeStatus.COMPLETE
    assert tiny_cell.simple_return is not None and tiny_cell.simple_return.is_finite()
    huge_agg = aggregate_results([huge_cell])
    assert huge_agg[0].mean_simple_return is not None
    assert huge_agg[0].mean_simple_return.is_finite()
    assert not huge_agg[0].mean_simple_return.is_nan()
    tiny_agg = aggregate_results([tiny_cell])
    assert tiny_agg[0].mean_simple_return is not None
    assert tiny_agg[0].mean_simple_return.is_finite()
    assert not tiny_agg[0].mean_simple_return.is_nan()

    # Controlled research error when Decimal context cannot represent an aggregate.
    with pytest.raises(ResearchError):
        aggregate_results([huge_cell, tiny_cell])


# --- determinism / outputs ---


def test_full_run_is_deterministic_and_emits_artifacts(tmp_path: Path) -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    event = _event(source_event_time=t0)
    delay = timedelta(minutes=1)
    hold = timedelta(minutes=1)
    entry = t0 + delay
    exit_ = entry + hold
    obs = [_obs(entry, "10"), _obs(exit_, "11")]
    report_a = build_report(
        events=[event],
        observations=obs,
        venue="binance",
        start=t0,
        end=t0 + timedelta(days=1),
        max_events=10,
        entry_delays=[delay],
        holding_periods=[hold],
    )
    report_b = build_report(
        events=[event],
        observations=list(reversed(obs)),
        venue="binance",
        start=t0,
        end=t0 + timedelta(days=1),
        max_events=10,
        entry_delays=[delay],
        holding_periods=[hold],
    )
    assert report_a.meta.config_id == report_b.meta.config_id
    assert report_a.meta.run_id == report_b.meta.run_id
    assert report_a.cell_results == report_b.cell_results
    assert "descriptive_gross_market_return" in report_a.meta.study_kind
    paths = emit_event_study_artifacts(report_a, tmp_path)
    assert paths["json"].exists()
    assert paths["csv"].exists()
    assert paths["markdown"].exists()
    text = paths["json"].read_text(encoding="utf-8")
    assert "descriptive_gross_market_return_research_not_trading_advice" in text
    assert "not_executable_pnl_not_strategy_optimization" in text
    md = paths["markdown"].read_text(encoding="utf-8")
    assert "Phase 3 event-study" in md


def test_zero_sample_report_is_valid(tmp_path: Path) -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    report = build_report(
        events=[],
        observations=[],
        venue="binance",
        start=t0,
        end=t0 + timedelta(days=1),
        max_events=5,
        entry_delays=[timedelta(minutes=1)],
        holding_periods=[timedelta(minutes=1)],
    )
    assert report.meta.event_count == 0
    assert report.aggregates == ()
    assert report.cell_results == ()
    paths = emit_event_study_artifacts(report, tmp_path)
    assert '"event_count": 0' in paths["json"].read_text(encoding="utf-8")


# --- CLI bounds ---


def test_event_study_help_and_required_bounds() -> None:
    help_result = runner.invoke(app, ["event-study", "--help"])
    assert help_result.exit_code == 0
    assert "max-events" in help_result.output
    assert "venue" in help_result.output.lower()

    root = runner.invoke(app, ["--help"])
    assert "event-study" in root.output

    missing = runner.invoke(app, ["event-study", "--venue", "binance"])
    assert missing.exit_code != 0

    bad_range = runner.invoke(
        app,
        [
            "event-study",
            "--venue",
            "binance",
            "--start",
            "2024-01-02T00:00:00+00:00",
            "--end",
            "2024-01-01T00:00:00+00:00",
            "--max-events",
            "10",
            "--output-dir",
            "/tmp/newcoin-event-study-test",
        ],
    )
    # Fails at config validation (may also fail DB connect depending on order).
    assert bad_range.exit_code == 2

    bad_max = runner.invoke(
        app,
        [
            "event-study",
            "--venue",
            "binance",
            "--start",
            "2024-01-01T00:00:00Z",
            "--end",
            "2024-01-02T00:00:00Z",
            "--max-events",
            "0",
            "--output-dir",
            "/tmp/newcoin-event-study-test",
        ],
    )
    assert bad_max.exit_code == 2


# --- blocker: provenance retained from evaluate_cell through artifacts ---


def test_cell_provenance_survives_json_and_csv(tmp_path: Path) -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    delay = timedelta(minutes=1)
    hold = timedelta(minutes=1)
    entry = t0 + delay
    exit_ = entry + hold
    event = _event(
        source_event_time=t0,
        source="listing-feed",
        provenance={"token_id": "7", "source_event_time_field": "created_time"},
    )
    obs = [
        _obs(entry, "10", source="entry-feed", provenance={"obs_id": "entry-1", "kind": "trade"}),
        _obs(exit_, "11", source="exit-feed", provenance={"obs_id": "exit-9", "kind": "trade"}),
    ]
    cell = evaluate_cell(event, obs, entry_delay=delay, holding_period=hold)
    assert cell.status is CellOutcomeStatus.COMPLETE
    assert cell.event_source == "listing-feed"
    assert cell.event_provenance == {"token_id": "7", "source_event_time_field": "created_time"}
    assert cell.entry_source == "entry-feed"
    assert cell.entry_provenance == {"obs_id": "entry-1", "kind": "trade"}
    assert cell.exit_source == "exit-feed"
    assert cell.exit_provenance == {"obs_id": "exit-9", "kind": "trade"}
    assert cell.event_source != cell.entry_source != cell.exit_source

    report = build_report(
        events=[event],
        observations=obs,
        venue="binance",
        start=t0,
        end=t0 + timedelta(days=1),
        max_events=10,
        entry_delays=[delay],
        holding_periods=[hold],
    )
    paths = emit_event_study_artifacts(report, tmp_path)
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert "cell_results" in payload
    assert len(payload["cell_results"]) == 1
    row = payload["cell_results"][0]
    assert row["event_source"] == "listing-feed"
    assert row["entry_source"] == "entry-feed"
    assert row["exit_source"] == "exit-feed"
    assert row["event_provenance"]["token_id"] == "7"
    assert row["entry_provenance"]["obs_id"] == "entry-1"
    assert row["exit_provenance"]["obs_id"] == "exit-9"

    csv_text = paths["csv"].read_text(encoding="utf-8")
    header = csv_text.splitlines()[0]
    for field in (
        "event_source",
        "event_provenance",
        "entry_source",
        "entry_provenance",
        "exit_source",
        "exit_provenance",
    ):
        assert field in header
    assert "listing-feed" in csv_text
    assert "entry-feed" in csv_text
    assert "exit-feed" in csv_text
    assert "entry-1" in csv_text
    assert "exit-9" in csv_text


# --- blocker: bounded snapshot reads from event+grid windows ---


def test_observation_snapshot_bounds_from_events_and_grid() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    t1 = datetime(2024, 1, 2, tzinfo=UTC)
    events = [
        _event(source_event_time=t1, event_id="late"),
        _event(source_event_time=t0, event_id="early"),
    ]
    delays = (timedelta(minutes=5), timedelta(hours=1))
    holdings = (timedelta(minutes=10), timedelta(hours=2))
    lower, upper = observation_snapshot_bounds(events, delays, holdings)
    assert lower == t0
    assert upper == t1 + timedelta(hours=1) + timedelta(hours=2)


def test_observations_query_includes_lower_upper_and_budget_cap() -> None:
    from sqlalchemy.dialects import postgresql

    start = datetime(2024, 6, 1, tzinfo=UTC)
    end = datetime(2024, 6, 2, tzinfo=UTC)
    repo = EventStudyRepository(session=MagicMock())
    stmt = repo._observations_query(
        token_ids=[42],
        venue="binance",
        start=start,
        end=end,
        max_observations=1000,
    )
    sql = str(stmt.compile(dialect=postgresql.dialect())).lower()
    assert "price_snapshots.timestamp >=" in sql or "timestamp >=" in sql
    assert "price_snapshots.timestamp <=" in sql or "timestamp <=" in sql
    assert "limit" in sql
    # budget+1 fetch: never materialize an unbounded result set
    assert "1001" in str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))


@pytest.mark.asyncio
async def test_list_observations_raises_when_budget_exceeded() -> None:
    session = AsyncMock()
    # Simulate budget+1 rows returned (budget=2 → fetch 3).
    fake_rows = [(MagicMock(), MagicMock()) for _ in range(3)]
    result = MagicMock()
    result.all.return_value = fake_rows
    session.execute = AsyncMock(return_value=result)

    # Normalize path needs realish token attrs — patch build path via side_effect avoidance:
    # Make list_observations check length before mapping when possible.
    repo = EventStudyRepository(session=session)

    # Monkeypatch mapping by ensuring we raise before build when over budget.
    # Provide token_ids and bounds; the repository must raise ConfigError.
    with pytest.raises(ConfigError, match="observation"):
        await repo.list_observations_for_tokens(
            token_ids=[1],
            venue="binance",
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=datetime(2024, 1, 2, tzinfo=UTC),
            max_observations=2,
        )


def test_default_max_observations_is_finite_positive() -> None:
    assert isinstance(DEFAULT_MAX_OBSERVATIONS, int)
    assert DEFAULT_MAX_OBSERVATIONS > 0


# --- blocker: finite Decimal-safe return arithmetic ---


def test_extreme_and_nonfinite_returns_are_finite_or_invalid() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    event = _event(source_event_time=t0)
    delay = timedelta(minutes=1)
    hold = timedelta(minutes=1)
    entry = t0 + delay
    exit_ = entry + hold

    huge = evaluate_cell(
        event,
        [_obs(entry, "1e-200"), _obs(exit_, "1e200")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert huge.status is CellOutcomeStatus.COMPLETE
    assert huge.simple_return is not None and huge.simple_return.is_finite()
    assert huge.log_return is not None and huge.log_return.is_finite()
    assert huge.simple_return > 0

    tiny = evaluate_cell(
        event,
        [_obs(entry, "1e200"), _obs(exit_, "1e-200")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert tiny.status is CellOutcomeStatus.COMPLETE
    assert tiny.simple_return is not None and tiny.simple_return.is_finite()
    assert tiny.log_return is not None and tiny.log_return.is_finite()
    assert tiny.simple_return < 0

    normal = evaluate_cell(
        event,
        [_obs(entry, "100"), _obs(exit_, "110")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert normal.status is CellOutcomeStatus.COMPLETE
    assert normal.simple_return == Decimal("0.1")
    assert normal.log_return is not None and normal.log_return.is_finite()

    nonpos = evaluate_cell(
        event,
        [_obs(entry, "0"), _obs(exit_, "1")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert nonpos.status is CellOutcomeStatus.INVALID_MARKET_DATA

    # Non-finite Decimal prices are rejected by MarketObservation validation; where
    # constructible via model_construct, evaluate_cell must still classify invalid.
    nan_obs = MarketObservation.model_construct(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=entry,
        price=Decimal("NaN"),
        resolution=ObservationResolution.POINT,
        source="binance:trades",
        provenance=None,
    )
    nan_entry = evaluate_cell(
        event,
        [nan_obs, _obs(exit_, "1")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert nan_entry.status is CellOutcomeStatus.INVALID_MARKET_DATA

    inf_obs = MarketObservation.model_construct(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=exit_,
        price=Decimal("Infinity"),
        resolution=ObservationResolution.POINT,
        source="binance:trades",
        provenance=None,
    )
    inf_exit = evaluate_cell(
        event,
        [_obs(entry, "1"), inf_obs],
        entry_delay=delay,
        holding_period=hold,
    )
    assert inf_exit.status is CellOutcomeStatus.INVALID_MARKET_DATA


# --- blocker: intermediate path prices must be finite positive ---


def test_invalid_intermediate_path_prices_are_not_silently_dropped() -> None:
    """Any invalid included path point → invalid_market_data (never silent drop)."""
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    event = _event(source_event_time=t0)
    delay = timedelta(minutes=1)
    hold = timedelta(minutes=3)
    entry = t0 + delay
    mid = entry + timedelta(minutes=1)
    exit_ = entry + hold

    zero_mid = evaluate_cell(
        event,
        [_obs(entry, "100"), _obs(mid, "0"), _obs(exit_, "120")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert zero_mid.status is CellOutcomeStatus.INVALID_MARKET_DATA
    assert zero_mid.simple_return is None

    neg_mid = evaluate_cell(
        event,
        [_obs(entry, "100"), _obs(mid, "-5"), _obs(exit_, "120")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert neg_mid.status is CellOutcomeStatus.INVALID_MARKET_DATA

    nan_mid = MarketObservation.model_construct(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=mid,
        price=Decimal("NaN"),
        resolution=ObservationResolution.POINT,
        source="binance:trades",
        provenance=None,
    )
    nonfinite_mid = evaluate_cell(
        event,
        [_obs(entry, "100"), nan_mid, _obs(exit_, "120")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert nonfinite_mid.status is CellOutcomeStatus.INVALID_MARKET_DATA

    # Valid path unchanged.
    ok = evaluate_cell(
        event,
        [_obs(entry, "100"), _obs(mid, "150"), _obs(exit_, "120")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert ok.status is CellOutcomeStatus.COMPLETE
    assert ok.path.path_available is True
    assert ok.path.mfe == Decimal("0.5")
    assert ok.path.mae == Decimal("0")


# --- blocker: Decimal Overflow must not escape event-cell evaluation ---


def _assert_no_nan_inf_complete(cell: EventStudyCellResult) -> None:
    assert cell.status is CellOutcomeStatus.COMPLETE
    for value in (
        cell.simple_return,
        cell.log_return,
        cell.entry_price,
        cell.exit_price,
        cell.path.mfe,
        cell.path.mae,
        cell.path.peak_price,
        cell.path.trough_price,
    ):
        assert value is not None
        assert value.is_finite()
        assert not value.is_nan()
        assert not value.is_infinite()


def test_decimal_overflow_in_returns_and_path_ratios_is_invalid_market_data() -> None:
    """Raw Decimal Overflow on ratio/simple/log or peak/trough → invalid_market_data."""
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    event = _event(source_event_time=t0)
    delay = timedelta(minutes=1)
    hold = timedelta(minutes=2)
    entry = t0 + delay
    mid = entry + timedelta(minutes=1)
    exit_ = entry + hold

    # Endpoint ratio Overflow (exit/entry) must not escape.
    endpoint = evaluate_cell(
        event,
        [_obs(entry, "1e-999999"), _obs(exit_, "1e999999")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert endpoint.status is CellOutcomeStatus.INVALID_MARKET_DATA
    assert endpoint.simple_return is None
    assert endpoint.log_return is None

    # Peak path ratio Overflow while endpoint returns remain representable.
    peak = evaluate_cell(
        event,
        [_obs(entry, "1e-999999"), _obs(mid, "1e999999"), _obs(exit_, "2e-999999")],
        entry_delay=delay,
        holding_period=hold,
    )
    assert peak.status is CellOutcomeStatus.INVALID_MARKET_DATA
    assert peak.simple_return is None
    assert peak.log_return is None

    # Trough path ratio Overflow: entry_price << trough (and peak). Both path ratios
    # overflow; _path_stats must return None rather than raise Decimal Overflow.
    trough_price = Decimal("5e999998")
    tiny_entry = Decimal("1e-999999")
    with pytest.raises(Overflow):
        _ = (trough_price / tiny_entry) - Decimal("1")
    trough_stats = _path_stats(
        (_obs(entry, "1e999999"), _obs(mid, "5e999998"), _obs(exit_, "2e999999")),
        entry_time=entry,
        entry_price=tiny_entry,
    )
    assert trough_stats is None

    # Ordinary large-but-representable magnitudes keep prior COMPLETE semantics.
    hold_1m = timedelta(minutes=1)
    exit_1m = entry + hold_1m
    huge = evaluate_cell(
        event,
        [_obs(entry, "1e-200"), _obs(exit_1m, "1e200")],
        entry_delay=delay,
        holding_period=hold_1m,
    )
    _assert_no_nan_inf_complete(huge)
    assert huge.simple_return is not None and huge.simple_return > 0

    tiny = evaluate_cell(
        event,
        [_obs(entry, "1e200"), _obs(exit_1m, "1e-200")],
        entry_delay=delay,
        holding_period=hold_1m,
    )
    _assert_no_nan_inf_complete(tiny)
    assert tiny.simple_return is not None and tiny.simple_return < 0

    normal = evaluate_cell(
        event,
        [_obs(entry, "100"), _obs(exit_1m, "110")],
        entry_delay=delay,
        holding_period=hold_1m,
    )
    _assert_no_nan_inf_complete(normal)
    assert normal.simple_return == Decimal("0.1")
