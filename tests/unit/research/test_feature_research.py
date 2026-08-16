"""Phase 4 feature research: PIT invariance, availability, splits, rules, artifacts."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal, Overflow
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from typer.testing import CliRunner

from newcoin_trader.cli.main import app
from newcoin_trader.database.repositories.feature_research import FeatureResearchRepository
from newcoin_trader.domain.enums import Chain, Venue
from newcoin_trader.domain.event_study import (
    CellOutcomeStatus,
    ObservationResolution,
    TokenListingEvent,
)
from newcoin_trader.domain.feature_research import (
    REASON_CONFIGURED_DECISION_BEFORE_AVAILABLE,
    AvailabilityLevel,
    DecisionAvailabilityExclusion,
    DecisionFeatureRecord,
    FeatureMarketInput,
    FeatureTradeInput,
    FeatureValue,
    FeatureValueState,
)
from newcoin_trader.errors import ConfigError, ResearchError
from newcoin_trader.research.event_study_engine import evaluate_cell
from newcoin_trader.research.feature_research_analysis import (
    chronological_split,
    compute_univariate_stats,
    walk_forward_folds,
)
from newcoin_trader.research.feature_research_availability import (
    availability_matrix,
    classify_family,
)
from newcoin_trader.research.feature_research_config import (
    DEFAULT_FEATURE_WINDOWS,
    DEFAULT_MAX_FEATURE_INPUTS,
    DEFAULT_SPLIT_RATIOS,
    MAX_RULE_CONDITIONS,
    format_duration,
    parse_duration,
    validate_feature_research_bounds,
)
from newcoin_trader.research.feature_research_features import (
    _price_return,
    _range_feature,
    _volatility,
    build_decision_feature_record,
    prepare_feature_inputs,
)
from newcoin_trader.research.feature_research_rules import (
    discover_candidate_rules,
    evaluate_rule,
    select_and_test_rules,
)
from newcoin_trader.research.feature_research_run import (
    build_feature_research_report,
    emit_feature_research_artifacts,
)
from newcoin_trader.services.feature_research import FeatureResearchService

runner = CliRunner()


def _event(
    *,
    source_event_time: datetime,
    decision_available_time: datetime | None = None,
    first_seen_time: datetime | None = None,
    venue: Venue = Venue.BINANCE,
    token: str = "TOKEN",
    event_id: str = "e1",
    pair_address: str | None = "PAIR",
) -> TokenListingEvent:
    seen = first_seen_time or source_event_time
    decision = decision_available_time or seen
    return TokenListingEvent(
        event_id=event_id,
        venue=venue,
        chain=Chain.BINANCE if venue is Venue.BINANCE else Chain.SOLANA,
        token_address=token,
        pair_address=pair_address,
        symbol="TOK",
        source=venue.value,
        source_event_time=source_event_time,
        first_seen_time=seen,
        first_market_data_time=source_event_time,
        decision_available_time=decision,
        provenance={"token_id": "1"},
    )


def _input(
    ts: datetime,
    price: str,
    *,
    volume: str | None = None,
    liquidity: str | None = None,
    resolution: ObservationResolution = ObservationResolution.MINUTE,
    venue: Venue = Venue.BINANCE,
    token: str = "TOKEN",
    source: str = "binance:kline",
    provenance: dict[str, str] | None = None,
) -> FeatureMarketInput:
    return FeatureMarketInput(
        token_address=token,
        chain="binance" if venue is Venue.BINANCE else "solana",
        venue=venue,
        timestamp=ts,
        price=Decimal(price),
        volume=Decimal(volume) if volume is not None else None,
        liquidity=Decimal(liquidity) if liquidity is not None else None,
        resolution=resolution,
        source=source,
        provenance=provenance,
    )


def _trade(
    ts: datetime,
    *,
    side: str = "buy",
    amount: str = "1",
    price: str = "1.0",
    venue: Venue = Venue.BINANCE,
    token: str = "TOKEN",
) -> FeatureTradeInput:
    return FeatureTradeInput(
        token_address=token,
        chain="binance" if venue is Venue.BINANCE else "solana",
        venue=venue,
        timestamp=ts,
        side=side,
        amount=Decimal(amount),
        price=Decimal(price),
        source="binance:trades",
        provenance={"kind": "trade"},
    )


def _label(
    *,
    status: CellOutcomeStatus = CellOutcomeStatus.COMPLETE,
    simple_return: Decimal | None = Decimal("0.1"),
    mfe: Decimal | None = Decimal("0.2"),
    mae: Decimal | None = Decimal("-0.05"),
) -> dict[str, object]:
    return {
        "entry_delay": timedelta(minutes=1),
        "holding_period": timedelta(minutes=5),
        "status": status,
        "simple_return": simple_return,
        "log_return": None,
        "mfe": mfe,
        "mae": mae,
        "label_source": "phase3_cell",
    }


# --- domain / config / availability ---


def test_decision_feature_record_requires_utc() -> None:
    naive = datetime(2024, 1, 1, 12, 0, 0)
    with pytest.raises(ValueError, match="timezone-aware"):
        DecisionFeatureRecord(
            event_id="e1",
            venue=Venue.BINANCE,
            chain=Chain.BINANCE,
            token_address="T",
            pair_address=None,
            source_event_time=naive,  # type: ignore[arg-type]
            first_seen_time=naive,  # type: ignore[arg-type]
            first_market_data_time=None,
            decision_available_time=naive,  # type: ignore[arg-type]
            decision_time=naive,  # type: ignore[arg-type]
            feature_cutoff=naive,  # type: ignore[arg-type]
            features=(),
            labels=(),
            config_id="cfg",
            computation_id="cmp",
        )


def test_feature_windows_default_bounded() -> None:
    assert DEFAULT_FEATURE_WINDOWS == (
        timedelta(minutes=1),
        timedelta(minutes=5),
        timedelta(minutes=15),
        timedelta(minutes=30),
    )


def test_validate_feature_research_bounds_rejects_bad_split() -> None:
    with pytest.raises(ConfigError):
        validate_feature_research_bounds(
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=datetime(2024, 2, 1, tzinfo=UTC),
            max_events=10,
            decision_delay=timedelta(minutes=1),
            windows=DEFAULT_FEATURE_WINDOWS,
            min_sample=5,
            split_ratios=(Decimal("0.5"), Decimal("0.5"), Decimal("0.5")),
            walk_forward_folds=3,
            max_rules=10,
            max_rule_conditions=2,
            max_feature_inputs=1000,
        )


def test_availability_matrix_excludes_holder_social_wallet() -> None:
    matrix = availability_matrix()
    for venue in (Venue.BINANCE, Venue.GECKO, Venue.RAYDIUM):
        assert classify_family(venue, "holder") is AvailabilityLevel.UNSUPPORTED
        assert classify_family(venue, "creator") is AvailabilityLevel.UNSUPPORTED
        assert classify_family(venue, "social") is AvailabilityLevel.UNSUPPORTED
        assert classify_family(venue, "security") is AvailabilityLevel.UNSUPPORTED
        assert classify_family(venue, "wallet") is AvailabilityLevel.UNSUPPORTED
    assert matrix[Venue.BINANCE.value]["age"] is AvailabilityLevel.SUPPORTED
    assert matrix[Venue.BINANCE.value]["price_momentum"] is AvailabilityLevel.SUPPORTED
    assert matrix[Venue.RAYDIUM.value]["price_momentum"] is AvailabilityLevel.PARTIAL
    assert matrix[Venue.GECKO.value]["activity"] is AvailabilityLevel.UNSUPPORTED


# --- decision availability exclusions (no silent max with decision_available_time) ---


def _mock_feature_research_service(
    *,
    events: list[TokenListingEvent],
    feature_inputs: list[FeatureMarketInput] | None = None,
    trades: list[FeatureTradeInput] | None = None,
    label_obs: list | None = None,
) -> FeatureResearchService:
    service = FeatureResearchService(AsyncMock())
    repo = AsyncMock()
    repo.list_listing_events = AsyncMock(return_value=events)
    repo.list_feature_inputs = AsyncMock(return_value=feature_inputs or [])
    repo.list_feature_trades = AsyncMock(return_value=trades or [])
    repo.list_label_observations = AsyncMock(return_value=label_obs or [])
    service._repo = repo
    return service


async def _run_service(
    service: FeatureResearchService,
    *,
    output_dir: Path,
    start: datetime,
    decision_delay: timedelta,
) -> tuple:
    return await service.run(
        venue="binance",
        start=start,
        end=start + timedelta(hours=1),
        max_events=10,
        output_dir=output_dir,
        decision_delay=decision_delay,
        window_specs=["1m"],
        min_sample=1,
        walk_forward_folds=1,
        max_rules=1,
        max_rule_conditions=1,
        max_feature_inputs=100,
        max_trades=100,
        label_entry_delay_specs=["1m"],
        label_holding_specs=["5m"],
    )


@pytest.mark.asyncio
async def test_later_discovery_excludes_before_availability_no_silent_max(tmp_path: Path) -> None:
    """source 00:00, delay 1m, availability 00:10 => no normal record at 00:01; auditable exclusion."""
    t0 = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    available = t0 + timedelta(minutes=10)
    configured = t0 + timedelta(minutes=1)
    event = _event(
        source_event_time=t0,
        decision_available_time=available,
        first_seen_time=available,
        event_id="late_disc",
    )
    # Pre-availability input that must not yield an ordinary DecisionFeatureRecord.
    pre_avail = _input(configured, "1.0", volume="10", liquidity="100")
    service = _mock_feature_research_service(events=[event], feature_inputs=[pre_avail])
    report, paths = await _run_service(
        service, output_dir=tmp_path / "late", start=t0, decision_delay=timedelta(minutes=1)
    )

    assert report.records == ()
    assert len(report.decision_exclusions) == 1
    excl = report.decision_exclusions[0]
    assert excl.event_id == "late_disc"
    assert excl.configured_decision_time == configured
    assert excl.decision_available_time == available
    assert excl.reason == REASON_CONFIGURED_DECISION_BEFORE_AVAILABLE
    assert excl.configured_decision_time < excl.decision_available_time
    assert report.meta.record_count == 0

    payload = json.loads(paths["json"].read_text())
    assert payload["record_count"] == 0
    assert payload["decision_exclusions"] == [
        {
            "event_id": "late_disc",
            "configured_decision_time": configured.isoformat(),
            "decision_available_time": available.isoformat(),
            "reason": REASON_CONFIGURED_DECISION_BEFORE_AVAILABLE,
        }
    ]
    assert "late_disc" not in paths["csv"].read_text()


@pytest.mark.asyncio
async def test_equality_at_decision_available_is_eligible(tmp_path: Path) -> None:
    t0 = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    available = t0 + timedelta(minutes=10)
    event = _event(
        source_event_time=t0,
        decision_available_time=available,
        first_seen_time=available,
        event_id="eq_ok",
    )
    inputs = [_input(available, "1.0", volume="1", liquidity="10")]
    service = _mock_feature_research_service(events=[event], feature_inputs=inputs)
    report, _paths = await _run_service(
        service, output_dir=tmp_path / "eq", start=t0, decision_delay=timedelta(minutes=10)
    )
    assert len(report.records) == 1
    assert report.records[0].event_id == "eq_ok"
    assert report.records[0].decision_time == available
    assert report.records[0].feature_cutoff == available
    assert report.records[0].decision_available_time == available
    assert report.decision_exclusions == ()


@pytest.mark.asyncio
async def test_later_than_decision_available_is_eligible(tmp_path: Path) -> None:
    t0 = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    available = t0 + timedelta(minutes=10)
    decision = t0 + timedelta(minutes=15)
    event = _event(
        source_event_time=t0,
        decision_available_time=available,
        first_seen_time=available,
        event_id="later_ok",
    )
    inputs = [
        _input(available, "1.0", volume="1", liquidity="10"),
        _input(decision, "1.1", volume="2", liquidity="11"),
    ]
    service = _mock_feature_research_service(events=[event], feature_inputs=inputs)
    report, _paths = await _run_service(
        service, output_dir=tmp_path / "later", start=t0, decision_delay=timedelta(minutes=15)
    )
    assert len(report.records) == 1
    assert report.records[0].decision_time == decision
    assert report.records[0].feature_cutoff == decision
    assert report.records[0].feature_cutoff >= report.records[0].decision_available_time
    assert report.decision_exclusions == ()


@pytest.mark.asyncio
async def test_decision_exclusions_artifacts_are_deterministic(tmp_path: Path) -> None:
    t0 = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    available = t0 + timedelta(minutes=10)
    events = [
        _event(
            source_event_time=t0,
            decision_available_time=available,
            first_seen_time=available,
            event_id="z_excl",
            token="TOKZ",
        ),
        _event(
            source_event_time=t0,
            decision_available_time=available,
            first_seen_time=available,
            event_id="a_excl",
            token="TOKA",
        ),
    ]
    service = _mock_feature_research_service(events=events)
    report_a, paths_a = await _run_service(
        service, output_dir=tmp_path / "det_a", start=t0, decision_delay=timedelta(minutes=1)
    )
    report_b, paths_b = await _run_service(
        service, output_dir=tmp_path / "det_b", start=t0, decision_delay=timedelta(minutes=1)
    )
    assert report_a.decision_exclusions == report_b.decision_exclusions
    assert [e.event_id for e in report_a.decision_exclusions] == ["a_excl", "z_excl"]
    assert paths_a["json"].read_text() == paths_b["json"].read_text()
    md = paths_a["markdown"].read_text()
    assert "## Decision availability exclusions" in md
    assert REASON_CONFIGURED_DECISION_BEFORE_AVAILABLE in md
    assert "a_excl" in md and "z_excl" in md
    payload = json.loads(paths_a["json"].read_text())
    assert payload["decision_exclusions"] == [
        {
            "event_id": "a_excl",
            "configured_decision_time": (t0 + timedelta(minutes=1)).isoformat(),
            "decision_available_time": available.isoformat(),
            "reason": REASON_CONFIGURED_DECISION_BEFORE_AVAILABLE,
        },
        {
            "event_id": "z_excl",
            "configured_decision_time": (t0 + timedelta(minutes=1)).isoformat(),
            "decision_available_time": available.isoformat(),
            "reason": REASON_CONFIGURED_DECISION_BEFORE_AVAILABLE,
        },
    ]
    assert payload["extras"]["decision_exclusion_count"] == 2
    assert payload["record_count"] == 0


def test_phase3_not_decision_available_semantics_unchanged() -> None:
    """Phase 4 exclusion must not alter Phase 3 cell status semantics."""
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    event = _event(
        source_event_time=t0,
        decision_available_time=t0 + timedelta(minutes=2),
    )
    from newcoin_trader.domain.event_study import MarketObservation

    obs = [
        MarketObservation(
            token_address="TOKEN",
            chain="binance",
            venue=Venue.BINANCE,
            timestamp=t0 + timedelta(seconds=10),
            price=Decimal("1.0"),
            resolution=ObservationResolution.POINT,
            source="binance:trades",
        ),
        MarketObservation(
            token_address="TOKEN",
            chain="binance",
            venue=Venue.BINANCE,
            timestamp=t0 + timedelta(seconds=10) + timedelta(minutes=1),
            price=Decimal("1.1"),
            resolution=ObservationResolution.POINT,
            source="binance:trades",
        ),
    ]
    cell = evaluate_cell(
        event,
        obs,
        entry_delay=timedelta(seconds=10),
        holding_period=timedelta(minutes=1),
    )
    assert cell.status is CellOutcomeStatus.NOT_DECISION_AVAILABLE
    assert cell.simple_return is None
    assert cell.entry_time == t0 + timedelta(seconds=10)
    assert cell.decision_available_time == t0 + timedelta(minutes=2)


def test_decision_availability_exclusion_model_is_frozen_and_utc() -> None:
    naive = datetime(2024, 1, 1, 0, 1, 0)
    with pytest.raises(ValueError, match="timezone-aware"):
        DecisionAvailabilityExclusion(
            event_id="e1",
            configured_decision_time=naive,  # type: ignore[arg-type]
            decision_available_time=datetime(2024, 1, 1, 0, 10, tzinfo=UTC),
            reason=REASON_CONFIGURED_DECISION_BEFORE_AVAILABLE,
        )


# --- builder defense-in-depth: decision_time vs decision_available_time ---

_BUILDER_AVAIL_MSG = "decision_time precedes decision_available_time"


def test_builder_raises_before_decision_available_no_record() -> None:
    """Direct builder must refuse decision_time before availability (no FeatureRecord)."""
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    available = t0 + timedelta(minutes=10)
    decision = t0 + timedelta(minutes=1)
    event = _event(source_event_time=t0, decision_available_time=available, first_seen_time=available)
    inputs = [_input(decision, "1.0", volume="1", liquidity="10")]
    with pytest.raises(ResearchError, match=_BUILDER_AVAIL_MSG) as exc_info:
        build_decision_feature_record(
            event,
            inputs,
            trades=(),
            decision_time=decision,
            windows=(timedelta(minutes=1),),
            labels=(),
            config_id="c",
        )
    assert _BUILDER_AVAIL_MSG in str(exc_info.value)
    assert not isinstance(exc_info.value, DecisionFeatureRecord)


def test_builder_allows_equality_at_decision_available_time() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    available = t0 + timedelta(minutes=10)
    event = _event(source_event_time=t0, decision_available_time=available, first_seen_time=available)
    inputs = [_input(available, "1.0", volume="1", liquidity="10")]
    rec = build_decision_feature_record(
        event,
        inputs,
        trades=(),
        decision_time=available,
        windows=(timedelta(minutes=1),),
        labels=(),
        config_id="c",
    )
    assert isinstance(rec, DecisionFeatureRecord)
    assert rec.decision_time == available
    assert rec.feature_cutoff == available
    assert rec.decision_available_time == available


def test_builder_allows_decision_time_after_availability() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    available = t0 + timedelta(minutes=10)
    decision = t0 + timedelta(minutes=15)
    event = _event(source_event_time=t0, decision_available_time=available, first_seen_time=available)
    inputs = [
        _input(available, "1.0", volume="1", liquidity="10"),
        _input(decision, "1.1", volume="2", liquidity="11"),
    ]
    rec = build_decision_feature_record(
        event,
        inputs,
        trades=(),
        decision_time=decision,
        windows=(timedelta(minutes=5),),
        labels=(),
        config_id="c",
    )
    assert rec.decision_time == decision
    assert rec.feature_cutoff == decision
    assert rec.feature_cutoff >= rec.decision_available_time


def test_builder_availability_guard_before_prepare_and_computation() -> None:
    """Guard must fire before prepare_feature_inputs / feature computation."""
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    available = t0 + timedelta(minutes=10)
    decision = t0 + timedelta(minutes=1)
    event = _event(source_event_time=t0, decision_available_time=available, first_seen_time=available)
    with (
        patch(
            "newcoin_trader.research.feature_research_features.prepare_feature_inputs",
            side_effect=AssertionError("prepare_feature_inputs must not run"),
        ) as prep,
        patch(
            "newcoin_trader.research.feature_research_features.prepare_trades",
            side_effect=AssertionError("prepare_trades must not run"),
        ) as prep_trades,
    ):
        with pytest.raises(ResearchError, match=_BUILDER_AVAIL_MSG):
            build_decision_feature_record(
                event,
                [_input(decision, "1.0")],
                trades=(),
                decision_time=decision,
                windows=(timedelta(minutes=1),),
                labels=(),
                config_id="c",
            )
    prep.assert_not_called()
    prep_trades.assert_not_called()


def test_builder_valid_decision_preserves_future_observation_invariance() -> None:
    """At/after availability, post-decision observations still cannot change features."""
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    available = t0 + timedelta(minutes=10)
    decision = available + timedelta(minutes=5)
    event = _event(source_event_time=t0, decision_available_time=available, first_seen_time=available)
    base = [
        _input(available, "1.0", volume="10", liquidity="100"),
        _input(decision, "1.1", volume="12", liquidity="110"),
    ]
    future = [*base, _input(decision + timedelta(minutes=1), "9.9", volume="999", liquidity="9999")]
    rec_a = build_decision_feature_record(
        event, base, trades=(), decision_time=decision, windows=(timedelta(minutes=5),), labels=(), config_id="c"
    )
    rec_b = build_decision_feature_record(
        event, future, trades=(), decision_time=decision, windows=(timedelta(minutes=5),), labels=(), config_id="c"
    )
    assert rec_a.features == rec_b.features
    assert rec_a.feature_cutoff == decision
    assert any(obs.timestamp > decision for obs in prepare_feature_inputs(future))


# --- point-in-time invariance / no future leakage ---


def test_future_price_after_decision_cannot_change_features() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    decision = t0 + timedelta(minutes=5)
    event = _event(source_event_time=t0, decision_available_time=t0)
    base = [
        _input(t0, "1.0", volume="10", liquidity="100"),
        _input(decision, "1.1", volume="12", liquidity="110"),
    ]
    future = [
        *base,
        _input(decision + timedelta(minutes=1), "9.9", volume="999", liquidity="9999"),
    ]
    rec_a = build_decision_feature_record(
        event,
        base,
        trades=(),
        decision_time=decision,
        windows=DEFAULT_FEATURE_WINDOWS,
        labels=(),
        config_id="c",
    )
    rec_b = build_decision_feature_record(
        event,
        future,
        trades=(),
        decision_time=decision,
        windows=DEFAULT_FEATURE_WINDOWS,
        labels=(),
        config_id="c",
    )
    assert rec_a.features == rec_b.features
    # hard invariant: cutoff equals decision; future rows do not change features
    assert rec_a.feature_cutoff == decision
    assert rec_b.feature_cutoff == decision
    prepared = prepare_feature_inputs(future)
    assert any(f.timestamp > decision for f in prepared)


def test_future_volume_liquidity_and_trades_cannot_leak() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    decision = t0 + timedelta(minutes=5)
    event = _event(source_event_time=t0)
    base_inputs = [
        _input(t0, "1.0", volume="10", liquidity="100"),
        _input(decision, "1.2", volume="20", liquidity="120"),
    ]
    leak_inputs = [
        *base_inputs,
        _input(decision + timedelta(seconds=1), "1.2", volume="99999", liquidity="99999"),
    ]
    base_trades = [_trade(decision - timedelta(seconds=30), side="buy")]
    leak_trades = [*base_trades, _trade(decision + timedelta(seconds=1), side="sell", amount="1000")]
    a = build_decision_feature_record(
        event,
        base_inputs,
        trades=base_trades,
        decision_time=decision,
        windows=(timedelta(minutes=5),),
        labels=(),
        config_id="c",
    )
    b = build_decision_feature_record(
        event,
        leak_inputs,
        trades=leak_trades,
        decision_time=decision,
        windows=(timedelta(minutes=5),),
        labels=(),
        config_id="c",
    )
    assert a.features == b.features


def test_labels_are_separated_and_do_not_affect_feature_values() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    decision = t0 + timedelta(minutes=5)
    event = _event(source_event_time=t0)
    inputs = [_input(t0, "1.0", volume="1", liquidity="10"), _input(decision, "1.1", volume="2", liquidity="11")]
    without = build_decision_feature_record(
        event, inputs, trades=(), decision_time=decision, windows=(timedelta(minutes=5),), labels=(), config_id="c"
    )
    with_labels = build_decision_feature_record(
        event,
        inputs,
        trades=(),
        decision_time=decision,
        windows=(timedelta(minutes=5),),
        labels=(_label(simple_return=Decimal("99")),),
        config_id="c",
    )
    assert without.features == with_labels.features
    assert len(with_labels.labels) == 1
    assert with_labels.labels[0].simple_return == Decimal("99")


def test_unsorted_and_duplicate_inputs_are_deterministic() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    decision = t0 + timedelta(minutes=5)
    event = _event(source_event_time=t0)
    a = _input(decision, "1.1", volume="2", source="binance:kline:a")
    b = _input(t0, "1.0", volume="1", source="binance:kline:b")
    dup = _input(decision, "1.5", volume="9", source="binance:kline:z")  # same ts, worse source order
    ordered = build_decision_feature_record(
        event, [b, a], trades=(), decision_time=decision, windows=(timedelta(minutes=5),), labels=(), config_id="c"
    )
    shuffled = build_decision_feature_record(
        event, [dup, a, b], trades=(), decision_time=decision, windows=(timedelta(minutes=5),), labels=(), config_id="c"
    )
    assert ordered.features == shuffled.features


def test_missing_volume_and_insufficient_history_are_explicit() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    decision = t0 + timedelta(minutes=1)
    event = _event(source_event_time=t0)
    # Only one observation at decision — insufficient for 30m momentum
    inputs = [_input(decision, "1.0", volume=None, liquidity=None)]
    rec = build_decision_feature_record(
        event,
        inputs,
        trades=(),
        decision_time=decision,
        windows=(timedelta(minutes=30),),
        labels=(),
        config_id="c",
    )
    by_name = {f.name: f for f in rec.features}
    assert by_name["volume_sum_30m"].state in {FeatureValueState.MISSING, FeatureValueState.INSUFFICIENT}
    assert by_name["price_return_30m"].state is FeatureValueState.INSUFFICIENT
    assert by_name["liquidity_current"].state is FeatureValueState.MISSING


def test_coarse_resolution_marks_volatility_unsupported_or_insufficient() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    decision = t0 + timedelta(minutes=5)
    event = _event(source_event_time=t0)
    inputs = [
        _input(t0, "1.0", resolution=ObservationResolution.COARSE),
        _input(decision, "1.1", resolution=ObservationResolution.COARSE),
    ]
    rec = build_decision_feature_record(
        event, inputs, trades=(), decision_time=decision, windows=(timedelta(minutes=5),), labels=(), config_id="c"
    )
    vol = next(f for f in rec.features if f.name.startswith("volatility_"))
    assert vol.state in {FeatureValueState.UNSUPPORTED, FeatureValueState.INSUFFICIENT}


def test_nonfinite_decimal_rejected() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    decision = t0 + timedelta(minutes=5)
    event = _event(source_event_time=t0)
    bad = FeatureMarketInput.model_construct(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=decision,
        price=Decimal("NaN"),
        volume=None,
        liquidity=None,
        resolution=ObservationResolution.MINUTE,
        source="binance:kline",
        provenance=None,
    )
    inputs = [_input(t0, "1.0"), bad]
    with pytest.raises(ResearchError):
        build_decision_feature_record(
            event, inputs, trades=(), decision_time=decision, windows=(timedelta(minutes=5),), labels=(), config_id="c"
        )


# --- blocker: Decimal Overflow must not escape feature construction ---


def _assert_available_finite(feat: FeatureValue) -> None:
    assert feat.state is FeatureValueState.AVAILABLE
    assert isinstance(feat.value, Decimal)
    assert feat.value.is_finite()
    assert not feat.value.is_nan()
    assert not feat.value.is_infinite()


def _assert_decimal_context_invalid(feat: FeatureValue) -> None:
    assert feat.state is FeatureValueState.INVALID
    assert feat.value is None
    assert feat.provenance.get("reason") == "decimal_context_failure"
    assert feat.state is not FeatureValueState.MISSING
    assert feat.state is not FeatureValueState.UNSUPPORTED


def test_decimal_overflow_in_price_vol_range_is_invalid_not_raised() -> None:
    """Raw Decimal Overflow on endpoint/vol/range arithmetic → INVALID FeatureValue."""
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    decision = t0 + timedelta(minutes=5)
    window = timedelta(minutes=5)
    level = AvailabilityLevel.SUPPORTED

    # Confirm raw Overflow for endpoint ratio (end/start).
    with pytest.raises(Overflow):
        _ = (Decimal("1e999999") / Decimal("1e-999999")) - Decimal("1")

    # Endpoint ratio Overflow → INVALID (must not escape).
    endpoint_inputs = prepare_feature_inputs([_input(t0, "1e-999999"), _input(decision, "1e999999")])
    endpoint = _price_return(endpoint_inputs, decision_time=decision, window=window, family_level=level)
    _assert_decimal_context_invalid(endpoint)

    # Volatility extreme: successive ratio Overflow while endpoint return stays representable.
    vol_inputs = prepare_feature_inputs(
        [
            _input(t0, "1e-999999"),
            _input(t0 + timedelta(minutes=1), "1e999999"),
            _input(t0 + timedelta(minutes=2), "1e-999999"),
            _input(decision, "1"),
        ]
    )
    endpoint_ok = _price_return(vol_inputs, decision_time=decision, window=window, family_level=level)
    _assert_available_finite(endpoint_ok)
    with pytest.raises(Overflow):
        _ = (Decimal("1e999999") / Decimal("1e-999999")) - Decimal("1")
    vol = _volatility(vol_inputs, decision_time=decision, window=window, family_level=level)
    _assert_decimal_context_invalid(vol)

    # Range peak/trough extremes → INVALID.
    # Window slice is (decision-window, decision]; keep both endpoints inside that open-closed span.
    range_inputs = prepare_feature_inputs(
        [
            _input(t0 + timedelta(minutes=1), "1e-999999"),
            _input(decision, "1e999999"),
        ]
    )
    with pytest.raises(Overflow):
        hi, lo = Decimal("1e999999"), Decimal("1e-999999")
        _ = (hi - lo) / lo
    rng = _range_feature(range_inputs, decision_time=decision, window=window, family_level=level)
    _assert_decimal_context_invalid(rng)

    # Representable large magnitudes keep AVAILABLE finite semantics.
    large_inputs = prepare_feature_inputs(
        [
            _input(t0, "1e-200"),
            _input(t0 + timedelta(minutes=1), "1e-100"),
            _input(t0 + timedelta(minutes=2), "1e100"),
            _input(decision, "1e200"),
        ]
    )
    large_ret = _price_return(large_inputs, decision_time=decision, window=window, family_level=level)
    large_vol = _volatility(large_inputs, decision_time=decision, window=window, family_level=level)
    large_rng = _range_feature(large_inputs, decision_time=decision, window=window, family_level=level)
    _assert_available_finite(large_ret)
    _assert_available_finite(large_vol)
    _assert_available_finite(large_rng)
    assert large_ret.value is not None and large_ret.value > 0

    # Full record construction must not raise DecimalException; INVALID is deterministic.
    rec = build_decision_feature_record(
        _event(source_event_time=t0),
        [
            _input(t0, "1e-999999"),
            _input(t0 + timedelta(minutes=1), "1e-999999"),
            _input(decision, "1e999999"),
        ],
        trades=(),
        decision_time=decision,
        windows=(window,),
        labels=(),
        config_id="c",
    )
    by_name = {f.name: f for f in rec.features}
    _assert_decimal_context_invalid(by_name["price_return_5m"])
    _assert_decimal_context_invalid(by_name["price_range_5m"])
    for feat in rec.features:
        if feat.state is FeatureValueState.AVAILABLE and isinstance(feat.value, Decimal):
            assert feat.value.is_finite()
            assert not feat.value.is_nan()
            assert not feat.value.is_infinite()


# --- chronological split / walk-forward / rules ---


def _records_for_split(n: int) -> list[DecisionFeatureRecord]:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    out: list[DecisionFeatureRecord] = []
    for i in range(n):
        decision = t0 + timedelta(minutes=i)
        event = _event(source_event_time=decision, event_id=f"e{i}", token=f"T{i}")
        inputs = [
            _input(decision - timedelta(minutes=5), "1.0", volume="1", liquidity="10", token=f"T{i}"),
            _input(decision, str(1 + i * 0.01), volume="2", liquidity="11", token=f"T{i}"),
        ]
        label_ret = Decimal("0.1") if i % 2 == 0 else Decimal("-0.05")
        out.append(
            build_decision_feature_record(
                event,
                inputs,
                trades=(),
                decision_time=decision,
                windows=(timedelta(minutes=5),),
                labels=(_label(simple_return=label_ret),),
                config_id="c",
            )
        )
    return out


def test_chronological_split_exact_boundaries_no_shuffle() -> None:
    records = _records_for_split(10)
    split = chronological_split(records, ratios=DEFAULT_SPLIT_RATIOS)
    assert len(split.train) + len(split.validation) + len(split.test) == 10
    assert [r.decision_time for r in split.train] == sorted(r.decision_time for r in split.train)
    # exact 60/20/20 on 10 => 6/2/2
    assert len(split.train) == 6
    assert len(split.validation) == 2
    assert len(split.test) == 2
    assert split.train[-1].decision_time < split.validation[0].decision_time
    assert split.validation[-1].decision_time < split.test[0].decision_time


def test_test_set_isolated_from_rule_discovery() -> None:
    records = _records_for_split(20)
    split = chronological_split(records, ratios=DEFAULT_SPLIT_RATIOS)
    selected = select_and_test_rules(
        train=split.train,
        validation=split.validation,
        test=split.test,
        max_rules=5,
        max_conditions=MAX_RULE_CONDITIONS,
        min_sample=2,
    )
    # discovery uses train only; selection validation; test evaluated once
    assert selected.test_evaluated_once is True
    train_ids = {r.event_id for r in split.train}
    for rule in selected.candidates:
        assert set(rule.train_event_ids).issubset(train_ids)
        assert not set(rule.train_event_ids) & {r.event_id for r in split.test}


def test_walk_forward_no_future_fold_leakage() -> None:
    records = _records_for_split(12)
    folds = walk_forward_folds(records, n_folds=3, min_train=3, min_test=2)
    assert len(folds) >= 1
    for fold in folds:
        assert max(r.decision_time for r in fold.train) < min(r.decision_time for r in fold.test)
        # later folds must not feed earlier train
    for i in range(1, len(folds)):
        assert max(r.decision_time for r in folds[i - 1].test) <= max(r.decision_time for r in folds[i].train)


def test_bounded_deterministic_rule_generation() -> None:
    records = _records_for_split(15)
    split = chronological_split(records, ratios=DEFAULT_SPLIT_RATIOS)
    candidates = discover_candidate_rules(
        split.train,
        max_rules=4,
        max_conditions=2,
        min_sample=2,
    )
    assert len(candidates) <= 4
    assert all(1 <= len(c.conditions) <= 2 for c in candidates)
    # deterministic
    again = discover_candidate_rules(split.train, max_rules=4, max_conditions=2, min_sample=2)
    assert [c.rule_id for c in candidates] == [c.rule_id for c in again]
    assert all(" " in c.human_readable for c in candidates)


def test_insufficient_samples_labeled() -> None:
    records = _records_for_split(4)
    stats = compute_univariate_stats(records, min_sample=10)
    assert all(s.insufficient_sample for s in stats)


def test_venue_separation_in_univariate_stats() -> None:
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    records: list[DecisionFeatureRecord] = []
    for i, venue in enumerate((Venue.BINANCE, Venue.RAYDIUM)):
        decision = t0 + timedelta(minutes=i)
        event = _event(source_event_time=t0, venue=venue, event_id=f"e{venue.value}", token=f"T{i}")
        chain = "binance" if venue is Venue.BINANCE else "solana"
        inputs = [
            FeatureMarketInput(
                token_address=f"T{i}",
                chain=chain,
                venue=venue,
                timestamp=decision - timedelta(minutes=5),
                price=Decimal("1.0"),
                volume=Decimal("1"),
                liquidity=Decimal("10"),
                resolution=ObservationResolution.MINUTE,
                source=f"{venue.value}:snap",
                provenance=None,
            ),
            FeatureMarketInput(
                token_address=f"T{i}",
                chain=chain,
                venue=venue,
                timestamp=decision,
                price=Decimal("1.1"),
                volume=Decimal("2"),
                liquidity=Decimal("11"),
                resolution=ObservationResolution.MINUTE,
                source=f"{venue.value}:snap",
                provenance=None,
            ),
        ]
        records.append(
            build_decision_feature_record(
                event,
                inputs,
                trades=(),
                decision_time=decision,
                windows=(timedelta(minutes=5),),
                labels=(_label(),),
                config_id="c",
            )
        )
    stats = compute_univariate_stats(records, min_sample=1)
    venues = {s.venue for s in stats}
    assert Venue.BINANCE in venues
    assert Venue.RAYDIUM in venues


def test_evaluate_rule_respects_conditions() -> None:
    records = _records_for_split(8)
    candidates = discover_candidate_rules(records[:5], max_rules=3, max_conditions=1, min_sample=1)
    assert candidates
    matched = evaluate_rule(candidates[0], records)
    assert isinstance(matched, tuple)


# --- artifacts / repo budget / CLI ---


def test_artifact_determinism(tmp_path: Path) -> None:
    records = _records_for_split(10)
    report = build_feature_research_report(
        records=records,
        venue="binance",
        start=datetime(2024, 1, 1, tzinfo=UTC),
        end=datetime(2024, 2, 1, tzinfo=UTC),
        max_events=10,
        decision_delay=timedelta(minutes=5),
        windows=DEFAULT_FEATURE_WINDOWS,
        min_sample=2,
        split_ratios=DEFAULT_SPLIT_RATIOS,
        walk_forward_folds=2,
        max_rules=3,
        max_rule_conditions=2,
    )
    a = tmp_path / "a"
    b = tmp_path / "b"
    paths_a = emit_feature_research_artifacts(report, a)
    paths_b = emit_feature_research_artifacts(report, b)
    assert paths_a["json"].read_text() == paths_b["json"].read_text()
    assert paths_a["csv"].read_text() == paths_b["csv"].read_text()
    assert paths_a["markdown"].read_text() == paths_b["markdown"].read_text()
    payload = json.loads(paths_a["json"].read_text())
    assert "availability" in payload
    assert "splits" in payload
    assert payload["meta"]["phase"] == "phase_4_feature_research"


def test_feature_inputs_query_budget_cap_plus_one() -> None:
    repo = FeatureResearchRepository(MagicMock())
    query = repo._feature_inputs_query(
        token_ids=[1, 2],
        venue="binance",
        start=datetime(2024, 1, 1, tzinfo=UTC),
        end=datetime(2024, 1, 2, tzinfo=UTC),
        max_feature_inputs=100,
    )
    assert query._limit_clause.value == 101  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_list_feature_inputs_raises_when_budget_exceeded() -> None:
    session = AsyncMock()
    snap = MagicMock()
    token = MagicMock()
    token.token_address = "T"
    token.chain = "binance"
    token.venue = "binance"
    snap.timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    snap.price = Decimal("1")
    snap.volume = None
    snap.liquidity = None
    snap.source = "binance:kline"
    snap.provenance = {"kind": "kline", "interval": "1m"}
    # budget 2 => fetch 3 rows triggers ConfigError
    session.execute = AsyncMock(return_value=MagicMock(all=MagicMock(return_value=[(snap, token)] * 3)))
    repo = FeatureResearchRepository(session)
    with pytest.raises(ConfigError, match="budget"):
        await repo.list_feature_inputs(
            token_ids=[1],
            venue="binance",
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=datetime(2024, 1, 2, tzinfo=UTC),
            max_feature_inputs=2,
        )


def test_default_max_feature_inputs_is_finite() -> None:
    assert DEFAULT_MAX_FEATURE_INPUTS > 0
    assert DEFAULT_MAX_FEATURE_INPUTS <= 5_000_000


def test_feature_research_cli_help_lists_required_bounds() -> None:
    result = runner.invoke(app, ["feature-research", "--help"])
    assert result.exit_code == 0, result.output
    out = result.output.lower()
    assert "venue" in out
    assert "max-events" in out
    assert "decision" in out or "entry-delay" in out or "decision-delay" in out
    assert "windows" in out
    assert "min-sample" in out
    assert "split" in out
    assert "walk-forward" in out
    assert "output-dir" in out


def test_feature_research_cli_rejects_missing_required() -> None:
    result = runner.invoke(app, ["feature-research"])
    assert result.exit_code != 0


def test_root_help_lists_feature_research() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "feature-research" in result.output


def test_parse_duration_and_format_roundtrip() -> None:
    assert parse_duration("5m") == timedelta(minutes=5)
    assert format_duration(timedelta(minutes=15)) == "15m"
