"""Phase 6.6 failed-exit observability: structured audits, reasons, artifacts."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from newcoin_trader.database.repositories.live_paper import LivePaperRepository
from newcoin_trader.domain.enums import Chain, Side, Venue
from newcoin_trader.domain.event_study import ObservationResolution, TokenListingEvent
from newcoin_trader.domain.executable_backtest import (
    DepthLevel,
    FrozenCandidateIdentity,
    HistoricalDepthBook,
)
from newcoin_trader.domain.feature_research import RuleCondition
from newcoin_trader.domain.live_paper import (
    FailedExitReason,
    PositionLifecycle,
    ReplayMarketEvent,
)
from newcoin_trader.research.live_paper_config import (
    DEFAULT_FUTURE_TOLERANCE,
    MAX_EXIT_ATTEMPT_AUDITS,
)
from newcoin_trader.research.live_paper_engine import process_live_paper_session
from newcoin_trader.research.live_paper_run import emit_live_paper_artifacts

T0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
DECISION = T0 + timedelta(minutes=1)
EXIT_DEADLINE = DECISION + timedelta(minutes=5)


def _identity(*, rule_id: str = "frozen-rule-1") -> FrozenCandidateIdentity:
    cond = RuleCondition(feature_name="age_source_event_seconds", op="gte", threshold=Decimal("0"))
    return FrozenCandidateIdentity(
        rule_id=rule_id,
        conditions=(cond,),
        human_readable="age_source_event_seconds gte 0",
        phase4_config_id="cfg-phase4",
        split_label="test",
        fold_index=0,
        provenance={"source": "frozen_phase4"},
    )


def _listing() -> TokenListingEvent:
    return TokenListingEvent(
        event_id="e1",
        venue=Venue.BINANCE,
        chain=Chain.BINANCE,
        token_address="TOKEN",
        pair_address="PAIR",
        symbol="TOK",
        source="binance",
        source_event_time=T0,
        first_seen_time=T0,
        first_market_data_time=T0,
        decision_available_time=T0,
        provenance={"token_id": "1"},
    )


def _listing_event() -> ReplayMarketEvent:
    listing = _listing()
    return ReplayMarketEvent(
        event_id=listing.event_id,
        kind="listing",
        venue=listing.venue,
        token_address=listing.token_address,
        chain=listing.chain.value,
        source_timestamp=listing.source_event_time,
        received_timestamp=listing.source_event_time,
        source=listing.source,
        listing=listing,
        provenance=dict(listing.provenance),
    )


def _market(
    *,
    ts: datetime,
    price: str | None = "10",
    liquidity: str = "100000",
    received: datetime | None = None,
    source: str = "binance:trade",
    depth: HistoricalDepthBook | None = None,
) -> ReplayMarketEvent:
    return ReplayMarketEvent(
        event_id="e1",
        kind="market",
        venue=Venue.BINANCE,
        token_address="TOKEN",
        chain="binance",
        source_timestamp=ts,
        received_timestamp=received or ts,
        price=None if price is None else Decimal(price),
        liquidity=Decimal(liquidity),
        volume=Decimal("1000"),
        resolution=ObservationResolution.POINT,
        source=source,
        depth=depth,
        provenance={"kind": "trade"},
    )


def _entry_market() -> ReplayMarketEvent:
    return _market(ts=DECISION, price="10", liquidity="100000")


def _default_kwargs(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "venue": Venue.BINANCE,
        "session_start": T0,
        "duration": timedelta(hours=1),
        "max_events": 50,
        "max_signals": 20,
        "max_trades": 20,
        "queue_capacity": 100,
        "starting_cash": Decimal("10000"),
        "position_notional": Decimal("100"),
        "holding_period": timedelta(minutes=5),
        "max_open_positions": 3,
        "max_token_exposure": Decimal("5000"),
        "max_venue_exposure": Decimal("2000"),
        "min_liquidity": Decimal("1000"),
        "max_participation": Decimal("0.10"),
        "max_impact_slippage": Decimal("0.05"),
        "daily_loss_limit": Decimal("500"),
        "session_loss_limit": Decimal("500"),
        "freshness_max_age": timedelta(minutes=5),
        "future_tolerance": DEFAULT_FUTURE_TOLERANCE,
        "assumed_fee_bps": Decimal("10"),
        "decision_delay": timedelta(minutes=1),
        "identity": _identity(),
    }
    base.update(overrides)
    return base


def _exit_book(
    *,
    ts: datetime,
    bid_qty: Decimal | None,
    provenance: dict[str, str] | None = None,
) -> HistoricalDepthBook:
    bids: tuple[DepthLevel, ...]
    if bid_qty is None:
        bids = ()
    else:
        bids = (DepthLevel(price=Decimal("11"), quantity=bid_qty),)
    return HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=ts,
        bids=bids,
        asks=(DepthLevel(price=Decimal("11.1"), quantity=Decimal("100")),),
        source="binance:depth",
        provenance=provenance,
    )


def _entry_book() -> HistoricalDepthBook:
    return HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=DECISION,
        bids=(DepthLevel(price=Decimal("9.9"), quantity=Decimal("100")),),
        asks=(DepthLevel(price=Decimal("10"), quantity=Decimal("10")),),
        source="binance:depth",
    )


def _failed(report: object) -> object:
    positions = report.positions  # type: ignore[attr-defined]
    failed = [p for p in positions if p.lifecycle is PositionLifecycle.FAILED_EXIT]
    assert failed
    return failed[-1]


def _diag(position: object) -> object:
    diagnostics = position.exit_diagnostics  # type: ignore[attr-defined]
    assert diagnostics is not None
    return diagnostics


def test_failed_exit_no_candidate_reason() -> None:
    report = process_live_paper_session(
        events=[_listing_event(), _entry_market()],
        **_default_kwargs(),
    )
    pos = _failed(report)
    diag = _diag(pos)
    assert pos.lifecycle is PositionLifecycle.FAILED_EXIT
    assert diag.failed_exit_reason is FailedExitReason.NO_USABLE_EXIT_CANDIDATE
    assert diag.exit_deadline == EXIT_DEADLINE
    assert diag.attempt_count_total == 0
    assert diag.attempt_count_retained == 0
    assert diag.truncated is False
    assert diag.attempts == ()
    assert diag.last_candidate_clock is None
    assert diag.last_reject_or_nofill_reason is None
    assert all(a.source_timestamp >= EXIT_DEADLINE for a in diag.attempts)


def test_failed_exit_future_received_reason() -> None:
    exit_ts = EXIT_DEADLINE
    report = process_live_paper_session(
        events=[
            _listing_event(),
            _entry_market(),
            _market(ts=exit_ts, price="11", received=exit_ts + timedelta(seconds=1)),
        ],
        **_default_kwargs(),
    )
    pos = _failed(report)
    diag = _diag(pos)
    assert diag.failed_exit_reason is FailedExitReason.ALL_EXIT_CANDIDATES_REJECTED
    assert len(diag.attempts) == 1
    audit = diag.attempts[0]
    assert audit.event_id == "e1"
    assert audit.source_timestamp == exit_ts
    assert audit.received_timestamp == exit_ts + timedelta(seconds=1)
    assert audit.price == Decimal("11")
    assert audit.requested_qty > 0
    assert audit.market_usable is False
    assert audit.market_reject_reason == "future_received"
    assert audit.execution_mode == "none"
    assert audit.attempted is False
    assert audit.fill_qty is None
    assert audit.fill_price is None
    assert audit.no_fill_reason is None
    assert audit.outcome == "rejected"
    assert diag.last_reject_or_nofill_reason == "future_received"


def test_failed_exit_stale_reason() -> None:
    exit_ts = EXIT_DEADLINE
    stale_depth = _exit_book(
        ts=exit_ts,
        bid_qty=Decimal("100"),
        provenance={
            "depth_source_timestamp": T0.isoformat(),
            "depth_received_timestamp": exit_ts.isoformat(),
        },
    )
    report = process_live_paper_session(
        events=[
            _listing_event(),
            _entry_market(),
            _market(ts=exit_ts, price="11", liquidity="0", depth=stale_depth),
        ],
        **_default_kwargs(),
    )
    pos = _failed(report)
    diag = _diag(pos)
    audit = diag.attempts[0]
    assert audit.market_usable is True
    assert audit.market_reject_reason is None
    assert audit.depth_available is True
    assert audit.depth_source_timestamp == T0
    assert audit.depth_received_timestamp == exit_ts
    assert audit.depth_pit_accepted is False
    assert audit.depth_pit_reason == "stale_source"
    assert audit.execution_mode == "modeled"
    assert audit.attempted is True
    assert audit.outcome == "unfilled"
    assert "stale" in (audit.depth_pit_reason or "")


def test_failed_exit_missing_price_reason() -> None:
    exit_ts = EXIT_DEADLINE
    report = process_live_paper_session(
        events=[
            _listing_event(),
            _entry_market(),
            _market(ts=exit_ts, price=None, source="binance:trade:noprice"),
        ],
        **_default_kwargs(),
    )
    pos = _failed(report)
    diag = _diag(pos)
    assert diag.failed_exit_reason is FailedExitReason.ALL_EXIT_CANDIDATES_REJECTED
    audit = diag.attempts[0]
    assert audit.price is None
    assert audit.market_usable is False
    assert audit.market_reject_reason == "missing_price"
    assert audit.execution_mode == "none"
    assert audit.attempted is False
    assert audit.outcome == "rejected"
    assert audit.fill_qty is None
    assert audit.fill_price is None


def test_failed_exit_depth_pit_rejected() -> None:
    exit_ts = EXIT_DEADLINE
    before_session = (T0 - timedelta(minutes=1)).isoformat()
    pit_book = _exit_book(
        ts=exit_ts,
        bid_qty=Decimal("100"),
        provenance={
            "depth_source_timestamp": before_session,
            "depth_received_timestamp": before_session,
        },
    )
    report = process_live_paper_session(
        events=[
            _listing_event(),
            _entry_market(),
            _market(ts=exit_ts, price="11", liquidity="0", depth=pit_book),
        ],
        **_default_kwargs(),
    )
    pos = _failed(report)
    diag = _diag(pos)
    audit = diag.attempts[0]
    assert audit.market_usable is True
    assert audit.depth_available is True
    assert audit.depth_source_timestamp == T0 - timedelta(minutes=1)
    assert audit.depth_received_timestamp == T0 - timedelta(minutes=1)
    assert audit.depth_pit_accepted is False
    assert audit.depth_pit_reason == "before_session"
    assert audit.execution_mode == "modeled"
    assert audit.attempted is True
    assert audit.outcome == "unfilled"
    assert audit.no_fill_reason == "insufficient_liquidity"


def test_failed_exit_exact_depth_zero_no_fill() -> None:
    exit_ts = EXIT_DEADLINE
    empty_bids = _exit_book(ts=exit_ts, bid_qty=None)
    report = process_live_paper_session(
        events=[
            _listing_event(),
            _entry_market(),
            _market(ts=exit_ts, price="11", depth=empty_bids),
        ],
        **_default_kwargs(),
    )
    pos = _failed(report)
    diag = _diag(pos)
    assert diag.failed_exit_reason is FailedExitReason.ALL_EXIT_EXECUTION_ATTEMPTS_UNFILLED
    audit = diag.attempts[0]
    assert audit.market_usable is True
    assert audit.depth_available is True
    assert audit.depth_pit_accepted is True
    assert audit.depth_pit_reason is None
    assert audit.execution_mode == "exact_depth"
    assert audit.attempted is True
    assert audit.fill_qty == Decimal("0")
    assert audit.fill_price == Decimal("0")
    assert audit.no_fill_reason == "unfilled"
    assert audit.outcome == "unfilled"


def test_failed_exit_modeled_no_fill() -> None:
    exit_ts = EXIT_DEADLINE
    report = process_live_paper_session(
        events=[
            _listing_event(),
            _entry_market(),
            _market(ts=exit_ts, price="11", liquidity="0"),
        ],
        **_default_kwargs(),
    )
    pos = _failed(report)
    diag = _diag(pos)
    assert diag.failed_exit_reason is FailedExitReason.ALL_EXIT_EXECUTION_ATTEMPTS_UNFILLED
    audit = diag.attempts[0]
    assert audit.depth_available is False
    assert audit.depth_source_timestamp is None
    assert audit.depth_received_timestamp is None
    assert audit.depth_pit_accepted is None
    assert audit.execution_mode == "modeled"
    assert audit.attempted is True
    assert audit.fill_qty == Decimal("0")
    assert audit.fill_price == Decimal("11")
    assert audit.no_fill_reason == "insufficient_liquidity"
    assert audit.outcome == "unfilled"


def test_successful_exit_has_no_failure_reason() -> None:
    exit_ts = EXIT_DEADLINE
    report = process_live_paper_session(
        events=[
            _listing_event(),
            _entry_market(),
            _market(ts=exit_ts, price="12", liquidity="100000"),
        ],
        **_default_kwargs(),
    )
    assert report.positions
    pos = report.positions[-1]
    assert pos.lifecycle is PositionLifecycle.CLOSED
    assert pos.exit_diagnostics is None or pos.exit_diagnostics.failed_exit_reason is None
    if pos.exit_diagnostics is not None:
        assert pos.exit_diagnostics.failed_exit_reason is None
    sells = [f for f in report.fills if f.side is Side.SELL]
    assert sells
    assert sells[0].fill_qty > 0


def test_partial_then_failed_remainder_has_reason() -> None:
    exit1 = EXIT_DEADLINE
    exit2 = EXIT_DEADLINE + timedelta(minutes=1)
    thin = _exit_book(ts=exit1, bid_qty=Decimal("5"))
    empty = _exit_book(ts=exit2, bid_qty=None)
    report = process_live_paper_session(
        events=[
            _listing_event(),
            _market(ts=DECISION, price="10", liquidity="100000", depth=_entry_book()),
            _market(ts=exit1, price="11", liquidity="100000", depth=thin),
            _market(ts=exit2, price="11", liquidity="100000", depth=empty),
        ],
        **_default_kwargs(position_notional=Decimal("100"), max_token_exposure=Decimal("5000")),
    )
    assert report.positions
    pos = report.positions[-1]
    assert pos.lifecycle is PositionLifecycle.CLOSING
    assert pos.remaining_qty is not None and pos.remaining_qty > 0
    diag = _diag(pos)
    assert diag.failed_exit_reason is FailedExitReason.MIXED_EXIT_FAILURES
    outcomes = [a.outcome for a in diag.attempts]
    assert "partial" in outcomes or "filled" in outcomes
    assert "unfilled" in outcomes


def test_duplicate_exit_candidate_audited_not_applied() -> None:
    exit1 = EXIT_DEADLINE
    thin = _exit_book(ts=exit1, bid_qty=Decimal("5"))
    report = process_live_paper_session(
        events=[
            _listing_event(),
            _market(ts=DECISION, price="10", liquidity="100000", depth=_entry_book()),
            _market(ts=exit1, price="11", liquidity="100000", depth=thin),
            # Same market identity/source/timestamp as the first exit candidate:
            # its deterministic SELL fill id collides with seen_fills.
            _market(ts=exit1, price="11", liquidity="100000", depth=_exit_book(ts=exit1, bid_qty=Decimal("100"))),
        ],
        **_default_kwargs(position_notional=Decimal("100"), max_token_exposure=Decimal("5000")),
    )
    pos = report.positions[-1]
    # Duplicate candidate is observational only: no extra fill, no lifecycle change.
    assert pos.lifecycle is PositionLifecycle.CLOSING
    assert pos.remaining_qty == Decimal("5")
    sells = [f for f in report.fills if f.side is Side.SELL]
    assert len(sells) == 1
    # Exactly one applied sell fill and a positive (non-zero) single-fill realized PnL:
    # if the duplicate had been re-applied, realized PnL would have doubled.
    assert report.portfolio.realized_pnl > 0
    assert report.portfolio.realized_pnl < Decimal("10")

    diag = _diag(pos)
    assert diag.attempt_count_total == 2
    assert diag.attempt_count_retained == 2
    assert diag.truncated is False
    assert [a.outcome for a in diag.attempts] == ["partial", "duplicate_fill_identity"]
    dup = diag.attempts[1]
    assert dup.event_id == "e1"
    assert dup.source_timestamp == exit1
    assert dup.market_usable is True
    assert dup.attempted is True
    assert dup.no_fill_reason == "duplicate_fill_identity"
    assert dup.fill_qty is None
    assert dup.fill_price is None


def test_mixed_exit_failures_reason_deterministic() -> None:
    exit1 = EXIT_DEADLINE
    exit2 = EXIT_DEADLINE + timedelta(minutes=1)
    report = process_live_paper_session(
        events=[
            _listing_event(),
            _entry_market(),
            _market(ts=exit1, price=None, source="binance:trade:missing"),
            _market(ts=exit2, price="11", liquidity="0", source="binance:trade:unfilled"),
        ],
        **_default_kwargs(),
    )
    pos = _failed(report)
    diag = _diag(pos)
    assert diag.failed_exit_reason is FailedExitReason.MIXED_EXIT_FAILURES
    assert [a.outcome for a in diag.attempts] == ["rejected", "unfilled"]
    again = process_live_paper_session(
        events=[
            _listing_event(),
            _entry_market(),
            _market(ts=exit1, price=None, source="binance:trade:missing"),
            _market(ts=exit2, price="11", liquidity="0", source="binance:trade:unfilled"),
        ],
        **_default_kwargs(),
    )
    assert again.positions[-1].exit_diagnostics.failed_exit_reason is FailedExitReason.MIXED_EXIT_FAILURES
    assert again.positions[-1].exit_diagnostics.model_dump(mode="json") == diag.model_dump(mode="json")


def test_exit_attempt_audit_bounded_truncation() -> None:
    events = [_listing_event(), _entry_market()]
    clocks = [EXIT_DEADLINE + timedelta(seconds=i) for i in range(MAX_EXIT_ATTEMPT_AUDITS + 4)]
    for i, ts in enumerate(clocks):
        events.append(_market(ts=ts, price=None, source=f"binance:trade:miss-{i}"))
    report = process_live_paper_session(events=events, **_default_kwargs())
    pos = _failed(report)
    diag = _diag(pos)
    assert diag.attempt_count_total == len(clocks)
    assert diag.attempt_count_retained == MAX_EXIT_ATTEMPT_AUDITS
    assert diag.truncated is True
    assert len(diag.attempts) == MAX_EXIT_ATTEMPT_AUDITS
    assert diag.last_candidate_clock == clocks[-1]
    assert diag.last_reject_or_nofill_reason == "missing_price"
    assert all(a.source_timestamp >= EXIT_DEADLINE for a in diag.attempts)


def test_failed_exit_diagnostics_persist_reload_and_artifact_determinism(tmp_path: Path) -> None:
    events = [
        _listing_event(),
        _entry_market(),
        _market(ts=EXIT_DEADLINE, price=None, source="binance:trade:miss"),
    ]
    report = process_live_paper_session(events=events, **_default_kwargs())
    pos = _failed(report)
    diag = _diag(pos)
    meta = LivePaperRepository.position_meta_json(pos)
    assert meta["failed_exit_reason"] == FailedExitReason.ALL_EXIT_CANDIDATES_REJECTED.value
    assert meta["exit_deadline"] == EXIT_DEADLINE.isoformat()
    assert isinstance(meta["exit_attempt_audits"], list)
    assert len(meta["exit_attempt_audits"]) == 1
    assert meta["exit_attempt_count_total"] == 1
    assert meta["exit_attempt_count_retained"] == 1
    assert meta["exit_attempt_audits_truncated"] is False
    assert "remaining_qty" in meta
    assert "label" in meta

    paths_a = emit_live_paper_artifacts(report, tmp_path / "a")
    paths_b = emit_live_paper_artifacts(report, tmp_path / "b")
    assert paths_a["failed_exits_csv"].read_text() == paths_b["failed_exits_csv"].read_text()
    assert paths_a["markdown"].read_text() == paths_b["markdown"].read_text()
    assert paths_a["json"].read_text() == paths_b["json"].read_text()

    csv_text = paths_a["failed_exits_csv"].read_text(encoding="utf-8")
    header = csv_text.splitlines()[0]
    for col in (
        "position_id",
        "exit_deadline",
        "failed_exit_reason",
        "attempt_count",
        "last_candidate_clock",
        "last_reject_or_nofill_reason",
    ):
        assert col in header
    assert pos.position_id in csv_text
    assert FailedExitReason.ALL_EXIT_CANDIDATES_REJECTED.value in csv_text
    assert "missing_price" in csv_text

    md = paths_a["markdown"].read_text(encoding="utf-8")
    assert "Failed exits" in md
    assert FailedExitReason.ALL_EXIT_CANDIDATES_REJECTED.value in md
    assert pos.position_id in md

    payload = json.loads(paths_a["json"].read_text(encoding="utf-8"))
    loaded = payload["positions"][-1]["exit_diagnostics"]
    assert loaded["failed_exit_reason"] == diag.failed_exit_reason.value
    assert loaded["attempt_count_total"] == 1
    assert loaded["attempts"][0]["market_reject_reason"] == "missing_price"
    assert "datetime.now" not in paths_a["json"].read_text()
    assert "wall" not in csv_text.lower()
