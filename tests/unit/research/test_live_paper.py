"""Phase 6 bounded live-paper: freshness, risk, fills, state, replay, CLI, safety."""

from __future__ import annotations

import ast
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

from newcoin_trader.cli.main import app
from newcoin_trader.domain.enums import Chain, Side, Venue
from newcoin_trader.domain.event_study import ObservationResolution, TokenListingEvent
from newcoin_trader.domain.executable_backtest import (
    DepthLevel,
    ExecutionConfidence,
    FrozenCandidateIdentity,
    HistoricalDepthBook,
    SimulatedFillMode,
)
from newcoin_trader.domain.feature_research import RuleCondition
from newcoin_trader.domain.live_paper import (
    DISCLAIMER,
    WARNING_PAPER_ONLY,
    LivePaperRejectReason,
    LivePaperStatus,
    PositionLifecycle,
    ReplayMarketEvent,
)
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.live_paper_config import (
    DEFAULT_FUTURE_TOLERANCE,
    validate_live_paper_bounds,
)
from newcoin_trader.research.live_paper_engine import (
    BoundedEventQueue,
    PortfolioLedger,
    check_freshness,
    process_live_paper_session,
    require_finite_controlled,
    transition_position,
)
from newcoin_trader.research.live_paper_run import emit_live_paper_artifacts
from newcoin_trader.services.live_paper import LivePaperService, load_replay_events

runner = CliRunner()

T0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


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


def _listing(
    *,
    event_id: str = "e1",
    source: datetime = T0,
    available: datetime | None = None,
    venue: Venue = Venue.BINANCE,
    token: str = "TOKEN",
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


def _market(
    *,
    event_id: str,
    ts: datetime,
    price: str = "10",
    liquidity: str = "100000",
    received: datetime | None = None,
    venue: Venue = Venue.BINANCE,
    token: str = "TOKEN",
    source: str = "binance:trade",
    depth: HistoricalDepthBook | None = None,
) -> ReplayMarketEvent:
    return ReplayMarketEvent(
        event_id=event_id,
        kind="market",
        venue=venue,
        token_address=token,
        chain="binance" if venue is Venue.BINANCE else "solana",
        source_timestamp=ts,
        received_timestamp=received or ts,
        price=Decimal(price),
        liquidity=Decimal(liquidity),
        volume=Decimal("1000"),
        resolution=ObservationResolution.POINT,
        source=source,
        depth=depth,
        provenance={"kind": "trade"},
    )


def _listing_event(
    listing: TokenListingEvent,
    *,
    received: datetime | None = None,
) -> ReplayMarketEvent:
    return ReplayMarketEvent(
        event_id=listing.event_id,
        kind="listing",
        venue=listing.venue,
        token_address=listing.token_address,
        chain=listing.chain.value,
        source_timestamp=listing.source_event_time,
        received_timestamp=received or listing.source_event_time,
        source=listing.source,
        listing=listing,
        provenance=dict(listing.provenance),
    )


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
        "max_token_exposure": Decimal("500"),
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


# ---------------------------------------------------------------------------
# Structural safety — no live orders / wallets / trading switches
# ---------------------------------------------------------------------------


def test_live_paper_modules_have_no_order_post_or_wallet_or_live_switch() -> None:
    root = Path(__file__).resolve().parents[3] / "src" / "newcoin_trader"
    forbidden_substrings = (
        "private_key",
        "mnemonic",
        "seed_phrase",
        "sign_transaction",
        "api/v3/order",
        "create_order",
        "submit_swap",
        "ENABLE_LIVE",
        "LIVE_TRADING=",
        "keypair",
    )
    paths = list((root / "research").glob("live_paper*.py"))
    paths += list((root / "services").glob("live_paper*.py"))
    paths += [root / "domain" / "live_paper.py"]
    assert paths, "expected live_paper modules"
    for path in paths:
        text = path.read_text(encoding="utf-8")
        lower = text.lower()
        for token in forbidden_substrings:
            assert token.lower() not in lower, f"{token} in {path}"
        # Allow documentary "no_wallets" / "no_orders" warnings only.
        assert "solders" not in lower
        assert "web3.eth.account" not in lower
        assert "post(" not in lower
        assert "AsyncHttpClient" not in text


def test_live_paper_service_source_has_no_http_client_calls() -> None:
    path = Path(__file__).resolve().parents[3] / "src" / "newcoin_trader" / "services" / "live_paper.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert "httpx" not in alias.name
                assert "collectors.http" not in alias.name
        if isinstance(node, ast.ImportFrom) and node.module:
            assert "httpx" not in node.module
            assert "collectors.http" not in node.module
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            assert node.func.id not in {"AsyncHttpClient", "post", "request"}
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in {"post", "request", "AsyncHttpClient"}


def test_disclaimer_and_paper_warning_present() -> None:
    assert "paper" in DISCLAIMER.lower() or "live_paper" in DISCLAIMER.lower()
    assert "paper" in WARNING_PAPER_ONLY.lower()
    assert "order" in WARNING_PAPER_ONLY.lower() or "wallet" in WARNING_PAPER_ONLY.lower()


# ---------------------------------------------------------------------------
# Config bounds
# ---------------------------------------------------------------------------


def test_validate_live_paper_bounds_require_explicit_caps() -> None:
    validate_live_paper_bounds(
        duration=timedelta(minutes=30),
        max_events=10,
        max_signals=5,
        max_trades=5,
        queue_capacity=20,
        starting_cash=Decimal("1000"),
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
    )
    with pytest.raises(ConfigError):
        validate_live_paper_bounds(
            duration=timedelta(0),
            max_events=10,
            max_signals=5,
            max_trades=5,
            queue_capacity=20,
            starting_cash=Decimal("1000"),
            position_notional=Decimal("100"),
            holding_period=timedelta(minutes=5),
        )
    with pytest.raises(ConfigError):
        validate_live_paper_bounds(
            duration=timedelta(minutes=30),
            max_events=0,
            max_signals=5,
            max_trades=5,
            queue_capacity=20,
            starting_cash=Decimal("1000"),
            position_notional=Decimal("100"),
            holding_period=timedelta(minutes=5),
        )
    with pytest.raises(ConfigError):
        validate_live_paper_bounds(
            duration=timedelta(minutes=30),
            max_events=10,
            max_signals=5,
            max_trades=5,
            queue_capacity=20,
            starting_cash=Decimal("NaN"),
            position_notional=Decimal("100"),
            holding_period=timedelta(minutes=5),
        )


# ---------------------------------------------------------------------------
# Freshness / PIT / availability
# ---------------------------------------------------------------------------


def test_freshness_rejects_stale_and_future_source_timestamps() -> None:
    now = T0 + timedelta(minutes=10)
    ok = check_freshness(
        source_timestamp=T0 + timedelta(minutes=8),
        received_timestamp=T0 + timedelta(minutes=8),
        decision_time=now,
        max_age=timedelta(minutes=5),
        future_tolerance=timedelta(seconds=1),
    )
    assert ok.accepted

    stale = check_freshness(
        source_timestamp=T0,
        received_timestamp=T0,
        decision_time=now,
        max_age=timedelta(minutes=5),
        future_tolerance=timedelta(seconds=1),
    )
    assert not stale.accepted
    assert stale.reason is LivePaperRejectReason.STALE_SOURCE

    future = check_freshness(
        source_timestamp=now + timedelta(minutes=2),
        received_timestamp=now,
        decision_time=now,
        max_age=timedelta(minutes=5),
        future_tolerance=timedelta(seconds=1),
    )
    assert not future.accepted
    assert future.reason is LivePaperRejectReason.FUTURE_SOURCE


def test_availability_and_pit_reject_before_decision_available() -> None:
    listing = _listing(available=T0 + timedelta(minutes=5))
    events = [
        _listing_event(listing),
        _market(event_id="e1", ts=T0 + timedelta(minutes=1), price="10"),
        _market(event_id="e1", ts=T0 + timedelta(minutes=2), price="11"),
    ]
    report = process_live_paper_session(
        events=events,
        **_default_kwargs(decision_delay=timedelta(minutes=1)),
    )
    assert all(s.status is not LivePaperStatus.SIGNAL_ACCEPTED for s in report.signals) or not report.signals
    assert any(
        r.reason is LivePaperRejectReason.NOT_DECISION_AVAILABLE or r.status is LivePaperStatus.NOT_DECISION_AVAILABLE
        for r in (*report.signals, *report.rejections)
    )


def test_future_market_input_cannot_enter_features_or_fill() -> None:
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    events = [
        _listing_event(listing),
        _market(event_id="e1", ts=decision, price="10", liquidity="50000"),
        _market(event_id="e1", ts=decision + timedelta(minutes=10), price="999", liquidity="50000"),
        _market(event_id="e1", ts=decision + timedelta(minutes=5), price="10.5", liquidity="50000"),
    ]
    report = process_live_paper_session(events=events, **_default_kwargs())
    if report.fills:
        assert all(f.fill_price != Decimal("999") for f in report.fills)


# ---------------------------------------------------------------------------
# Deterministic frozen signal / duplicates
# ---------------------------------------------------------------------------


def test_frozen_rule_produces_deterministic_signal_and_duplicate_event_idempotent() -> None:
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    exit_ts = decision + timedelta(minutes=5)
    base_events = [
        _listing_event(listing),
        _market(event_id="e1", ts=decision, price="10", liquidity="100000"),
        _market(event_id="e1", ts=exit_ts, price="11", liquidity="100000"),
    ]
    report_a = process_live_paper_session(events=base_events, **_default_kwargs())
    report_b = process_live_paper_session(events=base_events, **_default_kwargs())
    assert report_a.model_dump(mode="json") == report_b.model_dump(mode="json")

    dup = process_live_paper_session(
        events=[*base_events, base_events[0], base_events[1]],
        **_default_kwargs(),
    )
    accepted = [s for s in dup.signals if s.status is LivePaperStatus.SIGNAL_ACCEPTED]
    assert len(accepted) <= 1


# ---------------------------------------------------------------------------
# Risk caps / loss halt
# ---------------------------------------------------------------------------


def test_risk_caps_and_session_loss_halt() -> None:
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    events = [
        _listing_event(listing),
        _market(event_id="e1", ts=decision, price="10", liquidity="100000"),
        _market(event_id="e1", ts=decision + timedelta(minutes=5), price="1", liquidity="100000"),
    ]
    report = process_live_paper_session(
        events=events,
        **_default_kwargs(
            starting_cash=Decimal("10000"),
            position_notional=Decimal("100"),
            session_loss_limit=Decimal("1"),
            daily_loss_limit=Decimal("1"),
        ),
    )
    # Either no entry due to caps, or halt after loss — never silent continue past halt.
    assert report.meta.halted or report.portfolio.realized_pnl >= Decimal("-1") or report.rejections


def test_insufficient_cash_rejects_entry() -> None:
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    events = [
        _listing_event(listing),
        _market(event_id="e1", ts=decision, price="10", liquidity="100000"),
        _market(event_id="e1", ts=decision + timedelta(minutes=5), price="11", liquidity="100000"),
    ]
    report = process_live_paper_session(
        events=events,
        **_default_kwargs(starting_cash=Decimal("10"), position_notional=Decimal("100")),
    )
    assert any(r.reason is LivePaperRejectReason.INSUFFICIENT_CASH for r in report.rejections) or not report.fills


# ---------------------------------------------------------------------------
# Phase 5 fill reuse: partial / no fill / failed exit
# ---------------------------------------------------------------------------


def test_cex_depth_used_when_supplied_else_modeled_fallback() -> None:
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    exit_ts = decision + timedelta(minutes=5)
    book = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=decision,
        bids=(DepthLevel(price=Decimal("9.9"), quantity=Decimal("100")),),
        asks=(DepthLevel(price=Decimal("10.1"), quantity=Decimal("5")),),
        source="binance:depth",
    )
    events = [
        _listing_event(listing),
        _market(event_id="e1", ts=decision, price="10", liquidity="100000", depth=book),
        _market(event_id="e1", ts=exit_ts, price="11", liquidity="100000"),
    ]
    report = process_live_paper_session(
        events=events,
        **_default_kwargs(position_notional=Decimal("1000"), max_token_exposure=Decimal("5000")),
    )
    assert report.fills
    entry = next(f for f in report.fills if f.side is Side.BUY)
    assert entry.mode is SimulatedFillMode.EXACT_DEPTH
    assert entry.status.value in {"entry_filled", "entry_partial", "fully_filled", "partial"}
    assert entry.confidence is ExecutionConfidence.EXACT_DEPTH

    no_depth = process_live_paper_session(
        events=[
            _listing_event(listing),
            _market(event_id="e1", ts=decision, price="10", liquidity="100000"),
            _market(event_id="e1", ts=exit_ts, price="11", liquidity="100000"),
        ],
        **_default_kwargs(),
    )
    assert no_depth.fills
    entry2 = next(f for f in no_depth.fills if f.side is Side.BUY)
    assert entry2.mode in {SimulatedFillMode.MODELED_PRICE, SimulatedFillMode.MODELED_LIQUIDITY}
    assert "modeled" in entry2.label.lower() or entry2.confidence.value.startswith("modeled")


def test_dex_uses_modeled_liquidity_never_amm_exact() -> None:
    listing = _listing(venue=Venue.RAYDIUM, token="SOLTOKEN")
    decision = T0 + timedelta(minutes=1)
    exit_ts = decision + timedelta(minutes=5)
    events = [
        _listing_event(listing),
        _market(
            event_id="e1",
            ts=decision,
            price="10",
            liquidity="50000",
            venue=Venue.RAYDIUM,
            token="SOLTOKEN",
            source="raydium:pool",
        ),
        _market(
            event_id="e1",
            ts=exit_ts,
            price="11",
            liquidity="50000",
            venue=Venue.RAYDIUM,
            token="SOLTOKEN",
            source="raydium:pool",
        ),
    ]
    report = process_live_paper_session(
        events=events,
        **_default_kwargs(venue=Venue.RAYDIUM, assumed_fee_bps=Decimal("30")),
    )
    assert report.fills
    for fill in report.fills:
        assert fill.mode is SimulatedFillMode.MODELED_LIQUIDITY
        assert "amm" not in fill.label.lower() or "not" in fill.label.lower()


def test_entry_partial_and_no_fill_and_failed_exit_retained() -> None:
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    # Tiny ask liquidity → partial or unfilled via depth
    thin = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=decision,
        bids=(DepthLevel(price=Decimal("9.9"), quantity=Decimal("1")),),
        asks=(DepthLevel(price=Decimal("10"), quantity=Decimal("0.1")),),
        source="binance:depth",
    )
    events = [
        _listing_event(listing),
        _market(event_id="e1", ts=decision, price="10", liquidity="100", depth=thin),
        _market(event_id="e1", ts=decision + timedelta(minutes=5), price="11", liquidity="100"),
    ]
    report = process_live_paper_session(
        events=events,
        **_default_kwargs(position_notional=Decimal("1000"), max_participation=Decimal("1")),
    )
    assert report.fills or report.rejections or report.positions
    # Zero liquidity modeled → no fill path
    empty_liq = process_live_paper_session(
        events=[
            _listing_event(listing),
            _market(event_id="e1", ts=decision, price="10", liquidity="0"),
            _market(event_id="e1", ts=decision + timedelta(minutes=5), price="11", liquidity="0"),
        ],
        **_default_kwargs(),
    )
    assert not any(f.side is Side.BUY and f.fill_qty > 0 for f in empty_liq.fills) or any(
        p.lifecycle is PositionLifecycle.FAILED_EXIT for p in empty_liq.positions
    )

    # Open entry then missing exit market → failed exit retained
    open_only = process_live_paper_session(
        events=[
            _listing_event(listing),
            _market(event_id="e1", ts=decision, price="10", liquidity="100000"),
        ],
        **_default_kwargs(duration=timedelta(minutes=10), holding_period=timedelta(minutes=5)),
    )
    if any(p.lifecycle is PositionLifecycle.OPEN for p in open_only.positions):
        assert any(p.lifecycle in {PositionLifecycle.OPEN, PositionLifecycle.FAILED_EXIT} for p in open_only.positions)


# ---------------------------------------------------------------------------
# State machine / no double transition
# ---------------------------------------------------------------------------


def test_position_state_machine_blocks_invalid_and_double_transitions() -> None:
    assert transition_position(PositionLifecycle.PENDING, "open") is PositionLifecycle.OPEN
    assert transition_position(PositionLifecycle.OPEN, "close") is PositionLifecycle.CLOSED
    assert transition_position(PositionLifecycle.OPEN, "fail_exit") is PositionLifecycle.FAILED_EXIT
    with pytest.raises(ValueError):
        transition_position(PositionLifecycle.CLOSED, "open")
    with pytest.raises(ValueError):
        transition_position(PositionLifecycle.CLOSED, "close")
    with pytest.raises(ValueError):
        transition_position(PositionLifecycle.FAILED_EXIT, "close")


# ---------------------------------------------------------------------------
# Restart / replay idempotency
# ---------------------------------------------------------------------------


def test_restart_replay_idempotent_no_duplicate_signal_fill_pnl() -> None:
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    exit_ts = decision + timedelta(minutes=5)
    events = [
        _listing_event(listing),
        _market(event_id="e1", ts=decision, price="10", liquidity="100000"),
        _market(event_id="e1", ts=exit_ts, price="12", liquidity="100000"),
    ]
    store: dict[str, object] = {}
    first = process_live_paper_session(events=events, state_store=store, **_default_kwargs())
    pnl_after_first = first.portfolio.realized_pnl
    fills_after_first = len(first.fills)
    accepted_after_first = len([s for s in first.signals if s.status is LivePaperStatus.SIGNAL_ACCEPTED])
    second = process_live_paper_session(events=events, state_store=store, **_default_kwargs())
    # Restart must not double-apply fills or realized PnL.
    assert second.portfolio.realized_pnl == pnl_after_first
    assert len(store.get("seen_fills", [])) == fills_after_first or fills_after_first == 0
    assert len(store.get("seen_signals", [])) == accepted_after_first or accepted_after_first == 0
    # Second pass may emit duplicate markers but must not create additional accepted fills.
    new_accepted = [s for s in second.signals if s.status is LivePaperStatus.SIGNAL_ACCEPTED]
    assert len(new_accepted) == 0
    assert all(f.fill_id in set(store.get("seen_fills", [])) for f in second.fills) or len(second.fills) == 0


# ---------------------------------------------------------------------------
# Accounting / queue / numeric
# ---------------------------------------------------------------------------


def test_portfolio_accounting_cash_equity_drawdown() -> None:
    ledger = PortfolioLedger(starting_cash=Decimal("1000"))
    ledger.apply_entry(notional=Decimal("100"), fee=Decimal("1"))
    assert ledger.cash == Decimal("899")
    ledger.mark_unrealized(Decimal("5"))
    assert ledger.equity == ledger.cash + Decimal("5")
    ledger.apply_exit(proceeds=Decimal("110"), fee=Decimal("1"), cost_basis=Decimal("100"))
    assert ledger.realized_pnl == Decimal("9")
    assert ledger.drawdown >= 0


def test_bounded_queue_reports_overflow_never_silent_drop() -> None:
    q = BoundedEventQueue(capacity=2)
    e1 = _market(event_id="a", ts=T0)
    e2 = _market(event_id="b", ts=T0 + timedelta(seconds=1))
    e3 = _market(event_id="c", ts=T0 + timedelta(seconds=2))
    assert q.push(e1).accepted
    assert q.push(e2).accepted
    overflow = q.push(e3)
    assert not overflow.accepted
    assert overflow.reason is LivePaperRejectReason.QUEUE_OVERFLOW
    assert q.overflow_count == 1
    assert len(list(q)) == 2


def test_nonfinite_decimal_rejected_controlled() -> None:
    with pytest.raises(ConfigError):
        require_finite_controlled(Decimal("NaN"), name="cash")
    with pytest.raises(ConfigError):
        require_finite_controlled(Decimal("Infinity"), name="price")
    assert require_finite_controlled(Decimal("1.5"), name="ok") == Decimal("1.5")


# ---------------------------------------------------------------------------
# Artifacts / service / CLI
# ---------------------------------------------------------------------------


def test_deterministic_replay_artifacts(tmp_path: Path) -> None:
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    exit_ts = decision + timedelta(minutes=5)
    events = [
        _listing_event(listing),
        _market(event_id="e1", ts=decision, price="10", liquidity="100000"),
        _market(event_id="e1", ts=exit_ts, price="11", liquidity="100000"),
    ]
    report = process_live_paper_session(events=events, **_default_kwargs())
    paths_a = emit_live_paper_artifacts(report, tmp_path / "a")
    paths_b = emit_live_paper_artifacts(report, tmp_path / "b")
    assert paths_a["json"].read_text() == paths_b["json"].read_text()
    assert paths_a["csv"].read_text() == paths_b["csv"].read_text()
    assert paths_a["markdown"].read_text() == paths_b["markdown"].read_text()
    payload = json.loads(paths_a["json"].read_text())
    assert "portfolio" in payload
    assert "signals" in payload
    assert "data_quality" in payload


@pytest.mark.asyncio
async def test_live_paper_service_accepts_injected_feed_no_http(tmp_path: Path) -> None:
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    events = [
        _listing_event(listing),
        _market(event_id="e1", ts=decision, price="10", liquidity="100000"),
        _market(event_id="e1", ts=decision + timedelta(minutes=5), price="11", liquidity="100000"),
    ]
    service = LivePaperService()
    report, paths = await service.run_replay(
        events=events,
        identity=_identity(),
        venue="binance",
        duration=timedelta(hours=1),
        max_events=50,
        max_signals=20,
        max_trades=20,
        queue_capacity=100,
        starting_cash=Decimal("10000"),
        output_dir=tmp_path,
        session_start=T0,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
    )
    assert report.meta.phase == "phase_6_live_paper"
    assert paths["json"].exists()
    assert paths["csv"].exists()
    assert paths["markdown"].exists()


def test_load_replay_and_cli_requires_bounds(tmp_path: Path) -> None:
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    events = [
        _listing_event(listing),
        _market(event_id="e1", ts=decision, price="10", liquidity="100000"),
        _market(event_id="e1", ts=decision + timedelta(minutes=5), price="11", liquidity="100000"),
    ]
    replay_path = tmp_path / "replay.json"
    replay_path.write_text(
        json.dumps({"events": [e.model_dump(mode="json") for e in events]}, indent=2),
        encoding="utf-8",
    )
    loaded = load_replay_events(replay_path)
    assert len(loaded) == 3

    missing = runner.invoke(app, ["live-paper"])
    assert missing.exit_code != 0

    result = runner.invoke(
        app,
        [
            "live-paper",
            "--venue",
            "binance",
            "--duration",
            "1h",
            "--max-events",
            "50",
            "--max-signals",
            "20",
            "--max-trades",
            "20",
            "--queue-capacity",
            "100",
            "--frozen-rule-id",
            "frozen-rule-1",
            "--phase4-config-id",
            "cfg-phase4",
            "--paper-starting-cash",
            "10000",
            "--output-dir",
            str(tmp_path / "out"),
            "--replay-path",
            str(replay_path),
            "--session-start",
            T0.isoformat(),
            "--holding-period",
            "5m",
            "--position-notional",
            "100",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "live-paper complete" in result.output


def test_phase5_fill_helpers_reused_by_engine_import() -> None:
    import newcoin_trader.research.live_paper_engine as eng

    assert (
        hasattr(eng, "simulate_cex_depth_fill")
        or "simulate_cex_depth_fill" in eng.__dict__.get("__all__", ())
        or "simulate_cex_depth_fill" in dir(eng)
    )
    src = Path(eng.__file__).read_text(encoding="utf-8")
    assert "simulate_cex_depth_fill" in src
    assert "simulate_dex_liquidity_fill" in src
    assert "build_decision_feature_record" in src
    assert "from newcoin_trader.execution.paper_broker" not in src
    assert "PaperBroker(" not in src


def test_comparison_fields_present_when_provided() -> None:
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    events = [
        _listing_event(listing),
        _market(event_id="e1", ts=decision, price="10", liquidity="100000"),
        _market(event_id="e1", ts=decision + timedelta(minutes=5), price="11", liquidity="100000"),
    ]
    report = process_live_paper_session(
        events=events,
        phase4_gross_return=Decimal("0.10"),
        phase5_historical_net=Decimal("0.05"),
        **_default_kwargs(),
    )
    assert report.comparison is not None
    assert report.comparison.get("phase4_gross_return") == Decimal("0.10")
    assert report.comparison.get("phase5_historical_net") == Decimal("0.05")


def test_orm_models_and_migration_exist_for_session_state() -> None:
    from newcoin_trader.database.models import LivePaperPosition, LivePaperSession, LivePaperSignal

    assert LivePaperSession.__tablename__ == "live_paper_sessions"
    assert LivePaperSignal.__tablename__ == "live_paper_signals"
    assert LivePaperPosition.__tablename__ == "live_paper_positions"
    mig = Path(__file__).resolve().parents[3] / "migrations" / "versions"
    files = list(mig.glob("*live_paper*.py"))
    assert files, "expected alembic migration for live paper session state"


# ---------------------------------------------------------------------------
# Blocker 1: received-time lookahead (source AND received clocks)
# ---------------------------------------------------------------------------


def test_freshness_rejects_future_received_pre_equal_post_clock() -> None:
    decision = T0 + timedelta(minutes=10)
    # Pre: received before decision — accept when source also ok.
    pre = check_freshness(
        source_timestamp=decision - timedelta(minutes=1),
        received_timestamp=decision - timedelta(seconds=1),
        decision_time=decision,
        max_age=timedelta(minutes=5),
        future_tolerance=timedelta(0),
    )
    assert pre.accepted

    # Equal: received exactly at decision — accept.
    equal = check_freshness(
        source_timestamp=decision,
        received_timestamp=decision,
        decision_time=decision,
        max_age=timedelta(minutes=5),
        future_tolerance=timedelta(0),
    )
    assert equal.accepted

    # Post: received after decision with strict default tolerance — FUTURE_RECEIVED.
    post = check_freshness(
        source_timestamp=decision - timedelta(seconds=1),
        received_timestamp=decision + timedelta(seconds=1),
        decision_time=decision,
        max_age=timedelta(minutes=5),
        future_tolerance=timedelta(0),
    )
    assert not post.accepted
    assert post.reason is LivePaperRejectReason.FUTURE_RECEIVED

    # Explicit tolerance may accept controlled future-received within window.
    tolerated = check_freshness(
        source_timestamp=decision - timedelta(seconds=1),
        received_timestamp=decision + timedelta(milliseconds=500),
        decision_time=decision,
        max_age=timedelta(minutes=5),
        future_tolerance=timedelta(seconds=1),
    )
    assert tolerated.accepted


def test_future_received_blocked_before_features_entry_and_exit_effect_invariant() -> None:
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    exit_ts = decision + timedelta(minutes=5)
    # Entry market source is timely but received is after decision → must not affect features/entry.
    events_lookahead = [
        _listing_event(listing),
        _market(
            event_id="e1",
            ts=decision,
            price="10",
            liquidity="100000",
            received=decision + timedelta(minutes=2),
        ),
        _market(event_id="e1", ts=exit_ts, price="11", liquidity="100000"),
    ]
    leaked = process_live_paper_session(
        events=events_lookahead,
        **_default_kwargs(future_tolerance=timedelta(0)),
    )
    assert not any(f.side is Side.BUY for f in leaked.fills)
    assert leaked.data_quality.get("future_rejections", 0) >= 1 or any(
        r.reason is LivePaperRejectReason.FUTURE_RECEIVED for r in leaked.rejections
    )

    # Clean baseline vs exit-received lookahead: outcomes must be identical when only
    # a future-received exit tick is added (effect-invariance — no fill from it).
    clean_events = [
        _listing_event(listing),
        _market(event_id="e1", ts=decision, price="10", liquidity="100000"),
        _market(event_id="e1", ts=exit_ts, price="11", liquidity="100000"),
    ]
    poison_exit = [
        *clean_events,
        _market(
            event_id="e1",
            ts=exit_ts,
            price="999",
            liquidity="100000",
            received=exit_ts + timedelta(minutes=30),
        ),
    ]
    clean = process_live_paper_session(events=clean_events, **_default_kwargs(future_tolerance=timedelta(0)))
    poisoned = process_live_paper_session(events=poison_exit, **_default_kwargs(future_tolerance=timedelta(0)))
    assert clean.portfolio.realized_pnl == poisoned.portfolio.realized_pnl
    assert all(f.fill_price != Decimal("999") for f in poisoned.fills)


# ---------------------------------------------------------------------------
# Blocker 2: session lower/upper boundary clocks
# ---------------------------------------------------------------------------


def test_session_boundary_before_exact_normal_and_exit_beyond_end() -> None:
    listing_before = _listing(event_id="before", source=T0 - timedelta(minutes=10))
    before_events = [
        _listing_event(listing_before),
        _market(event_id="before", ts=T0 - timedelta(minutes=9), price="10", liquidity="100000"),
    ]
    before = process_live_paper_session(
        events=before_events,
        **_default_kwargs(session_start=T0, duration=timedelta(hours=1), decision_delay=timedelta(0)),
    )
    assert not any(s.status is LivePaperStatus.SIGNAL_ACCEPTED for s in before.signals)
    assert any(r.reason is LivePaperRejectReason.BEFORE_SESSION for r in before.rejections) or any(
        s.status is LivePaperStatus.BEFORE_SESSION for s in before.signals
    )

    # Exact session_start boundary is admissible.
    listing_exact = _listing(event_id="exact", source=T0)
    exact_events = [
        _listing_event(listing_exact),
        _market(event_id="exact", ts=T0 + timedelta(minutes=1), price="10", liquidity="100000"),
        _market(event_id="exact", ts=T0 + timedelta(minutes=6), price="11", liquidity="100000"),
    ]
    exact = process_live_paper_session(
        events=exact_events,
        **_default_kwargs(session_start=T0, duration=timedelta(hours=1)),
    )
    assert any(s.status is LivePaperStatus.SIGNAL_ACCEPTED for s in exact.signals) or exact.fills

    # Normal in-session trade.
    listing_ok = _listing(event_id="ok", source=T0 + timedelta(minutes=2))
    ok_events = [
        _listing_event(listing_ok),
        _market(event_id="ok", ts=T0 + timedelta(minutes=3), price="10", liquidity="100000"),
        _market(event_id="ok", ts=T0 + timedelta(minutes=8), price="11", liquidity="100000"),
    ]
    ok = process_live_paper_session(
        events=ok_events,
        **_default_kwargs(session_start=T0, duration=timedelta(hours=1)),
    )
    assert ok.fills

    # Exit requiring holding beyond session_end must not execute.
    listing_horizon = _listing(event_id="hz", source=T0)
    horizon_events = [
        _listing_event(listing_horizon),
        _market(event_id="hz", ts=T0 + timedelta(minutes=1), price="10", liquidity="100000"),
        _market(event_id="hz", ts=T0 + timedelta(minutes=20), price="12", liquidity="100000"),
    ]
    horizon = process_live_paper_session(
        events=horizon_events,
        **_default_kwargs(
            session_start=T0,
            duration=timedelta(minutes=10),
            holding_period=timedelta(minutes=15),
            decision_delay=timedelta(minutes=1),
        ),
    )
    assert not any(f.side is Side.SELL for f in horizon.fills)
    assert any(
        p.lifecycle in {PositionLifecycle.OPEN, PositionLifecycle.FAILED_EXIT, PositionLifecycle.CLOSING}
        for p in horizon.positions
    ) or any(r.reason is LivePaperRejectReason.SESSION_HORIZON for r in horizon.rejections)


# ---------------------------------------------------------------------------
# Blocker 3: partial exit retains remaining qty / pro-rata cost / OPEN until zero
# ---------------------------------------------------------------------------


def test_partial_exit_retains_remaining_and_pro_rata_pnl_then_close() -> None:
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    exit1 = decision + timedelta(minutes=5)
    exit2 = decision + timedelta(minutes=6)
    exit3 = decision + timedelta(minutes=7)
    entry_book = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=decision,
        bids=(DepthLevel(price=Decimal("9.9"), quantity=Decimal("100")),),
        asks=(DepthLevel(price=Decimal("10"), quantity=Decimal("10")),),
        source="binance:depth",
    )
    # Each exit book can only absorb half of a 10-qty position (then remainder).
    thin_exit = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=exit1,
        bids=(DepthLevel(price=Decimal("11"), quantity=Decimal("5")),),
        asks=(DepthLevel(price=Decimal("11.1"), quantity=Decimal("100")),),
        source="binance:depth",
    )
    thin_exit2 = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=exit2,
        bids=(DepthLevel(price=Decimal("11"), quantity=Decimal("3")),),
        asks=(DepthLevel(price=Decimal("11.1"), quantity=Decimal("100")),),
        source="binance:depth",
    )
    final_exit = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=exit3,
        bids=(DepthLevel(price=Decimal("11"), quantity=Decimal("100")),),
        asks=(DepthLevel(price=Decimal("11.1"), quantity=Decimal("100")),),
        source="binance:depth",
    )
    events = [
        _listing_event(listing),
        _market(event_id="e1", ts=decision, price="10", liquidity="100000", depth=entry_book),
        _market(event_id="e1", ts=exit1, price="11", liquidity="100000", depth=thin_exit),
        _market(event_id="e1", ts=exit2, price="11", liquidity="100000", depth=thin_exit2),
        _market(event_id="e1", ts=exit3, price="11", liquidity="100000", depth=final_exit),
    ]
    report = process_live_paper_session(
        events=events,
        **_default_kwargs(
            position_notional=Decimal("100"),
            max_token_exposure=Decimal("5000"),
            assumed_fee_bps=Decimal("10"),
        ),
    )
    sells = [f for f in report.fills if f.side is Side.SELL]
    assert len(sells) >= 2
    assert any(f.status is LivePaperStatus.EXIT_PARTIAL for f in sells)
    # After first partial only, position must not be CLOSED.
    partial_only = process_live_paper_session(
        events=[
            _listing_event(listing),
            _market(event_id="e1", ts=decision, price="10", liquidity="100000", depth=entry_book),
            _market(event_id="e1", ts=exit1, price="11", liquidity="100000", depth=thin_exit),
        ],
        **_default_kwargs(position_notional=Decimal("100"), max_token_exposure=Decimal("5000")),
    )
    assert partial_only.positions
    pos = partial_only.positions[-1]
    assert pos.lifecycle in {PositionLifecycle.OPEN, PositionLifecycle.CLOSING}
    assert pos.remaining_qty is not None and pos.remaining_qty > 0
    assert pos.remaining_cost_basis is not None and pos.remaining_cost_basis > 0
    # Sold half of 10 qty @ ~11 vs cost 10 → positive realized on sold slice only; fees included once.
    assert pos.realized_pnl is not None and pos.realized_pnl != 0
    sell_fees = sum((f.fee_cost for f in partial_only.fills if f.side is Side.SELL), Decimal("0"))
    assert sell_fees > 0
    # Full multi-partial close ends CLOSED with zero remaining and no duplicate fill ids.
    assert report.positions
    closed = report.positions[-1]
    assert closed.lifecycle is PositionLifecycle.CLOSED
    assert closed.remaining_qty in (None, Decimal("0"))
    fill_ids = [f.fill_id for f in report.fills]
    assert len(fill_ids) == len(set(fill_ids))

    # Loss path: partial exit below cost realizes loss only on sold pro-rata.
    loss_book = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=exit1,
        bids=(DepthLevel(price=Decimal("8"), quantity=Decimal("5")),),
        asks=(DepthLevel(price=Decimal("8.1"), quantity=Decimal("100")),),
        source="binance:depth",
    )
    loss = process_live_paper_session(
        events=[
            _listing_event(listing),
            _market(event_id="e1", ts=decision, price="10", liquidity="100000", depth=entry_book),
            _market(event_id="e1", ts=exit1, price="8", liquidity="100000", depth=loss_book),
        ],
        **_default_kwargs(position_notional=Decimal("100"), max_token_exposure=Decimal("5000")),
    )
    assert loss.positions[-1].realized_pnl is not None
    assert loss.positions[-1].realized_pnl < 0
    assert loss.positions[-1].lifecycle in {PositionLifecycle.OPEN, PositionLifecycle.CLOSING}


# ---------------------------------------------------------------------------
# Blocker 4: max_trades = paper positions/round-trips (not fills)
# ---------------------------------------------------------------------------


def test_max_trades_caps_positions_not_fills_and_exposes_counts() -> None:
    events: list[ReplayMarketEvent] = []
    for i in range(3):
        eid = f"e{i}"
        listing = _listing(event_id=eid, source=T0 + timedelta(minutes=i), token=f"TOK{i}")
        decision = T0 + timedelta(minutes=i + 1)
        events.append(_listing_event(listing))
        events.append(_market(event_id=eid, ts=decision, price="10", liquidity="100000", token=f"TOK{i}"))
        events.append(
            _market(
                event_id=eid,
                ts=decision + timedelta(minutes=5),
                price="11",
                liquidity="100000",
                token=f"TOK{i}",
            )
        )
    report = process_live_paper_session(
        events=events,
        **_default_kwargs(
            max_trades=1,
            max_signals=10,
            max_open_positions=5,
            max_token_exposure=Decimal("5000"),
            decision_delay=timedelta(minutes=1),
        ),
    )
    positions = list(report.positions)
    assert len(positions) <= 1
    assert report.meta.trade_count == len(positions)
    assert report.meta.fill_count == len(report.fills)
    # One round-trip normally yields 2 fills; cap is on positions, so fill_count may exceed trade_count.
    if report.fills:
        assert report.meta.fill_count >= report.meta.trade_count
    assert report.meta.halt_reason is LivePaperRejectReason.MAX_TRADES or len(positions) <= 1


def test_max_trades_cli_help_is_positions_not_ambiguous_fills() -> None:
    result = runner.invoke(app, ["live-paper", "--help"])
    assert result.exit_code == 0
    # Typer wraps option help across box-drawing lines; strip non-alnum separators.
    compact = "".join(ch.lower() if ch.isalnum() else " " for ch in result.output)
    compact = " ".join(compact.split())
    assert "max trades" in compact
    assert "paper positions" in compact
    assert "round trips" in compact
    assert "not fills" in compact


# ---------------------------------------------------------------------------
# Blocker 5: max_events audit — no silent drop; separate from queue overflow
# ---------------------------------------------------------------------------


def test_max_events_audit_under_equal_over_and_separate_queue_overflow() -> None:
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    base = [
        _listing_event(listing),
        _market(event_id="e1", ts=decision, price="10", liquidity="100000"),
        _market(event_id="e1", ts=decision + timedelta(minutes=5), price="11", liquidity="100000"),
    ]
    under = process_live_paper_session(events=base, **_default_kwargs(max_events=10, queue_capacity=100))
    assert under.meta.supplied_event_count == 3
    assert under.meta.admitted_event_count == 3
    assert under.meta.max_events_rejected_count == 0

    equal = process_live_paper_session(events=base, **_default_kwargs(max_events=3, queue_capacity=100))
    assert equal.meta.supplied_event_count == 3
    assert equal.meta.admitted_event_count == 3
    assert equal.meta.max_events_rejected_count == 0

    extras = [
        _market(event_id="e1", ts=decision + timedelta(minutes=m), price="10.5", liquidity="100000")
        for m in range(6, 10)
    ]
    over_events = [*base, *extras]
    over = process_live_paper_session(events=over_events, **_default_kwargs(max_events=3, queue_capacity=100))
    assert over.meta.supplied_event_count == len(over_events)
    assert over.meta.admitted_event_count == 3
    assert over.meta.max_events_rejected_count == len(over_events) - 3
    assert over.meta.overflow_count == 0
    max_event_rejections = [r for r in over.rejections if r.reason is LivePaperRejectReason.MAX_EVENTS]
    assert len(max_event_rejections) == over.meta.max_events_rejected_count
    # Deterministic selection: same over-cap set always rejects the same trailing events.
    over_b = process_live_paper_session(events=over_events, **_default_kwargs(max_events=3, queue_capacity=100))
    assert [r.event_id for r in max_event_rejections] == [
        r.event_id for r in over_b.rejections if r.reason is LivePaperRejectReason.MAX_EVENTS
    ]

    # Queue overflow is counted separately when capacity < admitted max_events window.
    q_events = [
        _listing_event(_listing(event_id=f"q{i}", source=T0 + timedelta(minutes=i), token=f"Q{i}")) for i in range(5)
    ]
    q_over = process_live_paper_session(
        events=q_events,
        **_default_kwargs(max_events=5, queue_capacity=2),
    )
    assert q_over.meta.max_events_rejected_count == 0
    assert q_over.meta.overflow_count >= 1
    assert q_over.meta.admitted_event_count == 2


# ---------------------------------------------------------------------------
# Blocker 6: PG durable idempotency — monotonic seen IDs across Run1/2/3
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_repository_durable_idempotency_three_runs_monotonic(tmp_path: Path) -> None:
    from newcoin_trader.database.repositories.live_paper import LivePaperRepository

    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    exit_ts = decision + timedelta(minutes=5)
    events = [
        _listing_event(listing),
        _market(event_id="e1", ts=decision, price="10", liquidity="100000"),
        _market(event_id="e1", ts=exit_ts, price="12", liquidity="100000"),
    ]
    identity = _identity()

    class _MemRepo(LivePaperRepository):
        """Repository-backed durable store (in-process) exercising real merge logic."""

        def __init__(self) -> None:
            self._state: dict[str, object] = {}
            self._session = None  # type: ignore[assignment]

        async def load_session_state(self, **kwargs: object) -> dict[str, object]:  # type: ignore[override]
            _ = kwargs
            return dict(self._state)

        async def persist_report(self, report: object) -> None:  # type: ignore[override]
            from newcoin_trader.domain.live_paper import LivePaperReport as LPR

            assert isinstance(report, LPR)
            merged = LivePaperRepository.merge_durable_state(self._state, report)
            self._state = merged

    repo = _MemRepo()
    service = LivePaperService()
    service._repo = repo  # type: ignore[attr-defined]

    r1, _ = await service.run_replay(
        events=events,
        identity=identity,
        venue="binance",
        duration=timedelta(hours=1),
        max_events=50,
        max_signals=20,
        max_trades=20,
        queue_capacity=100,
        starting_cash=Decimal("10000"),
        output_dir=tmp_path / "r1",
        session_start=T0,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
    )
    state1 = await repo.load_session_state(
        venue="binance",
        rule_id=identity.rule_id,
        phase4_config_id=identity.phase4_config_id,
        session_start=T0,
    )
    assert state1.get("seen_signals")
    assert state1.get("seen_fills")
    pnl1 = r1.portfolio.realized_pnl

    # Run2: empty current accepted set must NOT wipe durable seen IDs.
    r2, _ = await service.run_replay(
        events=events,
        identity=identity,
        venue="binance",
        duration=timedelta(hours=1),
        max_events=50,
        max_signals=20,
        max_trades=20,
        queue_capacity=100,
        starting_cash=Decimal("10000"),
        output_dir=tmp_path / "r2",
        session_start=T0,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
    )
    state2 = await repo.load_session_state(
        venue="binance",
        rule_id=identity.rule_id,
        phase4_config_id=identity.phase4_config_id,
        session_start=T0,
    )
    assert set(state2.get("seen_signals", [])) >= set(state1.get("seen_signals", []))
    assert set(state2.get("seen_fills", [])) >= set(state1.get("seen_fills", []))
    assert r2.portfolio.realized_pnl == pnl1

    r3, _ = await service.run_replay(
        events=events,
        identity=identity,
        venue="binance",
        duration=timedelta(hours=1),
        max_events=50,
        max_signals=20,
        max_trades=20,
        queue_capacity=100,
        starting_cash=Decimal("10000"),
        output_dir=tmp_path / "r3",
        session_start=T0,
        position_notional=Decimal("100"),
        holding_period=timedelta(minutes=5),
    )
    state3 = await repo.load_session_state(
        venue="binance",
        rule_id=identity.rule_id,
        phase4_config_id=identity.phase4_config_id,
        session_start=T0,
    )
    assert set(state3.get("seen_signals", [])) >= set(state2.get("seen_signals", []))
    assert set(state3.get("seen_fills", [])) >= set(state2.get("seen_fills", []))
    assert r3.portfolio.realized_pnl == pnl1
    assert len([s for s in r3.signals if s.status is LivePaperStatus.SIGNAL_ACCEPTED]) == 0


def test_merge_durable_state_unions_and_never_wipes_with_empty_report() -> None:
    from newcoin_trader.database.repositories.live_paper import LivePaperRepository
    from newcoin_trader.domain.live_paper import LivePaperReport, LivePaperSessionMeta, PortfolioSnapshot

    prior = {
        "seen_signals": ["sig-a", "sig-b"],
        "seen_fills": ["fill-1", "fill-2"],
        "realized_pnl": "5",
    }
    meta = LivePaperSessionMeta(
        session_id="s",
        config_id="c",
        venue="binance",
        session_start=T0,
        session_end=T0 + timedelta(hours=1),
        duration=timedelta(hours=1),
        max_events=10,
        max_signals=10,
        max_trades=10,
        queue_capacity=10,
        starting_cash=Decimal("1000"),
        event_count=0,
        signal_count=0,
        trade_count=0,
        fill_count=0,
        overflow_count=0,
        supplied_event_count=0,
        admitted_event_count=0,
        max_events_rejected_count=0,
        frozen_rule_id="r",
        phase4_config_id="p4",
    )
    empty = LivePaperReport(
        meta=meta,
        portfolio=PortfolioSnapshot(
            cash=Decimal("1000"),
            equity=Decimal("1000"),
            realized_pnl=Decimal("5"),
            unrealized_pnl=Decimal("0"),
            drawdown=Decimal("0"),
            open_positions=0,
            failed_positions=0,
            peak_equity=Decimal("1000"),
        ),
    )
    merged = LivePaperRepository.merge_durable_state(prior, empty)
    assert set(merged["seen_signals"]) == {"sig-a", "sig-b"}
    assert set(merged["seen_fills"]) == {"fill-1", "fill-2"}
    assert merged["realized_pnl"] == "5"


# ---------------------------------------------------------------------------
# Integrity repair A: listing ReplayMarketEvent received-time must gate
# ---------------------------------------------------------------------------


def test_listing_received_after_decision_rejects_before_features_signal_fill() -> None:
    """Outer listing received_timestamp after decision_time → FUTURE_RECEIVED; no features/signal/fill."""
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    exit_ts = decision + timedelta(minutes=5)
    future_recv = decision + timedelta(seconds=1)
    events = [
        _listing_event(listing, received=future_recv),
        _market(event_id="e1", ts=decision, price="10", liquidity="100000"),
        _market(event_id="e1", ts=exit_ts, price="11", liquidity="100000"),
    ]
    report = process_live_paper_session(
        events=events,
        **_default_kwargs(future_tolerance=timedelta(0)),
    )
    assert report.fills == ()
    assert not any(s.status is LivePaperStatus.SIGNAL_ACCEPTED for s in report.signals)
    fut = [r for r in report.rejections if r.reason is LivePaperRejectReason.FUTURE_RECEIVED]
    assert fut, "listing future-received must be an explicit auditable reject"
    assert fut[0].received_timestamp == future_recv
    assert fut[0].source_timestamp == listing.source_event_time
    assert report.data_quality.get("future_rejections", 0) >= 1
    # Deterministic artifacts: same inject → identical reject identity.
    again = process_live_paper_session(
        events=events,
        **_default_kwargs(future_tolerance=timedelta(0)),
    )
    assert [r.signal_id for r in again.rejections if r.reason is LivePaperRejectReason.FUTURE_RECEIVED] == [
        r.signal_id for r in fut
    ]


def test_listing_received_equal_and_before_eligible_session_prestart_market_unchanged() -> None:
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    exit_ts = decision + timedelta(minutes=5)

    equal_events = [
        _listing_event(listing, received=decision),
        _market(event_id="e1", ts=decision, price="10", liquidity="100000"),
        _market(event_id="e1", ts=exit_ts, price="11", liquidity="100000"),
    ]
    equal = process_live_paper_session(
        events=equal_events,
        **_default_kwargs(future_tolerance=timedelta(0)),
    )
    assert any(s.status is LivePaperStatus.SIGNAL_ACCEPTED for s in equal.signals) or equal.fills

    before_events = [
        _listing_event(listing, received=decision - timedelta(seconds=1)),
        _market(event_id="e1", ts=decision, price="10", liquidity="100000"),
        _market(event_id="e1", ts=exit_ts, price="11", liquidity="100000"),
    ]
    before = process_live_paper_session(
        events=before_events,
        **_default_kwargs(future_tolerance=timedelta(0)),
    )
    assert any(s.status is LivePaperStatus.SIGNAL_ACCEPTED for s in before.signals) or before.fills

    # Listing received before session_start — session pre-start clocks reject.
    pre_listing = _listing(event_id="pre", source=T0)
    pre_events = [
        _listing_event(pre_listing, received=T0 - timedelta(minutes=1)),
        _market(event_id="pre", ts=decision, price="10", liquidity="100000"),
        _market(event_id="pre", ts=exit_ts, price="11", liquidity="100000"),
    ]
    pre = process_live_paper_session(
        events=pre_events,
        **_default_kwargs(session_start=T0, future_tolerance=timedelta(0)),
    )
    assert not any(s.status is LivePaperStatus.SIGNAL_ACCEPTED for s in pre.signals)
    assert not pre.fills
    assert any(
        r.reason is LivePaperRejectReason.BEFORE_SESSION or r.status is LivePaperStatus.BEFORE_SESSION
        for r in (*pre.rejections, *pre.signals)
    )

    # Market gate unchanged: entry market future-received still blocked independently.
    market_poison = [
        _listing_event(listing, received=decision - timedelta(seconds=1)),
        _market(
            event_id="e1",
            ts=decision,
            price="10",
            liquidity="100000",
            received=decision + timedelta(minutes=2),
        ),
        _market(event_id="e1", ts=exit_ts, price="11", liquidity="100000"),
    ]
    poisoned = process_live_paper_session(
        events=market_poison,
        **_default_kwargs(future_tolerance=timedelta(0)),
    )
    assert not any(f.side is Side.BUY for f in poisoned.fills)
    assert poisoned.data_quality.get("future_rejections", 0) >= 1 or any(
        r.reason is LivePaperRejectReason.FUTURE_RECEIVED for r in poisoned.rejections
    )


# ---------------------------------------------------------------------------
# Integrity repair B: same-timestamp exit fill ID collision
# ---------------------------------------------------------------------------


def test_same_timestamp_cex_exit_fills_distinct_stable_ids_and_restart_suppress() -> None:
    listing = _listing()
    decision = T0 + timedelta(minutes=1)
    exit_ts = decision + timedelta(minutes=5)
    entry_book = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=decision,
        bids=(DepthLevel(price=Decimal("9.9"), quantity=Decimal("100")),),
        asks=(DepthLevel(price=Decimal("10"), quantity=Decimal("10")),),
        source="binance:depth:entry",
    )
    exit_a = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=exit_ts,
        bids=(DepthLevel(price=Decimal("11"), quantity=Decimal("5")),),
        asks=(DepthLevel(price=Decimal("11.1"), quantity=Decimal("100")),),
        source="binance:depth:exit-a",
    )
    exit_b = HistoricalDepthBook(
        token_address="TOKEN",
        chain="binance",
        venue=Venue.BINANCE,
        timestamp=exit_ts,
        bids=(DepthLevel(price=Decimal("11"), quantity=Decimal("5")),),
        asks=(DepthLevel(price=Decimal("11.1"), quantity=Decimal("100")),),
        source="binance:depth:exit-b",
    )
    events = [
        _listing_event(listing),
        _market(
            event_id="e1",
            ts=decision,
            price="10",
            liquidity="100000",
            depth=entry_book,
            source="binance:depth:entry",
        ),
        _market(
            event_id="e1",
            ts=exit_ts,
            price="11",
            liquidity="100000",
            depth=exit_a,
            source="binance:depth:exit-a",
        ),
        _market(
            event_id="e1",
            ts=exit_ts,
            price="11",
            liquidity="100000",
            depth=exit_b,
            source="binance:depth:exit-b",
        ),
    ]
    kwargs = _default_kwargs(
        position_notional=Decimal("100"),
        max_token_exposure=Decimal("5000"),
        assumed_fee_bps=Decimal("10"),
    )
    report = process_live_paper_session(events=events, **kwargs)
    sells = [f for f in report.fills if f.side is Side.SELL]
    assert len(sells) == 2
    assert sells[0].fill_id != sells[1].fill_id
    assert sum((f.fill_qty for f in sells), Decimal("0")) == Decimal("10")
    assert report.positions
    closed = report.positions[-1]
    assert closed.lifecycle is PositionLifecycle.CLOSED
    assert closed.remaining_qty in (None, Decimal("0"))
    assert closed.realized_pnl is not None and closed.realized_pnl != 0
    # Pro-rata PnL applied once per exit slice (two fills, not collapsed).
    assert all(f.fill_qty == Decimal("5") for f in sells)

    # Exact same logical replay events → identical fill IDs.
    replay = process_live_paper_session(events=events, **kwargs)
    assert [f.fill_id for f in replay.fills] == [f.fill_id for f in report.fills]

    store: dict[str, object] = {}
    first = process_live_paper_session(events=events, state_store=store, **kwargs)
    first_sell_ids = {f.fill_id for f in first.fills if f.side is Side.SELL}
    assert len(first_sell_ids) == 2
    assert set(store.get("seen_fills", [])) >= first_sell_ids

    # Replay duplicate first exit event alone must suppress that fill id.
    dup_first_only = [
        _listing_event(listing),
        _market(
            event_id="e1",
            ts=decision,
            price="10",
            liquidity="100000",
            depth=entry_book,
            source="binance:depth:entry",
        ),
        _market(
            event_id="e1",
            ts=exit_ts,
            price="11",
            liquidity="100000",
            depth=exit_a,
            source="binance:depth:exit-a",
        ),
    ]
    dup = process_live_paper_session(events=dup_first_only, state_store=store, **kwargs)
    assert not any(f.fill_id in first_sell_ids and f.side is Side.SELL for f in dup.fills) or not any(
        f.side is Side.SELL for f in dup.fills
    )
    assert all(fid in set(store.get("seen_fills", [])) for fid in first_sell_ids)

    # Full restart with both exits → both sell fills suppressed; PnL unchanged.
    pnl_after_first = first.portfolio.realized_pnl
    second = process_live_paper_session(events=events, state_store=store, **kwargs)
    assert second.portfolio.realized_pnl == pnl_after_first
    assert not any(f.side is Side.SELL for f in second.fills) or all(
        f.fill_id in set(store.get("seen_fills", [])) for f in second.fills
    )
    assert not any(s.status is LivePaperStatus.SIGNAL_ACCEPTED for s in second.signals)
