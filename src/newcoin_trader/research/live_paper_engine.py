"""Pure Phase 6 live-paper engine (no I/O, no HTTP, no PaperBroker, no orders)."""

from __future__ import annotations

from collections.abc import Iterator, MutableMapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal, DecimalException
from hashlib import sha256
from typing import Any

from newcoin_trader.domain.enums import Side, Venue
from newcoin_trader.domain.executable_backtest import (
    ExecutableBacktestStatus,
    ExecutionMarketObservation,
    FrozenCandidateIdentity,
)
from newcoin_trader.domain.feature_research import CandidateRule, FeatureMarketInput
from newcoin_trader.domain.live_paper import (
    DISCLAIMER,
    WARNING_PAPER_ONLY,
    FreshnessDecision,
    LivePaperRejectReason,
    LivePaperReport,
    LivePaperSessionMeta,
    LivePaperStatus,
    PaperFillRecord,
    PaperPositionRecord,
    PaperSignalRecord,
    PortfolioSnapshot,
    PositionLifecycle,
    ReplayMarketEvent,
)
from newcoin_trader.domain.numeric import require_finite_decimal
from newcoin_trader.domain.types import require_utc
from newcoin_trader.errors import ConfigError, ResearchError
from newcoin_trader.research.event_study_config import format_duration
from newcoin_trader.research.event_study_run import git_identity
from newcoin_trader.research.executable_backtest_engine import (
    simulate_cex_depth_fill,
    simulate_dex_liquidity_fill,
    simulate_modeled_price_fill,
)
from newcoin_trader.research.feature_research_config import DEFAULT_FEATURE_WINDOWS
from newcoin_trader.research.feature_research_features import build_decision_feature_record
from newcoin_trader.research.feature_research_rules import evaluate_rule
from newcoin_trader.research.live_paper_config import (
    DEFAULT_DAILY_LOSS_LIMIT,
    DEFAULT_DECISION_DELAY,
    DEFAULT_FRESHNESS_MAX_AGE,
    DEFAULT_FUTURE_TOLERANCE,
    DEFAULT_MAX_IMPACT_SLIPPAGE,
    DEFAULT_MAX_OPEN_POSITIONS,
    DEFAULT_MAX_PARTICIPATION,
    DEFAULT_MAX_TOKEN_EXPOSURE,
    DEFAULT_MAX_VENUE_EXPOSURE,
    DEFAULT_MIN_LIQUIDITY,
    DEFAULT_SESSION_LOSS_LIMIT,
    ELIGIBILITY_RULES,
    validate_live_paper_bounds,
)

__all__ = [
    "BoundedEventQueue",
    "PortfolioLedger",
    "check_freshness",
    "process_live_paper_session",
    "require_finite_controlled",
    "simulate_cex_depth_fill",
    "simulate_dex_liquidity_fill",
    "simulate_modeled_price_fill",
    "transition_position",
]


def require_finite_controlled(value: Decimal, *, name: str) -> Decimal:
    return require_finite_decimal(value, name=name)


def check_freshness(
    *,
    source_timestamp: datetime,
    received_timestamp: datetime,
    decision_time: datetime,
    max_age: timedelta,
    future_tolerance: timedelta,
) -> FreshnessDecision:
    source = require_utc(source_timestamp)
    received = require_utc(received_timestamp)
    decision = require_utc(decision_time)
    if source > decision + future_tolerance:
        return FreshnessDecision(
            accepted=False,
            reason=LivePaperRejectReason.FUTURE_SOURCE,
            detail="source_timestamp after decision_time beyond tolerance",
        )
    if received > decision + future_tolerance:
        return FreshnessDecision(
            accepted=False,
            reason=LivePaperRejectReason.FUTURE_RECEIVED,
            detail="received_timestamp after decision_time beyond tolerance",
        )
    if decision - source > max_age:
        return FreshnessDecision(
            accepted=False,
            reason=LivePaperRejectReason.STALE_SOURCE,
            detail="source_timestamp older than freshness max_age",
        )
    return FreshnessDecision(accepted=True)


_TRANSITIONS: dict[tuple[PositionLifecycle, str], PositionLifecycle] = {
    (PositionLifecycle.PENDING, "open"): PositionLifecycle.OPEN,
    (PositionLifecycle.OPEN, "closing"): PositionLifecycle.CLOSING,
    (PositionLifecycle.OPEN, "close"): PositionLifecycle.CLOSED,
    (PositionLifecycle.OPEN, "fail_exit"): PositionLifecycle.FAILED_EXIT,
    (PositionLifecycle.CLOSING, "closing"): PositionLifecycle.CLOSING,
    (PositionLifecycle.CLOSING, "close"): PositionLifecycle.CLOSED,
    (PositionLifecycle.CLOSING, "fail_exit"): PositionLifecycle.FAILED_EXIT,
}


def transition_position(current: PositionLifecycle, action: str) -> PositionLifecycle:
    nxt = _TRANSITIONS.get((current, action))
    if nxt is None:
        raise ValueError(f"invalid position transition {current.value!r} --{action}->")
    return nxt


@dataclass
class QueuePushResult:
    accepted: bool
    reason: LivePaperRejectReason | None = None


@dataclass
class BoundedEventQueue:
    capacity: int
    _items: list[ReplayMarketEvent] = field(default_factory=list)
    overflow_count: int = 0

    def push(self, event: ReplayMarketEvent) -> QueuePushResult:
        if len(self._items) >= self.capacity:
            self.overflow_count += 1
            return QueuePushResult(accepted=False, reason=LivePaperRejectReason.QUEUE_OVERFLOW)
        self._items.append(event)
        return QueuePushResult(accepted=True)

    def __iter__(self) -> Iterator[ReplayMarketEvent]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)


@dataclass
class PortfolioLedger:
    starting_cash: Decimal
    cash: Decimal = field(init=False)
    realized_pnl: Decimal = field(default_factory=lambda: Decimal("0"))
    unrealized_pnl: Decimal = field(default_factory=lambda: Decimal("0"))
    peak_equity: Decimal = field(init=False)
    open_count: int = 0
    failed_count: int = 0
    token_exposure: dict[str, Decimal] = field(default_factory=dict)
    venue_exposure: dict[str, Decimal] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.cash = require_finite_controlled(self.starting_cash, name="starting_cash")
        self.peak_equity = self.cash

    @property
    def equity(self) -> Decimal:
        return self.cash + self.unrealized_pnl

    @property
    def drawdown(self) -> Decimal:
        eq = self.equity
        if eq > self.peak_equity:
            self.peak_equity = eq
        if self.peak_equity <= 0:
            return Decimal("0")
        dd = (self.peak_equity - eq) / self.peak_equity
        return dd if dd.is_finite() and dd > 0 else Decimal("0")

    def mark_unrealized(self, value: Decimal) -> None:
        self.unrealized_pnl = require_finite_controlled(value, name="unrealized_pnl")
        if self.equity > self.peak_equity:
            self.peak_equity = self.equity

    def apply_entry(self, *, notional: Decimal, fee: Decimal, token: str = "", venue: str = "") -> None:
        cost = require_finite_controlled(notional + fee, name="entry_cost")
        self.cash = require_finite_controlled(self.cash - cost, name="cash")
        self.open_count += 1
        if token:
            self.token_exposure[token] = self.token_exposure.get(token, Decimal("0")) + notional
        if venue:
            self.venue_exposure[venue] = self.venue_exposure.get(venue, Decimal("0")) + notional
        if self.equity > self.peak_equity:
            self.peak_equity = self.equity

    def apply_exit(
        self,
        *,
        proceeds: Decimal,
        fee: Decimal,
        cost_basis: Decimal,
        token: str = "",
        venue: str = "",
        close_position: bool = True,
    ) -> Decimal:
        net = require_finite_controlled(proceeds - fee, name="exit_net")
        pnl = require_finite_controlled(net - cost_basis, name="realized_pnl_delta")
        self.cash = require_finite_controlled(self.cash + net, name="cash")
        self.realized_pnl = require_finite_controlled(self.realized_pnl + pnl, name="realized_pnl")
        if token and token in self.token_exposure:
            self.token_exposure[token] = max(Decimal("0"), self.token_exposure[token] - cost_basis)
        if venue and venue in self.venue_exposure:
            self.venue_exposure[venue] = max(Decimal("0"), self.venue_exposure[venue] - cost_basis)
        if close_position:
            self.unrealized_pnl = Decimal("0")
            self.open_count = max(0, self.open_count - 1)
        if self.equity > self.peak_equity:
            self.peak_equity = self.equity
        return pnl

    def snapshot(self) -> PortfolioSnapshot:
        return PortfolioSnapshot(
            cash=self.cash,
            equity=self.equity,
            realized_pnl=self.realized_pnl,
            unrealized_pnl=self.unrealized_pnl,
            drawdown=self.drawdown,
            open_positions=self.open_count,
            failed_positions=self.failed_count,
            peak_equity=self.peak_equity,
        )


def _signal_id(session_id: str, event_id: str, rule_id: str, decision_time: datetime) -> str:
    material = f"{session_id}|{event_id}|{rule_id}|{decision_time.isoformat()}"
    return sha256(material.encode()).hexdigest()[:24]


def _position_id(signal_id: str) -> str:
    return sha256(f"pos|{signal_id}".encode()).hexdigest()[:24]


def _fill_id(
    position_id: str,
    side: Side,
    fill_time: datetime,
    *,
    market_event_id: str,
    source: str,
    ordinal: int = 0,
) -> str:
    """Deterministic fill identity from position/side/time plus stable market discriminators."""
    material = f"{position_id}|{side.value}|{fill_time.isoformat()}|{market_event_id}|{source}"
    if ordinal:
        material = f"{material}|{ordinal}"
    return sha256(material.encode()).hexdigest()[:24]


def _config_id(
    *,
    venue: str,
    duration: timedelta,
    max_events: int,
    max_signals: int,
    max_trades: int,
    queue_capacity: int,
    starting_cash: Decimal,
    rule_id: str,
    phase4_config_id: str,
) -> str:
    payload = (
        f"{venue}|{format_duration(duration)}|{max_events}|{max_signals}|{max_trades}|"
        f"{queue_capacity}|{starting_cash}|{rule_id}|{phase4_config_id}|"
        f"{'|'.join(ELIGIBILITY_RULES)}"
    )
    return sha256(payload.encode()).hexdigest()[:16]


def _session_id(config_id: str, session_start: datetime) -> str:
    return sha256(f"{config_id}|{session_start.isoformat()}".encode()).hexdigest()[:32]


def _rule_matches(identity: FrozenCandidateIdentity, record: Any) -> bool:
    rule = CandidateRule(
        rule_id=identity.rule_id,
        conditions=identity.conditions,
        human_readable=identity.human_readable,
        selected=True,
    )
    return len(evaluate_rule(rule, (record,))) == 1


def _market_to_feature_input(event: ReplayMarketEvent) -> FeatureMarketInput | None:
    if event.price is None:
        return None
    try:
        price = require_finite_controlled(event.price, name="price")
        liq = require_finite_controlled(event.liquidity, name="liquidity") if event.liquidity is not None else None
        vol = require_finite_controlled(event.volume, name="volume") if event.volume is not None else None
    except ConfigError:
        return None
    return FeatureMarketInput(
        token_address=event.token_address,
        chain=event.chain,
        venue=event.venue,
        timestamp=event.source_timestamp,
        price=price,
        volume=vol,
        liquidity=liq,
        resolution=event.resolution,
        source=event.source,
        provenance=dict(event.provenance),
    )


def _to_execution_obs(event: ReplayMarketEvent) -> ExecutionMarketObservation | None:
    if event.price is None:
        return None
    return ExecutionMarketObservation(
        token_address=event.token_address,
        chain=event.chain,
        venue=event.venue,
        timestamp=event.source_timestamp,
        price=event.price,
        liquidity=event.liquidity,
        volume=event.volume,
        resolution=event.resolution,
        source=event.source,
        provenance=dict(event.provenance),
    )


def _impact_frac(fill: Any) -> Decimal:
    try:
        if fill.notional <= 0:
            return Decimal("0")
        return Decimal(fill.impact_cost + fill.slippage_cost) / Decimal(fill.notional)
    except (DecimalException, ZeroDivisionError):
        return Decimal("Infinity")


def _evaluate_entry_risk(
    *,
    ledger: PortfolioLedger,
    notional: Decimal,
    token: str,
    venue: str,
    liquidity: Decimal | None,
    min_liquidity: Decimal,
    max_open: int,
    max_token: Decimal,
    max_venue: Decimal,
    session_loss_limit: Decimal,
    daily_loss_limit: Decimal,
) -> LivePaperRejectReason | None:
    if ledger.realized_pnl <= -session_loss_limit:
        return LivePaperRejectReason.SESSION_LOSS_HALT
    if ledger.realized_pnl <= -daily_loss_limit:
        return LivePaperRejectReason.DAILY_LOSS_HALT
    if ledger.cash < notional:
        return LivePaperRejectReason.INSUFFICIENT_CASH
    if ledger.open_count >= max_open:
        return LivePaperRejectReason.MAX_OPEN_POSITIONS
    if ledger.token_exposure.get(token, Decimal("0")) + notional > max_token:
        return LivePaperRejectReason.TOKEN_EXPOSURE
    if ledger.venue_exposure.get(venue, Decimal("0")) + notional > max_venue:
        return LivePaperRejectReason.VENUE_EXPOSURE
    if liquidity is None or liquidity < min_liquidity:
        return LivePaperRejectReason.MIN_LIQUIDITY
    return None


def _simulate_entry(
    *,
    market: ReplayMarketEvent,
    venue: Venue,
    position_notional: Decimal,
    max_participation: Decimal,
    assumed_fee_bps: Decimal,
    request_time: datetime,
    fill_time: datetime,
) -> Any:
    if market.depth is not None:
        try:
            mid = market.price or market.depth.asks[0].price
            qty = position_notional / mid
        except (DecimalException, IndexError, TypeError):
            return None
        return simulate_cex_depth_fill(
            book=market.depth,
            side=Side.BUY,
            requested_qty=qty,
            assumed_fee_bps=assumed_fee_bps,
            request_time=request_time,
            fill_time=fill_time,
        )
    obs = _to_execution_obs(market)
    if obs is None:
        return None
    if venue in {Venue.RAYDIUM, Venue.GECKO}:
        return simulate_dex_liquidity_fill(
            observation=obs,
            side=Side.BUY,
            position_notional=position_notional,
            max_participation=max_participation,
            assumed_fee_bps=assumed_fee_bps,
            request_time=request_time,
            fill_time=fill_time,
        )
    return simulate_modeled_price_fill(
        observation=obs,
        side=Side.BUY,
        position_notional=position_notional,
        max_participation=max_participation,
        assumed_fee_bps=assumed_fee_bps,
        request_time=request_time,
        fill_time=fill_time,
    )


def _simulate_exit(
    *,
    market: ReplayMarketEvent,
    venue: Venue,
    qty: Decimal,
    max_participation: Decimal,
    assumed_fee_bps: Decimal,
    request_time: datetime,
    fill_time: datetime,
) -> Any:
    if market.depth is not None:
        return simulate_cex_depth_fill(
            book=market.depth,
            side=Side.SELL,
            requested_qty=qty,
            assumed_fee_bps=assumed_fee_bps,
            request_time=request_time,
            fill_time=fill_time,
        )
    obs = _to_execution_obs(market)
    if obs is None or market.price is None:
        return None
    try:
        notional = qty * market.price
    except DecimalException:
        return None
    if venue in {Venue.RAYDIUM, Venue.GECKO}:
        return simulate_dex_liquidity_fill(
            observation=obs,
            side=Side.SELL,
            position_notional=notional,
            max_participation=max_participation,
            assumed_fee_bps=assumed_fee_bps,
            request_time=request_time,
            fill_time=fill_time,
        )
    return simulate_modeled_price_fill(
        observation=obs,
        side=Side.SELL,
        position_notional=notional,
        max_participation=max_participation,
        assumed_fee_bps=assumed_fee_bps,
        request_time=request_time,
        fill_time=fill_time,
    )


def process_live_paper_session(
    *,
    events: Sequence[ReplayMarketEvent],
    venue: Venue,
    session_start: datetime,
    duration: timedelta,
    max_events: int,
    max_signals: int,
    max_trades: int,
    queue_capacity: int,
    starting_cash: Decimal,
    position_notional: Decimal,
    holding_period: timedelta,
    identity: FrozenCandidateIdentity,
    max_open_positions: int = DEFAULT_MAX_OPEN_POSITIONS,
    max_token_exposure: Decimal = DEFAULT_MAX_TOKEN_EXPOSURE,
    max_venue_exposure: Decimal = DEFAULT_MAX_VENUE_EXPOSURE,
    min_liquidity: Decimal = DEFAULT_MIN_LIQUIDITY,
    max_participation: Decimal = DEFAULT_MAX_PARTICIPATION,
    max_impact_slippage: Decimal = DEFAULT_MAX_IMPACT_SLIPPAGE,
    daily_loss_limit: Decimal = DEFAULT_DAILY_LOSS_LIMIT,
    session_loss_limit: Decimal = DEFAULT_SESSION_LOSS_LIMIT,
    freshness_max_age: timedelta = DEFAULT_FRESHNESS_MAX_AGE,
    future_tolerance: timedelta = DEFAULT_FUTURE_TOLERANCE,
    assumed_fee_bps: Decimal = Decimal("10"),
    decision_delay: timedelta = DEFAULT_DECISION_DELAY,
    state_store: MutableMapping[str, Any] | None = None,
    phase4_gross_return: Decimal | None = None,
    phase5_historical_net: Decimal | None = None,
) -> LivePaperReport:
    """Process a bounded injected replay batch into a deterministic paper session report."""
    validate_live_paper_bounds(
        duration=duration,
        max_events=max_events,
        max_signals=max_signals,
        max_trades=max_trades,
        queue_capacity=queue_capacity,
        starting_cash=starting_cash,
        position_notional=position_notional,
        holding_period=holding_period,
        max_participation=max_participation,
        assumed_fee_bps=assumed_fee_bps,
    )
    start = require_utc(session_start)
    end = start + duration
    cfg = _config_id(
        venue=venue.value,
        duration=duration,
        max_events=max_events,
        max_signals=max_signals,
        max_trades=max_trades,
        queue_capacity=queue_capacity,
        starting_cash=starting_cash,
        rule_id=identity.rule_id,
        phase4_config_id=identity.phase4_config_id,
    )
    session_id = _session_id(cfg, start)
    store = state_store if state_store is not None else {}
    seen_signals: set[str] = set(store.get("seen_signals", ()))
    seen_fills: set[str] = set(store.get("seen_fills", ()))
    prior_realized = Decimal(str(store.get("realized_pnl", "0")))

    queue = BoundedEventQueue(capacity=queue_capacity)
    ordered = sorted(
        events,
        key=lambda e: (e.source_timestamp, e.received_timestamp, e.kind, e.event_id, e.source),
    )
    supplied_event_count = len(ordered)
    admitted_event_count = 0
    max_events_rejected_count = 0
    max_event_rejections: list[PaperSignalRecord] = []

    for event in ordered:
        if admitted_event_count >= max_events:
            max_events_rejected_count += 1
            max_event_rejections.append(
                PaperSignalRecord(
                    signal_id=sha256(
                        f"max_events|{session_id}|{event.event_id}|{event.kind}|"
                        f"{event.source_timestamp.isoformat()}|{event.source}".encode()
                    ).hexdigest()[:24],
                    session_id=session_id,
                    event_id=event.event_id,
                    rule_id=identity.rule_id,
                    phase4_config_id=identity.phase4_config_id,
                    split_label=identity.split_label,
                    fold_index=identity.fold_index,
                    decision_time=event.source_timestamp if start <= event.source_timestamp <= end else start,
                    status=LivePaperStatus.REJECTED,
                    reason=LivePaperRejectReason.MAX_EVENTS,
                    source_timestamp=event.source_timestamp,
                    received_timestamp=event.received_timestamp,
                    provenance={"reject": LivePaperRejectReason.MAX_EVENTS.value, "kind": event.kind},
                )
            )
            continue
        pushed = queue.push(event)
        if pushed.accepted:
            admitted_event_count += 1

    listings: dict[str, ReplayMarketEvent] = {}
    markets: dict[str, list[ReplayMarketEvent]] = {}
    for event in queue:
        if event.kind == "listing" and event.listing is not None:
            # Preserve outer ReplayMarketEvent clocks/identity/provenance (not listing alone).
            listings[event.event_id] = event
        elif event.kind == "market":
            markets.setdefault(event.event_id, []).append(event)

    ledger = PortfolioLedger(starting_cash=starting_cash)
    if prior_realized != 0:
        ledger.realized_pnl = prior_realized

    signals: list[PaperSignalRecord] = []
    rejections: list[PaperSignalRecord] = list(max_event_rejections)
    signals.extend(max_event_rejections)
    fills: list[PaperFillRecord] = []
    positions: list[PaperPositionRecord] = []
    halted = False
    halt_reason: LivePaperRejectReason | None = None
    stale_count = 0
    future_count = 0
    duplicate_count = 0
    position_count = 0

    def _note_freshness(decision: FreshnessDecision) -> None:
        nonlocal stale_count, future_count
        if decision.reason is LivePaperRejectReason.STALE_SOURCE:
            stale_count += 1
        if decision.reason in {
            LivePaperRejectReason.FUTURE_SOURCE,
            LivePaperRejectReason.FUTURE_RECEIVED,
        }:
            future_count += 1

    def _reject(
        *,
        signal_id: str,
        event_id: str,
        decision_time: datetime,
        reason: LivePaperRejectReason,
        status: LivePaperStatus = LivePaperStatus.REJECTED,
        source_ts: datetime | None = None,
        recv_ts: datetime | None = None,
    ) -> None:
        rec = PaperSignalRecord(
            signal_id=signal_id,
            session_id=session_id,
            event_id=event_id,
            rule_id=identity.rule_id,
            phase4_config_id=identity.phase4_config_id,
            split_label=identity.split_label,
            fold_index=identity.fold_index,
            decision_time=decision_time,
            status=status,
            reason=reason,
            source_timestamp=source_ts,
            received_timestamp=recv_ts,
            provenance={"reject": reason.value},
        )
        rejections.append(rec)
        signals.append(rec)

    def _market_usable(m: ReplayMarketEvent, *, as_of: datetime) -> bool:
        if m.source_timestamp < start or m.source_timestamp > end:
            return False
        if m.received_timestamp < start or m.received_timestamp > end:
            return False
        if m.source_timestamp > as_of:
            return False
        freshness = check_freshness(
            source_timestamp=m.source_timestamp,
            received_timestamp=m.received_timestamp,
            decision_time=as_of,
            max_age=freshness_max_age,
            future_tolerance=future_tolerance,
        )
        if not freshness.accepted:
            _note_freshness(freshness)
            return False
        return True

    for event_id, listing_event in sorted(
        listings.items(),
        key=lambda kv: (kv[1].source_timestamp, kv[1].received_timestamp, kv[0]),
    ):
        listing = listing_event.listing
        if listing is None:
            continue
        if listing.venue is not venue:
            continue
        if halted:
            break
        if len([s for s in signals if s.status is LivePaperStatus.SIGNAL_ACCEPTED]) >= max_signals:
            halt_reason = LivePaperRejectReason.MAX_SIGNALS
            halted = True
            break
        if position_count >= max_trades:
            halt_reason = LivePaperRejectReason.MAX_TRADES
            halted = True
            break

        decision_time = listing.source_event_time + decision_delay
        sig_id = _signal_id(session_id, event_id, identity.rule_id, decision_time)

        if sig_id in seen_signals:
            duplicate_count += 1
            _reject(
                signal_id=sig_id,
                event_id=event_id,
                decision_time=decision_time,
                reason=LivePaperRejectReason.DUPLICATE_SIGNAL,
                status=LivePaperStatus.DUPLICATE,
            )
            continue

        if (
            decision_time < start
            or listing.source_event_time < start
            or listing_event.source_timestamp < start
            or listing_event.received_timestamp < start
        ):
            _reject(
                signal_id=sig_id,
                event_id=event_id,
                decision_time=decision_time if decision_time >= start else start,
                reason=LivePaperRejectReason.BEFORE_SESSION,
                status=LivePaperStatus.BEFORE_SESSION,
                source_ts=listing_event.source_timestamp,
                recv_ts=listing_event.received_timestamp,
            )
            continue

        # Listing received/source clocks gate before availability / features / rule / entry.
        listing_fresh = check_freshness(
            source_timestamp=listing_event.source_timestamp,
            received_timestamp=listing_event.received_timestamp,
            decision_time=decision_time,
            max_age=freshness_max_age,
            future_tolerance=future_tolerance,
        )
        if not listing_fresh.accepted:
            _note_freshness(listing_fresh)
            _reject(
                signal_id=sig_id,
                event_id=event_id,
                decision_time=decision_time,
                reason=listing_fresh.reason or LivePaperRejectReason.FUTURE_RECEIVED,
                source_ts=listing_event.source_timestamp,
                recv_ts=listing_event.received_timestamp,
            )
            continue

        # Session upper bound on listing clocks (same window as markets).
        if listing_event.source_timestamp > end or listing_event.received_timestamp > end:
            _reject(
                signal_id=sig_id,
                event_id=event_id,
                decision_time=decision_time,
                reason=LivePaperRejectReason.SESSION_EXPIRED,
                source_ts=listing_event.source_timestamp,
                recv_ts=listing_event.received_timestamp,
            )
            continue

        if decision_time < listing.decision_available_time:
            _reject(
                signal_id=sig_id,
                event_id=event_id,
                decision_time=decision_time,
                reason=LivePaperRejectReason.NOT_DECISION_AVAILABLE,
                status=LivePaperStatus.NOT_DECISION_AVAILABLE,
                source_ts=listing_event.source_timestamp,
                recv_ts=listing_event.received_timestamp,
            )
            continue

        if decision_time > end:
            _reject(
                signal_id=sig_id,
                event_id=event_id,
                decision_time=decision_time,
                reason=LivePaperRejectReason.SESSION_EXPIRED,
                source_ts=listing_event.source_timestamp,
                recv_ts=listing_event.received_timestamp,
            )
            continue

        event_markets = markets.get(event_id, [])
        feature_inputs: list[FeatureMarketInput] = []
        fresh_markets: list[ReplayMarketEvent] = []
        for m in event_markets:
            if not _market_usable(m, as_of=decision_time):
                continue
            fi = _market_to_feature_input(m)
            if fi is not None:
                feature_inputs.append(fi)
                fresh_markets.append(m)

        try:
            record = build_decision_feature_record(
                listing,
                feature_inputs,
                decision_time=decision_time,
                windows=DEFAULT_FEATURE_WINDOWS,
                config_id=identity.phase4_config_id,
            )
        except ResearchError:
            _reject(
                signal_id=sig_id,
                event_id=event_id,
                decision_time=decision_time,
                reason=LivePaperRejectReason.NOT_DECISION_AVAILABLE,
                status=LivePaperStatus.NOT_DECISION_AVAILABLE,
            )
            continue

        if not _rule_matches(identity, record):
            _reject(
                signal_id=sig_id,
                event_id=event_id,
                decision_time=decision_time,
                reason=LivePaperRejectReason.RULE_NOT_MATCHED,
                status=LivePaperStatus.RULE_NOT_MATCHED,
            )
            continue

        entry_market = None
        for m in sorted(fresh_markets, key=lambda x: x.source_timestamp, reverse=True):
            if m.source_timestamp <= decision_time and m.price is not None:
                entry_market = m
                break
        if entry_market is None:
            _reject(
                signal_id=sig_id,
                event_id=event_id,
                decision_time=decision_time,
                reason=LivePaperRejectReason.INVALID_MARKET_DATA,
            )
            continue

        risk = _evaluate_entry_risk(
            ledger=ledger,
            notional=position_notional,
            token=listing.token_address,
            venue=venue.value,
            liquidity=entry_market.liquidity,
            min_liquidity=min_liquidity,
            max_open=max_open_positions,
            max_token=max_token_exposure,
            max_venue=max_venue_exposure,
            session_loss_limit=session_loss_limit,
            daily_loss_limit=daily_loss_limit,
        )
        if risk is not None:
            if risk in {LivePaperRejectReason.SESSION_LOSS_HALT, LivePaperRejectReason.DAILY_LOSS_HALT}:
                halted = True
                halt_reason = risk
            _reject(
                signal_id=sig_id,
                event_id=event_id,
                decision_time=decision_time,
                reason=risk,
                source_ts=entry_market.source_timestamp,
                recv_ts=entry_market.received_timestamp,
            )
            continue

        fill_fresh = check_freshness(
            source_timestamp=entry_market.source_timestamp,
            received_timestamp=entry_market.received_timestamp,
            decision_time=decision_time,
            max_age=freshness_max_age,
            future_tolerance=future_tolerance,
        )
        if not fill_fresh.accepted:
            _note_freshness(fill_fresh)
            _reject(
                signal_id=sig_id,
                event_id=event_id,
                decision_time=decision_time,
                reason=fill_fresh.reason or LivePaperRejectReason.STALE_SOURCE,
                source_ts=entry_market.source_timestamp,
                recv_ts=entry_market.received_timestamp,
            )
            continue

        entry_fill = _simulate_entry(
            market=entry_market,
            venue=venue,
            position_notional=position_notional,
            max_participation=max_participation,
            assumed_fee_bps=assumed_fee_bps,
            request_time=decision_time,
            fill_time=decision_time,
        )
        if entry_fill is None or entry_fill.fill_qty <= 0:
            _reject(
                signal_id=sig_id,
                event_id=event_id,
                decision_time=decision_time,
                reason=LivePaperRejectReason.INSUFFICIENT_LIQUIDITY,
                source_ts=entry_market.source_timestamp,
                recv_ts=entry_market.received_timestamp,
            )
            continue

        impact = _impact_frac(entry_fill)
        if impact.is_finite() and impact > max_impact_slippage:
            _reject(
                signal_id=sig_id,
                event_id=event_id,
                decision_time=decision_time,
                reason=LivePaperRejectReason.IMPACT_SLIPPAGE,
            )
            continue

        pos_id = _position_id(sig_id)
        entry_fill_id = _fill_id(
            pos_id,
            Side.BUY,
            decision_time,
            market_event_id=entry_market.event_id,
            source=entry_market.source,
        )
        if entry_fill_id in seen_fills:
            duplicate_count += 1
            continue

        entry_status = (
            LivePaperStatus.ENTRY_PARTIAL
            if entry_fill.status is ExecutableBacktestStatus.PARTIAL
            else LivePaperStatus.ENTRY_FILLED
        )
        if entry_fill.status in {
            ExecutableBacktestStatus.UNFILLED,
            ExecutableBacktestStatus.INSUFFICIENT_LIQUIDITY,
        }:
            _reject(
                signal_id=sig_id,
                event_id=event_id,
                decision_time=decision_time,
                reason=LivePaperRejectReason.INSUFFICIENT_LIQUIDITY,
            )
            continue

        accepted = PaperSignalRecord(
            signal_id=sig_id,
            session_id=session_id,
            event_id=event_id,
            rule_id=identity.rule_id,
            phase4_config_id=identity.phase4_config_id,
            split_label=identity.split_label,
            fold_index=identity.fold_index,
            decision_time=decision_time,
            status=LivePaperStatus.SIGNAL_ACCEPTED,
            source_timestamp=entry_market.source_timestamp,
            received_timestamp=entry_market.received_timestamp,
            provenance={"fill_mode": entry_fill.mode.value},
        )
        signals.append(accepted)
        seen_signals.add(sig_id)

        fills.append(
            PaperFillRecord.from_simulated(
                entry_fill,
                fill_id=entry_fill_id,
                session_id=session_id,
                signal_id=sig_id,
                position_id=pos_id,
                status=entry_status,
            )
        )
        seen_fills.add(entry_fill_id)
        ledger.apply_entry(
            notional=entry_fill.notional,
            fee=entry_fill.fee_cost,
            token=listing.token_address,
            venue=venue.value,
        )
        lifecycle = transition_position(PositionLifecycle.PENDING, "open")
        position_count += 1
        exit_deadline = decision_time + holding_period

        remaining_qty = entry_fill.fill_qty
        remaining_cost = entry_fill.notional
        realized_total = Decimal("0")
        exit_qty_total = Decimal("0")
        last_exit_price: Decimal | None = None
        last_exit_time: datetime | None = None
        any_exit = False

        if exit_deadline > end:
            _reject(
                signal_id=sig_id,
                event_id=event_id,
                decision_time=decision_time,
                reason=LivePaperRejectReason.SESSION_HORIZON,
                status=LivePaperStatus.SESSION_HORIZON,
            )
            lifecycle = transition_position(lifecycle, "fail_exit")
            ledger.failed_count += 1
            positions.append(
                PaperPositionRecord(
                    position_id=pos_id,
                    session_id=session_id,
                    signal_id=sig_id,
                    event_id=event_id,
                    token_address=listing.token_address,
                    venue=venue,
                    lifecycle=lifecycle,
                    side=Side.BUY,
                    entry_notional=entry_fill.notional,
                    entry_qty=entry_fill.fill_qty,
                    entry_price=entry_fill.fill_price,
                    remaining_qty=remaining_qty,
                    remaining_cost_basis=remaining_cost,
                    entry_time=decision_time,
                    holding_period=holding_period,
                )
            )
            continue

        for m in sorted(event_markets, key=lambda x: (x.source_timestamp, x.received_timestamp)):
            if remaining_qty <= 0:
                break
            if m.source_timestamp < exit_deadline:
                continue
            if m.source_timestamp > end:
                break
            if m.price is None:
                continue
            fill_as_of = m.source_timestamp
            if not _market_usable(m, as_of=fill_as_of):
                continue

            exit_fill = _simulate_exit(
                market=m,
                venue=venue,
                qty=remaining_qty,
                max_participation=max_participation,
                assumed_fee_bps=assumed_fee_bps,
                request_time=exit_deadline,
                fill_time=fill_as_of,
            )
            if exit_fill is None or exit_fill.fill_qty <= 0:
                continue

            sold_qty = min(exit_fill.fill_qty, remaining_qty)
            if sold_qty <= 0 or remaining_qty <= 0:
                continue
            scale = sold_qty / exit_fill.fill_qty if exit_fill.fill_qty > 0 else Decimal("0")
            proceeds = require_finite_controlled(exit_fill.notional * scale, name="exit_proceeds")
            fee = require_finite_controlled(exit_fill.fee_cost * scale, name="exit_fee")
            sold_cost = require_finite_controlled(
                remaining_cost * (sold_qty / remaining_qty),
                name="sold_cost_basis",
            )
            fully_closed = sold_qty >= remaining_qty
            exit_fill_id = _fill_id(
                pos_id,
                Side.SELL,
                fill_as_of,
                market_event_id=m.event_id,
                source=m.source,
            )
            if exit_fill_id in seen_fills:
                duplicate_count += 1
                continue

            if fully_closed and sold_qty == exit_fill.fill_qty:
                exit_status = LivePaperStatus.EXIT_FILLED
            else:
                exit_status = LivePaperStatus.EXIT_PARTIAL

            fills.append(
                PaperFillRecord(
                    fill_id=exit_fill_id,
                    session_id=session_id,
                    signal_id=sig_id,
                    position_id=pos_id,
                    side=Side.SELL,
                    status=exit_status,
                    mode=exit_fill.mode,
                    confidence=exit_fill.confidence,
                    request_time=exit_fill.request_time,
                    fill_time=exit_fill.fill_time,
                    requested_qty=remaining_qty,
                    fill_qty=sold_qty,
                    fill_price=exit_fill.fill_price,
                    notional=proceeds,
                    fee_cost=fee,
                    spread_cost=require_finite_controlled(exit_fill.spread_cost * scale, name="spread"),
                    slippage_cost=require_finite_controlled(exit_fill.slippage_cost * scale, name="slip"),
                    impact_cost=require_finite_controlled(exit_fill.impact_cost * scale, name="impact"),
                    label=exit_fill.label,
                    source=exit_fill.source,
                )
            )
            seen_fills.add(exit_fill_id)
            pnl = ledger.apply_exit(
                proceeds=proceeds,
                fee=fee,
                cost_basis=sold_cost,
                token=listing.token_address,
                venue=venue.value,
                close_position=fully_closed,
            )
            realized_total += pnl
            exit_qty_total += sold_qty
            remaining_qty -= sold_qty
            remaining_cost -= sold_cost
            last_exit_price = exit_fill.fill_price
            last_exit_time = fill_as_of
            any_exit = True
            if fully_closed:
                lifecycle = transition_position(lifecycle, "close")
            else:
                lifecycle = transition_position(lifecycle, "closing")
            if ledger.realized_pnl <= -session_loss_limit:
                halted = True
                halt_reason = LivePaperRejectReason.SESSION_LOSS_HALT
            elif ledger.realized_pnl <= -daily_loss_limit:
                halted = True
                halt_reason = LivePaperRejectReason.DAILY_LOSS_HALT

        if remaining_qty > 0 and not any_exit:
            lifecycle = transition_position(lifecycle, "fail_exit")
            ledger.failed_count += 1
            positions.append(
                PaperPositionRecord(
                    position_id=pos_id,
                    session_id=session_id,
                    signal_id=sig_id,
                    event_id=event_id,
                    token_address=listing.token_address,
                    venue=venue,
                    lifecycle=lifecycle,
                    side=Side.BUY,
                    entry_notional=entry_fill.notional,
                    entry_qty=entry_fill.fill_qty,
                    entry_price=entry_fill.fill_price,
                    remaining_qty=remaining_qty,
                    remaining_cost_basis=remaining_cost,
                    entry_time=decision_time,
                    holding_period=holding_period,
                )
            )
            continue

        unrealized = Decimal("0")
        if remaining_qty > 0 and last_exit_price is not None:
            mark = last_exit_price * remaining_qty
            unrealized = require_finite_controlled(mark - remaining_cost, name="unrealized_pnl")
            ledger.mark_unrealized(unrealized)

        positions.append(
            PaperPositionRecord(
                position_id=pos_id,
                session_id=session_id,
                signal_id=sig_id,
                event_id=event_id,
                token_address=listing.token_address,
                venue=venue,
                lifecycle=lifecycle,
                side=Side.BUY,
                entry_notional=entry_fill.notional,
                entry_qty=entry_fill.fill_qty,
                entry_price=entry_fill.fill_price,
                exit_qty=exit_qty_total if exit_qty_total > 0 else None,
                exit_price=last_exit_price,
                remaining_qty=remaining_qty if remaining_qty > 0 else Decimal("0"),
                remaining_cost_basis=remaining_cost if remaining_qty > 0 else Decimal("0"),
                realized_pnl=realized_total if any_exit else None,
                unrealized_pnl=unrealized if remaining_qty > 0 else Decimal("0"),
                entry_time=decision_time,
                exit_time=last_exit_time if remaining_qty <= 0 else None,
                holding_period=holding_period,
            )
        )

    store["seen_signals"] = sorted(seen_signals)
    store["seen_fills"] = sorted(seen_fills)
    store["realized_pnl"] = str(ledger.realized_pnl)

    accepted_signals = tuple(s for s in signals if s.status is LivePaperStatus.SIGNAL_ACCEPTED)
    comparison = None
    if phase4_gross_return is not None or phase5_historical_net is not None:
        comparison = {
            "phase4_gross_return": phase4_gross_return,
            "phase5_historical_net": phase5_historical_net,
            "phase6_paper_net": ledger.realized_pnl,
        }

    meta = LivePaperSessionMeta(
        session_id=session_id,
        config_id=cfg,
        venue=venue.value,
        session_start=start,
        session_end=end,
        duration=duration,
        max_events=max_events,
        max_signals=max_signals,
        max_trades=max_trades,
        queue_capacity=queue_capacity,
        starting_cash=starting_cash,
        event_count=admitted_event_count,
        signal_count=len(accepted_signals),
        trade_count=len(positions),
        fill_count=len(fills),
        supplied_event_count=supplied_event_count,
        admitted_event_count=admitted_event_count,
        max_events_rejected_count=max_events_rejected_count,
        overflow_count=queue.overflow_count,
        halted=halted,
        halt_reason=halt_reason,
        frozen_rule_id=identity.rule_id,
        phase4_config_id=identity.phase4_config_id,
        git_identity=git_identity(),
    )
    return LivePaperReport(
        meta=meta,
        signals=tuple(signals),
        rejections=tuple(rejections),
        fills=tuple(fills),
        positions=tuple(positions),
        portfolio=ledger.snapshot(),
        data_quality={
            "stale_rejections": stale_count,
            "future_rejections": future_count,
            "duplicate_events": duplicate_count,
            "queue_overflow": queue.overflow_count,
            "supplied_event_count": supplied_event_count,
            "admitted_event_count": admitted_event_count,
            "max_events_rejected_count": max_events_rejected_count,
            "fill_count": len(fills),
            "trade_count": len(positions),
            "disclaimer": DISCLAIMER,
            "warning": WARNING_PAPER_ONLY,
        },
        comparison=comparison,
        extras={"durable_state": dict(store)},
    )
