"""Phase 6 bounded live-paper domain (paper simulation only — never live orders)."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from newcoin_trader.domain.enums import Side, Venue
from newcoin_trader.domain.event_study import ObservationResolution, TokenListingEvent
from newcoin_trader.domain.executable_backtest import (
    ExecutionConfidence,
    HistoricalDepthBook,
    SimulatedFill,
    SimulatedFillMode,
)
from newcoin_trader.domain.types import require_utc

DISCLAIMER = "bounded_live_paper_simulation_not_live_trading_advice"
WARNING_PAPER_ONLY = "paper_only_no_orders_no_wallets_no_real_capital"
WARNING_MODELED = "fills_may_be_modeled_not_amm_exact_or_live_book"


class LivePaperStatus(StrEnum):
    SIGNAL_ACCEPTED = "signal_accepted"
    REJECTED = "rejected"
    NOT_DECISION_AVAILABLE = "not_decision_available"
    RULE_NOT_MATCHED = "rule_not_matched"
    ENTRY_FILLED = "entry_filled"
    ENTRY_PARTIAL = "entry_partial"
    ENTRY_UNFILLED = "entry_unfilled"
    EXIT_FILLED = "exit_filled"
    EXIT_PARTIAL = "exit_partial"
    EXIT_FAILED = "exit_failed"
    QUEUE_OVERFLOW = "queue_overflow"
    SESSION_HALTED = "session_halted"
    DUPLICATE = "duplicate"
    BEFORE_SESSION = "before_session"
    SESSION_HORIZON = "session_horizon"


class LivePaperRejectReason(StrEnum):
    STALE_SOURCE = "stale_source"
    FUTURE_SOURCE = "future_source"
    FUTURE_RECEIVED = "future_received"
    NOT_DECISION_AVAILABLE = "not_decision_available"
    RULE_NOT_MATCHED = "rule_not_matched"
    INSUFFICIENT_CASH = "insufficient_cash"
    MAX_OPEN_POSITIONS = "max_open_positions"
    TOKEN_EXPOSURE = "token_exposure"
    VENUE_EXPOSURE = "venue_exposure"
    MIN_LIQUIDITY = "min_liquidity"
    MAX_PARTICIPATION = "max_participation"
    IMPACT_SLIPPAGE = "impact_slippage"
    DAILY_LOSS_HALT = "daily_loss_halt"
    SESSION_LOSS_HALT = "session_loss_halt"
    DUPLICATE_SIGNAL = "duplicate_signal"
    INVALID_TRANSITION = "invalid_transition"
    NONFINITE_DECIMAL = "nonfinite_decimal"
    QUEUE_OVERFLOW = "queue_overflow"
    INVALID_MARKET_DATA = "invalid_market_data"
    INSUFFICIENT_LIQUIDITY = "insufficient_liquidity"
    MAX_EVENTS = "max_events"
    MAX_SIGNALS = "max_signals"
    MAX_TRADES = "max_trades"
    SESSION_EXPIRED = "session_expired"
    BEFORE_SESSION = "before_session"
    SESSION_HORIZON = "session_horizon"


class PositionLifecycle(StrEnum):
    PENDING = "pending"
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"
    FAILED_EXIT = "failed_exit"


class FailedExitReason(StrEnum):
    NO_USABLE_EXIT_CANDIDATE = "no_usable_exit_candidate"
    ALL_EXIT_CANDIDATES_REJECTED = "all_exit_candidates_rejected"
    ALL_EXIT_EXECUTION_ATTEMPTS_UNFILLED = "all_exit_execution_attempts_unfilled"
    MIXED_EXIT_FAILURES = "mixed_exit_failures"


class ExitAttemptAudit(BaseModel):
    """Observational record of one post-deadline exit candidate. No invented clocks or fills."""

    model_config = ConfigDict(frozen=True)

    event_id: str
    source: str
    source_timestamp: datetime
    received_timestamp: datetime
    price: Decimal | None = None
    requested_qty: Decimal
    market_usable: bool
    market_reject_reason: str | None = None
    depth_available: bool
    depth_source_timestamp: datetime | None = None
    depth_received_timestamp: datetime | None = None
    depth_pit_accepted: bool | None = None
    depth_pit_reason: str | None = None
    execution_mode: str
    attempted: bool
    fill_qty: Decimal | None = None
    fill_price: Decimal | None = None
    no_fill_reason: str | None = None
    outcome: str

    @field_validator(
        "source_timestamp",
        "received_timestamp",
        "depth_source_timestamp",
        "depth_received_timestamp",
    )
    @classmethod
    def _aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return require_utc(value)


class ExitAttemptDiagnostics(BaseModel):
    """Bounded failed-exit diagnostics retained on a position."""

    model_config = ConfigDict(frozen=True)

    exit_deadline: datetime
    failed_exit_reason: FailedExitReason | None = None
    attempts: tuple[ExitAttemptAudit, ...] = ()
    attempt_count_total: int
    attempt_count_retained: int
    truncated: bool
    last_candidate_clock: datetime | None = None
    last_reject_or_nofill_reason: str | None = None

    @field_validator("exit_deadline", "last_candidate_clock")
    @classmethod
    def _aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return require_utc(value)


class FreshnessDecision(BaseModel):
    model_config = ConfigDict(frozen=True)

    accepted: bool
    reason: LivePaperRejectReason | None = None
    detail: str | None = None


class ReplayMarketEvent(BaseModel):
    """Timestamped replay/feed event with source and received clocks."""

    model_config = ConfigDict(frozen=True)

    event_id: str
    kind: str  # listing | market
    venue: Venue
    token_address: str
    chain: str
    source_timestamp: datetime
    received_timestamp: datetime
    price: Decimal | None = None
    liquidity: Decimal | None = None
    volume: Decimal | None = None
    resolution: ObservationResolution = ObservationResolution.POINT
    source: str
    depth: HistoricalDepthBook | None = None
    listing: TokenListingEvent | None = None
    provenance: dict[str, str] = Field(default_factory=dict)

    @field_validator("source_timestamp", "received_timestamp")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class PaperSignalRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    signal_id: str
    session_id: str
    event_id: str
    rule_id: str
    phase4_config_id: str
    split_label: str
    fold_index: int | None
    decision_time: datetime
    status: LivePaperStatus
    reason: LivePaperRejectReason | None = None
    source_timestamp: datetime | None = None
    received_timestamp: datetime | None = None
    provenance: dict[str, str] = Field(default_factory=dict)

    @field_validator("decision_time", "source_timestamp", "received_timestamp")
    @classmethod
    def _aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return require_utc(value)


class PaperPositionRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    position_id: str
    session_id: str
    signal_id: str
    event_id: str
    token_address: str
    venue: Venue
    lifecycle: PositionLifecycle
    side: Side
    entry_notional: Decimal
    entry_qty: Decimal
    entry_price: Decimal
    exit_qty: Decimal | None = None
    exit_price: Decimal | None = None
    remaining_qty: Decimal | None = None
    remaining_cost_basis: Decimal | None = None
    realized_pnl: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    entry_time: datetime
    exit_time: datetime | None = None
    holding_period: timedelta
    label: str = WARNING_MODELED
    exit_diagnostics: ExitAttemptDiagnostics | None = None

    @field_validator("entry_time", "exit_time")
    @classmethod
    def _aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return require_utc(value)


class PaperFillRecord(BaseModel):
    """Auditable paper fill wrapping Phase 5 SimulatedFill fields."""

    model_config = ConfigDict(frozen=True)

    fill_id: str
    session_id: str
    signal_id: str
    position_id: str
    side: Side
    status: LivePaperStatus
    mode: SimulatedFillMode
    confidence: ExecutionConfidence
    request_time: datetime
    fill_time: datetime
    requested_qty: Decimal
    fill_qty: Decimal
    fill_price: Decimal
    notional: Decimal
    fee_cost: Decimal
    spread_cost: Decimal
    slippage_cost: Decimal
    impact_cost: Decimal
    label: str
    source: str | None = None

    @field_validator("request_time", "fill_time")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)

    @classmethod
    def from_simulated(
        cls,
        fill: SimulatedFill,
        *,
        fill_id: str,
        session_id: str,
        signal_id: str,
        position_id: str,
        status: LivePaperStatus,
    ) -> PaperFillRecord:
        return cls(
            fill_id=fill_id,
            session_id=session_id,
            signal_id=signal_id,
            position_id=position_id,
            side=fill.side,
            status=status,
            mode=fill.mode,
            confidence=fill.confidence,
            request_time=fill.request_time,
            fill_time=fill.fill_time,
            requested_qty=fill.requested_qty,
            fill_qty=fill.fill_qty,
            fill_price=fill.fill_price,
            notional=fill.notional,
            fee_cost=fill.fee_cost,
            spread_cost=fill.spread_cost,
            slippage_cost=fill.slippage_cost,
            impact_cost=fill.impact_cost,
            label=fill.label,
            source=fill.source,
        )


class PortfolioSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    cash: Decimal
    equity: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    drawdown: Decimal
    open_positions: int
    failed_positions: int
    peak_equity: Decimal


class LivePaperSessionMeta(BaseModel):
    model_config = ConfigDict(frozen=True)

    session_id: str
    config_id: str
    phase: str = "phase_6_live_paper"
    study_kind: str = "bounded_live_paper_replay"
    venue: str
    session_start: datetime
    session_end: datetime
    duration: timedelta
    max_events: int
    max_signals: int
    max_trades: int
    queue_capacity: int
    starting_cash: Decimal
    event_count: int
    signal_count: int
    trade_count: int  # paper positions / round-trips (not fills)
    fill_count: int = 0
    supplied_event_count: int = 0
    admitted_event_count: int = 0
    max_events_rejected_count: int = 0
    overflow_count: int
    halted: bool = False
    halt_reason: LivePaperRejectReason | None = None
    frozen_rule_id: str
    phase4_config_id: str
    git_identity: str | None = None
    warnings: tuple[str, ...] = (
        DISCLAIMER,
        WARNING_PAPER_ONLY,
        WARNING_MODELED,
        "no_real_orders_no_wallets_no_capital",
        "cex_depth_only_when_supplied_else_phase5_modeled",
        "dex_modeled_liquidity_never_amm_exact",
        "frozen_phase4_identity_no_rediscovery",
        "bounded_queue_overflow_auditable",
        "max_trades_caps_paper_positions_not_fills",
        "max_events_exceeded_auditable_not_silent",
        "received_and_source_clocks_pit_enforced",
    )

    @field_validator("session_start", "session_end")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class LivePaperReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    meta: LivePaperSessionMeta
    signals: tuple[PaperSignalRecord, ...] = ()
    rejections: tuple[PaperSignalRecord, ...] = ()
    fills: tuple[PaperFillRecord, ...] = ()
    positions: tuple[PaperPositionRecord, ...] = ()
    portfolio: PortfolioSnapshot
    data_quality: dict[str, Any] = Field(default_factory=dict)
    comparison: dict[str, Any] | None = None
    extras: dict[str, Any] = Field(default_factory=dict)
