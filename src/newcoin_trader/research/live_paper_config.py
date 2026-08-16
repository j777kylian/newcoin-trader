"""Phase 6 live-paper configuration: explicit bounds and freshness defaults."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from newcoin_trader.domain.numeric import require_finite_decimal
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.event_study_config import (
    MAX_EVENTS_MAX,
    MAX_EVENTS_MIN,
    format_duration,
    parse_duration,
)
from newcoin_trader.research.executable_backtest_config import (
    FEE_BPS_MAX,
    FEE_BPS_MIN,
    MAX_PARTICIPATION_MAX,
    MAX_PARTICIPATION_MIN,
)

# Strict by default: no future-received/source tolerance unless caller sets one explicitly.
DEFAULT_FUTURE_TOLERANCE = timedelta(0)
DEFAULT_FRESHNESS_MAX_AGE = timedelta(minutes=5)
DEFAULT_HOLDING_PERIOD = timedelta(minutes=5)
DEFAULT_DECISION_DELAY = timedelta(minutes=1)
DEFAULT_POSITION_NOTIONAL = Decimal("100")
DEFAULT_MAX_OPEN_POSITIONS = 5
DEFAULT_MAX_TOKEN_EXPOSURE = Decimal("1000")
DEFAULT_MAX_VENUE_EXPOSURE = Decimal("5000")
DEFAULT_MIN_LIQUIDITY = Decimal("1000")
DEFAULT_MAX_PARTICIPATION = Decimal("0.10")
DEFAULT_MAX_IMPACT_SLIPPAGE = Decimal("0.05")
DEFAULT_DAILY_LOSS_LIMIT = Decimal("1000")
DEFAULT_SESSION_LOSS_LIMIT = Decimal("1000")

MAX_SIGNALS_MIN = 1
MAX_SIGNALS_MAX = 100_000
MAX_TRADES_MIN = 1
MAX_TRADES_MAX = 100_000
QUEUE_CAPACITY_MIN = 1
QUEUE_CAPACITY_MAX = 1_000_000
DURATION_MAX = timedelta(days=7)

ELIGIBILITY_RULES: tuple[str, ...] = (
    "phase6_paper_only_no_real_orders",
    "preserve_phase3_4_clocks_and_availability_guard",
    "pit_feature_inputs_at_or_before_decision",
    "freshness_reject_stale_and_future_source_and_received",
    "frozen_phase4_identity_no_rediscovery_retune_short",
    "phase5_fills_cex_depth_when_supplied_else_modeled",
    "dex_modeled_liquidity_never_amm_exact",
    "failed_exits_retained",
    "bounded_queue_overflow_auditable_never_silent_drop",
    "max_events_exceeded_auditable_separate_from_queue_overflow",
    "max_trades_caps_paper_positions_round_trips_not_fills",
    "partial_exit_retains_remaining_qty_and_pro_rata_cost",
    "session_clocks_bounded_to_session_window",
    "idempotent_signal_fill_pnl_on_restart_replay",
    "nonfinite_decimals_rejected_controlled",
)


def validate_live_paper_bounds(
    *,
    duration: timedelta,
    max_events: int,
    max_signals: int,
    max_trades: int,
    queue_capacity: int,
    starting_cash: Decimal,
    position_notional: Decimal,
    holding_period: timedelta,
    max_participation: Decimal = DEFAULT_MAX_PARTICIPATION,
    assumed_fee_bps: Decimal = Decimal("10"),
) -> None:
    if duration.total_seconds() <= 0:
        raise ConfigError("live-paper duration must be positive (no indefinite daemon)")
    if duration > DURATION_MAX:
        raise ConfigError(f"live-paper duration must be <= {DURATION_MAX}")
    if max_events < MAX_EVENTS_MIN or max_events > MAX_EVENTS_MAX:
        raise ConfigError(f"max_events must be in [{MAX_EVENTS_MIN}, {MAX_EVENTS_MAX}]")
    if max_signals < MAX_SIGNALS_MIN or max_signals > MAX_SIGNALS_MAX:
        raise ConfigError(f"max_signals must be in [{MAX_SIGNALS_MIN}, {MAX_SIGNALS_MAX}]")
    if max_trades < MAX_TRADES_MIN or max_trades > MAX_TRADES_MAX:
        raise ConfigError(f"max_trades must be in [{MAX_TRADES_MIN}, {MAX_TRADES_MAX}]")
    if queue_capacity < QUEUE_CAPACITY_MIN or queue_capacity > QUEUE_CAPACITY_MAX:
        raise ConfigError(f"queue_capacity must be in [{QUEUE_CAPACITY_MIN}, {QUEUE_CAPACITY_MAX}]")
    cash = require_finite_decimal(starting_cash, name="paper_starting_cash")
    if cash <= 0:
        raise ConfigError("paper_starting_cash must be positive")
    notional = require_finite_decimal(position_notional, name="position_notional")
    if notional <= 0:
        raise ConfigError("position_notional must be positive")
    if holding_period.total_seconds() <= 0:
        raise ConfigError("holding_period must be positive")
    participation = require_finite_decimal(max_participation, name="max_participation")
    if participation < MAX_PARTICIPATION_MIN or participation > MAX_PARTICIPATION_MAX:
        raise ConfigError(f"max_participation must be in [{MAX_PARTICIPATION_MIN}, {MAX_PARTICIPATION_MAX}]")
    fee = require_finite_decimal(assumed_fee_bps, name="assumed_fee_bps")
    if fee < FEE_BPS_MIN or fee > FEE_BPS_MAX:
        raise ConfigError(f"assumed_fee_bps must be in [{FEE_BPS_MIN}, {FEE_BPS_MAX}]")


__all__ = [
    "DEFAULT_DAILY_LOSS_LIMIT",
    "DEFAULT_DECISION_DELAY",
    "DEFAULT_FRESHNESS_MAX_AGE",
    "DEFAULT_FUTURE_TOLERANCE",
    "DEFAULT_HOLDING_PERIOD",
    "DEFAULT_MAX_IMPACT_SLIPPAGE",
    "DEFAULT_MAX_OPEN_POSITIONS",
    "DEFAULT_MAX_PARTICIPATION",
    "DEFAULT_MAX_TOKEN_EXPOSURE",
    "DEFAULT_MAX_VENUE_EXPOSURE",
    "DEFAULT_MIN_LIQUIDITY",
    "DEFAULT_POSITION_NOTIONAL",
    "DEFAULT_SESSION_LOSS_LIMIT",
    "DURATION_MAX",
    "ELIGIBILITY_RULES",
    "MAX_SIGNALS_MAX",
    "MAX_SIGNALS_MIN",
    "MAX_TRADES_MAX",
    "MAX_TRADES_MIN",
    "QUEUE_CAPACITY_MAX",
    "QUEUE_CAPACITY_MIN",
    "format_duration",
    "parse_duration",
    "validate_live_paper_bounds",
]
