"""Phase 5 executable-backtest configuration: latency/position grids and budgets."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from decimal import Decimal

from newcoin_trader.domain.enums import Venue
from newcoin_trader.domain.event_study import TokenListingEvent
from newcoin_trader.domain.numeric import require_finite_decimal
from newcoin_trader.domain.types import require_utc
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.event_study_config import (
    MAX_EVENTS_MAX,
    MAX_EVENTS_MIN,
    format_duration,
    parse_duration,
    parse_duration_list,
)

DEFAULT_LATENCIES: tuple[timedelta, ...] = (
    timedelta(0),
    timedelta(seconds=10),
    timedelta(seconds=30),
    timedelta(minutes=1),
)
DEFAULT_HOLDING_PERIODS: tuple[timedelta, ...] = (
    timedelta(minutes=1),
    timedelta(minutes=5),
    timedelta(minutes=15),
)
DEFAULT_POSITION_NOTIONALS: tuple[Decimal, ...] = (
    Decimal("10"),
    Decimal("100"),
    Decimal("1000"),
)
DEFAULT_MAX_PARTICIPATION = Decimal("0.10")
DEFAULT_IMPACT_COEFFICIENT = Decimal("1")

DEFAULT_ASSUMED_FEE_BPS: dict[Venue, Decimal] = {
    Venue.BINANCE: Decimal("10"),
    Venue.RAYDIUM: Decimal("30"),
    Venue.GECKO: Decimal("30"),
    Venue.BIRDEYE: Decimal("30"),
}

DEFAULT_MAX_TRADES = 10_000
DEFAULT_MAX_EXECUTION_INPUTS = 250_000
MAX_TRADES_MIN = 1
MAX_TRADES_MAX = 500_000
MAX_EXECUTION_INPUTS_MIN = 1
MAX_EXECUTION_INPUTS_MAX = 5_000_000
MAX_LATENCY_GRID = 16
MAX_HOLDING_GRID = 16
MAX_POSITION_GRID = 8
MAX_PARTICIPATION_MIN = Decimal("0.0001")
MAX_PARTICIPATION_MAX = Decimal("1")
FEE_BPS_MIN = Decimal("0")
FEE_BPS_MAX = Decimal("10_000")

ELIGIBILITY_RULES: tuple[str, ...] = (
    "fill_time_must_be_at_or_after_decision_available_and_signal",
    "latency_forward_only_never_negative",
    "no_future_observation_at_fill",
    "subminute_latency_requires_point_resolution",
    "cex_depth_only_when_supplied_historical_l2",
    "db_has_no_historical_depth_table_modeled_fallback",
    "dex_liquidity_impact_modeled_not_amm_exact",
    "fees_assumed_when_historical_unavailable",
    "frozen_phase4_identity_no_rediscovery_or_test_tuning",
    "failed_exits_never_silently_dropped",
    "nonfinite_decimals_rejected_controlled_status_only",
    "venues_never_pooled",
    "position_participation_capped",
)


def validate_executable_backtest_bounds(
    *,
    start: datetime,
    end: datetime,
    max_events: int,
    max_trades: int,
    max_execution_inputs: int,
    latencies: Sequence[timedelta],
    holding_periods: Sequence[timedelta],
    position_notionals: Sequence[Decimal],
    max_participation: Decimal,
    assumed_fee_bps: Decimal,
) -> None:
    start_utc = require_utc(start)
    end_utc = require_utc(end)
    if end_utc <= start_utc:
        raise ConfigError("executable-backtest end must be after start")
    if max_events < MAX_EVENTS_MIN or max_events > MAX_EVENTS_MAX:
        raise ConfigError(f"max_events must be in [{MAX_EVENTS_MIN}, {MAX_EVENTS_MAX}]")
    if max_trades < MAX_TRADES_MIN or max_trades > MAX_TRADES_MAX:
        raise ConfigError(f"max_trades must be in [{MAX_TRADES_MIN}, {MAX_TRADES_MAX}]")
    if max_execution_inputs < MAX_EXECUTION_INPUTS_MIN or max_execution_inputs > MAX_EXECUTION_INPUTS_MAX:
        raise ConfigError(f"max_execution_inputs must be in [{MAX_EXECUTION_INPUTS_MIN}, {MAX_EXECUTION_INPUTS_MAX}]")
    if not latencies:
        raise ConfigError("latencies must be non-empty")
    if len(latencies) > MAX_LATENCY_GRID:
        raise ConfigError(f"at most {MAX_LATENCY_GRID} latencies allowed")
    for latency in latencies:
        if latency.total_seconds() < 0:
            raise ConfigError("latencies must be non-negative (forward-only)")
    if not holding_periods:
        raise ConfigError("holding_periods must be non-empty")
    if len(holding_periods) > MAX_HOLDING_GRID:
        raise ConfigError(f"at most {MAX_HOLDING_GRID} holding_periods allowed")
    for holding in holding_periods:
        if holding.total_seconds() <= 0:
            raise ConfigError("holding periods must be positive")
    if not position_notionals:
        raise ConfigError("position_notionals must be non-empty")
    if len(position_notionals) > MAX_POSITION_GRID:
        raise ConfigError(f"at most {MAX_POSITION_GRID} position_notionals allowed")
    for notional in position_notionals:
        finite = require_finite_decimal(notional, name="position_notional")
        if finite <= 0:
            raise ConfigError("position_notionals must be positive")
    participation = require_finite_decimal(max_participation, name="max_participation")
    if participation < MAX_PARTICIPATION_MIN or participation > MAX_PARTICIPATION_MAX:
        raise ConfigError(f"max_participation must be in [{MAX_PARTICIPATION_MIN}, {MAX_PARTICIPATION_MAX}]")
    fee = require_finite_decimal(assumed_fee_bps, name="assumed_fee_bps")
    if fee < FEE_BPS_MIN or fee > FEE_BPS_MAX:
        raise ConfigError(f"assumed_fee_bps must be in [{FEE_BPS_MIN}, {FEE_BPS_MAX}]")


def parse_latency(spec: str) -> timedelta:
    """Parse latency allowing ``0s`` (forward-only zero delay)."""
    text = spec.strip().lower()
    if text in {"0", "0s"}:
        return timedelta(0)
    return parse_duration(spec)


def parse_latency_list(
    specs: list[str] | tuple[str, ...] | None,
    *,
    default: tuple[timedelta, ...],
) -> tuple[timedelta, ...]:
    if specs is None or len(specs) == 0:
        return default
    if len(specs) > MAX_LATENCY_GRID:
        raise ConfigError(f"at most {MAX_LATENCY_GRID} latencies allowed")
    parsed = tuple(parse_latency(item) for item in specs)
    seen: set[timedelta] = set()
    ordered: list[timedelta] = []
    for item in parsed:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return tuple(ordered)


def parse_decimal_list(spec: str | None, *, default: tuple[Decimal, ...]) -> tuple[Decimal, ...]:
    if spec is None or spec.strip() == "":
        return default
    parts = [p.strip() for p in spec.split(",") if p.strip()]
    if not parts:
        return default
    if len(parts) > MAX_POSITION_GRID:
        raise ConfigError(f"at most {MAX_POSITION_GRID} position_notionals allowed")
    values: list[Decimal] = []
    for part in parts:
        values.append(require_finite_decimal(part, name="position_notional"))
    return tuple(values)


def assumed_fee_for_venue(venue: Venue | str, override: Decimal | None = None) -> Decimal:
    if override is not None:
        return require_finite_decimal(override, name="assumed_fee_bps")
    if isinstance(venue, str):
        try:
            venue = Venue(venue)
        except ValueError as exc:
            raise ConfigError(f"unsupported venue for assumed fee: {venue}") from exc
    return DEFAULT_ASSUMED_FEE_BPS.get(venue, Decimal("30"))


def execution_observation_bounds(
    events: Sequence[TokenListingEvent],
    *,
    max_latency: timedelta,
    holding_periods: Sequence[timedelta],
) -> tuple[datetime, datetime]:
    if not events:
        raise ConfigError("execution_observation_bounds requires at least one event")
    if not holding_periods:
        raise ConfigError("execution_observation_bounds requires holding periods")
    earliest = min(event.source_event_time for event in events)
    latest = max(event.source_event_time for event in events)
    return earliest, latest + max_latency + max(holding_periods) + max_latency


__all__ = [
    "DEFAULT_ASSUMED_FEE_BPS",
    "DEFAULT_HOLDING_PERIODS",
    "DEFAULT_IMPACT_COEFFICIENT",
    "DEFAULT_LATENCIES",
    "DEFAULT_MAX_EXECUTION_INPUTS",
    "DEFAULT_MAX_PARTICIPATION",
    "DEFAULT_MAX_TRADES",
    "DEFAULT_POSITION_NOTIONALS",
    "ELIGIBILITY_RULES",
    "MAX_EVENTS_MAX",
    "MAX_EVENTS_MIN",
    "MAX_EXECUTION_INPUTS_MAX",
    "MAX_EXECUTION_INPUTS_MIN",
    "MAX_HOLDING_GRID",
    "MAX_LATENCY_GRID",
    "MAX_POSITION_GRID",
    "MAX_TRADES_MAX",
    "MAX_TRADES_MIN",
    "assumed_fee_for_venue",
    "execution_observation_bounds",
    "format_duration",
    "parse_decimal_list",
    "parse_duration",
    "parse_duration_list",
    "parse_latency",
    "parse_latency_list",
    "validate_executable_backtest_bounds",
]
