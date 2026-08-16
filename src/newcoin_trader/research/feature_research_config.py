"""Phase 4 feature-research configuration: windows, budgets, split/rule bounds."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from decimal import Decimal

from newcoin_trader.domain.event_study import TokenListingEvent
from newcoin_trader.domain.types import require_utc
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.event_study_config import (
    MAX_EVENTS_MAX,
    MAX_EVENTS_MIN,
    format_duration,
    parse_duration,
    parse_duration_list,
)

DEFAULT_FEATURE_WINDOWS: tuple[timedelta, ...] = (
    timedelta(minutes=1),
    timedelta(minutes=5),
    timedelta(minutes=15),
    timedelta(minutes=30),
)

DEFAULT_DECISION_DELAY = timedelta(minutes=1)
DEFAULT_MIN_SAMPLE = 20
DEFAULT_SPLIT_RATIOS: tuple[Decimal, Decimal, Decimal] = (
    Decimal("0.6"),
    Decimal("0.2"),
    Decimal("0.2"),
)
DEFAULT_WALK_FORWARD_FOLDS = 3
DEFAULT_MAX_RULES = 25
MAX_RULE_CONDITIONS = 2
DEFAULT_MAX_RULE_CONDITIONS = MAX_RULE_CONDITIONS

DEFAULT_MAX_FEATURE_INPUTS = 250_000
MAX_FEATURE_INPUTS_MIN = 1
MAX_FEATURE_INPUTS_MAX = 5_000_000
DEFAULT_MAX_TRADES = 250_000
MAX_WINDOWS = 8
MAX_WALK_FORWARD_FOLDS = 20
MIN_WALK_FORWARD_FOLDS = 1
MAX_RULES_CAP = 100
MIN_SAMPLE_MIN = 1
MIN_SAMPLE_MAX = 10_000

ALLOWED_WINDOWS: frozenset[timedelta] = frozenset(DEFAULT_FEATURE_WINDOWS)

ELIGIBILITY_RULES: tuple[str, ...] = (
    "feature_inputs_must_be_at_or_before_decision_time",
    "no_forward_fill_for_missing_observations",
    "future_labels_never_supplied_to_feature_computation",
    "venues_never_pooled",
    "chronological_split_no_shuffle_no_test_tuning",
    "rules_discovered_on_train_selected_on_validation_tested_once",
    "nonfinite_decimals_rejected",
    "holder_creator_social_security_wallet_features_excluded",
)


def validate_feature_research_bounds(
    *,
    start: datetime,
    end: datetime,
    max_events: int,
    decision_delay: timedelta,
    windows: Sequence[timedelta],
    min_sample: int,
    split_ratios: tuple[Decimal, Decimal, Decimal],
    walk_forward_folds: int,
    max_rules: int,
    max_rule_conditions: int,
    max_feature_inputs: int = DEFAULT_MAX_FEATURE_INPUTS,
) -> None:
    start_utc = require_utc(start)
    end_utc = require_utc(end)
    if end_utc <= start_utc:
        raise ConfigError("feature-research end must be after start")
    if max_events < MAX_EVENTS_MIN or max_events > MAX_EVENTS_MAX:
        raise ConfigError(f"max_events must be in [{MAX_EVENTS_MIN}, {MAX_EVENTS_MAX}]")
    if decision_delay.total_seconds() < 0:
        raise ConfigError("decision_delay must be non-negative")
    if not windows:
        raise ConfigError("windows must be non-empty")
    if len(windows) > MAX_WINDOWS:
        raise ConfigError(f"at most {MAX_WINDOWS} windows allowed")
    for window in windows:
        if window.total_seconds() <= 0:
            raise ConfigError("windows must be positive")
        if window not in ALLOWED_WINDOWS:
            raise ConfigError(
                f"window {format_duration(window)} not in allowed set "
                f"{[format_duration(w) for w in DEFAULT_FEATURE_WINDOWS]}"
            )
    if min_sample < MIN_SAMPLE_MIN or min_sample > MIN_SAMPLE_MAX:
        raise ConfigError(f"min_sample must be in [{MIN_SAMPLE_MIN}, {MIN_SAMPLE_MAX}]")
    train_r, val_r, test_r = split_ratios
    total = train_r + val_r + test_r
    if total != Decimal("1"):
        raise ConfigError("split_ratios must sum to 1")
    if any(r <= 0 for r in split_ratios):
        raise ConfigError("split_ratios must be positive")
    if walk_forward_folds < MIN_WALK_FORWARD_FOLDS or walk_forward_folds > MAX_WALK_FORWARD_FOLDS:
        raise ConfigError(f"walk_forward_folds must be in [{MIN_WALK_FORWARD_FOLDS}, {MAX_WALK_FORWARD_FOLDS}]")
    if max_rules < 1 or max_rules > MAX_RULES_CAP:
        raise ConfigError(f"max_rules must be in [1, {MAX_RULES_CAP}]")
    if max_rule_conditions < 1 or max_rule_conditions > MAX_RULE_CONDITIONS:
        raise ConfigError(f"max_rule_conditions must be in [1, {MAX_RULE_CONDITIONS}]")
    if max_feature_inputs < MAX_FEATURE_INPUTS_MIN or max_feature_inputs > MAX_FEATURE_INPUTS_MAX:
        raise ConfigError(f"max_feature_inputs must be in [{MAX_FEATURE_INPUTS_MIN}, {MAX_FEATURE_INPUTS_MAX}]")


def parse_split_ratios(spec: str | None) -> tuple[Decimal, Decimal, Decimal]:
    if spec is None or spec.strip() == "":
        return DEFAULT_SPLIT_RATIOS
    parts = [p.strip() for p in spec.split(",") if p.strip()]
    if len(parts) != 3:
        raise ConfigError("split must be three comma-separated ratios summing to 1")
    ratios = tuple(Decimal(p) for p in parts)
    return ratios[0], ratios[1], ratios[2]


def feature_input_bounds(
    events: Sequence[TokenListingEvent],
    *,
    decision_delay: timedelta,
    windows: Sequence[timedelta],
) -> tuple[datetime, datetime]:
    """Derive inclusive feature-input window: earliest decision-max_window .. latest decision."""
    if not events:
        raise ConfigError("feature_input_bounds requires at least one event")
    if not windows:
        raise ConfigError("feature_input_bounds requires non-empty windows")
    max_window = max(windows)
    decisions = [event.source_event_time + decision_delay for event in events]
    return min(decisions) - max_window, max(decisions)


def label_observation_bounds(
    events: Sequence[TokenListingEvent],
    *,
    decision_delay: timedelta,
    entry_delays: Sequence[timedelta],
    holding_periods: Sequence[timedelta],
) -> tuple[datetime, datetime]:
    """Separate future-label observation window (Phase 3 style); not used for features."""
    if not events:
        raise ConfigError("label_observation_bounds requires at least one event")
    if not entry_delays or not holding_periods:
        raise ConfigError("label_observation_bounds requires delay and holding grids")
    earliest = min(event.source_event_time for event in events)
    latest = max(event.source_event_time for event in events)
    # Labels may use entry at source+entry_delay; decision_delay is the feature clock.
    return earliest, latest + max(entry_delays) + max(holding_periods) + decision_delay


__all__ = [
    "ALLOWED_WINDOWS",
    "DEFAULT_DECISION_DELAY",
    "DEFAULT_FEATURE_WINDOWS",
    "DEFAULT_MAX_FEATURE_INPUTS",
    "DEFAULT_MAX_RULES",
    "DEFAULT_MAX_RULE_CONDITIONS",
    "DEFAULT_MAX_TRADES",
    "DEFAULT_MIN_SAMPLE",
    "DEFAULT_SPLIT_RATIOS",
    "DEFAULT_WALK_FORWARD_FOLDS",
    "ELIGIBILITY_RULES",
    "MAX_EVENTS_MAX",
    "MAX_EVENTS_MIN",
    "MAX_FEATURE_INPUTS_MAX",
    "MAX_FEATURE_INPUTS_MIN",
    "MAX_RULE_CONDITIONS",
    "MAX_RULES_CAP",
    "MAX_WALK_FORWARD_FOLDS",
    "feature_input_bounds",
    "format_duration",
    "label_observation_bounds",
    "parse_duration",
    "parse_duration_list",
    "parse_split_ratios",
    "validate_feature_research_bounds",
]
