"""Phase 4 decision-level feature research domain records (research only)."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from newcoin_trader.domain.enums import Chain, Venue
from newcoin_trader.domain.event_study import CellOutcomeStatus, ObservationResolution
from newcoin_trader.domain.types import require_utc

DISCLAIMER = "descriptive_feature_research_not_trading_advice"
WARNING_NO_EXECUTION = "not_executable_strategy_not_backtest_not_live_orders"
REASON_CONFIGURED_DECISION_BEFORE_AVAILABLE = "configured_decision_time_before_decision_available_time"


class AvailabilityLevel(StrEnum):
    SUPPORTED = "supported"
    PARTIAL = "partial"
    UNSUPPORTED = "unsupported"


class FeatureValueState(StrEnum):
    AVAILABLE = "available"
    MISSING = "missing"
    INSUFFICIENT = "insufficient"
    UNSUPPORTED = "unsupported"
    INVALID = "invalid"


class FeatureMarketInput(BaseModel):
    """Point-in-time market input for feature computation (not a future label)."""

    model_config = ConfigDict(frozen=True)

    token_address: str
    chain: str
    venue: Venue
    timestamp: datetime
    price: Decimal
    volume: Decimal | None = None
    liquidity: Decimal | None = None
    resolution: ObservationResolution
    source: str
    provenance: dict[str, str] | None = None

    @field_validator("timestamp")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class FeatureTradeInput(BaseModel):
    """Bounded trade row used only when activity/imbalance is genuinely available."""

    model_config = ConfigDict(frozen=True)

    token_address: str
    chain: str
    venue: Venue
    timestamp: datetime
    side: str
    amount: Decimal
    price: Decimal
    source: str
    provenance: dict[str, str] | None = None

    @field_validator("timestamp")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class FeatureValue(BaseModel):
    """One auditable feature cell with explicit state and provenance."""

    model_config = ConfigDict(frozen=True)

    name: str
    family: str
    value: Decimal | str | None = None
    state: FeatureValueState
    source: str | None = None
    provenance: dict[str, str] = Field(default_factory=dict)
    window: timedelta | None = None


class FutureLabel(BaseModel):
    """Phase-3-consistent future outcome; never used as a feature input."""

    model_config = ConfigDict(frozen=True)

    entry_delay: timedelta
    holding_period: timedelta
    status: CellOutcomeStatus
    simple_return: Decimal | None = None
    log_return: Decimal | None = None
    mfe: Decimal | None = None
    mae: Decimal | None = None
    label_source: str = "phase3_cell"


class DecisionFeatureRecord(BaseModel):
    """Decision-level auditable feature record with distinctly separated labels."""

    model_config = ConfigDict(frozen=True)

    event_id: str
    venue: Venue
    chain: Chain
    token_address: str
    pair_address: str | None = None
    source_event_time: datetime
    first_seen_time: datetime
    first_market_data_time: datetime | None = None
    decision_available_time: datetime
    decision_time: datetime
    feature_cutoff: datetime
    features: tuple[FeatureValue, ...] = ()
    labels: tuple[FutureLabel, ...] = ()
    config_id: str
    computation_id: str
    event_source: str = ""
    event_provenance: dict[str, str] = Field(default_factory=dict)
    label: str = DISCLAIMER
    warning: str = WARNING_NO_EXECUTION

    @field_validator(
        "source_event_time",
        "first_seen_time",
        "first_market_data_time",
        "decision_available_time",
        "decision_time",
        "feature_cutoff",
    )
    @classmethod
    def _aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return require_utc(value)


class DecisionAvailabilityExclusion(BaseModel):
    """Auditable skip when configured decision_time is before decision_available_time."""

    model_config = ConfigDict(frozen=True)

    event_id: str
    configured_decision_time: datetime
    decision_available_time: datetime
    reason: str = REASON_CONFIGURED_DECISION_BEFORE_AVAILABLE

    @field_validator("configured_decision_time", "decision_available_time")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class FeatureBinStats(BaseModel):
    """Per-venue univariate stats for one feature bin (or whole feature)."""

    model_config = ConfigDict(frozen=True)

    venue: Venue
    feature_name: str
    bin_label: str
    samples: int
    complete_count: int
    censored_count: int
    valid_return_count: int
    mean_simple_return: Decimal | None = None
    median_simple_return: Decimal | None = None
    win_rate: Decimal | None = None
    p10: Decimal | None = None
    p25: Decimal | None = None
    p75: Decimal | None = None
    p90: Decimal | None = None
    std_simple_return: Decimal | None = None
    mean_mfe: Decimal | None = None
    mean_mae: Decimal | None = None
    insufficient_sample: bool = False
    label: str = DISCLAIMER
    warning: str = WARNING_NO_EXECUTION


class RuleCondition(BaseModel):
    model_config = ConfigDict(frozen=True)

    feature_name: str
    op: str  # gt | gte | lt | lte | eq
    threshold: Decimal | str


class CandidateRule(BaseModel):
    model_config = ConfigDict(frozen=True)

    rule_id: str
    conditions: tuple[RuleCondition, ...]
    human_readable: str
    train_event_ids: tuple[str, ...] = ()
    train_sample_count: int = 0
    train_mean_return: Decimal | None = None
    validation_mean_return: Decimal | None = None
    test_mean_return: Decimal | None = None
    selected: bool = False


class ChronologicalSplit(BaseModel):
    model_config = ConfigDict(frozen=True)

    train: tuple[DecisionFeatureRecord, ...]
    validation: tuple[DecisionFeatureRecord, ...]
    test: tuple[DecisionFeatureRecord, ...]
    ratios: tuple[Decimal, Decimal, Decimal]


class WalkForwardFold(BaseModel):
    model_config = ConfigDict(frozen=True)

    fold_index: int
    train: tuple[DecisionFeatureRecord, ...]
    test: tuple[DecisionFeatureRecord, ...]


class RuleSelectionResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    candidates: tuple[CandidateRule, ...]
    selected: tuple[CandidateRule, ...]
    test_evaluated_once: bool = True


class FeatureResearchRunMeta(BaseModel):
    model_config = ConfigDict(frozen=True)

    run_id: str
    config_id: str
    phase: str = "phase_4_feature_research"
    study_kind: str = "decision_feature_vs_future_label_research"
    venue: str
    start: datetime
    end: datetime
    max_events: int
    decision_delay: timedelta
    windows: tuple[timedelta, ...]
    min_sample: int
    split_ratios: tuple[Decimal, Decimal, Decimal]
    walk_forward_folds: int
    max_rules: int
    max_rule_conditions: int
    event_count: int
    record_count: int
    git_identity: str | None = None
    warnings: tuple[str, ...] = (
        DISCLAIMER,
        WARNING_NO_EXECUTION,
        "venues_never_pooled",
        "no_forward_fill",
        "feature_inputs_at_or_before_decision_time",
        "labels_never_used_as_features",
    )

    @field_validator("start", "end")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_utc(value)


class FeatureResearchReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    meta: FeatureResearchRunMeta
    availability: dict[str, dict[str, str]]
    records: tuple[DecisionFeatureRecord, ...] = ()
    univariate: tuple[FeatureBinStats, ...] = ()
    split: ChronologicalSplit | None = None
    rules: RuleSelectionResult | None = None
    folds: tuple[WalkForwardFold, ...] = ()
    exclusions: tuple[str, ...] = ()
    decision_exclusions: tuple[DecisionAvailabilityExclusion, ...] = ()
    extras: dict[str, Any] = Field(default_factory=dict)
