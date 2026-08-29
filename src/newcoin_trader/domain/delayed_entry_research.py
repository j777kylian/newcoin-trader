"""Phase 8D.1 delayed-entry research contracts; no acquisition or execution."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from newcoin_trader.domain.event_study import CellOutcomeStatus
from newcoin_trader.domain.types import require_utc
from newcoin_trader.research.event_study_config import DEFAULT_ENTRY_DELAYS, DEFAULT_HOLDING_PERIODS


class ImmediateEntryProtocolV1:
    """Descriptor that deliberately references, never replaces, Phase 3 grids."""

    version = "ImmediateEntryProtocolV1"
    entry_delays = DEFAULT_ENTRY_DELAYS
    holding_periods = DEFAULT_HOLDING_PERIODS


class DelayedEntryProtocolV1(BaseModel):
    """Additive delayed-entry protocol, research-only."""

    model_config = ConfigDict(frozen=True)

    version: str = "DelayedEntryProtocolV1"
    entry_delays: tuple[timedelta, ...] = (
        timedelta(hours=1),
        timedelta(hours=2),
        timedelta(hours=3),
        timedelta(hours=4),
        timedelta(hours=6),
        timedelta(hours=8),
        timedelta(hours=12),
    )
    holding_periods: tuple[timedelta, ...] = (
        timedelta(minutes=30),
        timedelta(hours=1),
        timedelta(hours=2),
        timedelta(hours=4),
        timedelta(hours=6),
        timedelta(hours=12),
        timedelta(hours=24),
    )

    @model_validator(mode="after")
    def _exact_protocol(self) -> Self:
        if self.version != "DelayedEntryProtocolV1":
            raise ValueError("DelayedEntryProtocolV1 requires its exact version")
        if self.entry_delays != type(self).model_fields["entry_delays"].default:
            raise ValueError("DelayedEntryProtocolV1 entry_delays are frozen")
        if self.holding_periods != type(self).model_fields["holding_periods"].default:
            raise ValueError("DelayedEntryProtocolV1 holding_periods are frozen")
        return self


class FieldPITClass(StrEnum):
    HISTORICAL_PIT_VERIFIED = "historical_pit_verified"
    RECONSTRUCTABLE_ONCHAIN = "reconstructable_onchain"
    REALTIME_ONLY = "realtime_only"
    PROPRIETARY_ENRICHMENT = "proprietary_enrichment"
    UNVERIFIED = "unverified"


class CandidateDispositionKind(StrEnum):
    ELIGIBLE = "eligible"
    DEAD = "dead"
    RUG = "rug"
    NONEXITABLE = "nonexitable"
    MISSING = "missing"


class CandidateUniverseV1(BaseModel):
    """Bound contemporaneous universe denominator; outcomes cannot redefine it."""

    model_config = ConfigDict(frozen=True)

    universe_id: str
    source_qualification_id: str
    venue: str
    chain: str
    candidate_ids: tuple[str, ...]
    candidate_count: int
    candidate_digest: str

    @model_validator(mode="after")
    def _bound_identity(self) -> Self:
        identity_fields = (
            self.universe_id,
            self.source_qualification_id,
            self.venue,
            self.chain,
        )
        if not all(value.strip() for value in identity_fields):
            raise ValueError("universe identity fields must be non-empty")
        if not self.candidate_ids:
            raise ValueError("candidate_ids must be non-empty")
        if tuple(sorted(self.candidate_ids)) != self.candidate_ids or len(set(self.candidate_ids)) != len(
            self.candidate_ids
        ):
            raise ValueError("candidate_ids must be sorted and unique")
        if any(not item.startswith(f"{self.chain}:") or item.count(":") < 2 for item in self.candidate_ids):
            raise ValueError("candidate_ids require canonical chain-native identity")
        if self.candidate_count != len(self.candidate_ids):
            raise ValueError("candidate_count must equal candidate_ids length")
        if self.candidate_digest != compute_candidate_universe_digest(self.candidate_ids):
            raise ValueError("candidate_digest does not bind candidate_ids")
        return self

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        if update is None:
            return super().model_copy(deep=deep)
        return type(self).model_validate({**self.model_dump(mode="python"), **update})


class CandidateDisposition(BaseModel):
    """Disposition fixed independently of later return/outcome analysis."""

    model_config = ConfigDict(frozen=True)

    candidate_id: str
    kind: CandidateDispositionKind

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        if update is None:
            return super().model_copy(deep=deep)
        return type(self).model_validate({**self.model_dump(mode="python"), **update})


class CohortSnapshotV1(BaseModel):
    """Digest-bound dispositions, so copied adverse outcomes cannot become eligible."""

    model_config = ConfigDict(frozen=True)

    universe: CandidateUniverseV1
    dispositions: tuple[CandidateDisposition, ...]
    disposition_digest: str | None = None

    @model_validator(mode="after")
    def _bound_dispositions(self) -> Self:
        ids = tuple(row.candidate_id for row in self.dispositions)
        if len(ids) != len(set(ids)) or set(ids) != set(self.universe.candidate_ids):
            raise ValueError("candidate dispositions must exactly cover the candidate universe")
        expected = compute_disposition_digest(self.dispositions)
        if self.disposition_digest is None:
            object.__setattr__(self, "disposition_digest", expected)
        elif self.disposition_digest != expected:
            raise ValueError("disposition_digest does not bind dispositions")
        return self

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        if update is None:
            return super().model_copy(deep=deep)
        return type(self).model_validate({**self.model_dump(mode="python"), **update})


class DelayedOutcome(BaseModel):
    """Descriptive outcome contract, explicitly not executable PnL."""

    model_config = ConfigDict(frozen=True)

    candidate_id: str
    entry_delay: timedelta
    holding_period: timedelta
    status: CellOutcomeStatus
    raw_market_return: Decimal | None = None
    cost_sensitivity_estimate: Decimal | None = None
    survival: bool
    liquidity_collapse: bool
    rug_or_nonexitable: bool
    max_drawdown: Decimal | None = None
    observed_at: datetime
    warning: str = "not_executable_pnl_cost_sensitivity_only"

    @field_validator("observed_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)


def compute_candidate_universe_digest(candidate_ids: Sequence[str]) -> str:
    """Canonical digest for an already sorted candidate universe."""
    return sha256("\n".join(candidate_ids).encode("utf-8")).hexdigest()


def compute_disposition_digest(dispositions: Sequence[CandidateDisposition]) -> str:
    """Canonical digest binding each candidate to its independently recorded disposition."""
    rows = tuple(sorted(f"{row.candidate_id}\x1f{row.kind.value}" for row in dispositions))
    return sha256("\n".join(rows).encode("utf-8")).hexdigest()


def _validated(model: BaseModel) -> BaseModel:
    """Public trust boundary: construction/copy bypasses never survive consumption."""
    return type(model).model_validate(model.model_dump(mode="python"))


def validate_candidate_cohort(snapshot: CohortSnapshotV1) -> tuple[CandidateDisposition, ...]:
    """Revalidate a digest-bound cohort before any descriptive aggregation."""
    verified_snapshot = _validated(snapshot)
    assert isinstance(verified_snapshot, CohortSnapshotV1)
    return verified_snapshot.dispositions
