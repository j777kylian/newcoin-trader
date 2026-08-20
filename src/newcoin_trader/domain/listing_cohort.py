"""Phase 8.1 historical Binance Spot listing-cohort domain records."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator

from newcoin_trader.domain.types import require_utc


class SpotClass(StrEnum):
    SPOT_LISTING = "SPOT_LISTING"
    NOT_SPOT = "NOT_SPOT"
    AMBIGUOUS = "AMBIGUOUS"


class SourceEventTimeStatus(StrEnum):
    EXTRACTED = "extracted"
    MISSING = "missing"


class CompletenessStatus(StrEnum):
    COMPLETE = "complete"
    INCOMPLETE = "incomplete"


class ListingAnnouncement(BaseModel):
    """Normalized catalog-48 CMS article (list endpoint; body optional from detail)."""

    model_config = ConfigDict(frozen=True)

    code: str
    id: str
    release_date_ms: int
    title: str
    type: str | None = None
    provenance: dict[str, str] = Field(default_factory=dict)
    body: str = ""


class AnnouncementListPage(BaseModel):
    model_config = ConfigDict(frozen=True)

    catalog_id: int
    total: int
    articles: tuple[ListingAnnouncement, ...]


class ParsedListing(BaseModel):
    """Conservative Spot classification + extraction; trading-start never inferred."""

    model_config = ConfigDict(frozen=True)

    announcement_code: str
    announcement_id: str
    title: str
    classification: SpotClass
    symbol: str | None = None
    release_date: datetime
    source_event_time: datetime | None = None
    source_event_time_status: SourceEventTimeStatus
    body: str = ""
    provenance: dict[str, str] = Field(default_factory=dict)
    exclusion_reason: str | None = None

    @field_validator("release_date", "source_event_time")
    @classmethod
    def _aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return require_utc(value)


class CohortListing(BaseModel):
    """Cohort row after Vision corroboration. ``source_event_time`` is never overwritten."""

    model_config = ConfigDict(frozen=True)

    announcement_code: str
    announcement_id: str
    title: str
    classification: SpotClass
    symbol: str
    release_date: datetime
    source_event_time: datetime | None = None
    source_event_time_status: SourceEventTimeStatus
    first_seen_time: datetime
    first_kline_time: datetime | None = None
    first_trade_time: datetime | None = None
    first_market_data_time: datetime | None = None
    decision_available_time: datetime
    completeness: CompletenessStatus
    provenance: dict[str, str] = Field(default_factory=dict)
    exclusion_reason: str | None = None

    @field_validator(
        "release_date",
        "source_event_time",
        "first_seen_time",
        "first_kline_time",
        "first_trade_time",
        "first_market_data_time",
        "decision_available_time",
    )
    @classmethod
    def _aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return require_utc(value)


class ListingExclusion(BaseModel):
    model_config = ConfigDict(frozen=True)

    announcement_code: str
    symbol: str
    reason: str
    title: str


class ListingCohortPipelineReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    cohort_count: int
    exclusion_count: int
    raw_article_count: int
    event_study_event_count: int
    feature_record_count: int
    train_count: int
    validation_count: int
    test_count: int
