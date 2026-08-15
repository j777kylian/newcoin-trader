"""UTC / Decimal helpers used at domain boundaries."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any


def require_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware UTC")
    return value.astimezone(UTC)


def utc_from_millis(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def utc_from_seconds(seconds: int | float) -> datetime:
    return datetime.fromtimestamp(float(seconds), tz=UTC)


def as_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value is None:
        raise ValueError("cannot convert None to Decimal")
    return Decimal(str(value))


def maybe_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    return as_decimal(value)
