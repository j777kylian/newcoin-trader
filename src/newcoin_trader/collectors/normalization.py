"""Shared parsing helpers for venue payloads."""

from __future__ import annotations

import math
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from functools import wraps
from typing import Any, ParamSpec, TypeVar

from pydantic import ValidationError

from newcoin_trader.domain.types import utc_from_millis, utc_from_seconds
from newcoin_trader.errors import ParseError

P = ParamSpec("P")
T = TypeVar("T")

# Parser-internal failures that must surface as ParseError at collector boundaries.
_PARSER_FAILURES = (
    KeyError,
    IndexError,
    TypeError,
    ValueError,
    InvalidOperation,
    OverflowError,
    OSError,
    ArithmeticError,
    ValidationError,
)


def guard_parse(context: str) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Normalize parser-internal exceptions into ParseError without swallowing ParseError."""

    def decorator(fn: Callable[P, T]) -> Callable[P, T]:
        @wraps(fn)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
            try:
                return fn(*args, **kwargs)
            except ParseError:
                raise
            except _PARSER_FAILURES as exc:
                raise ParseError(f"{context}: malformed payload ({type(exc).__name__})") from exc

        return wrapped

    return decorator


def parse_decimal(value: Any, *, context: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ParseError(f"{context}: invalid decimal")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ParseError(f"{context}: invalid decimal {value!r}") from exc
    if not result.is_finite():
        raise ParseError(f"{context}: non-finite decimal")
    return result


def parse_int(value: Any, *, context: str) -> int:
    """Parse an integral scalar. Rejects bool/null, non-finite, and fractional values."""
    if isinstance(value, bool) or value is None:
        raise ParseError(f"{context}: invalid int")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value) or value != math.trunc(value):
            raise ParseError(f"{context}: invalid int {value!r}")
        return int(value)
    if isinstance(value, Decimal):
        if not value.is_finite() or value != value.to_integral_value():
            raise ParseError(f"{context}: invalid int {value!r}")
        return int(value)
    if isinstance(value, str):
        try:
            as_dec = Decimal(value)
        except (InvalidOperation, ValueError) as exc:
            raise ParseError(f"{context}: invalid int {value!r}") from exc
        if not as_dec.is_finite() or as_dec != as_dec.to_integral_value():
            raise ParseError(f"{context}: invalid int {value!r}")
        return int(as_dec)
    raise ParseError(f"{context}: invalid int {value!r}")


def require_mapping(payload: Any, *, context: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ParseError(f"{context}: expected object")
    return payload


def require_list(payload: Any, *, context: str) -> list[Any]:
    if not isinstance(payload, list):
        raise ParseError(f"{context}: expected list")
    return payload


def parse_venue_time(value: Any) -> datetime:
    """Permissive timestamp parse for non-Binance callers (ISO strings, seconds, millis)."""
    if value is None:
        raise ParseError("missing timestamp")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, str):
        if value.isdigit():
            return parse_venue_time(int(value))
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ParseError(f"invalid timestamp: {value}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    if isinstance(value, bool):
        raise ParseError("invalid timestamp type: bool")
    if isinstance(value, (int, float)):
        try:
            if isinstance(value, float) and not math.isfinite(value):
                raise ParseError(f"invalid timestamp: {value!r}")
            if value > 10_000_000_000:
                return utc_from_millis(int(value))
            return utc_from_seconds(value)
        except (OverflowError, OSError, ValueError) as exc:
            raise ParseError(f"invalid timestamp: {value!r}") from exc
    raise ParseError(f"invalid timestamp type: {type(value)!r}")


def parse_required_venue_time(value: Any, *, context: str) -> datetime:
    """Strict required venue timestamp: present, finite, integral, and strictly positive."""
    if value is None or isinstance(value, bool):
        raise ParseError(f"{context}: missing or invalid timestamp")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            parsed = value.replace(tzinfo=UTC)
        else:
            parsed = value.astimezone(UTC)
        if parsed.timestamp() <= 0:
            raise ParseError(f"{context}: non-positive timestamp")
        return parsed
    if isinstance(value, str):
        if value.isdigit():
            return parse_required_venue_time(int(value), context=context)
        raise ParseError(f"{context}: invalid timestamp {value!r}")
    if isinstance(value, float):
        if not math.isfinite(value) or value != math.trunc(value):
            raise ParseError(f"{context}: invalid timestamp {value!r}")
        value = int(value)
    if isinstance(value, int):
        if value <= 0:
            raise ParseError(f"{context}: non-positive timestamp")
        try:
            if value > 10_000_000_000:
                return utc_from_millis(value)
            return utc_from_seconds(value)
        except (OverflowError, OSError, ValueError) as exc:
            raise ParseError(f"{context}: invalid timestamp {value!r}") from exc
    raise ParseError(f"{context}: invalid timestamp type: {type(value)!r}")
