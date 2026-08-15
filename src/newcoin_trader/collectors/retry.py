"""Retry policy helpers. Auth and schema errors are never retried."""

from __future__ import annotations

from collections.abc import Mapping

RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
AUTH_STATUS_CODES = frozenset({401, 403})


def is_retryable_status(status_code: int) -> bool:
    return status_code in RETRYABLE_STATUS_CODES


def parse_retry_after(headers: Mapping[str, str]) -> float | None:
    raw = headers.get("Retry-After") or headers.get("retry-after")
    if raw is None:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return max(value, 0.0)


def backoff_for_attempt(attempt: int, base_seconds: float) -> float:
    """Exponential backoff without jitter for deterministic tests."""
    factor = 2 ** max(attempt - 1, 0)
    return float(base_seconds * factor)
