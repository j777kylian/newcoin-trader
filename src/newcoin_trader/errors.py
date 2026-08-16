"""Shared error taxonomy. Live execution errors are never swallowed."""

from __future__ import annotations


class NewcoinError(Exception):
    """Base error for the research platform."""


class ConfigError(NewcoinError):
    """Invalid or unsafe configuration."""


class ResearchError(NewcoinError):
    """Deterministic research/computation failure (finite inputs, unsafe context)."""


class CollectorError(NewcoinError):
    """Market-data collector failure."""


class RateLimitError(CollectorError):
    """Retryable HTTP 429 / venue rate limit."""

    def __init__(self, message: str, *, retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class TimeoutError(CollectorError):  # noqa: A001
    """Retryable request timeout."""


class AuthError(CollectorError):
    """Non-retryable 401/403."""


class NotFoundError(CollectorError):
    """HTTP 404 or empty required resource."""


class ParseError(CollectorError):
    """Response schema could not be normalized."""


class RetryableHttpError(CollectorError):
    """Retryable 5xx or similar transient HTTP failure."""

    def __init__(self, message: str, *, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


class RepositoryError(NewcoinError):
    """Persistence failure."""


class IntegrityConflict(RepositoryError):
    """Unique/idempotency conflict that was not absorbed by upsert."""


class LiveExecutionForbiddenError(NewcoinError):
    """Fail-closed guard: live/non-paper execution is never permitted."""
