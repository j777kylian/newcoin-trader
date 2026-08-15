"""Idempotency helpers. first_seen_time is never moved later."""

from __future__ import annotations

from datetime import datetime


def earliest_first_seen(*, existing: datetime, incoming: datetime) -> datetime:
    return existing if existing <= incoming else incoming


def snapshot_idempotency_key(
    *,
    token_id: int,
    timestamp: datetime,
    source: str,
) -> tuple[int, datetime, str]:
    return (token_id, timestamp, source)
