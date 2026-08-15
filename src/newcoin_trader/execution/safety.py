"""Live-execution fail-closed helpers."""

from __future__ import annotations

from newcoin_trader.domain.enums import ExecMode
from newcoin_trader.errors import LiveExecutionForbiddenError


def ensure_paper_mode(mode: ExecMode | str) -> ExecMode:
    try:
        normalized = mode if isinstance(mode, ExecMode) else ExecMode(str(mode).lower())
    except ValueError as exc:
        raise LiveExecutionForbiddenError(
            "Live/non-paper execution is forbidden. Raise occurs before any broker, HTTP, or network call."
        ) from exc
    if normalized is not ExecMode.PAPER:
        raise LiveExecutionForbiddenError(
            "Live/non-paper execution is forbidden. Raise occurs before any broker, HTTP, or network call."
        )
    return normalized
