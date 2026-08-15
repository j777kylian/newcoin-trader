"""Finite numeric helpers for config and broker boundaries."""

from __future__ import annotations

import math
from decimal import Decimal, InvalidOperation
from typing import Any

from newcoin_trader.errors import ConfigError


def require_finite_decimal(value: Any, *, name: str) -> Decimal:
    try:
        if isinstance(value, Decimal):
            decimal_value = value
        else:
            decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ConfigError(f"{name} must be a finite decimal") from exc
    if not decimal_value.is_finite():
        raise ConfigError(f"{name} must be finite")
    return decimal_value


def require_finite_float(value: float, *, name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite number") from exc
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite")
    return number
