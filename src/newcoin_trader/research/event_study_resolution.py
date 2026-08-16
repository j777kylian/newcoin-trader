"""Infer and enforce market-data temporal resolution for Phase 3."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from newcoin_trader.domain.event_study import ObservationResolution

_SUBMINUTE = timedelta(minutes=1)


def parse_interval_seconds(interval: str) -> int | None:
    """Parse common bar intervals (``1s``, ``10s``, ``1m``, ``1h``, ``1d``)."""
    text = interval.strip().lower()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    unit = text[-1]
    try:
        amount = int(text[:-1])
    except ValueError:
        return None
    if amount <= 0:
        return None
    if unit == "s":
        return amount
    if unit == "m":
        return amount * 60
    if unit == "h":
        return amount * 3600
    if unit == "d":
        return amount * 86400
    return None


def resolution_from_provenance(
    provenance: Mapping[str, Any] | None,
    *,
    source: str = "",
) -> ObservationResolution:
    """Map stored snapshot provenance/source to an explicit resolution class."""
    prov = dict(provenance or {})
    kind = str(prov.get("kind", "")).lower()
    if kind in {"trade", "aggtrade", "point", "tick", "quote", "pool"}:
        return ObservationResolution.POINT
    if kind == "kline" or "ohlcv" in kind:
        interval = str(prov.get("interval", "") or "")
        seconds = parse_interval_seconds(interval)
        if seconds is None:
            # Unknown kline interval: treat as coarse to avoid manufacturing sub-minute data.
            return ObservationResolution.COARSE
        if seconds < 60:
            return ObservationResolution.POINT
        if seconds == 60:
            return ObservationResolution.MINUTE
        return ObservationResolution.COARSE

    source_l = source.lower()
    if any(token in source_l for token in ("trade", "aggtrade", "tick")):
        return ObservationResolution.POINT
    if "ohlcv" in source_l or "kline" in source_l:
        # Try trailing interval fragment e.g. geckoterminal:ohlcv:1m
        parts = source_l.split(":")
        for part in reversed(parts):
            seconds = parse_interval_seconds(part)
            if seconds is not None:
                if seconds < 60:
                    return ObservationResolution.POINT
                if seconds == 60:
                    return ObservationResolution.MINUTE
                return ObservationResolution.COARSE
        return ObservationResolution.COARSE

    # Bare pool/spot snapshots without interval metadata are point quotes.
    if kind == "" and source_l in {"raydium", "birdeye", "binance", "geckoterminal"}:
        return ObservationResolution.POINT

    # Unknown product: do not claim sub-minute support.
    return ObservationResolution.MINUTE


def supports_entry_delay(resolution: ObservationResolution, delay: timedelta) -> bool:
    """Sub-minute delays require point/trade resolution; never approximate from minute bars."""
    if delay < _SUBMINUTE:
        return resolution is ObservationResolution.POINT
    if delay < timedelta(hours=1):
        return resolution in {ObservationResolution.POINT, ObservationResolution.MINUTE}
    return True


def finest_resolution(resolutions: set[ObservationResolution]) -> ObservationResolution | None:
    if not resolutions:
        return None
    if ObservationResolution.POINT in resolutions:
        return ObservationResolution.POINT
    if ObservationResolution.MINUTE in resolutions:
        return ObservationResolution.MINUTE
    return ObservationResolution.COARSE
