"""Read-only Binance Vision daily Spot kline/aggTrade zip downloader."""

from __future__ import annotations

import csv
import io
import zipfile
from datetime import UTC, date, datetime
from typing import Literal

from newcoin_trader.collectors.http import GetBytesClient
from newcoin_trader.collectors.normalization import parse_int
from newcoin_trader.errors import ParseError

VISION_BASE_URL = "https://data.binance.vision"
VisionKind = Literal["kline", "aggTrade"]

_KLINE_HEADER_TOKENS = frozenset({"open_time", "open time"})
_AGG_HEADER_TOKENS = frozenset({"agg_trade_id", "aggregate_trade_id"})
_MIN_SUPPORTED_SECONDS = 1_000_000_000
_MAX_SUPPORTED_SECONDS = 9_999_999_999
_MIN_SUPPORTED_MILLIS = 1_000_000_000_000
_MAX_SUPPORTED_MILLIS = 9_999_999_999_999
_MIN_SUPPORTED_MICROS = 1_000_000_000_000_000
_MAX_SUPPORTED_MICROS = 9_999_999_999_999_999


def kline_daily_url(symbol: str, day: date, *, base_url: str = VISION_BASE_URL) -> str:
    stamp = day.isoformat()
    return f"{base_url.rstrip('/')}/data/spot/daily/klines/{symbol}/1m/{symbol}-1m-{stamp}.zip"


def aggtrade_daily_url(symbol: str, day: date, *, base_url: str = VISION_BASE_URL) -> str:
    stamp = day.isoformat()
    return f"{base_url.rstrip('/')}/data/spot/daily/aggTrades/{symbol}/{symbol}-aggTrades-{stamp}.zip"


def _first_csv_rows(blob: bytes) -> list[list[str]]:
    try:
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            names = sorted(n for n in zf.namelist() if not n.endswith("/"))
            if not names:
                raise ParseError("vision zip: empty archive")
            raw = zf.read(names[0])
    except zipfile.BadZipFile as exc:
        raise ParseError("vision zip: not a zip archive") from exc
    text = raw.decode("utf-8", errors="replace")
    return list(csv.reader(io.StringIO(text)))


def _is_header(row: list[str], *, kind: VisionKind) -> bool:
    if not row:
        return False
    token = row[0].strip().lower().replace(" ", "_")
    if kind == "kline":
        return token in _KLINE_HEADER_TOKENS
    return token in _AGG_HEADER_TOKENS


def _utc_from_numeric_epoch(epoch: int, *, context: str) -> datetime:
    """Normalize seconds/milliseconds/microseconds epoch to UTC datetime."""
    if _MIN_SUPPORTED_SECONDS <= epoch <= _MAX_SUPPORTED_SECONDS:
        seconds = float(epoch)
    elif _MIN_SUPPORTED_MILLIS <= epoch <= _MAX_SUPPORTED_MILLIS:
        seconds = epoch / 1000
    elif _MIN_SUPPORTED_MICROS <= epoch <= _MAX_SUPPORTED_MICROS:
        seconds = epoch / 1_000_000
    else:
        raise ParseError(f"{context}: unsupported epoch magnitude {epoch}")
    try:
        return datetime.fromtimestamp(seconds, tz=UTC)
    except (OverflowError, OSError, ValueError) as exc:
        raise ParseError(f"{context}: invalid epoch timestamp {epoch}") from exc


def earliest_timestamp_from_daily_zip(blob: bytes, *, kind: VisionKind) -> datetime:
    rows = _first_csv_rows(blob)
    index = 0 if kind == "kline" else 5
    close_time_index = 6 if kind == "kline" else None
    for i, row in enumerate(rows):
        if not row:
            continue
        if i == 0 and _is_header(row, kind=kind):
            continue
        if len(row) <= index:
            raise ParseError(f"vision {kind} csv: expected column {index}")
        raw = parse_int(row[index], context=f"vision_{kind}_time")
        if raw <= 0:
            raise ParseError(f"vision {kind} csv: non-positive timestamp")
        primary = _utc_from_numeric_epoch(raw, context=f"vision {kind} csv")
        if close_time_index is not None:
            if len(row) <= close_time_index:
                raise ParseError(f"vision {kind} csv: expected column {close_time_index}")
            close_raw = parse_int(row[close_time_index], context="vision_kline_close_time")
            if close_raw <= 0:
                raise ParseError("vision kline csv: non-positive close timestamp")
            close_time = _utc_from_numeric_epoch(close_raw, context="vision kline csv close")
            if close_time < primary:
                raise ParseError("vision kline csv: close time before open time")
        return primary
    raise ParseError(f"vision {kind} csv: no data rows")


class BinanceVisionClient:
    """GET-only daily Vision zip client."""

    def __init__(self, *, http: GetBytesClient, base_url: str = VISION_BASE_URL) -> None:
        self._http = http
        self._base = base_url.rstrip("/")

    async def fetch_daily_kline_zip(self, symbol: str, day: date) -> bytes:
        return await self._http.get_bytes(kline_daily_url(symbol, day, base_url=self._base))

    async def fetch_daily_aggtrade_zip(self, symbol: str, day: date) -> bytes:
        return await self._http.get_bytes(aggtrade_daily_url(symbol, day, base_url=self._base))
