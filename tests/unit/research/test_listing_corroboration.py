"""Phase 8.1 Binance Vision earliest-market-data corroboration (fixture bytes; no network)."""

from __future__ import annotations

import csv
import io
import zipfile
from datetime import UTC, datetime

import httpx
import pytest

from newcoin_trader.collectors.binance.vision import (
    VISION_BASE_URL,
    BinanceVisionClient,
    earliest_timestamp_from_daily_zip,
)
from newcoin_trader.collectors.http import AsyncHttpClient
from newcoin_trader.domain.listing_cohort import (
    ListingAnnouncement,
    SourceEventTimeStatus,
    SpotClass,
)
from newcoin_trader.errors import NotFoundError, ParseError
from newcoin_trader.research.listing_cohort import ParsedListing, classify_and_extract
from newcoin_trader.research.listing_corroboration import corroborate_listing


def _zip_csv(inner_name: str, rows: list[list[object]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        text_buf = io.StringIO()
        writer = csv.writer(text_buf)
        for row in rows:
            writer.writerow(row)
        zf.writestr(inner_name, text_buf.getvalue())
    return buf.getvalue()


def _kline_zip(*, open_ms: int) -> bytes:
    return _zip_csv(
        "NEWUSDT-1m-2024-01-15.csv",
        [
            [open_ms, "1.00", "1.10", "0.90", "1.05", "100", open_ms + 59999, "105", 10, "50", "52", "0"],
            [open_ms + 60_000, "1.05", "1.20", "1.00", "1.10", "80", open_ms + 119999, "88", 8, "40", "44", "0"],
        ],
    )


def _kline_zip_with_close(*, open_time: int, close_time: int) -> bytes:
    return _zip_csv(
        "NEWUSDT-1m-2026-04-28.csv",
        [
            [open_time, "1.00", "1.10", "0.90", "1.05", "100", close_time, "105", 10, "50", "52", "0"],
        ],
    )


def _agg_zip(*, trade_ms: int) -> bytes:
    return _zip_csv(
        "NEWUSDT-aggTrades-2024-01-15.csv",
        [
            [1, "1.00", "2.0", 1, 1, trade_ms, "false"],
            [2, "1.01", "1.0", 2, 2, trade_ms + 1000, "true"],
        ],
    )


def test_earliest_timestamp_from_kline_zip_uses_first_open_time() -> None:
    ts = earliest_timestamp_from_daily_zip(_kline_zip(open_ms=1705305600000), kind="kline")
    assert ts == datetime(2024, 1, 15, 8, 0, tzinfo=UTC)


def test_earliest_timestamp_from_aggtrade_zip_uses_first_transact_time() -> None:
    ts = earliest_timestamp_from_daily_zip(_agg_zip(trade_ms=1705305601500), kind="aggTrade")
    assert ts == datetime(2024, 1, 15, 8, 0, 1, 500000, tzinfo=UTC)


def test_earliest_timestamp_from_kline_zip_accepts_microsecond_open_and_close_times() -> None:
    ts = earliest_timestamp_from_daily_zip(
        _kline_zip_with_close(open_time=1777363200000000, close_time=1777363259999000),
        kind="kline",
    )
    assert ts == datetime(2026, 4, 28, 8, 0, tzinfo=UTC)


def test_earliest_timestamp_from_kline_zip_accepts_millisecond_open_and_close_times() -> None:
    ts = earliest_timestamp_from_daily_zip(
        _kline_zip_with_close(open_time=1705305600000, close_time=1705305659999),
        kind="kline",
    )
    assert ts == datetime(2024, 1, 15, 8, 0, tzinfo=UTC)


def test_earliest_timestamp_from_kline_zip_accepts_second_precision() -> None:
    ts = earliest_timestamp_from_daily_zip(
        _kline_zip_with_close(open_time=1705305600, close_time=1705305659),
        kind="kline",
    )
    assert ts == datetime(2024, 1, 15, 8, 0, tzinfo=UTC)


def test_earliest_timestamp_from_aggtrade_zip_accepts_microsecond_trade_time() -> None:
    ts = earliest_timestamp_from_daily_zip(_agg_zip(trade_ms=1777363200123456), kind="aggTrade")
    assert ts == datetime(2026, 4, 28, 8, 0, 0, 123456, tzinfo=UTC)


def test_earliest_timestamp_rejects_impossible_epoch_magnitude() -> None:
    with pytest.raises(ParseError):
        earliest_timestamp_from_daily_zip(_kline_zip(open_ms=1705305600000000000), kind="kline")


@pytest.mark.asyncio
async def test_corroboration_records_first_times_and_never_overwrites_source_event_time() -> None:
    kline_ms = 1705305600000
    trade_ms = 1705305600500
    parsed = classify_and_extract(
        ListingAnnouncement(
            code="spot-new-explicit",
            id="101",
            release_date_ms=1705233600000,
            title="Binance Will Open Trading for NEW/USDT Spot Trading Pairs at 2024-01-15 08:00 (UTC)",
            type="1",
            provenance={"source": "fixture"},
        )
    )
    assert parsed.source_event_time is not None
    announced = parsed.source_event_time

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert str(request.url).startswith(VISION_BASE_URL)
        path = request.url.path
        if path.endswith("NEWUSDT-1m-2024-01-15.zip"):
            return httpx.Response(200, content=_kline_zip(open_ms=kline_ms))
        if path.endswith("NEWUSDT-aggTrades-2024-01-15.zip"):
            return httpx.Response(200, content=_agg_zip(trade_ms=trade_ms))
        return httpx.Response(404, text="not found")

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    vision = BinanceVisionClient(http=http)
    row = await corroborate_listing(
        parsed,
        vision=vision,
        max_probe_days=3,
        lookback_before_days=0,
    )
    await http.aclose()

    assert row.source_event_time == announced
    assert row.source_event_time_status is SourceEventTimeStatus.EXTRACTED
    assert row.first_kline_time == datetime(2024, 1, 15, 8, 0, tzinfo=UTC)
    assert row.first_trade_time == datetime(2024, 1, 15, 8, 0, 0, 500000, tzinfo=UTC)
    assert row.first_market_data_time == row.first_kline_time
    assert row.first_market_data_time == min(row.first_kline_time, row.first_trade_time)


@pytest.mark.asyncio
async def test_missing_source_event_time_stays_missing_when_market_data_exists() -> None:
    parsed = ParsedListing(
        announcement_code="spot-missing-time",
        announcement_id="109",
        title="Binance Will Open Trading for LATE/USDT Spot Trading Pair",
        classification=SpotClass.SPOT_LISTING,
        symbol="LATEUSDT",
        release_date=datetime(2024, 1, 13, 5, 0, tzinfo=UTC),
        source_event_time=None,
        source_event_time_status=SourceEventTimeStatus.MISSING,
        body="Exact opening time will be announced later.",
        provenance={"source": "fixture"},
        exclusion_reason=None,
    )
    kline_ms = 1705125600000  # 2024-01-13 06:00 UTC — must not become source_event_time

    def handler(request: httpx.Request) -> httpx.Response:
        if "LATEUSDT-1m-2024-01-13.zip" in request.url.path:
            return httpx.Response(200, content=_kline_zip(open_ms=kline_ms))
        return httpx.Response(404, text="missing")

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    vision = BinanceVisionClient(http=http)
    row = await corroborate_listing(parsed, vision=vision, max_probe_days=2, lookback_before_days=0)
    await http.aclose()
    assert row.source_event_time is None
    assert row.source_event_time_status is SourceEventTimeStatus.MISSING
    assert row.first_kline_time == datetime(2024, 1, 13, 6, 0, tzinfo=UTC)
    assert row.first_market_data_time == row.first_kline_time
    assert row.release_date != row.first_market_data_time


@pytest.mark.asyncio
async def test_vision_404_days_are_missing_not_errors() -> None:
    parsed = classify_and_extract(
        ListingAnnouncement(
            code="x",
            id="1",
            release_date_ms=1705233600000,
            title="Binance Will Open Trading for NEW/USDT Spot Trading Pair at 2024-01-15 08:00 (UTC)",
            type="1",
            provenance={},
        )
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="nope")

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    vision = BinanceVisionClient(http=http)
    row = await corroborate_listing(parsed, vision=vision, max_probe_days=1, lookback_before_days=0)
    await http.aclose()
    assert row.first_kline_time is None
    assert row.first_trade_time is None
    assert row.first_market_data_time is None
    assert row.source_event_time is not None


@pytest.mark.asyncio
async def test_vision_client_get_bytes_is_get_only() -> None:
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        return httpx.Response(404, text="x")

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    vision = BinanceVisionClient(http=http)
    with pytest.raises(NotFoundError):
        await vision.fetch_daily_kline_zip("NEWUSDT", datetime(2024, 1, 15, tzinfo=UTC).date())
    await http.aclose()
    assert methods == ["GET"]
