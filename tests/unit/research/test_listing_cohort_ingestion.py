"""Phase 8.1 follow-up: cohort-driven historical market-data ingestion (no network)."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest

from newcoin_trader.collectors.binance.announcements import BinanceAnnouncementClient
from newcoin_trader.collectors.binance.client import BinanceClient
from newcoin_trader.collectors.binance.vision import BinanceVisionClient
from newcoin_trader.collectors.http import AsyncHttpClient
from newcoin_trader.domain.enums import Chain, Side, Venue
from newcoin_trader.domain.event_study import (
    CellOutcomeStatus,
    ObservationResolution,
    TokenListingEvent,
)
from newcoin_trader.domain.listing_cohort import (
    CohortListing,
    CompletenessStatus,
    SourceEventTimeStatus,
    SpotClass,
)
from newcoin_trader.domain.market import Kline, TradeTick
from newcoin_trader.errors import ConfigError
from newcoin_trader.research.event_study_config import DEFAULT_ENTRY_DELAYS, DEFAULT_HOLDING_PERIODS
from newcoin_trader.research.event_study_engine import run_event_study
from newcoin_trader.research.listing_cohort_ingestion import (
    AGG_TRADE_SOURCE,
    INGEST_WINDOW_BUFFER,
    KLINE_SOURCE,
    compute_listing_ingest_window,
    ingest_cohort_market_history,
    kline_to_market_input,
    trade_to_market_input,
    trade_to_trade_input,
)
from newcoin_trader.research.listing_cohort_run import (
    ARTIFACT_NAMES,
    _input_to_observation,
    run_listing_cohort_pilot,
)

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures" / "binance" / "announcements"
T0 = datetime(2024, 1, 15, 8, 0, tzinfo=UTC)
RELEASE = datetime(2024, 1, 14, 12, 0, tzinfo=UTC)


def _ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _load(name: str) -> object:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _zip_csv(inner_name: str, rows: list[list[object]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        text_buf = io.StringIO()
        writer = csv.writer(text_buf)
        for row in rows:
            writer.writerow(row)
        zf.writestr(inner_name, text_buf.getvalue())
    return buf.getvalue()


def _kline_rows(symbol: str, start: datetime, minutes: int) -> bytes:
    start_ms = _ms(start)
    rows: list[list[object]] = []
    for i in range(minutes):
        open_ms = start_ms + i * 60_000
        price = Decimal("1.00") + Decimal("0.01") * i
        close = price + Decimal("0.005")
        rows.append(
            [
                open_ms,
                str(price),
                str(close),
                str(price),
                str(close),
                "100",
                open_ms + 59_999,
                "105",
                10,
                "50",
                "52",
                "0",
            ]
        )
    return _zip_csv(f"{symbol}-1m-2024-01-15.csv", rows)


def _agg_rows(symbol: str, start: datetime) -> bytes:
    start_ms = _ms(start)
    return _zip_csv(
        f"{symbol}-aggTrades-2024-01-15.csv",
        [[1, "1.00", "2.0", 1, 1, start_ms, "false"]],
    )


def _cohort(**overrides: Any) -> CohortListing:
    payload: dict[str, Any] = {
        "announcement_code": "spot-will-list",
        "announcement_id": "101",
        "title": "Binance Will Open Trading for NEW/USDT Spot Trading Pairs at 2024-01-15 08:00 (UTC)",
        "classification": SpotClass.SPOT_LISTING,
        "symbol": "NEWUSDT",
        "release_date": RELEASE,
        "source_event_time": T0,
        "source_event_time_status": SourceEventTimeStatus.EXTRACTED,
        "first_seen_time": RELEASE,
        "first_kline_time": T0,
        "first_trade_time": T0,
        "first_market_data_time": T0,
        "decision_available_time": RELEASE,
        "completeness": CompletenessStatus.COMPLETE,
        "provenance": {"source_event_time": T0.isoformat()},
    }
    payload.update(overrides)
    return CohortListing(**payload)


def _kline(
    *,
    symbol: str = "NEWUSDT",
    open_time: datetime,
    close: str = "1.00",
    volume: str = "100",
) -> Kline:
    return Kline(
        token_address=symbol,
        chain="binance",
        open_time=open_time,
        close_time=open_time + timedelta(milliseconds=59_999),
        open=Decimal(close),
        high=Decimal(close),
        low=Decimal(close),
        close=Decimal(close),
        volume=Decimal(volume),
        quote_volume=Decimal("105"),
        trade_count=10,
        interval="1m",
        source="binance",
        venue=Venue.BINANCE,
    )


def _trade(
    *,
    symbol: str = "NEWUSDT",
    timestamp: datetime,
    price: str = "1.00",
    amount: str = "2.0",
    agg_id: str = "1",
    side: Side = Side.BUY,
) -> TradeTick:
    return TradeTick(
        token_address=symbol,
        chain="binance",
        timestamp=timestamp,
        side=side,
        amount=Decimal(amount),
        price=Decimal(price),
        external_trade_id=agg_id,
        source="binance:aggTrades",
        provenance={"kind": "aggTrade", "endpoint": "/api/v3/aggTrades"},
    )


class SeriesMarketHistory:
    """In-memory Binance Spot klines/aggTrades with start_time/from_id pagination."""

    def __init__(self, klines: list[Kline], trades: list[TradeTick]) -> None:
        self._klines = sorted(klines, key=lambda row: row.open_time)
        self._trades = sorted(
            trades,
            key=lambda row: (row.timestamp, int(row.external_trade_id or 0)),
        )
        self.klines_calls: list[dict[str, Any]] = []
        self.agg_calls: list[dict[str, Any]] = []

    async def klines(
        self,
        symbol: str,
        *,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 500,
    ) -> list[Kline]:
        assert limit <= 1000
        self.klines_calls.append(
            {
                "symbol": symbol,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time,
                "limit": limit,
            }
        )
        rows = [row for row in self._klines if row.token_address == symbol]
        if start_time is not None:
            rows = [row for row in rows if _ms(row.open_time) >= start_time]
        if end_time is not None:
            rows = [row for row in rows if _ms(row.open_time) <= end_time]
        return rows[:limit]

    async def agg_trades(
        self,
        symbol: str,
        *,
        from_id: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 500,
    ) -> list[TradeTick]:
        assert limit <= 1000
        self.agg_calls.append(
            {
                "symbol": symbol,
                "from_id": from_id,
                "start_time": start_time,
                "end_time": end_time,
                "limit": limit,
            }
        )
        rows = [row for row in self._trades if row.token_address == symbol]
        if start_time is not None:
            rows = [row for row in rows if _ms(row.timestamp) >= start_time]
            if end_time is not None:
                rows = [row for row in rows if _ms(row.timestamp) <= end_time]
        elif from_id is not None:
            rows = [row for row in rows if int(row.external_trade_id or 0) >= from_id]
        return rows[:limit]


def test_kline_converts_to_minute_feature_market_input() -> None:
    kline = _kline(open_time=T0, close="1.25", volume="40")
    inp = kline_to_market_input(kline)
    assert inp.resolution is ObservationResolution.MINUTE
    assert inp.timestamp == kline.close_time
    assert inp.price == Decimal("1.25")
    assert inp.volume == Decimal("40")
    assert inp.source == KLINE_SOURCE == "binance:kline:1m"
    assert inp.venue is Venue.BINANCE
    assert inp.token_address == "NEWUSDT"


def test_agg_trade_converts_to_point_market_and_trade_inputs() -> None:
    trade = _trade(timestamp=T0 + timedelta(seconds=10), price="1.02", amount="3.5", side=Side.SELL)
    market = trade_to_market_input(trade)
    feat = trade_to_trade_input(trade)
    assert market.resolution is ObservationResolution.POINT
    assert market.timestamp == trade.timestamp
    assert market.price == Decimal("1.02")
    assert market.source == AGG_TRADE_SOURCE == "binance:agg_trade"
    assert feat.timestamp == trade.timestamp
    assert feat.side == "sell"
    assert feat.amount == Decimal("3.5")
    assert feat.price == Decimal("1.02")
    assert feat.source == AGG_TRADE_SOURCE


def test_ingest_window_uses_phase3_grids_and_small_buffer() -> None:
    start, end = compute_listing_ingest_window(_cohort(), now_utc=T0 + timedelta(days=10))
    assert INGEST_WINDOW_BUFFER == timedelta(minutes=1)
    assert start == T0 - INGEST_WINDOW_BUFFER
    assert end == T0 + max(DEFAULT_ENTRY_DELAYS) + max(DEFAULT_HOLDING_PERIODS) + INGEST_WINDOW_BUFFER
    assert max(DEFAULT_ENTRY_DELAYS) == timedelta(minutes=30)
    assert max(DEFAULT_HOLDING_PERIODS) == timedelta(hours=24)
    assert end == T0 + timedelta(hours=24, minutes=31)


def test_ingest_window_keeps_source_event_and_first_market_data_distinct() -> None:
    first_market = T0 + timedelta(minutes=10)
    first_kline = T0 - timedelta(minutes=5)
    listing = _cohort(
        first_market_data_time=first_market,
        first_kline_time=first_kline,
        first_trade_time=first_market,
    )
    start, end = compute_listing_ingest_window(listing, now_utc=T0 + timedelta(days=10))
    assert listing.source_event_time == T0
    assert listing.first_market_data_time == first_market
    assert listing.source_event_time != listing.first_market_data_time
    assert start == first_kline - INGEST_WINDOW_BUFFER
    assert end == first_market + timedelta(hours=24, minutes=31)


def test_ingest_window_clamps_to_now_utc_and_never_extends_past_current() -> None:
    now = T0 + timedelta(hours=2)
    window = compute_listing_ingest_window(_cohort(), now_utc=now)
    assert window is not None
    start, end = window
    assert start == T0 - INGEST_WINDOW_BUFFER
    assert end == now
    assert end <= now


@pytest.mark.asyncio
async def test_cohort_to_historical_market_inputs_and_no_pre_listing_leakage() -> None:
    listing = _cohort()
    klines = [
        _kline(open_time=T0 - timedelta(minutes=2), close="0.90"),
        _kline(open_time=T0 - timedelta(minutes=1), close="0.95"),
        _kline(open_time=T0, close="1.00"),
        _kline(open_time=T0 + timedelta(minutes=1), close="1.01"),
    ]
    trades = [
        _trade(timestamp=T0 - timedelta(seconds=5), price="0.89", agg_id="1"),
        _trade(timestamp=T0, price="1.00", agg_id="2"),
        _trade(timestamp=T0 + timedelta(seconds=10), price="1.02", agg_id="3"),
    ]
    source = SeriesMarketHistory(klines, trades)
    market_inputs, trade_inputs = await ingest_cohort_market_history([listing], source, limit=1000)

    assert source.klines_calls
    assert all(call["limit"] <= 1000 for call in source.klines_calls)
    assert all(call["interval"] == "1m" for call in source.klines_calls)
    assert source.agg_calls
    assert all(call["limit"] <= 1000 for call in source.agg_calls)

    kline_inputs = [row for row in market_inputs if row.resolution is ObservationResolution.MINUTE]
    point_inputs = [row for row in market_inputs if row.resolution is ObservationResolution.POINT]
    assert {row.price for row in kline_inputs} == {Decimal("1.00"), Decimal("1.01")}
    assert all(row.timestamp >= T0 for row in market_inputs)
    assert listing.first_market_data_time is not None
    assert all(row.timestamp >= listing.first_market_data_time for row in market_inputs)
    assert all(row.timestamp >= T0 for row in trade_inputs)
    assert Decimal("0.90") not in {row.price for row in kline_inputs}
    assert Decimal("0.89") not in {row.price for row in point_inputs}
    assert {row.timestamp for row in point_inputs} == {T0, T0 + timedelta(seconds=10)}
    assert len(trade_inputs) == 2
    assert listing.source_event_time == T0
    assert listing.source_event_time != listing.release_date


@pytest.mark.asyncio
async def test_no_pre_listing_leakage_when_first_market_data_is_later_than_announced_start() -> None:
    first_market = T0 + timedelta(minutes=2)
    listing = _cohort(
        first_market_data_time=first_market,
        first_kline_time=first_market,
        first_trade_time=first_market,
    )
    klines = [_kline(open_time=T0 + timedelta(minutes=i), close=str(1 + i / 100)) for i in range(4)]
    trades = [_trade(timestamp=T0 + timedelta(minutes=i), agg_id=str(i + 1)) for i in range(4)]
    market_inputs, trade_inputs = await ingest_cohort_market_history(
        [listing],
        SeriesMarketHistory(klines, trades),
        limit=500,
    )
    assert listing.source_event_time == T0
    assert listing.first_market_data_time == first_market
    assert all(row.timestamp >= first_market for row in market_inputs)
    assert all(row.timestamp >= first_market for row in trade_inputs)
    assert not any(row.timestamp == T0 for row in market_inputs)


@pytest.mark.asyncio
async def test_pagination_follows_start_time_and_from_id_until_window_covered() -> None:
    listing = _cohort()
    klines = [_kline(open_time=T0 + timedelta(minutes=i), close=str(1 + i / 100)) for i in range(5)]
    trades = [
        _trade(timestamp=T0 + timedelta(seconds=i * 5), price=str(1 + i / 100), agg_id=str(i + 1)) for i in range(5)
    ]
    source = SeriesMarketHistory(klines, trades)
    market_inputs, trade_inputs = await ingest_cohort_market_history([listing], source, limit=2)
    assert len(source.klines_calls) >= 3
    assert source.klines_calls[0]["start_time"] is not None
    assert source.klines_calls[1]["start_time"] is not None
    assert source.klines_calls[1]["start_time"] > source.klines_calls[0]["start_time"]
    assert source.agg_calls[0]["from_id"] is None
    assert source.agg_calls[0]["start_time"] is not None
    assert any(call["from_id"] is not None and call["start_time"] is None for call in source.agg_calls[1:])
    assert len([row for row in market_inputs if row.resolution is ObservationResolution.MINUTE]) == 5
    assert len(trade_inputs) == 5


@pytest.mark.asyncio
async def test_ingest_rejects_binance_limit_outside_1_to_1000() -> None:
    source = SeriesMarketHistory([], [])
    with pytest.raises(ConfigError, match="binance_limit"):
        await ingest_cohort_market_history([_cohort()], source, limit=0)
    with pytest.raises(ConfigError, match="binance_limit"):
        await ingest_cohort_market_history([_cohort()], source, limit=1001)


@pytest.mark.asyncio
async def test_ingest_uses_existing_binance_client_methods_via_mocked_http() -> None:
    listing = _cohort()
    open_ms = _ms(T0)
    kline_payload = [
        [
            open_ms,
            "1.00000000",
            "1.25000000",
            "0.90000000",
            "1.10000000",
            "100.00000000",
            open_ms + 59_999,
            "110.00000000",
            42,
            "50.00000000",
            "55.00000000",
            "0",
        ]
    ]
    trade_payload = [
        {
            "a": 10,
            "p": "1.10000000",
            "q": "4.00000000",
            "f": 1,
            "l": 1,
            "T": open_ms,
            "m": False,
            "M": True,
        }
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/api/v3/klines"):
            return httpx.Response(200, json=kline_payload)
        if path.endswith("/api/v3/aggTrades"):
            return httpx.Response(200, json=trade_payload)
        return httpx.Response(404, json={"error": path})

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    client = BinanceClient(http=http, base_url="https://api.binance.com")
    market_inputs, trade_inputs = await ingest_cohort_market_history([listing], client, limit=500)
    await http.aclose()
    assert any(row.source == "binance:kline:1m" for row in market_inputs)
    assert any(row.source == "binance:agg_trade" for row in market_inputs)
    assert any(row.resolution is ObservationResolution.POINT for row in market_inputs)
    assert trade_inputs[0].side == "buy"
    assert trade_inputs[0].amount == Decimal("4.00000000")


def _event_from_listing(listing: CohortListing) -> TokenListingEvent:
    assert listing.source_event_time is not None
    return TokenListingEvent(
        event_id="binance:binance:NEWUSDT:spot-will-list",
        venue=Venue.BINANCE,
        chain=Chain.BINANCE,
        token_address=listing.symbol,
        symbol=listing.symbol,
        source="binance:cms:catalog48",
        source_event_time=listing.source_event_time,
        first_seen_time=listing.first_seen_time,
        first_market_data_time=listing.first_market_data_time,
        decision_available_time=listing.decision_available_time,
        provenance=dict(listing.provenance),
    )


@pytest.mark.asyncio
async def test_subminute_cells_use_point_observations_else_unsupported_resolution() -> None:
    listing = _cohort()
    minute_only = SeriesMarketHistory(
        [_kline(open_time=T0 + timedelta(minutes=i), close="1.00") for i in range(10)],
        [],
    )
    minute_inputs, _trades = await ingest_cohort_market_history([listing], minute_only, limit=1000)
    minute_obs = tuple(_input_to_observation(row) for row in minute_inputs)
    event = _event_from_listing(listing)
    minute_cells = run_event_study(
        (event,),
        minute_obs,
        entry_delays=(timedelta(seconds=10), timedelta(seconds=30)),
        holding_periods=(timedelta(minutes=5),),
    )
    assert minute_cells
    assert all(cell.status is CellOutcomeStatus.UNSUPPORTED_RESOLUTION for cell in minute_cells)

    point_source = SeriesMarketHistory(
        [_kline(open_time=T0, close="1.00")],
        [
            _trade(timestamp=T0 + timedelta(seconds=10), price="1.00", agg_id="1"),
            _trade(timestamp=T0 + timedelta(seconds=30), price="1.01", agg_id="2"),
            _trade(timestamp=T0 + timedelta(seconds=10) + timedelta(minutes=5), price="1.10", agg_id="3"),
            _trade(timestamp=T0 + timedelta(seconds=30) + timedelta(minutes=5), price="1.12", agg_id="4"),
        ],
    )
    market_inputs, trade_inputs = await ingest_cohort_market_history([listing], point_source, limit=1000)
    assert trade_inputs
    observations = tuple(_input_to_observation(row) for row in market_inputs)
    point_cells = run_event_study(
        (event,),
        observations,
        entry_delays=(timedelta(seconds=10), timedelta(seconds=30)),
        holding_periods=(timedelta(minutes=5),),
    )
    assert {cell.status for cell in point_cells} == {CellOutcomeStatus.COMPLETE}
    assert all(cell.entry_source == AGG_TRADE_SOURCE for cell in point_cells)


def _announcement_vision_handler(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    params = dict(request.url.params)
    if path.endswith("/article/list/query"):
        if params.get("pageNo") == "1":
            return httpx.Response(200, json=_load("list_page1.json"))
        return httpx.Response(200, json=_load("list_empty.json"))
    if path.endswith("/article/detail/query"):
        mapping = {
            "spot-will-list": "detail_spot_new.json",
            "spot-missing-time": "detail_spot_missing_time.json",
            "spot-new-explicit": "detail_empty_body.json",
        }
        name = mapping.get(params.get("articleCode") or "")
        if name:
            return httpx.Response(200, json=_load(name))
        return httpx.Response(404, text="no detail")
    if "NEWUSDT-1m-" in path:
        return httpx.Response(200, content=_kline_rows("NEWUSDT", T0, 120))
    if "NEWUSDT-aggTrades-" in path:
        return httpx.Response(200, content=_agg_rows("NEWUSDT", T0))
    if "LATEUSDT-1m-" in path:
        late = datetime(2024, 1, 13, 6, 0, tzinfo=UTC)
        return httpx.Response(200, content=_kline_rows("LATEUSDT", late, 120))
    if "LATEUSDT-aggTrades-" in path:
        late = datetime(2024, 1, 13, 6, 0, tzinfo=UTC)
        return httpx.Response(200, content=_agg_rows("LATEUSDT", late))
    return httpx.Response(404, text="missing")


def _pilot_market_history() -> SeriesMarketHistory:
    late = datetime(2024, 1, 13, 6, 0, tzinfo=UTC)
    klines: list[Kline] = []
    trades: list[TradeTick] = []
    for symbol, start in (("NEWUSDT", T0), ("LATEUSDT", late)):
        for i in range(120):
            ts = start + timedelta(minutes=i)
            klines.append(_kline(symbol=symbol, open_time=ts, close=str(Decimal("1.00") + Decimal("0.01") * i)))
            trades.append(
                _trade(
                    symbol=symbol,
                    timestamp=ts,
                    price=str(Decimal("1.00") + Decimal("0.01") * i),
                    agg_id=str(i + 1),
                )
            )
        trades.append(_trade(symbol=symbol, timestamp=start + timedelta(seconds=10), price="1.00", agg_id="200"))
        trades.append(_trade(symbol=symbol, timestamp=start + timedelta(seconds=30), price="1.00", agg_id="201"))
    return SeriesMarketHistory(klines, trades)


@pytest.mark.asyncio
async def test_pilot_emits_deterministic_artifacts_a_through_h(tmp_path: Path) -> None:
    http = AsyncHttpClient(
        transport=httpx.MockTransport(_announcement_vision_handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    announcements = BinanceAnnouncementClient(http=http)
    vision = BinanceVisionClient(http=http)
    source = _pilot_market_history()
    first = tmp_path / "run1"
    second = tmp_path / "run2"
    kwargs = {
        "announcement_client": announcements,
        "vision_client": vision,
        "market_history": source,
        "requested_start": datetime(2024, 1, 1, tzinfo=UTC),
        "requested_end": datetime(2024, 2, 1, tzinfo=UTC),
        "max_pages": 1,
        "max_articles": 20,
        "page_size": 20,
        "max_probe_days": 2,
        "binance_limit": 1000,
        "lookback_before_days": 0,
        "fetch_details": True,
        "now_utc": datetime(2026, 8, 19, 4, 40, tzinfo=UTC),
    }
    report1 = await run_listing_cohort_pilot(output_dir=first, **kwargs)
    report2 = await run_listing_cohort_pilot(output_dir=second, **kwargs)
    await http.aclose()

    assert report1.cohort_count >= 2
    assert report2.cohort_count == report1.cohort_count
    assert report1.event_study_event_count >= 1
    assert report1.feature_record_count >= 1
    for filename in ARTIFACT_NAMES.values():
        left = first / filename
        right = second / filename
        assert left.exists(), filename
        assert hashlib.sha256(left.read_bytes()).hexdigest() == hashlib.sha256(right.read_bytes()).hexdigest()

    cohort = json.loads((first / "listing_cohort.json").read_text(encoding="utf-8"))
    new = next(row for row in cohort["listings"] if row["symbol"] == "NEWUSDT")
    assert new["source_event_time"] is not None
    assert new["source_event_time"] != new["release_date"]
    late = next(row for row in cohort["listings"] if row["symbol"] == "LATEUSDT")
    assert late["source_event_time"] is None
    assert late["first_market_data_time"] is not None

    return_matrix = json.loads((first / "return_matrix.json").read_text(encoding="utf-8"))
    delay_hold = {(row["entry_delay"], row["holding_period"]) for row in return_matrix["aggregates"]}
    assert ("10s", "1m") in delay_hold
    assert ("30m", "1h") in delay_hold
    assert ("0m", "5m") not in delay_hold
    assert len(delay_hold) == 8 * 8

    event_rows = list(csv.DictReader((first / "event_study_cells.csv").open(encoding="utf-8")))
    assert event_rows
    assert any(row["status"] == "complete" for row in event_rows)
    feature_rows = list(csv.DictReader((first / "pit_feature_dataset.csv").open(encoding="utf-8")))
    assert feature_rows
    assert not any("depth" in name or "spread" in name or "bid_" in name for name in feature_rows[0].keys())
    assert all(call["limit"] <= 1000 for call in source.klines_calls)
    assert all(call["limit"] <= 1000 for call in source.agg_calls)


@pytest.mark.asyncio
async def test_pilot_respects_max_articles_and_binance_limit_bounds() -> None:
    http = AsyncHttpClient(
        transport=httpx.MockTransport(_announcement_vision_handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    source = _pilot_market_history()
    with pytest.raises(ConfigError):
        await run_listing_cohort_pilot(
            announcement_client=BinanceAnnouncementClient(http=http),
            vision_client=BinanceVisionClient(http=http),
            market_history=source,
            output_dir=Path("/tmp/unused"),
            requested_start=datetime(2024, 1, 1, tzinfo=UTC),
            requested_end=datetime(2024, 2, 1, tzinfo=UTC),
            max_pages=1,
            max_articles=20,
            page_size=20,
            max_probe_days=2,
            binance_limit=1001,
        )
    await http.aclose()
    assert source.klines_calls == []
    assert source.agg_calls == []
