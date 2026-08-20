"""Phase 8.1 cohort pipeline: coverage, event-study reuse, PIT features, artifacts A–H."""

from __future__ import annotations

import csv
import io
import json
import zipfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from newcoin_trader.collectors.binance.announcements import BinanceAnnouncementClient
from newcoin_trader.collectors.binance.vision import BinanceVisionClient
from newcoin_trader.collectors.http import AsyncHttpClient
from newcoin_trader.domain.enums import Venue
from newcoin_trader.domain.event_study import ObservationResolution
from newcoin_trader.domain.feature_research import FeatureMarketInput, FeatureTradeInput
from newcoin_trader.research.event_study_config import DEFAULT_ENTRY_DELAYS, DEFAULT_HOLDING_PERIODS, format_duration
from newcoin_trader.research.listing_cohort_config import PHASE81_SPLIT_RATIOS
from newcoin_trader.research.listing_cohort_run import (
    ALPHA_DISCOVERY_QUESTIONS,
    ARTIFACT_NAMES,
    run_listing_cohort_pipeline,
)

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures" / "binance" / "announcements"
T0 = datetime(2024, 1, 15, 8, 0, tzinfo=UTC)


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
    start_ms = int(start.timestamp() * 1000)
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
    start_ms = int(start.timestamp() * 1000)
    return _zip_csv(
        f"{symbol}-aggTrades-2024-01-15.csv",
        [[1, "1.00", "2.0", 1, 1, start_ms, "false"]],
    )


def _feature_inputs(symbol: str, start: datetime, minutes: int) -> tuple[FeatureMarketInput, ...]:
    rows: list[FeatureMarketInput] = []
    for i in range(minutes):
        ts = start + timedelta(minutes=i)
        price = Decimal("1.00") + Decimal("0.01") * i
        rows.append(
            FeatureMarketInput(
                token_address=symbol,
                chain="binance",
                venue=Venue.BINANCE,
                timestamp=ts,
                price=price,
                volume=Decimal("100"),
                liquidity=Decimal("105"),
                resolution=ObservationResolution.MINUTE,
                source="binance_vision:kline:1m",
                provenance={"kind": "kline", "interval": "1m"},
            )
        )
    return tuple(rows)


def _trade_inputs(symbol: str, start: datetime) -> tuple[FeatureTradeInput, ...]:
    return (
        FeatureTradeInput(
            token_address=symbol,
            chain="binance",
            venue=Venue.BINANCE,
            timestamp=start,
            side="buy",
            amount=Decimal("2.0"),
            price=Decimal("1.00"),
            source="binance_vision:aggTrades",
            provenance={"kind": "aggtrade"},
        ),
    )


@pytest.mark.asyncio
async def test_pipeline_emits_artifacts_a_through_h_and_answers_readiness(
    tmp_path: Path,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        path = request.url.path
        params = dict(request.url.params)
        if path.endswith("/article/list/query"):
            if params.get("pageNo") == "1":
                return httpx.Response(200, json=_load("list_page1.json"))
            return httpx.Response(200, json=_load("list_empty.json"))
        if path.endswith("/article/detail/query"):
            code = params.get("articleCode")
            mapping = {
                "spot-will-list": "detail_spot_new.json",
                "spot-missing-time": "detail_spot_missing_time.json",
                "spot-new-explicit": "detail_empty_body.json",
            }
            name = mapping.get(code or "")
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

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    announcements = BinanceAnnouncementClient(http=http)
    vision = BinanceVisionClient(http=http)
    extra_obs = _feature_inputs("NEWUSDT", T0, 120) + _feature_inputs(
        "LATEUSDT", datetime(2024, 1, 13, 6, 0, tzinfo=UTC), 120
    )
    extra_trades = _trade_inputs("NEWUSDT", T0) + _trade_inputs("LATEUSDT", datetime(2024, 1, 13, 6, 0, tzinfo=UTC))
    report = await run_listing_cohort_pipeline(
        announcement_client=announcements,
        vision_client=vision,
        output_dir=tmp_path,
        requested_start=datetime(2024, 1, 1, tzinfo=UTC),
        requested_end=datetime(2024, 2, 1, tzinfo=UTC),
        max_pages=1,
        max_articles=20,
        page_size=20,
        max_probe_days=2,
        lookback_before_days=0,
        fetch_details=True,
        market_inputs=extra_obs,
        trade_inputs=extra_trades,
    )
    await http.aclose()

    assert tuple(DEFAULT_ENTRY_DELAYS) == (
        timedelta(seconds=10),
        timedelta(seconds=30),
        timedelta(minutes=1),
        timedelta(minutes=2),
        timedelta(minutes=5),
        timedelta(minutes=10),
        timedelta(minutes=15),
        timedelta(minutes=30),
    )
    assert tuple(DEFAULT_HOLDING_PERIODS) == (
        timedelta(minutes=1),
        timedelta(minutes=5),
        timedelta(minutes=15),
        timedelta(minutes=30),
        timedelta(hours=1),
        timedelta(hours=2),
        timedelta(hours=4),
        timedelta(hours=24),
    )
    assert PHASE81_SPLIT_RATIOS[0] + PHASE81_SPLIT_RATIOS[1] + PHASE81_SPLIT_RATIOS[2] == Decimal("1")

    for key, filename in ARTIFACT_NAMES.items():
        path = tmp_path / filename
        assert path.exists(), f"missing artifact {key}: {filename}"

    cohort_json = json.loads((tmp_path / "listing_cohort.json").read_text(encoding="utf-8"))
    symbols = {row["symbol"] for row in cohort_json["listings"]}
    assert "NEWUSDT" in symbols
    assert "LATEUSDT" in symbols
    late = next(row for row in cohort_json["listings"] if row["symbol"] == "LATEUSDT")
    assert late["source_event_time"] is None
    assert late["source_event_time_status"] == "missing"
    assert late["first_market_data_time"] is not None
    new = next(row for row in cohort_json["listings"] if row["symbol"] == "NEWUSDT")
    assert new["source_event_time"] is not None
    assert new["source_event_time"] != new["release_date"]
    assert "completeness" in new
    assert "provenance" in new

    exclusions = list(csv.DictReader((tmp_path / "exclusions.csv").open(encoding="utf-8")))
    reasons = {row["reason"] for row in exclusions}
    assert any("not_spot" in r or "futures" in r or "margin" in r or "alpha" in r for r in reasons)
    assert any("ambiguous" in r for r in reasons)
    excluded_codes = {row["announcement_code"] for row in exclusions}
    assert "futures-perp" in excluded_codes
    assert "ambiguous-other" in excluded_codes

    coverage = json.loads((tmp_path / "coverage.json").read_text(encoding="utf-8"))
    assert coverage["requested_period"]["start"].startswith("2024-01-01")
    assert coverage["requested_period"]["end"].startswith("2024-02-01")
    assert coverage["effective_period"]["end"] == coverage["requested_period"]["end"]
    assert "now_utc" in coverage
    assert coverage["usable_period"]["start"] is not None
    assert coverage["counts"]["raw_articles"] == 9
    assert coverage["klines"]["available"] >= 1
    assert coverage["trades"]["available"] >= 1
    assert coverage["depth"]["status"] == "unsupported_historical_l2"
    assert "liquidity" in coverage

    return_matrix = json.loads((tmp_path / "return_matrix.json").read_text(encoding="utf-8"))
    cells = return_matrix["aggregates"]
    delay_hold = {(c["entry_delay"], c["holding_period"]) for c in cells}
    assert ("10s", "1m") in delay_hold
    assert ("30s", format_duration(timedelta(hours=24))) in delay_hold
    assert ("30m", "1h") in delay_hold
    assert ("0m", "5m") not in delay_hold
    assert len(delay_hold) == 8 * 8
    assert all("mean_simple_return" in c for c in cells)
    assert all("mean_mfe" in c for c in cells)

    event_rows = list(csv.DictReader((tmp_path / "event_study_cells.csv").open(encoding="utf-8")))
    assert event_rows
    statuses = {row["status"] for row in event_rows}
    assert statuses
    assert any(row["log_return"] for row in event_rows) or any(row["status"] != "complete" for row in event_rows)

    feature_rows = list(csv.DictReader((tmp_path / "pit_feature_dataset.csv").open(encoding="utf-8")))
    assert feature_rows
    feature_names = feature_rows[0].keys()
    assert "age_source_event_seconds" in feature_names
    assert "price_return_5m" in feature_names
    assert "volatility_5m" in feature_names
    assert "volume_sum_5m" in feature_names
    assert "liquidity_current" in feature_names
    assert "trade_count_5m" in feature_names
    assert not any("depth" in name or "spread" in name or "bid_" in name for name in feature_names)

    split = json.loads((tmp_path / "split_manifest.json").read_text(encoding="utf-8"))
    assert split["shuffled"] is False
    assert "train_count" in split and "validation_count" in split and "test_count" in split
    assert split["train_count"] + split["validation_count"] + split["test_count"] == len(feature_rows)

    readiness = json.loads((tmp_path / "alpha_discovery_readiness.json").read_text(encoding="utf-8"))
    assert set(readiness["questions"].keys()) == set(ALPHA_DISCOVERY_QUESTIONS)
    assert list(readiness["questions"].keys()) == sorted(ALPHA_DISCOVERY_QUESTIONS)
    for answer in readiness["questions"].values():
        assert "verdict" in answer
        assert "rationale" in answer
    assert readiness["gross_vs_executable_vs_prospective"]["gross"] == "this_pass"
    assert readiness["gross_vs_executable_vs_prospective"]["executable"] == "not_this_pass"
    assert readiness["gross_vs_executable_vs_prospective"]["prospective"] == "not_this_pass"
    assert readiness["rule_discovery"] == "not_run"

    summary = (tmp_path / "phase81_summary.md").read_text(encoding="utf-8")
    assert "Phase 8.1" in summary
    assert "NEWUSDT" in summary
    assert "0m delay" not in summary
    readiness_blob = json.dumps(readiness)
    assert "0m delay" not in readiness_blob
    assert "10s" in readiness_blob or "sub-minute" in readiness_blob.lower() or "subminute" in readiness_blob.lower()
    assert report.cohort_count >= 2
    assert report.exclusion_count >= 1


def test_artifact_names_are_a_through_h() -> None:
    assert list(ARTIFACT_NAMES) == ["A", "B", "C", "D", "E", "F", "G", "H"]
    assert len(ALPHA_DISCOVERY_QUESTIONS) == 5
