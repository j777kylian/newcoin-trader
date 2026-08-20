"""Phase 8.1B: 50 most-recent valid crypto listings and UTC end clamp (no network)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from newcoin_trader.collectors.binance.announcements import BinanceAnnouncementClient
from newcoin_trader.collectors.binance.vision import BinanceVisionClient
from newcoin_trader.collectors.http import AsyncHttpClient
from newcoin_trader.domain.listing_cohort import SpotClass
from newcoin_trader.research.listing_cohort_config import PILOT_LOOKBACK, TARGET_VALID_CRYPTO_LISTINGS
from newcoin_trader.research.listing_cohort_run import collect_classified_listings, run_listing_cohort_pipeline

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures" / "binance" / "announcements"
_CASES = json.loads((FIXTURES / "classifier_cases.json").read_text(encoding="utf-8"))
NOW = datetime(2025, 6, 15, 12, 0, tzinfo=UTC)


def _ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _cms_list(articles: list[dict[str, Any]], *, total: int | None = None) -> dict[str, Any]:
    return {
        "code": "000000",
        "message": None,
        "data": {
            "catalogs": [
                {
                    "catalogId": 48,
                    "catalogName": "New Cryptocurrency Listing",
                    "total": total if total is not None else len(articles),
                    "articles": articles,
                }
            ]
        },
    }


def _article(
    *,
    code: str,
    title: str,
    released: datetime,
    article_id: int = 1,
) -> dict[str, Any]:
    return {
        "id": article_id,
        "code": code,
        "title": title,
        "type": 1,
        "releaseDate": _ms(released),
    }


def _spot_title(symbol_base: str, when: datetime) -> str:
    clock = when.strftime("%Y-%m-%d %H:%M")
    return (
        f"Binance Will List Coin {symbol_base} ({symbol_base}) and Open Trading for "
        f"{symbol_base}/USDT Spot Trading Pairs at {clock} (UTC)"
    )


def _spot_body(symbol_base: str, when: datetime) -> str:
    clock = when.strftime("%Y-%m-%d %H:%M")
    return (
        f"Binance will list Coin {symbol_base} ({symbol_base}) and open trading for the following "
        f"spot trading pairs at {clock} (UTC):\n{symbol_base}/USDT"
    )


def _client_for(
    pages: dict[str, list[dict[str, Any]]],
    bodies: dict[str, str],
) -> tuple[BinanceAnnouncementClient, AsyncHttpClient]:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        path = request.url.path
        params = dict(request.url.params)
        if path.endswith("/article/list/query"):
            page_no = params.get("pageNo") or "1"
            articles = pages.get(page_no, [])
            return httpx.Response(200, json=_cms_list(articles))
        if path.endswith("/article/detail/query"):
            code = params.get("articleCode") or ""
            body = bodies.get(code)
            if body is None:
                return httpx.Response(404, text="no detail")
            return httpx.Response(
                200,
                json={"code": "000000", "data": {"code": code, "body": body}},
            )
        return httpx.Response(404, text="missing")

    http = AsyncHttpClient(transport=httpx.MockTransport(handler), max_attempts=1, rate_limit_per_second=1000.0)
    return BinanceAnnouncementClient(http=http), http


@pytest.mark.asyncio
async def test_selects_fifty_most_recent_valid_listings_not_first_fifty_articles() -> None:
    genuine_times = [NOW - timedelta(days=i + 1) for i in range(55)]
    genuine = [
        _article(
            code=f"spot-{i:02d}",
            title=_spot_title(f"C{i:02d}", genuine_times[i]),
            released=genuine_times[i],
            article_id=1000 + i,
        )
        for i in range(55)
    ]
    junk = [
        _article(
            code="futures-junk",
            title="Binance Futures Will Launch USDⓈ-M OLDUSDT Perpetual Contract With Up to 50x Leverage",
            released=NOW - timedelta(hours=1),
            article_id=1,
        ),
        _article(
            code="alpha-junk",
            title="Binance Alpha Will List Mystery Token (MYST)",
            released=NOW - timedelta(hours=2),
            article_id=2,
        ),
        _article(
            code="pair-junk",
            title="Binance Will Add OLD/FDUSD Spot Trading Pair",
            released=NOW - timedelta(hours=3),
            article_id=3,
        ),
    ]
    # Newest-first catalog: junk then 55 genuine (oldest last).
    newest_first = junk + genuine
    pages = {
        "1": newest_first[:10],
        "2": newest_first[10:20],
        "3": newest_first[20:30],
        "4": newest_first[30:40],
        "5": newest_first[40:50],
        "6": newest_first[50:60],
        "7": [],
    }
    bodies = {row["code"]: _spot_body(f"C{i:02d}", genuine_times[i]) for i, row in enumerate(genuine)}
    bodies["pair-junk"] = _CASES["pair_addition_already_listed"]["body"]
    client, http = _client_for(pages, bodies)
    classified = await collect_classified_listings(
        announcement_client=client,
        requested_start=NOW - timedelta(days=400),
        requested_end=NOW + timedelta(days=30),
        max_pages=10,
        max_articles=500,
        page_size=10,
        now_utc=NOW,
        target_valid_crypto_listings=TARGET_VALID_CRYPTO_LISTINGS,
    )
    await http.aclose()
    assert classified.selection["target_valid_crypto_listings"] == 50
    assert classified.selection["selected"] == 50
    assert classified.selection["stop_reason"] == "target_reached"
    assert len(classified.valid) == 50
    codes = [row.announcement_code for row in classified.valid]
    assert "spot-00" in codes
    assert "spot-49" in codes
    assert "spot-50" not in codes
    assert "spot-54" not in codes
    assert "futures-junk" not in codes
    assert "pair-junk" not in codes
    assert all(row.classification is SpotClass.SPOT_LISTING for row in classified.valid)
    assert classified.effective_end == NOW
    assert classified.requested_end == NOW + timedelta(days=30)


@pytest.mark.asyncio
async def test_selection_audit_reports_newest_and_oldest_scanned_release_timestamps() -> None:
    newest = NOW - timedelta(days=1)
    middle = NOW - timedelta(days=5)
    oldest = NOW - timedelta(days=10)
    articles = [
        _article(code="spot-new", title=_spot_title("NEW", newest), released=newest, article_id=1),
        _article(code="spot-mid", title=_spot_title("MID", middle), released=middle, article_id=2),
        _article(code="spot-old", title=_spot_title("OLD", oldest), released=oldest, article_id=3),
    ]
    bodies = {
        "spot-new": _spot_body("NEW", newest),
        "spot-mid": _spot_body("MID", middle),
        "spot-old": _spot_body("OLD", oldest),
    }
    client, http = _client_for({"1": articles, "2": []}, bodies)
    classified = await collect_classified_listings(
        announcement_client=client,
        requested_start=NOW - timedelta(days=30),
        requested_end=NOW + timedelta(days=1),
        max_pages=2,
        max_articles=50,
        page_size=20,
        now_utc=NOW,
        target_valid_crypto_listings=50,
    )
    await http.aclose()
    assert classified.selection["newest_scanned_release_at"] == newest.isoformat()
    assert classified.selection["oldest_scanned_release_at"] == oldest.isoformat()
    assert classified.selection["selected"] == 3
    assert [row.announcement_code for row in classified.valid] == ["spot-new", "spot-mid", "spot-old"]


@pytest.mark.asyncio
async def test_selection_audit_release_bounds_are_null_when_nothing_scanned() -> None:
    future = NOW + timedelta(days=2)
    articles = [
        _article(code="spot-future", title=_spot_title("FUT", future), released=future, article_id=1),
    ]
    bodies = {"spot-future": _spot_body("FUT", future)}
    client, http = _client_for({"1": articles, "2": []}, bodies)
    classified = await collect_classified_listings(
        announcement_client=client,
        requested_start=NOW - timedelta(days=30),
        requested_end=NOW + timedelta(days=1),
        max_pages=2,
        max_articles=50,
        page_size=20,
        now_utc=NOW,
        target_valid_crypto_listings=50,
        fetch_details=False,
    )
    await http.aclose()
    assert classified.selection["newest_scanned_release_at"] is None
    assert classified.selection["oldest_scanned_release_at"] is None
    assert classified.selection["articles_scanned"] == 0
    assert classified.selection["selected"] == 0
    assert classified.valid == ()


@pytest.mark.asyncio
async def test_lookback_exhaustion_uses_available_n_and_does_not_weaken_classifier() -> None:
    inside = NOW - timedelta(days=10)
    outside = NOW - PILOT_LOOKBACK - timedelta(days=5)
    articles = [
        _article(
            code="futures-recent",
            title="Binance Futures Will Launch USDⓈ-M OLDUSDT Perpetual Contract With Up to 50x Leverage",
            released=NOW - timedelta(days=1),
            article_id=1,
        ),
        _article(
            code="spot-recent",
            title=_spot_title("NEW", inside),
            released=inside,
            article_id=2,
        ),
        _article(
            code="spot-too-old",
            title=_spot_title("OLD", outside),
            released=outside,
            article_id=3,
        ),
    ]
    bodies = {
        "spot-recent": _spot_body("NEW", inside),
        "spot-too-old": _spot_body("OLD", outside),
        "bstock-pad": _CASES["bstocks"][0]["body"],
    }
    client, http = _client_for({"1": articles, "2": []}, bodies)
    classified = await collect_classified_listings(
        announcement_client=client,
        requested_start=NOW - timedelta(days=800),
        requested_end=NOW + timedelta(days=1),
        max_pages=5,
        max_articles=500,
        page_size=20,
        now_utc=NOW,
        target_valid_crypto_listings=50,
    )
    await http.aclose()
    assert classified.selection["selected"] == 1
    assert classified.selection["stop_reason"] == "lookback_exhausted"
    assert classified.selection["shortfall"] == 49
    assert classified.valid[0].announcement_code == "spot-recent"
    assert classified.valid[0].symbol == "NEWUSDT"
    excluded_codes = {row.announcement_code for row in classified.exclusions}
    assert "futures-recent" in excluded_codes
    assert "spot-too-old" not in {row.announcement_code for row in classified.valid}


@pytest.mark.asyncio
async def test_bstocks_are_not_used_to_pad_the_pilot_to_fifty() -> None:
    genuine_time = NOW - timedelta(days=2)
    bstock_articles = [
        _article(
            code=row["code"],
            title=row["title"],
            released=NOW - timedelta(hours=i + 1),
            article_id=10 + i,
        )
        for i, row in enumerate(_CASES["bstocks"])
    ]
    genuine = _article(
        code="spot-only",
        title=_spot_title("AAA", genuine_time),
        released=genuine_time,
        article_id=99,
    )
    bodies = {row["code"]: row["body"] for row in _CASES["bstocks"]}
    bodies["spot-only"] = _spot_body("AAA", genuine_time)
    client, http = _client_for({"1": [*bstock_articles, genuine], "2": []}, bodies)
    classified = await collect_classified_listings(
        announcement_client=client,
        requested_start=NOW - timedelta(days=30),
        requested_end=NOW + timedelta(days=1),
        max_pages=3,
        max_articles=100,
        page_size=20,
        now_utc=NOW,
        target_valid_crypto_listings=50,
    )
    await http.aclose()
    assert [row.announcement_code for row in classified.valid] == ["spot-only"]
    bstock_reasons = {row.reason for row in classified.exclusions if row.announcement_code.startswith("bstock")}
    assert bstock_reasons == {"tokenized_security"}
    assert classified.selection["selected"] == 1
    assert classified.selection["selected"] < 50


@pytest.mark.asyncio
async def test_future_articles_are_dropped_when_requested_end_is_clamped(
    tmp_path: Path,
) -> None:
    now = datetime(2024, 1, 20, tzinfo=UTC)
    future = datetime(2024, 1, 25, tzinfo=UTC)
    past = datetime(2024, 1, 10, tzinfo=UTC)
    articles = [
        _article(code="spot-future", title=_spot_title("FUT", future), released=future, article_id=1),
        _article(code="spot-past", title=_spot_title("PST", past), released=past, article_id=2),
    ]
    bodies = {
        "spot-future": _spot_body("FUT", future),
        "spot-past": _spot_body("PST", past),
    }
    client, http = _client_for({"1": articles, "2": []}, bodies)
    vision = BinanceVisionClient(http=http)
    report = await run_listing_cohort_pipeline(
        announcement_client=client,
        vision_client=vision,
        output_dir=tmp_path,
        requested_start=datetime(2024, 1, 1, tzinfo=UTC),
        requested_end=datetime(2024, 2, 1, tzinfo=UTC),
        max_pages=2,
        max_articles=50,
        page_size=20,
        max_probe_days=1,
        now_utc=now,
        fetch_details=True,
    )
    await http.aclose()
    coverage = json.loads((tmp_path / "coverage.json").read_text(encoding="utf-8"))
    assert coverage["requested_period"]["end"].startswith("2024-02-01")
    assert coverage["effective_period"]["end"].startswith("2024-01-20")
    assert coverage["now_utc"].startswith("2024-01-20")
    assert coverage["cohort_selection"]["selected"] == report.cohort_count
    listings = json.loads((tmp_path / "listing_cohort.json").read_text(encoding="utf-8"))["listings"]
    listing_codes = {row["announcement_code"] for row in listings}
    assert "spot-past" in listing_codes
    assert "spot-future" not in listing_codes
    summary = (tmp_path / "phase81_summary.md").read_text(encoding="utf-8")
    assert "requested_end" in summary
    assert "effective_end" in summary
    assert "0m delay" not in summary
