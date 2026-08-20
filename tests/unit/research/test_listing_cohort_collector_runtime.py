"""Phase 8.1C: title prefilter, detail-fetch efficiency, CMS HTTP policy (no network)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from newcoin_trader.collectors.binance.announcements import (
    BinanceAnnouncementClient,
    create_cms_http_client,
)
from newcoin_trader.collectors.http import AsyncHttpClient
from newcoin_trader.research.listing_cohort import (
    detail_fetch_expectation_for_titles,
    prefilter_title,
)
from newcoin_trader.research.listing_cohort_run import collect_classified_listings

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


def _article(*, code: str, title: str, released: datetime, article_id: int = 1) -> dict[str, Any]:
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


def _tracking_client(
    pages: dict[str, list[dict[str, Any]]],
    bodies: dict[str, str],
) -> tuple[BinanceAnnouncementClient, AsyncHttpClient, list[str]]:
    detail_codes: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        path = request.url.path
        params = dict(request.url.params)
        if path.endswith("/article/list/query"):
            page_no = params.get("pageNo") or "1"
            return httpx.Response(200, json=_cms_list(pages.get(page_no, [])))
        if path.endswith("/article/detail/query"):
            code = params.get("articleCode") or ""
            detail_codes.append(code)
            body = bodies.get(code)
            if body is None:
                return httpx.Response(404, text="no detail")
            return httpx.Response(
                200,
                json={"code": "000000", "data": {"code": code, "body": body}},
            )
        return httpx.Response(404, text="missing")

    http = AsyncHttpClient(transport=httpx.MockTransport(handler), max_attempts=1, rate_limit_per_second=1000.0)
    return BinanceAnnouncementClient(http=http), http, detail_codes


def _sample_38_titles() -> list[str]:
    """Previously-seen pilot in-window scan: 36 title-excludable + 2 detail candidates."""
    titles: list[str] = []
    titles.extend(row["title"] for row in _CASES["bstocks"])
    titles.append(_CASES["futures_launch"]["title"])
    titles.append(_CASES["collateral_addition"]["title"])
    titles.append(_CASES["pair_addition_already_listed"]["title"])
    titles.append(_CASES["spot_delist"]["title"])
    titles.extend(
        [
            "Binance Futures Will Launch USDⓈ-M BTCUSDT Perpetual Contract With Up to 75x Leverage",
            "Binance Futures Will Launch COIN-M ETHUSD Perpetual Contract",
            "Binance Will Add ABC on Isolated Margin and Cross Margin",
            "Binance Alpha Will List Mystery Token (MYST)",
            "Binance Will Delist FOOBAR and Remove Spot Trading Pairs",
            "Binance Will Add OLD2/FDUSD Spot Trading Pair",
            "Binance Will List QQQ as a New Collateral Asset on Portfolio Margin",
            "Binance Will List SPY as Collateral on Portfolio Margin",
            "Binance Will List Microsoft Tokenized Stock (MSFTx) bStocks",
            "Binance Futures Will Launch USD-M DOGEUSDT Perpetual Contract",
            "Binance Will Add XYZ on Isolated Margin",
            "Binance Will Add DEF on Cross Margin",
            "Binance Will Delist ALPHAB and Remove Spot Trading Pairs",
            "Binance Will Add GHI/USDC Spot Trading Pair",
            "Binance Will Add JKL/TRY Spot Trading Pair",
            "Binance Will List TSLA Tokenized Stock (TSLAx)",
            "Binance Futures Will Launch BNBUSD Perpetual Contract With Up to 25x Leverage",
            "Binance Will List AMZN Tokenized Securities (AMZNx)",
            "Binance Will Add MNO/BNB Spot Trading Pair",
            "Binance Will Delist QRSUSDT and Remove Spot Trading Pairs",
            "Binance Alpha Will List Beta Token (BETA)",
            "Binance Futures Will Launch ADAUSDT Perpetual Contract",
            "Binance Will Add PQR/ETH Spot Trading Pair",
            "Binance Will Delist STUUSDT Spot Trading Pairs",
            "Binance Will Add VWX/USDT Spot Trading Pair",
        ]
    )
    titles.append("Binance Will List Newcoin (NEW)")
    titles.append(_CASES["aero_spot_plus_alpha_delist"]["title"])
    assert len(titles) == 38
    return titles


# --- prefilter_title unit tests ---


def test_prefilter_futures_title_returns_not_spot_futures() -> None:
    title = _CASES["futures_launch"]["title"]
    assert prefilter_title(title) == "not_spot_futures"


def test_prefilter_collateral_title_returns_not_spot_collateral() -> None:
    assert prefilter_title(_CASES["collateral_addition"]["title"]) == "not_spot_collateral"


@pytest.mark.parametrize("row", _CASES["bstocks"], ids=lambda row: str(row["code"]))
def test_prefilter_bstock_title_returns_tokenized_security(row: dict[str, str]) -> None:
    assert prefilter_title(row["title"]) == "tokenized_security"


def test_prefilter_pair_addition_title_returns_not_spot_pair_addition() -> None:
    assert prefilter_title(_CASES["pair_addition_already_listed"]["title"]) == "not_spot_pair_addition"


def test_prefilter_obvious_non_candidate_titles_without_detail() -> None:
    assert prefilter_title("Binance Will Delist XYZUSDT and Remove Spot Trading Pairs") == "not_spot_delisting"
    assert prefilter_title("Binance Alpha Will List Mystery Token (MYST)") == "not_spot_alpha"
    assert prefilter_title("Binance Will Add ABC on Isolated Margin and Cross Margin") == "not_spot_margin"


def test_prefilter_returns_none_for_ambiguous_or_probable_spot_titles() -> None:
    assert prefilter_title("Binance Will List Newcoin (NEW)") is None
    assert prefilter_title(_CASES["aero_spot_plus_alpha_delist"]["title"]) is None
    assert prefilter_title("Important Notice Regarding New Cryptocurrency Listing Process") is None
    assert (
        prefilter_title("Binance Will Open Trading for NEW/USDT, NEW/USDC Spot Trading Pairs at 2024-01-15 08:00 (UTC)")
        is None
    )


def test_prefilter_never_overrides_uncertain_will_list_titles() -> None:
    uncertain = [
        "Binance Will List Aerodrome Finance (AERO)",
        "Binance Will List Foo Token (FOO)",
        "Binance Will List Something (XYZ)",
    ]
    assert all(prefilter_title(title) is None for title in uncertain)


def test_sample_38_detail_fetch_expectation() -> None:
    before, after = detail_fetch_expectation_for_titles(_sample_38_titles())
    assert before == 38
    assert after == 2


# --- collect_classified_listings integration (detail fetch tracking) ---


@pytest.mark.asyncio
async def test_futures_exclusion_skips_detail_fetch() -> None:
    title = _CASES["futures_launch"]["title"]
    articles = [_article(code="futures-1", title=title, released=NOW - timedelta(days=1))]
    client, http, detail_codes = _tracking_client({"1": articles}, {})
    classified = await collect_classified_listings(
        announcement_client=client,
        requested_start=NOW - timedelta(days=30),
        requested_end=NOW + timedelta(days=1),
        max_pages=1,
        max_articles=10,
        page_size=10,
        now_utc=NOW,
        target_valid_crypto_listings=20,
    )
    await http.aclose()
    assert detail_codes == []
    assert classified.exclusions[0].reason == "not_spot_futures"
    assert classified.parsed[0].provenance.get("detail_fetch") == "skipped"


@pytest.mark.asyncio
async def test_collateral_exclusion_skips_detail_fetch() -> None:
    title = _CASES["collateral_addition"]["title"]
    articles = [_article(code="collateral-1", title=title, released=NOW - timedelta(days=1))]
    client, http, detail_codes = _tracking_client({"1": articles}, {})
    classified = await collect_classified_listings(
        announcement_client=client,
        requested_start=NOW - timedelta(days=30),
        requested_end=NOW + timedelta(days=1),
        max_pages=1,
        max_articles=10,
        page_size=10,
        now_utc=NOW,
    )
    await http.aclose()
    assert detail_codes == []
    assert classified.exclusions[0].reason == "not_spot_collateral"


@pytest.mark.asyncio
async def test_bstock_exclusion_skips_detail_fetch() -> None:
    row = _CASES["bstocks"][0]
    articles = [_article(code=row["code"], title=row["title"], released=NOW - timedelta(days=1))]
    client, http, detail_codes = _tracking_client({"1": articles}, {})
    classified = await collect_classified_listings(
        announcement_client=client,
        requested_start=NOW - timedelta(days=30),
        requested_end=NOW + timedelta(days=1),
        max_pages=1,
        max_articles=10,
        page_size=10,
        now_utc=NOW,
    )
    await http.aclose()
    assert detail_codes == []
    assert classified.exclusions[0].reason == "tokenized_security"


@pytest.mark.asyncio
async def test_obvious_non_candidate_skips_detail_fetch() -> None:
    title = _CASES["pair_addition_already_listed"]["title"]
    articles = [_article(code="pair-add", title=title, released=NOW - timedelta(days=1))]
    client, http, detail_codes = _tracking_client({"1": articles}, {})
    classified = await collect_classified_listings(
        announcement_client=client,
        requested_start=NOW - timedelta(days=30),
        requested_end=NOW + timedelta(days=1),
        max_pages=1,
        max_articles=10,
        page_size=10,
        now_utc=NOW,
    )
    await http.aclose()
    assert detail_codes == []
    assert classified.exclusions[0].reason == "not_spot_pair_addition"


@pytest.mark.asyncio
async def test_ambiguous_will_list_fetches_detail() -> None:
    when = NOW - timedelta(days=2)
    articles = [_article(code="spot-will-list", title="Binance Will List Newcoin (NEW)", released=when)]
    bodies = {"spot-will-list": _spot_body("NEW", when)}
    client, http, detail_codes = _tracking_client({"1": articles}, bodies)
    classified = await collect_classified_listings(
        announcement_client=client,
        requested_start=NOW - timedelta(days=30),
        requested_end=NOW + timedelta(days=1),
        max_pages=1,
        max_articles=10,
        page_size=10,
        now_utc=NOW,
    )
    await http.aclose()
    assert detail_codes == ["spot-will-list"]
    assert classified.valid[0].symbol == "NEWUSDT"


@pytest.mark.asyncio
async def test_aero_still_included_after_body_classification() -> None:
    case = _CASES["aero_spot_plus_alpha_delist"]
    when = NOW - timedelta(days=3)
    articles = [_article(code="aero", title=case["title"], released=when)]
    bodies = {"aero": case["body"]}
    client, http, detail_codes = _tracking_client({"1": articles}, bodies)
    classified = await collect_classified_listings(
        announcement_client=client,
        requested_start=NOW - timedelta(days=30),
        requested_end=NOW + timedelta(days=1),
        max_pages=1,
        max_articles=10,
        page_size=10,
        now_utc=NOW,
        target_valid_crypto_listings=1,
    )
    await http.aclose()
    assert detail_codes == ["aero"]
    assert len(classified.valid) == 1
    assert classified.valid[0].symbol == "AEROUSDT"
    assert classified.valid[0].exclusion_reason is None


@pytest.mark.asyncio
async def test_sample_38_scan_skips_most_detail_fetches() -> None:
    when = NOW - timedelta(days=5)
    titles = _sample_38_titles()
    articles = [
        _article(code=f"art-{i:02d}", title=title, released=NOW - timedelta(hours=i + 1), article_id=100 + i)
        for i, title in enumerate(titles)
    ]
    bodies = {
        "art-36": _spot_body("NEW", when),
        "art-37": _CASES["aero_spot_plus_alpha_delist"]["body"],
    }
    client, http, detail_codes = _tracking_client({"1": articles}, bodies)
    classified = await collect_classified_listings(
        announcement_client=client,
        requested_start=NOW - timedelta(days=400),
        requested_end=NOW + timedelta(days=1),
        max_pages=1,
        max_articles=50,
        page_size=50,
        now_utc=NOW,
        target_valid_crypto_listings=2,
    )
    await http.aclose()
    assert len(detail_codes) == 2
    assert set(detail_codes) == {"art-36", "art-37"}
    assert len(classified.valid) == 2
    valid_symbols = {row.symbol for row in classified.valid}
    assert valid_symbols == {"NEWUSDT", "AEROUSDT"}


def test_create_cms_http_client_defaults_to_one_request_per_second() -> None:
    async def _check() -> None:
        client = create_cms_http_client(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json={})),
        )
        assert client._limiter._rate == 1.0  # noqa: SLF001
        assert client._rate_limit_429_cooldowns == (15.0, 30.0, 60.0, 120.0)  # noqa: SLF001
        await client.aclose()

    import asyncio

    asyncio.run(_check())
