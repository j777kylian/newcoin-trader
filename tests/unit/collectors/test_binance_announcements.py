"""Phase 8.1 Binance CMS announcement collector (fixture HTTP; no network)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from newcoin_trader.collectors.binance.announcements import (
    ARTICLE_DETAIL_PATH,
    ARTICLE_LIST_PATH,
    MAX_ARTICLES_CAP,
    MAX_PAGES_CAP,
    NEW_LISTING_CATALOG_ID,
    BinanceAnnouncementClient,
    normalize_announcement_list,
    normalize_article_detail_body,
    validate_collect_bounds,
)
from newcoin_trader.collectors.http import AsyncHttpClient
from newcoin_trader.domain.listing_cohort import ListingAnnouncement
from newcoin_trader.errors import ConfigError, ParseError

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures" / "binance" / "announcements"


def _load(name: str) -> object:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_normalize_list_reads_catalog_48_articles() -> None:
    page = normalize_announcement_list(_load("list_page1.json"))
    assert page.catalog_id == NEW_LISTING_CATALOG_ID
    assert page.total == 2231
    assert len(page.articles) == 9
    first = page.articles[0]
    assert isinstance(first, ListingAnnouncement)
    assert first.code == "spot-new-explicit"
    assert first.id == "101"
    assert first.release_date_ms == 1705233600000
    assert "NEW/USDT" in first.title
    assert first.type == "1"
    assert first.provenance["source"] == "binance:cms:article_list"
    assert first.provenance["catalog_id"] == "48"


def test_normalize_list_malformed_payloads_raise() -> None:
    with pytest.raises(ParseError):
        normalize_announcement_list({"data": {}})
    with pytest.raises(ParseError):
        normalize_announcement_list("nope")
    with pytest.raises(ParseError):
        normalize_announcement_list({"data": {"catalogs": []}})
    with pytest.raises(ParseError):
        normalize_announcement_list({"data": {"catalogs": [{"articles": [None]}]}})


def test_normalize_article_detail_body_strips_html() -> None:
    body = normalize_article_detail_body(_load("detail_spot_new.json"))
    assert "NEW/USDT" in body
    assert "<" not in body
    assert "2024-01-14 08:00 (UTC)" in body


def test_normalize_article_detail_empty_body_is_empty_string() -> None:
    assert normalize_article_detail_body(_load("detail_empty_body.json")) == ""


@pytest.mark.asyncio
async def test_collect_uses_bounded_pagination_and_stops() -> None:
    seen: list[tuple[str, dict[str, Any]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        params = dict(request.url.params)
        seen.append((path, params))
        assert request.method == "GET"
        if path == ARTICLE_LIST_PATH:
            page_no = params.get("pageNo")
            if page_no == "1":
                return httpx.Response(200, json=_load("list_page1.json"))
            if page_no == "2":
                return httpx.Response(200, json=_load("list_page2.json"))
            return httpx.Response(200, json=_load("list_empty.json"))
        raise AssertionError(f"unexpected path {path}")

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    client = BinanceAnnouncementClient(http=http, base_url="https://www.binance.com")
    articles = await client.collect(max_pages=3, max_articles=20, page_size=9)
    await http.aclose()

    codes = [a.code for a in articles]
    assert "spot-new-explicit" in codes
    assert "spot-page2" in codes
    assert len(articles) == 10
    assert all(path == ARTICLE_LIST_PATH for path, _ in seen)
    assert seen[0][1]["type"] == "1"
    assert seen[0][1]["catalogId"] == "48"
    assert seen[0][1]["pageNo"] == "1"


def test_collect_bounds_support_full_1095_day_traversal_with_20_item_pages() -> None:
    assert MAX_PAGES_CAP == 100
    assert MAX_ARTICLES_CAP == 2000
    validate_collect_bounds(max_pages=100, max_articles=2000, page_size=20)
    with pytest.raises(ConfigError, match="max_pages"):
        validate_collect_bounds(max_pages=101, max_articles=2000, page_size=20)
    with pytest.raises(ConfigError, match="max_articles"):
        validate_collect_bounds(max_pages=100, max_articles=2001, page_size=20)


@pytest.mark.asyncio
async def test_collect_respects_max_articles_and_max_pages_caps() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=_load("list_page1.json"))

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    client = BinanceAnnouncementClient(http=http)
    articles = await client.collect(max_pages=5, max_articles=4, page_size=9)
    await http.aclose()
    assert len(articles) == 4
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_fetch_detail_uses_article_code_query() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(f"{request.method} {request.url.path}?{request.url.query.decode()}")
        return httpx.Response(200, json=_load("detail_spot_new.json"))

    http = AsyncHttpClient(
        transport=httpx.MockTransport(handler),
        max_attempts=1,
        rate_limit_per_second=1000.0,
    )
    client = BinanceAnnouncementClient(http=http)
    body = await client.fetch_detail("spot-will-list")
    await http.aclose()
    assert "NEW/USDT" in (body or "")
    assert seen == [f"GET {ARTICLE_DETAIL_PATH}?articleCode=spot-will-list"]
