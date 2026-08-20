"""Read-only Binance CMS announcement collector (catalog 48, no auth)."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Mapping
from html import unescape
from typing import Any

import httpx

from newcoin_trader.collectors.http import AsyncHttpClient, GetJsonClient
from newcoin_trader.collectors.normalization import (
    guard_parse,
    parse_int,
    require_list,
    require_mapping,
)
from newcoin_trader.domain.listing_cohort import AnnouncementListPage, ListingAnnouncement
from newcoin_trader.errors import ConfigError, NotFoundError, ParseError

ARTICLE_LIST_PATH = "/bapi/composite/v1/public/cms/article/list/query"
ARTICLE_DETAIL_PATH = "/bapi/composite/v1/public/cms/article/detail/query"
DEFAULT_CMS_BASE_URL = "https://www.binance.com"
NEW_LISTING_CATALOG_ID = 48
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 50
MAX_PAGES_CAP = 100
MAX_ARTICLES_CAP = 2000
DEFAULT_CMS_RATE_LIMIT_PER_SECOND = 1.0
CMS_429_COOLDOWNS = (15.0, 30.0, 60.0, 120.0)
CMS_MAX_ATTEMPTS = 5

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def strip_html(text: str) -> str:
    plain = _HTML_TAG_RE.sub(" ", unescape(text))
    return _WS_RE.sub(" ", plain).strip()


def _stringify(value: Any, *, context: str) -> str:
    if value is None or isinstance(value, bool):
        raise ParseError(f"{context}: missing")
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ParseError(f"{context}: empty")
        return text
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    raise ParseError(f"{context}: invalid {type(value).__name__}")


def _optional_stringify(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, int):
        return str(value)
    return str(value)


@guard_parse("binance cms article list")
def normalize_announcement_list(payload: Any) -> AnnouncementListPage:
    root = require_mapping(payload, context="cms_list")
    data = require_mapping(root.get("data"), context="cms_list.data")
    catalogs = require_list(data.get("catalogs"), context="cms_list.data.catalogs")
    if not catalogs:
        raise ParseError("cms_list.data.catalogs: empty")
    catalog = require_mapping(catalogs[0], context="cms_list.data.catalogs[0]")
    catalog_id = parse_int(catalog.get("catalogId"), context="cms_list.catalogId")
    total = parse_int(catalog.get("total"), context="cms_list.total")
    raw_articles = require_list(catalog.get("articles"), context="cms_list.articles")
    articles: list[ListingAnnouncement] = []
    for idx, raw in enumerate(raw_articles):
        item = require_mapping(raw, context=f"cms_list.articles[{idx}]")
        code = _stringify(item.get("code"), context=f"cms_list.articles[{idx}].code")
        article_id = _stringify(item.get("id"), context=f"cms_list.articles[{idx}].id")
        title = item.get("title")
        if not isinstance(title, str) or not title.strip():
            raise ParseError(f"cms_list.articles[{idx}].title: missing")
        release_date_ms = parse_int(item.get("releaseDate"), context=f"cms_list.articles[{idx}].releaseDate")
        if release_date_ms <= 0:
            raise ParseError(f"cms_list.articles[{idx}].releaseDate: non-positive")
        articles.append(
            ListingAnnouncement(
                code=code,
                id=article_id,
                release_date_ms=release_date_ms,
                title=title.strip(),
                type=_optional_stringify(item.get("type")),
                provenance={
                    "source": "binance:cms:article_list",
                    "catalog_id": str(catalog_id),
                    "endpoint": ARTICLE_LIST_PATH,
                },
            )
        )
    return AnnouncementListPage(catalog_id=catalog_id, total=total, articles=tuple(articles))


@guard_parse("binance cms article detail")
def normalize_article_detail_body(payload: Any) -> str:
    root = require_mapping(payload, context="cms_detail")
    data = require_mapping(root.get("data"), context="cms_detail.data")
    body = data.get("body")
    if body is None:
        return ""
    if not isinstance(body, str):
        raise ParseError("cms_detail.data.body: expected string")
    return strip_html(body)


def validate_collect_bounds(*, max_pages: int, max_articles: int, page_size: int) -> None:
    if max_pages < 1 or max_pages > MAX_PAGES_CAP:
        raise ConfigError(f"max_pages must be in [1, {MAX_PAGES_CAP}]")
    if max_articles < 1 or max_articles > MAX_ARTICLES_CAP:
        raise ConfigError(f"max_articles must be in [1, {MAX_ARTICLES_CAP}]")
    if page_size < 1 or page_size > MAX_PAGE_SIZE:
        raise ConfigError(f"page_size must be in [1, {MAX_PAGE_SIZE}]")


def create_cms_http_client(
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    timeout_seconds: float = 15.0,
    max_attempts: int = CMS_MAX_ATTEMPTS,
    backoff_seconds: float = 0.25,
    rate_limit_per_second: float = DEFAULT_CMS_RATE_LIMIT_PER_SECOND,
    headers: Mapping[str, str] | None = None,
    sleep: Any = None,
) -> AsyncHttpClient:
    """Conservative CMS-only HTTP client (<=1 req/s; bounded CloudFront 429 cooldowns)."""
    return AsyncHttpClient(
        transport=transport,
        timeout_seconds=timeout_seconds,
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        rate_limit_per_second=rate_limit_per_second,
        rate_limit_429_cooldowns=CMS_429_COOLDOWNS,
        headers=headers,
        sleep=sleep or asyncio.sleep,
    )


class BinanceAnnouncementClient:
    """GET-only CMS list/detail client. No auth, no order endpoints."""

    def __init__(self, *, http: GetJsonClient, base_url: str = DEFAULT_CMS_BASE_URL) -> None:
        self._http = http
        self._base = base_url.rstrip("/")

    async def list_page(
        self,
        *,
        page_no: int,
        page_size: int,
        catalog_id: int = NEW_LISTING_CATALOG_ID,
    ) -> AnnouncementListPage:
        payload = await self._http.get_json(
            f"{self._base}{ARTICLE_LIST_PATH}",
            params={
                "type": 1,
                "catalogId": catalog_id,
                "pageNo": page_no,
                "pageSize": page_size,
            },
        )
        return normalize_announcement_list(payload)

    async def fetch_detail(self, article_code: str) -> str | None:
        try:
            payload = await self._http.get_json(
                f"{self._base}{ARTICLE_DETAIL_PATH}",
                params={"articleCode": article_code},
            )
        except NotFoundError:
            return None
        return normalize_article_detail_body(payload)

    async def collect(
        self,
        *,
        max_pages: int,
        max_articles: int,
        page_size: int = DEFAULT_PAGE_SIZE,
        catalog_id: int = NEW_LISTING_CATALOG_ID,
    ) -> tuple[ListingAnnouncement, ...]:
        validate_collect_bounds(max_pages=max_pages, max_articles=max_articles, page_size=page_size)
        collected: list[ListingAnnouncement] = []
        seen_codes: set[str] = set()
        for page_no in range(1, max_pages + 1):
            if len(collected) >= max_articles:
                break
            page = await self.list_page(page_no=page_no, page_size=page_size, catalog_id=catalog_id)
            if not page.articles:
                break
            for article in page.articles:
                if article.code in seen_codes:
                    continue
                seen_codes.add(article.code)
                collected.append(article)
                if len(collected) >= max_articles:
                    break
            if len(page.articles) < page_size:
                break
        return tuple(collected)
