"""Conservative Binance Spot listing classification and field extraction."""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import UTC, datetime

from newcoin_trader.domain.listing_cohort import (
    CompletenessStatus,
    ListingAnnouncement,
    ParsedListing,
    SourceEventTimeStatus,
    SpotClass,
)
from newcoin_trader.domain.types import utc_from_millis

_QUOTE_PREFERENCE = (
    "USDT",
    "USDC",
    "FDUSD",
    "BNB",
    "BTC",
    "ETH",
    "EUR",
    "TRY",
    "BRL",
    "TUSD",
)
_PAIR_RE = re.compile(
    r"\b([A-Z0-9]{2,20})/(" + "|".join(_QUOTE_PREFERENCE) + r")\b",
)
_TIME_RE = re.compile(
    r"(?P<date>\d{4}-\d{2}-\d{2})\s+(?P<time>\d{1,2}:\d{2}(?::\d{2})?)"
    r"(?:\s*(?:\((?P<tz>[A-Za-z]+)\)|(?P<tz2>UTC|GMT|UT)\b))?",
    re.IGNORECASE,
)
_ACCEPTED_TZ = frozenset({"UTC", "GMT", "UT"})
_WILL_LIST_TICKER_RE = re.compile(r"\bwill list\b.{1,160}\([A-Z0-9]{2,20}\)", re.IGNORECASE | re.DOTALL)
_DELIST_WINDOW_RE = re.compile(r".{0,160}delist(?:ed|ing)?.{0,160}", re.IGNORECASE | re.DOTALL)
# Phase 8.1C pilot in-window scan (catalog 48): 38 detail fetches before title prefilter; ~2 after.
SAMPLE_38_DETAIL_FETCH_BEFORE = 38
SAMPLE_38_DETAIL_FETCH_AFTER = 2


def prefilter_title(title: str) -> str | None:
    """Return a high-confidence exclusion reason from title alone, or None to fetch detail."""
    text_l = title.lower()
    if "bstock" in text_l or "bstocks" in text_l:
        return "tokenized_security"
    if (
        "tokenized security" in text_l
        or "tokenized securities" in text_l
        or "tokenized stock" in text_l
        or "tokenized stocks" in text_l
    ):
        return "tokenized_security"
    if re.search(r"\bcollateral\b", text_l):
        return "not_spot_collateral"
    if (re.search(r"\bdelist", text_l) or "delisting" in text_l) and "spot trading pair" in text_l:
        return "not_spot_delisting"
    if "isolated margin" in text_l or "cross margin" in text_l or re.search(r"\bon margin\b", text_l):
        return "not_spot_margin"
    if re.search(r"\bbinance alpha will list\b", text_l) or re.search(r"\balpha will list\b", text_l):
        return "not_spot_alpha"
    if re.search(r"\bwill add\b", text_l) and "spot trading pair" in text_l:
        return "not_spot_pair_addition"
    if (
        re.search(r"\bfutures\b", text_l)
        or re.search(r"\bperpetual\b", text_l)
        or "usdⓢ-m" in text_l
        or "usd-m" in text_l
        or "coin-m" in text_l
    ):
        return "not_spot_futures"
    return None


def detail_fetch_expectation_for_titles(titles: Sequence[str]) -> tuple[int, int]:
    """Return (before, after) expected CMS detail-fetch counts for a title list."""
    before = len(titles)
    after = sum(1 for title in titles if prefilter_title(title) is None)
    return before, after


def _combined_text(announcement: ListingAnnouncement) -> str:
    body = announcement.body.strip()
    if body:
        return f"{announcement.title}\n{body}"
    return announcement.title


def _is_tokenized_security(text_l: str) -> bool:
    """bStocks / tokenized securities are out of scope (checked before timestamps)."""
    if "bstock" in text_l:
        return True
    if "tokenized security" in text_l or "tokenized securities" in text_l:
        return True
    if "tokenized stock" in text_l or "tokenized stocks" in text_l:
        return True
    return False


def _is_genuine_new_spot_asset(text: str, text_l: str) -> bool:
    has_will_list_ticker = _WILL_LIST_TICKER_RE.search(text) is not None
    has_spot_open = (
        "spot trading pair" in text_l
        or "open trading for the following spot trading pairs" in text_l
        or "will open trading for" in text_l
    )
    if has_will_list_ticker and has_spot_open:
        return True
    return "open trading for the following spot trading pairs at" in text_l


def _delist_mentions_are_alpha_only(text: str) -> bool:
    windows = _DELIST_WINDOW_RE.findall(text)
    if not windows:
        return False
    return all("alpha" in window.lower() for window in windows)


def _is_pair_addition(text_l: str, *, genuine_new_asset: bool) -> bool:
    if genuine_new_asset:
        return False
    if re.search(r"\bwill add\b", text_l) and "spot trading pair" in text_l:
        return True
    if "additional spot trading pair" in text_l:
        return True
    return "already listed" in text_l and "spot trading pair" in text_l


def _not_spot_reason(text: str, text_l: str) -> str | None:
    genuine = _is_genuine_new_spot_asset(text, text_l)
    if re.search(r"\bdelist", text_l) or "delisting" in text_l:
        if not (genuine and _delist_mentions_are_alpha_only(text)):
            return "not_spot_delisting"
    if not genuine:
        if re.search(r"\bbinance alpha will list\b", text_l) or re.search(r"\balpha will list\b", text_l):
            return "not_spot_alpha"
        if "binance alpha" in text_l:
            return "not_spot_alpha"
        if re.search(r"\bcollateral\b", text_l):
            return "not_spot_collateral"
        if "isolated margin" in text_l or "cross margin" in text_l or re.search(r"\bon margin\b", text_l):
            return "not_spot_margin"
        if (
            re.search(r"\bfutures\b", text_l)
            or re.search(r"\bperpetual\b", text_l)
            or "usdⓢ-m" in text_l
            or "usd-m" in text_l
            or "coin-m" in text_l
        ):
            return "not_spot_futures"
        if _is_pair_addition(text_l, genuine_new_asset=False):
            return "not_spot_pair_addition"
    return None


def _has_spot_language(text_l: str) -> bool:
    if "spot trading pair" in text_l or "spot trading pairs" in text_l:
        return True
    if "will open trading for" in text_l:
        return True
    if re.search(r"\bwill list\b", text_l) and re.search(r"\([A-Z0-9]{2,20}\)", text_l, re.I):
        return True
    return False


def _extract_pairs(text: str) -> list[tuple[str, str]]:
    return [(base.upper(), quote.upper()) for base, quote in _PAIR_RE.findall(text)]


def _preferred_symbol(pairs: list[tuple[str, str]]) -> str | None:
    if not pairs:
        return None
    bases = {base for base, _quote in pairs}
    if len(bases) > 1:
        return None
    by_quote = {quote: base for base, quote in pairs}
    for quote in _QUOTE_PREFERENCE:
        base = by_quote.get(quote)
        if base is not None:
            return f"{base}{quote}"
    base, quote = pairs[0]
    return f"{base}{quote}"


def _parse_clock(date_s: str, time_s: str) -> datetime | None:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            parsed = datetime.strptime(f"{date_s} {time_s}", fmt)
        except ValueError:
            continue
        return parsed.replace(tzinfo=UTC)
    return None


def _extract_utc_times(text: str) -> tuple[datetime, ...]:
    found: list[datetime] = []
    seen: set[datetime] = set()
    for match in _TIME_RE.finditer(text):
        tz = (match.group("tz") or match.group("tz2") or "").upper()
        if tz not in _ACCEPTED_TZ:
            continue
        parsed = _parse_clock(match.group("date"), match.group("time"))
        if parsed is None or parsed in seen:
            continue
        seen.add(parsed)
        found.append(parsed)
    return tuple(found)


def _parsed(
    announcement: ListingAnnouncement,
    *,
    release_date: datetime,
    provenance: dict[str, str],
    classification: SpotClass,
    exclusion_reason: str | None,
    symbol: str | None = None,
    source_event_time: datetime | None = None,
    source_event_time_status: SourceEventTimeStatus = SourceEventTimeStatus.MISSING,
) -> ParsedListing:
    return ParsedListing(
        announcement_code=announcement.code,
        announcement_id=announcement.id,
        title=announcement.title,
        classification=classification,
        symbol=symbol,
        release_date=release_date,
        source_event_time=source_event_time,
        source_event_time_status=source_event_time_status,
        body=announcement.body,
        provenance=provenance,
        exclusion_reason=exclusion_reason,
    )


def classify_and_extract(announcement: ListingAnnouncement) -> ParsedListing:
    """Fail-closed Spot vs not-Spot classification; never infer trading-start from releaseDate."""
    release_date = utc_from_millis(announcement.release_date_ms)
    text = _combined_text(announcement)
    text_l = text.lower()
    provenance = dict(announcement.provenance)
    provenance["parser"] = "listing_cohort_v1"
    provenance["release_date"] = release_date.isoformat()

    if _is_tokenized_security(text_l):
        provenance["exclusion"] = "tokenized_security"
        return _parsed(
            announcement,
            release_date=release_date,
            provenance=provenance,
            classification=SpotClass.NOT_SPOT,
            exclusion_reason="tokenized_security",
        )

    not_spot = _not_spot_reason(text, text_l)
    has_spot = _has_spot_language(text_l)
    pairs = _extract_pairs(text)
    symbol = _preferred_symbol(pairs)
    times = _extract_utc_times(text)

    if not_spot is not None:
        provenance["exclusion"] = not_spot
        return _parsed(
            announcement,
            release_date=release_date,
            provenance=provenance,
            classification=SpotClass.NOT_SPOT,
            exclusion_reason=not_spot,
            symbol=symbol,
        )

    if not has_spot:
        return _parsed(
            announcement,
            release_date=release_date,
            provenance=provenance,
            classification=SpotClass.AMBIGUOUS,
            exclusion_reason="ambiguous_not_spot_listing",
            symbol=symbol,
        )

    if len(times) > 1:
        provenance["extracted_times"] = ",".join(t.isoformat() for t in times)
        return _parsed(
            announcement,
            release_date=release_date,
            provenance=provenance,
            classification=SpotClass.AMBIGUOUS,
            exclusion_reason="ambiguous_trading_start",
            symbol=symbol,
        )

    if symbol is None and len({b for b, _q in pairs}) > 1:
        return _parsed(
            announcement,
            release_date=release_date,
            provenance=provenance,
            classification=SpotClass.AMBIGUOUS,
            exclusion_reason="ambiguous_multiple_bases",
        )

    source_event_time = times[0] if times else None
    status = SourceEventTimeStatus.EXTRACTED if source_event_time is not None else SourceEventTimeStatus.MISSING
    provenance["source_event_time_status"] = status.value
    provenance["source_event_time_field"] = (
        "announced_spot_trading_start" if source_event_time is not None else "missing_not_inferred_from_release_date"
    )
    if symbol is None:
        return _parsed(
            announcement,
            release_date=release_date,
            provenance=provenance,
            classification=SpotClass.SPOT_LISTING,
            exclusion_reason="symbol_not_extractable",
            source_event_time=source_event_time,
            source_event_time_status=status,
        )
    return _parsed(
        announcement,
        release_date=release_date,
        provenance=provenance,
        classification=SpotClass.SPOT_LISTING,
        exclusion_reason=None,
        symbol=symbol,
        source_event_time=source_event_time,
        source_event_time_status=status,
    )


def completeness_for(
    *,
    symbol: str | None,
    source_event_time: datetime | None,
    first_market_data_time: datetime | None,
) -> CompletenessStatus:
    if symbol and (source_event_time is not None or first_market_data_time is not None):
        return CompletenessStatus.COMPLETE
    return CompletenessStatus.INCOMPLETE
