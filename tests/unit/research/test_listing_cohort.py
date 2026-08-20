"""Phase 8.1 conservative Spot classification and extraction (no network)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from newcoin_trader.domain.listing_cohort import (
    ListingAnnouncement,
    SourceEventTimeStatus,
    SpotClass,
)
from newcoin_trader.research.listing_cohort import classify_and_extract

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures" / "binance" / "announcements"
_CASES = json.loads((FIXTURES / "classifier_cases.json").read_text(encoding="utf-8"))


def _ann(
    *,
    code: str = "c1",
    title: str,
    release_date_ms: int = 1705233600000,
    body: str = "",
) -> ListingAnnouncement:
    return ListingAnnouncement(
        code=code,
        id="1",
        release_date_ms=release_date_ms,
        title=title,
        type="1",
        provenance={"source": "fixture"},
        body=body,
    )


def test_classifies_explicit_spot_and_extracts_symbol_and_trading_start() -> None:
    parsed = classify_and_extract(
        _ann(
            title="Binance Will Open Trading for NEW/USDT, NEW/USDC Spot Trading Pairs at 2024-01-15 08:00 (UTC)",
        )
    )
    assert parsed.classification is SpotClass.SPOT_LISTING
    assert parsed.symbol == "NEWUSDT"
    assert parsed.source_event_time == datetime(2024, 1, 15, 8, 0, tzinfo=UTC)
    assert parsed.source_event_time_status is SourceEventTimeStatus.EXTRACTED
    assert parsed.release_date == datetime(2024, 1, 14, 12, 0, tzinfo=UTC)
    assert parsed.exclusion_reason is None


def test_title_only_will_list_uses_body_for_spot_pair_and_time() -> None:
    parsed = classify_and_extract(
        _ann(
            title="Binance Will List Newcoin (NEW)",
            body="Binance will open trading for NEW/USDT Spot Trading Pair at 2024-01-14 08:00 (UTC).",
        )
    )
    assert parsed.classification is SpotClass.SPOT_LISTING
    assert parsed.symbol == "NEWUSDT"
    assert parsed.source_event_time == datetime(2024, 1, 14, 8, 0, tzinfo=UTC)


def test_genuine_spot_listing_not_negated_by_unrelated_alpha_delist() -> None:
    case = _CASES["aero_spot_plus_alpha_delist"]
    parsed = classify_and_extract(_ann(code="aero", title=case["title"], body=case["body"]))
    assert parsed.classification is SpotClass.SPOT_LISTING
    assert parsed.symbol == "AEROUSDT"
    assert parsed.source_event_time == datetime(2024, 10, 17, 12, 0, tzinfo=UTC)
    assert parsed.exclusion_reason is None
    assert parsed.exclusion_reason != "not_spot_delisting"
    assert parsed.exclusion_reason != "not_spot_alpha"
    assert parsed.exclusion_reason != "ambiguous_trading_start"


def test_genuine_spot_delisting_is_excluded() -> None:
    case = _CASES["spot_delist"]
    parsed = classify_and_extract(_ann(code="spot-delist", title=case["title"], body=case["body"]))
    assert parsed.classification is SpotClass.NOT_SPOT
    assert parsed.exclusion_reason == "not_spot_delisting"
    assert parsed.source_event_time is None


def test_futures_launch_is_excluded() -> None:
    case = _CASES["futures_launch"]
    parsed = classify_and_extract(_ann(code="futures", title=case["title"], body=case["body"]))
    assert parsed.classification is SpotClass.NOT_SPOT
    assert parsed.exclusion_reason == "not_spot_futures"
    assert parsed.source_event_time is None


def test_collateral_addition_is_excluded() -> None:
    case = _CASES["collateral_addition"]
    parsed = classify_and_extract(_ann(code="collateral", title=case["title"], body=case["body"]))
    assert parsed.classification is SpotClass.NOT_SPOT
    assert parsed.exclusion_reason == "not_spot_collateral"
    assert parsed.source_event_time is None


def test_trading_pair_addition_for_already_listed_asset_is_excluded() -> None:
    case = _CASES["pair_addition_already_listed"]
    parsed = classify_and_extract(_ann(code="pair-add", title=case["title"], body=case["body"]))
    assert parsed.classification is SpotClass.NOT_SPOT
    assert parsed.exclusion_reason == "not_spot_pair_addition"
    assert parsed.source_event_time is None
    assert parsed.exclusion_reason != "ambiguous_trading_start"


def test_usd1_try_additional_pair_for_already_listed_asset_is_excluded() -> None:
    case = _CASES["usd1_try_pair_addition_already_listed"]
    parsed = classify_and_extract(_ann(code="usd1-try", title=case["title"], body=case["body"]))
    assert parsed.classification is SpotClass.NOT_SPOT
    assert parsed.exclusion_reason == "not_spot_pair_addition"
    assert parsed.source_event_time is None


def test_newtoken_listing_with_usdt_and_try_pairs_is_included() -> None:
    case = _CASES["newtoken_multi_pair_first_listing"]
    parsed = classify_and_extract(_ann(code="newtoken", title=case["title"], body=case["body"]))
    assert parsed.classification is SpotClass.SPOT_LISTING
    assert parsed.symbol == "NEWTOKENUSDT"
    assert parsed.source_event_time == datetime(2026, 8, 18, 8, 0, tzinfo=UTC)
    assert parsed.exclusion_reason is None


def test_new_stablecoin_first_listing_stays_includable() -> None:
    case = _CASES["stablecoin_first_listing_includable"]
    parsed = classify_and_extract(_ann(code="usdx", title=case["title"], body=case["body"]))
    assert parsed.classification is SpotClass.SPOT_LISTING
    assert parsed.symbol == "USDXUSDT"
    assert parsed.source_event_time == datetime(2026, 8, 18, 8, 0, tzinfo=UTC)
    assert parsed.exclusion_reason is None


@pytest.mark.parametrize("row", _CASES["bstocks"], ids=lambda row: str(row["code"]))
def test_bstock_tokenized_securities_are_deterministic_exclusions(row: dict[str, str]) -> None:
    parsed = classify_and_extract(_ann(code=row["code"], title=row["title"], body=row["body"]))
    assert parsed.classification is SpotClass.NOT_SPOT
    assert parsed.exclusion_reason == "tokenized_security"
    assert parsed.source_event_time is None
    assert parsed.source_event_time_status is SourceEventTimeStatus.MISSING
    assert parsed.exclusion_reason != "ambiguous_trading_start"


def test_seven_previously_ambiguous_bstocks_are_all_tokenized_security() -> None:
    rows = _CASES["bstocks"]
    assert len(rows) == 7
    reasons = [
        classify_and_extract(_ann(code=row["code"], title=row["title"], body=row["body"])).exclusion_reason
        for row in rows
    ]
    assert reasons == ["tokenized_security"] * 7


@pytest.mark.parametrize(
    "title",
    [
        "Binance Futures Will Launch USDⓈ-M OLDUSDT Perpetual Contract With Up to 50x Leverage",
        "Binance Will Add ABC on Isolated Margin and Cross Margin",
        "Binance Will List XYZ as a New Collateral Asset on Portfolio Margin",
        "Binance Alpha Will List Mystery Token (MYST)",
        "Binance Will Delist XYZUSDT and Remove Spot Trading Pairs",
    ],
)
def test_not_spot_titles_are_excluded(title: str) -> None:
    parsed = classify_and_extract(_ann(title=title))
    assert parsed.classification is SpotClass.NOT_SPOT
    assert parsed.exclusion_reason is not None
    assert parsed.symbol is None or parsed.exclusion_reason.startswith("not_spot")


def test_ambiguous_title_is_fail_closed() -> None:
    parsed = classify_and_extract(_ann(title="Important Notice Regarding New Cryptocurrency Listing Process"))
    assert parsed.classification is SpotClass.AMBIGUOUS
    assert parsed.exclusion_reason == "ambiguous_not_spot_listing"
    assert parsed.source_event_time is None


def test_missing_trading_start_is_not_inferred_from_release_date() -> None:
    parsed = classify_and_extract(
        _ann(
            title="Binance Will Open Trading for LATE/USDT Spot Trading Pair",
            body="Exact opening time will be announced later.",
            release_date_ms=1705122000000,
        )
    )
    assert parsed.classification is SpotClass.SPOT_LISTING
    assert parsed.symbol == "LATEUSDT"
    assert parsed.source_event_time is None
    assert parsed.source_event_time_status is SourceEventTimeStatus.MISSING
    assert parsed.release_date == datetime.fromtimestamp(1705122000000 / 1000, tz=UTC)
    assert parsed.release_date != parsed.source_event_time


def test_non_utc_or_unstated_timezone_does_not_fabricate_trading_start() -> None:
    parsed = classify_and_extract(
        _ann(title="Binance Will Open Trading for NEW/USDT Spot Trading Pair at 2024-01-15 08:00 (HKT)")
    )
    assert parsed.classification is SpotClass.SPOT_LISTING
    assert parsed.source_event_time is None
    assert parsed.source_event_time_status is SourceEventTimeStatus.MISSING


def test_conflicting_trading_start_times_are_ambiguous() -> None:
    parsed = classify_and_extract(
        _ann(
            title="Binance Will Open Trading for NEW/USDT Spot Trading Pair at 2024-01-15 08:00 (UTC)",
            body="Trading opens at 2024-01-16 09:00 (UTC).",
        )
    )
    assert parsed.classification is SpotClass.AMBIGUOUS
    assert parsed.source_event_time is None
    assert parsed.exclusion_reason == "ambiguous_trading_start"
