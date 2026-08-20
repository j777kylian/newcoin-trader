"""Binance Vision earliest kline/aggTrade corroboration (does not overwrite source_event_time)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from newcoin_trader.collectors.binance.vision import BinanceVisionClient, earliest_timestamp_from_daily_zip
from newcoin_trader.domain.listing_cohort import CohortListing, ParsedListing
from newcoin_trader.errors import NotFoundError, ParseError
from newcoin_trader.research.listing_cohort import completeness_for


def _probe_dates(
    start: datetime,
    *,
    lookback_before_days: int,
    max_probe_days: int,
    now_utc: datetime,
) -> tuple[date, ...]:
    day0 = start.date()
    today = now_utc.date()
    days: list[date] = []
    for offset in range(-lookback_before_days, max_probe_days):
        day = day0 + timedelta(days=offset)
        if day <= today:
            days.append(day)
    return tuple(days)


async def _first_existing_time(
    vision: BinanceVisionClient,
    *,
    symbol: str,
    kind: str,
    search_start: datetime,
    lookback_before_days: int,
    max_probe_days: int,
    now_utc: datetime,
) -> datetime | None:
    for day in _probe_dates(
        search_start,
        lookback_before_days=lookback_before_days,
        max_probe_days=max_probe_days,
        now_utc=now_utc,
    ):
        try:
            if kind == "kline":
                blob = await vision.fetch_daily_kline_zip(symbol, day)
                return earliest_timestamp_from_daily_zip(blob, kind="kline")
            blob = await vision.fetch_daily_aggtrade_zip(symbol, day)
            return earliest_timestamp_from_daily_zip(blob, kind="aggTrade")
        except NotFoundError:
            continue
        except ParseError:
            continue
    return None


async def corroborate_listing(
    parsed: ParsedListing,
    *,
    vision: BinanceVisionClient,
    max_probe_days: int,
    lookback_before_days: int = 0,
    now_utc: datetime | None = None,
) -> CohortListing:
    """Attach first_* market clocks. Never writes them into ``source_event_time``."""
    if parsed.symbol is None:
        raise ParseError("corroborate_listing requires an extracted symbol")
    resolved_now = now_utc if now_utc is not None else datetime.now(UTC)
    search_start = parsed.source_event_time or parsed.release_date
    first_kline = await _first_existing_time(
        vision,
        symbol=parsed.symbol,
        kind="kline",
        search_start=search_start,
        lookback_before_days=lookback_before_days,
        max_probe_days=max_probe_days,
        now_utc=resolved_now,
    )
    first_trade = await _first_existing_time(
        vision,
        symbol=parsed.symbol,
        kind="aggTrade",
        search_start=search_start,
        lookback_before_days=lookback_before_days,
        max_probe_days=max_probe_days,
        now_utc=resolved_now,
    )
    present = [ts for ts in (first_kline, first_trade) if ts is not None]
    first_market = min(present) if present else None
    provenance = dict(parsed.provenance)
    provenance["corroboration"] = "binance_vision_daily"
    provenance["source_event_time_status"] = parsed.source_event_time_status.value
    if parsed.source_event_time is not None:
        provenance["source_event_time"] = parsed.source_event_time.isoformat()
    else:
        provenance["source_event_time"] = "MISSING"
        provenance["source_event_time_not_inferred"] = "release_date_and_first_market_data_are_separate"
    if first_kline is not None:
        provenance["first_kline_time"] = first_kline.isoformat()
    if first_trade is not None:
        provenance["first_trade_time"] = first_trade.isoformat()
    if first_market is not None:
        provenance["first_market_data_time"] = first_market.isoformat()
    completeness = completeness_for(
        symbol=parsed.symbol,
        source_event_time=parsed.source_event_time,
        first_market_data_time=first_market,
    )
    return CohortListing(
        announcement_code=parsed.announcement_code,
        announcement_id=parsed.announcement_id,
        title=parsed.title,
        classification=parsed.classification,
        symbol=parsed.symbol,
        release_date=parsed.release_date,
        source_event_time=parsed.source_event_time,
        source_event_time_status=parsed.source_event_time_status,
        first_seen_time=parsed.release_date,
        first_kline_time=first_kline,
        first_trade_time=first_trade,
        first_market_data_time=first_market,
        decision_available_time=parsed.release_date,
        completeness=completeness,
        provenance=provenance,
        exclusion_reason=parsed.exclusion_reason,
    )
