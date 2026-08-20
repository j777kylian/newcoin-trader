"""Phase 8.1 listing-cohort pipeline: assemble artifacts A–H (gross research only)."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

from newcoin_trader.collectors.binance.announcements import (
    DEFAULT_PAGE_SIZE,
    BinanceAnnouncementClient,
    validate_collect_bounds,
)
from newcoin_trader.collectors.binance.vision import BinanceVisionClient
from newcoin_trader.domain.enums import Chain, Venue
from newcoin_trader.domain.event_study import (
    CellAggregate,
    EventStudyCellResult,
    MarketObservation,
    ObservationResolution,
    TokenListingEvent,
)
from newcoin_trader.domain.feature_research import (
    DecisionFeatureRecord,
    FeatureMarketInput,
    FeatureTradeInput,
    FutureLabel,
)
from newcoin_trader.domain.listing_cohort import (
    CohortListing,
    CompletenessStatus,
    ListingAnnouncement,
    ListingCohortPipelineReport,
    ListingExclusion,
    ParsedListing,
    SourceEventTimeStatus,
    SpotClass,
)
from newcoin_trader.domain.types import require_utc, utc_from_millis
from newcoin_trader.errors import ConfigError, ParseError
from newcoin_trader.reports.schemas import to_jsonable
from newcoin_trader.reports.writers import write_csv, write_json
from newcoin_trader.research.event_study_aggregate import aggregate_results
from newcoin_trader.research.event_study_engine import run_event_study
from newcoin_trader.research.feature_research_analysis import chronological_split
from newcoin_trader.research.feature_research_config import DEFAULT_DECISION_DELAY, DEFAULT_FEATURE_WINDOWS
from newcoin_trader.research.feature_research_features import build_decision_feature_record
from newcoin_trader.research.feature_research_run import FEATURE_CSV_BASE, _feature_csv_columns
from newcoin_trader.research.listing_cohort import classify_and_extract, prefilter_title
from newcoin_trader.research.listing_cohort_config import (
    DEFAULT_ENTRY_DELAYS,
    DEFAULT_HOLDING_PERIODS,
    PHASE81_SPLIT_RATIOS,
    PILOT_LOOKBACK,
    TARGET_VALID_CRYPTO_LISTINGS,
    clamp_research_end,
    format_listing_duration,
    listing_search_start,
    validate_listing_cohort_bounds,
)
from newcoin_trader.research.listing_cohort_ingestion import (
    BinanceMarketHistory,
    ingest_cohort_market_history,
    validate_binance_limit,
)
from newcoin_trader.research.listing_corroboration import corroborate_listing

ARTIFACT_NAMES: dict[str, str] = {
    "A": "listing_cohort.json",
    "B": "coverage.json",
    "C": "return_matrix.json",
    "D": "event_study_cells.csv",
    "E": "pit_feature_dataset.csv",
    "F": "split_manifest.json",
    "G": "alpha_discovery_readiness.json",
    "H": "phase81_summary.md",
}

ALPHA_DISCOVERY_QUESTIONS: tuple[str, ...] = (
    "cohort_constructible_with_explicit_listing_time_provenance",
    "market_data_coverage_sufficient_for_event_study_grid",
    "usable_sample_honestly_bounded_with_exclusions_recorded",
    "chronological_splits_usable_without_shuffle_or_leakage",
    "phase_82_rule_discovery_statistically_meaningful",
)

COHORT_CSV_FIELDS = (
    "announcement_code",
    "announcement_id",
    "symbol",
    "title",
    "classification",
    "release_date",
    "source_event_time",
    "source_event_time_status",
    "first_seen_time",
    "first_kline_time",
    "first_trade_time",
    "first_market_data_time",
    "decision_available_time",
    "completeness",
    "provenance",
)

EXCLUSION_CSV_FIELDS = ("announcement_code", "symbol", "reason", "title")

EVENT_CSV_FIELDS = (
    "event_id",
    "venue",
    "token_address",
    "chain",
    "source_event_time",
    "first_seen_time",
    "first_market_data_time",
    "decision_available_time",
    "entry_delay",
    "holding_period",
    "entry_time",
    "exit_time",
    "status",
    "entry_price",
    "exit_price",
    "simple_return",
    "log_return",
    "mfe",
    "mae",
    "path_available",
    "path_observation_count",
    "event_source",
    "event_provenance",
    "label",
    "warning",
)

_MIN_EVENTS_FOR_RULE_DISCOVERY = 20


@dataclass(frozen=True)
class ClassifiedCatalog:
    """CMS pagination + classification before Vision corroboration."""

    in_window: tuple[ListingAnnouncement, ...]
    parsed: tuple[ParsedListing, ...]
    valid: tuple[ParsedListing, ...]
    exclusions: tuple[ListingExclusion, ...]
    requested_start: datetime
    requested_end: datetime
    effective_end: datetime
    now_utc: datetime
    search_start: datetime
    selection: dict[str, Any]


@dataclass(frozen=True)
class AssembledListingCohort:
    """Announcement classification + Vision corroboration (no market series yet)."""

    in_window: tuple[ListingAnnouncement, ...]
    parsed: tuple[ParsedListing, ...]
    cohort: tuple[CohortListing, ...]
    exclusions: tuple[ListingExclusion, ...]
    requested_end: datetime
    effective_end: datetime
    now_utc: datetime
    search_start: datetime
    selection: dict[str, Any]


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _resolve_now(now_utc: datetime | None) -> datetime:
    return require_utc(now_utc) if now_utc is not None else datetime.now(UTC)


async def _with_detail(client: BinanceAnnouncementClient, article: ListingAnnouncement) -> ListingAnnouncement:
    try:
        body = await client.fetch_detail(article.code)
    except ParseError:
        body = None
    provenance = dict(article.provenance)
    if body:
        provenance["detail_endpoint"] = "binance:cms:article_detail"
        provenance["body_source"] = "article_detail"
        provenance["detail_fetch"] = "fetched"
    else:
        provenance["body_source"] = "title_only"
        provenance["detail_fetch"] = "fetched"
    return article.model_copy(update={"body": body or "", "provenance": provenance})


def _title_prefiltered(article: ListingAnnouncement, *, reason: str) -> ListingAnnouncement:
    provenance = dict(article.provenance)
    provenance["body_source"] = "title_prefilter"
    provenance["title_prefilter"] = reason
    provenance["detail_fetch"] = "skipped"
    return article.model_copy(update={"body": "", "provenance": provenance})


def _parse_enriched(article: ListingAnnouncement) -> ParsedListing:
    title_reason = article.provenance.get("title_prefilter")
    if title_reason is None:
        return classify_and_extract(article)
    release_date = utc_from_millis(article.release_date_ms)
    provenance = dict(article.provenance)
    provenance["parser"] = "listing_cohort_v1"
    provenance["release_date"] = release_date.isoformat()
    provenance["exclusion"] = title_reason
    return ParsedListing(
        announcement_code=article.code,
        announcement_id=article.id,
        title=article.title,
        classification=SpotClass.NOT_SPOT,
        symbol=None,
        release_date=release_date,
        source_event_time=None,
        source_event_time_status=SourceEventTimeStatus.MISSING,
        body=article.body,
        provenance=provenance,
        exclusion_reason=title_reason,
    )


async def _enrich_for_classification(
    client: BinanceAnnouncementClient,
    article: ListingAnnouncement,
    *,
    fetch_details: bool,
) -> ListingAnnouncement:
    if not fetch_details:
        return article
    title_reason = prefilter_title(article.title)
    if title_reason is not None:
        return _title_prefiltered(article, reason=title_reason)
    return await _with_detail(client, article)


def _cohort_row_dict(row: CohortListing) -> dict[str, Any]:
    return {
        "announcement_code": row.announcement_code,
        "announcement_id": row.announcement_id,
        "symbol": row.symbol,
        "title": row.title,
        "classification": row.classification.value,
        "release_date": _iso(row.release_date),
        "source_event_time": _iso(row.source_event_time),
        "source_event_time_status": row.source_event_time_status.value,
        "first_seen_time": _iso(row.first_seen_time),
        "first_kline_time": _iso(row.first_kline_time),
        "first_trade_time": _iso(row.first_trade_time),
        "first_market_data_time": _iso(row.first_market_data_time),
        "decision_available_time": _iso(row.decision_available_time),
        "completeness": row.completeness.value,
        "provenance": dict(row.provenance),
    }


def _cohort_csv_row(row: CohortListing) -> dict[str, Any]:
    payload = _cohort_row_dict(row)
    payload["provenance"] = json.dumps(payload["provenance"], sort_keys=True, separators=(",", ":"))
    return payload


def _to_event(row: CohortListing) -> TokenListingEvent | None:
    event_clock = row.source_event_time or row.first_market_data_time
    if event_clock is None:
        return None
    provenance = dict(row.provenance)
    if row.source_event_time is None:
        provenance["event_clock_field"] = "first_market_data_time"
        provenance["source_event_time"] = "MISSING"
    else:
        provenance["event_clock_field"] = "announced_spot_trading_start"
    return TokenListingEvent(
        event_id=f"binance:binance:{row.symbol}:{row.announcement_code}",
        venue=Venue.BINANCE,
        chain=Chain.BINANCE,
        token_address=row.symbol,
        symbol=row.symbol,
        source="binance:cms:catalog48",
        source_event_time=event_clock,
        first_seen_time=row.first_seen_time,
        first_market_data_time=row.first_market_data_time,
        decision_available_time=row.decision_available_time,
        provenance=provenance,
    )


def _input_to_observation(inp: FeatureMarketInput) -> MarketObservation:
    return MarketObservation(
        token_address=inp.token_address,
        chain=inp.chain,
        venue=inp.venue,
        timestamp=inp.timestamp,
        price=inp.price,
        resolution=inp.resolution,
        source=inp.source,
        provenance=dict(inp.provenance) if inp.provenance else None,
    )


def _trade_to_observation(trade: FeatureTradeInput) -> MarketObservation:
    return MarketObservation(
        token_address=trade.token_address,
        chain=trade.chain,
        venue=trade.venue,
        timestamp=trade.timestamp,
        price=trade.price,
        resolution=ObservationResolution.POINT,
        source=trade.source,
        provenance=dict(trade.provenance) if trade.provenance else {"kind": "aggtrade"},
    )


def _cell_csv_row(cell: EventStudyCellResult) -> dict[str, Any]:
    return {
        "event_id": cell.event_id,
        "venue": cell.venue.value,
        "token_address": cell.token_address,
        "chain": cell.chain.value,
        "source_event_time": cell.source_event_time.isoformat(),
        "first_seen_time": cell.first_seen_time.isoformat(),
        "first_market_data_time": _iso(cell.first_market_data_time),
        "decision_available_time": cell.decision_available_time.isoformat(),
        "entry_delay": format_listing_duration(cell.entry_delay),
        "holding_period": format_listing_duration(cell.holding_period),
        "entry_time": cell.entry_time.isoformat(),
        "exit_time": cell.exit_time.isoformat(),
        "status": cell.status.value,
        "entry_price": cell.entry_price,
        "exit_price": cell.exit_price,
        "simple_return": cell.simple_return,
        "log_return": cell.log_return,
        "mfe": cell.path.mfe,
        "mae": cell.path.mae,
        "path_available": cell.path.path_available,
        "path_observation_count": cell.path.path_observation_count,
        "event_source": cell.event_source,
        "event_provenance": json.dumps(dict(cell.event_provenance), sort_keys=True, separators=(",", ":")),
        "label": cell.label,
        "warning": cell.warning,
    }


def _aggregate_dict(agg: CellAggregate) -> dict[str, Any]:
    return {
        "venue": agg.venue.value,
        "entry_delay": format_listing_duration(agg.entry_delay),
        "holding_period": format_listing_duration(agg.holding_period),
        "samples": agg.samples,
        "complete_count": agg.complete_count,
        "valid_return_count": agg.valid_return_count,
        "censored_count": agg.censored_count,
        "status_counts": dict(agg.status_counts),
        "mean_simple_return": agg.mean_simple_return,
        "median_simple_return": agg.median_simple_return,
        "std_simple_return": agg.std_simple_return,
        "win_rate": agg.win_rate,
        "mean_mfe": agg.mean_mfe,
        "mean_mae": agg.mean_mae,
        "label": agg.label,
        "warning": agg.warning,
    }


def _feature_csv_row(record: DecisionFeatureRecord, feature_cols: Sequence[str]) -> dict[str, object]:
    feat_map = {f.name: f for f in record.features}
    label: FutureLabel | None = record.labels[0] if record.labels else None
    row: dict[str, object] = {
        "event_id": record.event_id,
        "venue": record.venue.value,
        "chain": record.chain.value,
        "token_address": record.token_address,
        "pair_address": record.pair_address,
        "source_event_time": record.source_event_time.isoformat(),
        "first_seen_time": record.first_seen_time.isoformat(),
        "first_market_data_time": _iso(record.first_market_data_time),
        "decision_available_time": record.decision_available_time.isoformat(),
        "decision_time": record.decision_time.isoformat(),
        "feature_cutoff": record.feature_cutoff.isoformat(),
        "config_id": record.config_id,
        "computation_id": record.computation_id,
        "label_status": label.status.value if label else None,
        "label_simple_return": label.simple_return if label else None,
        "label_mfe": label.mfe if label else None,
        "label_mae": label.mae if label else None,
        "label_entry_delay": format_listing_duration(label.entry_delay) if label else None,
        "label_holding_period": format_listing_duration(label.holding_period) if label else None,
    }
    for col in feature_cols:
        if col.endswith("__state"):
            name = col[: -len("__state")]
            feat = feat_map.get(name)
            row[col] = feat.state.value if feat else None
        else:
            feat = feat_map.get(col)
            row[col] = feat.value if feat else None
    return row


def _phase81_config_id() -> str:
    payload = {
        "phase": "phase_8_1_listing_cohort",
        "entry_delays": [format_listing_duration(d) for d in DEFAULT_ENTRY_DELAYS],
        "holding_periods": [format_listing_duration(h) for h in DEFAULT_HOLDING_PERIODS],
        "split_ratios": [str(r) for r in PHASE81_SPLIT_RATIOS],
    }
    return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]


def _build_coverage(
    *,
    requested_start: datetime,
    requested_end: datetime,
    effective_end: datetime,
    now_utc: datetime,
    search_start: datetime,
    raw_count: int,
    parsed: Sequence[ParsedListing],
    cohort: Sequence[CohortListing],
    exclusions: Sequence[ListingExclusion],
    selection: Mapping[str, Any],
) -> dict[str, Any]:
    clocks = [row.source_event_time or row.first_market_data_time for row in cohort]
    usable = [c for c in clocks if c is not None]
    kline_ok = sum(1 for row in cohort if row.first_kline_time is not None)
    trade_ok = sum(1 for row in cohort if row.first_trade_time is not None)
    class_counts = Counter(item.classification.value for item in parsed)
    reasons = Counter(item.reason for item in exclusions)
    return {
        "requested_period": {"start": requested_start.isoformat(), "end": requested_end.isoformat()},
        "effective_period": {"start": search_start.isoformat(), "end": effective_end.isoformat()},
        "now_utc": now_utc.isoformat(),
        "usable_period": {
            "start": min(usable).isoformat() if usable else None,
            "end": max(usable).isoformat() if usable else None,
        },
        "cohort_selection": dict(selection),
        "counts": {
            "raw_articles": raw_count,
            "spot_listings": class_counts.get(SpotClass.SPOT_LISTING.value, 0),
            "not_spot": class_counts.get(SpotClass.NOT_SPOT.value, 0),
            "ambiguous": class_counts.get(SpotClass.AMBIGUOUS.value, 0),
            "cohort": len(cohort),
            "complete": sum(1 for row in cohort if row.completeness is CompletenessStatus.COMPLETE),
            "incomplete": sum(1 for row in cohort if row.completeness is CompletenessStatus.INCOMPLETE),
            "exclusions": len(exclusions),
        },
        "klines": {"available": kline_ok, "missing": len(cohort) - kline_ok},
        "trades": {"available": trade_ok, "missing": len(cohort) - trade_ok},
        "liquidity": {
            "available": kline_ok,
            "missing": len(cohort) - kline_ok,
            "note": "kline quote_volume proxy; no historical L2",
        },
        "depth": {
            "available": 0,
            "missing": len(cohort),
            "status": "unsupported_historical_l2",
            "note": "Phase 8.1 does not add depth features; historical L2 is unavailable",
        },
        "exclusion_reasons": dict(sorted(reasons.items())),
    }


def _answer(verdict: str, rationale: str) -> dict[str, str]:
    return {"verdict": verdict, "rationale": rationale}


def _readiness(
    *,
    coverage: Mapping[str, Any],
    cohort: Sequence[CohortListing],
    cells: Sequence[EventStudyCellResult],
    split_counts: tuple[int, int, int],
    feature_count: int,
) -> dict[str, Any]:
    extracted = sum(1 for row in cohort if row.source_event_time is not None)
    missing_kept = sum(1 for row in cohort if row.source_event_time is None)
    release_collision = any(
        row.source_event_time is not None and row.source_event_time == row.release_date for row in cohort
    )
    q1 = (
        _answer(
            "NOT_READY",
            "No Spot cohort rows were assembled, so listing-time provenance cannot be audited.",
        )
        if not cohort
        else _answer(
            "READY" if extracted or missing_kept and not release_collision else "PARTIAL",
            "Announcement trading-start is stored separately from release_date; "
            f"{extracted} extracted, {missing_kept} MISSING (not inferred from publication time).",
        )
    )
    complete_cells = sum(1 for cell in cells if cell.status.value == "complete")
    kline_ok = int(coverage["klines"]["available"])
    q2 = _answer(
        "READY" if complete_cells > 0 and kline_ok > 0 else "PARTIAL" if kline_ok > 0 or cells else "NOT_READY",
        f"Event-study cells={len(cells)} complete={complete_cells}; "
        f"klines available={kline_ok}, trades available={coverage['trades']['available']}. "
        "Sub-minute 10s/30s delays require POINT-resolution (aggTrades/trades) observations; "
        "if only 1m klines exist those cells are unsupported (never synthesized from 1m candles). "
        "1m+ delays use 1m klines. Depth is unsupported.",
    )
    q3 = _answer(
        "READY" if coverage["counts"]["raw_articles"] > 0 else "NOT_READY",
        "Exclusions are written with reasons; requested vs usable periods are recorded; "
        f"raw_articles={coverage['counts']['raw_articles']} cohort={coverage['counts']['cohort']} "
        f"exclusions={coverage['counts']['exclusions']}.",
    )
    train, val, test = split_counts
    q4 = _answer(
        "READY"
        if train and val and test and feature_count >= _MIN_EVENTS_FOR_RULE_DISCOVERY
        else "PARTIAL"
        if feature_count > 0
        else "NOT_READY",
        f"Chronological split (no shuffle) train={train} validation={val} test={test} records={feature_count}.",
    )
    q5 = _answer(
        "NOT_READY",
        "Phase 8.2 rule discovery was not run. Fixture/bounded sample is below a defensible "
        f"minimum ({feature_count} < {_MIN_EVENTS_FOR_RULE_DISCOVERY} feature records). "
        "This pass is code+fixtures only; no bulk real collection.",
    )
    questions = {
        ALPHA_DISCOVERY_QUESTIONS[0]: q1,
        ALPHA_DISCOVERY_QUESTIONS[1]: q2,
        ALPHA_DISCOVERY_QUESTIONS[2]: q3,
        ALPHA_DISCOVERY_QUESTIONS[3]: q4,
        ALPHA_DISCOVERY_QUESTIONS[4]: q5,
    }
    return {
        "questions": questions,
        "gross_vs_executable_vs_prospective": {
            "gross": "this_pass",
            "executable": "not_this_pass",
            "prospective": "not_this_pass",
        },
        "rule_discovery": "not_run",
        "winner_selection": "not_run",
        "coverage_counts": coverage["counts"],
    }


def _write_summary(
    path: Path,
    *,
    cohort: Sequence[CohortListing],
    coverage: Mapping[str, Any],
    readiness: Mapping[str, Any],
    split_counts: tuple[int, int, int],
) -> None:
    symbols = ", ".join(sorted({row.symbol for row in cohort})) or "(none)"
    lines = [
        "# Phase 8.1 historical Binance Spot listing cohort",
        "",
        "Gross market-return research only. Not executable PnL, not a live/paper trading session, "
        "and not Phase 8.2 rule discovery.",
        "",
        f"- requested_period: `{coverage['requested_period']['start']}` → `{coverage['requested_period']['end']}`",
        f"- requested_end: `{coverage['requested_period']['end']}`",
        f"- effective_end: `{coverage['effective_period']['end']}`",
        f"- now_utc: `{coverage['now_utc']}`",
        f"- usable_period: `{coverage['usable_period']['start']}` → `{coverage['usable_period']['end']}`",
        f"- cohort_selection: target=`{coverage['cohort_selection'].get('target_valid_crypto_listings')}` "
        f"selected=`{coverage['cohort_selection'].get('selected')}` "
        f"stop_reason=`{coverage['cohort_selection'].get('stop_reason')}`",
        f"- raw_articles: `{coverage['counts']['raw_articles']}`",
        f"- cohort: `{coverage['counts']['cohort']}` complete=`{coverage['counts']['complete']}` "
        f"incomplete=`{coverage['counts']['incomplete']}`",
        f"- exclusions: `{coverage['counts']['exclusions']}`",
        f"- symbols: `{symbols}`",
        f"- split: train=`{split_counts[0]}` validation=`{split_counts[1]}` test=`{split_counts[2]}` (no shuffle)",
        f"- depth: `{coverage['depth']['status']}`",
        "",
        "## Alpha-discovery readiness",
        "",
    ]
    questions = readiness["questions"]
    assert isinstance(questions, dict)
    for key in ALPHA_DISCOVERY_QUESTIONS:
        answer = questions[key]
        lines.append(f"- `{key}`: **{answer['verdict']}** — {answer['rationale']}")
    lines.extend(["", "## Exclusion reasons", ""])
    reasons = coverage.get("exclusion_reasons") or {}
    if not reasons:
        lines.append("- _(none)_")
    else:
        assert isinstance(reasons, dict)
        for reason, count in reasons.items():
            lines.append(f"- `{reason}`: {count}")
    lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _selection_payload(
    *,
    target: int,
    selected: int,
    articles_scanned: int,
    pages_fetched: int,
    stop_reason: str,
    requested_start: datetime,
    requested_end: datetime,
    effective_end: datetime,
    search_start: datetime,
    now_utc: datetime,
    newest_scanned_release_at: str | None,
    oldest_scanned_release_at: str | None,
) -> dict[str, Any]:
    return {
        "target_valid_crypto_listings": target,
        "selected": selected,
        "articles_scanned": articles_scanned,
        "pages_fetched": pages_fetched,
        "stop_reason": stop_reason,
        "lookback": f"{int(PILOT_LOOKBACK.total_seconds() // 86400)}d",
        "search_start": search_start.isoformat(),
        "requested_start": requested_start.isoformat(),
        "requested_end": requested_end.isoformat(),
        "effective_end": effective_end.isoformat(),
        "now_utc": now_utc.isoformat(),
        "newest_scanned_release_at": newest_scanned_release_at,
        "oldest_scanned_release_at": oldest_scanned_release_at,
        "shortfall": max(0, target - selected),
        "classifier_not_weakened": True,
    }


def _scanned_release_bounds(in_window: list[ListingAnnouncement]) -> tuple[str | None, str | None]:
    """Audit-only newest/oldest release timestamps from scanned in-window catalog records."""
    if not in_window:
        return None, None
    newest_ms = max(article.release_date_ms for article in in_window)
    oldest_ms = min(article.release_date_ms for article in in_window)
    return utc_from_millis(newest_ms).isoformat(), utc_from_millis(oldest_ms).isoformat()


async def collect_classified_listings(
    *,
    announcement_client: BinanceAnnouncementClient,
    requested_start: datetime,
    requested_end: datetime,
    max_pages: int,
    max_articles: int,
    page_size: int,
    now_utc: datetime | None = None,
    target_valid_crypto_listings: int = TARGET_VALID_CRYPTO_LISTINGS,
    fetch_details: bool = True,
) -> ClassifiedCatalog:
    """Paginate catalog 48 newest-first until 50 valid crypto listings or bounds exhaust."""
    validate_collect_bounds(max_pages=max_pages, max_articles=max_articles, page_size=page_size)
    resolved_now = _resolve_now(now_utc)
    effective_end = clamp_research_end(requested_end=requested_end, now_utc=resolved_now)
    if effective_end <= requested_start:
        raise ConfigError("listing-cohort effective_end must be after start after clamping to current UTC")
    search_start = listing_search_start(requested_start=requested_start, effective_end=effective_end)

    in_window: list[ListingAnnouncement] = []
    parsed_rows: list[ParsedListing] = []
    valid: list[ParsedListing] = []
    exclusions: list[ListingExclusion] = []
    stop_reason = "catalog_exhausted"
    lookback_hit = False
    pages_fetched = 0

    for page_no in range(1, max_pages + 1):
        if len(valid) >= target_valid_crypto_listings:
            stop_reason = "target_reached"
            break
        if len(in_window) >= max_articles:
            stop_reason = "max_articles_exhausted"
            break
        page = await announcement_client.list_page(page_no=page_no, page_size=page_size)
        pages_fetched += 1
        if not page.articles:
            stop_reason = "catalog_exhausted"
            break
        for article in page.articles:
            if len(valid) >= target_valid_crypto_listings:
                stop_reason = "target_reached"
                break
            if len(in_window) >= max_articles:
                stop_reason = "max_articles_exhausted"
                break
            released = utc_from_millis(article.release_date_ms)
            if released >= effective_end:
                continue
            if released < search_start:
                lookback_hit = True
                stop_reason = "lookback_exhausted"
                break
            enriched = await _enrich_for_classification(
                announcement_client,
                article,
                fetch_details=fetch_details,
            )
            in_window.append(enriched)
            item = _parse_enriched(enriched)
            parsed_rows.append(item)
            if item.classification is SpotClass.SPOT_LISTING and item.symbol and item.exclusion_reason is None:
                valid.append(item)
            else:
                exclusions.append(
                    ListingExclusion(
                        announcement_code=item.announcement_code,
                        symbol=item.symbol or "",
                        reason=item.exclusion_reason or "excluded",
                        title=item.title,
                    )
                )
        if len(valid) >= target_valid_crypto_listings:
            stop_reason = "target_reached"
            break
        if lookback_hit:
            stop_reason = "lookback_exhausted"
            break
        if len(in_window) >= max_articles:
            stop_reason = "max_articles_exhausted"
            break
        if len(page.articles) < page_size:
            stop_reason = "catalog_exhausted"
            break
    else:
        stop_reason = "target_reached" if len(valid) >= target_valid_crypto_listings else "max_pages_exhausted"

    if len(valid) >= target_valid_crypto_listings:
        stop_reason = "target_reached"

    newest_scanned, oldest_scanned = _scanned_release_bounds(in_window)
    selection = _selection_payload(
        target=target_valid_crypto_listings,
        selected=len(valid),
        articles_scanned=len(in_window),
        pages_fetched=pages_fetched,
        stop_reason=stop_reason,
        requested_start=requested_start,
        requested_end=requested_end,
        effective_end=effective_end,
        search_start=search_start,
        now_utc=resolved_now,
        newest_scanned_release_at=newest_scanned,
        oldest_scanned_release_at=oldest_scanned,
    )
    return ClassifiedCatalog(
        in_window=tuple(in_window),
        parsed=tuple(parsed_rows),
        valid=tuple(valid),
        exclusions=tuple(sorted(exclusions, key=lambda e: (e.reason, e.announcement_code))),
        requested_start=requested_start,
        requested_end=requested_end,
        effective_end=effective_end,
        now_utc=resolved_now,
        search_start=search_start,
        selection=selection,
    )


async def assemble_listing_cohort(
    *,
    announcement_client: BinanceAnnouncementClient,
    vision_client: BinanceVisionClient,
    requested_start: datetime,
    requested_end: datetime,
    max_pages: int,
    max_articles: int,
    page_size: int,
    max_probe_days: int,
    lookback_before_days: int = 0,
    fetch_details: bool = True,
    now_utc: datetime | None = None,
    target_valid_crypto_listings: int = TARGET_VALID_CRYPTO_LISTINGS,
) -> AssembledListingCohort:
    """Fetch CMS articles, classify Spot listings, and corroborate Vision earliest times."""
    classified = await collect_classified_listings(
        announcement_client=announcement_client,
        requested_start=requested_start,
        requested_end=requested_end,
        max_pages=max_pages,
        max_articles=max_articles,
        page_size=page_size,
        now_utc=now_utc,
        target_valid_crypto_listings=target_valid_crypto_listings,
        fetch_details=fetch_details,
    )
    cohort: list[CohortListing] = []
    for item in classified.valid:
        cohort.append(
            await corroborate_listing(
                item,
                vision=vision_client,
                max_probe_days=max_probe_days,
                lookback_before_days=lookback_before_days,
                now_utc=classified.now_utc,
            )
        )
    cohort_rows = tuple(
        sorted(cohort, key=lambda r: (r.release_date, r.symbol, r.announcement_code)),
    )
    return AssembledListingCohort(
        in_window=classified.in_window,
        parsed=classified.parsed,
        cohort=cohort_rows,
        exclusions=classified.exclusions,
        requested_end=classified.requested_end,
        effective_end=classified.effective_end,
        now_utc=classified.now_utc,
        search_start=classified.search_start,
        selection=classified.selection,
    )


async def run_listing_cohort_pipeline(
    *,
    announcement_client: BinanceAnnouncementClient,
    vision_client: BinanceVisionClient,
    output_dir: Path,
    requested_start: datetime,
    requested_end: datetime,
    max_pages: int,
    max_articles: int,
    page_size: int,
    max_probe_days: int,
    lookback_before_days: int = 0,
    fetch_details: bool = True,
    market_inputs: Sequence[FeatureMarketInput] = (),
    trade_inputs: Sequence[FeatureTradeInput] = (),
    decision_delay: timedelta = DEFAULT_DECISION_DELAY,
    assembled: AssembledListingCohort | None = None,
    now_utc: datetime | None = None,
    target_valid_crypto_listings: int = TARGET_VALID_CRYPTO_LISTINGS,
) -> ListingCohortPipelineReport:
    validate_listing_cohort_bounds(
        start=requested_start,
        end=requested_end,
        max_probe_days=max_probe_days,
        lookback_before_days=lookback_before_days,
    )
    if assembled is None:
        assembled = await assemble_listing_cohort(
            announcement_client=announcement_client,
            vision_client=vision_client,
            requested_start=requested_start,
            requested_end=requested_end,
            max_pages=max_pages,
            max_articles=max_articles,
            page_size=page_size,
            max_probe_days=max_probe_days,
            lookback_before_days=lookback_before_days,
            fetch_details=fetch_details,
            now_utc=now_utc,
            target_valid_crypto_listings=target_valid_crypto_listings,
        )
    parsed = assembled.parsed
    in_window = assembled.in_window
    cohort_rows = assembled.cohort
    exclusion_rows = assembled.exclusions

    events = tuple(event for event in (_to_event(row) for row in cohort_rows) if event is not None)
    observations = tuple(_input_to_observation(inp) for inp in market_inputs) + tuple(
        _trade_to_observation(trade) for trade in trade_inputs
    )
    cells = (
        run_event_study(
            events,
            observations,
            entry_delays=DEFAULT_ENTRY_DELAYS,
            holding_periods=DEFAULT_HOLDING_PERIODS,
        )
        if events
        else ()
    )
    aggregates = aggregate_results(cells)
    config_id = _phase81_config_id()
    cells_by_event: dict[str, list[EventStudyCellResult]] = {}
    for cell in cells:
        cells_by_event.setdefault(cell.event_id, []).append(cell)

    records: list[DecisionFeatureRecord] = []
    for event in events:
        labels = tuple(
            FutureLabel(
                entry_delay=cell.entry_delay,
                holding_period=cell.holding_period,
                status=cell.status,
                simple_return=cell.simple_return,
                log_return=cell.log_return,
                mfe=cell.path.mfe,
                mae=cell.path.mae,
            )
            for cell in cells_by_event.get(event.event_id, ())
        )
        decision_time = event.source_event_time + decision_delay
        if decision_time < event.decision_available_time:
            continue
        records.append(
            build_decision_feature_record(
                event,
                market_inputs,
                trades=trade_inputs,
                decision_time=decision_time,
                windows=DEFAULT_FEATURE_WINDOWS,
                labels=labels,
                config_id=config_id,
            )
        )
    split = chronological_split(records, ratios=PHASE81_SPLIT_RATIOS)
    split_counts = (len(split.train), len(split.validation), len(split.test))
    coverage = _build_coverage(
        requested_start=requested_start,
        requested_end=requested_end,
        effective_end=assembled.effective_end,
        now_utc=assembled.now_utc,
        search_start=assembled.search_start,
        raw_count=len(in_window),
        parsed=parsed,
        cohort=cohort_rows,
        exclusions=exclusion_rows,
        selection=assembled.selection,
    )
    readiness = _readiness(
        coverage=coverage,
        cohort=cohort_rows,
        cells=cells,
        split_counts=split_counts,
        feature_count=len(records),
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(
        output_dir / ARTIFACT_NAMES["A"],
        {
            "listings": [_cohort_row_dict(row) for row in cohort_rows],
            "count": len(cohort_rows),
            "requested_period": coverage["requested_period"],
            "effective_period": coverage["effective_period"],
            "now_utc": coverage["now_utc"],
            "cohort_selection": coverage["cohort_selection"],
            "usable_period": coverage["usable_period"],
        },
    )
    write_csv(
        output_dir / "listing_cohort.csv",
        [_cohort_csv_row(row) for row in cohort_rows],
        fieldnames=COHORT_CSV_FIELDS,
    )
    write_json(output_dir / ARTIFACT_NAMES["B"], coverage)
    write_csv(
        output_dir / "exclusions.csv",
        [
            {
                "announcement_code": row.announcement_code,
                "symbol": row.symbol,
                "reason": row.reason,
                "title": row.title,
            }
            for row in exclusion_rows
        ],
        fieldnames=EXCLUSION_CSV_FIELDS,
    )
    write_json(
        output_dir / ARTIFACT_NAMES["C"],
        {
            "entry_delays": [format_listing_duration(d) for d in DEFAULT_ENTRY_DELAYS],
            "holding_periods": [format_listing_duration(h) for h in DEFAULT_HOLDING_PERIODS],
            "aggregates": [_aggregate_dict(a) for a in aggregates],
            "study_kind": "descriptive_gross_market_return",
            "warning": "not_executable_pnl_not_strategy_optimization",
        },
    )
    write_csv(
        output_dir / ARTIFACT_NAMES["D"],
        [_cell_csv_row(cell) for cell in cells],
        fieldnames=EVENT_CSV_FIELDS,
    )
    feature_cols = _feature_csv_columns(records)
    write_csv(
        output_dir / ARTIFACT_NAMES["E"],
        [_feature_csv_row(record, feature_cols) for record in records],
        fieldnames=list(FEATURE_CSV_BASE) + list(feature_cols),
    )
    write_json(
        output_dir / ARTIFACT_NAMES["F"],
        {
            "shuffled": False,
            "ratios": [str(r) for r in PHASE81_SPLIT_RATIOS],
            "train_count": split_counts[0],
            "validation_count": split_counts[1],
            "test_count": split_counts[2],
            "train_event_ids": [r.event_id for r in split.train],
            "validation_event_ids": [r.event_id for r in split.validation],
            "test_event_ids": [r.event_id for r in split.test],
        },
    )
    write_json(output_dir / ARTIFACT_NAMES["G"], to_jsonable(readiness))
    _write_summary(
        output_dir / ARTIFACT_NAMES["H"],
        cohort=cohort_rows,
        coverage=coverage,
        readiness=readiness,
        split_counts=split_counts,
    )
    return ListingCohortPipelineReport(
        cohort_count=len(cohort_rows),
        exclusion_count=len(exclusion_rows),
        raw_article_count=len(in_window),
        event_study_event_count=len(events),
        feature_record_count=len(records),
        train_count=split_counts[0],
        validation_count=split_counts[1],
        test_count=split_counts[2],
    )


async def run_listing_cohort_pilot(
    *,
    announcement_client: BinanceAnnouncementClient,
    vision_client: BinanceVisionClient,
    market_history: BinanceMarketHistory,
    output_dir: Path,
    requested_start: datetime,
    requested_end: datetime,
    max_pages: int,
    max_articles: int,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_probe_days: int,
    binance_limit: int,
    lookback_before_days: int = 0,
    fetch_details: bool = True,
    now_utc: datetime | None = None,
    target_valid_crypto_listings: int = TARGET_VALID_CRYPTO_LISTINGS,
) -> ListingCohortPipelineReport:
    """Bounded listing-cohort run: assemble, ingest historical Spot series, emit artifacts A–H."""
    validate_listing_cohort_bounds(
        start=requested_start,
        end=requested_end,
        max_probe_days=max_probe_days,
        lookback_before_days=lookback_before_days,
    )
    validate_collect_bounds(max_pages=max_pages, max_articles=max_articles, page_size=page_size)
    validate_binance_limit(binance_limit)
    assembled = await assemble_listing_cohort(
        announcement_client=announcement_client,
        vision_client=vision_client,
        requested_start=requested_start,
        requested_end=requested_end,
        max_pages=max_pages,
        max_articles=max_articles,
        page_size=page_size,
        max_probe_days=max_probe_days,
        lookback_before_days=lookback_before_days,
        fetch_details=fetch_details,
        now_utc=now_utc,
        target_valid_crypto_listings=target_valid_crypto_listings,
    )
    market_inputs, trade_inputs = await ingest_cohort_market_history(
        assembled.cohort,
        market_history,
        limit=binance_limit,
        now_utc=assembled.now_utc,
    )
    return await run_listing_cohort_pipeline(
        announcement_client=announcement_client,
        vision_client=vision_client,
        output_dir=output_dir,
        requested_start=requested_start,
        requested_end=requested_end,
        max_pages=max_pages,
        max_articles=max_articles,
        page_size=page_size,
        max_probe_days=max_probe_days,
        lookback_before_days=lookback_before_days,
        fetch_details=fetch_details,
        market_inputs=market_inputs,
        trade_inputs=trade_inputs,
        assembled=assembled,
        now_utc=assembled.now_utc,
        target_valid_crypto_listings=target_valid_crypto_listings,
    )
