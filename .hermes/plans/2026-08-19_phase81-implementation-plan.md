# Phase 8.1 — Capability Findings + Bounded Implementation Plan

> Assessed 2026-08-19 (UTC). Live read-only checks completed; source confirmed viable.

## Capability findings (the six requested answers)

1. **Official announcement history — accessible reproducibly.** No-auth GET:
   `https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&catalogId=48&pageNo=1&pageSize=N`.
   Catalog 48 = "New Cryptocurrency Listing", `total=2231` announcements.

2. **Metadata/timestamps exposed.** Per article (structured): `code` (announcement id), `id`,
   `releaseDate` (ms epoch = announcement **publication** time), `title`, `type`. The **announced Spot
   trading-start time is NOT a structured list field** — it is embedded in the title/body (e.g.
   "…will open trading for X/USDT at 2026-08-18 08:00 UTC") and requires per-article fetch + parse.
   The article-detail endpoint was not pinned in this probe (list gives title+releaseDate only).

3. **exchangeInfo `onboardDate` check — ABSENT.** Live `GET /api/v3/exchangeInfo`: 3681 symbols,
   `has_onboardDate=False`, 0 symbols carry it, no date-like keys. The `onboardDate` field in the
   synthetic fixture is not a live capability. **No listing dates from exchangeInfo.**

4. **Binance Vision — can corroborate earliest market-data times.** Daily files download (HTTP 200):
   `data/spot/daily/klines/{SYM}/1m/{SYM}-1m-{date}.zip` and
   `data/spot/daily/aggTrades/{SYM}/{SYM}-aggTrades-{date}.zip`.

5. **Estimated usable cohort size.** 2231 raw "New Cryptocurrency Listing" announcements (all history),
   **but the catalog mixes Futures / Margin / non-Spot announcements** (the first article is a Futures
   launch). Genuine Binance Spot listings are a subset; the exact count requires classification.
   The 12–24-month window is a further subset (likely several hundred Spot listings).

6. **Exact implementation gaps.**
   a. Announcement **classification** (Spot vs Futures/Margin/collateral/delisting/Alpha).
   b. **Symbol + trading-start-time extraction** (title + body parse; never infer trading-start from
      publication time when it is separately stated).
   c. **Article-detail endpoint** reverse-engineering (body with exact trading-start time).
   d. **Pagination** over 2231 announcements (bounded).
   e. **Cohort-driven Binance Vision ingestion** (earliest kline/aggTrade time per symbol).
   f. **Provenance** for every field (source_event_time vs first_market_data_time vs first_trade_time
      vs first_kline_time).

## Implementation plan (one coherent Cursor pass; no commit/push; TDD)

### Task 1 — Binance announcement collector (read-only, no auth)
- New `src/newcoin_trader/collectors/binance/announcements.py`.
- GET the catalog-48 list; bounded pagination (max pages/articles caps).
- Normalize → `ListingAnnouncement(code, id, releaseDate_ms, title, type, provenance)`.
- Fixtures for unit tests (no network in tests).

### Task 2 — Spot-listing classification + symbol/time extraction
- `src/newcoin_trader/research/listing_cohort.py`.
- Conservative classifier: SPOT_LISTING vs NOT_SPOT (Futures/Margin/collateral/delisting/Alpha/other)
  from title/body; **exclude when ambiguous** (fail-closed), record exclusion reason.
- Extract `symbol` (pair) + `source_event_time` (announced trading-start) when explicitly stated;
  otherwise mark `source_event_time` MISSING (never infer from `releaseDate`).
- Preserve `releaseDate` (publication) separately from trading-start.

### Task 3 — Binance Vision earliest-market-data corroboration
- `src/newcoin_trader/research/listing_corroboration.py`.
- Per cohort symbol: earliest 1m kline open time + earliest aggTrade time from Vision daily files
  (bounded lookback), recorded as `first_kline_time` / `first_trade_time`; `first_market_data_time`
  = min of the two. Never overwrite `source_event_time`.

### Task 4 — Cohort assembly + coverage matrix + exclusion report
- Emit deterministic `listing_cohort.csv/.json` (all provenance fields + completeness status),
  `coverage.json` (klines/trades/liquidity/depth availability), `exclusions.csv` (symbol, reason).
- `requested_period`, `usable_period`, counts, exclusion reasons — honest, no silent drops.

### Task 5 — Reuse Phase 3 event study + Phase 4 PIT features + chronological splits
- Entry-delay grid {0,1,3,5,10,15,30}m × holding grid {5,15,30,60}m through the existing Phase 3
  event-study framework (gross return, log return, MFE/MAE, censoring, valid/invalid).
- Existing Phase 4 PIT feature builder over the cohort (age/momentum/volatility/volume/liquidity/
  activity) — **no new features, no depth features**.
- Chronological train/val/test split manifest (no shuffle); report per-split counts.

### Task 6 — Artifacts + readiness report + markdown summary
- Deterministic machine-readable outputs (A–H from the spec), clearly separating gross vs executable
  vs prospective. `alpha_discovery_readiness.md` answering the five readiness questions.

## Safety / non-goals
- Long-only; no rule-2; no A/B/C/E winner selection; no prospective sessions; no short/live trading;
  no Phase 5/6/6.5/6.6 changes; no depth features; GET-only + Binance Vision downloads only.

## Acceptance (Phase 8.1 PASS)
Reproducible real listing cohort, no pre-listing contamination, explicit listing-time provenance,
bounded ingestion, entry×hold event study, chronological split manifest, PIT feature dataset,
deterministic artifacts, honest missing/exclusion reporting, and a defensible verdict on whether
Phase 8.2 rule discovery is statistically meaningful.
