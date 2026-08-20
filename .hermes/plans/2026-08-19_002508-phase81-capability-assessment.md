# Phase 8.1 — Read-Only Capability Assessment (Historical Listing Cohort)

> Assessed: 2026-08-19 (UTC). Read-only; no network, no implementation, no Cursor.

## Verdict (preflight)

**BLOCKER — no executable end-to-end historical Binance Spot listing-event source exists in the
frozen archive.** The historical listing cohort required by Phase 8.1 cannot be constructed
reproducibly with the current collectors. Per the fail-closed capability rule, no manifest, provider
request, or ingestion is authorized until a listing source is chosen.

## 1. Existing historical listing-data capability

| Source | Discovery | Historical? | Notes |
|---|---|---|---|
| Binance `exchange_info` | currently-trading symbols | **no** | parses a hopeful `onboardDate` field; see below |
| Binance `klines` | n/a (per symbol) | **yes** | `startTime`/`endTime`/`limit` ≤1000/call |
| Binance `agg_trades` | n/a (per symbol) | **yes** | `fromId`/`startTime`/`endTime` ≤1000/call |
| Binance `recent_trades` | recent only | no | no historical window |
| Binance `order_book` | current L2 only | **no** | no historical depth |
| Binance `ticker_24h` | current only | no | no historical ticker |
| Birdeye `new_tokens`/`new_pairs` | recent/current, Solana, API key | **no** | not Binance Spot, not 12–24 mo history |
| Raydium | Solana DEX pools | no | not Binance Spot |
| Gecko | supplied pool only | partial | no listing discovery |

## 2. What listing timestamps can actually be reconstructed

- `source_event_time` (authoritative listing time): **NOT reliably reconstructible.**
  - The only `onboardDate` evidence in the repo is a 2-symbol **synthetic test fixture**
    (`fixtures/binance/exchange_info.json`: fictional `NEWUSDT`/`OLDUSDT`, round-number dates).
  - No captured real exchangeInfo response confirms the live API returns `onboardDate`.
  - Even if `onboardDate` were live, `exchange_info` only lists **currently-trading** symbols, so any
    derived cohort would be **survivorship-biased** (delisted listings invisible).
- `first_seen_time`: = `exchange_info` `serverTime` at poll time (≈ now) — meaningless for a historical
  cohort.
- `first_market_data_time`: **reconstructible per-symbol** from the earliest `klines`/`agg_trades`
  timestamp — but only for a symbol list we already know are listings (which we cannot discover).

## 3. Historical market-data coverage

- 1m OHLCV (klines): available, paginated (≤1000/call).
- Aggregated trades: available, paginated (≤1000/call, `fromId` continuation).
- Ticker-derived history: not available.
- Liquidity proxy: indirect via kline `quote_volume` (no true historical L2/liquidity).
- **Historical L2 depth: NOT available** → Phase 5 historical execution must use the existing modeled
  path (as anticipated; Phase 8.1 already defers depth features).

## 4. Estimated cohort size

**Unknown / unconstructible.** The application research DB contains no historical corpus:
`price_snapshots=0`, `trades=0`, `paper_trades=0`, `strategy_results=0` (and the listing table is
effectively empty). No listing cohort or market history has ever been ingested.

## 5. Can existing Phase 3/4 code do most of the work?

**Yes — analytically.** The validated Phase 3 event-study framework, Phase 4 PIT feature builder, and
Phase 5 executable backtest would compute the entry-delay × holding-period matrix, PIT features, and
chronological splits **if** given (a) a valid listing cohort with timestamps and (b) per-listing
historical klines/aggTrades. The gap is **data acquisition**, not analytics.

## 6. Exact missing pieces requiring implementation

1. **A historical Binance Spot listing-event source** (symbol universe + authoritative listing time).
   This is the fundamental blocker. No existing collector provides it.
2. **Bounded cohort-driven historical ingestion** — the existing `MarketHistoryService` is
   single-symbol/manual; it needs a cohort loop + kline/aggTrade pagination + provenance backfill.
3. (No depth work — explicitly deferred by Phase 8.1.)

## Decision needed before any implementation

Which historical listing source to authorize (see chat for options). This must be resolved before a
bounded implementation plan or any Cursor pass is meaningful.
