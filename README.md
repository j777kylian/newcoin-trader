# newcoin-trader

Paper-only research platform for newly listed Binance spot assets and newly launched Solana meme coins.

The pipeline is **discover → store → analyze listing-relative behavior → infer candidate windows → paper-simulate → report**. Live trading, wallets, signing, and LLM trade decisions are out of scope and structurally blocked.

## Safety policy

- Research and simulation only. Real order execution is forbidden.
- No wallet libraries, private keys, seed phrases, signing, transaction submission, or authenticated account/order endpoints.
- LLMs do not decide trades. There is no LLM integration in this repository.
- The sole execution gateway is paper-only and **fail-closed**: any live/non-paper request raises `LiveExecutionForbiddenError` **before** any broker, HTTP, or network call.
- Collectors are read-only public market-data wrappers. Raydium is pool lookup and GET quote compute only — never swap transaction submission.
- Secrets are never committed. Configuration is environment-only. This project does **not** load a local `.env` file. Use `.env.example` as a placeholder template.

## Architecture

```text
CLI / offline smoke
        │
        ▼
Collectors (httpx, public GET) → typed domain records
        │
        ▼
PostgreSQL (SQLAlchemy 2 async + Alembic)  ← idempotent upserts
        │
        ▼
Research (pandas, listing-relative windows, no look-ahead)
        │
        ▼
Deterministic Strategy → Risk checks → ExecutionGateway (PAPER only)
        │
        ▼
Reports (JSON + CSV)
```

Package layout under `src/newcoin_trader`:

| Area | Role |
|------|------|
| `collectors` | Binance, Birdeye, Raydium, GeckoTerminal read-only wrappers |
| `services` | Discovery/ingestion orchestration (`collect-once`, poll loop) |
| `database` | Async engine, ORM models, idempotent repositories |
| `domain` / `models` | Typed records (`Decimal`, UTC datetimes) |
| `research` | Listing-relative metrics and candidate windows |
| `strategies` | Deterministic `Strategy` protocol (no LLM) |
| `risk` | Notional / size / open / drawdown / liquidity limits |
| `execution` | Paper broker + fail-closed gateway |
| `reports` | Reproducible JSON/CSV writers |
| `cli` | Typer entry point (`smoke-offline`, `collect-once`, `poll`, `ingest-market-history`, `event-study`) |

## Data flow

1. **Discovery** — Birdeye new tokens/pairs and Binance `exchangeInfo` produce `NewListingEvent` records. Repositories upsert `tokens`; `first_seen_time` is never moved later.
2. **Market ingest** — Binance klines/trades/book/ticker, GeckoTerminal pool+OHLCV, Raydium pool+quote normalize into `PriceSnapshot` / `TradeTick` / `Kline` / `PoolQuote` and upsert with unique provenance keys.
3. **Research** — Load a series relative to listing time `t0`, compute return / volatility / max drawdown / liquidity-volume stats over configurable windows, infer candidate entry/exit windows. Output is **research, not trading advice**.
4. **Paper simulation** — A deterministic strategy emits signals from data at-or-before evaluation time. Risk rejects or accepts. The gateway accepts `paper` mode only. The paper broker applies configured fees and slippage, caps fills by observed liquidity, and **rejects** market snapshots after `signal_ts` (no look-ahead) or with token/chain mismatch.
5. **Reports** — JSON and CSV artifacts under `artifacts/` (gitignored).

## Reproducible Phase 1/2 setup

This Phase 1/2 baseline was runtime-verified on **macOS** with:

| Component | Verified version / tool |
|---|---|
| Python | **3.11.16** (`.python-version`) |
| Dependency manager | `uv 0.12.4` |
| PostgreSQL | **16.15 Homebrew** (PostgreSQL **16** is required) |
| SQLAlchemy / Alembic / asyncpg | 2.0.52 / 1.19.1 / 0.31.0 |
| pytest / Ruff / Mypy | 9.1.1 / 0.16.3 / 2.3.0 |
| Build backend | Hatchling, built through `uv build` |

`pyproject.toml` defines dependencies and **`uv.lock` is the committed reproducible resolution**. Do not use an unlocked install for acceptance work. The lock is multi-platform; a fresh Linux or macOS environment resolves the exact locked artifacts appropriate to its platform.

### 1. Clone and install Python/dependencies

```bash
git clone <REPOSITORY_URL> newcoin-trader
cd newcoin-trader

# Install uv if needed: https://docs.astral.sh/uv/getting-started/installation/
# Install the exact tested interpreter when it is not already available.
uv python install 3.11.16

# Creates the project virtual environment and installs the locked runtime + dev set.
uv sync --locked --extra dev
source .venv/bin/activate

python --version               # Python 3.11.16
uv lock --check                # verifies uv.lock still matches pyproject.toml
```

The checked-in `.python-version` lets version managers and `uv` discover the intended interpreter. `pyproject.toml` permits Python 3.11+, but **3.11.16 is the tested baseline**.

### 2. PostgreSQL 16

PostgreSQL is required for migrations, database-backed CLI work, and PostgreSQL integration tests. It is not needed for the offline smoke test.

#### macOS/Homebrew — runtime verified

```bash
brew install postgresql@16
brew services start postgresql@16
createdb newcoin
createdb newcoin_test

psql -d newcoin_test -c 'SELECT version(), current_database();'
# Local peer-auth test URL verified in this repository:
export NEWCOIN_TEST_DATABASE_URL=postgresql+asyncpg://localhost/newcoin_test
```

The local acceptance run used PostgreSQL **16.15** supplied by Homebrew. Your local PostgreSQL role must be allowed to create the two databases above; use your own role/credentials rather than committing them.

#### Linux/VPS — documented, not runtime-verified here

Install PostgreSQL **16** through your distribution or PostgreSQL's official packages, start the service, then create separate application and test databases with the administrative role:

```bash
sudo -u postgres createdb newcoin
sudo -u postgres createdb newcoin_test
```

Set URLs for the account you created; do not use a superuser URL in a deployment. The application expects SQLAlchemy's asynchronous driver form:

```bash
export DATABASE_URL='postgresql+asyncpg://USER:PASSWORD@HOST:5432/newcoin'
export NEWCOIN_TEST_DATABASE_URL='postgresql+asyncpg://USER:PASSWORD@HOST:5432/newcoin_test'
```

#### Optional Docker Compose — documented, not runtime-verified here

The included `docker-compose.yml` already pins `postgres:16-alpine` and provides an isolated developer PostgreSQL service:

```bash
docker compose up -d postgres
docker compose exec postgres pg_isready -U newcoin -d newcoin
# Compose developer defaults only (replace for any non-local deployment):
export DATABASE_URL='postgresql+asyncpg://newcoin:newcoin@localhost:5432/newcoin'
```

Docker is optional; it is not required for the verified Homebrew path.

### 3. Environment variables

Configuration is **process-environment only**. The application never loads a local `.env` file. `.env.example` contains placeholders and safe defaults; real `.env`, API keys, passwords, and tokens are ignored by Git.

| Variable group | Status | Purpose |
|---|---|---|
| `DATABASE_URL` | Required for application/research DB commands | Async PostgreSQL URL for Alembic and DB-backed CLI work |
| `NEWCOIN_TEST_DATABASE_URL` | Test-only; required to execute PostgreSQL integration tests | Points to a separate disposable test database; safe local example: `postgresql+asyncpg://localhost/newcoin_test` |
| `EXECUTION_MODE` | Optional; defaults to `paper` | Must remain `paper`; any other value is rejected |
| `BIRDEYE_API_KEY` | API-specific | Required only for live `collect-once`/`poll` Solana discovery; not needed for offline/unit/PostgreSQL fixture tests |
| `BINANCE_BASE_URL`, `BIRDEYE_BASE_URL`, `RAYDIUM_*`, `GECKO_BASE_URL` | Optional | Public read-only API endpoint overrides |
| `HTTP_*` | Optional | HTTP timeout, retries, backoff, and rate limits |
| `PAPER_*`, `RISK_*`, `POLL_INTERVAL_SECONDS` | Optional | Deterministic paper-simulation/risk/polling controls |
| `REPORTS_DIR`, `LOG_LEVEL` | Optional | Local report output and logging |

Set only what you need in the shell. Do **not** run `source .env.example` unchanged: `DATABASE_URL` intentionally contains `USER:PASSWORD` placeholders.

### 4. Initialize the application database with Alembic

The application/research database and the test database are separate.

```bash
# Application/research database only — migrations, never test-only create_all().
export DATABASE_URL='postgresql+asyncpg://USER:PASSWORD@localhost:5432/newcoin'
alembic upgrade head
alembic current

# Test database is created separately and is selected only through this variable.
export NEWCOIN_TEST_DATABASE_URL=postgresql+asyncpg://localhost/newcoin_test
```

`alembic current` reports the applied revision; `alembic upgrade head --sql` generates PostgreSQL DDL for offline review without applying it.

### 5. Fresh-environment and validation commands

Offline smoke requires no API keys, wallet, funds, network, or PostgreSQL:

```bash
newcoin-trader smoke-offline --output-dir artifacts/smoke
```

With PostgreSQL available, run the exact baseline gates:

```bash
export NEWCOIN_TEST_DATABASE_URL=postgresql+asyncpg://localhost/newcoin_test

# PostgreSQL runtime paths
pytest -q tests/integration/test_db_repositories.py -rs
pytest -q tests/integration/test_market_history_persistence.py -rs

# Complete suite; integration tests must execute when the variable is set.
pytest -q -rs

# Static/packaging/reproducibility checks
ruff check src tests scripts
ruff format --check src tests scripts   # see note below about historical formatting debt
mypy src
DATABASE_URL='postgresql+asyncpg://localhost/newcoin_test' alembic upgrade head --sql > /tmp/newcoin-schema.sql
python scripts/smoke_offline.py
python -m compileall -q src/newcoin_trader
uv build --wheel
```

For a fresh isolated reproduction without touching an existing `.venv`, copy or freshly clone the repository, then run `uv sync --locked --extra dev`; run `python -c 'import newcoin_trader'`, the installed `newcoin-trader smoke-offline` command, and the PostgreSQL test commands above from that isolated checkout. The verified baseline performed this with PostgreSQL 16.15 locally.

**Formatting policy:** every file changed by a maintenance pass must pass a scoped `ruff format --check`. Repository-wide formatting may report historical unrelated files; do not reformat unrelated code merely to silence that report.

## Offline demo

```bash
python scripts/smoke_offline.py
# equivalent installed CLI (subcommand required):
newcoin-trader smoke-offline --output-dir artifacts
```

Loads packaged `newcoin_trader.resources.demo_run` fixtures (optional `--fixtures-dir` override), computes listing-relative analysis, runs the listing-momentum strategy through risk + paper execution, and writes artifacts. No HTTP client is constructed and no database is required.

## Live discovery / ingestion (opt-in research collection)

These commands use public **GET-only** APIs and write research data to PostgreSQL. They never place orders, submit transactions, access a wallet, or sign data. They are not part of the offline smoke gate.

Prerequisites:

1. Application PostgreSQL database is up and migrated through Alembic.
2. `DATABASE_URL` is exported for that application database.
3. A real `BIRDEYE_API_KEY` is exported only when `collect-once` or `poll` needs Solana discovery.

```bash
# One bounded discovery cycle: Binance exchangeInfo + Birdeye new tokens/pairs → tokens upsert
newcoin-trader collect-once --birdeye-limit 10

# Bounded polling example: two cycles, then exit.
newcoin-trader poll --interval 60 --max-iterations 2

# Bounded GET-only market history (Binance / Raydium / GeckoTerminal) → snapshots + trades
newcoin-trader ingest-market-history --binance-symbol NEWUSDT --binance-limit 100
newcoin-trader ingest-market-history --raydium-page-size 10 --raydium-page 1
newcoin-trader ingest-market-history --gecko-network solana --gecko-pool <POOL> --gecko-ohlcv-limit 100
```

`ingest-market-history` request/page/record controls are strictly positive integers. Zero, negative, non-integer, and excessive values fail at the CLI and `MarketHistoryService` **before any HTTP or database work**. Omit `--raydium-page-size` to skip Raydium. Bounds (inclusive):

| Control | Min | Max | Notes |
|---------|-----|-----|--------|
| `--binance-limit` | 1 | 1000 | Binance klines / aggTrades / trades page size |
| `--raydium-page` | 1 | 100 | Raydium pool list page |
| `--raydium-page-size` | 1 | 100 | Omit to skip Raydium |
| `--gecko-ohlcv-limit` | 1 | 1000 | GeckoTerminal OHLCV limit |

## External data sources

All Phase 1/2 collectors are public, read-only, and structurally GET-only.

| Source | Current Phase 1/2 purpose | Authentication / variables |
|--------|---------------------------|----------------------------|
| **Binance Spot** | Listing discovery plus klines, public trades, depth, and ticker market history | No API key. `BINANCE_BASE_URL` optional. Weight-limited public REST. |
| **Raydium** | Solana pool discovery/metadata and read-only quote computation | No API key. `RAYDIUM_POOL_BASE_URL` / `RAYDIUM_QUOTE_BASE_URL` optional. Swap transaction submission endpoints are absent. |
| **GeckoTerminal** | Pool state and OHLCV market history | No API key. `GECKO_BASE_URL` optional. JSON:API and rate-limited public endpoints. |
| **Birdeye** | Solana new-token/new-pair discovery | `BIRDEYE_API_KEY` required only for live discovery. `BIRDEYE_BASE_URL` and `BIRDEYE_CHAIN` optional. |

Candidate entry/exit windows are deterministic research labels on historical returns. They are **not** investment or trading advice.

## Phase 3 descriptive event-study (research only)

Phase 3 answers **future gross market-return distributions** by `venue × entry_delay × holding_period` from existing PostgreSQL token/price snapshot rows. It is **descriptive research**, not strategy optimization and not executable PnL.

```bash
# Requires DATABASE_URL. Bounds are mandatory (no analyze-everything default).
newcoin-trader event-study \
  --venue binance \
  --start 2024-01-01T00:00:00+00:00 \
  --end 2024-02-01T00:00:00+00:00 \
  --max-events 100 \
  --output-dir artifacts/event_study
```

Defaults: entry delays `10s,30s,1m,2m,5m,10m,15m,30m`; holdings `1m,5m,15m,30m,1h,2h,4h,24h`. Sub-minute delays require point/trade resolution and are marked `unsupported_resolution` for minute OHLCV. Outputs: `event_study_summary.json`, `event_study_cells.csv`, `event_study_summary.md`. Empty databases emit a valid zero-sample report.

## License

Research MVP. Use at your own risk. No warranty.
