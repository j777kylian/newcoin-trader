"""Database model metadata and repository SQL (no live Postgres required)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from newcoin_trader.database.idempotency import earliest_first_seen, snapshot_idempotency_key
from newcoin_trader.database.models import PaperTrade, PriceSnapshot, StrategyResult, Token, Trade
from newcoin_trader.database.repositories.tokens import token_upsert_statement


def _constraint_names(table: object) -> set[str]:
    return {c.name for c in table.constraints if c.name}  # type: ignore[attr-defined]


def _index_names(table: object) -> set[str]:
    return {i.name for i in table.indexes if i.name}  # type: ignore[attr-defined]


def test_tokens_unique_chain_address() -> None:
    names = _constraint_names(Token.__table__)
    assert "uq_tokens_chain_address" in names


def test_price_snapshots_unique_idempotency_key() -> None:
    names = _constraint_names(PriceSnapshot.__table__)
    assert "uq_price_snapshots_token_ts_source" in names


def test_trades_idempotency_indexes() -> None:
    names = _index_names(Trade.__table__) | _constraint_names(Trade.__table__)
    assert "uq_trades_token_source_external_id" in names
    assert "uq_trades_composite_fallback" in names


def test_paper_trades_mode_must_be_paper() -> None:
    sql = str(CreateTable(PaperTrade.__table__).compile(dialect=postgresql.dialect()))
    assert "mode" in sql
    checks = [c.sqltext.text for c in PaperTrade.__table__.constraints if hasattr(c, "sqltext")]
    assert any("paper" in text for text in checks)


def test_numeric_precision_on_snapshots() -> None:
    col = PriceSnapshot.__table__.c.price
    assert col.type.precision == 38  # type: ignore[attr-defined]
    assert col.type.scale == 18  # type: ignore[attr-defined]


def test_strategy_results_has_run_and_version() -> None:
    columns = set(StrategyResult.__table__.c.keys())
    assert {"run_id", "strategy_name", "strategy_version", "params_json", "metrics_json"} <= columns


def test_first_seen_never_moves_later() -> None:
    early = datetime(2024, 1, 1, tzinfo=UTC)
    late = datetime(2024, 6, 1, tzinfo=UTC)
    assert earliest_first_seen(existing=early, incoming=late) == early
    assert earliest_first_seen(existing=late, incoming=early) == early


def test_token_upsert_sql_uses_least_for_first_seen() -> None:
    stmt = token_upsert_statement(
        chain="solana",
        token_address="Mint111",
        symbol="MEME",
        created_time=datetime(2024, 1, 1, tzinfo=UTC),
        first_seen_time=datetime(2024, 1, 2, tzinfo=UTC),
        source="birdeye",
        venue="raydium",
        metadata_json={"k": "v"},
    )
    compiled = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": False}))
    assert "least" in compiled.lower()
    assert "on conflict" in compiled.lower()


def test_snapshot_idempotency_key_is_stable() -> None:
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    key = snapshot_idempotency_key(token_id=1, timestamp=ts, source="gecko")
    assert key == (1, ts, "gecko")


def test_decimal_column_python_type() -> None:
    assert PriceSnapshot.__table__.c.volume.type.as_generic().python_type is Decimal  # type: ignore[union-attr]
