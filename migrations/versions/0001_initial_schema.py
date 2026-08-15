"""Initial research schema.

Revision ID: 0001_initial_schema
Revises:
Create Date: 2024-01-01 00:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial_schema"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "tokens",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("token_address", sa.Text(), nullable=False),
        sa.Column("chain", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("created_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("first_seen_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("venue", sa.Text(), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("chain", "token_address", name="uq_tokens_chain_address"),
    )
    op.create_index("ix_tokens_first_seen_time", "tokens", ["first_seen_time"])
    op.create_index("ix_tokens_symbol", "tokens", ["symbol"])
    op.create_index("ix_tokens_created_time", "tokens", ["created_time"])

    op.create_table(
        "price_snapshots",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("token_id", sa.BigInteger(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("price", sa.Numeric(38, 18), nullable=False),
        sa.Column("volume", sa.Numeric(38, 18), nullable=True),
        sa.Column("liquidity", sa.Numeric(38, 18), nullable=True),
        sa.Column("market_cap", sa.Numeric(38, 18), nullable=True),
        sa.Column("buy_count", sa.Integer(), nullable=True),
        sa.Column("sell_count", sa.Integer(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("provenance", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("token_id", "timestamp", "source", name="uq_price_snapshots_token_ts_source"),
    )
    op.create_index(
        "ix_price_snapshots_token_timestamp",
        "price_snapshots",
        ["token_id", "timestamp"],
    )

    op.create_table(
        "trades",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("token_id", sa.BigInteger(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("amount", sa.Numeric(38, 18), nullable=False),
        sa.Column("price", sa.Numeric(38, 18), nullable=False),
        sa.Column("external_trade_id", sa.Text(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("provenance", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_trades_token_timestamp", "trades", ["token_id", "timestamp"])
    op.create_index(
        "uq_trades_composite_fallback",
        "trades",
        ["token_id", "timestamp", "side", "amount", "price", "source"],
        unique=True,
        postgresql_where=sa.text("external_trade_id IS NULL"),
    )
    op.create_index(
        "uq_trades_token_source_external_id",
        "trades",
        ["token_id", "source", "external_trade_id"],
        unique=True,
        postgresql_where=sa.text("external_trade_id IS NOT NULL"),
    )

    op.create_table(
        "strategy_results",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("strategy_name", sa.Text(), nullable=False),
        sa.Column("strategy_version", sa.Text(), nullable=False),
        sa.Column("token_id", sa.BigInteger(), nullable=True),
        sa.Column("params_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("metrics_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("signals_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id",
            "strategy_name",
            "strategy_version",
            "token_id",
            "window_start",
            "window_end",
            name="uq_strategy_results_run_strategy_token_window",
            postgresql_nulls_not_distinct=True,
        ),
    )
    op.create_index("ix_strategy_results_run_id", "strategy_results", ["run_id"])
    op.create_index(
        "ix_strategy_results_name_created",
        "strategy_results",
        ["strategy_name", "created_at"],
    )
    op.create_index("ix_strategy_results_token_id", "strategy_results", ["token_id"])

    op.create_table(
        "paper_trades",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("token_id", sa.BigInteger(), nullable=False),
        sa.Column("signal_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("requested_qty", sa.Numeric(38, 18), nullable=False),
        sa.Column("requested_price", sa.Numeric(38, 18), nullable=False),
        sa.Column("fill_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("fill_qty", sa.Numeric(38, 18), nullable=True),
        sa.Column("fee", sa.Numeric(38, 18), nullable=True),
        sa.Column("slippage_bps", sa.Numeric(18, 8), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("reject_reason", sa.Text(), nullable=True),
        sa.Column("mode", sa.Text(), server_default=sa.text("'paper'"), nullable=False),
        sa.Column("meta_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("mode = 'paper'", name="ck_paper_trades_mode_paper"),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id",
            "token_id",
            "signal_ts",
            "side",
            "requested_qty",
            "requested_price",
            name="uq_paper_trades_run_order",
        ),
    )
    op.create_index("ix_paper_trades_run_id", "paper_trades", ["run_id"])
    op.create_index("ix_paper_trades_token_signal", "paper_trades", ["token_id", "signal_ts"])


def downgrade() -> None:
    op.drop_index("ix_paper_trades_token_signal", table_name="paper_trades")
    op.drop_index("ix_paper_trades_run_id", table_name="paper_trades")
    op.drop_table("paper_trades")
    op.drop_index("ix_strategy_results_token_id", table_name="strategy_results")
    op.drop_index("ix_strategy_results_name_created", table_name="strategy_results")
    op.drop_index("ix_strategy_results_run_id", table_name="strategy_results")
    op.drop_table("strategy_results")
    op.drop_index("uq_trades_token_source_external_id", table_name="trades")
    op.drop_index("uq_trades_composite_fallback", table_name="trades")
    op.drop_index("ix_trades_token_timestamp", table_name="trades")
    op.drop_table("trades")
    op.drop_index("ix_price_snapshots_token_timestamp", table_name="price_snapshots")
    op.drop_table("price_snapshots")
    op.drop_index("ix_tokens_created_time", table_name="tokens")
    op.drop_index("ix_tokens_symbol", table_name="tokens")
    op.drop_index("ix_tokens_first_seen_time", table_name="tokens")
    op.drop_table("tokens")
