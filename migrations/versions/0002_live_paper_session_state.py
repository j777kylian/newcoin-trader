"""Phase 6 live-paper durable session/signal/position tables.

Revision ID: 0002_live_paper_session_state
Revises: 0001_initial_schema
Create Date: 2026-08-16 00:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_live_paper_session_state"
down_revision: str | None = "0001_initial_schema"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "live_paper_sessions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("config_id", sa.Text(), nullable=False),
        sa.Column("venue", sa.Text(), nullable=False),
        sa.Column("session_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("frozen_rule_id", sa.Text(), nullable=False),
        sa.Column("phase4_config_id", sa.Text(), nullable=False),
        sa.Column("starting_cash", sa.Numeric(38, 18), nullable=False),
        sa.Column("realized_pnl", sa.Numeric(38, 18), server_default=sa.text("0"), nullable=False),
        sa.Column("halted", sa.Text(), server_default=sa.text("'false'"), nullable=False),
        sa.Column("state_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("session_id", name="uq_live_paper_sessions_session_id"),
    )
    op.create_index(
        "ix_live_paper_sessions_venue_start",
        "live_paper_sessions",
        ["venue", "session_start"],
    )

    op.create_table(
        "live_paper_signals",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("signal_id", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("rule_id", sa.Text(), nullable=False),
        sa.Column("decision_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("provenance_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("session_id", "signal_id", name="uq_live_paper_signals_session_signal"),
    )
    op.create_index("ix_live_paper_signals_session_id", "live_paper_signals", ["session_id"])

    op.create_table(
        "live_paper_positions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("position_id", sa.Text(), nullable=False),
        sa.Column("signal_id", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("token_address", sa.Text(), nullable=False),
        sa.Column("venue", sa.Text(), nullable=False),
        sa.Column("lifecycle", sa.Text(), nullable=False),
        sa.Column("entry_notional", sa.Numeric(38, 18), nullable=False),
        sa.Column("entry_qty", sa.Numeric(38, 18), nullable=False),
        sa.Column("entry_price", sa.Numeric(38, 18), nullable=False),
        sa.Column("exit_qty", sa.Numeric(38, 18), nullable=True),
        sa.Column("exit_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("realized_pnl", sa.Numeric(38, 18), nullable=True),
        sa.Column("entry_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("exit_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("meta_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("session_id", "position_id", name="uq_live_paper_positions_session_position"),
    )
    op.create_index("ix_live_paper_positions_session_id", "live_paper_positions", ["session_id"])


def downgrade() -> None:
    op.drop_index("ix_live_paper_positions_session_id", table_name="live_paper_positions")
    op.drop_table("live_paper_positions")
    op.drop_index("ix_live_paper_signals_session_id", table_name="live_paper_signals")
    op.drop_table("live_paper_signals")
    op.drop_index("ix_live_paper_sessions_venue_start", table_name="live_paper_sessions")
    op.drop_table("live_paper_sessions")
