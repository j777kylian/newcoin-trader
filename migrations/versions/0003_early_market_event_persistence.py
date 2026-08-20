"""Phase 8A.2 early-market-event persistence tables.

Revision ID: 0003_early_event_store
Revises: 0002_live_paper_session_state
Create Date: 2026-08-21 00:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003_early_event_store"
down_revision: str | None = "0002_live_paper_session_state"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "markets",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("market_key", sa.Text(), nullable=False),
        sa.Column("base_token_id", sa.BigInteger(), nullable=False),
        sa.Column("quote_token_id", sa.BigInteger(), nullable=True),
        sa.Column("pool_or_pair_address", sa.Text(), nullable=True),
        sa.Column("venue", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("source_native_market_id", sa.Text(), nullable=True),
        sa.Column("market_kind", sa.Text(), nullable=False),
        sa.Column("identity_status", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("provenance_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["base_token_id"], ["tokens.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["quote_token_id"], ["tokens.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("market_key", name="uq_markets_market_key"),
    )
    op.create_index("ix_markets_base_token_id", "markets", ["base_token_id"])
    op.create_index("ix_markets_base_token_pool", "markets", ["base_token_id", "pool_or_pair_address"])

    op.create_table(
        "early_market_events",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source_native_event_id", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("event_kind", sa.Text(), nullable=False),
        sa.Column("event_definition_version", sa.Text(), nullable=False),
        sa.Column("venue_or_protocol", sa.Text(), nullable=False),
        sa.Column("chain", sa.Text(), nullable=False),
        sa.Column("asset_token_id", sa.BigInteger(), nullable=False),
        sa.Column("market_id", sa.BigInteger(), nullable=True),
        sa.Column("source_event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("decision_available_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_market_data_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("first_liquidity_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("first_trade_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("event_time_semantics", sa.Text(), nullable=False),
        sa.Column("event_quality_status", sa.Text(), nullable=False),
        sa.Column("event_clock_quality", sa.Text(), nullable=False),
        sa.Column("provenance_ref", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["asset_token_id"], ["tokens.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["market_id"], ["markets.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source",
            "source_native_event_id",
            name="uq_early_market_events_source_native_id",
        ),
    )
    op.create_index(
        "ix_early_market_events_source_event_time_id",
        "early_market_events",
        ["source_event_time", "id"],
    )
    op.create_index("ix_early_market_events_asset_token_id", "early_market_events", ["asset_token_id"])

    op.create_table(
        "early_market_event_evidence",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("event_id", sa.BigInteger(), nullable=False),
        sa.Column("evidence_kind", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_native_evidence_id", sa.Text(), nullable=True),
        sa.Column("observed_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("endpoint", sa.Text(), nullable=True),
        sa.Column("dataset", sa.Text(), nullable=True),
        sa.Column("stable_locator", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("payload_metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["early_market_events.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_early_market_event_evidence_event_id",
        "early_market_event_evidence",
        ["event_id"],
    )
    op.create_index(
        "ix_early_market_event_evidence_observed_time_id",
        "early_market_event_evidence",
        ["observed_time", "id"],
    )

    op.create_table(
        "early_market_observations",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("market_id", sa.BigInteger(), nullable=False),
        sa.Column("event_id", sa.BigInteger(), nullable=True),
        sa.Column("source_native_observation_id", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("availability_status", sa.Text(), nullable=False),
        sa.Column("price", sa.Numeric(38, 18), nullable=True),
        sa.Column("quantity", sa.Numeric(38, 18), nullable=True),
        sa.Column("liquidity", sa.Numeric(38, 18), nullable=True),
        sa.Column("base_reserve", sa.Numeric(38, 18), nullable=True),
        sa.Column("quote_reserve", sa.Numeric(38, 18), nullable=True),
        sa.Column("side", sa.Text(), nullable=True),
        sa.Column("resolution", sa.Text(), nullable=True),
        sa.Column("provenance_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["market_id"], ["markets.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["event_id"], ["early_market_events.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source",
            "source_native_observation_id",
            name="uq_early_market_observations_source_native_id",
        ),
    )
    op.create_index(
        "ix_early_market_observations_source_time_id",
        "early_market_observations",
        ["source_time", "id"],
    )
    op.create_index("ix_early_market_observations_market_id", "early_market_observations", ["market_id"])


def downgrade() -> None:
    op.drop_index("ix_early_market_observations_market_id", table_name="early_market_observations")
    op.drop_index("ix_early_market_observations_source_time_id", table_name="early_market_observations")
    op.drop_table("early_market_observations")

    op.drop_index(
        "ix_early_market_event_evidence_observed_time_id",
        table_name="early_market_event_evidence",
    )
    op.drop_index("ix_early_market_event_evidence_event_id", table_name="early_market_event_evidence")
    op.drop_table("early_market_event_evidence")

    op.drop_index("ix_early_market_events_asset_token_id", table_name="early_market_events")
    op.drop_index("ix_early_market_events_source_event_time_id", table_name="early_market_events")
    op.drop_table("early_market_events")

    op.drop_index("ix_markets_base_token_pool", table_name="markets")
    op.drop_index("ix_markets_base_token_id", table_name="markets")
    op.drop_table("markets")
