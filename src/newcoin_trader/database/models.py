"""SQLAlchemy 2 mapped tables for the research MVP."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from newcoin_trader.database.base import Base

NUMERIC = Numeric(38, 18)


class Token(Base):
    __tablename__ = "tokens"
    __table_args__ = (
        UniqueConstraint("chain", "token_address", name="uq_tokens_chain_address"),
        Index("ix_tokens_first_seen_time", "first_seen_time"),
        Index("ix_tokens_symbol", "symbol"),
        Index("ix_tokens_created_time", "created_time"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    token_address: Mapped[str] = mapped_column(Text, nullable=False)
    chain: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    created_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_seen_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    venue: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    snapshots: Mapped[list[PriceSnapshot]] = relationship(back_populates="token")
    trades: Mapped[list[Trade]] = relationship(back_populates="token")


class PriceSnapshot(Base):
    __tablename__ = "price_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "token_id",
            "timestamp",
            "source",
            name="uq_price_snapshots_token_ts_source",
        ),
        Index("ix_price_snapshots_token_timestamp", "token_id", "timestamp"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    token_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("tokens.id", ondelete="CASCADE"), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    price: Mapped[Decimal] = mapped_column(NUMERIC, nullable=False)
    volume: Mapped[Decimal | None] = mapped_column(NUMERIC, nullable=True)
    liquidity: Mapped[Decimal | None] = mapped_column(NUMERIC, nullable=True)
    market_cap: Mapped[Decimal | None] = mapped_column(NUMERIC, nullable=True)
    buy_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sell_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    provenance: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    token: Mapped[Token] = relationship(back_populates="snapshots")


class Trade(Base):
    __tablename__ = "trades"
    __table_args__ = (
        Index(
            "uq_trades_composite_fallback",
            "token_id",
            "timestamp",
            "side",
            "amount",
            "price",
            "source",
            unique=True,
            postgresql_where=text("external_trade_id IS NULL"),
        ),
        Index(
            "uq_trades_token_source_external_id",
            "token_id",
            "source",
            "external_trade_id",
            unique=True,
            postgresql_where=text("external_trade_id IS NOT NULL"),
        ),
        Index("ix_trades_token_timestamp", "token_id", "timestamp"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    token_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("tokens.id", ondelete="CASCADE"), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    side: Mapped[str] = mapped_column(Text, nullable=False)
    amount: Mapped[Decimal] = mapped_column(NUMERIC, nullable=False)
    price: Mapped[Decimal] = mapped_column(NUMERIC, nullable=False)
    external_trade_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    provenance: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    token: Mapped[Token] = relationship(back_populates="trades")


class StrategyResult(Base):
    __tablename__ = "strategy_results"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "strategy_name",
            "strategy_version",
            "token_id",
            "window_start",
            "window_end",
            name="uq_strategy_results_run_strategy_token_window",
            postgresql_nulls_not_distinct=True,
        ),
        Index("ix_strategy_results_run_id", "run_id"),
        Index("ix_strategy_results_name_created", "strategy_name", "created_at"),
        Index("ix_strategy_results_token_id", "token_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    strategy_name: Mapped[str] = mapped_column(Text, nullable=False)
    strategy_version: Mapped[str] = mapped_column(Text, nullable=False)
    token_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("tokens.id", ondelete="SET NULL"), nullable=True
    )
    params_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    metrics_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    signals_json: Mapped[list[Any] | None] = mapped_column(JSONB, nullable=True)
    window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    window_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class PaperTrade(Base):
    __tablename__ = "paper_trades"
    __table_args__ = (
        CheckConstraint("mode = 'paper'", name="ck_paper_trades_mode_paper"),
        UniqueConstraint(
            "run_id",
            "token_id",
            "signal_ts",
            "side",
            "requested_qty",
            "requested_price",
            name="uq_paper_trades_run_order",
        ),
        Index("ix_paper_trades_run_id", "run_id"),
        Index("ix_paper_trades_token_signal", "token_id", "signal_ts"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    token_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("tokens.id", ondelete="CASCADE"), nullable=False)
    signal_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    side: Mapped[str] = mapped_column(Text, nullable=False)
    requested_qty: Mapped[Decimal] = mapped_column(NUMERIC, nullable=False)
    requested_price: Mapped[Decimal] = mapped_column(NUMERIC, nullable=False)
    fill_price: Mapped[Decimal | None] = mapped_column(NUMERIC, nullable=True)
    fill_qty: Mapped[Decimal | None] = mapped_column(NUMERIC, nullable=True)
    fee: Mapped[Decimal | None] = mapped_column(NUMERIC, nullable=True)
    slippage_bps: Mapped[Decimal | None] = mapped_column(Numeric(18, 8), nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    reject_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    mode: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'paper'"))
    meta_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
