"""Shared enumerations."""

from __future__ import annotations

from enum import StrEnum


class Chain(StrEnum):
    SOLANA = "solana"
    BINANCE = "binance"


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class Venue(StrEnum):
    BINANCE = "binance"
    BIRDEYE = "birdeye"
    RAYDIUM = "raydium"
    GECKO = "geckoterminal"


class ExecMode(StrEnum):
    PAPER = "paper"
    LIVE = "live"


class PaperStatus(StrEnum):
    FILLED = "filled"
    PARTIAL = "partial"
    REJECTED = "rejected"


class RejectReason(StrEnum):
    MAX_NOTIONAL = "max_notional"
    MAX_POSITION_SIZE = "max_position_size"
    MAX_OPEN_POSITIONS = "max_open_positions"
    MAX_DRAWDOWN = "max_drawdown"
    INSUFFICIENT_LIQUIDITY = "insufficient_liquidity"
    LIVE_EXECUTION_FORBIDDEN = "live_execution_forbidden"
    INVALID_ORDER = "invalid_order"
    LOOKAHEAD_FORBIDDEN = "lookahead_forbidden"
    MARKET_MISMATCH = "market_mismatch"
    LIMIT_NOT_MET = "limit_not_met"
    SELL_EXCEEDS_POSITION = "sell_exceeds_position"


class SignalKind(StrEnum):
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
