"""Domain package re-exports."""

from newcoin_trader.domain.enums import (
    Chain,
    ExecMode,
    PaperStatus,
    RejectReason,
    Side,
    SignalKind,
    Venue,
)
from newcoin_trader.domain.execution import PaperFill, PaperOrder, PortfolioState, RejectedOrder
from newcoin_trader.domain.market import (
    Kline,
    OrderBookL2,
    PoolQuote,
    PoolSnapshot,
    PriceSnapshot,
    Ticker24h,
    TradeTick,
)
from newcoin_trader.domain.research import CandidateWindow, ListingAnalysis, WindowStats
from newcoin_trader.domain.strategy import Signal, StrategyContext, StrategyRunResult
from newcoin_trader.domain.tokens import NewListingEvent, TokenRef

__all__ = [
    "CandidateWindow",
    "Chain",
    "ExecMode",
    "Kline",
    "ListingAnalysis",
    "NewListingEvent",
    "OrderBookL2",
    "PaperFill",
    "PaperOrder",
    "PaperStatus",
    "PoolQuote",
    "PoolSnapshot",
    "PortfolioState",
    "PriceSnapshot",
    "RejectedOrder",
    "RejectReason",
    "Side",
    "Signal",
    "SignalKind",
    "StrategyContext",
    "StrategyRunResult",
    "Ticker24h",
    "TokenRef",
    "TradeTick",
    "Venue",
    "WindowStats",
]
