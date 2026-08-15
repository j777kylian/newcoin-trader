"""Collector protocol placeholders."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol

from newcoin_trader.domain.market import Kline, TradeTick
from newcoin_trader.domain.tokens import NewListingEvent, TokenRef


class MarketCollector(Protocol):
    async def discover_new_listings(self, *, since: datetime | None = None) -> Sequence[NewListingEvent]: ...

    async def fetch_klines(
        self,
        ref: TokenRef,
        *,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> Sequence[Kline]: ...

    async def fetch_trades(
        self,
        ref: TokenRef,
        *,
        start: datetime,
        end: datetime,
    ) -> Sequence[TradeTick]: ...
