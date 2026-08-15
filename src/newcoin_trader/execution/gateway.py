"""Sole execution entrypoint. Live/non-paper raises before any broker call."""

from __future__ import annotations

from typing import Protocol

from newcoin_trader.domain.enums import ExecMode
from newcoin_trader.domain.execution import PaperFill, PaperOrder, RejectedOrder
from newcoin_trader.domain.market import PriceSnapshot
from newcoin_trader.execution.safety import ensure_paper_mode


class PaperBrokerProtocol(Protocol):
    def fill(
        self,
        order: PaperOrder,
        *,
        market: PriceSnapshot | None = None,
    ) -> PaperFill | RejectedOrder: ...


class ExecutionGateway:
    def __init__(self, broker: PaperBrokerProtocol) -> None:
        self._broker = broker

    def submit(
        self,
        order: PaperOrder,
        *,
        mode: ExecMode | str,
        market: PriceSnapshot | None = None,
    ) -> PaperFill | RejectedOrder:
        ensure_paper_mode(mode)
        return self._broker.fill(order, market=market)
