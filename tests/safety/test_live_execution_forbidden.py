"""Fail-closed live execution: raise before any broker or network call."""

from __future__ import annotations

from typing import Any

import pytest

from newcoin_trader.domain.enums import ExecMode, Side
from newcoin_trader.domain.execution import PaperOrder
from newcoin_trader.errors import LiveExecutionForbiddenError
from newcoin_trader.execution.gateway import ExecutionGateway


class CountingBroker:
    def __init__(self) -> None:
        self.calls = 0

    def fill(self, order: PaperOrder, **kwargs: Any) -> None:
        self.calls += 1
        raise AssertionError("broker must not be called for live/non-paper modes")


def _order() -> PaperOrder:
    return PaperOrder(
        token_address="So11111111111111111111111111111111111111112",
        chain="solana",
        side=Side.BUY,
        requested_qty="1",
        limit_price="1.00",
        signal_ts="2024-01-01T00:00:00+00:00",
    )


def test_live_mode_raises_before_broker_is_called() -> None:
    broker = CountingBroker()
    gateway = ExecutionGateway(broker=broker)

    with pytest.raises(LiveExecutionForbiddenError):
        gateway.submit(_order(), mode=ExecMode.LIVE)

    assert broker.calls == 0


def test_non_paper_string_mode_raises_before_broker_is_called() -> None:
    broker = CountingBroker()
    gateway = ExecutionGateway(broker=broker)

    with pytest.raises(LiveExecutionForbiddenError):
        gateway.submit(_order(), mode="live")  # type: ignore[arg-type]

    assert broker.calls == 0
