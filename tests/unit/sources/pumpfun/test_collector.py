"""Trust-boundary tests for the bounded Pump curve trade collector."""

from __future__ import annotations

import asyncio
import json

from newcoin_trader.sources.pumpfun.collector import PumpCurveTradeCollector
from newcoin_trader.sources.pumpfun.rpc import PumpRpcProvider

MINT = "So11111111111111111111111111111111111111112"
CURVE = "5Q544fKrFoe6tsEbCbWLZ8cM1iPV5QpZ2AJwFXB8i8VB"
PAYER = "4Nd1mBQtrMJVYVfKf2PJy9NZUZdTAsp7D4xWLs4gDB4T"
SIG_BUY = "3N5R8Zy6iDbeNqjR9JH4tt3FjhHVwLwWLCwT6mKrRj4D"
SIG_SELL = "2G4YbnQw5v5C5mR8h3D7sMjn6Qq5qP2e9u8Y4k6A1xT"

_BUY_DATA = "66063d1201daebea40420f000000000000ca9a3b00000000"
_SELL_DATA = "33e685a4017f83ad48537e02070000000000000000000000"  # token 30,106,604,360


def _trade_event_payload(sol: int, token: int, is_buy: bool) -> str:
    raw = bytearray(105)
    raw[0:8] = bytes.fromhex("e445a52e51cb9a1d")
    raw[48:56] = sol.to_bytes(8, "little")
    raw[56:64] = token.to_bytes(8, "little")
    raw[64] = 1 if is_buy else 0
    raw[97:105] = (1_754_006_400).to_bytes(8, "little", signed=True)
    return bytes(raw).hex()


def _tx(signature: str, side_data: str, event: str) -> dict[str, object]:
    return {
        "slot": 99,
        "meta": {
            "err": None,
            "innerInstructions": [
                {
                    "index": 0,
                    "instructions": [{"programId": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", "data": event}],
                }
            ],
        },
        "transaction": {
            "signatures": [signature],
            "message": {
                "instructions": [
                    {
                        "programId": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
                        "data": side_data,
                        "accounts": [PAYER, CURVE, MINT, CURVE],
                    }
                ]
            },
        },
    }


class _FakeTransport:
    def __init__(self, responses: list[object]) -> None:
        self._responses = responses
        self.calls = 0

    async def post_json(self, endpoint: str, payload: dict[str, object], timeout_seconds: float) -> object:
        response = self._responses[self.calls]
        self.calls += 1
        return response


def _rpc_response(request_id: int, result: object) -> dict[str, object]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def test_collector_binds_trade_event_sol_and_instruction_roles() -> None:
    transport = _FakeTransport(
        [
            _rpc_response(
                1, [{"signature": SIG_SELL, "slot": 99, "err": None}, {"signature": SIG_BUY, "slot": 100, "err": None}]
            ),
            _rpc_response(2, _tx(SIG_SELL, _SELL_DATA, _trade_event_payload(1_064_662, 30_106_604_360, False))),
            _rpc_response(3, _tx(SIG_BUY, _BUY_DATA, _trade_event_payload(9_442_556, 59_495_627_565_763, True))),
        ]
    )
    provider = PumpRpcProvider("https://rpc.example.invalid", transport=transport)
    collector = PumpCurveTradeCollector(provider, signature_page_limit=10, max_signatures=10)
    trades = asyncio.run(collector.collect(CURVE))
    assert len(trades) == 2
    sell, buy = trades
    assert sell["side"] == "sell" and buy["side"] == "buy"
    assert sell["trade_event"]["sol_lamports"] == 1_064_662
    assert buy["trade_event"]["sol_lamports"] == 9_442_556
    assert sell["instruction_token_amount"] == 30_106_604_360 == sell["trade_event"]["token_amount"]
    assert sell["mint"] == MINT and sell["bonding_curve"] == CURVE
    assert json.dumps(trades)


def test_collector_skips_failed_transactions() -> None:
    transport = _FakeTransport(
        [
            _rpc_response(1, [{"signature": SIG_BUY, "slot": 99, "err": {"InstructionError": [0, "Custom"]}}]),
        ]
    )
    provider = PumpRpcProvider("https://rpc.example.invalid", transport=transport)
    collector = PumpCurveTradeCollector(provider, signature_page_limit=10, max_signatures=10)
    trades = asyncio.run(collector.collect(CURVE))
    assert trades == []
    assert transport.calls == 1
