"""Focused PoolCreated decoder tests for Phase 8C.4."""

from __future__ import annotations

import pytest

from newcoin_trader.sources.base_uniswap_v3.contracts import (
    CHAIN_ID,
    FACTORY_ADDRESS,
    POOL_CREATED_TOPIC,
    PROTOCOL_VERSION,
)
from newcoin_trader.sources.base_uniswap_v3.pool_created_decoder import decode_pool_created_log


def _topic_address(addr: str) -> str:
    bare = addr.lower().removeprefix("0x")
    return "0x" + ("0" * 24) + bare


def _fee_topic(fee: int) -> str:
    return "0x" + f"{fee:064x}"


def _int24_word(value: int) -> str:
    if value < 0:
        value = (1 << 256) + value
    return f"{value:064x}"


def _address_word(addr: str) -> str:
    bare = addr.lower().removeprefix("0x")
    return ("0" * 24) + bare


TOKEN0 = "0x4200000000000000000000000000000000000006"
TOKEN1 = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
POOL = "0x6c561b446416e2b78e1a75e721ae6e4e60bfa7ff"
FEE = 500
TICK_SPACING = 10


def _valid_raw_log(**overrides: object) -> dict[str, object]:
    data = "0x" + _int24_word(TICK_SPACING) + _address_word(POOL)
    payload: dict[str, object] = {
        "address": FACTORY_ADDRESS,
        "topics": [
            POOL_CREATED_TOPIC,
            _topic_address(TOKEN0),
            _topic_address(TOKEN1),
            _fee_topic(FEE),
        ],
        "data": data,
        "blockNumber": "0x10",
        "blockHash": "0x" + ("ab" * 32),
        "transactionHash": "0x" + ("cd" * 32),
        "transactionIndex": "0x0",
        "logIndex": "0x1",
    }
    payload.update(overrides)
    return payload


def test_frozen_protocol_constants() -> None:
    assert CHAIN_ID == 8453
    assert FACTORY_ADDRESS == "0x33128a8fc17869897dce68ed026d694621f6fdfd"
    assert PROTOCOL_VERSION == "uniswap_v3_base_v1"
    assert POOL_CREATED_TOPIC == "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"


def test_decode_valid_pool_created() -> None:
    record = decode_pool_created_log(_valid_raw_log())
    assert record.chain_id == CHAIN_ID
    assert record.protocol_version == PROTOCOL_VERSION
    assert record.factory_address == FACTORY_ADDRESS
    assert record.token0 == TOKEN0
    assert record.token1 == TOKEN1
    assert record.fee == FEE
    assert record.tick_spacing == TICK_SPACING
    assert record.pool_address == POOL
    assert record.block_number == 16
    assert record.transaction_index == 0
    assert record.log_index == 1


def test_reject_wrong_factory_address() -> None:
    with pytest.raises(ValueError, match="factory"):
        decode_pool_created_log(_valid_raw_log(address="0x1111111111111111111111111111111111111111"))


def test_reject_wrong_pool_created_topic() -> None:
    topics = [
        "0x" + ("11" * 32),
        _topic_address(TOKEN0),
        _topic_address(TOKEN1),
        _fee_topic(FEE),
    ]
    with pytest.raises(ValueError, match="topic"):
        decode_pool_created_log(_valid_raw_log(topics=topics))
