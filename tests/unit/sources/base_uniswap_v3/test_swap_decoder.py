"""Focused Swap decoder tests for Phase 8C.4."""

from __future__ import annotations

import pytest

from newcoin_trader.sources.base_uniswap_v3.contracts import SWAP_TOPIC
from newcoin_trader.sources.base_uniswap_v3.swap_decoder import decode_swap_log

POOL = "0x6c561b446416e2b78e1a75e721ae6e4e60bfa7ff"
SENDER = "0x1111111111111111111111111111111111111111"
RECIPIENT = "0x2222222222222222222222222222222222222222"


def _topic_address(addr: str) -> str:
    bare = addr.lower().removeprefix("0x")
    return "0x" + ("0" * 24) + bare


def _int_word(value: int) -> str:
    if value < 0:
        value = (1 << 256) + value
    return f"{value:064x}"


def _valid_swap_raw(**overrides: object) -> dict[str, object]:
    # amount0=-1000, amount1=2000, sqrtPriceX96=79228162514264337593543950336, liquidity=1000000, tick=10
    data = "0x" + "".join(
        [
            _int_word(-1000),
            _int_word(2000),
            _int_word(79228162514264337593543950336),
            _int_word(1_000_000),
            _int_word(10),
        ]
    )
    payload: dict[str, object] = {
        "address": POOL,
        "topics": [SWAP_TOPIC, _topic_address(SENDER), _topic_address(RECIPIENT)],
        "data": data,
        "blockNumber": "0x20",
        "blockHash": "0x" + ("ab" * 32),
        "transactionHash": "0x" + ("cd" * 32),
        "transactionIndex": "0x1",
        "logIndex": "0x2",
    }
    payload.update(overrides)
    return payload


def test_decode_valid_swap() -> None:
    record = decode_swap_log(_valid_swap_raw())
    assert record.pool_address == POOL
    assert record.sender == SENDER
    assert record.recipient == RECIPIENT
    assert record.amount0 == -1000
    assert record.amount1 == 2000
    assert record.sqrt_price_x96 == 79228162514264337593543950336
    assert record.liquidity == 1_000_000
    assert record.tick == 10
    assert record.block_number == 32
    assert record.transaction_index == 1
    assert record.log_index == 2


def test_reject_malformed_swap_data_word_count() -> None:
    bad = _valid_swap_raw(data="0x" + ("00" * 64))
    with pytest.raises(ValueError, match="malformed ABI data"):
        decode_swap_log(bad)


def test_reject_wrong_swap_topic() -> None:
    topics = ["0x" + ("11" * 32), _topic_address(SENDER), _topic_address(RECIPIENT)]
    with pytest.raises(ValueError, match="topic"):
        decode_swap_log(_valid_swap_raw(topics=topics))
