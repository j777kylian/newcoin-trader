"""Frozen protocol constants for Base Uniswap V3 (in-memory boundary only)."""

from __future__ import annotations

CHAIN_ID: int = 8453
FACTORY_ADDRESS: str = "0x33128a8fc17869897dce68ed026d694621f6fdfd"
PROTOCOL_VERSION: str = "uniswap_v3_base_v1"
POOL_CREATED_TOPIC: str = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
SWAP_TOPIC: str = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

ZERO_ADDRESS: str = "0x0000000000000000000000000000000000000000"

__all__ = [
    "CHAIN_ID",
    "FACTORY_ADDRESS",
    "POOL_CREATED_TOPIC",
    "PROTOCOL_VERSION",
    "SWAP_TOPIC",
    "ZERO_ADDRESS",
]
