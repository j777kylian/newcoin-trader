"""Static safety: no wallet / live-trading code in sensitive packages."""

from __future__ import annotations

from pathlib import Path

FORBIDDEN = (
    "private_key",
    "mnemonic",
    "seed_phrase",
    "solders",
    "web3.eth.account",
    "keypair",
    "sign_transaction",
)


def test_execution_and_collectors_have_no_wallet_or_signing_code() -> None:
    root = Path(__file__).resolve().parents[2] / "src" / "newcoin_trader"
    for package in ("execution", "collectors", "strategies", "risk"):
        for path in (root / package).rglob("*.py"):
            text = path.read_text(encoding="utf-8").lower()
            for token in FORBIDDEN:
                assert token not in text, f"{token} found in {path}"
