"""Offline Pump V2 canonical identity validation tests; injected account fixtures only."""

from __future__ import annotations

import hashlib
import json

import pytest

from newcoin_trader.sources.pumpfun.evidence import (
    PUMP_PROGRAM_ADDRESS,
    PumpRawTransactionEvidence,
    parse_pump_instruction,
    pinned_pump_decoder_evidence,
)
from newcoin_trader.sources.pumpfun.identity import (
    TOKEN_PROGRAM_ID,
    PumpCanonicalIdentityRecord,
    PumpIdentityDisposition,
    classify_pump_identity,
    write_pump_corpus_v2_identity_manifest,
)

MINT = "HQpnf8AaXt8yBnjhf2jk3t1AKAqkBn3pqADLBTaipump"
BONDING_CURVE = "Ea2LrGwcFK6iDz6rYjoDXSbFZ4X6UBspqiBoKZQBjpa"
ASSOCIATED = "38tzG2ag8FFGmzS7qFqBLj9FgBX85TPESMTXK4uPhQNS"
SIGNATURE = "3N5R8Zy6iDbeNqjR9JH4tt3FjhHVwLwWLCwT6mKrRj4D"


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _launch_accounts(*, mint: str, bonding_curve: str, associated: str) -> list[str]:
    # Exact create_v2 named-account order; essential roles bound, fillers unique.
    roles = {
        "mint": mint,
        "bonding_curve": bonding_curve,
        "associated_bonding_curve": associated,
        "token_program": TOKEN_PROGRAM_ID,
    }
    fillers = iter("1111111111111111111111111111111" + hex(i)[2:] for i in range(16, 32))
    return [
        roles["mint"],
        next(fillers),  # mint_authority
        roles["bonding_curve"],
        roles["associated_bonding_curve"],
        next(fillers),  # global
        next(fillers),  # user
        next(fillers),  # system_program
        roles["token_program"],
        next(fillers),  # associated_token_program
        next(fillers),  # mayhem_program_id
        next(fillers),  # global_params
        next(fillers),  # sol_vault
        next(fillers),  # mayhem_state
        next(fillers),  # mayhem_token_vault
        next(fillers),  # event_authority
        next(fillers),  # program
    ]


def _transaction(*, mint: str, bonding_curve: str, associated: str) -> PumpRawTransactionEvidence:
    return PumpRawTransactionEvidence.from_get_transaction(
        signature=SIGNATURE,
        commitment="finalized",
        response={
            "slot": 20,
            "meta": {"err": None, "innerInstructions": []},
            "transaction": {
                "signatures": [SIGNATURE],
                "message": {
                    "instructions": [
                        {
                            "programId": PUMP_PROGRAM_ADDRESS,
                            "data": "d6904cec5f8b31b4",
                            "accounts": _launch_accounts(mint=mint, bonding_curve=bonding_curve, associated=associated),
                        }
                    ]
                },
            },
        },
    )


def _mint_response(*, mint_authority: str) -> dict[str, object]:
    return {
        "value": {
            "owner": TOKEN_PROGRAM_ID,
            "data": {
                "parsed": {
                    "type": "mint",
                    "info": {
                        "isInitialized": True,
                        "mintAuthority": mint_authority,
                        "supply": "1000000000",
                        "decimals": 6,
                        "freezeAuthority": None,
                    },
                }
            },
        }
    }


def _bonding_curve_response() -> dict[str, object]:
    return {"value": {"owner": PUMP_PROGRAM_ADDRESS, "data": ["", "base64"]}}


def _associated_response(*, mint: str, owner: str) -> dict[str, object]:
    return {
        "value": {
            "owner": TOKEN_PROGRAM_ID,
            "data": {
                "parsed": {
                    "type": "account",
                    "info": {
                        "isNative": False,
                        "mint": mint,
                        "owner": owner,
                        "state": "initialized",
                        "tokenAmount": {"amount": "0", "decimals": 6},
                    },
                }
            },
        }
    }


def _classify(*, source_digest: str, mint_authority: str, associated_owner: str) -> PumpCanonicalIdentityRecord:
    transaction = _transaction(mint=MINT, bonding_curve=BONDING_CURVE, associated=ASSOCIATED)
    fact = parse_pump_instruction(transaction, pinned_pump_decoder_evidence(), instruction_index=0)
    return classify_pump_identity(
        source_coordinate_digest=source_digest,
        signature=SIGNATURE,
        slot=20,
        transaction_index=0,
        fact=fact,
        transaction=transaction,
        mint_info=_mint_response(mint_authority=mint_authority),
        bonding_curve_info=_bonding_curve_response(),
        associated_info=_associated_response(mint=MINT, owner=associated_owner),
        decoder=pinned_pump_decoder_evidence(),
    )


def test_verified_create_v2_identity_passes_independent_account_evidence() -> None:
    record = _classify(source_digest="a" * 64, mint_authority=BONDING_CURVE, associated_owner=BONDING_CURVE)
    assert record.disposition is PumpIdentityDisposition.VERIFIED_PUMP_CREATE
    assert record.mint == MINT
    assert record.bonding_curve == BONDING_CURVE
    assert record.mint_authority == BONDING_CURVE
    assert record.token_program == TOKEN_PROGRAM_ID


def test_wrong_mint_authority_rejects_instead_of_accepting_decoder_claim() -> None:
    record = _classify(source_digest="b" * 64, mint_authority=ASSOCIATED, associated_owner=BONDING_CURVE)
    assert record.disposition is PumpIdentityDisposition.INVALID_MINT_ROLE


def test_non_mint_account_rejected_as_mint_type() -> None:
    transaction = _transaction(mint=MINT, bonding_curve=BONDING_CURVE, associated=ASSOCIATED)
    fact = parse_pump_instruction(transaction, pinned_pump_decoder_evidence(), instruction_index=0)
    record = classify_pump_identity(
        source_coordinate_digest="c" * 64,
        signature=SIGNATURE,
        slot=20,
        transaction_index=0,
        fact=fact,
        transaction=transaction,
        mint_info={
            "value": {"owner": TOKEN_PROGRAM_ID, "data": {"parsed": {"type": "account", "info": {"mint": MINT}}}}
        },
        bonding_curve_info=_bonding_curve_response(),
        associated_info=_associated_response(mint=MINT, owner=BONDING_CURVE),
        decoder=pinned_pump_decoder_evidence(),
    )
    assert record.disposition is PumpIdentityDisposition.INVALID_MINT_ACCOUNT_TYPE


def test_wrong_associated_owner_rejected() -> None:
    record = _classify(source_digest="d" * 64, mint_authority=BONDING_CURVE, associated_owner=ASSOCIATED)
    assert record.disposition is PumpIdentityDisposition.INVALID_ASSOCIATED_BONDING_CURVE


def test_identity_manifest_create_only_and_digest_bound(tmp_path) -> None:
    record = _classify(source_digest="e" * 64, mint_authority=BONDING_CURVE, associated_owner=BONDING_CURVE)
    path = write_pump_corpus_v2_identity_manifest(tmp_path, source_manifest_digest="f" * 64, records=(record,))
    assert path.exists()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["version"] == "PumpCanonicalIdentityManifestV1"
    assert len(payload["records"]) == 1
    with pytest.raises(FileExistsError):
        write_pump_corpus_v2_identity_manifest(tmp_path, source_manifest_digest="f" * 64, records=(record,))
