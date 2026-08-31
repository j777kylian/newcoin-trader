"""Phase 8D v3 Pump raw-chain evidence regressions; no I/O."""

from __future__ import annotations

import hashlib
import json

import pytest
from pydantic import ValidationError

from newcoin_trader.sources.pumpfun.evidence import (
    PUMP_PROGRAM_ADDRESS,
    PumpCandidateDispositionKind,
    PumpDecoderEvidence,
    PumpMintSelectionProof,
    PumpPageRecordDisposition,
    PumpQualifiedSourceProfile,
    PumpRawBlockEvidence,
    PumpRawProgramDataEvidence,
    PumpRawSignaturePageEvidence,
    PumpRawTransactionEvidence,
    PumpSignatureCandidate,
    PumpSignaturePage,
    VerifiedPumpLaunchUniverse,
    parse_pump_instruction,
    select_first_successful_buy,
)

MINT = "So11111111111111111111111111111111111111112"
MARKET = "5Q544fKrFoe6tsEbCbWLZ8cM1iPV5QpZ2AJwFXB8i8VB"
UPPER = "4vJ9JU1bJJE96FWSJKvHsmmFQy9rato4sVZ2BJTaL7h"
BUY = "3N5R8Zy6iDbeNqjR9JH4tt3FjhHVwLwWLCwT6mKrRj4D"
CREATE = "2G4YbnQw5v5C5mR8h3D7sMjn6Qq5qP2e9u8Y4k6A1xT"
BLOCKHASH = "3u111111111111111111111111111111111111111111"
LOADER = "BPFLoaderUpgradeab1e11111111111111111111111"


def _program_accounts(
    *, program_owner: str = LOADER, programdata_owner: str = LOADER, pointer: str = MARKET
) -> tuple[dict[str, object], dict[str, object]]:
    return (
        {
            "context": {"slot": 1},
            "value": {
                "owner": program_owner,
                "data": {"parsed": {"type": "program", "info": {"programData": pointer}}},
            },
        },
        {
            "context": {"slot": 1},
            "value": {
                "owner": programdata_owner,
                "data": {"parsed": {"type": "programData", "info": {"slot": 1}}},
            },
        },
    )


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _decoder() -> PumpDecoderEvidence:
    idl = {
        "instructions": [
            {
                "name": kind,
                "discriminator": discriminator,
                "accounts": [{"name": "payer"}, {"name": "bondingCurve"}, {"name": "mint"}, {"name": "market"}],
            }
            for kind, discriminator in (
                ("create", "181ec828051c0777"),
                ("create_v2", "d6904cec5f8b31b4"),
                ("buy", "66063d1201daebea"),
            )
        ]
    }
    return PumpDecoderEvidence(
        schema_version="pumpfun-decoder-v1",
        program_address=PUMP_PROGRAM_ADDRESS,
        raw_idl_response=idl,
        idl_digest=_digest({"schema": "pumpfun-decoder-v1", "program": PUMP_PROGRAM_ADDRESS, "rawIdlResponse": idl}),
    )


def _profile() -> PumpQualifiedSourceProfile:
    program, programdata = _program_accounts()
    programdata = PumpRawProgramDataEvidence.from_program_accounts(
        program_address=PUMP_PROGRAM_ADDRESS,
        programdata_address=MARKET,
        program_response=program,
        programdata_response=programdata,
    )
    decoder = _decoder()
    return PumpQualifiedSourceProfile(programdata=programdata, decoder=decoder, idl_digest=decoder.idl_digest)


def _transaction(
    signature: str, slot: int, *, kind: str, inner: bool = False, mint: str = MINT
) -> PumpRawTransactionEvidence:
    instruction = {
        "programId": PUMP_PROGRAM_ADDRESS,
        "data": {"create": "181ec828051c0777", "buy": "66063d1201daebea"}[kind],
        "accounts": [UPPER, BUY, mint, MARKET],
    }
    return PumpRawTransactionEvidence.from_get_transaction(
        signature=signature,
        commitment="finalized",
        response={
            "slot": slot,
            "meta": {"err": None, "innerInstructions": [{"index": 0, "instructions": [instruction]}] if inner else []},
            "transaction": {
                "signatures": [signature],
                "message": {
                    "instructions": [{"programId": "other", "data": "", "accounts": []}] if inner else [instruction]
                },
            },
        },
    )


def _block(slot: int, *signatures: str) -> PumpRawBlockEvidence:
    return PumpRawBlockEvidence.from_get_block(
        slot=slot,
        commitment="finalized",
        response={
            "blockhash": BLOCKHASH,
            "previousBlockhash": None,
            "blockHeight": slot,
            "blockTime": 1_754_006_400 + slot,
            "transactions": [{"transaction": {"signatures": [signature]}} for signature in signatures],
        },
    )


def _candidate(
    signature: str, slot: int, index: int, *, kind: str, inner: bool = False, mint: str = MINT
) -> PumpSignatureCandidate:
    transaction = _transaction(signature, slot, kind=kind, inner=inner, mint=mint)
    fact = parse_pump_instruction(
        transaction, _decoder(), instruction_index=0, inner_instruction_index=0 if inner else None
    )
    return PumpSignatureCandidate(
        signature=signature,
        slot=slot,
        transaction_index=index,
        source_time=1_754_006_400 + slot,
        raw_transaction=transaction,
        raw_block=_block(slot, *([UPPER] * index), signature),
        decoder=_decoder(),
        instruction_facts=(fact,),
    )


def _page(*candidates: PumpSignatureCandidate, before: str | None = None) -> PumpSignaturePage:
    records = [
        {
            "signature": candidate.signature,
            "slot": candidate.slot,
            "err": candidate.raw_transaction.meta_err,
            "blockTime": candidate.source_time,
        }
        for candidate in candidates
    ]
    return PumpSignaturePage(
        raw_page=PumpRawSignaturePageEvidence.from_get_signatures_for_address(
            before=before, limit=10, response=records
        ),
        classifications=tuple(
            PumpPageRecordDisposition(
                signature=candidate.signature,
                slot=candidate.slot,
                disposition=PumpCandidateDispositionKind.ELIGIBLE_DECODED,
                candidate=candidate,
            )
            for candidate in candidates
        ),
    )


def _proof(*, inner_buy: bool = False) -> PumpMintSelectionProof:
    create = _candidate(CREATE, 20, 0, kind="create")
    buy = _candidate(BUY, 20 if inner_buy else 21, 0, kind="buy", inner=inner_buy)
    upper = _candidate(UPPER, 30, 0, kind="buy", mint=UPPER)
    page = _page(upper, buy, create)
    universe = VerifiedPumpLaunchUniverse(
        profile=_profile(),
        pages=(page,),
        lower_signature=CREATE,
        upper_signature=UPPER,
        terminal_reason="lower_bound_reached",
        terminal_cursor=CREATE,
        expected_candidate_count=3,
        page_digests=(page.raw_page.raw_payload_digest,),
    )
    return PumpMintSelectionProof(universe=universe, mint=MINT)


def test_raw_transaction_rejects_response_primary_signature_mismatch() -> None:
    with pytest.raises(ValueError, match="primary signature"):
        PumpRawTransactionEvidence.from_get_transaction(
            signature=BUY,
            commitment="finalized",
            response={
                "slot": 1,
                "meta": {"err": None},
                "transaction": {"signatures": [UPPER], "message": {"instructions": []}},
            },
        )


def test_candidate_rejects_response_signature_different_from_candidate() -> None:
    candidate = _candidate(BUY, 20, 0, kind="buy")
    transaction = _transaction(UPPER, 20, kind="buy")
    with pytest.raises(ValidationError, match="transaction signature"):
        candidate.model_copy(
            update={
                "raw_transaction": transaction,
                "instruction_facts": (parse_pump_instruction(transaction, _decoder(), instruction_index=0),),
            }
        )


def test_profile_rejects_caller_asserted_digest_without_raw_programdata_and_decoder_evidence() -> None:
    with pytest.raises(ValidationError, match="required"):
        PumpQualifiedSourceProfile(idl_digest="a" * 64)


def test_profile_rejects_fake_programdata_or_idl_content() -> None:
    profile = _profile()
    with pytest.raises(ValidationError, match="ProgramData|digest|program_response"):
        profile.model_copy(
            update={"programdata": profile.programdata.model_copy(update={"program_response": {"fake": True}})}
        )
    with pytest.raises(ValidationError, match="pinned canonical IDL"):
        profile.model_copy(update={"decoder": profile.decoder.model_copy(update={"raw_idl_response": {"fake": True}})})


def test_parser_accepts_anchor_discriminator_with_instruction_arguments() -> None:
    transaction = PumpRawTransactionEvidence.from_get_transaction(
        signature=BUY,
        commitment="finalized",
        response={
            "slot": 20,
            "meta": {"err": None},
            "transaction": {
                "signatures": [BUY],
                "message": {
                    "instructions": [
                        {
                            "programId": PUMP_PROGRAM_ADDRESS,
                            "data": "TQC5U4sttDH1Zpfx",
                            "accounts": [UPPER, BUY, MINT, MARKET],
                        }
                    ]
                },
            },
        },
    )
    fact = parse_pump_instruction(transaction, _decoder(), instruction_index=0)
    assert (fact.instruction_kind, fact.mint) == ("create", MINT)


def test_parser_rejects_arbitrary_two_account_instruction() -> None:
    transaction = PumpRawTransactionEvidence.from_get_transaction(
        signature=BUY,
        commitment="finalized",
        response={
            "slot": 20,
            "meta": {"err": None},
            "transaction": {
                "signatures": [BUY],
                "message": {
                    "instructions": [
                        {"programId": PUMP_PROGRAM_ADDRESS, "data": "66063d1201daebea", "accounts": [MINT, MARKET]}
                    ]
                },
            },
        },
    )
    with pytest.raises(ValueError, match="account layout"):
        parse_pump_instruction(transaction, _decoder(), instruction_index=0)


def test_parser_requires_decoder_bound_raw_role_mapping() -> None:
    decoder = _decoder()
    with pytest.raises(ValidationError, match="IDL"):
        decoder.model_copy(
            update={
                "raw_idl_response": {},
                "idl_digest": _digest(
                    {"schema": decoder.schema_version, "program": decoder.program_address, "rawIdlResponse": {}}
                ),
            }
        )


def test_decoder_rejects_changed_idl_with_attacker_recomputed_digest() -> None:
    decoder = _decoder()
    changed_idl = {**decoder.raw_idl_response, "untrusted": True}
    with pytest.raises(ValidationError, match="pinned canonical IDL"):
        PumpDecoderEvidence(
            schema_version=decoder.schema_version,
            program_address=decoder.program_address,
            raw_idl_response=changed_idl,
            idl_digest=_digest(
                {
                    "schema": decoder.schema_version,
                    "program": decoder.program_address,
                    "rawIdlResponse": changed_idl,
                }
            ),
        )


def test_signature_page_canonicalizes_provider_confirmation_status() -> None:
    page = PumpRawSignaturePageEvidence.from_get_signatures_for_address(
        before=None,
        limit=1,
        response=[
            {
                "signature": BUY,
                "slot": 20,
                "err": None,
                "blockTime": 1,
                "confirmationStatus": "finalized",
            }
        ],
    )
    assert page.records == ({"signature": BUY, "slot": 20, "err": None, "blockTime": 1},)


@pytest.mark.parametrize(
    "record",
    (
        {"signature": BUY, "slot": 20, "err": "not-an-rpc-error-object", "blockTime": 1},
        {"signature": BUY, "slot": 20, "err": [], "blockTime": 1},
        {"signature": BUY, "slot": 20, "err": None, "blockTime": -1},
    ),
)
def test_signature_page_rejects_malformed_canonical_fields(record: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        PumpRawSignaturePageEvidence.from_get_signatures_for_address(before=None, limit=1, response=[record])


def test_signature_page_rejects_mutated_raw_records() -> None:
    page = _page(_candidate(BUY, 20, 0, kind="buy"))
    with pytest.raises(ValidationError, match="digest"):
        page.raw_page.model_copy(update={"records": ({"signature": UPPER, "slot": 20, "err": None, "blockTime": 1},)})


def test_signature_page_rejects_candidate_not_equal_to_raw_record() -> None:
    candidate = _candidate(BUY, 20, 0, kind="buy")
    raw = PumpRawSignaturePageEvidence.from_get_signatures_for_address(
        before=None,
        limit=10,
        response=[{"signature": UPPER, "slot": 20, "err": None, "blockTime": candidate.source_time}],
    )
    with pytest.raises(ValidationError, match="exactly bind"):
        PumpSignaturePage(
            raw_page=raw,
            classifications=(
                PumpPageRecordDisposition(
                    signature=BUY,
                    slot=20,
                    disposition=PumpCandidateDispositionKind.ELIGIBLE_DECODED,
                    candidate=candidate,
                ),
            ),
        )


def test_signature_page_rejects_raw_candidate_omission() -> None:
    first, second = _candidate(BUY, 20, 0, kind="buy"), _candidate(CREATE, 10, 0, kind="create")
    raw = PumpRawSignaturePageEvidence.from_get_signatures_for_address(
        before=None,
        limit=10,
        response=[
            {"signature": first.signature, "slot": first.slot, "err": None, "blockTime": first.source_time},
            {"signature": second.signature, "slot": second.slot, "err": None, "blockTime": second.source_time},
        ],
    )
    with pytest.raises(ValidationError, match="exactly bind"):
        PumpSignaturePage(
            raw_page=raw,
            classifications=(
                PumpPageRecordDisposition(
                    signature=first.signature,
                    slot=first.slot,
                    disposition=PumpCandidateDispositionKind.ELIGIBLE_DECODED,
                    candidate=first,
                ),
            ),
        )


def test_universe_rejects_incomplete_terminal_boundary_or_page_reconciliation() -> None:
    proof = _proof()
    universe = proof.universe
    with pytest.raises(ValidationError, match="terminal"):
        universe.model_copy(update={"terminal_cursor": BUY})
    with pytest.raises(ValidationError, match="reconciliation"):
        universe.model_copy(update={"expected_candidate_count": 2})
    with pytest.raises(ValidationError, match="reconciliation"):
        universe.model_copy(update={"page_digests": ()})


def test_universe_requires_previous_raw_terminal_cursor() -> None:
    upper, buy, create = (
        _candidate(UPPER, 30, 0, kind="buy"),
        _candidate(BUY, 20, 0, kind="buy"),
        _candidate(CREATE, 10, 0, kind="create"),
    )
    first = _page(upper, buy)
    second = _page(create, before=UPPER)
    with pytest.raises(ValidationError, match="continuation"):
        VerifiedPumpLaunchUniverse(
            profile=_profile(),
            pages=(first, second),
            lower_signature=CREATE,
            upper_signature=UPPER,
            terminal_reason="lower_bound_reached",
            terminal_cursor=CREATE,
            expected_candidate_count=3,
            page_digests=(first.raw_page.raw_payload_digest, second.raw_page.raw_payload_digest),
        )


def test_t0_orders_outer_before_inner_and_retains_inner_coordinate() -> None:
    fact = select_first_successful_buy(_proof(inner_buy=True))
    assert (fact.signature, fact.instruction_index, fact.inner_instruction_index) == (BUY, 0, 0)


def test_source_only_fact_has_no_phase3_projection() -> None:
    fact = select_first_successful_buy(_proof())
    assert not hasattr(fact, "to_phase3")
    assert fact.signature == BUY


def test_programdata_builder_rejects_arbitrary_program_or_programdata_owner() -> None:
    for program_owner, programdata_owner in ((PUMP_PROGRAM_ADDRESS, LOADER), (LOADER, PUMP_PROGRAM_ADDRESS)):
        program, programdata = _program_accounts(program_owner=program_owner, programdata_owner=programdata_owner)
        with pytest.raises(ValidationError, match="owner"):
            PumpRawProgramDataEvidence.from_program_accounts(
                program_address=PUMP_PROGRAM_ADDRESS,
                programdata_address=MARKET,
                program_response=program,
                programdata_response=programdata,
            )


def test_programdata_builder_rejects_programdata_pointer_not_equal_to_queried_account() -> None:
    program, programdata = _program_accounts()
    with pytest.raises(ValidationError, match="pointer"):
        PumpRawProgramDataEvidence.from_program_accounts(
            program_address=PUMP_PROGRAM_ADDRESS,
            programdata_address=UPPER,
            program_response=program,
            programdata_response=programdata,
        )


def test_programdata_builder_derives_programdata_metadata_from_raw_accounts() -> None:
    program, programdata = _program_accounts()
    evidence = PumpRawProgramDataEvidence.from_program_accounts(
        program_address=PUMP_PROGRAM_ADDRESS,
        programdata_address=MARKET,
        program_response=program,
        programdata_response=programdata,
    )
    assert evidence.programdata_address == MARKET
    assert evidence.last_deploy_slot == 1


def test_decoder_rejects_digest_consistent_roles_not_extracted_from_raw_idl() -> None:
    decoder = _decoder()
    roles = {kind: {"mint": 0, "market": 1} for kind in ("create", "create_v2", "buy")}
    with pytest.raises(ValidationError, match="role_mapping"):
        decoder.model_copy(update={"role_mapping": roles})


def test_decoder_rejects_ambiguous_duplicate_canonical_idl_discriminator() -> None:
    decoder = _decoder()
    raw = {
        **decoder.raw_idl_response,
        "instructions": [*decoder.raw_idl_response["instructions"], decoder.raw_idl_response["instructions"][0]],
    }
    with pytest.raises(ValidationError, match="canonical IDL"):
        PumpDecoderEvidence(
            schema_version=decoder.schema_version,
            program_address=decoder.program_address,
            raw_idl_response=raw,
            idl_digest=_digest(
                {"schema": decoder.schema_version, "program": decoder.program_address, "rawIdlResponse": raw}
            ),
        )


def test_signature_page_classifies_failed_raw_record_without_decoded_candidate() -> None:
    raw = PumpRawSignaturePageEvidence.from_get_signatures_for_address(
        before=None,
        limit=10,
        response=[{"signature": BUY, "slot": 20, "err": {"InstructionError": [0, "Custom"]}, "blockTime": None}],
    )
    page = PumpSignaturePage(
        raw_page=raw,
        classifications=(PumpPageRecordDisposition(signature=BUY, slot=20, disposition="failed_transaction"),),
    )
    assert page.candidates == ()


def test_universe_rejects_exhausted_exact_limit_page_without_empty_terminal_proof() -> None:
    candidate = _candidate(BUY, 20, 0, kind="buy")
    exact = PumpSignaturePage(
        raw_page=PumpRawSignaturePageEvidence.from_get_signatures_for_address(
            before=None,
            limit=1,
            response=[{"signature": BUY, "slot": 20, "err": None, "blockTime": candidate.source_time}],
        ),
        classifications=(
            PumpPageRecordDisposition(
                signature=BUY, slot=20, disposition=PumpCandidateDispositionKind.ELIGIBLE_DECODED, candidate=candidate
            ),
        ),
    )
    with pytest.raises(ValidationError, match="exhausted"):
        VerifiedPumpLaunchUniverse(
            profile=_profile(),
            pages=(exact,),
            lower_signature=BUY,
            upper_signature=BUY,
            terminal_reason="exhausted",
            terminal_cursor=BUY,
            expected_candidate_count=1,
            page_digests=(exact.raw_page.raw_payload_digest,),
        )


def test_parser_rejects_duplicate_raw_inner_parent_coordinate() -> None:
    transaction = _transaction(BUY, 20, kind="buy", inner=True)
    instruction = transaction.inner_instructions[0]["instructions"][0]
    forged = PumpRawTransactionEvidence.from_get_transaction(
        signature=BUY,
        commitment="finalized",
        response={
            "slot": 20,
            "meta": {
                "err": None,
                "innerInstructions": [
                    {"index": 0, "instructions": [instruction]},
                    {"index": 0, "instructions": [instruction]},
                ],
            },
            "transaction": {
                "signatures": [BUY],
                "message": {"instructions": [{"programId": "other", "data": "", "accounts": []}]},
            },
        },
    )
    with pytest.raises(ValueError, match="duplicate"):
        parse_pump_instruction(forged, _decoder(), instruction_index=0, inner_instruction_index=0)


def test_source_only_fact_rejects_object_setattr_clock_injection_at_public_boundary() -> None:
    fact = select_first_successful_buy(_proof())
    object.__setattr__(fact, "received_time", "fabricated")
    with pytest.raises(ValueError, match="forbids"):
        fact.model_dump()
