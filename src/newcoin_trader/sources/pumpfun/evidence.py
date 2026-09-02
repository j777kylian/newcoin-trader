"""Phase 8D v3 Pump bounded raw-chain evidence; pure models, no I/O."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, ClassVar, Self, cast

from pydantic import BaseModel, ConfigDict, model_validator

_BASE58 = frozenset("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
PUMP_PROGRAM_ADDRESS = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
_UPGRADEABLE_LOADER = "BPFLoaderUpgradeab1e11111111111111111111111"
_SOURCE_VERSION = "pumpfun-programdata-v0.1.0"
_COMPLETENESS = "RESEARCH_GRADE_BOUNDED_COMPLETENESS_V1"
_DISCRIMINATORS = {
    "create": "181ec828051c0777",
    "create_v2": "d6904cec5f8b31b4",
    "buy": "66063d1201daebea",
    "sell": "33e685a4017f83ad",
}
# Static supported decoder identity; live qualification must separately bind it to raw ProgramData/IDL evidence.
_SUPPORTED_DECODER_IDL_JSON = (
    '{"instructions":[{"name":"create","discriminator":"181ec828051c0777","accounts":['
    '{"name":"mint"},{"name":"mint_authority"},{"name":"bonding_curve"},{"name":"associated_bonding_curve"},'
    '{"name":"global"},{"name":"mpl_token_metadata"},{"name":"metadata"},{"name":"user"},{"name":"system_program"},'
    '{"name":"token_program"},{"name":"associated_token_program"},{"name":"rent"},'
    '{"name":"event_authority"},{"name":"program"}]},{"name":"create_v2","discriminator":"d6904cec5f8b31b4",'
    '"accounts":[{"name":"mint"},{"name":"mint_authority"},{"name":"bonding_curve"},'
    '{"name":"associated_bonding_curve"},{"name":"global"},{"name":"user"},{"name":"system_program"},'
    '{"name":"token_program"},{"name":"associated_token_program"},{"name":"mayhem_program_id"},'
    '{"name":"global_params"},{"name":"sol_vault"},{"name":"mayhem_state"},{"name":"mayhem_token_vault"},'
    '{"name":"event_authority"},{"name":"program"}]},{"name":"buy","discriminator":"66063d1201daebea",'
    '"accounts":[{"name":"payer"},{"name":"bondingCurve"},{"name":"mint"},{"name":"market"}]},'
    '{"name":"sell","discriminator":"33e685a4017f83ad",'
    '"accounts":[{"name":"payer"},{"name":"bondingCurve"},{"name":"mint"},{"name":"market"}]}]}'
)
_SUPPORTED_DECODER_DIGEST = "911684ba3a56cc0c222f1682e346646866d3c4746f091d7f6c9ffa32e5631bf1"


def _canonical(value: object) -> Any:
    try:
        return json.loads(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False))
    except (TypeError, ValueError) as error:
        raise ValueError("raw provider content must be canonical JSON") from error


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(_canonical(value), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _signature(value: object, *, field: str = "signature") -> str:
    if not isinstance(value, str) or not 32 <= len(value) <= 88 or any(char not in _BASE58 for char in value):
        raise ValueError(f"{field} must be base58")
    return value


def _index(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _instruction_data_hex(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    if len(value) % 2 == 0 and all(character in "0123456789abcdef" for character in value):
        return value
    if any(character not in _BASE58 for character in value):
        return None
    number = 0
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    for character in value:
        number = number * 58 + alphabet.index(character)
    raw = b"" if number == 0 else number.to_bytes((number.bit_length() + 7) // 8, "big")
    return (b"\0" * (len(value) - len(value.lstrip("1"))) + raw).hex()


def _utc_time(value: object, *, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


class _RevalidatingModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        return type(self).model_validate(
            self.model_dump(mode="python") if update is None else {**self.model_dump(mode="python"), **update}
        )


class PumpRawProgramDataEvidence(_RevalidatingModel):
    """Program/ProgramData facts derived only from explicit raw account responses."""

    program_address: str
    programdata_address: str
    program_response: dict[str, Any]
    programdata_response: dict[str, Any]
    raw_account_digest: str

    @classmethod
    def from_program_accounts(
        cls,
        *,
        program_address: str,
        programdata_address: str,
        program_response: Mapping[str, object],
        programdata_response: Mapping[str, object],
    ) -> Self:
        return cls(
            program_address=program_address,
            programdata_address=programdata_address,
            program_response=dict(program_response),
            programdata_response=dict(programdata_response),
            raw_account_digest=_digest(
                {
                    "program": program_address,
                    "programData": programdata_address,
                    "programResponse": program_response,
                    "programdataResponse": programdata_response,
                }
            ),
        )

    def _metadata(self) -> tuple[str, int, int, int]:
        try:
            program = self.program_response["value"]
            programdata = self.programdata_response["value"]
            program_owner = program["owner"]
            programdata_owner = programdata["owner"]
            pointer = program["data"]["parsed"]
            programdata_state = programdata["data"]["parsed"]
            address = pointer["info"]["programData"]
            slot = programdata_state["info"]["slot"]
            program_context_slot = self.program_response["context"]["slot"]
            programdata_context_slot = self.programdata_response["context"]["slot"]
        except (KeyError, TypeError) as error:
            raise ValueError("raw Program/ProgramData account responses are incomplete") from error
        if program_owner != _UPGRADEABLE_LOADER or programdata_owner != _UPGRADEABLE_LOADER:
            raise ValueError("Program and ProgramData account owners must be the BPF Upgradeable Loader")
        if pointer["type"] != "program" or programdata_state["type"] != "programData":
            raise ValueError("raw account states must be Program and ProgramData")
        return (
            _signature(address, field="ProgramData pointer"),
            _index(slot, field="ProgramData deploy slot"),
            _index(program_context_slot, field="Program account context slot"),
            _index(programdata_context_slot, field="ProgramData account context slot"),
        )

    @property
    def owner(self) -> str:
        self._metadata()
        return _UPGRADEABLE_LOADER

    @property
    def last_deploy_slot(self) -> int:
        return self._metadata()[1]

    @property
    def context_slots(self) -> tuple[int, int]:
        metadata = self._metadata()
        return metadata[2], metadata[3]

    @model_validator(mode="after")
    def _bound(self) -> Self:
        if self.program_address != PUMP_PROGRAM_ADDRESS:
            raise ValueError("ProgramData program is invalid")
        pointer, *_ = self._metadata()
        if self.programdata_address != pointer:
            raise ValueError("ProgramData pointer must equal queried ProgramData account")
        if self.raw_account_digest != _digest(
            {
                "program": self.program_address,
                "programData": self.programdata_address,
                "programResponse": self.program_response,
                "programdataResponse": self.programdata_response,
            }
        ):
            raise ValueError("ProgramData raw account digest does not bind content")
        return self


class PumpDecoderEvidence(_RevalidatingModel):
    """Decoder roles extracted from the retained raw canonical IDL response."""

    schema_version: str
    program_address: str
    raw_idl_response: dict[str, Any]
    idl_digest: str

    @property
    def idl_content(self) -> dict[str, Any]:
        return self.raw_idl_response

    @property
    def role_mapping(self) -> dict[str, dict[str, int]]:
        try:
            instructions = self.raw_idl_response["instructions"]
            selected = tuple(
                instruction
                for instruction in instructions
                if instruction["name"] in _DISCRIMINATORS
                and instruction["discriminator"] == _DISCRIMINATORS[instruction["name"]]
            )
            if len(selected) != len(_DISCRIMINATORS) or {instruction["name"] for instruction in selected} != set(
                _DISCRIMINATORS
            ):
                raise ValueError("raw canonical IDL has ambiguous required discriminators")
            required_roles = {
                "create": ("mint", "bonding_curve", "associated_bonding_curve"),
                "create_v2": ("mint", "bonding_curve", "associated_bonding_curve"),
                "buy": ("mint", "market"),
                "sell": ("mint", "market"),
            }
            mappings = {}
            for instruction in selected:
                kind = instruction["name"]
                accounts = instruction["accounts"]
                names = [account["name"] for account in accounts]
                if len(names) != len(set(names)) or any(role not in names for role in required_roles[kind]):
                    raise ValueError("raw canonical IDL lacks unique essential decoder roles")
                mappings[kind] = {role: names.index(role) for role in required_roles[kind]}
        except (KeyError, StopIteration, TypeError) as error:
            raise ValueError("raw canonical IDL lacks decoder roles") from error
        if set(mappings) != set(_DISCRIMINATORS):
            raise ValueError("raw canonical IDL lacks required discriminators")
        return mappings

    @model_validator(mode="after")
    def _bound(self) -> Self:
        if self.schema_version != "pumpfun-decoder-v1" or self.program_address != PUMP_PROGRAM_ADDRESS:
            raise ValueError("Pump decoder scope is invalid")
        if (
            self.raw_idl_response != json.loads(_SUPPORTED_DECODER_IDL_JSON)
            or self.idl_digest != _SUPPORTED_DECODER_DIGEST
        ):
            raise ValueError("decoder must bind the pinned canonical IDL identity")
        for kind, mapping in self.role_mapping.items():
            expected = (
                {"mint", "bonding_curve", "associated_bonding_curve"}
                if kind in {"create", "create_v2"}
                else {"mint", "market"}
            )
            if set(mapping) != expected:
                raise ValueError("canonical IDL requires named essential role mapping")
            positions = tuple(_index(position, field=f"{kind} role position") for position in mapping.values())
            if len(positions) != len(set(positions)):
                raise ValueError("decoder essential role positions must be distinct")
        return self


def pinned_pump_decoder_evidence() -> PumpDecoderEvidence:
    """Return the fixed local decoder identity; live account binding remains separate."""
    return PumpDecoderEvidence(
        schema_version="pumpfun-decoder-v1",
        program_address=PUMP_PROGRAM_ADDRESS,
        raw_idl_response=json.loads(_SUPPORTED_DECODER_IDL_JSON),
        idl_digest=_SUPPORTED_DECODER_DIGEST,
    )


class PumpQualifiedSourceProfile(_RevalidatingModel):
    profile_id: str = "pumpfun-raw-chain-v3"
    source: str = "pumpfun"
    source_version: str = _SOURCE_VERSION
    chain: str = "solana"
    finality_commitment: str = "finalized"
    program_address: str = PUMP_PROGRAM_ADDRESS
    programdata: PumpRawProgramDataEvidence
    decoder: PumpDecoderEvidence
    idl_digest: str

    @model_validator(mode="after")
    def _fixed_scope(self) -> Self:
        if (
            self.profile_id,
            self.source,
            self.source_version,
            self.chain,
            self.finality_commitment,
            self.program_address,
        ) != ("pumpfun-raw-chain-v3", "pumpfun", _SOURCE_VERSION, "solana", "finalized", PUMP_PROGRAM_ADDRESS):
            raise ValueError("Pump profile must bind v3 ProgramData/version/finalized scope")
        programdata = PumpRawProgramDataEvidence.model_validate(self.programdata.model_dump(mode="python"))
        decoder = PumpDecoderEvidence.model_validate(self.decoder.model_dump(mode="python"))
        if (
            programdata.program_address != self.program_address
            or decoder.program_address != self.program_address
            or self.idl_digest != decoder.idl_digest
        ):
            raise ValueError("profile must bind ProgramData and canonical IDL evidence")
        return self


class PumpRawTransactionEvidence(_RevalidatingModel):
    signature: str
    transaction_signatures: tuple[str, ...]
    slot: int
    commitment: str
    meta_err: object | None
    message: dict[str, Any]
    inner_instructions: tuple[dict[str, Any], ...] = ()
    balances: dict[str, Any] | None = None
    raw_payload_digest: str

    @classmethod
    def from_get_transaction(cls, *, signature: str, commitment: str, response: Mapping[str, object]) -> Self:
        transaction, meta = response.get("transaction"), response.get("meta")
        if (
            not isinstance(transaction, Mapping)
            or not isinstance(meta, Mapping)
            or not isinstance(transaction.get("message"), Mapping)
        ):
            raise ValueError("getTransaction response lacks consumed transaction/message/meta")
        signatures = transaction.get("signatures")
        if not isinstance(signatures, list) or not signatures:
            raise ValueError("getTransaction response lacks primary signature")
        if signatures[0] != signature:
            raise ValueError("getTransaction response primary signature mismatch")
        inner = meta.get("innerInstructions", ())
        balances = {key: meta[key] for key in ("preTokenBalances", "postTokenBalances") if key in meta} or None
        return cls(
            signature=signature,
            transaction_signatures=tuple(cast(str, item) for item in signatures),
            slot=_index(response.get("slot"), field="transaction slot"),
            commitment=commitment,
            meta_err=meta.get("err"),
            message=dict(transaction["message"]),
            inner_instructions=tuple(dict(item) for item in inner if isinstance(item, Mapping)),
            balances=balances,
            raw_payload_digest=_digest(
                cls._payload(
                    signature,
                    signatures,
                    response.get("slot"),
                    commitment,
                    meta.get("err"),
                    transaction["message"],
                    inner,
                    balances,
                )
            ),
        )

    @staticmethod
    def _payload(
        signature: object,
        signatures: object,
        slot: object,
        commitment: object,
        meta_err: object,
        message: object,
        inner: object,
        balances: object,
    ) -> dict[str, object]:
        return {
            "domain": "pumpfun.raw.getTransaction",
            "schema": "v3",
            "method": "getTransaction",
            "commitment": commitment,
            "signature": signature,
            "slot": slot,
            "meta": {"err": meta_err, "innerInstructions": inner, "balances": balances},
            "transaction": {"signatures": signatures, "message": message},
        }

    @model_validator(mode="after")
    def _bound(self) -> Self:
        _signature(self.signature)
        _index(self.slot, field="transaction slot")
        if not self.transaction_signatures or self.transaction_signatures[0] != self.signature:
            raise ValueError("raw transaction primary signature mismatch")
        for signature in self.transaction_signatures:
            _signature(signature, field="transaction signature")
        if self.commitment != "finalized" or not isinstance(self.message.get("instructions"), list):
            raise ValueError("getTransaction must be finalized with retained instructions")
        if self.raw_payload_digest != _digest(
            self._payload(
                self.signature,
                self.transaction_signatures,
                self.slot,
                self.commitment,
                self.meta_err,
                self.message,
                self.inner_instructions,
                self.balances,
            )
        ):
            raise ValueError("raw transaction digest does not bind consumed content")
        return self

    @property
    def meta_success(self) -> bool:
        return self.meta_err is None


class PumpRawBlockEvidence(_RevalidatingModel):
    slot: int
    blockhash: str
    previous_blockhash: str | None = None
    block_height: int | None = None
    block_time: int | None = None
    commitment: str
    signatures: tuple[str, ...]
    raw_payload_digest: str

    @classmethod
    def from_get_block(cls, *, slot: int, commitment: str, response: Mapping[str, object]) -> Self:
        raw_signatures = response.get("signatures")
        if isinstance(raw_signatures, list):
            signatures = raw_signatures
        else:
            rows = response.get("transactions")
            if not isinstance(rows, list):
                raise ValueError("getBlock response lacks ordered signatures")
            signatures = []
            for row in rows:
                try:
                    signatures.append(row["transaction"]["signatures"][0])
                except (KeyError, IndexError, TypeError) as error:
                    raise ValueError("getBlock transaction lacks primary signature") from error
        header = {key: response.get(key) for key in ("blockhash", "previousBlockhash", "blockHeight", "blockTime")}
        return cls(
            slot=slot,
            blockhash=cast(str, response.get("blockhash")),
            previous_blockhash=cast(str | None, response.get("previousBlockhash")),
            block_height=cast(int | None, response.get("blockHeight")),
            block_time=cast(int | None, response.get("blockTime")),
            commitment=commitment,
            signatures=tuple(signatures),
            raw_payload_digest=_digest(cls._payload(slot, commitment, header, signatures)),
        )

    @staticmethod
    def _payload(slot: object, commitment: object, header: object, signatures: object) -> dict[str, object]:
        return {
            "domain": "pumpfun.raw.getBlock",
            "schema": "v3",
            "method": "getBlock",
            "commitment": commitment,
            "slot": slot,
            "header": header,
            "orderedSignatureMembership": signatures,
        }

    @model_validator(mode="after")
    def _bound(self) -> Self:
        _index(self.slot, field="block slot")
        _signature(self.blockhash, field="blockhash")
        if self.previous_blockhash is not None:
            _signature(self.previous_blockhash, field="previous blockhash")
        if self.commitment != "finalized" or not self.signatures:
            raise ValueError("getBlock must be finalized with signatures")
        for signature in self.signatures:
            _signature(signature)
        if self.block_height is not None:
            _index(self.block_height, field="block height")
        if isinstance(self.block_time, bool) or self.block_time is not None and not isinstance(self.block_time, int):
            raise ValueError("block time must be integer or null")
        header = {
            "blockhash": self.blockhash,
            "previousBlockhash": self.previous_blockhash,
            "blockHeight": self.block_height,
            "blockTime": self.block_time,
        }
        if self.raw_payload_digest != _digest(self._payload(self.slot, self.commitment, header, self.signatures)):
            raise ValueError("raw block digest does not bind consumed content")
        return self


class PumpDecodedInstructionFact(_RevalidatingModel):
    raw_transaction_digest: str
    instruction_index: int
    inner_instruction_index: int | None = None
    instruction_kind: str
    mint: str
    market: str
    program_address: str
    discriminator: str
    decoder_digest: str
    bonding_curve: str | None = None
    associated_bonding_curve: str | None = None

    @model_validator(mode="after")
    def _shape(self) -> Self:
        _index(self.instruction_index, field="instruction coordinate")
        if self.inner_instruction_index is not None:
            _index(self.inner_instruction_index, field="inner instruction coordinate")
        if (
            self.instruction_kind not in _DISCRIMINATORS
            or self.discriminator != _DISCRIMINATORS[self.instruction_kind]
            or self.program_address != PUMP_PROGRAM_ADDRESS
        ):
            raise ValueError("decoded Pump schema is invalid")
        _signature(self.mint, field="mint")
        _signature(self.market, field="market")
        if self.instruction_kind in {"create", "create_v2"}:
            _signature(self.bonding_curve, field="bonding curve")
            _signature(self.associated_bonding_curve, field="associated bonding curve")
            if self.market != self.bonding_curve:
                raise ValueError("launch market must bind bonding curve")
        elif self.bonding_curve is not None or self.associated_bonding_curve is not None:
            raise ValueError("buy cannot claim launch-only account roles")
        if len(self.raw_transaction_digest) != 64 or len(self.decoder_digest) != 64:
            raise ValueError("decoded evidence digest is invalid")
        return self


def parse_pump_instruction(
    transaction: PumpRawTransactionEvidence,
    decoder: PumpDecoderEvidence,
    *,
    instruction_index: int,
    inner_instruction_index: int | None = None,
) -> PumpDecodedInstructionFact:
    transaction = PumpRawTransactionEvidence.model_validate(transaction.model_dump(mode="python"))
    decoder = PumpDecoderEvidence.model_validate(decoder.model_dump(mode="python"))
    instructions: object = transaction.message.get("instructions")
    coordinate = instruction_index
    if inner_instruction_index is not None:
        try:
            parents = tuple(
                item
                for item in transaction.inner_instructions
                if _index(item.get("index"), field="inner parent coordinate") == instruction_index
            )
            if len(parents) != 1:
                raise ValueError("duplicate or absent inner parent coordinate")
            instructions, coordinate = parents[0]["instructions"], inner_instruction_index
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("instruction coordinate absent or duplicate") from error
    try:
        raw = instructions[coordinate]  # type: ignore[index]
    except (IndexError, TypeError) as error:
        raise ValueError("instruction coordinate absent") from error
    if not isinstance(raw, Mapping) or not transaction.meta_success:
        raise ValueError("failed or absent raw instruction")
    program, data, accounts = raw.get("programId"), raw.get("data"), raw.get("accounts")
    decoded_data = _instruction_data_hex(data)
    kind = next(
        (
            name
            for name, discriminator in _DISCRIMINATORS.items()
            if decoded_data is not None and decoded_data.startswith(discriminator)
        ),
        None,
    )
    if program != decoder.program_address or kind is None or not isinstance(accounts, list):
        raise ValueError("unsupported Pump raw instruction")
    mapping = decoder.role_mapping[kind]
    try:
        required_account_count = len(
            next(
                instruction["accounts"]
                for instruction in decoder.idl_content["instructions"]
                if instruction["name"] == kind and instruction["discriminator"] == _DISCRIMINATORS[kind]
            )
        )
    except (KeyError, StopIteration, TypeError) as error:
        raise ValueError("unsupported Pump decoder layout") from error
    if (
        len(accounts) < required_account_count
        or kind == "create"
        and len(accounts) != required_account_count
        or any(not isinstance(account, str) or not account for account in accounts[:required_account_count])
        or any(not isinstance(accounts[position], str) or not accounts[position] for position in mapping.values())
        or len({accounts[position] for position in mapping.values()}) != len(mapping)
    ):
        raise ValueError("unsupported Pump raw account layout")
    launch_roles = mapping if kind in {"create", "create_v2"} else None
    return PumpDecodedInstructionFact(
        raw_transaction_digest=transaction.raw_payload_digest,
        instruction_index=instruction_index,
        inner_instruction_index=inner_instruction_index,
        instruction_kind=kind,
        mint=accounts[mapping["mint"]],
        market=accounts[mapping["bonding_curve"]] if launch_roles else accounts[mapping["market"]],
        bonding_curve=accounts[mapping["bonding_curve"]] if launch_roles else None,
        associated_bonding_curve=accounts[mapping["associated_bonding_curve"]] if launch_roles else None,
        program_address=cast(str, program),
        discriminator=_DISCRIMINATORS[kind],
        decoder_digest=decoder.idl_digest,
    )


class PumpSignatureCandidate(_RevalidatingModel):
    signature: str
    slot: int
    transaction_index: int
    source_time: int
    raw_transaction: PumpRawTransactionEvidence
    raw_block: PumpRawBlockEvidence
    decoder: PumpDecoderEvidence
    instruction_facts: tuple[PumpDecodedInstructionFact, ...]

    @model_validator(mode="after")
    def _bound(self) -> Self:
        _signature(self.signature)
        _index(self.slot, field="candidate slot")
        _index(self.transaction_index, field="transaction index")
        transaction = PumpRawTransactionEvidence.model_validate(
            self.raw_transaction.model_dump(mode="python")
            if isinstance(self.raw_transaction, PumpRawTransactionEvidence)
            else self.raw_transaction
        )
        block = PumpRawBlockEvidence.model_validate(
            self.raw_block.model_dump(mode="python")
            if isinstance(self.raw_block, PumpRawBlockEvidence)
            else self.raw_block
        )
        decoder = PumpDecoderEvidence.model_validate(
            self.decoder.model_dump(mode="python") if isinstance(self.decoder, PumpDecoderEvidence) else self.decoder
        )
        if transaction.signature != self.signature:
            raise ValueError("candidate transaction signature mismatch")
        if transaction.slot != self.slot or block.slot != self.slot:
            raise ValueError("candidate raw slot mismatch")
        if block.block_time != self.source_time:
            raise ValueError("candidate source time must bind raw block time")
        if (
            self.transaction_index >= len(block.signatures)
            or block.signatures[self.transaction_index] != self.signature
        ):
            raise ValueError("block membership/index does not bind candidate signature")
        facts = tuple(
            PumpDecodedInstructionFact.model_validate(
                fact.model_dump(mode="python") if isinstance(fact, PumpDecodedInstructionFact) else fact
            )
            for fact in self.instruction_facts
        )
        derived = tuple(
            parse_pump_instruction(
                transaction,
                decoder,
                instruction_index=fact.instruction_index,
                inner_instruction_index=fact.inner_instruction_index,
            )
            for fact in facts
        )
        if not facts or facts != derived:
            raise ValueError("decoded instruction is not derived from bound raw transaction")
        coordinates = tuple(
            (fact.instruction_index, -1 if fact.inner_instruction_index is None else fact.inner_instruction_index)
            for fact in facts
        )
        if coordinates != tuple(sorted(coordinates)) or len(coordinates) != len(set(coordinates)):
            raise ValueError("instruction coordinates must be canonical")
        return self


class PumpRawSignaturePageEvidence(_RevalidatingModel):
    program_address: str
    commitment: str
    request_before: str | None
    limit: int
    records: tuple[dict[str, Any], ...]
    raw_payload_digest: str

    @classmethod
    def from_get_signatures_for_address(cls, *, before: str | None, limit: int, response: object) -> Self:
        if not isinstance(response, list) or any(not isinstance(row, Mapping) for row in response):
            raise ValueError("getSignaturesForAddress response must be a list of records")
        try:
            records = tuple(
                {field: row[field] for field in ("signature", "slot", "err", "blockTime")} for row in response
            )
        except KeyError as error:
            raise ValueError("getSignaturesForAddress record is incomplete") from error
        return cls(
            program_address=PUMP_PROGRAM_ADDRESS,
            commitment="finalized",
            request_before=before,
            limit=limit,
            records=records,
            raw_payload_digest=_digest(cls._payload(before, limit, records)),
        )

    @staticmethod
    def _payload(before: object, limit: object, records: object) -> dict[str, object]:
        return {
            "domain": "pumpfun.raw.getSignaturesForAddress",
            "schema": "v3",
            "method": "getSignaturesForAddress",
            "address": PUMP_PROGRAM_ADDRESS,
            "commitment": "finalized",
            "before": before,
            "limit": limit,
            "records": records,
        }

    @model_validator(mode="after")
    def _bound(self) -> Self:
        if self.program_address != PUMP_PROGRAM_ADDRESS or self.commitment != "finalized":
            raise ValueError("signature page scope must be finalized Pump program")
        if self.request_before is not None:
            _signature(self.request_before, field="request_before")
        _index(self.limit, field="page limit")
        if not self.records or len(self.records) > self.limit:
            raise ValueError("signature page records must be non-empty and within limit")
        for record in self.records:
            if set(record) != {"signature", "slot", "err", "blockTime"}:
                raise ValueError("signature page record schema is invalid")
            _signature(record["signature"])
            _index(record["slot"], field="page record slot")
            if record["err"] is not None and not isinstance(record["err"], Mapping):
                raise ValueError("page record err is invalid")
            if record["blockTime"] is not None:
                _index(record["blockTime"], field="page record blockTime")
        if self.raw_payload_digest != _digest(self._payload(self.request_before, self.limit, self.records)):
            raise ValueError("raw signature page digest does not bind returned records")
        return self


class PumpCandidateDispositionKind(StrEnum):
    ELIGIBLE_DECODED = "eligible_decoded"
    FAILED_TRANSACTION = "failed_transaction"
    NULL_BLOCK_TIME = "null_block_time"
    CANDIDATE_CAP_REACHED = "candidate_cap_reached"
    UNSUPPORTED_OR_NO_RELEVANT_INSTRUCTION = "unsupported_or_no_relevant_instruction"


class PumpPageRecordDisposition(_RevalidatingModel):
    signature: str
    slot: int
    disposition: PumpCandidateDispositionKind
    candidate: PumpSignatureCandidate | None = None

    @model_validator(mode="after")
    def _classification(self) -> Self:
        _signature(self.signature)
        _index(self.slot, field="classification slot")
        if self.candidate is not None:
            candidate = PumpSignatureCandidate.model_validate(self.candidate.model_dump(mode="python"))
            if (candidate.signature, candidate.slot) != (self.signature, self.slot):
                raise ValueError("classification candidate identity mismatch")
            if self.disposition is not PumpCandidateDispositionKind.ELIGIBLE_DECODED:
                raise ValueError("decoded candidate requires eligible disposition")
        elif self.disposition is PumpCandidateDispositionKind.ELIGIBLE_DECODED:
            raise ValueError("eligible disposition requires decoded candidate")
        return self


class PumpSignaturePage(_RevalidatingModel):
    raw_page: PumpRawSignaturePageEvidence
    classifications: tuple[PumpPageRecordDisposition, ...]

    @property
    def candidates(self) -> tuple[PumpSignatureCandidate, ...]:
        return tuple(item.candidate for item in self.classifications if item.candidate is not None)

    @model_validator(mode="after")
    def _page(self) -> Self:
        raw = PumpRawSignaturePageEvidence.model_validate(self.raw_page.model_dump(mode="python"))
        classifications = tuple(
            PumpPageRecordDisposition.model_validate(item.model_dump(mode="python")) for item in self.classifications
        )
        expected = tuple((record["signature"], record["slot"]) for record in raw.records)
        actual = tuple((item.signature, item.slot) for item in classifications)
        if actual != expected:
            raise ValueError("signature page classifications must exactly bind ordered raw records")
        for record, item in zip(raw.records, classifications, strict=True):
            if item.candidate is not None:
                candidate = item.candidate
                if (
                    record["err"] is not None
                    or record["blockTime"] is None
                    or (candidate.raw_transaction.meta_err, candidate.source_time)
                    != (record["err"], record["blockTime"])
                ):
                    raise ValueError("decoded candidate must fully bind eligible raw record")
                continue
            if record["err"] is not None:
                allowed_dispositions = {PumpCandidateDispositionKind.FAILED_TRANSACTION}
            elif record["blockTime"] is None:
                allowed_dispositions = {PumpCandidateDispositionKind.NULL_BLOCK_TIME}
            else:
                allowed_dispositions = {
                    PumpCandidateDispositionKind.UNSUPPORTED_OR_NO_RELEVANT_INSTRUCTION,
                    PumpCandidateDispositionKind.CANDIDATE_CAP_REACHED,
                }
            if item.disposition not in allowed_dispositions:
                raise ValueError("raw record disposition is not explicit")
        return self


class VerifiedPumpLaunchUniverse(_RevalidatingModel):
    profile: PumpQualifiedSourceProfile
    pages: tuple[PumpSignaturePage, ...]
    lower_signature: str
    upper_signature: str
    terminal_reason: str
    terminal_cursor: str
    expected_candidate_count: int
    page_digests: tuple[str, ...]
    successful_launch_count: int | None = None
    completeness_level: str = _COMPLETENESS

    @model_validator(mode="after")
    def _complete(self) -> Self:
        if self.completeness_level != _COMPLETENESS or self.terminal_reason not in {"lower_bound_reached", "exhausted"}:
            raise ValueError("unsupported bounded completeness")
        pages = tuple(PumpSignaturePage.model_validate(page.model_dump(mode="python")) for page in self.pages)
        if not pages or pages[0].raw_page.request_before is not None:
            raise ValueError("first page must begin without a continuation")
        if any(
            page.raw_page.request_before != previous.raw_page.records[-1]["signature"]
            for previous, page in zip(pages, pages[1:], strict=False)
        ):
            raise ValueError("signature page continuation must bind preceding terminal cursor")
        flat = tuple(item for page in pages for item in page.classifications)
        if (
            flat[0].signature != self.upper_signature
            or flat[-1].signature != self.lower_signature
            or self.terminal_cursor != self.lower_signature
        ):
            raise ValueError("universe anchors and terminal cursor must bind page endpoints")
        if self.terminal_reason == "exhausted" and len(pages[-1].raw_page.records) >= pages[-1].raw_page.limit:
            raise ValueError("exhausted terminal requires a final short page")
        if self.expected_candidate_count != len(flat) or self.page_digests != tuple(
            page.raw_page.raw_payload_digest for page in pages
        ):
            raise ValueError("universe reconciliation does not bind raw page completeness")
        if len({item.signature for item in flat}) != len(flat):
            raise ValueError("duplicate bounded universe signature")
        decoder = PumpDecoderEvidence.model_validate(self.profile.decoder.model_dump(mode="python"))
        candidates = tuple(candidate for page in pages for candidate in page.candidates)
        if any(candidate.decoder != decoder for candidate in candidates):
            raise ValueError("candidate decoder must bind qualified profile")
        launches = [
            fact
            for candidate in candidates
            for fact in candidate.instruction_facts
            if fact.instruction_kind in {"create", "create_v2"}
        ]
        if self.successful_launch_count is None:
            object.__setattr__(self, "successful_launch_count", len(launches))
        elif self.successful_launch_count != len(launches):
            raise ValueError("successful launch count mismatch")
        return self


class PumpMintSelectionProof(_RevalidatingModel):
    universe: VerifiedPumpLaunchUniverse
    mint: str

    @model_validator(mode="after")
    def _selection(self) -> Self:
        universe = VerifiedPumpLaunchUniverse.model_validate(self.universe.model_dump(mode="python"))
        _signature(self.mint, field="mint")
        if not any(
            fact.mint == self.mint and fact.instruction_kind in {"create", "create_v2"}
            for page in universe.pages
            for candidate in page.candidates
            for fact in candidate.instruction_facts
        ):
            raise ValueError("universe has no selected mint launch")
        return self


class PumpHistoricalFirstSuccessfulBuyFact(_RevalidatingModel):
    _forbidden_clocks: ClassVar[frozenset[str]] = frozenset(
        {"received_time", "decision_available_time", "availability"}
    )
    profile_id: str
    mint: str
    signature: str
    slot: int
    transaction_index: int
    instruction_index: int
    inner_instruction_index: int | None = None
    source_event_time: datetime

    def _assert_no_forbidden_attributes(self) -> None:
        if self._forbidden_clocks & object.__getattribute__(self, "__dict__").keys():
            raise ValueError("source-only fact forbids receipt/decision clocks")

    def __getattribute__(self, name: str) -> Any:
        if name in object.__getattribute__(self, "_forbidden_clocks") and name in object.__getattribute__(
            self, "__dict__"
        ):
            raise AttributeError(name)
        return super().__getattribute__(name)

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self._assert_no_forbidden_attributes()
        return super().model_dump(*args, **kwargs)

    @classmethod
    def model_construct(cls, _fields_set: set[str] | None = None, **values: Any) -> Self:
        if cls._forbidden_clocks & values.keys():
            raise ValueError("source-only fact forbids receipt/decision clocks")
        return cls.model_validate(values)

    @model_validator(mode="after")
    def _source_only(self) -> Self:
        _signature(self.mint, field="mint")
        _signature(self.signature)
        _index(self.slot, field="slot")
        _index(self.transaction_index, field="transaction index")
        _index(self.instruction_index, field="instruction index")
        if self.inner_instruction_index is not None:
            _index(self.inner_instruction_index, field="inner instruction index")
        _utc_time(self.source_event_time, field="source_event_time")
        return self


def _event_coordinate(
    candidate: PumpSignatureCandidate, fact: PumpDecodedInstructionFact
) -> tuple[int, int, tuple[int, int]]:
    return (
        candidate.slot,
        candidate.transaction_index,
        (fact.instruction_index, -1 if fact.inner_instruction_index is None else fact.inner_instruction_index),
    )


def select_first_successful_buy(proof: PumpMintSelectionProof) -> PumpHistoricalFirstSuccessfulBuyFact:
    proof = PumpMintSelectionProof.model_validate(proof.model_dump(mode="python"))
    launches = [
        (candidate, fact)
        for page in proof.universe.pages
        for candidate in page.candidates
        for fact in candidate.instruction_facts
        if fact.mint == proof.mint and fact.instruction_kind in {"create", "create_v2"}
    ]
    eligible = [
        (candidate, fact)
        for page in proof.universe.pages
        for candidate in page.candidates
        for fact in candidate.instruction_facts
        if fact.mint == proof.mint
        and fact.instruction_kind == "buy"
        and any(
            fact.market == launch.market
            and _event_coordinate(candidate, fact) > _event_coordinate(launch_candidate, launch)
            for launch_candidate, launch in launches
        )
    ]
    if not eligible:
        raise ValueError("no successful Pump buy after launch")
    candidate, fact = min(eligible, key=lambda item: _event_coordinate(*item))
    return PumpHistoricalFirstSuccessfulBuyFact(
        profile_id=proof.universe.profile.profile_id,
        mint=proof.mint,
        signature=candidate.signature,
        slot=candidate.slot,
        transaction_index=candidate.transaction_index,
        instruction_index=fact.instruction_index,
        inner_instruction_index=fact.inner_instruction_index,
        source_event_time=datetime.fromtimestamp(candidate.source_time, UTC),
    )
