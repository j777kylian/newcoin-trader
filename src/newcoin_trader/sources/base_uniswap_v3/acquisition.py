"""Offline, bounded Base JSON-RPC evidence acquisition."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, TypedDict

from newcoin_trader.sources.base_uniswap_v3.contracts import CHAIN_ID, FACTORY_ADDRESS, POOL_CREATED_TOPIC, SWAP_TOPIC
from newcoin_trader.sources.base_uniswap_v3.models import (
    CanonicalPoolCreatedEvidence,
    CanonicalSwapEvidence,
    CapAmbiguityError,
    FactoryDeploymentAnchor,
    FactoryUniverseScanProof,
    FinalityBoundary,
    ScanKind,
    ScanStatus,
    TokenDecimalsEvidence,
    VerifiedBlock,
    VerifiedExactPoolSwapScanResult,
    VerifiedFactoryUniverse,
    VerifiedReceipt,
    compute_canonical_pool_created_candidates_digest,
    compute_canonical_swap_candidates_digest,
    compute_token_decimals_evidence_digest,
    normalize_address,
    normalize_hex32,
    parse_hex_uint,
    reject_removed_log,
    require_non_bool_int,
    strict_reconstruct_model,
)
from newcoin_trader.sources.base_uniswap_v3.pool_created_decoder import decode_pool_created_log
from newcoin_trader.sources.base_uniswap_v3.provider import (
    BaseRpcProvider,
    RpcCapabilityProfile,
    RpcError,
    RpcResponseError,
    RpcTransportError,
)
from newcoin_trader.sources.base_uniswap_v3.scan_ledger import InMemoryScanLedger
from newcoin_trader.sources.base_uniswap_v3.swap_decoder import decode_swap_log


class _ScanEntryArgs(TypedDict):
    scan_id: str
    parent_scan_id: str | None
    scan_kind: ScanKind
    from_block: int
    to_block: int
    configured_cap: int
    address: str
    topic0: str
    provider_endpoint: str
    provider_version: str
    split_depth: int


class AcquisitionFailure(StrEnum):
    TRANSPORT_FAILURE = "TRANSPORT_FAILURE"
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
    RPC_ERROR = "RPC_ERROR"
    MALFORMED_RESPONSE = "MALFORMED_RESPONSE"
    UNSUPPORTED_PROVIDER_CAPABILITY = "UNSUPPORTED_PROVIDER_CAPABILITY"
    FINALITY_MISMATCH = "FINALITY_MISMATCH"
    INCOMPLETE_SCAN = "INCOMPLETE_SCAN"
    CAP_AMBIGUITY = "CAP_AMBIGUITY"
    REQUEST_BUDGET_EXHAUSTED = "REQUEST_BUDGET_EXHAUSTED"
    SPLIT_DEPTH_EXHAUSTED = "SPLIT_DEPTH_EXHAUSTED"
    RECEIPT_UNAVAILABLE = "RECEIPT_UNAVAILABLE"
    RECEIPT_MISMATCH = "RECEIPT_MISMATCH"
    BLOCK_UNAVAILABLE = "BLOCK_UNAVAILABLE"
    BLOCK_MISMATCH = "BLOCK_MISMATCH"
    DECIMALS_UNAVAILABLE = "DECIMALS_UNAVAILABLE"
    DECIMALS_MALFORMED = "DECIMALS_MALFORMED"


@dataclass(frozen=True)
class AcquisitionFailureResult:
    failure: AcquisitionFailure


class AcquisitionError(RuntimeError):
    def __init__(self, failure: AcquisitionFailure) -> None:
        self.result = AcquisitionFailureResult(failure)
        super().__init__(failure.value)


class BaseEvidenceAcquirer:
    """One capability profile, finite ranges, ledger-backed scans, and no latest fallback."""

    def __init__(
        self,
        provider: BaseRpcProvider,
        *,
        profile: RpcCapabilityProfile | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.provider = provider
        self.profile = profile or provider.profile
        if self.profile != provider.profile:
            raise ValueError("acquirer profile must equal provider profile")
        self._now = now
        self._receipts: dict[str, VerifiedReceipt] = {}
        self._blocks: dict[int, VerifiedBlock] = {}
        self._block_identities: dict[str, VerifiedBlock] = {}
        self._active_budget: list[int] | None = None

    def _validated_profile(self) -> RpcCapabilityProfile:
        profile = RpcCapabilityProfile(**self.profile.__dict__)
        if profile != self.provider._validated_profile():
            raise ValueError("acquirer profile must equal provider profile")
        return profile

    @contextmanager
    def _operation(self, budget: int) -> Iterator[None]:
        self._validated_profile()
        require_non_bool_int(budget, field_name="operation budget", minimum=1)
        if self._active_budget is not None:
            yield
            return
        self._active_budget = [budget]
        try:
            yield
        finally:
            self._active_budget = None

    async def _call(self, method: str, params: list[object], *, allow_rpc_error: bool = False) -> object:
        profile = self._validated_profile()

        def consume_attempt() -> None:
            if self._active_budget is not None:
                if self._active_budget[0] < 1:
                    raise AcquisitionError(AcquisitionFailure.REQUEST_BUDGET_EXHAUSTED)
                self._active_budget[0] -= 1

        try:
            call = await self.provider.call(method, params, on_attempt=consume_attempt)
        except RpcTransportError as exc:
            if exc.retry_exhausted:
                raise AcquisitionError(AcquisitionFailure.RETRY_EXHAUSTED) from None
            failure = AcquisitionFailure.MALFORMED_RESPONSE if exc.malformed else AcquisitionFailure.TRANSPORT_FAILURE
            raise AcquisitionError(failure) from None
        except RpcError:
            raise AcquisitionError(AcquisitionFailure.RPC_ERROR) from None
        if call.rpc_error is not None:
            if call.attempt_count >= self.profile.max_attempts and call.rpc_error.code in profile.retryable_rpc_codes:
                raise AcquisitionError(AcquisitionFailure.RETRY_EXHAUSTED)
            if allow_rpc_error:
                raise call.rpc_error
            raise AcquisitionError(AcquisitionFailure.RPC_ERROR)
        return call.result

    async def _chain_gate(self) -> int:
        chain_id = parse_hex_uint(await self._call("eth_chainId", []), field_name="eth_chainId")
        if chain_id != CHAIN_ID or chain_id != self._validated_profile().chain_id:
            raise ValueError("RPC endpoint is not Base")
        return chain_id

    async def verify_chain(self) -> int:
        with self._operation(self._validated_profile().factory_request_budget):
            return await self._chain_gate()

    async def read_finality_boundary(self) -> FinalityBoundary:
        with self._operation(self._validated_profile().factory_request_budget):
            await self._chain_gate()
            return await self._read_finality_boundary()

    async def _read_finality_boundary(self) -> FinalityBoundary:
        profile = self._validated_profile()
        if not profile.supports_finalized_tag:
            raise AcquisitionError(AcquisitionFailure.UNSUPPORTED_PROVIDER_CAPABILITY)
        tagged = await self._block("finalized", refresh=True)
        reread = await self._block(hex(tagged.number), refresh=True)
        if tagged != reread:
            raise AcquisitionError(AcquisitionFailure.FINALITY_MISMATCH)
        return FinalityBoundary.model_validate(
            {
                "number": tagged.number,
                "hash": tagged.hash,
                "policy": "rpc_finalized_tag_number_hash_v1",
                "version": "rpc_finalized_tag_number_hash_v1",
                "source": profile.sanitized_endpoint,
                "verified_timestamp": self._now(),
            }
        )

    async def verify_factory_deployment(self, anchor: FactoryDeploymentAnchor) -> FactoryDeploymentAnchor:
        with self._operation(self._validated_profile().factory_request_budget):
            await self._chain_gate()
            return await self._verify_factory_deployment(anchor)

    async def _verify_factory_deployment(self, anchor: FactoryDeploymentAnchor) -> FactoryDeploymentAnchor:
        supplied = strict_reconstruct_model(FactoryDeploymentAnchor, anchor)
        receipt = await self._receipt(supplied.deployment_transaction_hash, refresh=True)
        block = await self._block(hex(supplied.deployment_block_number), refresh=True)
        verified = FactoryDeploymentAnchor.model_validate(
            {
                **supplied.model_dump(mode="python"),
                "receipt": receipt,
                "block": block,
            }
        )
        if verified != supplied:
            raise ValueError("supplied factory deployment anchor mismatch")
        return supplied

    async def acquire_token_decimals(
        self, creation: CanonicalPoolCreatedEvidence
    ) -> tuple[TokenDecimalsEvidence, TokenDecimalsEvidence]:
        with self._operation(self._validated_profile().exact_pool_request_budget):
            await self._chain_gate()
            return await self._acquire_token_decimals(creation)

    async def _acquire_token_decimals(
        self, creation: CanonicalPoolCreatedEvidence
    ) -> tuple[TokenDecimalsEvidence, TokenDecimalsEvidence]:
        profile = self._validated_profile()
        if not profile.supports_eip1898_block_hash_call or not profile.archive_eth_call_available:
            raise AcquisitionError(AcquisitionFailure.UNSUPPORTED_PROVIDER_CAPABILITY)
        evidence = strict_reconstruct_model(CanonicalPoolCreatedEvidence, creation)
        block = evidence.block
        tokens = (evidence.creation.token0, evidence.creation.token1)
        values: list[TokenDecimalsEvidence] = []
        for token in tokens:
            result = await self._call(
                "eth_call",
                [
                    {"to": token, "data": "0x313ce567"},
                    {"blockHash": block.hash, "requireCanonical": True},
                ],
            )
            if result is None:
                raise AcquisitionError(AcquisitionFailure.DECIMALS_UNAVAILABLE)
            if not isinstance(result, str) or len(result) != 66 or not result.startswith("0x"):
                raise AcquisitionError(AcquisitionFailure.DECIMALS_MALFORMED)
            try:
                decimals = parse_hex_uint(result, field_name="eth_call decimals")
            except ValueError:
                raise AcquisitionError(AcquisitionFailure.DECIMALS_MALFORMED) from None
            if decimals > 255:
                raise AcquisitionError(AcquisitionFailure.DECIMALS_MALFORMED)
            version = "base_erc20_decimals_eip1898_v1"
            values.append(
                TokenDecimalsEvidence.model_validate(
                    {
                        "chain_id": CHAIN_ID,
                        "token_address": token,
                        "decimals": decimals,
                        "evidence_block_number": block.number,
                        "evidence_block_hash": block.hash,
                        "verification_version": version,
                        "evidence_digest": compute_token_decimals_evidence_digest(
                            chain_id=CHAIN_ID,
                            token_address=token,
                            decimals=decimals,
                            evidence_block_number=block.number,
                            evidence_block_hash=block.hash,
                            verification_version=version,
                        ),
                    }
                )
            )
        return tuple(values)  # type: ignore[return-value]

    async def _validate_finality(self, finality: FinalityBoundary, *, lower: int) -> FinalityBoundary:
        boundary = strict_reconstruct_model(FinalityBoundary, finality)
        if (
            boundary.policy != "rpc_finalized_tag_number_hash_v1"
            or boundary.version != "rpc_finalized_tag_number_hash_v1"
            or boundary.source != self._validated_profile().sanitized_endpoint
        ):
            raise AcquisitionError(AcquisitionFailure.FINALITY_MISMATCH)
        if boundary.number < lower:
            raise AcquisitionError(AcquisitionFailure.FINALITY_MISMATCH)
        observed = await self._read_finality_boundary()
        if boundary.number != observed.number or boundary.hash != observed.hash:
            raise AcquisitionError(AcquisitionFailure.FINALITY_MISMATCH)
        return boundary

    async def acquire_factory_universe(
        self, anchor: FactoryDeploymentAnchor, finality: FinalityBoundary
    ) -> VerifiedFactoryUniverse:
        with self._operation(self._validated_profile().factory_request_budget):
            await self._chain_gate()
            return await self._acquire_factory_universe(anchor, finality)

    async def _acquire_factory_universe(
        self, anchor: FactoryDeploymentAnchor, finality: FinalityBoundary
    ) -> VerifiedFactoryUniverse:
        supplied = await self._verify_factory_deployment(anchor)
        boundary = await self._validate_finality(finality, lower=supplied.deployment_block_number)
        ledger = InMemoryScanLedger()
        candidates = await self._scan(
            ledger=ledger,
            kind=ScanKind.FACTORY_POOL_CREATED,
            address=FACTORY_ADDRESS,
            topic0=POOL_CREATED_TOPIC,
            lower=supplied.deployment_block_number,
            upper=boundary.number,
            budget=self.profile.factory_request_budget,
            ceiling=self.profile.factory_candidate_ceiling,
            evidence=self._pool_created_evidence,
            digest=compute_canonical_pool_created_candidates_digest,
        )
        proof = FactoryUniverseScanProof.model_validate(
            {
                "factory_address": FACTORY_ADDRESS,
                "topic0": POOL_CREATED_TOPIC,
                "deployment_lower_block": supplied.deployment_block_number,
                "finality": boundary,
                "entries": ledger.entries,
            }
        )
        return VerifiedFactoryUniverse.from_complete_candidates(factory_scan_proof=proof, candidates=candidates)

    async def acquire_exact_pool_swaps(
        self, creation: CanonicalPoolCreatedEvidence, finality: FinalityBoundary
    ) -> VerifiedExactPoolSwapScanResult:
        with self._operation(self._validated_profile().exact_pool_request_budget):
            await self._chain_gate()
            return await self._acquire_exact_pool_swaps(creation, finality)

    async def _acquire_exact_pool_swaps(
        self, creation: CanonicalPoolCreatedEvidence, finality: FinalityBoundary
    ) -> VerifiedExactPoolSwapScanResult:
        created = strict_reconstruct_model(CanonicalPoolCreatedEvidence, creation)
        boundary = await self._validate_finality(finality, lower=created.creation.block_number)
        ledger = InMemoryScanLedger()
        candidates = await self._scan(
            ledger=ledger,
            kind=ScanKind.POOL_SWAP,
            address=created.creation.pool_address,
            topic0=SWAP_TOPIC,
            lower=created.creation.block_number,
            upper=boundary.number,
            budget=self.profile.exact_pool_request_budget,
            ceiling=self.profile.exact_pool_candidate_ceiling,
            evidence=self._swap_evidence,
            digest=compute_canonical_swap_candidates_digest,
        )
        from newcoin_trader.sources.base_uniswap_v3.models import ExactPoolHistoryScanProof

        proof = ExactPoolHistoryScanProof.model_validate(
            {
                "pool_address": created.creation.pool_address,
                "topic0": SWAP_TOPIC,
                "creation_block": created.creation.block_number,
                "finality": boundary,
                "entries": ledger.entries,
            }
        )
        return VerifiedExactPoolSwapScanResult.from_complete_candidates(
            pool_address=created.creation.pool_address, pool_scan_proof=proof, candidates=candidates
        )

    async def _scan(
        self,
        *,
        ledger: InMemoryScanLedger,
        kind: ScanKind,
        address: str,
        topic0: str,
        lower: int,
        upper: int,
        budget: int,
        ceiling: int,
        evidence: Callable[[dict[str, Any]], Awaitable[Any]],
        digest: Callable[[tuple[Any, ...]], str],
    ) -> tuple[Any, ...]:
        require_non_bool_int(lower, field_name="scan lower", minimum=0)
        require_non_bool_int(upper, field_name="scan upper", minimum=0)
        require_non_bool_int(budget, field_name="scan budget", minimum=1)
        require_non_bool_int(ceiling, field_name="candidate ceiling", minimum=1)
        if upper < lower:
            raise ValueError("scan upper must be >= lower")
        addr = normalize_address(address, field_name="scan address")
        topic = normalize_hex32(topic0, field_name="scan topic0")
        collected: list[Any] = []
        seen_raw: set[tuple[str, str, int]] = set()
        previous_budget = self._active_budget
        if self._active_budget is None:
            self._active_budget = [budget]

        def fields(scan_id: str, parent: str | None, start: int, end: int, depth: int) -> _ScanEntryArgs:
            return {
                "scan_id": scan_id,
                "parent_scan_id": parent,
                "scan_kind": kind,
                "from_block": start,
                "to_block": end,
                "configured_cap": self.profile.log_result_cap,
                "address": addr,
                "topic0": topic,
                "provider_endpoint": self.profile.sanitized_endpoint,
                "provider_version": self.profile.profile_version,
                "split_depth": depth,
            }

        def raw_identity(raw: dict[str, Any]) -> tuple[str, str, int]:
            return (
                normalize_hex32(raw.get("blockHash"), field_name="raw_log.blockHash"),
                normalize_hex32(raw.get("transactionHash"), field_name="raw_log.transactionHash"),
                parse_hex_uint(raw.get("logIndex"), field_name="raw_log.logIndex"),
            )

        def validate_raw(raw: object, start: int, end: int) -> dict[str, Any]:
            if not isinstance(raw, dict):
                raise ValueError("eth_getLogs item must be an object")
            reject_removed_log(raw)
            if normalize_address(raw.get("address"), field_name="raw_log.address") != addr:
                raise ValueError("raw log address filter mismatch")
            topics = raw.get("topics")
            if not isinstance(topics, list) or not topics or topics[0] != topic:
                raise ValueError("raw log topic filter mismatch")
            number = parse_hex_uint(raw.get("blockNumber"), field_name="raw_log.blockNumber")
            if number < start or number > end:
                raise ValueError("raw log block outside queried range")
            raw_identity(raw)
            return raw

        async def visit(start: int, end: int, depth: int, parent: str | None = None) -> None:
            if depth > self.profile.max_split_depth:
                raise AcquisitionError(AcquisitionFailure.SPLIT_DEPTH_EXHAUSTED)
            scan_id = f"{kind.value}:{start}:{end}:{depth}"
            try:
                result = await self._call(
                    "eth_getLogs",
                    [{"address": addr, "topics": [topic], "fromBlock": hex(start), "toBlock": hex(end)}],
                    allow_rpc_error=True,
                )
                if not isinstance(result, list):
                    raise ValueError("eth_getLogs result must be a list")
                raw_items = tuple(validate_raw(item, start, end) for item in result)
                identities = {raw_identity(item) for item in raw_items}
                if len(identities) != len(raw_items):
                    raise ValueError("duplicate canonical raw log identity")
                if len(result) >= self._validated_profile().log_result_cap:
                    if start == end:
                        failed = ledger.mark_failed(
                            status=ScanStatus.FAILED_CAP_AMBIGUITY,
                            response_count=len(result),
                            **fields(scan_id, parent, start, end, depth),
                        )
                        raise CapAmbiguityError(failed)
                    ledger.append_incomplete(**fields(scan_id, parent, start, end, depth))
                    midpoint = (start + end) // 2
                    await visit(start, midpoint, depth + 1, scan_id)
                    await visit(midpoint + 1, end, depth + 1, scan_id)
                    return
                if seen_raw & identities:
                    raise ValueError("duplicate canonical raw log identity")
                seen_raw.update(identities)
                if len(collected) + len(raw_items) > ceiling:
                    raise ValueError("candidate ceiling exceeded")
                items = tuple([await evidence(item) for item in raw_items])
                ledger.mark_completed(
                    response_count=len(items),
                    response_digest=digest(items) if items else "",
                    **fields(scan_id, parent, start, end, depth),
                )
                collected.extend(items)
            except CapAmbiguityError:
                raise AcquisitionError(AcquisitionFailure.CAP_AMBIGUITY) from None
            except RpcResponseError as exc:
                if exc.code in self._validated_profile().range_limit_rpc_codes and start < end:
                    ledger.append_incomplete(**fields(scan_id, parent, start, end, depth))
                    midpoint = (start + end) // 2
                    await visit(start, midpoint, depth + 1, scan_id)
                    await visit(midpoint + 1, end, depth + 1, scan_id)
                    return
                ledger.mark_failed(
                    status=ScanStatus.FAILED_PROVIDER,
                    response_count=0,
                    note="rpc_error",
                    **fields(scan_id, parent, start, end, depth),
                )
                raise AcquisitionError(AcquisitionFailure.INCOMPLETE_SCAN) from None
            except AcquisitionError:
                ledger.mark_failed(
                    status=ScanStatus.FAILED_PROVIDER,
                    response_count=0,
                    note="acquisition_failure",
                    **fields(scan_id, parent, start, end, depth),
                )
                raise
            except Exception as exc:
                ledger.mark_failed(
                    status=ScanStatus.FAILED_PROVIDER,
                    response_count=0,
                    note=type(exc).__name__,
                    **fields(scan_id, parent, start, end, depth),
                )
                raise

        try:
            for start in range(lower, upper + 1, self.profile.max_block_span):
                await visit(start, min(upper, start + self.profile.max_block_span - 1), 0)
        finally:
            self._active_budget = previous_budget
        return tuple(collected)

    async def _pool_created_evidence(self, raw: dict[str, Any]) -> CanonicalPoolCreatedEvidence:
        creation = decode_pool_created_log(raw)
        return CanonicalPoolCreatedEvidence.model_validate(
            {
                "raw_log": raw,
                "creation": creation,
                "receipt": await self._receipt(creation.transaction_hash),
                "block": await self._block(hex(creation.block_number)),
            }
        )

    async def _swap_evidence(self, raw: dict[str, Any]) -> CanonicalSwapEvidence:
        swap = decode_swap_log(raw)
        return CanonicalSwapEvidence.model_validate(
            {
                "raw_log": raw,
                "swap": swap,
                "receipt": await self._receipt(swap.transaction_hash),
                "block": await self._block(hex(swap.block_number)),
            }
        )

    async def _receipt(self, transaction_hash: str, *, refresh: bool = False) -> VerifiedReceipt:
        if not self._validated_profile().receipt_history_available:
            raise AcquisitionError(AcquisitionFailure.UNSUPPORTED_PROVIDER_CAPABILITY)
        tx = normalize_hex32(transaction_hash, field_name="transaction_hash")
        if not refresh and tx in self._receipts:
            return self._receipts[tx]
        raw = await self._call("eth_getTransactionReceipt", [tx])
        if raw is None:
            raise AcquisitionError(AcquisitionFailure.RECEIPT_UNAVAILABLE)
        if not isinstance(raw, dict):
            raise AcquisitionError(AcquisitionFailure.RECEIPT_UNAVAILABLE)
        try:
            receipt = VerifiedReceipt.model_validate(
                {
                    "transaction_hash": raw.get("transactionHash"),
                    "block_hash": raw.get("blockHash"),
                    "block_number": parse_hex_uint(raw.get("blockNumber"), field_name="receipt.blockNumber"),
                    "transaction_index": parse_hex_uint(
                        raw.get("transactionIndex"), field_name="receipt.transactionIndex"
                    ),
                    "status": parse_hex_uint(raw.get("status"), field_name="receipt.status"),
                    "contract_address": raw.get("contractAddress"),
                }
            )
        except (TypeError, ValueError):
            raise AcquisitionError(AcquisitionFailure.RECEIPT_MISMATCH) from None
        if receipt.transaction_hash != tx or receipt.status != 1:
            raise AcquisitionError(AcquisitionFailure.RECEIPT_MISMATCH)
        cached = self._receipts.get(tx)
        if cached is not None and cached != receipt:
            raise AcquisitionError(AcquisitionFailure.RECEIPT_MISMATCH)
        self._receipts[tx] = receipt
        return receipt

    async def _block(self, identifier: str, *, refresh: bool = False) -> VerifiedBlock:
        key = identifier.lower()
        if not refresh and key in self._block_identities:
            return self._block_identities[key]
        raw = await self._call("eth_getBlockByNumber", [identifier, False])
        if raw is None:
            raise AcquisitionError(AcquisitionFailure.BLOCK_UNAVAILABLE)
        if not isinstance(raw, dict):
            raise AcquisitionError(AcquisitionFailure.BLOCK_UNAVAILABLE)
        try:
            block = VerifiedBlock.model_validate(
                {
                    "number": parse_hex_uint(raw.get("number"), field_name="block.number"),
                    "hash": raw.get("hash"),
                    "timestamp": datetime.fromtimestamp(
                        parse_hex_uint(raw.get("timestamp"), field_name="block.timestamp"), UTC
                    ),
                }
            )
        except (OSError, TypeError, ValueError):
            raise AcquisitionError(AcquisitionFailure.BLOCK_MISMATCH) from None
        if key.startswith("0x") and block.number != parse_hex_uint(key, field_name="block identifier"):
            raise AcquisitionError(AcquisitionFailure.BLOCK_MISMATCH)
        cached = self._blocks.get(block.number)
        if cached is not None and cached != block:
            raise AcquisitionError(AcquisitionFailure.BLOCK_MISMATCH)
        self._blocks[block.number] = block
        self._block_identities[key] = block
        return block


__all__ = ["AcquisitionError", "AcquisitionFailure", "AcquisitionFailureResult", "BaseEvidenceAcquirer"]
