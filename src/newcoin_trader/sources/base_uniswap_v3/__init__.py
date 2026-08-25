"""Base Uniswap V3 in-memory source boundary (Phase 8C.4)."""

from newcoin_trader.sources.base_uniswap_v3.adapter import (
    AdapterRequest,
    adapt_dex_first_trade,
    build_eligible_scope,
    compute_realized_execution_price,
    select_earliest_valid_swap,
    validate_factory_universe,
)
from newcoin_trader.sources.base_uniswap_v3.contracts import (
    CHAIN_ID,
    FACTORY_ADDRESS,
    POOL_CREATED_TOPIC,
    PROTOCOL_VERSION,
    SWAP_TOPIC,
)
from newcoin_trader.sources.base_uniswap_v3.models import (
    CanonicalPoolCreatedEvidence,
    CanonicalSwapEvidence,
    CapAmbiguityError,
    CapPolicy,
    ExactPoolHistoryScanProof,
    FactoryPoolCreatedRecord,
    FactoryUniverseScanProof,
    FinalityBoundary,
    ScanKind,
    ScanLedgerEntry,
    ScanStatus,
    SwapLogRecord,
    VerifiedBlock,
    VerifiedExactPoolSwapScanResult,
    VerifiedFactoryUniverse,
    VerifiedReceipt,
    compute_canonical_swap_candidates_digest,
)
from newcoin_trader.sources.base_uniswap_v3.pool_created_decoder import decode_pool_created_log
from newcoin_trader.sources.base_uniswap_v3.scan_ledger import InMemoryScanLedger
from newcoin_trader.sources.base_uniswap_v3.swap_decoder import decode_swap_log

__all__ = [
    "CHAIN_ID",
    "FACTORY_ADDRESS",
    "POOL_CREATED_TOPIC",
    "PROTOCOL_VERSION",
    "SWAP_TOPIC",
    "AdapterRequest",
    "CanonicalPoolCreatedEvidence",
    "CanonicalSwapEvidence",
    "CapAmbiguityError",
    "CapPolicy",
    "ExactPoolHistoryScanProof",
    "FactoryPoolCreatedRecord",
    "FactoryUniverseScanProof",
    "FinalityBoundary",
    "InMemoryScanLedger",
    "ScanKind",
    "ScanLedgerEntry",
    "ScanStatus",
    "SwapLogRecord",
    "VerifiedBlock",
    "VerifiedFactoryUniverse",
    "VerifiedExactPoolSwapScanResult",
    "VerifiedReceipt",
    "adapt_dex_first_trade",
    "build_eligible_scope",
    "compute_canonical_swap_candidates_digest",
    "compute_realized_execution_price",
    "decode_pool_created_log",
    "decode_swap_log",
    "select_earliest_valid_swap",
    "validate_factory_universe",
]
