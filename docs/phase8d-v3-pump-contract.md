# Phase 8D v3 — Pump historical source contract

## Scope and trust graph

This is a **pure, bounded historical source-fact** slice for the official Pump program
`6EF8…wF6P`, frozen at `pumpfun-programdata-v0.1.0` and a pinned IDL digest. It is not
collector/RPC acquisition, corpus building, pricing, event study, persistence, or a Phase 3
projection. Trust flows only:

`finalized signature pages → canonical getTransaction + getBlock raw evidence → exact Pump instruction coordinate → program-wide bounded launch universe → mint selection → SOURCE_TIME_ONLY fact`.

A source fact structurally has no receipt, decision, availability, or Phase 3 projection fields.
`SOURCE_TIME_ONLY` means its timestamp is source/block time only; it asserts no historical
real-time observability.

## Raw-chain evidence

Each consumed `getTransaction` response is sanitized and SHA-256 canonicalized with fixed
`domain=pumpfun.raw.getTransaction`, `schema=v3`, `method=getTransaction`, finalized commitment,
signature, slot, meta error, message (including instructions), and inner instructions/balances only
when consumed. Each `getBlock` digest uses fixed `domain=pumpfun.raw.getBlock`, `schema=v3`,
`method=getBlock`, finalized commitment, requested slot, header (`blockhash`, optional previous
hash/block height/time), and ordered primary-signature membership.

A candidate requires equal transaction/candidate/block slot and requires its signature at exactly
its claimed index in the fetched block. **It must not compare a transaction message's
`recentBlockhash` to the containing block hash.** The latter is a transaction validity reference,
not block-membership evidence.

The narrow Pump parser receives canonical transaction evidence plus `(outer instruction index,
optional inner index)`, revalidates the raw digest, reads only that retained instruction, and derives
its roles only from the module-private pinned decoder IDL fixture and fixed decoder digest. Raw decoder
content and digest must exactly equal that fixture; this static identity alone does **not** prove the
live ProgramData/IDL. A future live qualification must independently bind this static identity to its
raw ProgramData and IDL evidence. Constructed/copied nested evidence is rebuilt at
the candidate, page, universe, selection, and final selector boundaries.

## Ordering, completeness, and deferred economics

The universe is program-level and mint-free before selection. Its bounded page sequence has fixed
upper/lower anchors, contiguous cursors, unique signatures, and a declared
`RESEARCH_GRADE_BOUNDED_COMPLETENESS_V1` scope; it is not an absolute backend-completeness claim.
`t0` is the earliest causally post-launch supported buy ordered strictly by
`(slot, transaction_index, instruction_coordinate)`, never page order, timestamp, or signature.

Price is deliberately deferred. A later reviewed protocol must specify attributable trade amount and
asset orientation, token-decimal denominator, fee/cost treatment, exact execution-price definition,
and source coverage before any return/event-study use. Reviewer rubric: reject any assertion not
bound to canonical raw evidence; reject clock injection, failed transaction, altered raw payload,
wrong slot/index/membership/coordinate, non-program-wide universe, or any implicit price/receipt/
decision claim.
