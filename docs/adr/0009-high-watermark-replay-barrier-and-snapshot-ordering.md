# ADR-0009: High-watermark replay barrier + snapshot ordering

- **Status:** Accepted
- **Date:** 2026-06-06
- **Design reference:** [`../design.md`](../design.md) §3.6 (D5, D19, invariant I8)

## Context

On taking over a tracker partition, the dispatch loop replays the tracker up to a **barrier** to
rebuild the in-memory index before dispatching (the I4 gate). Two distinct hazards make the choice
of barrier and the *timing* of its snapshot correctness-critical:

- **The LSO hazard.** A previous owner dispatched entry X in committed transaction T (destination
  write D_X + completion tombstone C_X at tracker offset `o_C`). Meanwhile an *ingest* transaction
  open since before T committed holds an ADD below `o_C`, so `LSO(p) < o_C`. A replay that stops at
  the LSO never sees C_X, concludes X is pending, and re-dispatches it — a duplicate.
- **The snapshot-ordering hazard.** A *stalled* predecessor whose `sendOffsetsToTransaction` was
  already accepted, then stalls before `commitTransaction`, can land C_X *above* a barrier snapshot
  taken at callback time, then commit successfully (its producer was never fenced). A replay gated
  to the stale barrier never applies C_X — duplicate again.

## Decision

The barrier is the partition **high watermark**, obtained via
`Admin.listOffsets(latest, READ_UNCOMMITTED)` — not the consumer's `endOffsets`, which returns the
LSO under `read_committed`. It is snapshotted **strictly after** the committed-cursor fetch for the
current assignment epoch resolves (**invariant I8**): under `read_committed` that fetch sets
`require_stable` and internally retries `UNSTABLE_OFFSET_COMMIT` until every pending transactional
offset commit for the partition has resolved. The barrier future is tagged with the assignment
epoch and discarded if the partition is revoked. The shard becomes `ACTIVE` when `position ≥
barrier`; `barrier ≤ cursor` short-circuits straight to `ACTIVE`. Rebalance callbacks take no
barrier snapshot — they do O(1) bookkeeping only.

## Consequences

- A `read_committed` consumer cannot pass an offset until every transaction below it resolves, so
  `position ≥ barrier` proves every record below the barrier is stable — committed COMPLETEs
  (including late, stalled-predecessor ones) are applied; aborted records are skipped. The wait is
  bounded by `transaction.timeout.ms` (30 s default).
- Load-bearing dependency on `read_committed` everywhere (the `require_stable` ordering —
  [ADR-0012](0012-locked-isolation-and-offset-reset.md)) and on the client behaviors of
  `listOffsets(READ_UNCOMMITTED)` and committed-offset fetch blocking through
  `UNSTABLE_OFFSET_COMMIT` (verified against the exact client at M5; design risk #2).
- The LSO-hazard and barrier-ordering (I8) integration tests are non-negotiable; getting either
  wrong silently reintroduces a duplicate window.
- Recovery cost is bounded separately by the v2 cursor
  ([ADR-0011](0011-committed-cursor-v2-position-plus-sidecar.md)); the barrier bounds *correctness*,
  the cursor bounds *cost*.
