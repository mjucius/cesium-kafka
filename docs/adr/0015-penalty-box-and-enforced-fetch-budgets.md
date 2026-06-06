# ADR-0015: Penalty box + enforced fetch budgets

- **Status:** Accepted
- **Date:** 2026-06-06
- **Design reference:** [`../design.md`](../design.md) §7 (D8, D22; revisions R8, R9)

## Context

Because payloads are pointer-only ([ADR-0002](0002-pointer-only-payloads-and-retention-validation.md)),
the dispatch loop re-fetches each payload from the source at dispatch time. Three failure modes in
the PoC and the first design draft had to be closed:

- the PoC did one `seek` + one 10 ms `poll` per entry and **silently dropped** on a miss;
- `fetchAll` materialized a whole batch's payloads, so the byte bound was unenforceable at drain
  time (1 MB payloads × 10k batch = OOM — finding R8);
- one degraded source partition head-of-line blocked all shards through the single dispatch thread
  and hot-spun the loop (finding R9).

## Decision

A budgeted, partition-isolated batch fetch **outside** the transaction (D8, D22):

- **One seek + sequential forward scan per source partition** serves all of that partition's due
  entries (the midnight thundering-herd becomes one sequential pass, not 10k random seeks).
- **Three enforced budgets:** a per-partition time slice, an overall `dispatch.fetch.timeout` (30 s),
  and a **decompressed-byte budget** `dispatch.batch.max-bytes` (32 MiB) accumulated as records
  arrive — when it trips the batch is **truncated** (fetched entries proceed; unfetched return to
  pending). The bound is real because it is enforced exactly where record sizes become known.
- **Tri-state outcome** `FOUND | GONE | TRANSIENT`. `GONE` (provably expired) is resolved exactly
  once by the unfetchable policy (`dispatch.on-unfetchable-payload: DLQ | DROP | FAIL`, default DLQ)
  **inside the dispatch transaction** — the COMPLETE is always written in non-FAIL modes, so an
  unfetchable entry never replays forever (D-9).
- **Per-source-partition penalty box (D22):** a `TRANSIENT` outcome stamps a `not-before` deadline
  (exponential backoff `PT0.05S → PT10S`, reset on success) in one `long[]` indexed by partition;
  `drainDue` skips penalized entries even when due, and they contribute their penalty deadline to
  the poll timeout (no zero-timeout hot-spin).

Dispatch batch default is 10,000 entries.

## Consequences

- The fetch-path heap budget is enforceable and derivable
  (`brokers_in_flight × fetch.max.bytes × decompression_factor`); a 1 MB-record macro test asserts it
  holds.
- One degraded source partition can neither head-of-line block healthy partitions nor hot-spin the
  loop; metrics (`cesium_fetch_penalized_partitions`, `cesium_fetch_duration_seconds`,
  `cesium_fetch_bytes_total`) make degradation observable.
- Payload eviction is resolved exactly once and never silently lost, closing the PoC's silent-drop
  bug.
- Cold-segment fetches for long delays cost broker disk IOPS (ops guide budgets it); a partition-
  affine seek-consumer pool is a reserved v1.1 extension — the penalty box is the v1 isolation
  mechanism.
