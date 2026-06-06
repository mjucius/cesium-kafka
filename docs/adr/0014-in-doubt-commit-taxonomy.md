# ADR-0014: In-doubt commit taxonomy

- **Status:** Accepted
- **Date:** 2026-06-06
- **Design reference:** [`../design.md`](../design.md) §3.8 (D20, invariant I9; revision R4)

## Context

When `commitTransaction` returns an ambiguous failure (`TimeoutException`, or any outcome-unknown
error), the broker **may have committed** — `PREPARE_COMMIT` can complete as a commit after the
client gives up. Treating that as "abort + restore the in-flight batch" double-delivers, because the
durable state may already contain the committed destination writes and completion tombstones.
`initTransactions()` on restart resolves the dangling transaction deterministically broker-side but
**does not report which way it went** (finding R4).

## Decision

A single **three-way taxonomy** governs every transactional failure (D20), with the load-bearing
**invariant I9**: an in-doubt commit *never* restores in-memory batch state.

1. **Definitively aborted** (`CommitFailedException` at `sendOffsetsToTransaction`, fenced-member
   offset commit, successful `abortTransaction`, abortable exceptions including
   `InvalidProducerEpochException` per KIP-588) ⇒ abort, **restore** popped entries, bounded retries
   with backoff.
2. **Fatal** (`ProducerFencedException`, `OutOfOrderSequenceException`, unrecoverable auth/config)
   ⇒ close clients, fail the worker, exit non-zero; the durable log is authoritative for the
   successor.
3. **In-doubt** (ambiguous `commitTransaction`) ⇒ **never restore (I9)**. Retry the commit to a
   definitive outcome (bounded by `kafka.transactions.commit-retry`); if still ambiguous, drop the
   in-memory shards for every touched partition, recreate the producer, `initTransactions()`, re-
   fetch committed cursors, and re-enter `RECOVERING` — the replay reconstructs the truth either
   way (cheap with the v2 cursor,
   [ADR-0011](0011-committed-cursor-v2-position-plus-sidecar.md)).

When definitively-abortable retries exhaust (e.g. destination below `min.insync.replicas` for
20 min), the loop **parks and degrades** — entries return to pending with a penalty not-before
([ADR-0015](0015-penalty-box-and-enforced-fetch-budgets.md)), membership stays alive, `cesium_degraded`
flips with cause, an alert fires — rather than crash-looping with a full replay per cycle.

## Consequences

- An ambiguous commit can never produce a duplicate via restore.
- Stores must keep committed-batch effects **recoverable purely from durable state**: the engine
  drops + re-recovers rather than restoring, so `onBatchAborted` is *never* called after an in-doubt
  commit. The store testkit asserts this (I9) for both archetypes.
- Correctness does not depend on the broker reporting the in-doubt outcome — the drop-and-replay
  fallback is sound regardless (design risk #3); the fast path (retry-to-definitive) merely avoids a
  recovery cycle.
- No fault path is a silent terminal state: every one ends in retry, park-and-degrade with an alert
  ([ADR-0013](0013-readiness-decoupled-from-recovery-and-static-membership.md)), or fail-fast with a
  runbook.
