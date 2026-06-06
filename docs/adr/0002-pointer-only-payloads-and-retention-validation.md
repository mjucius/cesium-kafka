# ADR-0002: Pointer-only payloads + source-retention validation

- **Status:** Accepted
- **Date:** 2026-06-06
- **Design reference:** [`../design.md`](../design.md) §1.1, §7, §7.6 (R5, R13)

## Context

Copying every delayed payload into scheduler state (or a changelog-backed store) duplicates data and
makes the memory goal — millions of pending entries in a modest heap — unreachable. The scheduler
only needs *when* to deliver and *where to find* each payload. But if payloads are not copied, they
must still exist at dispatch time, and a source topic can evict them by time
(`retention.ms`), by size (`retention.bytes`), or by tiered/remote storage long before a long delay
elapses. `retention.ms=-1` looks safe while `retention.bytes` quietly evicts in minutes.

## Decision

Store only the pointer `(sourcePartition, sourceOffset, dispatchAtMs[, clamped])` (~32 B/entry
nominal) and **re-fetch the payload from the source at dispatch time** via group-less seek
consumers. Guard payload availability explicitly:

- Startup + periodic validation of source `retention.ms` vs `delay.max + margin`
  (`startup-checks.retention: FAIL | WARN | SKIP`, default `FAIL`).
- `retention.ms=-1` is **not** a pass when `retention.bytes != -1` or tiered/remote storage is
  enabled: time-based validation cannot bound payload lifetime, so startup **fails** unless the
  operator sets the named acceptance `startup-checks.size-based-retention: ACKNOWLEDGED`.
- A runtime probe of each partition's *observed earliest-available record age* feeds
  `cesium_retention_margin_seconds` — honest under time-, size-, and tier-based eviction — with an
  alert before the loss class bites.
- Compacted source topics fail/warn (pointer-only storage cannot survive source compaction).
- Residual eviction races are resolved exactly once by the unfetchable-payload policy
  (`dispatch.on-unfetchable-payload: DLQ` default), inside the dispatch transaction —
  see [ADR-0015](0015-penalty-box-and-enforced-fetch-budgets.md).

## Consequences

- The ~32 B/entry nominal floor is achievable; no payload is ever duplicated.
- Dispatch-time fetches hit cold (non-page-cache) segments for long delays; per-partition forward
  scans and the penalty box ([ADR-0015](0015-penalty-box-and-enforced-fetch-budgets.md)) mitigate,
  and broker disk IOPS must be budgeted (operations guide).
- Payload eviction is an unrecoverable loss class **by design** — made explicit (named
  acknowledgment, honest margin gauge, DLQ loss-notice with a versioned JSON contract), never
  silent as in the PoC.
- Source-topic compaction is unsupported for routes that schedule delays.
