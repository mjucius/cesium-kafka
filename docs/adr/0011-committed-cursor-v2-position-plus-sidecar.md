# ADR-0011: Committed cursor v2 (position + pinned-entry sidecar)

- **Status:** Accepted
- **Date:** 2026-06-06
- **Design reference:** [`../design.md`](../design.md) §3.5 (D16, replay rule R1, revision R1)

## Context

Group B's committed `(offset, metadata)` per tracker partition *is* the bounded-replay cursor. A
naive cursor — the minimum still-pending ADD offset — makes replay cost scale with *completion
throughput × pin age*: one legitimate `delay.max`-aged entry on a 5k-dispatch/s route pins the
cursor for a day, and because every tombstone above the cursor is younger than the tombstone-
retention floor ([ADR-0005](0005-tracker-format-compaction-only-and-tombstone-retention-floor.md)),
compaction cannot thin the range. Replay would re-read billions of completion tombstones for a
single long-delay entry (adversarial-review finding R1, critical).

## Decision

**Cursor v2** = a position-tracking offset plus a **pinned-entry sidecar** in the offset metadata
(D16). The sidecar is a versioned, Base64 binary blob of varint-delta `(trackerAddOffset,
sourceOffset, dispatchAtMs)` triples plus an identity header (`{v, clusterId, sourceTopicId,
trackerTopicId}`). The engine greedily encodes the oldest pending entries (read off the ring head —
[ADR-0010](0010-arrival-ring-binary-search-no-hash-map.md)) until the budget
(`dispatch.cursor.sidecar-max-bytes`, default 3 KiB) is exhausted, then:

- all pending encoded ⇒ `cursorOffset = position(p)`, sidecar = full pending set;
- overflow ⇒ `cursorOffset = trackerAddOffset` of the first non-encoded pending entry (the
  min-pending watermark **as a fallback**).

Recovery seeds the index from the sidecar, then replays `[cursorOffset, barrier)` applying R1/R2.
**Invariant I5** (asserted before every commit): the offset is monotonic, and every pending entry
either has `trackerAddOffset ≥ cursorOffset` or rides the sidecar.

## Consequences

- Replay cost ≈ sidecar decode + traffic since the last successful commit (+ downtime traffic) —
  decoupled from how long any single entry has been pending, in the common case.
- **Overflow residual is honest:** a route whose steady state pins more than ~200–300 long-delay
  entries per partition reverts to `completion_rate × age(cut) + pending` replay; surfaced by
  `cesium_pinned_entries` saturation + a replay-ETA alert, and tuned by raising the sidecar budget
  (validated `≤ broker offset.metadata.max.bytes`).
- The tombstone-retention floor remains correctness-relevant for the overflow fallback (cost is
  bounded by this cursor; *correctness* by the floor + the HW barrier
  [ADR-0009](0009-high-watermark-replay-barrier-and-snapshot-ordering.md)).
- Because the cursor tracks position, standard consumer-lag tooling reads group B approximately
  correctly in steady state; only overflow mode shows inflated lag (documented).
- The external archetype mirrors this idea: an offset-metadata reconciliation cursor upgrades it
  from at-least-once to effectively-once
  ([ADR-0003](0003-sealed-two-archetype-store-spi.md); finalized in the store testkit).
