# ADR-0010: Arrival-ring binary search (no hash map)

- **Status:** Accepted
- **Date:** 2026-06-06
- **Design reference:** [`../design.md`](../design.md) §5.2 (D6)

## Context

Replay and live tailing apply completion records (`COMPLETE`, keyed by source offset) and anomalous
duplicate ADDs, both of which require locating an existing entry by its source offset. The obvious
structure — a `long → slot` hash map — costs roughly 24 B/entry of resident overhead, and a
*transient* recovery map during a 10 M-entry replay would add ~240 MB on top of the ~32 B/entry
index. That overhead is the difference between hitting and missing the modest-heap goal.

The arrival ring (slot ids in tracker-arrival order) has a property worth exploiting: ingest
appends ADDs in source-offset order, Kafka preserves per-partition order, and the unique-committed-
ADD invariant ([ADR-0001](0001-two-consumer-group-architecture.md)) means there is exactly one ADD
per source offset — so the ring is **simultaneously sorted by tracker offset and by source offset**.

## Decision

Locate entries by **binary search over the arrival ring** — O(log n), zero per-entry map overhead
and no transient recovery map (D6). Sidecar-seeded entries are appended first during recovery (they
are the oldest, in encoded order), preserving sortedness. The ring also yields the oldest pending
entries off its head in O(1) amortized — exactly the greedy sidecar-encoding order
([ADR-0011](0011-committed-cursor-v2-position-plus-sidecar.md)).

A correctness invariant falls out: **slots are freed only when they leave the ring, not when
dispatched** (slot lifetime == ring residency). A slot freed and reused while still ring-referenced
would corrupt the source-offset ordering and silently break the binary search. A ring sweep
reclaims completed-but-head-pinned slots above a threshold.

## Consequences

- The ~32 B/entry nominal floor is reachable; completion lookup and duplicate-ADD handling cost
  O(log n) with no map.
- The slot-reuse hazard is real and is guarded by the ring-residency invariant (property-tested).
- Anomalous duplicate ADDs (replay rule R1) resolve via the same binary search: `dispatchAtMs` is
  updated in place while the original `trackerAddOffset` is kept (invariant I5).
- The structures are realized on fastutil big lists
  ([ADR-0016](0016-fastutil-backed-primitive-index.md)), which preserve all of these semantics —
  binary search over an append-only arrival log with a head pointer, lazy deletion, O(1)-drop
  revocation.
