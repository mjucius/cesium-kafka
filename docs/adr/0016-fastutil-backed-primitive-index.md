# ADR-0016: fastutil-backed primitive index

- **Status:** Accepted
- **Date:** 2026-06-06
- **Design reference:** [`../design.md`](../design.md) §5 (post-approval revision 1)

> This ADR records a **post-approval project-owner decision** that supersedes the specific
> hand-rolled data structures of design §5.2. All §5 *semantics and invariants* are unchanged; only
> the backing implementation and the capacity-planning constants move.

## Context

Design §5.2 specified the per-partition index as hand-rolled **chunked** primitive arrays
(`long[][]`/`int[][]` with power-of-two chunks, an intrusive free list, a binary heap with lazy
deletion, and a ring with a completed bitmap) to reach a ~32 B/entry nominal floor. That hits the
memory goal but is a large amount of bespoke, hazard-prone array bookkeeping to own and maintain.

## Decision

Implement the index with **fastutil primitive collections** (`fastutil-core`, in the
`cesium-kafka-store-kafka` module only):

- the entry pool as parallel `LongBigArrayBigList`s with an `IntArrayList` free-slot stack;
- the min-heap as an `IntHeapPriorityQueue` with an indirect comparator plus a thin
  lazy-deletion/rebuild wrapper;
- the arrival structure as an **append-only `IntBigArrayBigList` arrival log with a head pointer**
  (no ring wraparound) plus a `java.util.BitSet` of completed bits.

Every §5 semantic survives unchanged: lazy deletion, slot-lifetime == log residency, log binary
search instead of a hash map ([ADR-0010](0010-arrival-ring-binary-search-no-hash-map.md)), and
O(1)-drop revocation.

## Consequences

- Maintainability and familiar `Collection` interfaces in exchange for the last bytes/entry.
- Memory: power-of-two **doubling** growth replaces the bounded per-chunk slack, so live arrays can
  carry up to ~2× slack at an arbitrary fill. Capacity planning moves to **64 B/entry typical,
  80 B worst** (supersedes the pre-fastutil 48/64 figures), and the §11.4 JOL gate relaxes to
  **≤ 56 B/entry**.
- Measured: **40.54 B/entry @ 1 M** and **45.96 B/entry @ 10 M** (`ShardFootprintTest`) — well under
  the 56 B gate; *plan* with 64/80, *expect* ~40.
- The chunked design remains a contained, behavior-preserving swap if measurements at 10 M+ scale
  ever miss targets — the swap changes no semantics or invariants.
