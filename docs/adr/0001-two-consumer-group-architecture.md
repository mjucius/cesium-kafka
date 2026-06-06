# ADR-0001: Two-consumer-group architecture

- **Status:** Accepted
- **Date:** 2026-06-06
- **Design reference:** [`../design.md`](../design.md) §1.2–1.3, §3.3–3.4

## Context

cesium-kafka must consume a source topic, durably record per-message schedule state, and relay each
message at its due time with exactly-once delivery as observed by `read_committed` consumers of the
destination. The durable scheduler state and the transactional producer that advances it both need
broker-arbitrated fencing so that a paused/partitioned predecessor cannot double-deliver.

The PoC used a single consumer group with a per-partition `transactional.id` and a coarse
`ReadWriteLock` between the consume and dispatch sides. Under KIP-447 that pattern is broken: a
manually `assign()`ed consumer's `groupMetadata()` carries no valid generation, so transactional
offset commits lose group fencing, and Kafka 4.0 removed the
`sendOffsetsToTransaction(Map, String)` overload (KAFKA-12690) precisely because KIP-447 obsoleted
it. Alternatives considered: one group subscribed to both topics; Kafka Streams + RocksDB state
stores (duplicates payloads, no control of memory layout, punctuation fights wall-clock scheduling).

## Decision

Run **two consumer groups**:

- **Group A** `cesium.<applicationId>.ingest` `subscribe()`s the source; the ingest loop is
  stateless and produces immediate relays, tracker ADDs, and DLQ records inside one transaction per
  poll batch.
- **Group B** `cesium.<applicationId>.dispatch` `subscribe()`s the compacted tracker topic; tracker
  partition ownership *is* ownership of the in-memory index shard for that partition.

Both loops commit offsets only via `sendOffsetsToTransaction(Map, ConsumerGroupMetadata)` with
metadata from the live `consumer.groupMetadata()`, so both get native KIP-447 group-metadata
fencing. Group-less seek consumers (`assign()` + `seek()`) re-fetch payloads and commit nothing.
There is no shared mutable state between ingest and dispatch; the recovery path and the live-tailing
path are the same `onTrackerRecord` code, so recovery is exercised continuously.

## Consequences

- Both loops are fenced natively; `subscribe()` gives broker-arbitrated, exclusive index-shard
  ownership; the PoC's `ReadWriteLock` disappears structurally (see [ADR-0016](0016-fastutil-backed-primitive-index.md)).
- Ingest and dispatch scale independently via the `roles` config (separate fleets).
- Group B's committed offset doubles as the bounded-replay cursor — see [ADR-0011](0011-committed-cursor-v2-position-plus-sidecar.md).
- Cost: the tracker topic becomes internal durable state with its own lifecycle and ACL
  requirements ([ADR-0005](0005-tracker-format-compaction-only-and-tombstone-retention-floor.md)),
  and two groups present two rebalance surfaces (mitigated by static membership,
  [ADR-0013](0013-readiness-decoupled-from-recovery-and-static-membership.md)).
- The "unique committed ADD per (partition, source offset)" invariant from ingest atomicity
  underpins the ring sortedness of [ADR-0010](0010-arrival-ring-binary-search-no-hash-map.md).
