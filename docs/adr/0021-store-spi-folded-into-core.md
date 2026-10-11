# ADR-0021: Store SPI folded into core; config surface trimmed

- **Status:** Accepted
- **Date:** 2026-10-08
- **Design reference:** [`../design.md`](../design.md) post-approval revision 5 (2.0.0); supersedes
  [ADR-0003](0003-sealed-two-archetype-store-spi.md); amends
  [ADR-0007](0007-ascii-decimal-control-headers.md),
  [ADR-0017](0017-kafka-4-floor-and-repo-only-publishing.md) and
  [ADR-0019](0019-2-0-stability-baseline.md)

## Context

ADR-0003 made the scheduler store pluggable: a sealed `SchedulerStore` root with two archetypes,
ServiceLoader discovery by `store.type`, and a published contract kit. By 2.0.0 none of that
pluggability was in use:

- The engine never supported the second archetype. `ExternalSchedulerStore` had no dispatch loop,
  and the app rejected any store that was not tracker-backed.
- One provider existed. The app already depended on `KafkaTrackerStore` at compile time, so the
  ServiceLoader lookup chose between one option.
- The engine never branched on `StoreCapabilities`, and nothing read `StoreContext.epoch()` or
  `clock()`. Most of `ConfigView` and `RouteDescriptor` was never read.
- The api and testkit jars are not published anywhere (ADR-0017, repo-only publishing).

The boundary also duplicated state and settings. Shard recovery state, the shard-epoch counter, the last
committed cursor offset and the penalty box were each tracked on both sides of it, the sidecar budget
had two config keys, and the heap-budget rule was written in two modules.

The same 2.0 review also marked these settings and features for removal: `ingest.workers` and `dispatch.workers` (the
latter already forced to 1), the startup heap-budget check, the PoC-era binary header decode, the
`-Dcesium.*` system-property overlay, the tracker ACL grant on topic creation, and `ingest.max-batch`.

## Decision

- `KafkaTrackerStore` is the only scheduler store. The `cesium-kafka-api`, `cesium-kafka-store-kafka`
  and `cesium-kafka-store-testkit` modules fold into `cesium-kafka-core`, keeping their package names.
  The contract kit becomes part of core's test suite.
- `ExternalSchedulerStore`, `StoreCapabilities`, `OwnershipEpoch`, `SchedulerStoreProvider` and the
  ServiceLoader registration, `StoreContext`, `ConfigView` and `RouteDescriptor` are removed. The engine
  constructs the store directly and passes its identity, settings and meter registry.
- The loops still reach the store through `TrackerBackedStore`, now an engine-internal interface with
  no stability promise. It stays because the loop unit tests script the store through it.
- `store.type` and `store.properties` are removed. The cursor sidecar budget is the single key
  `dispatch.cursor.sidecar-max-bytes`, and the recovery (H1) ceiling uses the engine's heap rule.
- Each process runs at most one ingest loop and one dispatch loop. `ingest.workers` and
  `dispatch.workers` are removed, and both halves scale by instances.
- The startup heap-budget check (`startup-checks.heap-budget`) is removed. The runtime caps already
  bound the index: AUTO `dispatch.max-pending-total` pauses ACTIVE shards at the same heap budget, and
  the H1 ceiling bounds RECOVERING shards.
- `headers.accept-binary-long-values` is removed. ASCII decimal is the only control-header encoding
  (amends ADR-0007).
- The `-Dcesium.*` overlay is removed. Configuration comes from defaults, YAML and `CESIUM_*`
  environment variables.
- cesium no longer grants the tracker ACL when it creates the tracker topic. It still verifies the
  ACL at startup and on every re-validation round (`startup-checks.tracker-acl`).
- `ingest.max-batch` is removed. The ingest consumer keeps its tuned `max.poll.records` default,
  which can be overridden through `kafka.ingest-consumer.properties`.
- Wire formats are unchanged: header protocol version 1, tracker value format `0x01`, the
  committed-cursor sidecar and the group-A identity blob byte for byte, the group ids, and worker 0's
  `transactional.id` (including its `.0` suffix) and `group.instance.id`.

## Consequences

- Writing a new store means changing the engine, and that change needs its own ADR.
- The dispatch loop and the store still each track shard recovery state, the shard epoch, the last
  committed cursor offset and the penalty box. With one module, merging them is possible, but 2.0
  does not do it.
- ADR-0019's api/testkit stability clause no longer applies, because those modules no longer exist.
  The supported product surface is the runnable app (ADR-0017), and `cesium-kafka-core` stays
  internal.
- A mixed 1.1.5/2.0.0 rolling restart is still safe, because no byte on the wire changed.
- Operators upgrading from 1.x must remove the deleted keys, which now fail startup as unknown keys
  (exit 78). They must also create the tracker ACL themselves before the first start.
- The cesium principal no longer needs `Cluster:ALTER`, because it never calls `createAcls`.
