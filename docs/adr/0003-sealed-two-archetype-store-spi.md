# ADR-0003: Sealed two-archetype store SPI

- **Status:** Accepted
- **Date:** 2026-06-06
- **Design reference:** [`../design.md`](../design.md) §4 (D7), §4.5

## Context

The scheduler store is pluggable, but a store's correctness depends on *how its durable writes relate
to the engine's Kafka transactions*. A store that encodes its mutations as Kafka records the engine
produces inside the ingest/dispatch transactions gets exactly-once for free; a store that writes to
an external system (a database) cannot enlist in those transactions and is at best at-least-once
unless it reconciles. The engine must know which model it is orchestrating to wire the dispatch loop
and to surface the resulting guarantee — it cannot discover this at runtime.

## Decision

`SchedulerStore` is a **`sealed interface`** permitting exactly two `non-sealed` archetypes:

- `TrackerBackedStore` — `KAFKA_TRANSACTIONAL` affinity, `EXACTLY_ONCE`; durable writes are tracker
  records the engine produces in its transactions, plus the offset-metadata cursor.
- `ExternalSchedulerStore` — `EXTERNAL` affinity, `AT_LEAST_ONCE` baseline, upgradeable to
  effectively-once via an optional offset-metadata reconciliation cursor.

The engine wires its dispatch loop with an **exhaustive Java 21 `switch`** over the two archetypes —
no `instanceof` chains. `capabilities()` declares affinity + dispatch guarantee (surfaced on
`/info`). Discovery is `SchedulerStoreProvider` via `META-INF/services` with **mandatory explicit
selection** (`store.type: kafka-tracker` or `store.type: class:...`); duplicate `typeId`s fail
startup. The published contract-test kit (`cesium-kafka-store-testkit`) is the executable
specification each archetype's implementers subclass.

## Consequences

- Degraded guarantees are explicit, not discovered in production.
- A new transaction-participation model is an SPI change (the root is sealed) and requires its own
  ADR — a deliberate, reviewable evolution gate.
- Additive evolution within an archetype is via interface `default` methods; wire-format versioning
  belongs to stores, not the SPI.
- `cesium-kafka-api` and `cesium-kafka-store-testkit` are the stable, semver-published surface for
  store implementers from 1.0 (see [ADR-0017](0017-kafka-4-floor-and-repo-only-publishing.md)); the
  `ExternalSchedulerStore` contract is finalized in the testkit as the executable spec for future
  DB-backed stores.
