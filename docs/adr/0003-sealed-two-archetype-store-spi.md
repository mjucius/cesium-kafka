# ADR-0003: Sealed two-archetype store SPI

- **Status:** Superseded by [ADR-0021](0021-store-spi-folded-into-core.md)
- **Date:** 2026-06-06
- **Amended by:** [ADR-0019](0019-2-0-stability-baseline.md) (2.0 stability baseline)
- **Design reference:** [`../design.md`](../design.md) §4 (D7), §4.5

## Context

The scheduler store is pluggable, but a store's correctness depends on *how its durable writes relate
to the engine's Kafka transactions*. A store that encodes its mutations as Kafka records the engine
produces inside the ingest/dispatch transactions gets exactly-once for free; a store that writes to
an external system (a database) cannot enlist in those transactions and is at best at-least-once
unless it reconciles. The engine must know which model it is orchestrating to wire the dispatch loop
and to surface the resulting guarantee. It cannot discover this at runtime.

## Decision

`SchedulerStore` is a `sealed interface` permitting two `non-sealed` archetypes:

- `TrackerBackedStore`: `KAFKA_TRANSACTIONAL` affinity, `EXACTLY_ONCE`; durable writes are tracker
  records the engine produces in its transactions, plus the offset-metadata cursor.
- `ExternalSchedulerStore`: `EXTERNAL` affinity, `AT_LEAST_ONCE` baseline, upgradeable to
  effectively-once via an optional offset-metadata reconciliation cursor.

The engine wires its dispatch loop with an exhaustive Java 21 `switch` over the two archetypes,
with no `instanceof` chains. `capabilities()` declares affinity + dispatch guarantee (surfaced on
`/info`). Discovery is `SchedulerStoreProvider` via `META-INF/services` with mandatory explicit
selection by `typeId` (`store.type: kafka-tracker`); an unknown or duplicate `typeId` fails
startup. The published contract-test kit (`cesium-kafka-store-testkit`) is the executable
specification each archetype's implementers subclass.

## Consequences

- A store declares its degraded guarantee up front, so it is not first discovered in production.
- A new transaction-participation model is an SPI change (the root is sealed) and requires its own
  ADR, which puts that change through review.
- Additive evolution within an archetype is via interface `default` methods; wire-format versioning
  belongs to stores, outside the SPI.
- `cesium-kafka-api` and `cesium-kafka-store-testkit` are the stable, semver-published surface for store
  implementers from 2.0.0 (ADR-0019; see [ADR-0017](0017-kafka-4-floor-and-repo-only-publishing.md)); the
  `ExternalSchedulerStore` contract is finalized in the testkit as the executable spec for future
  DB-backed stores.
- `StoreContext.epoch()` reports the dispatch group's (group B) ownership, so it can fence
  dispatch-side writes only; ingest (group A) may own the same partition elsewhere.

> **As shipped:** the external branch of the archetype `switch` has no dispatch loop yet: the app
> accepts a `TrackerBackedStore` and rejects any other archetype at startup.
