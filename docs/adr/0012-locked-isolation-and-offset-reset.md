# ADR-0012: Locked isolation + auto.offset.reset + offsets-retention posture

- **Status:** Accepted
- **Date:** 2026-06-06
- **Design reference:** [`../design.md`](../design.md) §3.4 (D17, D18; revisions R5, R6)

## Context

Two Kafka consumer settings, if left as tuning knobs, silently destroy the guarantees:

- **`isolation.level`.** Under `read_committed`, a consumer's offset fetch sets the KIP-447
  `require_stable` flag, which gives the takeover ordering the replay barrier proof depends on
  ([ADR-0009](0009-high-watermark-replay-barrier-and-snapshot-ordering.md)). With
  `read_uncommitted`, a new ingest owner can fetch a stale stable offset while a zombie's accepted-
  but-uncommitted offsets are pending, reprocess, and let both copies commit — destroying both
  exactly-once relay and the unique-committed-ADD invariant — and it relays upstream-aborted
  records (finding R5).
- **`auto.offset.reset`.** If unspecified, KIP-211 committed-offset expiry after an outage longer
  than the broker's `offsets.retention.minutes` silently loses (`latest`) or mass-duplicates
  (`earliest`) (finding R6).

## Decision

`isolation.level=read_committed` and `auto.offset.reset=none` are **locked keys on every cesium
consumer** (ingest, tracker, seek) — rejected if supplied as passthrough, with an explanatory
message (D17, D18). Group B performs an *explicit* seek-to-beginning **only** when provably first
run (no committed offsets for the whole group anywhere). At startup the engine checks broker
`offsets.retention.minutes` against `startup-checks.max-tolerated-outage`; a missing committed
offset (`NoOffsetForPartitionException`) is a fail-fast with a runbook, so the reset point is always
an explicit operator decision.

## Consequences

- The `require_stable` takeover ordering holds, which is what makes the I8 barrier proof and the
  zombie-with-accepted-offset analysis sound.
- Neither silent skip nor mass duplication is possible after a long outage; the operator chooses
  the reset point.
- The exactly-once guarantee is therefore **conditional on committed-offset retention** — documented
  prominently in the delivery-semantics doc, alongside group B's correct-but-slow
  replay-from-beginning of the compacted tracker.
- These keys join the locked set (`group.id`, `transactional.id`, `enable.auto.commit`,
  `enable.idempotence`, serializers) the engine owns by construction.
