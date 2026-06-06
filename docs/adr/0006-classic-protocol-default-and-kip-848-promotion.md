# ADR-0006: Classic protocol default + KIP-848 promotion criteria

- **Status:** Accepted
- **Date:** 2026-06-06
- **Design reference:** [`../design.md`](../design.md) §3.4.5 (D12), §15 risk #1

## Context

KIP-848 (the new `consumer` rebalance protocol) is GA in Kafka 4.0, and `ConsumerGroupMetadata`
carries the member epoch the engine's KIP-447 transactional-offset fencing
([ADR-0001](0001-two-consumer-group-architecture.md)) relies on. But transactional offset commits
under `group.protocol=consumer` have far less production mileage than the classic protocol, and the
exact member-epoch semantics must be re-verified against the pinned kafka-clients version. The
engine's rebalance handlers and per-partition shard state machine are written to be
delta-incremental and idempotent, so they are protocol-agnostic — classic and `consumer` share one
code path.

## Decision

**v1 defaults to `group.protocol=classic`** with `CooperativeStickyAssignor` (stickiness minimizes
replay churn) and static membership default on
([ADR-0013](0013-readiness-decoupled-from-recovery-and-static-membership.md)). The `consumer`
protocol is a **tested but non-blocking** configuration: a dedicated CI lane runs the EOS-critical
scenario set (restart-recovery, zombie fencing, LSO hazard, barrier-ordering, rebalance) under
`group.protocol=consumer` as a `continue-on-error` nightly job, so a 848 incompatibility never
masks a classic regression.

**Promotion criteria** (promotion to a gating lane, and later to the default, is a *new* ADR):
the consumer-protocol lane green and stable over a sustained nightly window on the EOS-critical
scenarios; member-epoch / `onPartitionsLost` semantics verified against the pinned client; no
EOS-relevant divergence from the classic lane.

## Consequences

- Classic is the supported, recommended default; `consumer` is exercised continuously but never
  blocks a v1 release.
- One protocol-agnostic shard state machine means promotion is a configuration and confidence
  change, not a rewrite.
- Promotion is a deliberate, reviewable gate (an ADR), keeping the EOS posture honest while the new
  protocol matures.
