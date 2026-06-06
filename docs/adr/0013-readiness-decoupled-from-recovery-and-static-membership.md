# ADR-0013: Readiness decoupled from recovery + static membership

- **Status:** Accepted
- **Date:** 2026-06-06
- **Design reference:** [`../design.md`](../design.md) §9, §3.3 (D21, D10; revision R14)

## Context

A natural readiness definition — "ready when all shards are `ACTIVE`" — is a trap: it wedges
rolling deploys behind replay durations, and every rollout that moves partitions multiplies replay
work through rebalance churn (a returning member re-replays). Compounding this, a rolling restart
that moves partitions at all forces unnecessary replays. The instance is perfectly able to serve
already-`ACTIVE` shards while others recover (recovery is per-partition,
[ADR-0009](0009-high-watermark-replay-barrier-and-snapshot-ordering.md)).

## Decision

**Readiness is decoupled from shard recovery (D21).** Readiness = startup checks passed AND loops
alive AND consumers have assignments AND a recent poll. Per-shard recovery state is **explicitly
not** part of readiness — it is exposed instead through the `/health/ready` detail payload (per-shard
state + records remaining + ETA) and `cesium_shard_paused`. (The design-§9 `cesium_shard_state` /
`cesium_replay_remaining_records` gauges are deferred past M8 and not emitted in this release — see
[operations.md](../operations.md) §13.) Liveness is loop-heartbeat freshness + thread liveness.

**Static membership is default on (D10/D21):** `group.instance.id` is derived from the *required*
stable `instanceId` (which also seeds the transactional ids), so with `session.timeout.ms` greater
than pod restart time, a rolling restart **moves zero partitions** and replay happens only on the
returning member. The documented K8s recipe warns against HPA-ing dispatch-role fleets on CPU
(replay is CPU/network-heavy; scale-out mid-recovery triggers replay-multiplying rebalances).

## Consequences

- A healthily replaying instance is `ready`; rollouts are not wedged behind replay.
- Rolling restarts with static membership move zero partitions (integration-tested), so replay work
  is not multiplied by deploys.
- A `degraded` detail flag (with cause) surfaces park-and-degrade states
  ([ADR-0014](0014-in-doubt-commit-taxonomy.md)) without failing probes — degradation is observable
  but does not flap readiness.
- `instanceId` is required (a stable deployment-slot id; `random` is an explicit opt-in that trades
  crash-failover latency for convenience).
