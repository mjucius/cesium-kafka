# Operations guide

Running cesium-kafka in production: topic bootstrap, the tracker sizing worksheet, the tracker
settings that correctness depends on, disaster recovery, scaling, the Kubernetes rollout recipe,
JVM/GC and broker IOPS budgeting, alerting, and a runbook for every fail-fast exit.

Every config key, metric name, and runbook action here is checked against the code (the
`cesium-kafka-core` config records, the Micrometer registrations, and the fail-fast messages).
Section numbers (`§3.6`, `§5.4`, ...) point into [docs/design.md](design.md), which holds the design
rationale. Companion docs:
[configuration.md](configuration.md) (full key reference), [performance.md](performance.md) (measured
numbers, memory worksheet, GC, seek-fetch I/O), [header-protocol.md](header-protocol.md) (wire
contract).

**Contents**

1. [Topics and bootstrap (CREATE vs FAIL)](#1-topics-and-bootstrap-create-vs-fail)
2. [The tracker sizing worksheet](#2-the-tracker-sizing-worksheet)
3. [Tracker settings that correctness depends on](#3-tracker-settings-that-correctness-depends-on)
4. [Lowering `delay.max` (runbook)](#4-lowering-delaymax-runbook)
5. [Least-privilege deployment: TLS, SASL, and the ACL matrix](#5-least-privilege-deployment-tls-sasl-and-the-acl-matrix)
6. [Tracker disaster recovery (runbook)](#6-tracker-disaster-recovery-runbook)
7. [Offsets retention vs outage tolerance](#7-offsets-retention-vs-outage-tolerance)
8. [Scaling roles](#8-scaling-roles)
9. [Growing partitions (runbook)](#9-growing-partitions-runbook)
10. [Kubernetes rollout recipe](#10-kubernetes-rollout-recipe)
11. [JVM, heap, and GC](#11-jvm-heap-and-gc)
12. [Broker IOPS budgeting for seek-fetch](#12-broker-iops-budgeting-for-seek-fetch)
13. [Metrics and alerting](#13-metrics-and-alerting)
14. [Fail-fast runbook (every exit)](#14-fail-fast-runbook-every-exit)

---

## 1. Topics and bootstrap (CREATE vs FAIL)

cesium serves one route per process: a user-owned source and destination, an internal tracker, and
an optional DLQ.

| Topic | Owned by | Provisioning |
|---|---|---|
| `route.source.topic` | you | Create it yourself. Must not be compacted (offsets must stay fetchable); checked at startup. |
| `route.destination.topic` | you | Create it yourself. Consumers must use `read_committed`. |
| `route.tracker.topic` | cesium | Provisioned by bootstrap (below). Default name `cesium.<application-id>.tracker`. |
| `route.dlq.topic` | you / cesium | Required whenever any policy routes to DLQ (the defaults do). |

### Tracker bootstrap modes (`route.tracker.bootstrap`)

- **`CREATE`** (default): cesium creates the tracker topic if absent, with the `§2.1` configs
  (compaction-only cleanup, the tombstone-retention floor, the compaction-lag floor, ~1 h
  `segment.ms`, `LogAppendTime`), mirroring the source partition count, and applies the write ACL
  when `route.tracker.acl-principal` is set. After creating it, cesium waits (bounded, up to 10 s) for
  the topic to become describable: `createTopics` is acknowledged by the KRaft controller, but the
  broker that answers the next describe publishes that metadata asynchronously, so a freshly created
  topic can be briefly invisible on a healthy cluster. A wait above 1 s is logged as a startup
  warning ([ADR-0018](adr/0018-bounded-wait-for-proven-topic-metadata.md)).
- **`FAIL`**: cesium requires a pre-provisioned tracker topic and validates its configs,
  failing fast on any drift (wrong partition count, wrong `cleanup.policy`, `delete.retention.ms`
  below the floor, `min.compaction.lag.ms` below the floor). Use `FAIL` where topic creation is
  centrally controlled; provision the topic to match §2.1 and §3 below.

> The tracker topic's **partition count MUST equal the source's**: every tracker record for source
> partition *p* lands on tracker partition *p*. This is validated at startup (a mismatch is a
> fail-fast). At runtime, ingest fails fast on the first record from a source partition that has no
> tracker partition (I-7). Re-validation also re-checks parity every
> `startup-checks.revalidation-interval`; a mismatch found there degrades readiness and alerts
> (`cesium_revalidation_errors`) instead of exiting (§9, §13, ADR-0020).

The DLQ topic's existence is validated whenever a policy routes to it. Create it with a partition
count that suits your DLQ-drain tooling (it need not match the source).

---

## 2. The tracker sizing worksheet

The tracker topic holds all of the system's durable scheduler state. Its disk use is dominated by
retained completion tombstones, which the PoC's "26 B × pending" estimate left out, undercounting by
~3000× (R10). Pending entries are a smaller term.

### Formula

```
tracker_bytes_per_replica ≈ pending × ~70 B                                  (pending ADDs)
                          + completion_rate_per_s × retention_window_s × ~64 B   (retained tombstones)
                          + uncleaned_tail                                    (active, not-yet-compacted segments)

provisioned_disk = tracker_bytes_per_replica × replication_factor × safety_margin
```

- `pending`: entries scheduled but not yet delivered ≈ `arrival_rate × average_delay`.
- `completion_rate_per_s`: entries completed per second (≈ the delayed-scheduling rate in steady
  state; each completion writes one ~64 B tombstone).
- `retention_window_s`: `delete.retention.ms / 1000`, with a floor of `2 × delay.max` (§3). With
  the default `delay.max = P1D`, the floor is `2 days = 172,800 s`.
- `uncleaned_tail`: roughly the active segments since the last clean (governed by `segment.ms ≈ 1 h`
  and `min.compaction.lag.ms`). A modest additive term.
- `replication_factor`: typically 3, which multiplies on-disk bytes 3×.
- Note self-consumption (the dispatch loop reads its own tombstones, ~2× read bandwidth) affects
  *throughput*, not disk.

### Why `delay.max` drives disk

The retained-tombstone term is `completion_rate × retention_window × 64 B`, and `retention_window ≥
2 × delay.max`, so doubling `delay.max` roughly doubles tracker disk. That is why `delay.max`
defaults to `P1D` instead of the original P7D, and why raising it is an explicit, worksheet-reviewed
decision (D14).

### Worked examples (default `delay.max = P1D` ⇒ `retention_window = 172,800 s`)

Per-replica tombstone term = `completion_rate × 172,800 s × 64 B`:

| Completion rate | Retained tombstones | Tombstone bytes / replica | × replication 3 |
|---|---|---|---|
| 1,000 msg/s | 1.7×10⁸ | ~11 GB | ~33 GB |
| 10,000 msg/s | 1.7×10⁹ | ~110 GB | ~330 GB |
| 100,000 msg/s | 1.7×10¹⁰ | ~1.1 TB | ~3.3 TB |

Add the pending term. Example: a 10,000 msg/s route where the average delay is 1 h holds
`10,000 × 3,600 ≈ 3.6×10⁷` pending entries × 70 B ≈ 2.5 GB/replica, against ~110 GB for the
tombstone term. The tombstone term dominates by one to two orders of magnitude.

### The `delay.max = P7D` contrast (why the default dropped)

Raise `delay.max` to P7D on the same 10,000 msg/s route and `retention_window` becomes
`14 days = 1,209,600 s`:

`10,000 × 1,209,600 × 64 B ≈ 774 GB/replica ≈ ~1 TB`, or ~2.3 TB at replication 3 (risk #11). The
delay ceiling sets the disk bill, more than message size or pending count do.

### Sizing actions

- Choose `delay.max` on purpose. If you need long delays on a busy route, run this worksheet and
  provision disk accordingly.
- Provision tracker disk for `provisioned_disk` with headroom for the uncleaned tail and bursty
  completion rates. Alert on tracker-topic disk growth through your broker disk/partition-size
  monitoring ([§13](#13-metrics-and-alerting) lists the in-process signals).
- Keep `segment.ms ≈ 1 h` and `min.compaction.lag.ms` at the floor so the cleaner collects
  tombstones; oversized segments delay collection and inflate disk.

### No in-process rate limiting; bound the DLQ

cesium has no per-tenant rate limiting: ingest has no per-producer, per-key, or per-partition quota
or fairness. A flood or far-future delay-bomb grows the tracker (and, for malformed records, the DLQ)
on disk, bounded only by controls that sit in front of cesium. Rely on:

- broker client/produce quotas on the source-producer principals;
- source `retention.bytes` to bound what a flood can accumulate (size/tier eviction interacts with
  the payload-lifetime check, `startup-checks.size-based-retention`, [§14.2](#142-startup-validation-against-the-cluster-exit-1));
- per-tenant topic isolation (a route per tenant instead of one shared source);
- a considered `delay.max`, which multiplies tracker disk through the `2 × delay.max`
  tombstone-retention floor (this worksheet).

**Bound the DLQ's retention yourself.** Startup checks that the DLQ topic exists and logs a warning
only when its retention is fully unbounded (`retention.ms=-1` and `retention.bytes=-1`); it does not
check that a bound fits your drain cadence. A malformed-record flood (default `DLQ` policy copies the full original payload 1:1) can
fill an unbounded DLQ. Set `retention.ms`/`retention.bytes` on the DLQ topic to match your drain
cadence.

---

## 3. Tracker settings that correctness depends on

cesium validates these at startup and re-validates them every `startup-checks.revalidation-interval`,
where a violation degrades readiness and alerts instead of exiting ([§13](#13-metrics-and-alerting)).
Getting them wrong can cause a duplicate or silent loss.

| Setting | Required value | Why it matters |
|---|---|---|
| `cleanup.policy` | `compact` (never `compact,delete`) | `compact,delete` would delete a still-pending ADD older than retention → silent loss (D4). Compaction-only means a lone pending ADD can never expire. |
| `delete.retention.ms` | **`≥ 2 × max(delay.max, observed oldest-pending age, committed-cursor age)`** | In replay overflow-fallback mode, a replayer's cursor can sit as far back as the oldest pending entry. If a completion tombstone it needs was already deleted, the entry looks pending → duplicate. The `delay.max` term is a startup FAIL. On dispatch instances, re-validation checks the committed-cursor age against `delete.retention.ms / 2 + 1h` and degrades + alerts on a breach; the oldest-pending term is covered by it, since a replay starts at the cursor (ADR-0020, [§4](#4-lowering-delaymax-runbook)). |
| `min.compaction.lag.ms` | `≥ max(2 × transaction.timeout.ms, 1 h)` | Keeps the cleaner away from the active tail / LSO region. Below the floor is a startup FAIL. |
| `segment.ms` | `≈ 1 h` | So cleaning happens. Larger is a WARN (delays tombstone collection, grows disk). |
| `message.timestamp.type` | `LogAppendTime` | So tracker record timestamps reflect append order. Otherwise a WARN. |

> **`delete.retention.ms` is the single most important tracker setting.** Never lower it below
> `2 × delay.max`. To shrink it, first shrink `delay.max` and drain ([§4](#4-lowering-delaymax-runbook)).

The startup failure when it is too low (from `StartupValidator`):

> *tracker topic '…' has delete.retention.ms=… ms, below the tombstone-retention floor
> 2 x delay.max = … ms (D14, §3.7, R-8): a replayer in overflow-fallback mode may need tombstones as
> old as the oldest pending entry, and cleaning them earlier converts a replay into a duplicate.
> Raise delete.retention.ms to >= … ms, or lower delay.max (drain entries scheduled under the old
> maximum first — see the lowering-delay.max runbook).*

---

## 4. Lowering `delay.max` (runbook)

Lowering `delay.max` and shrinking `delete.retention.ms` together can strand pending entries that
were scheduled under the old, larger maximum: their tombstones could be cleaned before a replay
reads them (R20). You have to drain before you can shrink retention. Re-validation watches the
committed-cursor age (ADR-0020), but the drain below is what keeps you out of that state.

1. **Lower `delay.max` in config and restart.** New records are now capped at the lower ceiling.
   Older pending entries (scheduled under the previous maximum) remain valid and are still honored.
2. **Do NOT lower `delete.retention.ms` yet.** Leave it at the floor for the old `delay.max`.
3. **Wait out (drain) the entries scheduled under the old maximum.** Wait at least the old
   `delay.max`, confirming via the per-partition pending signal (`cesium_pending_entries`) that the
   long-tail entries have been delivered.
4. **Only then lower `delete.retention.ms`** to the floor for the new `delay.max`
   (`2 × new delay.max`), and restart so startup re-validates.

If you skip the drain, startup passes (it checks only `2 × delay.max`). Re-validation then degrades
once a committed cursor is older than `delete.retention.ms / 2 + 1h`, which is the overflow-mode state
in which a replay can deliver a duplicate. Raise `delete.retention.ms` again if that happens; it
applies live.

---

## 5. Least-privilege deployment: TLS, SASL, and the ACL matrix

cesium delegates authentication and authorization to the broker: it opens ordinary Kafka clients
and the broker decides what each may do. [SECURITY.md](../SECURITY.md) is the authoritative
secure-deployment guide; this section repeats the operational points.

### Require TLS + SASL in production

Set `security.protocol` (`SSL` or `SASL_SSL`) and a SASL mechanism through the `kafka.properties`
passthrough (applied to every cesium client) so the broker can identify cesium as one principal. Keep
secrets out of the config file by referencing them with `${env:VAR}` (e.g.
`sasl.jaas.config: ${env:KAFKA_JAAS_CONFIG}`, `ssl.truststore.password: ${env:KAFKA_TRUSTSTORE_PASSWORD}`).
The [PLAINTEXT quickstart compose](../config/docker-compose.yaml) is a local demo only, not a
production setup.

### Least-privilege ACLs for all six client roles

All six cesium clients authenticate as a single cesium principal (`User:cesium`). Grant it these ACLs
and nothing else (substitute your `application-id` for `<app>`). `DESCRIBE` is implied by
`READ`/`WRITE` on the same resource; idempotent producer writes are authorized by topic `WRITE` in
Kafka ≥ 3.0; a transactional producer that commits offsets needs `READ` on the consumer `Group`.

| Client role | Resource (type · name/pattern) | Ops |
|---|---|---|
| Ingest consumer (group A) | `Group` `cesium.<app>.ingest` (literal); `Topic` *source* | READ; READ |
| Dispatch consumer (group B) | `Group` `cesium.<app>.dispatch` (literal); `Topic` *tracker* | READ; READ |
| Seek consumer (group-less) | `Topic` *source* | READ (**no `Group` ACL**) |
| Ingest txn producer | `TransactionalId` `cesium.<app>.ingest.*` (prefixed); `Topic` *tracker* / *destination* / *DLQ*; `Group` `cesium.<app>.ingest` | WRITE; WRITE; READ |
| Dispatch txn producer | `TransactionalId` `cesium.<app>.dispatch.*` (prefixed); `Topic` *destination* / *tracker* / *DLQ*; `Group` `cesium.<app>.dispatch` | WRITE; WRITE; READ |
| Admin client | `Cluster` (DESCRIBE + DESCRIBE_CONFIGS: broker configs, tracker ACL check); `Topic` *source/dest/tracker/DLQ* (DESCRIBE + DESCRIBE_CONFIGS); `Group` both (DESCRIBE: identity check, first-run probe, the dispatch group's cursor-age read); **`CREATE` bootstrap only:** `Cluster` CREATE + ALTER. The checks run at startup and every `startup-checks.revalidation-interval`; re-validation needs no extra grant (`listOffsets` by timestamp needs only Topic DESCRIBE) | per cell |

> **The `TransactionalId` and `Group` ACLs are required (M3).** cesium's transactional ids
> (`cesium.<app>.<role>.<instance>.<ordinal>`) and `group.instance.id`s
> (`cesium.<app>.<role>.<instance>`, plus `.<ordinal>` for workers after the first) are deterministic. The `application-id` seed is easy to infer
> (it names the consumer groups and the default tracker topic) and appears on the unauthenticated
> `/info` when `observability.detailed-info: true`. Without a `TransactionalId` ACL scoping
> `cesium.<app>.*` to the cesium principal, any authenticated co-tenant can open a producer with
> cesium's transactional id and fence it into a `ProducerFencedException` → fatal exit → restart →
> re-fence crash-loop (and likewise `FencedInstanceIdException` via the static group id). The tracker
> topic ACL alone is not enough. Under `route.tracker.bootstrap: FAIL` the cesium principal does not
> need `Cluster` CREATE/ALTER; provision the tracker topic and its write ACL out of band.

**Topic ownership** (restrict `WRITE` accordingly): your upstream producers write the source
(cesium only reads it); cesium writes the destination (downstream consumers read it with
`read_committed`); cesium writes the DLQ (your drain tooling reads it); cesium alone writes and
reads the tracker.

### Tracker write ACL (the R12 control)

Restricting tracker write access to the cesium principal is a normative deployment requirement
(R12). The tracker topic is the durable scheduler state:

- a forged ADD record lets an attacker inject duplicates at will;
- a forged completion tombstone lets an attacker cause data loss.

Actions:

- Set `route.tracker.acl-principal: User:<cesium-principal>`. With `route.tracker.bootstrap: CREATE`
  and a cluster authorizer present, cesium grants that principal write/read/describe on the tracker
  topic at bootstrap. (If the cluster has no authorizer, cesium logs that the ACL could not be
  applied; provision it yourself. Under `FAIL` bootstrap mode cesium does not apply the ACL;
  provision it out of band.)
- Ensure no other principal has write access to the tracker topic.

**The invalid-records counter is not a forgery detector.** cesium counts structurally malformed or
version-skewed tracker writes as `cesium_tracker_invalid_records_total` (logged at WARN); a nonzero
rate flags a malformed/version-skew foreign writer ([§13](#13-metrics-and-alerting)). It does not
detect well-formed forgeries: a wire-format-aware adversary with tracker write access can craft an ADD
or tombstone that decodes cleanly and is applied as legitimate state, and this counter never moves
(L2). The write-restricting ACL is the control. Detecting competent tampering requires broker
authorizer audit logging (who wrote the tracker) or the reserved, deferred `store.kafka.hmac.*`
record-level HMAC (config namespace reserved, not implemented; it only helps in hostile
clusters where cesium's own credentials are already suspect).

### Untrusted-producer ingress: avoid the `FAIL` delay policies (L5)

`delay.on-malformed-header: FAIL` and `delay.on-over-max: FAIL` (both non-default) are unsafe when
the source topic has untrusted producers. One crafted record fatally stops ingest, tears down all
workers, and exits non-zero. The offset never advances and `auto.offset.reset=none` is locked, so
restart re-polls and re-fails: a pipeline-wide outage that survives restarts. Keep the `DLQ` defaults
(or `RELAY_IMMEDIATE`) for multi-tenant ingress. See [configuration.md §5](configuration.md#5-delay-protocol-delay).

### Observability port is unauthenticated (L1)

The observability HTTP server ([§13](#13-metrics-and-alerting); default port 8081) is read-only but
unauthenticated, and it binds all interfaces by default (`observability.bind-address: 0.0.0.0`) so
k8s probes reach it. It caps accepted connections (`jdk.httpserver.maxConnections`, default 64) on
top of the slow-client reaper (M1). **It MUST be network-restricted.** Set
`observability.bind-address: 127.0.0.1` to restrict it to a loopback sidecar scrape (recommended when
off-host probes are not required), or keep `0.0.0.0` and apply a NetworkPolicy scoping port 8081 to the
monitoring namespace (the scraper) only. By default `/info` returns only `version`, `gitCommit`, and
the store type; `observability.detailed-info: true` adds the `application-id` (the seed of the fencing
ids above), roles, store capabilities, and acknowledgments. Leave it off unless the port is
restricted (M3).

See also [SECURITY.md](../SECURITY.md).

---

## 6. Tracker disaster recovery (runbook)

The tracker topic must never be silently recreated or truncated: an `earliest` reset on an empty
topic looks healthy after all pending state is gone. cesium guards against this:
committed-offset identity (cluster id, source/tracker topic ids) and offset-range sanity are checked
at startup and at each partition takeover, and it never auto-resets into an empty or out-of-range log.

**Symptoms / triggers:**

- Startup or runtime fail-fast: *tracker offset out of range … the tracker topic was truncated or
  recreated under the same name (§3.6 integrity, R-9). Never auto-reset — follow the tracker DR
  runbook.*
- Topic-ID mismatch fail-fast.
- A step-collapse of `cesium_pending_entries` (the integrity canary, R-9).

**If the tracker was lost (deleted / recreated / truncated):** the durable pending state is gone,
and individual pending entries cannot be reconstructed from a destroyed tracker. Choose one as an
explicit operator decision (cesium will not guess):

- **Accept the loss and re-bootstrap.** Recreate the tracker topic (`CREATE` mode), accept that
  entries pending at the moment of loss are not delivered, and resume. New scheduling works
  immediately.
- **Replay from the source if you can.** If your source topic still retains the relevant records and
  you can re-feed them (re-produce, or reset the ingest group to a point that re-ingests the affected
  window), the entries are re-scheduled. This risks delivering already-delivered records, so only do
  it if a duplicate is acceptable for the affected window.

After recovery, audit why the tracker was lost (an out-of-band topic deletion, a cluster restore from
backup, an ACL gap, ...) and close the gap. An intentional same-id cluster restore from backup is
undetectable (risk #12) and is the one case where you must reason about correctness manually.

---

## 7. Offsets retention vs outage tolerance

cesium locks `auto.offset.reset=none` on both consumer groups (D18). After an outage longer than the
broker's `offsets.retention.minutes`, the committed offsets expire (KIP-211) and cesium fails fast.
The alternatives are silent loss (a `latest` reset) or mass duplication (an `earliest` reset).

- Set `startup-checks.max-tolerated-outage` (default `P7D`) to the longest outage you must survive.
  Startup checks it against the broker's `offsets.retention.minutes` and FAILs (or WARNs, per
  `startup-checks.outage-check`) if the broker would expire offsets sooner.
- Ensure broker `offsets.retention.minutes` ≥ your `max-tolerated-outage`. This is usually the
  fix: raise the broker setting (it defaults to 7 days on many clusters) to cover your worst planned
  downtime.
- The first run is special: see the [first-run / offset-reset runbook](#143-runtime-fail-fast-exit-1-readiness-false).

---

## 8. Scaling roles

`roles` selects which loops a process runs (`[ingest, dispatch]` by default). The two halves scale
independently on separate fleets:

- **Ingest fleet** (`roles: [ingest]`): scales with source ingest throughput. The loops are
  stateless; parallelism is useful up to the source partition count.
- **Dispatch fleet** (`roles: [dispatch]`): scales with dispatch + re-fetch throughput and owns the
  in-memory index shards. Parallelism is useful up to the tracker partition count fleet-wide.

Per-instance ingest parallelism is `ingest.workers` (default 1). Dispatch runs one worker per
process (`dispatch.workers` must be 1), so scale dispatch by adding instances. Throughput is
ultimately bounded by transaction-commit latency per worker and, for dispatch, by seek-fetch I/O
([§12](#12-broker-iops-budgeting-for-seek-fetch)). Prefer scaling out with more instances (more partition
owners) over raising `ingest.workers`.

> **Do not autoscale the dispatch fleet on CPU.** See the HPA warning in [§10](#10-kubernetes-rollout-recipe).

---

## 9. Growing partitions (runbook)

The tracker partition count must equal the source partition count. Growing the source without
first growing the tracker halts ingest: a source record on a new partition has no tracker partition
to land its ADD on.

**Grow the tracker first:**

1. Increase the **tracker** topic partition count to the target.
2. Increase the **source** topic partition count to the same target.
3. (If using `route.relay.partitioning: SOURCE_PARTITION`, grow the destination to match too; it is
   validated equal at startup.)

In this order, producers never reach a new source partition before its tracker
partition exists. The startup check fails fast on a mismatch, and at runtime ingest fails fast on
the first record from a source partition with no tracker partition (I-7, R-7). Between steps 1 and 2
the counts differ, and re-validation reports that as an R-7 error: readiness shows degraded and
`cesium_revalidation_errors` is non-zero until the source catches up. It never exits on drift. Adding partitions changes key→partition hashing for new
records; existing pending entries keep their original partition.

---

## 10. Kubernetes rollout recipe

The readiness model and static membership exist to make rollouts safe (D21, R14). Two rules make
rolling restarts move zero partitions and avoid churn that multiplies replay.

### Static membership + a session timeout that outlives a pod restart

cesium derives `group.instance.id` from `instance-id` (static membership, default on). Give each pod
a stable `instance-id` and set the consumer `session.timeout.ms` greater than a pod's restart
time. A restarting pod then rejoins as the same member and reclaims its own partitions, with no
rebalance and no partition movement; replay happens only on the returning member.

Use a `StatefulSet` so pod names (and thus the instance id) are stable:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: cesium
spec:
  serviceName: cesium
  replicas: 3
  podManagementPolicy: Parallel
  selector:
    matchLabels: { app: cesium }
  template:
    metadata:
      labels: { app: cesium }
    spec:
      terminationGracePeriodSeconds: 60     # >= shutdown drain time
      containers:
        - name: cesium
          image: cesium-kafka:2.0.0
          ports:
            - { name: observability, containerPort: 8081 }
          env:
            # Stable per-slot instance id from the stable pod name (cesium-0, cesium-1, ...):
            - name: CESIUM_INSTANCE_ID
              valueFrom: { fieldRef: { fieldPath: metadata.name } }
            - name: CESIUM_KAFKA__PROPERTIES__BOOTSTRAP_SERVERS
              value: kafka:9092
            # session.timeout.ms must exceed a pod restart; raise broker
            # group.max.session.timeout.ms if needed.
            - name: CESIUM_KAFKA__TRACKER_CONSUMER__PROPERTIES__SESSION_TIMEOUT_MS
              value: "90000"
            - name: CESIUM_KAFKA__INGEST_CONSUMER__PROPERTIES__SESSION_TIMEOUT_MS
              value: "90000"
          resources:
            limits:   { memory: "3Gi" }     # heap sized from this by MaxRAMPercentage (§11)
            requests: { memory: "3Gi" }
          # Readiness is decoupled from shard recovery (D21): a replaying instance IS ready.
          readinessProbe:
            httpGet: { path: /health/ready, port: 8081 }
            periodSeconds: 5
            failureThreshold: 3
          livenessProbe:
            httpGet: { path: /health/live, port: 8081 }
            periodSeconds: 10
            failureThreshold: 6             # generous: never kill a healthy slow-replaying pod
          lifecycle:
            preStop:
              # Delay SIGTERM so endpoint removal propagates; on SIGTERM readiness flips false
              # and the loops drain (engine shutdown budget 30 s).
              exec: { command: ["sh", "-c", "sleep 5"] }
  updateStrategy:
    type: RollingUpdate
```

Key points:

- Readiness is not gated on recovery. A healthily replaying instance reports ready. Gating
  rollouts on replay completion wedges deploys behind replay durations and multiplies replay work.
  Recovery progress shows in the `/health/ready` detail payload, `cesium_shard_state` and
  `cesium_replay_remaining_records`; the probe does not fail.
- Liveness is loop-heartbeat freshness, with a generous `failureThreshold` so a slow but healthy
  replay is never killed.
- `preStop` delays SIGTERM; on SIGTERM readiness flips false and the loops finish/abort their open
  transaction at a batch boundary before consumers close (graceful, fenced shutdown, bounded at
  30 s). Keep `terminationGracePeriodSeconds` above the `preStop` delay plus 30 s.
- A rolling restart moves zero partitions because the returning member reclaims its own assignment
  within the session timeout.
- Restrict the observability port (8081). It is unauthenticated; add a NetworkPolicy scoping
  ingress on 8081 to the monitoring namespace (the Prometheus scraper) and the kubelet probe source
  only. See [§5](#5-least-privilege-deployment-tls-sasl-and-the-acl-matrix) / [§13](#13-metrics-and-alerting) (L1).

### HPA warning

**Do NOT autoscale the dispatch role on CPU.** Replay is CPU- and network-heavy, so a dispatch-role
HPA scaling out during recovery triggers a rebalance that multiplies replay work and can cascade.
Size the dispatch fleet to the tracker partition count and change it by hand. (An ingest-role HPA on
lag is fine, since ingest is stateless.)

---

## 11. JVM, heap, and GC

Full detail and the memory worksheet are in [performance.md §5–§6](performance.md). Summary:

- **Heap sizing.** Plan the index at 64 B/entry (typical) / 80 B (worst); expect ~40 B
  measured. Recommended heap by scale:

  | Pending entries | Recommended heap | GC |
  |---|---|---|
  | 1 M | 512 MB – 1 GB | G1 (default) |
  | 10 M | 2 – 3 GB | G1 |
  | 100 M | 8 – 12 GB | ZGC generational |

  Add the non-index consumers (producer `buffer.memory` 64 MB × 2, ingest fetch buffers, the
  seek-fetch budget `dispatch.batch.max-bytes` 32 MiB plus one in-flight fetch response per broker ×
  decompression factor) and +30 % G1 / +20 % ZGC headroom. Container memory = heap + ~1 GB native.

- **Container flags.** The image sets `-XX:MaxRAMPercentage=60.0 -XX:InitialRAMPercentage=60.0
  -XX:+ExitOnOutOfMemoryError` (heap sized from the container memory limit; initial = max keeps the
  index resident). Extend via `JAVA_OPTS` / `CESIUM_KAFKA_OPTS`.

- **GC.** ≤ ~10 M entries / ≤ 4 GB heap: G1 default, `-Xms=-Xmx`; add `-XX:+AlwaysPreTouch` for
  latency-critical installs. ≥ 8 GB / 100 M-entry scale: ZGC generational
  (`-XX:+UseZGC -XX:+ZGenerational` on JDK 21), which makes dispatch-accuracy p99 independent of heap
  size. Keep JFR continuous recording on to catch footprint/pause regressions in production.

- **Backpressure.** `dispatch.max-pending-per-partition` pauses ACTIVE shards above its high-water
  (resumes below half); the process-wide `dispatch.max-pending-total` (AUTO ≈ 25 % of `Xmx` ÷ 64 B) pauses
  ACTIVE shards above the total. A RECOVERING shard is never paused. The startup heap-budget check
  (`startup-checks.heap-budget`) sizes the steady-state caps only; recovery is bounded by a runtime
  resident-entry ceiling that fails fast instead of running out of heap
  (`cesium_recovery_over_budget_total`, [§14.3](#143-runtime-fail-fast-exit-1-readiness-false)).
  Watch `cesium_shard_paused`.

---

## 12. Broker IOPS budgeting for seek-fetch

Payloads are never copied; cesium re-fetches them from the source at dispatch time. For long
delays this hits cold (non-page-cache) segments, so budget broker disk IOPS for it (detail in
[performance.md §7](performance.md)):

- One `seek` + a sequential forward scan serves all of a partition's due entries. The midnight
  thundering herd (10 k due from one partition) is one sequential pass instead of 10 k random seeks,
  so budget sequential read bandwidth for bursty due-sets.
- Sparse due-sets across many partitions degrade toward random reads, the harder IOPS case.
  Budget random-read IOPS for the worst-case scattered-due profile of your workload.
- Tiered / remote storage fetches are slow. The byte-budget truncate-and-carry-over still bounds
  heap, but dispatch latency rises.
- A degraded source partition is put in the penalty box (`dispatch.fetch.penalty.backoff`,
  exponential `PT0.05S → PT10S`) so it does not head-of-line-block healthy partitions; watch
  `cesium_fetch_penalized_partitions` and `cesium_fetch_duration_seconds`.

If dispatch-time fetches are your bottleneck, provision faster broker disks for the source, keep the
source's hot set in page cache where delays are short, and consider source `delay.max` choices that
keep fetches warmer.

---

## 13. Metrics and alerting

cesium serves Prometheus metrics at `http://<host>:<observability.port>/metrics` (default port 8081),
plus `/health/live`, `/health/ready`, `/info`. `/metrics` exposes the `cesium_*` meters listed
below, and each carries only the per-meter tags shown in the table (`loop`, `partition`, `outcome`,
`event`, `type`, `result`, `cause`, `reason`). Two things design `§9` describes are not wired in
this release: there is no `application_id` / `role` common tag, and the Kafka-client
(`kafka_consumer_*` / `kafka_producer_*`) and JVM/process binders are not registered, so `kafka_*`
and `jvm_*` series are absent. Do not write alerts against them. For JVM/client metrics, run a
JMX→Prometheus exporter sidecar against the JVM.

> **This port is unauthenticated and MUST be network-restricted.** Anyone with network reach can read
> all four endpoints (and the `application-id`, if `observability.detailed-info: true`). It binds all interfaces by
> default (`0.0.0.0`) and caps accepted connections (`jdk.httpserver.maxConnections`, default 64) on
> top of the slow-client reaper (M1). Set `observability.bind-address: 127.0.0.1` to
> restrict it to loopback, or scope it with a Kubernetes NetworkPolicy (monitoring namespace only)
> ([§5](#5-least-privilege-deployment-tls-sasl-and-the-acl-matrix), L1).

### Metric inventory

In Prometheus exposition, counters end in `_total`; timers expose `_seconds_count`,
`_seconds_sum`, `_seconds_max` (and histogram buckets for the lag/fetch timers); gauges are bare.

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `cesium_ingest_records_total` | counter | `outcome=relayed_immediate\|scheduled\|clamped\|dlq\|unrelayable_dropped` | ingest dispositions |
| `cesium_dispatch_records_total` | counter | `outcome=dispatched\|payload_expired\|dropped\|unrelayable` | dispatch dispositions (flushed only after commit); `unrelayable` also counts entries dropped under `on-unrelayable: DROP`, so use `cesium_dlq_records_total` for DLQ volume |
| `cesium_dispatch_lag_seconds` | timer (histogram) | | actual − scheduled; the headline precision SLO |
| `cesium_dispatch_poll_gap_seconds` | gauge | | recent max time between group-B polls; alert ≪ `max.poll.interval.ms` |
| `cesium_txn_commit_seconds` | timer | `loop` | `commitTransaction` latency per attempt |
| `cesium_transactions_total` | counter | `loop`, `result=committed\|aborted\|in_doubt`, `cause` | aborts ⇒ fencing/contention; in-doubt occurrences |
| `cesium_header_errors_total` | counter | `type=malformed\|over_max\|conflict` | protocol violations |
| `cesium_dlq_records_total` | counter | `reason=malformed-header\|over-max-delay\|unrelayable\|payload-expired` | committed DLQ records from both loops: ingest (header policy, unrelayable) and dispatch (payload-expired, unrelayable); flushed only after commit |
| `cesium_unrelayable_dlq_rejected_total` | counter | | an unrelayable record's DLQ write was also permanently rejected, so it escalated to DROP (registered on first occurrence) |
| `cesium_ingest_rebalances_total` | counter | `event=assigned\|revoked\|lost` | group-A churn; `lost` = fenced/unclean |
| `cesium_dispatch_rebalances_total` | counter | `event=assigned\|revoked\|lost` | group-B churn; `lost` = fenced/unclean |
| `cesium_fetch_attempts_total` | counter | | seek-fetch attempts |
| `cesium_fetch_misses_total` | counter | | seek-fetch misses |
| `cesium_fetch_unfetchable_total` | counter | | provably-expired payloads |
| `cesium_fetch_bytes_total` | counter | | decompressed payload volume (budget observability) |
| `cesium_fetch_duration_seconds` | timer (histogram) | | per fetch pass; attributes cold-segment / degraded-broker cost |
| `cesium_fetch_penalized_partitions` | gauge | | penalty-box occupancy |
| `cesium_pending_entries` | gauge | `partition` | live index size; a step-collapse is the tracker-integrity canary (R-9) |
| `cesium_pinned_entries` | gauge | `partition` | sidecar occupancy; sustained at max ⇒ overflow mode |
| `cesium_cursor_sidecar_bytes` | gauge | `partition` | encoded sidecar size vs budget |
| `cesium_shard_paused` | gauge | `partition` | backpressure pause state |
| `cesium_shard_state` | gauge | `partition` | `0` ASSIGNED (recovery not begun), `1` RECOVERING, `2` ACTIVE (§3.6) |
| `cesium_replay_remaining_records` | gauge | `partition` | barrier − position while RECOVERING; `0` when ASSIGNED or ACTIVE |
| `cesium_degraded` | gauge | `loop` | park-and-degrade state (§3.8); cause is logged |
| `cesium_loop_last_iteration_timestamp_seconds` | gauge | `loop` | epoch-seconds of the last loop iteration; feeds liveness |
| `cesium_revalidation_errors` | gauge | | errors found by the last re-validation round (`startup-checks.revalidation-interval`, default 5 min); `0` when clean. Each error is logged at ERROR on `cesium-admin` and sets the readiness degraded flag |
| `cesium_tracker_invalid_records_total` | counter | | tracker wire-format violations (malformed / version-skew); foreign-writer canary. Does NOT detect well-formed forgeries ([§5](#5-least-privilege-deployment-tls-sasl-and-the-acl-matrix), L2) |
| `cesium_tracker_cancel_records_total` | counter | | reserved CANCEL records seen (counted no-ops) |
| `cesium_tracker_unknown_reason_records_total` | counter | | tombstones with an unrecognized completion reason (applied) |
| `cesium_cursor_guard_violations_total` | counter | | I5 / monotonic cursor-guard trips (a surfaced bug; the last safe cursor is returned) |
| `cesium_store_index_anomalies_total` | counter | | duplicate ADDs / out-of-order drops (R1) |
| `cesium_store_heap_rebuilds_total` | counter | | dispatch-heap rebuilds across shards |
| `cesium_store_log_sweeps_total` | counter | | arrival-log sweeps across shards |
| `cesium_recovery_over_budget_total` | counter | | recovery seed/replay refused because the backlog would exceed the heap budget (H1); the process then fails fast |

> **Not yet emitted (specified in design `§9`, deferred).** Do not write alerts against these; the
> series do not exist in this release: `cesium_lso_lag`,
> `cesium_store_recovery_duration_seconds`,
> `cesium_store_replay_records_total`,
> `cesium_pending_oldest_deadline_seconds`, `cesium_index_bytes_estimate`. Use the proxies noted
> below until they land. (`cesium_lso_lag` is deferred in the dispatch loop because it needs a
> per-partition read_uncommitted/read_committed offset diff from the admin plane.)

### Health endpoints

- **`/health/live`**: loop heartbeat freshness + thread liveness. A failing liveness means a loop
  stalled and the process should be restarted.
- **`/health/ready`**: startup checks passed, loops alive, consumers assigned, recent poll.
  Recovery state is not part of readiness, so a replaying instance is ready. The
  detail payload lists each non-ACTIVE shard (`partition`, `state`, `recordsRemaining`,
  `etaMillis`; `-1` until two 1 s samples show the backlog shrinking); a `degraded` flag (with cause) surfaces
  park-and-degrade and re-validation errors without failing the probe.
- **`/info`**: by default only `version`, `gitCommit`, and `store.type`. With
  `observability.detailed-info: true` it adds `applicationId`, `roles`, store `capabilities`, and any
  acknowledged escape hatches (`acknowledgments`).

### Suggested alerts (real metrics only)

| Condition | Expression (sketch) | Meaning / action |
|---|---|---|
| Loop degraded | `cesium_degraded > 0` | Park-and-degrade (§3.8): broker degradation outlasting retries. Page; check the logged cause (e.g. destination ISR shortage). Self-recovers when the dependency returns. |
| Re-validation error | `cesium_revalidation_errors > 0` | The environment broke under a running engine: a tracker setting below the §3 floors, a committed cursor older than `delete.retention.ms / 2 + 1h`, partition drift, a missing topic, a retention change, or a round that could not complete. The ERROR log on `cesium-admin` names it. Fix it live; the next clean round clears the flag ([§14.5](#145-re-validation-errors-not-an-exit)). |
| Loop stalled | `time() − cesium_loop_last_iteration_timestamp_seconds > N` | A loop stopped iterating. Page; check thread/JVM health. |
| Dispatch precision SLO | `histogram_quantile(0.99, cesium_dispatch_lag_seconds_bucket) > SLO` | Lateness over budget. Investigate commit/IO/GC tails ([performance.md](performance.md)). |
| Poll-gap approaching eviction | `cesium_dispatch_poll_gap_seconds > 0.5 × max.poll.interval.ms/1000` | Risk of group eviction under a due-storm. Lower `dispatch.drain.max-slice` or `dispatch.batch.max-entries`. |
| Transaction aborts | `rate(cesium_transactions_total{result="aborted"}[5m]) > threshold` | Fencing/contention churn. Correlate with rebalances. |
| In-doubt commits | `increase(cesium_transactions_total{result="in_doubt"}[15m]) > 0` | Ambiguous commits resolved by the I9 procedure. Investigate broker/commit latency. |
| Unclean rebalance | `increase(cesium_dispatch_rebalances_total{event="lost"}[15m]) > 0` | A member was fenced/lost. Check session timeouts and pod restarts. |
| Payload expiry | `rate(cesium_fetch_unfetchable_total[10m]) > 0` (sustained) | Payloads gone at dispatch: source retention too short or size/tier eviction. Raise source retention; inspect DLQ. |
| Degraded source partition | `cesium_fetch_penalized_partitions > 0` (sustained) | A source partition's leader/disk is degraded. Investigate broker. |
| Cold-fetch cost | `histogram_quantile(0.99, cesium_fetch_duration_seconds_bucket) > threshold` | Cold-segment / IOPS pressure ([§12](#12-broker-iops-budgeting-for-seek-fetch)). |
| Sidecar overflow (replay cost rising) | `cesium_cursor_sidecar_bytes` near `dispatch.cursor.sidecar-max-bytes` with `cesium_pinned_entries` flat at its ceiling, sustained (entries are variable-length, so there is no fixed entry ceiling) | Overflow mode (§3.5): replay reverts to `completion_rate × pin age`. Raise `dispatch.cursor.sidecar-max-bytes` (with broker `offset.metadata.max.bytes`), or inspect long-delay producers. |
| Tracker integrity canary | step-collapse of `cesium_pending_entries` | Tracker possibly truncated/recreated (R-9). Follow [§6](#6-tracker-disaster-recovery-runbook). |
| Malformed tracker write / foreign writer | `rate(cesium_tracker_invalid_records_total[10m]) > 0` | A *malformed or version-skewed* write hit the tracker. Check the ACL ([§5](#5-least-privilege-deployment-tls-sasl-and-the-acl-matrix)). A well-formed forgery would not move this counter; detecting competent tampering needs broker authorizer audit logging (L2). |
| Cursor-guard / index anomaly | `rate(cesium_cursor_guard_violations_total[15m]) > 0` or `rate(cesium_store_index_anomalies_total[15m]) > 0` | A surfaced invariant violation. No data is lost (the last safe cursor is returned), but file a bug with logs. |
| Slow replay | `cesium_shard_state == 1` and `cesium_replay_remaining_records` not falling (sustained) | A shard is stuck recovering. Check group-B fetch throughput and tracker-topic size. Readiness is unaffected. |
| Backpressure pause | `cesium_shard_paused > 0` (sustained) | Index near cap; intake paused. Add dispatch capacity or raise the heap / caps. |
| DLQ drain | `rate(cesium_dlq_records_total[10m]) > 0` | Inspect and drain the DLQ; correlate `reason` (`payload-expired` means a loss notice: the payload was gone at dispatch time). |
| Header misuse | `rate(cesium_header_errors_total[10m]) > 0` | Producers sending malformed/over-max/conflicting headers. Fix the producer. |

**Broker-side monitoring cesium does not replace:** tracker-topic disk and partition size against
the [§2](#2-the-tracker-sizing-worksheet) worksheet, group-B consumer lag (the v2 cursor tracks
position, so lag reads approximately correctly except in sidecar-overflow mode), and the source's
earliest-record age. Use `cesium_pinned_entries` + `cesium_cursor_sidecar_bytes` for overflow
detection.

---

## 14. Fail-fast runbook (every exit)

cesium never ends in a silent terminal state: every fault path is a retry, a park-and-degrade (with
`cesium_degraded` + alert, self-healing), a re-validation degrade (with `cesium_revalidation_errors`
+ alert, cleared by the next clean round), or a fail-fast with a runbook entry. The fail-fast
exits:

- **Exit 78** (`EX_CONFIG`): a configuration error, caught before the engine builds.
- **Exit 1** (`EX_FATAL`): startup validation against the live cluster, or a runtime fatal; readiness
  is already false.

### 14.1 Configuration errors (exit 78)

The app prints the full aggregate report and exits 78. Causes and fixes:

| Cause | Fix |
|---|---|
| Unknown key (YAML or `CESIUM_`/`-D` overlay) | Typo or stale key; the report names the path. Correct it against [configuration.md](configuration.md). |
| Missing required key (`application-id`, `instance-id`, `route.source.topic`, `route.destination.topic`) | Provide it. |
| Locked Kafka key set in passthrough | Remove it. The message explains why it is engine-owned ([configuration.md §3](configuration.md#locked-kafka-keys-rejected-with-an-explanation)). |
| `${env:VAR}` references an undefined variable | Define the variable or fix the reference. |
| DLQ policy active but no `route.dlq.topic` | Configure the DLQ topic, or change the policy off DLQ. |
| Worst-case index footprint exceeds the heap budget (config load assumes 1 partition; the check re-runs at the real partition count after cluster validation and fails with exit 1 there) | Lower `dispatch.max-pending-per-partition`, raise the heap, or set `startup-checks.heap-budget: WARN` to accept. |

### 14.2 Startup validation against the cluster (exit 1)

Discovered after config binds, against the live cluster:

| Failure | Runbook |
|---|---|
| Source or destination topic does not exist; DLQ topic does not exist while a policy routes to it | Create the topic or fix the name. |
| Source topic is compacted | cesium needs fetchable offsets; point at a non-compacted source. |
| Source `retention.ms` < `delay.max + margin` (`startup-checks.retention: FAIL`) | Raise source retention above `delay.max`, lower `delay.max`, or set the check to `WARN`/`SKIP` accepting the payload-expiry risk. |
| Size/tier eviction detected, not acknowledged | Set `startup-checks.size-based-retention: ACKNOWLEDGED` (named acceptance) or remove `retention.bytes`/tiered storage from the source. |
| Tracker topic missing in `FAIL` bootstrap mode | Pre-provision the tracker topic per [§1](#1-topics-and-bootstrap-create-vs-fail)/[§3](#3-tracker-settings-that-correctness-depends-on), or use `CREATE`. |
| Tracker partition count ≠ source | Grow the tracker to match ([§9](#9-growing-partitions-runbook)). |
| Tracker `cleanup.policy` ≠ `compact` (e.g. `compact,delete`) | Set `cleanup.policy=compact`; `compact,delete` causes silent loss (D4). |
| Tracker `delete.retention.ms` below the floor | Raise it to `≥ 2 × delay.max`, or lower `delay.max` and drain ([§4](#4-lowering-delaymax-runbook)). |
| Tracker `min.compaction.lag.ms` below floor | Raise to `≥ max(2 × transaction.timeout.ms, 1 h)`. |
| Broker `offsets.retention.minutes` < `max-tolerated-outage` (`outage-check: FAIL`) | Raise broker `offsets.retention.minutes`, lower `max-tolerated-outage`, or set the check to `WARN` ([§7](#7-offsets-retention-vs-outage-tolerance)). |
| `SOURCE_PARTITION` relay but destination partition count ≠ source | Grow the destination to match, or use `BY_KEY`. |
| Tracker write ACL not verifiably in force (`startup-checks.tracker-acl: FAIL`): no matching grant, a foreign writer, `route.tracker.acl-principal` unset, or no authorizer | Grant the cesium principal exclusive write and revoke foreign writers ([§5](#5-least-privilege-deployment-tls-sasl-and-the-acl-matrix)), set `route.tracker.acl-principal`, or set the check to `WARN`/`SKIP` if you enforce it out of band. |
| Applying the tracker write ACL failed in `CREATE` mode (an error other than "no authorizer") | Give the admin client `Cluster` ALTER, or apply the ACL by hand and retry. |
| **Source topic identity mismatch** (recreated under the same name) | The source was deleted and recreated (new topic id), so delivering old-offset payloads would be wrong (R-10/R17). Reset the tracker (accept loss) or restore the original source, as an explicit decision. |
| Tracker topic created but never became describable within the metadata-propagation budget | cesium created the tracker, then waited out normal KRaft propagation (10 s) and still could not get any broker to describe it with its full partition count. The ordinary sub-millisecond window is already absorbed by that wait. Retry startup; if it recurs, investigate broker metadata lag (a stalled metadata publisher) or controller availability. A `WARNING` naming a propagation wait over 1 s on a startup that *succeeded* is the early form of the same problem ([ADR-0018](adr/0018-bounded-wait-for-proven-topic-metadata.md)). |
| `dispatch.cursor.sidecar-max-bytes` > broker `offset.metadata.max.bytes` (warning only, no exit; the budget is clamped to the broker cap) | Raise the broker setting to keep the configured budget, or lower the sidecar budget to silence the warning. |
| A topic is reported as not existing that you are sure exists | cesium fails fast on operator-provisioned topics and does not wait out metadata propagation for them (the common cause is a typo, and waiting would slow that diagnosis down). If your provisioning pipeline creates the topic and starts cesium in the same breath, have it wait for the topic to become describable, not merely for `createTopics` to return, before launching cesium. |

### 14.3 Runtime fail-fast (exit 1, readiness false)

| Failure (and the message) | Runbook |
|---|---|
| **Ingest: no committed offset**: *no committed offset for assigned source partition(s) and auto.offset.reset=none is locked: either committed offsets expired (broker offsets.retention.minutes shorter than the outage) or this is a first run; an operator must seed group offsets explicitly* | **First run:** seed the ingest group's offsets to your chosen start (typically earliest) once, e.g. `kafka-consumer-groups.sh --group cesium.<application-id>.ingest --topic <source> --reset-offsets --to-earliest --execute`. **After an outage > offsets retention:** the operator chooses the reset point explicitly (earliest = reprocess/possible duplicates; a later point = skip). Raise broker `offsets.retention.minutes` to prevent recurrence ([§7](#7-offsets-retention-vs-outage-tolerance)). |
| **Dispatch: no committed cursor on a non-first run** | Same class as above for group B: committed cursors expired or were removed. The dispatch group seeks to beginning only on a provable first run; otherwise it fails fast. Treat as a tracker/offset integrity event ([§6](#6-tracker-disaster-recovery-runbook)). |
| **Dispatch: tracker offset out of range**: *the tracker topic was truncated or recreated under the same name (§3.6 integrity, R-9). Never auto-reset — follow the tracker DR runbook* | The tracker was truncated/recreated. Follow [§6](#6-tracker-disaster-recovery-runbook). cesium will not auto-reset into an empty log. |
| **Committed-offset identity mismatch** (source recreated) at a runtime offset fetch | Same as the startup identity mismatch ([§14.2](#142-startup-validation-against-the-cluster-exit-1)). |
| **Topic identity change found by re-validation**: *fatal engine failure (re-validation)*, with *re-validation found the route identity changed since startup* or *the tracker topic '…' id changed* | The source or tracker was recreated under the same name while the engine ran (R17). The engine exits within one `startup-checks.revalidation-interval`. For the source, follow the startup identity mismatch row; for the tracker, follow [§6](#6-tracker-disaster-recovery-runbook). |
| **Fatal producer error** (`ProducerFencedException`, `OutOfOrderSequenceException`, unrecoverable auth/config) | The clients close, the worker fails, the process exits non-zero (the durable log is authoritative for the successor). Restart the instance; if it recurs, check fencing (duplicate `instance-id`?), ACLs, and broker health. |
| **Partition-count drift**: *source partition … has no tracker partition: tracker topic '…' has … partition(s) — the source partition count grew past the tracker (I-7, R-7)* | Raised by ingest on the first record from the new source partition; re-validation reports the drift earlier as a degrading error. Grow the tracker first ([§9](#9-growing-partitions-runbook)). |
| **Recovery over heap budget**: *RECOVERY OVER HEAP BUDGET — refusing to … another pending entry for tracker partition …* (`cesium_recovery_over_budget_total`) | The durable backlog exceeds what the heap can hold. Nothing was committed. Raise `-Xmx` (or `store.properties.heap-budget-bytes`) and/or shrink the backlog, then restart. |
| **Ingest: `delay.on-*` policy FAIL fired** | A record violated the delay protocol under a `FAIL` policy; restart re-fails on the same record. Switch the policy to `DLQ`/`RELAY_IMMEDIATE` or move past the record explicitly ([§5](#5-least-privilege-deployment-tls-sasl-and-the-acl-matrix), L5). |

### 14.4 Park-and-degrade (NOT an exit)

When definitively-abortable retries exhaust (e.g. the destination is under `NotEnoughReplicas` for an
extended period), the loop parks the batch (entries return to pending with a penalty not-before),
keeps polling (membership stays alive), raises `cesium_degraded{loop}` + an alert, and idles out
the broker degradation without a crash-loop or replay storm. It self-recovers on the first successful
commit. Action: investigate and fix the failing dependency (destination ISR, broker health); cesium
resumes automatically.

### 14.5 Re-validation errors (NOT an exit)

Every `startup-checks.revalidation-interval` (default `PT5M`) the `cesium-admin` thread re-runs the
§14.2 checks against the live cluster, without creating topics or applying ACLs, plus three runtime
checks: the source and tracker topic ids (a change is the exit in §14.3), the dispatch group's
committed-cursor age against `delete.retention.ms / 2 + 1h` (dispatch instances only), and, for an
acknowledged size- or tier-evicting source, whether eviction reached records inside
`delay.max + 1h`. Every other error is logged at ERROR as *re-validation finding route.tracker: …*,
sets `cesium_revalidation_errors` and the `/health/ready` degraded flag with a short cause, and
dispatch keeps running (ADR-0020). Fix the cause with the §14.2 runbook for the same finding; the
next clean round clears the flag. A warning is logged once, when it first appears.

---

For deeper background on any decision referenced here, see [docs/design.md](design.md) and the
failure-matrix coverage map in [docs/failure-matrix-coverage.md](failure-matrix-coverage.md).
