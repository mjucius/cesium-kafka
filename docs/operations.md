# Operations guide

Running cesium-kafka in production: topic bootstrap, the tracker sizing worksheet, the
correctness-load-bearing tracker settings, disaster recovery, scaling, the Kubernetes rollout
recipe, JVM/GC and broker IOPS budgeting, alerting, and a runbook for every fail-fast exit.

Operators act on this document, so every config key, metric name, and runbook action here is
verified against the code (the `cesium-kafka-core` config records, the Micrometer registrations, and
the actual fail-fast messages). Where the deep design rationale lives in
[docs/design.md](design.md), the section numbers (`§3.6`, `§5.4`, ...) point into it. Companion docs:
[configuration.md](configuration.md) (full key reference), [performance.md](performance.md) (measured
numbers, memory worksheet, GC, seek-fetch I/O), [header-protocol.md](header-protocol.md) (wire
contract).

**Contents**

1. [Topics and bootstrap (CREATE vs FAIL)](#1-topics-and-bootstrap-create-vs-fail)
2. [The tracker sizing worksheet](#2-the-tracker-sizing-worksheet)
3. [Correctness-load-bearing tracker settings](#3-correctness-load-bearing-tracker-settings)
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

cesium serves one **route** per process: a user-owned **source** and **destination**, an internal
**tracker**, and an optional **DLQ**.

| Topic | Owned by | Provisioning |
|---|---|---|
| `route.source.topic` | you | Create it yourself. Must **not** be compacted (offsets must stay fetchable) — startup-checked. |
| `route.destination.topic` | you | Create it yourself. Consumers **must** use `read_committed`. |
| `route.tracker.topic` | cesium | Provisioned by bootstrap (below). Default name `cesium.<application-id>.tracker`. |
| `route.dlq.topic` | you / cesium | Required whenever any policy routes to DLQ (the defaults do). |

### Tracker bootstrap modes (`route.tracker.bootstrap`)

- **`CREATE`** (default) — cesium creates the tracker topic if absent, with the `§2.1` configs
  (compaction-only cleanup, the tombstone-retention floor, the compaction-lag floor, ~1 h
  `segment.ms`, `LogAppendTime`), **mirroring the source partition count**, and applies the write ACL
  when `route.tracker.acl-principal` is set. After creating it, cesium waits (bounded, up to 10 s) for
  the topic to become describable: `createTopics` is acknowledged by the KRaft controller, but the
  broker that answers the next describe publishes that metadata asynchronously, so a freshly created
  topic can be briefly invisible on a healthy cluster. The wait is silent only when it is trivial —
  anything above 1 s is surfaced as a startup warning ([ADR-0018](adr/0018-bounded-wait-for-proven-topic-metadata.md)).
- **`FAIL`** — cesium requires a pre-provisioned tracker topic and **validates** its configs,
  failing fast on any drift (wrong partition count, wrong `cleanup.policy`, `delete.retention.ms`
  below the floor, `min.compaction.lag.ms` below the floor). Use `FAIL` where topic creation is
  centrally controlled; provision the topic to match §2.1 and §3 below.

> The tracker topic's **partition count MUST equal the source's** — every tracker record for source
> partition *p* lands on tracker partition *p*. This is validated at startup and periodically; a
> mismatch is a fail-fast (§9, §14).

The DLQ topic's existence is validated whenever a policy routes to it. Create it with a partition
count that suits your DLQ-drain tooling (it need not match the source).

---

## 2. The tracker sizing worksheet

The tracker topic is the system's entire durable scheduler state, and **its disk is dominated by
retained completion tombstones, not by pending entries** — the term the PoC's "26 B × pending"
estimate missed by ~3000× (R10). Use this worksheet.

### Formula

```
tracker_bytes_per_replica ≈ pending × ~70 B                                  (pending ADDs)
                          + completion_rate_per_s × retention_window_s × ~64 B   (retained tombstones)
                          + uncleaned_tail                                    (active, not-yet-compacted segments)

provisioned_disk = tracker_bytes_per_replica × replication_factor × safety_margin
```

- `pending` — entries scheduled but not yet delivered ≈ `arrival_rate × average_delay`.
- `completion_rate_per_s` — entries **completed** per second (≈ the delayed-scheduling rate in steady
  state; each completion writes one ~64 B tombstone).
- `retention_window_s` — `delete.retention.ms / 1000`, **and the floor is `2 × delay.max`** (§3). With
  the default `delay.max = P1D`, the floor is `2 days = 172,800 s`.
- `uncleaned_tail` — roughly the active segments since the last clean (governed by `segment.ms ≈ 1 h`
  and `min.compaction.lag.ms`); a modest additive term, not the driver.
- `replication_factor` — typically 3; **multiplies on-disk bytes 3×**. Do not forget it.
- Note self-consumption (the dispatch loop reads its own tombstones, ~2× read bandwidth) affects
  *throughput*, not disk.

### Why `delay.max` drives disk

The retained-tombstone term is `completion_rate × retention_window × 64 B`, and `retention_window ≥
2 × delay.max`. So **doubling `delay.max` roughly doubles tracker disk.** That is why `delay.max`
defaults to **`P1D`**, not the original P7D — and why raising it is an explicit, worksheet-reviewed
decision (D14).

### Worked examples (default `delay.max = P1D` ⇒ `retention_window = 172,800 s`)

Per-replica tombstone term = `completion_rate × 172,800 s × 64 B`:

| Completion rate | Retained tombstones | Tombstone bytes / replica | × replication 3 |
|---|---|---|---|
| 1,000 msg/s | 1.7×10⁸ | ~11 GB | ~33 GB |
| 10,000 msg/s | 1.7×10⁹ | ~110 GB | ~330 GB |
| 100,000 msg/s | 1.7×10¹⁰ | ~1.1 TB | ~3.3 TB |

Add the pending term. Example: a 10,000 msg/s route where the average delay is 1 h holds
`10,000 × 3,600 ≈ 3.6×10⁷` pending entries × 70 B ≈ **2.5 GB/replica** — small next to the ~110 GB
tombstone term. The tombstone term dominates by one to two orders of magnitude.

### The `delay.max = P7D` contrast (why the default dropped)

Raise `delay.max` to **P7D** on the same 10,000 msg/s route and `retention_window` becomes
`14 days = 1,209,600 s`:

`10,000 × 1,209,600 × 64 B ≈ 774 GB/replica ≈ ~1 TB` — **~2.3 TB at replication 3** (risk #11). The
delay ceiling, not the message size or pending count, sets the disk bill.

### Sizing actions

- Pick `delay.max` deliberately. If you need long delays on a busy route, run this worksheet and
  provision disk accordingly.
- Provision tracker disk for `provisioned_disk` with headroom for the uncleaned tail and bursty
  completion rates; alert on tracker-topic disk growth (monitor it via your broker disk/partition-size
  monitoring — see [§13](#13-metrics-and-alerting) for the in-process signals).
- Keep `segment.ms ≈ 1 h` and `min.compaction.lag.ms` at the floor so the cleaner actually collects
  tombstones; oversized segments delay collection and inflate disk.

### No in-process rate limiting; bound the DLQ

cesium has **no per-tenant rate limiting** — no per-producer / per-key / per-partition quota or
fairness anywhere in ingest. A flood or far-future delay-bomb grows the tracker (and, for malformed
records, the DLQ) on disk, bounded only by the controls that sit **before** cesium. Rely on:

- **Broker client/produce quotas** on the source-producer principals;
- **Source `retention.bytes`** to bound what a flood can accumulate (note size/tier eviction interacts
  with the payload-lifetime check — `startup-checks.size-based-retention`, [§14.2](#142-startup-validation-against-the-cluster-exit-1));
- **Per-tenant topic isolation** (a route per tenant, not one shared source);
- a deliberately chosen **`delay.max`** — it multiplies tracker disk through the `2 × delay.max`
  tombstone-retention floor (this worksheet).

**Bound the DLQ's retention yourself.** Startup validates only that the DLQ topic *exists*, not its
retention — a malformed-record flood (default `DLQ` policy copies the full original payload 1:1) can
fill an unbounded DLQ. Set `retention.ms`/`retention.bytes` on the DLQ topic to match your drain
cadence.

---

## 3. Correctness-load-bearing tracker settings

These are validated at startup (and the observed-age terms periodically), **not** merely
recommended — getting them wrong can cause a duplicate or silent loss.

| Setting | Required value | Why it is load-bearing |
|---|---|---|
| `cleanup.policy` | `compact` (never `compact,delete`) | `compact,delete` would delete a still-pending ADD older than retention → silent loss (D4). Compaction-only means a lone pending ADD can never expire. |
| `delete.retention.ms` | **`≥ 2 × max(delay.max, observed oldest-pending age, committed-cursor age)`** | In replay overflow-fallback mode, a replayer's cursor can sit as far back as the oldest pending entry. If a completion tombstone it needs was already deleted, the entry looks pending → **duplicate**. The `delay.max` term is a **startup FAIL**; the observed-age terms are **periodically re-validated** and refuse to advance into the unsafe regime (degraded + alert) rather than failing silently (D14, §3.7). |
| `min.compaction.lag.ms` | `≥ max(2 × transaction.timeout.ms, 1 h)` | Keeps the cleaner away from the active tail / LSO region. Below the floor is a startup FAIL. |
| `segment.ms` | `≈ 1 h` | So cleaning actually happens. Larger is a WARN (delays tombstone collection, grows disk). |
| `message.timestamp.type` | `LogAppendTime` | So tracker record timestamps reflect append order. Otherwise a WARN. |

> **`delete.retention.ms` is the single most important tracker setting.** Never lower it below
> `2 × delay.max`. If you must shrink it, you must first shrink `delay.max` and drain — see [§4](#4-lowering-delaymax-runbook).

The exact startup failure when it is too low (from `StartupValidator`):

> *tracker topic '…' has delete.retention.ms=… ms, below the tombstone-retention floor
> 2 x delay.max = … ms (D14, §3.7, R-8): a replayer in overflow-fallback mode may need tombstones as
> old as the oldest pending entry, and cleaning them earlier converts a replay into a duplicate.
> Raise delete.retention.ms to >= … ms, or lower delay.max (drain entries scheduled under the old
> maximum first — see the lowering-delay.max runbook).*

---

## 4. Lowering `delay.max` (runbook)

Lowering `delay.max` and shrinking `delete.retention.ms` together can strand pending entries that
were scheduled under the **old, larger** maximum — their tombstones could be cleaned before a replay
reads them (R20). The periodic re-validation against *observed oldest-pending age* refuses the unsafe
regime, but you must drain first to actually shrink retention.

1. **Lower `delay.max` in config and restart.** New records are now capped at the lower ceiling.
   Older pending entries (scheduled under the previous maximum) remain valid and are still honored.
2. **Do NOT lower `delete.retention.ms` yet.** Leave it at the floor for the *old* `delay.max`.
3. **Wait out (drain) the entries scheduled under the old maximum.** Wait at least the old
   `delay.max`, confirming via the per-partition pending signal (`cesium_pending_entries`) that the
   long-tail entries have been delivered.
4. **Only then lower `delete.retention.ms`** to the floor for the new `delay.max`
   (`2 × new delay.max`), and restart so startup re-validates.

If you skip the drain, the periodic re-validation will refuse to advance and flag a degraded state
rather than silently risk a duplicate.

---

## 5. Least-privilege deployment: TLS, SASL, and the ACL matrix

cesium **delegates authentication and authorization to the broker** — it opens ordinary Kafka clients
and the broker decides what each may do. [SECURITY.md](../SECURITY.md) is the authoritative
secure-deployment guide; the operational key points mirror here.

### Require TLS + SASL in production

Set `security.protocol` (`SSL` or `SASL_SSL`) and a SASL mechanism through the **`kafka.properties`
passthrough** (applied to every cesium client) so the broker can identify cesium as one principal. Keep
secrets out of the config file — reference them with `${env:VAR}` (e.g.
`sasl.jaas.config: ${env:KAFKA_JAAS_CONFIG}`, `ssl.truststore.password: ${env:KAFKA_TRUSTSTORE_PASSWORD}`).
The [PLAINTEXT quickstart compose](../config/docker-compose.yaml) is a local demo only — never a
production posture.

### Least-privilege ACLs for all six client roles

All six cesium clients authenticate as a **single cesium principal** (`User:cesium`); grant it exactly
these and nothing else (substitute your `application-id` for `<app>`). `DESCRIBE` is implied by
`READ`/`WRITE` on the same resource; idempotent producer writes are authorized by topic `WRITE` in
Kafka ≥ 3.0; a transactional producer that commits offsets needs `READ` on the consumer `Group`.

| Client role | Resource (type · name/pattern) | Ops |
|---|---|---|
| Ingest consumer (group A) | `Group` `cesium.<app>.ingest` (literal); `Topic` *source* | READ; READ |
| Dispatch consumer (group B) | `Group` `cesium.<app>.dispatch` (literal); `Topic` *tracker* | READ; READ |
| Seek consumer (group-less) | `Topic` *source* | READ (**no `Group` ACL**) |
| Ingest txn producer | `TransactionalId` `cesium.<app>.ingest.*` (prefixed); `Topic` *tracker* / *destination* / *DLQ*; `Group` `cesium.<app>.ingest` | WRITE; WRITE; READ |
| Dispatch txn producer | `TransactionalId` `cesium.<app>.dispatch.*` (prefixed); `Topic` *destination* / *tracker* / *DLQ*; `Group` `cesium.<app>.dispatch` | WRITE; WRITE; READ |
| Admin client | `Cluster` (DESCRIBE + DESCRIBE_CONFIGS); `Topic` *source/dest/tracker/DLQ* (DESCRIBE + DESCRIBE_CONFIGS); `Group` both (DESCRIBE); **`CREATE` bootstrap only:** `Cluster` CREATE + ALTER | per cell |

> **The `TransactionalId` and `Group` ACLs are load-bearing, not optional (M3).** cesium's
> transactional / `group.instance.id` ids are deterministic (`cesium.<app>.<role>.<instance>.<ordinal>`)
> and the `application-id` seed leaks on the unauthenticated `/info`. **Without a `TransactionalId` ACL
> scoping `cesium.<app>.*` to the cesium principal, any authenticated co-tenant can open a producer
> with cesium's transactional id and fence it into a `ProducerFencedException` → fatal exit → restart →
> re-fence crash-loop** (and likewise `FencedInstanceIdException` via the static group id). **Do not
> stop at the tracker topic ACL.** Under `route.tracker.bootstrap: FAIL` the cesium principal does not
> need `Cluster` CREATE/ALTER — provision the tracker topic and its write ACL out of band.

**Topic ownership** (restrict `WRITE` accordingly): your upstream producers write the **source**
(cesium only reads it); cesium writes the **destination** (downstream consumers read it with
`read_committed`); cesium writes the **DLQ** (your drain tooling reads it); cesium alone writes **and**
reads the **tracker**.

### Tracker write ACL (the R12 control)

**Restricting tracker write access to the cesium principal is a normative deployment requirement**
(R12). The tracker topic is the durable scheduler state:

- a forged **ADD** record is an at-will **duplicate-injection** primitive;
- a forged completion **tombstone** is a **data-loss** primitive.

Actions:

- Set `route.tracker.acl-principal: User:<cesium-principal>`. With `route.tracker.bootstrap: CREATE`
  and a cluster authorizer present, cesium grants that principal write/read/describe on the tracker
  topic at bootstrap. (If the cluster has no authorizer, cesium logs that the ACL could not be
  applied — provision it yourself. Under `FAIL` bootstrap mode cesium does **not** apply the ACL —
  provision it out of band.)
- Ensure **no other principal** has write access to the tracker topic.

**The invalid-records counter is not a forgery detector.** cesium counts *structurally malformed or
version-skewed* tracker writes as `cesium_tracker_invalid_records_total` (logged at WARN); a nonzero
rate flags a malformed/version-skew foreign writer ([§13](#13-metrics-and-alerting)). It does **not**
detect well-formed forgeries — a wire-format-aware adversary with tracker write access can craft an ADD
or tombstone that decodes cleanly and is applied as legitimate state, and this counter never moves
(L2). The write-restricting ACL is the v1 control; **detecting competent tampering requires broker
authorizer audit logging** (who actually wrote the tracker) or the reserved, **deferred**
`store.kafka.hmac.*` record-level HMAC (config namespace reserved, not implemented in v1 — it only
helps in hostile clusters where cesium's own credentials are already suspect).

### Untrusted-producer ingress: avoid the `FAIL` delay policies (L5)

`delay.on-malformed-header: FAIL` and `delay.on-over-max: FAIL` (both non-default) are **unsafe when
the source topic has untrusted producers**: one crafted record fatally stops ingest, tears down all
workers, and exits non-zero; the offset never advances and `auto.offset.reset=none` is locked, so
restart re-polls and re-fails — a pipeline-wide, **restart-persistent outage**. Keep the `DLQ` defaults
(or `RELAY_IMMEDIATE`) for multi-tenant ingress. See [configuration.md §5](configuration.md#5-delay-protocol-delay).

### Observability port is unauthenticated (L1)

The observability HTTP server ([§13](#13-metrics-and-alerting); default port 8081) is read-only but
**completely unauthenticated** and **binds all interfaces by default** (`observability.bind-address:
0.0.0.0`) so k8s probes reach it. It now caps accepted connections (`jdk.httpserver.maxConnections`,
default 64) on top of the slow-client reaper (M1). **It MUST be network-restricted** — set
`observability.bind-address: 127.0.0.1` to restrict it to a loopback sidecar scrape (recommended when
off-host probes are not required), or keep `0.0.0.0` and apply a NetworkPolicy scoping port 8081 to the
monitoring namespace (the scraper) only. `/info` discloses the `application-id` (which seeds the M3
fencing ids), so that restriction is part of the M3 control.

See also [SECURITY.md](../SECURITY.md).

---

## 6. Tracker disaster recovery (runbook)

The tracker topic must never be silently recreated or truncated — an `earliest` reset on an empty
topic looks healthy while having vaporized all pending state. cesium guards against this:
committed-offset identity (cluster id, source/tracker topic ids) and offset-range sanity are checked
at takeover and periodically; it **never auto-resets** into an empty or out-of-range log.

**Symptoms / triggers:**

- Startup or runtime **fail-fast**: *tracker offset out of range … the tracker topic was truncated or
  recreated under the same name (§3.6 integrity, R-9). Never auto-reset — follow the tracker DR
  runbook.*
- Topic-ID mismatch fail-fast.
- A **step-collapse** of `cesium_pending_entries` (the integrity canary, R-9).

**If the tracker was lost (deleted / recreated / truncated):** the durable pending state is gone.
There is no way to reconstruct individual pending entries from a destroyed tracker. Choose, as an
**explicit operator decision** (cesium will not guess):

- **Accept the loss and re-bootstrap.** Recreate the tracker topic (`CREATE` mode), accept that
  entries pending at the moment of loss are not delivered, and resume. New scheduling works
  immediately.
- **Replay from the source if you can.** If your source topic still retains the relevant records and
  you can re-feed them (re-produce, or reset the ingest group to a point that re-ingests the affected
  window), the entries are re-scheduled. This risks delivering already-delivered records — only do
  it if a duplicate is acceptable for the affected window.

After recovery, audit why the tracker was lost (an out-of-band topic deletion, a cluster restore from
backup, an ACL gap, ...) and close the gap — a deliberate same-id cluster restore from backup is
**undetectable** by design (risk #12) and is the one case where you must reason about correctness
manually.

---

## 7. Offsets retention vs outage tolerance

cesium locks `auto.offset.reset=none` on both consumer groups (D18). After an outage longer than the
broker's `offsets.retention.minutes`, the committed offsets expire (KIP-211) and cesium **fails fast**
rather than silently losing (a `latest` reset) or mass-duplicating (an `earliest` reset).

- Set `startup-checks.max-tolerated-outage` (default `P7D`) to the longest outage you must survive.
  Startup checks it against the broker's `offsets.retention.minutes` and **FAILs** (or WARNs, per
  `startup-checks.outage-check`) if the broker would expire offsets sooner.
- **Ensure broker `offsets.retention.minutes` ≥ your `max-tolerated-outage`.** This is usually the
  fix: raise the broker setting (it defaults to 7 days on many clusters) to cover your worst planned
  downtime.
- The first run is special: see the [first-run / offset-reset runbook](#143-runtime-fail-fast-exit-1-readiness-false).

---

## 8. Scaling roles

`roles` selects which loops a process runs (`[ingest, dispatch]` by default). The two halves scale
**independently** on separate fleets:

- **Ingest fleet** (`roles: [ingest]`) — scales with source ingest throughput; stateless loops; up to
  the source partition count of useful parallelism.
- **Dispatch fleet** (`roles: [dispatch]`) — scales with dispatch + re-fetch throughput; owns the
  in-memory index shards; up to the **tracker partition count** of useful parallelism fleet-wide.

Per-instance parallelism is `ingest.workers` / `dispatch.workers` (default 1 each). Throughput is
ultimately bounded by transaction-commit latency per worker and, for dispatch, by seek-fetch I/O
([§12](#12-broker-iops-budgeting-for-seek-fetch)). Scale out by adding instances (more partition
owners) more than by raising workers.

> **Do not autoscale the dispatch fleet on CPU** — see the HPA warning in [§10](#10-kubernetes-rollout-recipe).

---

## 9. Growing partitions (runbook)

The tracker partition count must equal the source partition count. Growing the **source** without
first growing the **tracker** halts ingest by design (correct but sharp): a source record on a new
partition has no tracker partition to land its ADD on.

**Grow the tracker first:**

1. Increase the **tracker** topic partition count to the target.
2. Increase the **source** topic partition count to the same target.
3. (If using `route.relay.partitioning: SOURCE_PARTITION`, grow the destination to match too — it is
   validated equal at startup.)

Doing it in this order means producers never reach a new source partition before its tracker
partition exists. cesium's periodic partition-parity validator alerts on drift; the startup check
fails fast on a mismatch (I-7, R-7). Note that adding partitions changes key→partition hashing for
new records — existing pending entries are unaffected (they keep their original partition).

---

## 10. Kubernetes rollout recipe

The design's readiness model and static membership exist precisely to make rollouts safe (D21, R14).
Two rules make rolling restarts move **zero** partitions and avoid replay-multiplying churn.

### Static membership + a session timeout that outlives a pod restart

cesium derives `group.instance.id` from `instance-id` (static membership, default on). Give each pod
a **stable** `instance-id` and set the consumer `session.timeout.ms` **greater than a pod's restart
time**, so a restarting pod rejoins as the *same* member and reclaims its own partitions — no
rebalance, no partition movement, replay happens only on the returning member.

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
          image: cesium-kafka:1.0.0
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
              # Flip readiness false and drain before SIGTERM closes consumers cleanly.
              exec: { command: ["sh", "-c", "sleep 5"] }
  updateStrategy:
    type: RollingUpdate
```

Key points:

- **Readiness is NOT gated on recovery.** A healthily replaying instance reports ready; gating
  rollouts on replay completion wedges deploys behind replay durations and multiplies replay work.
  Recovery progress is surfaced in the `/health/ready` detail payload and `cesium_shard_paused`, not
  by failing the probe.
- **Liveness is loop-heartbeat freshness**, with a generous `failureThreshold` so a slow-but-healthy
  replay is never killed.
- **`preStop` + `terminationGracePeriodSeconds`** let readiness flip false and the loops finish/abort
  their open transaction at a batch boundary before consumers close (graceful, fenced shutdown).
- **Rolling restart moves zero partitions** because the returning member reclaims its own assignment
  within the session timeout.
- **Restrict the observability port (8081).** It is unauthenticated; add a NetworkPolicy scoping
  ingress on 8081 to the monitoring namespace (the Prometheus scraper) and the kubelet probe source
  only — see [§5](#5-least-privilege-deployment-tls-sasl-and-the-acl-matrix) / [§13](#13-metrics-and-alerting) (L1).

### HPA warning

**Do NOT autoscale the dispatch role on CPU.** Replay is CPU- and network-heavy, so a dispatch-role
HPA scaling out *during* recovery triggers a rebalance that multiplies replay work and can cascade.
Size the dispatch fleet to the tracker partition count and scale it deliberately, not reactively. (An
ingest-role HPA on lag is fine — ingest is stateless.)

---

## 11. JVM, heap, and GC

Full detail and the memory worksheet are in [performance.md §5–§6](performance.md); the operational
summary:

- **Heap sizing.** Plan the index at **64 B/entry** (typical) / **80 B** (worst); expect ~40 B
  measured. The recommended heap by scale:

  | Pending entries | Recommended heap | GC |
  |---|---|---|
  | 1 M | 512 MB – 1 GB | G1 (default) |
  | 10 M | 2 – 3 GB | G1 |
  | 100 M | 8 – 12 GB | ZGC generational |

  Add the **non-index** consumers (producer `buffer.memory` 64 MB × 2, ingest fetch buffers, the
  seek-fetch budget `dispatch.batch.max-bytes` 32 MiB plus one in-flight fetch response per broker ×
  decompression factor) and **+30 % G1 / +20 % ZGC headroom**. Container memory = heap + ~1 GB native.

- **Container flags.** The image sets `-XX:MaxRAMPercentage=60.0 -XX:InitialRAMPercentage=60.0
  -XX:+ExitOnOutOfMemoryError` (heap sized from the container memory limit; initial = max keeps the
  index resident). Extend via `JAVA_OPTS` / `CESIUM_KAFKA_OPTS`.

- **GC.** ≤ ~10 M entries / ≤ 4 GB heap: **G1** default, `-Xms=-Xmx`; add `-XX:+AlwaysPreTouch` for
  latency-critical installs. ≥ 8 GB / 100 M-entry scale: **ZGC generational**
  (`-XX:+UseZGC -XX:+ZGenerational` on JDK 21) — dispatch-accuracy p99 becomes independent of heap
  size. Keep **JFR continuous recording on** to catch footprint/pause regressions from production.

- **Backpressure.** `dispatch.max-pending-per-partition` pauses ACTIVE shards above its high-water
  (resumes below half); the global `dispatch.max-pending-total` (AUTO ≈ 25 % of `Xmx` ÷ 64 B) pauses
  ACTIVE shards above the total. A RECOVERING shard is **never** paused; if recovery could breach the
  heap, the startup heap-budget check (`startup-checks.heap-budget`) should have refused. Watch
  `cesium_shard_paused`.

---

## 12. Broker IOPS budgeting for seek-fetch

Payloads are never copied; cesium re-fetches them from the source at **dispatch time**. For long
delays this hits **cold (non-page-cache)** segments — budget broker disk IOPS for it (detail in
[performance.md §7](performance.md)):

- **One `seek` + a sequential forward scan serves all of a partition's due entries.** The midnight
  thundering-herd (10 k due from one partition) is **one sequential pass, not 10 k random seeks** —
  budget sequential read bandwidth for bursty due-sets.
- **Sparse due-sets across many partitions degrade toward random reads** — the harder IOPS case.
  Budget random-read IOPS for the worst-case scattered-due profile of your workload.
- **Tiered / remote storage** fetches are slow by construction; the byte-budget truncate-and-carry-over
  bounds heap regardless, but dispatch latency rises.
- A degraded source partition is put in the **penalty box** (`dispatch.fetch.penalty.backoff`,
  exponential `PT0.05S → PT10S`) so it does not head-of-line-block healthy partitions; watch
  `cesium_fetch_penalized_partitions` and `cesium_fetch_duration_seconds`.

If dispatch-time fetches are your bottleneck, provision faster broker disks for the source, keep the
source's hot set in page cache where delays are short, and consider source `delay.max` choices that
keep fetches warmer.

---

## 13. Metrics and alerting

cesium serves Prometheus metrics at `http://<host>:<observability.port>/metrics` (default port 8081),
plus `/health/live`, `/health/ready`, `/info`. The `/metrics` surface is the cesium engine inventory
below (the `cesium_*` meters) — meters carry only the **per-meter** tags shown in the table
(`loop`, `partition`, `outcome`, `event`, `type`, `result`, `cause`, `reason`). Two things design
`§9` describes are **not wired in this release** (verified against the registry wiring): there is
**no** `application_id` / `role` common tag, and the Kafka-client (`kafka_consumer_*` /
`kafka_producer_*`) and JVM/process binders are **not** registered — so `kafka_*` and `jvm_*` series
are absent. Do not write alerts against them; to obtain JVM/client metrics, run a JMX→Prometheus
exporter sidecar against the JVM.

> **This port is unauthenticated and MUST be network-restricted** — all four endpoints (and the
> `application-id` `/info` discloses) are readable by anyone with network reach. It binds all
> interfaces by default (`0.0.0.0`) and now caps accepted connections (`jdk.httpserver.maxConnections`,
> default 64) on top of the slow-client reaper (M1). Set `observability.bind-address: 127.0.0.1` to
> restrict it to loopback, or scope it with a Kubernetes NetworkPolicy (monitoring namespace only)
> ([§5](#5-least-privilege-deployment-tls-sasl-and-the-acl-matrix), L1).

### Metric inventory (as emitted — verified against the code)

In Prometheus exposition: **counters** end in `_total`; **timers** expose `_seconds_count`,
`_seconds_sum`, `_seconds_max` (and histogram buckets for the lag/fetch timers); **gauges** are bare.

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `cesium_ingest_records_total` | counter | `outcome=relayed_immediate\|scheduled\|clamped\|dlq` | ingest dispositions |
| `cesium_dispatch_records_total` | counter | `outcome=dispatched\|payload_expired\|dropped` | dispatch dispositions (flushed only after commit) |
| `cesium_dispatch_lag_seconds` | timer (histogram) | | actual − scheduled — the headline precision SLO |
| `cesium_dispatch_poll_gap_seconds` | gauge | | recent max time between group-B polls; alert ≪ `max.poll.interval.ms` |
| `cesium_txn_commit_seconds` | timer | `loop` | `commitTransaction` latency per attempt |
| `cesium_transactions_total` | counter | `loop`, `result=committed\|aborted\|in_doubt`, `cause` | aborts ⇒ fencing/contention; in-doubt occurrences |
| `cesium_header_errors_total` | counter | `type=malformed\|over_max\|conflict` | protocol violations |
| `cesium_dlq_records_total` | counter | `reason` | DLQ production |
| `cesium_ingest_rebalances_total` | counter | `event=assigned\|revoked\|lost` | group-A churn; `lost` = fenced/unclean |
| `cesium_dispatch_rebalances_total` | counter | `event=assigned\|revoked\|lost` | group-B churn; `lost` = fenced/unclean |
| `cesium_fetch_attempts_total` | counter | | seek-fetch attempts |
| `cesium_fetch_misses_total` | counter | | seek-fetch misses |
| `cesium_fetch_unfetchable_total` | counter | | provably-expired payloads |
| `cesium_fetch_bytes_total` | counter | | decompressed payload volume (budget observability) |
| `cesium_fetch_duration_seconds` | timer (histogram) | | per fetch pass; attributes cold-segment / degraded-broker cost |
| `cesium_fetch_penalized_partitions` | gauge | | penalty-box occupancy |
| `cesium_pending_entries` | gauge | `partition` | live index size; **step-collapse = tracker-integrity canary (R-9)** |
| `cesium_pinned_entries` | gauge | `partition` | sidecar occupancy; sustained at max ⇒ overflow mode |
| `cesium_cursor_sidecar_bytes` | gauge | `partition` | encoded sidecar size vs budget |
| `cesium_shard_paused` | gauge | `partition` | backpressure pause state |
| `cesium_degraded` | gauge | `loop` | park-and-degrade state (§3.8); cause is logged |
| `cesium_loop_last_iteration_timestamp_seconds` | gauge | `loop` | epoch-seconds of the last loop iteration; feeds liveness |
| `cesium_tracker_invalid_records_total` | counter | | tracker wire-format violations (malformed / version-skew) — foreign-writer canary. **Does NOT detect well-formed forgeries** ([§5](#5-least-privilege-deployment-tls-sasl-and-the-acl-matrix), L2) |
| `cesium_tracker_cancel_records_total` | counter | | reserved CANCEL records seen (no-ops in v1) |
| `cesium_tracker_unknown_reason_records_total` | counter | | tombstones with an unrecognized completion reason (applied) |
| `cesium_cursor_guard_violations_total` | counter | | I5 / monotonic cursor-guard trips (a surfaced bug — last safe cursor returned) |
| `cesium_store_index_anomalies_total` | counter | | duplicate ADDs / out-of-order drops (R1) |
| `cesium_store_heap_rebuilds_total` | counter | | dispatch-heap rebuilds across shards |
| `cesium_store_log_sweeps_total` | counter | | arrival-log sweeps across shards |

> **Not yet emitted (specified in design `§9`, deferred past M8).** Do **not** write alerts against
> these — the series do not exist in this release: `cesium_lso_lag`, `cesium_shard_state`,
> `cesium_replay_remaining_records`, `cesium_store_recovery_duration_seconds`,
> `cesium_store_replay_records_total`, `cesium_retention_margin_seconds`,
> `cesium_tracker_cursor_lag` / `cesium_tracker_cursor_age_seconds`,
> `cesium_pending_oldest_deadline_seconds`, `cesium_index_bytes_estimate`. Use the proxies noted
> below until they land. (`cesium_lso_lag` is explicitly deferred in the dispatch loop — it needs a
> per-partition read_uncommitted/read_committed offset diff from the admin plane.)

### Health endpoints

- **`/health/live`** — loop heartbeat freshness + thread liveness. A failing liveness means a loop
  stalled; the process should be restarted.
- **`/health/ready`** — startup checks passed, loops alive, consumers assigned, recent poll.
  **Recovery state is intentionally NOT part of readiness** — a replaying instance is ready. The
  detail payload exposes per-shard recovery state; a `degraded` flag (with cause) surfaces
  park-and-degrade without failing the probe.
- **`/info`** — version, commit, `application-id`, store type + capabilities, and any acknowledged
  escape hatches.

### Suggested alerts (real metrics only)

| Condition | Expression (sketch) | Meaning / action |
|---|---|---|
| Loop degraded | `cesium_degraded > 0` | Park-and-degrade (§3.8) — broker degradation outlasting retries. Page; check the logged cause (e.g. destination ISR shortage). Self-recovers when the dependency returns. |
| Loop stalled | `time() − cesium_loop_last_iteration_timestamp_seconds > N` | A loop stopped iterating. Page; check thread/JVM health. |
| Dispatch precision SLO | `histogram_quantile(0.99, cesium_dispatch_lag_seconds_bucket) > SLO` | Lateness over budget. Investigate commit/IO/GC tails ([performance.md](performance.md)). |
| Poll-gap approaching eviction | `cesium_dispatch_poll_gap_seconds > 0.5 × max.poll.interval.ms/1000` | Risk of group eviction under a due-storm. Lower `dispatch.drain.max-slice` or `dispatch.batch.max-entries`. |
| Transaction aborts | `rate(cesium_transactions_total{result="aborted"}[5m]) > threshold` | Fencing/contention churn. Correlate with rebalances. |
| In-doubt commits | `increase(cesium_transactions_total{result="in_doubt"}[15m]) > 0` | Ambiguous commits resolved by the I9 procedure. Investigate broker/commit latency. |
| Unclean rebalance | `increase(cesium_dispatch_rebalances_total{event="lost"}[15m]) > 0` | A member was fenced/lost. Check session timeouts and pod restarts. |
| Payload expiry | `rate(cesium_fetch_unfetchable_total[10m]) > 0` (sustained) | Payloads gone at dispatch — source retention too short or size/tier eviction. Raise source retention; inspect DLQ. |
| Degraded source partition | `cesium_fetch_penalized_partitions > 0` (sustained) | A source partition's leader/disk is degraded. Investigate broker. |
| Cold-fetch cost | `histogram_quantile(0.99, cesium_fetch_duration_seconds_bucket) > threshold` | Cold-segment / IOPS pressure ([§12](#12-broker-iops-budgeting-for-seek-fetch)). |
| Sidecar overflow (replay cost rising) | `cesium_pinned_entries` near `dispatch.cursor.sidecar-max-bytes/12` **and** `cesium_cursor_sidecar_bytes` near budget, sustained | Overflow mode (§3.5) — replay reverts to `completion_rate × pin age`. Raise `dispatch.cursor.sidecar-max-bytes` (with broker `offset.metadata.max.bytes`), or inspect long-delay producers. |
| Tracker integrity canary | step-collapse of `cesium_pending_entries` | Tracker possibly truncated/recreated (R-9). Follow [§6](#6-tracker-disaster-recovery-runbook). |
| Malformed tracker write / foreign writer | `rate(cesium_tracker_invalid_records_total[10m]) > 0` | A *malformed or version-skewed* write hit the tracker. Check the ACL ([§5](#5-least-privilege-deployment-tls-sasl-and-the-acl-matrix)). Note: a well-formed forgery would **not** move this counter — detecting competent tampering needs broker authorizer audit logging (L2). |
| Cursor-guard / index anomaly | `rate(cesium_cursor_guard_violations_total[15m]) > 0` or `rate(cesium_store_index_anomalies_total[15m]) > 0` | A surfaced invariant violation — not data loss (last-safe-cursor returned), but file a bug with logs. |
| Backpressure pause | `cesium_shard_paused > 0` (sustained) | Index near cap; intake paused. Add dispatch capacity or raise the heap / caps. |
| DLQ drain | `rate(cesium_dlq_records_total[10m]) > 0` | Inspect and drain the DLQ; correlate `reason`. |
| Header misuse | `rate(cesium_header_errors_total[10m]) > 0` | Producers sending malformed/over-max/conflicting headers — fix the producer. |

**Proxies for the not-yet-emitted metrics:** monitor **tracker-topic disk/partition size** and **group-B
consumer lag** with your broker tooling (the v2 cursor tracks position, so lag reads approximately
correctly except in sidecar-overflow mode) in place of `cesium_tracker_cursor_age_seconds` /
`cesium_replay_remaining_records`; monitor **source-partition earliest-record age** externally in
place of `cesium_retention_margin_seconds`; use `cesium_pinned_entries` + `cesium_cursor_sidecar_bytes`
for overflow detection.

---

## 14. Fail-fast runbook (every exit)

cesium never ends in a silent terminal state: every fault path is a retry, a park-and-degrade (with
`cesium_degraded` + alert, self-healing), or a **fail-fast with a runbook entry**. The fail-fast
exits:

- **Exit 78** (`EX_CONFIG`) — a configuration error, caught before the engine builds.
- **Exit 1** (`EX_FATAL`) — startup validation against the live cluster, or a runtime fatal; readiness
  is already false.

### 14.1 Configuration errors (exit 78)

The app prints the full aggregate report and exits 78. Causes and fixes:

| Cause | Fix |
|---|---|
| Unknown key (YAML or `CESIUM_`/`-D` overlay) | Typo or stale key — the report names the path. Correct it against [configuration.md](configuration.md). |
| Missing required key (`application-id`, `instance-id`, `route.source.topic`, `route.destination.topic`) | Provide it. |
| Locked Kafka key set in passthrough | Remove it — the message explains why it is engine-owned ([configuration.md §3](configuration.md#locked-kafka-keys-rejected-with-an-explanation)). |
| `${env:VAR}` references an undefined variable | Define the variable or fix the reference. |
| DLQ policy active but no `route.dlq.topic` | Configure the DLQ topic, or change the policy off DLQ. |
| Worst-case index footprint exceeds the heap budget | Lower `dispatch.max-pending-per-partition` / `store.properties.max-pending-per-partition`, raise the heap, or set `startup-checks.heap-budget: WARN` to accept. |
| `dispatch.cursor.sidecar-max-bytes` > broker `offset.metadata.max.bytes` | Lower the sidecar budget, or raise the broker setting. |

### 14.2 Startup validation against the cluster (exit 1)

Discovered after config binds, against the live cluster:

| Failure | Runbook |
|---|---|
| Source topic is compacted | cesium needs fetchable offsets — point at a non-compacted source. |
| Source `retention.ms` < `delay.max + margin` (`startup-checks.retention: FAIL`) | Raise source retention above `delay.max`, lower `delay.max`, or set the check to `WARN`/`SKIP` accepting the payload-expiry risk. |
| Size/tier eviction detected, not acknowledged | Set `startup-checks.size-based-retention: ACKNOWLEDGED` (named acceptance) or remove `retention.bytes`/tiered storage from the source. |
| Tracker topic missing in `FAIL` bootstrap mode | Pre-provision the tracker topic per [§1](#1-topics-and-bootstrap-create-vs-fail)/[§3](#3-correctness-load-bearing-tracker-settings), or use `CREATE`. |
| Tracker partition count ≠ source | Grow the tracker to match — [§9](#9-growing-partitions-runbook). |
| Tracker `cleanup.policy` ≠ `compact` (e.g. `compact,delete`) | Set `cleanup.policy=compact` — `compact,delete` causes silent loss (D4). |
| Tracker `delete.retention.ms` below the floor | Raise it to `≥ 2 × delay.max`, or lower `delay.max` and drain — [§4](#4-lowering-delaymax-runbook). |
| Tracker `min.compaction.lag.ms` below floor | Raise to `≥ max(2 × transaction.timeout.ms, 1 h)`. |
| Broker `offsets.retention.minutes` < `max-tolerated-outage` (`outage-check: FAIL`) | Raise broker `offsets.retention.minutes`, lower `max-tolerated-outage`, or set the check to `WARN` — [§7](#7-offsets-retention-vs-outage-tolerance). |
| `SOURCE_PARTITION` relay but destination partition count ≠ source | Grow the destination to match, or use `BY_KEY`. |
| **Source topic identity mismatch** (recreated under the same name) | The source was deleted and recreated (new topic id) — delivering old-offset payloads would be wrong (R-10/R17). Reset the tracker (accept loss) or restore the original source, as an explicit decision. |
| Tracker topic created but never became describable within the metadata-propagation budget | cesium created the tracker, then waited out normal KRaft propagation (10 s) and still could not get any broker to describe it with its full partition count. This is **not** the ordinary sub-millisecond window — that is already absorbed. Retry startup; if it recurs, investigate broker metadata lag (a stalled metadata publisher) or controller availability. A `WARNING` naming a multi-second propagation wait on a startup that *succeeded* is the early form of the same problem — [ADR-0018](adr/0018-bounded-wait-for-proven-topic-metadata.md). |
| A topic is reported as not existing that you are sure exists | cesium fails fast on operator-provisioned topics by design and does **not** wait out metadata propagation for them (the common cause is a typo, and waiting would slow that diagnosis down). If your provisioning pipeline creates the topic and starts cesium in the same breath, have it wait for the topic to become *describable* — not merely for `createTopics` to return — before launching cesium. |

### 14.3 Runtime fail-fast (exit 1, readiness false)

| Failure (and the actual message) | Runbook |
|---|---|
| **Ingest: no committed offset** — *no committed offset for assigned source partition(s) and auto.offset.reset=none is locked: either committed offsets expired (broker offsets.retention.minutes shorter than the outage) or this is a first run; an operator must seed group offsets explicitly* | **First run:** seed the ingest group's offsets to your chosen start (typically earliest) once, e.g. `kafka-consumer-groups.sh --group cesium.<application-id>.ingest --topic <source> --reset-offsets --to-earliest --execute`. **After an outage > offsets retention:** the operator chooses the reset point explicitly (earliest = reprocess/possible duplicates; a later point = skip). Raise broker `offsets.retention.minutes` to prevent recurrence ([§7](#7-offsets-retention-vs-outage-tolerance)). |
| **Dispatch: no committed cursor on a non-first run** | Same class as above for group B — committed cursors expired or were removed. The dispatch group seeks to beginning **only** on a provable first run; otherwise it fails fast. Treat as a tracker/offset integrity event — [§6](#6-tracker-disaster-recovery-runbook). |
| **Dispatch: tracker offset out of range** — *the tracker topic was truncated or recreated under the same name (§3.6 integrity, R-9). Never auto-reset — follow the tracker DR runbook* | The tracker was truncated/recreated. Follow [§6](#6-tracker-disaster-recovery-runbook). cesium will not auto-reset into an empty log. |
| **Committed-offset identity mismatch** (source recreated) at a runtime offset fetch | Same as the startup identity mismatch — [§14.2](#142-startup-validation-against-the-cluster-exit-1). |
| **Fatal producer error** (`ProducerFencedException`, `OutOfOrderSequenceException`, unrecoverable auth/config) | The clients close, the worker fails, the process exits non-zero (the durable log is authoritative for the successor). Restart the instance; if it recurs, check fencing (duplicate `instance-id`?), ACLs, and broker health. |
| **Partition-count drift** detected periodically (tracker ≠ source) | Fail-fast + alert. Grow the tracker first — [§9](#9-growing-partitions-runbook). |

### 14.4 Park-and-degrade (NOT an exit)

When definitively-abortable retries exhaust (e.g. the destination is under `NotEnoughReplicas` for an
extended period), the loop **parks** the batch (entries return to pending with a penalty not-before),
**keeps polling** (membership stays alive), raises `cesium_degraded{loop}` + an alert, and idles out
the broker degradation — no crash-loop, no replay storm. It self-recovers on the first successful
commit. Action: investigate and fix the failing dependency (destination ISR, broker health); cesium
resumes automatically.

---

For deeper background on any decision referenced here, see [docs/design.md](design.md) and the
failure-matrix coverage map in [docs/failure-matrix-coverage.md](failure-matrix-coverage.md).
