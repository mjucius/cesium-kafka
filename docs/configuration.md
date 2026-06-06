# Configuration reference

Every cesium-kafka configuration key, its default, and what it means. This is the operator-facing
reference; the rationale behind the correctness-critical choices lives in
[docs/design.md](design.md) (the section numbers cited here, e.g. `§8`, point into it). For sizing
and tuning advice see [docs/operations.md](operations.md) and [docs/performance.md](performance.md).

The supported product surface is the **runnable app** (`cesium-kafka-app`). It reads a single YAML
file and applies the overlay grammar below. A complete, commented example is checked in at
[`config/cesium-example.yaml`](../config/cesium-example.yaml).

---

## 1. Load pipeline, precedence, and exit behavior

The app loads configuration in this order (design `§8`, `CesiumConfigLoader`):

1. **Parse the YAML file.** Path comes from `--config <file>` or the `CESIUM_CONFIG` environment
   variable (container default `/etc/cesium/cesium.yaml`).
2. **Reject unknown YAML keys** against the schema — every unknown key is collected (aggregate
   reporting), not just the first.
3. **Overlay environment variables** under the `CESIUM_` prefix (grammar below).
4. **Overlay system properties** under the `-Dcesium.` prefix — highest precedence.
5. **Interpolate `${env:VAR}`** references inside string values (keep secrets out of files); an
   undefined referenced variable is an error.
6. **Bind to immutable Java records.** Defaults materialize here, so precedence is
   **`defaults < YAML < environment < -D system properties`**.
7. **Validate** the whole config at once (every violation reported together).

**Any** configuration error — a missing required key, an unknown key, a locked Kafka key, an
out-of-range value, an undefined `${env:VAR}` — prints the full aggregate report and exits with code
**78** (`EX_CONFIG`, from BSD `sysexits.h`). A clean load still prints `WARNING`/`INFO` findings
(for example the always-present worst-case index-footprint line) and continues. Fatal problems
discovered *after* config binding — failed startup validation against the live cluster, engine
failures — exit with code **1**.

### Environment-variable overlay grammar

| Element | Rule | Example |
|---|---|---|
| Prefix | `CESIUM_` | `CESIUM_INSTANCE_ID` |
| Path separator | `__` (double underscore) between key segments | `CESIUM_ROUTE__SOURCE__TOPIC` → `route.source.topic` |
| Within a record-field segment | single `_` maps to `-` | `CESIUM_INSTANCE_ID` → `instance-id` |
| Within a map-key segment (e.g. `kafka.*.properties`) | single `_` maps to `.` | `CESIUM_KAFKA__PROPERTIES__BOOTSTRAP_SERVERS` → `kafka.properties.bootstrap.servers` |
| Reserved | `CESIUM_CONFIG` names the config *file*, not a key — the overlay skips it | `CESIUM_CONFIG=/etc/cesium/cesium.yaml` |

Unknown keys under the `CESIUM_` prefix are startup errors (exit 78), exactly like unknown YAML
keys — a typo'd override fails fast rather than being silently ignored.

System-property overlay uses dotted paths: `-Dcesium.route.source.topic=orders`,
`-Dcesium.instance-id=cesium-0`.

`${env:VAR}` interpolation works inside any string value, e.g.
`bootstrap.servers: ${env:KAFKA_BOOTSTRAP}` or a SASL JAAS string referencing `${env:KAFKA_PASSWORD}`.

---

## 2. Top-level keys

| Key | Default | Meaning |
|---|---|---|
| `application-id` | **required** | Namespaces consumer group ids, transactional ids, metric tags, and the default tracker topic name (`cesium.<application-id>.tracker`). One route per process keys off this. |
| `instance-id` | **required** (or the literal `random`) | Stable deployment-slot id (e.g. a StatefulSet ordinal). Keeps `transactional.id` and `group.instance.id` stable across restarts so a restarted slot immediately fences its own dangling transaction (D10) and rolling restarts move zero partitions (D21). `random` is an explicit opt-in that trades crash-failover latency for convenience. |
| `roles` | `[ingest, dispatch]` | Which loops this process runs. Give a fleet a single role (`[ingest]` or `[dispatch]`) to scale the halves independently. An empty set is a validation error. |

---

## 3. Kafka client configuration (`kafka.*`)

| Key | Default | Meaning |
|---|---|---|
| `kafka.group-protocol` | `classic` | `classic` or `consumer`. `consumer` is KIP-848; it is continuously tested (D12) but not the v1 default. |
| `kafka.properties` | `{}` | Common client passthrough applied to **every** cesium client (consumers, producers, admin). This is where `bootstrap.servers`, security, and SASL settings go. Locked keys (below) are rejected here. |
| `kafka.transactions.timeout` | `PT30S` | `transaction.timeout.ms`. Bounds the LSO-stall / barrier-gating window after a crash (D9). Raising it proportionally lengthens failover gating. |
| `kafka.transactions.commit-retry` | `5` | Bounded retries before an in-doubt `commitTransaction` resolves through the I9 procedure or the loop parks-and-degrades (§3.8). |
| `kafka.ingest-consumer.properties` | `{}` | Overlay for the group-A source consumer, layered over `kafka.properties`. |
| `kafka.tracker-consumer.properties` | `{}` | Overlay for the group-B tracker consumer. |
| `kafka.seek-consumer.properties` | `{}` | Overlay for the group-less payload seek-fetch consumer (`§7`). |
| `kafka.ingest-producer.properties` | `{}` | Overlay for the ingest transactional producer. |
| `kafka.dispatch-producer.properties` | `{}` | Overlay for the dispatch transactional producer. |
| `kafka.admin.properties` | `{}` | Overlay for the admin client (startup/periodic validation, barrier `listOffsets`). |

All overlay keys are **kebab-case** at the cesium level (`kafka.tracker-consumer.properties`); the
*values inside* a `properties` map are literal Kafka client property names (`max.poll.records`,
`fetch.max.bytes`, ...).

### Tuned client defaults (set by cesium, overridable where not locked)

cesium ships non-default tuning so the engine performs well out of the box (design `§8`):

- Producers: `linger.ms=10` (both the ingest and dispatch producers), `batch.size=256K`,
  `compression.type=lz4`, `buffer.memory=64M`. (design `§8` lists `10/5`; the implemented code uses
  `10/10` for both — see `KafkaClientFactory`.)
- Ingest consumer: `max.poll.records` follows `ingest.max-batch` (2000), `max.partition.fetch.bytes=4M`.
- Tracker consumer: `max.poll.records=10000`, `CooperativeStickyAssignor`, `group.instance.id`
  derived from `instance-id` (static membership, D21).
- Seek consumer: `max.partition.fetch.bytes=8M`, `fetch.max.bytes=64M` (mind the decompression-factor
  interaction with the heap budget — [operations.md](operations.md) §JVM & heap).

### Locked Kafka keys (rejected with an explanation)

These keys are owned by the engine because exactly-once correctness depends on them. Setting any of
them in `kafka.properties` or any overlay is a startup error (exit 78); the rejection message states
*why*. This eliminates the PoC's whole class of `Properties(defaults)` config drift by construction.

| Locked key | Forced value | Why it is locked |
|---|---|---|
| `group.id` | derived from `application-id` | An override would detach the ingest/dispatch groups from their transactional offset commits. |
| `group.instance.id` | derived from `instance-id` (D21) | Static-membership ids drive the zero-partition-movement rolling-restart recipe; an override breaks it. |
| `transactional.id` | `cesium.<application-id>.<role>.<instance-id>.<ordinal>` (D10) | Ad-hoc ids break immediate fencing of a predecessor's dangling transaction. |
| `enable.auto.commit` | `false` | Every offset commit travels inside a Kafka transaction via `sendOffsetsToTransaction`; auto-commit would advance offsets outside the transaction and break exactly-once. |
| `isolation.level` | `read_committed` (every cesium consumer, D17) | KIP-447 takeover ordering depends on the `require_stable` offset-fetch flag, which the consumer sets only under `read_committed`; `read_uncommitted` would also relay records from aborted upstream transactions. |
| `auto.offset.reset` | `none` (both consumer groups, D18) | After committed-offset expiry (KIP-211), an automatic reset silently loses (latest) or mass-duplicates (earliest); resets must be explicit operator decisions (see the offset-reset runbook in [operations.md](operations.md)). |
| `enable.idempotence` | `true` | A prerequisite of the transactional producer; disabling it breaks exactly-once. |
| `key.serializer` / `value.serializer` | byte-array | cesium relays keys/values as opaque bytes; serialization is engine-owned. |
| `key.deserializer` / `value.deserializer` | byte-array | cesium consumes keys/values as opaque bytes; deserialization is engine-owned. |

> The locked `auto.offset.reset=none` is why a brand-new deployment needs a one-time explicit
> offset seed for the ingest group — see the [first-run / offset-reset runbook](operations.md).

---

## 4. Route (`route.*`)

| Key | Default | Meaning |
|---|---|---|
| `route.source.topic` | **required** | The user-owned topic records are consumed from. Must not be compacted (offsets must stay fetchable) — startup-checked. |
| `route.destination.topic` | **required** | The user-owned topic records are relayed to at their due time. Consumers **must** use `read_committed` to observe exactly-once. |
| `route.tracker.topic` | `""` → `cesium.<application-id>.tracker` | Internal scheduler-state topic name. Leave blank for the default. |
| `route.tracker.bootstrap` | `CREATE` | `CREATE` provisions the tracker topic with the `§2.1` configs (compaction-only cleanup, tombstone-retention floor, compaction-lag floor), mirroring the source partition count, and applies the ACL when `acl-principal` is set. `FAIL` requires a pre-provisioned topic and validates its configs, failing fast on drift. |
| `route.tracker.acl-principal` | unset | Principal granted exclusive tracker write/read/describe access by `CREATE` bootstrap. Restricting tracker writes to the cesium principal is a **normative deployment requirement** (R12): a forged ADD is a duplicate-injection primitive, a forged tombstone is a data-loss primitive. |
| `route.dlq.topic` | unset | Dead-letter topic. **Required whenever any policy routes to DLQ** (the defaults do) — startup fails otherwise. Receives malformed-header, over-max, and payload-expired records. |
| `route.relay.timestamp` | `DISPATCH` | The timestamp a relayed record carries. `DISPATCH` (now) avoids violating the destination's `message.timestamp.difference.max.ms` and skewing time-based retention; the original time is recoverable from the `cesium-source-timestamp` provenance header. `SOURCE` preserves the original CreateTime. |
| `route.relay.partitioning` | `BY_KEY` | `BY_KEY` (destination partition count may differ) or `SOURCE_PARTITION` (partition counts validated equal at startup). |

---

## 5. Delay protocol (`delay.*`)

| Key | Default | Meaning |
|---|---|---|
| `delay.max` | `P1D` | Maximum accepted delay. **Load-bearing:** it drives the tracker tombstone-retention floor (`delete.retention.ms ≥ 2 × delay.max`) and therefore tracker disk. Lowered from P7D in the final design — raising it is an explicit, worksheet-reviewed decision ([operations.md](operations.md) sizing worksheet). Lowering it has a runbook (drain entries scheduled under the old maximum first). |
| `delay.on-over-max` | `DLQ` | Policy when a requested delay exceeds `delay.max`: `DLQ` (default), `CLAMP` (pin to `now + delay.max`, stamp `cesium-clamped: true`), or `FAIL`. |
| `delay.on-malformed-header` | `DLQ` | Policy when a control header fails the value grammar: `DLQ` (default), `RELAY_IMMEDIATE`, or `FAIL`. |

Delivering early a message someone intended to delay is a business hazard, so both default to DLQ —
the violation is made explicit while the pipeline stays alive (D3). DLQ policies require
`route.dlq.topic` to be configured.

---

## 6. Headers (`headers.*`)

| Key | Default | Meaning |
|---|---|---|
| `headers.stamp-provenance` | `true` | Stamp `cesium-relayed-at`, `cesium-source-*`, and `cesium-scheduled-for` provenance headers on relayed records. |
| `headers.accept-binary-long-values` | `false` | Accept 8-byte big-endian `long` control-header values **instead of** canonical ASCII decimal. The two modes are mutually exclusive because a length-8 value is ambiguous (D1). See [header-protocol.md](header-protocol.md). |

---

## 7. Scheduler store (`store.*`)

| Key | Default | Meaning |
|---|---|---|
| `store.type` | `kafka-tracker` | The ServiceLoader-registered store. Selection is always explicit. `kafka-tracker` is the flagship in-memory index durably backed by the tracker topic. Use `class:<FQCN>` to select a third-party store by class name. |
| `store.properties` | `{}` | The store's private namespace, opaque to the engine (exposed to the store via `ConfigView`). For the `kafka-tracker` store, see below. |

The `kafka-tracker` store reads these keys from `store.properties`:

| `store.properties` key | Default | Meaning |
|---|---|---|
| `max-pending-per-partition` | `2000000` | The store's own per-partition worst-case footprint cap, used in `validate()` (`partitionCount × max-pending-per-partition × 64 B` checked against the heap budget). Size it down for small containers (the quickstart uses `100000`). |
| `cursor.sidecar-max-bytes` | mirrors `dispatch.cursor.sidecar-max-bytes` (3072) | Store-side view of the sidecar budget. |

> The `kafka-tracker` store's `max-pending-per-partition` (footprint cap) and the engine's
> `dispatch.max-pending-per-partition` (the ACTIVE-shard pause/resume threshold) are distinct knobs
> that happen to share a name. Keep them aligned unless you have a specific reason not to.

---

## 8. Ingest loop (`ingest.*`)

| Key | Default | Meaning |
|---|---|---|
| `ingest.workers` | `1` | Ingest threads, each owning its own source consumer + transactional producer. |
| `ingest.max-batch` | `2000` | Maximum records per poll/transaction; becomes the source consumer's `max.poll.records`. |

---

## 9. Dispatch loop (`dispatch.*`)

| Key | Default | Meaning |
|---|---|---|
| `dispatch.workers` | `1` | Dispatch threads, each owning a tracker consumer + dispatch producer + seek consumer and disjoint shards. Useful up to the tracker partition count fleet-wide. |
| `dispatch.batch.max-entries` | `10000` | Maximum entries per dispatch transaction (D8). |
| `dispatch.batch.max-bytes` | `33554432` (32 MiB) | Decompressed payload byte budget per dispatch transaction, enforced in the fetch pass with truncate-and-carry-over (R8). |
| `dispatch.drain.max-slice` | `PT1M` | Maximum back-to-back transaction time before the loop returns to a real `poll()`. Validated `≤ max.poll.interval.ms / 3` when the poll interval is overridden — membership must survive due-storms (§6). |
| `dispatch.coalesce` | `PT0S` | Intentional dispatch coalescing window. Off by default: never early, never deliberately late. |
| `dispatch.idle-cursor-interval` | `PT30S` | How long a partition may go untouched before its cursor advances in a records-free transaction (§3.5). |
| `dispatch.cursor.sidecar-max-bytes` | `3072` | Pinned-entry sidecar budget in the offset metadata (§3.5). **Validated `≤` broker `offset.metadata.max.bytes` at startup.** ~200–300 pinned entries. |
| `dispatch.fetch.timeout` | `PT30S` | Overall seek-fetch deadline per batch (§7). |
| `dispatch.fetch.partition-time-floor` | `PT2S` | Minimum per-partition fetch time slice — one slow partition must not consume the whole deadline. |
| `dispatch.fetch.penalty.backoff` | `PT0.05S` | Initial per-source-partition penalty-box backoff after a TRANSIENT fetch outcome (§7.3, D22). |
| `dispatch.fetch.penalty.backoff-max` | `PT10S` | Penalty-box backoff ceiling under consecutive failures. |
| `dispatch.on-unfetchable-payload` | `DLQ` | Policy for provably-expired payloads: `DLQ` (loss notice + COMPLETE, atomic), `DROP` (COMPLETE only + metric), or `FAIL`. In all non-FAIL modes the COMPLETE is always written, so a poison entry never replays forever. |
| `dispatch.max-pending-per-partition` | `2000000` | Backpressure high-water mark per **ACTIVE** shard; pauses the tracker consumer above it, resumes below half. A RECOVERING shard is never paused. Backlog accumulates durably in the tracker topic. |
| `dispatch.max-pending-total` | `0` (AUTO) | Global pending cap. `0` (or unset) derives it from the heap budget: ≈ 25% of `Xmx` ÷ 64 B/entry. There is no writable `AUTO` literal — `0` is the sentinel. `validate()` cross-checks the worst case against the heap. |

---

## 10. Observability (`observability.*`)

| Key | Default | Meaning |
|---|---|---|
| `observability.port` | `8081` | HTTP port serving `/metrics`, `/health/live`, `/health/ready`, `/info` (`§9`). |

See [operations.md](operations.md) for the metric inventory and alert rules.

---

## 11. Startup checks (`startup-checks.*`)

| Key | Default | Meaning |
|---|---|---|
| `startup-checks.retention` | `FAIL` | Validate source `retention.ms` against `delay.max + margin`: `FAIL`, `WARN`, or `SKIP`. |
| `startup-checks.size-based-retention` | `FAIL` | When `retention.bytes != -1` or remote/tiered storage is enabled on the source, time-based validation cannot bound payload lifetime. Startup fails unless set to the explicit, named acceptance `ACKNOWLEDGED` (R13). |
| `startup-checks.max-tolerated-outage` | `P7D` | The longest outage after which committed offsets must still exist; checked against broker `offsets.retention.minutes` (D18, R6). |
| `startup-checks.outage-check` | `FAIL` | Strictness of the `max-tolerated-outage` vs broker offsets-retention check: `FAIL` or `WARN` only (`SKIP` is rejected). |
| `startup-checks.heap-budget` | `FAIL` | Strictness of the worst-case index-footprint vs heap-budget check (§5.3): `FAIL` or `WARN` only. |

Operator-acknowledged escape hatches (`size-based-retention: ACKNOWLEDGED`, any check set to `SKIP`
or `WARN`) shift responsibility to you. They are named, logged at startup, and surfaced in `/info`.

---

## 12. Durations and the schema

All durations are ISO-8601 (`PT30S`, `PT2S`, `PT1M`, `P1D`, `P7D`). Numeric byte budgets are plain
integers (`33554432` for 32 MiB). Enums are case-sensitive uppercase as shown (`DLQ`, `CLAMP`,
`DISPATCH`, `BY_KEY`, `FAIL`, `ACKNOWLEDGED`, ...).

The schema is strict: any key not in the schema — at any nesting level, from YAML or the env/`-D`
overlay — fails startup with exit 78 and the offending path named. There is no silent ignore.
