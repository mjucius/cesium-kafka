# Configuration reference

Every cesium-kafka configuration key, its default, and what it means. The rationale behind the
correctness-critical choices lives in [docs/design.md](design.md) (the section numbers cited here,
e.g. `§8`, point into it). For sizing and tuning advice see [docs/operations.md](operations.md) and
[docs/performance.md](performance.md).

The supported product surface is the runnable app (`cesium-kafka-app`). It reads a single YAML
file and applies the overlay grammar below. A complete, commented example is checked in at
[`config/cesium-example.yaml`](../config/cesium-example.yaml).

---

## 1. Load pipeline, precedence, and exit behavior

The app loads configuration in this order (design `§8`, `CesiumConfigLoader`):

1. Parse the YAML file. The path comes from `--config <file>` or the `CESIUM_CONFIG` environment
   variable (container default `/etc/cesium/cesium.yaml`).
2. Reject unknown YAML keys against the schema. Every unknown key is collected and reported
   together. A key repeated in the same mapping is also rejected, so a pasted duplicate cannot
   silently win.
3. Overlay environment variables under the `CESIUM_` prefix (grammar below; highest precedence).
4. Interpolate `${env:VAR}` references inside string values (keep secrets out of files). An
   undefined referenced variable is an error.
5. Bind to immutable Java records. Defaults materialize here, so precedence is
   `defaults < YAML < environment`.
6. Validate the whole config at once (every violation reported together).

There is no `-Dcesium.*` system-property overlay (removed in 2.0.0, ADR-0021).

Any configuration error (a missing required key, an unknown key, a locked Kafka key, an
out-of-range value, an undefined `${env:VAR}`) prints the full aggregate report and exits with code
78 (`EX_CONFIG`, from BSD `sysexits.h`). A clean load still prints `WARNING`/`INFO` findings
(for example the always-present index-cap line) and continues. Fatal problems
discovered after config binding, such as failed startup validation against the live cluster or
engine failures, exit with code 1.

### Environment-variable overlay grammar

| Element | Rule | Example |
|---|---|---|
| Prefix | `CESIUM_` | `CESIUM_INSTANCE_ID` |
| Path separator | `__` (double underscore) between key segments | `CESIUM_ROUTE__SOURCE__TOPIC` → `route.source.topic` |
| Within a record-field segment | single `_` maps to `-` | `CESIUM_INSTANCE_ID` → `instance-id` |
| Within a map-key segment (e.g. `kafka.*.properties`) | single `_` maps to `.` | `CESIUM_KAFKA__PROPERTIES__BOOTSTRAP_SERVERS` → `kafka.properties.bootstrap.servers` |
| List values | comma-separated | `CESIUM_ROLES=ingest,dispatch` |
| Reserved | `CESIUM_CONFIG` names the config file, so the overlay skips it | `CESIUM_CONFIG=/etc/cesium/cesium.yaml` |
| Reserved | `CESIUM_KAFKA_OPTS` holds JVM flags for the start script, so the overlay skips it | `CESIUM_KAFKA_OPTS="-Xmx4g"` |

Segments are matched case-insensitively (they are lowercased first). Because every `_` inside a
map-key segment becomes `.`, a map key that contains `-` or `_` cannot be set from the environment:
`CESIUM_KAFKA__PROPERTIES__MY_KEY` sets `kafka.properties.my.key`. Kafka client keys use only dots,
so this matters only for unusual map keys; set those in YAML.

Unknown keys under the `CESIUM_` prefix are startup errors (exit 78), like unknown YAML keys, so a
typo'd override fails fast instead of being ignored.

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
| `kafka.group-protocol` | `classic` | `classic` or `consumer`. `consumer` is KIP-848; it is continuously tested (D12) but not the default. This typed key wins over a raw `group.protocol` in any `properties` map. Under `consumer`, cesium strips the classic-only `partition.assignment.strategy`, `session.timeout.ms`, and `heartbeat.interval.ms` from the group consumers. |
| `kafka.properties` | `{}` | Common client passthrough applied to every cesium client (consumers, producers, admin). This is where `bootstrap.servers`, security, and SASL settings go. Locked keys (below) are rejected here. |
| `kafka.transactions.timeout` | `PT30S` | `transaction.timeout.ms` (wins over a raw `transaction.timeout.ms` in a `properties` map). Bounds the LSO-stall / barrier-gating window after a crash (D9). Raising it proportionally lengthens failover gating. |
| `kafka.transactions.commit-retry` | `5` | Attempts at a timed-out `commitTransaction` (at least 1) before the outcome is declared in-doubt and resolved through the I9 procedure (§3.8). |
| `kafka.ingest-consumer.properties` | `{}` | Overlay for the group-A source consumer, layered over `kafka.properties`. |
| `kafka.tracker-consumer.properties` | `{}` | Overlay for the group-B tracker consumer. |
| `kafka.seek-consumer.properties` | `{}` | Overlay for the group-less payload seek-fetch consumer (`§7`). |
| `kafka.ingest-producer.properties` | `{}` | Overlay for the ingest transactional producer. |
| `kafka.dispatch-producer.properties` | `{}` | Overlay for the dispatch transactional producer. |
| `kafka.admin.properties` | `{}` | Overlay for the admin client (startup validation, barrier `listOffsets`). |

All overlay keys are kebab-case at the cesium level (`kafka.tracker-consumer.properties`). The keys
inside a `properties` map are literal Kafka client property names (`max.poll.records`,
`fetch.max.bytes`, ...).

### Tuned client defaults (set by cesium, overridable where not locked)

cesium overrides these client defaults (design `§8`):

- Producers: `linger.ms=10` (both the ingest and dispatch producers), `batch.size=256K`,
  `compression.type=lz4`, `buffer.memory=64M`. (design `§8` lists `10/5`; the implemented code uses
  `10/10` for both; see `KafkaClientFactory`.)
- Ingest consumer: `max.poll.records=2000` (records per poll, so per ingest transaction), `max.partition.fetch.bytes=4M`.
- Tracker consumer: `max.poll.records=10000`, `CooperativeStickyAssignor` (classic protocol only).
- Ingest and tracker consumers: `group.instance.id` derived from `instance-id` (static membership,
  D21); omitted under `instance-id: random`.
- Seek consumer: `max.partition.fetch.bytes=8M`, `fetch.max.bytes=64M` (mind the decompression-factor
  interaction with the heap budget; see [operations.md](operations.md) §JVM & heap).

### Locked Kafka keys (rejected with an explanation)

These keys are owned by the engine because exactly-once correctness depends on them. Setting any of
them in `kafka.properties` or any overlay is a startup error (exit 78), and the rejection message
says why. This removes the PoC's `Properties(defaults)` config drift.

| Locked key | Forced value | Why it is locked |
|---|---|---|
| `group.id` | derived from `application-id` | An override would detach the ingest/dispatch groups from their transactional offset commits. |
| `group.instance.id` | derived from `instance-id` (D21) | Static-membership ids drive the zero-partition-movement rolling-restart recipe; an override breaks it. |
| `transactional.id` | `cesium.<application-id>.<role>.<instance-id>.0` (D10; the `.0` is kept from 1.x) | Ad-hoc ids break immediate fencing of a predecessor's dangling transaction. |
| `enable.auto.commit` | `false` | Every offset commit travels inside a Kafka transaction via `sendOffsetsToTransaction`; auto-commit would advance offsets outside the transaction and break exactly-once. |
| `isolation.level` | `read_committed` (every cesium consumer, D17) | KIP-447 takeover ordering depends on the `require_stable` offset-fetch flag, which the consumer sets only under `read_committed`; `read_uncommitted` would also relay records from aborted upstream transactions. |
| `auto.offset.reset` | `none` (both consumer groups, D18) | After committed-offset expiry (KIP-211), an automatic reset silently loses (latest) or mass-duplicates (earliest); resets must be explicit operator decisions (see the offset-reset runbook in [operations.md](operations.md)). |
| `enable.idempotence` | `true` | A prerequisite of the transactional producer; disabling it breaks exactly-once. |
| `key.serializer` / `value.serializer` | byte-array | cesium relays keys/values as opaque bytes; serialization is engine-owned. |
| `key.deserializer` / `value.deserializer` | byte-array | cesium consumes keys/values as opaque bytes; deserialization is engine-owned. |

> The locked `auto.offset.reset=none` is why a brand-new deployment needs a one-time explicit
> offset seed for the ingest group. See the [first-run / offset-reset runbook](operations.md).

### Passthrough keys that need care

These kafka-clients settings are accepted in any `properties` map, but they interact with cesium:

- **`bootstrap.resolve.timeout.ms`: leave it at the default `0`.** With `0`, each client resolves
  `bootstrap.servers` while it is being built, so a name that does not resolve stops cesium at
  startup with exit 1, as it did before kafka-clients 4.4. A positive value turns on Kafka's
  asynchronous bootstrap resolution, which Kafka marks as evolving. The client then retries DNS from
  its first network call until the timeout, and after that throws `BootstrapResolutionException`
  from every call until it is closed and rebuilt. cesium does not treat that exception as fatal, so
  the loop that owns the client retries with backoff indefinitely (for example
  `tracker poll failed; backing off`) instead of exiting, and only a restart recovers it. Passing
  startup validation does not rule this out: each client starts its own timer, and the seek
  consumer's first fetch can come long after startup.
- **Keep `connections.max.idle.ms` at or above `max.poll.interval.ms`.** Since kafka-clients 4.4, a
  consumer on the classic group protocol (the default, and always the group-less seek consumer) logs
  a WARN at startup when it is lower: *Configuration 'connections.max.idle.ms' with value '…' is
  lower than configuration 'max.poll.interval.ms' with value '…'*. The client can then close its
  idle connection to the group coordinator during a rebalance, which can slow or disrupt the rejoin.
  The consumer defaults (`connections.max.idle.ms` 540000 ms, `max.poll.interval.ms` 300000 ms)
  don't trigger it. If you raise `max.poll.interval.ms` past 540000, for
  example to allow a `dispatch.drain.max-slice` above `PT3M` ([§9](#9-dispatch-loop-dispatch)), raise
  `connections.max.idle.ms` to at least the same value in the same `properties` map.

---

## 4. Route (`route.*`)

| Key | Default | Meaning |
|---|---|---|
| `route.source.topic` | **required** | The user-owned topic records are consumed from. Must not be compacted (offsets must stay fetchable); checked at startup. |
| `route.destination.topic` | **required** | The user-owned topic records are relayed to at their due time. Consumers must use `read_committed` to observe exactly-once. |
| `route.tracker.topic` | `""` → `cesium.<application-id>.tracker` | Internal scheduler-state topic name. Leave blank for the default. |
| `route.tracker.bootstrap` | `CREATE` | `CREATE` provisions the tracker topic with the `§2.1` configs (compaction-only cleanup, tombstone-retention floor, compaction-lag floor), mirroring the source partition count. It never grants ACLs. `FAIL` requires a pre-provisioned topic and validates its configs, failing fast on drift. |
| `route.tracker.acl-principal` | unset | The cesium principal that `startup-checks.tracker-acl` verifies is the only tracker writer. cesium never grants the ACL; create it yourself (operations.md, tracker write ACL). Restricting tracker writes to the cesium principal is a normative deployment requirement (R12): a forged ADD is a duplicate-injection primitive, a forged tombstone is a data-loss primitive. |
| `route.dlq.topic` | unset | Dead-letter topic. Required whenever any policy routes to DLQ (the defaults do); startup fails otherwise. Receives malformed-header, over-max, payload-expired, and unrelayable records. |
| `route.relay.timestamp` | `DISPATCH` | The timestamp a relayed record carries. `DISPATCH` (now) avoids violating the destination's `message.timestamp.difference.max.ms` and skewing time-based retention; the original time is recoverable from the `cesium-source-timestamp` provenance header. `SOURCE` preserves the original CreateTime. |
| `route.relay.partitioning` | `BY_KEY` | `BY_KEY` (destination partition count may differ) or `SOURCE_PARTITION` (partition counts validated equal at startup). |
| `route.relay.on-unrelayable` | `DLQ` | Policy when the destination permanently rejects a relay on produce (record too large for the producer `max.request.size` or destination `max.message.bytes`, invalid record): `DLQ` (route the poison record to the DLQ with reason `unrelayable`, atomically with advancing past it, so the partition keeps moving), `DROP` (advance past it with a metric, no DLQ record), or `FAIL` (stop the loop). Applies to both the ingest immediate-relay and the dispatch delayed-relay path. Transient produce failures stay on the retry path. `DLQ` requires `route.dlq.topic`. To avoid the DLQ entirely, size destination `max.message.bytes ≥ source + provenance-header overhead`. |

---

## 5. Delay protocol (`delay.*`)

| Key | Default | Meaning |
|---|---|---|
| `delay.max` | `P1D` | Maximum accepted delay. It sets the tracker tombstone-retention floor (`delete.retention.ms ≥ 2 × delay.max`) and therefore tracker disk. Lowered from P7D in the final design; raising it is an explicit, worksheet-reviewed decision ([operations.md](operations.md) sizing worksheet). Lowering it has a runbook (drain entries scheduled under the old maximum first). |
| `delay.on-over-max` | `DLQ` | Policy when a requested delay exceeds `delay.max`: `DLQ` (default), `CLAMP` (pin to `now + delay.max`, stamp `cesium-clamped: true`), or `FAIL`. |
| `delay.on-malformed-header` | `DLQ` | Policy when a control header fails the value grammar: `DLQ` (default), `RELAY_IMMEDIATE`, or `FAIL`. |

A `cesium-delay-ms` delay counts from the source record timestamp, and a record already due when
ingest reads it is relayed at once ([header-protocol.md](header-protocol.md) §1.2, §1.5).

Delivering early a message someone intended to delay is a business hazard, so both default to DLQ:
the violation is visible and the pipeline stays alive (D3). DLQ policies require
`route.dlq.topic` to be configured.

> **`FAIL` is unsafe whenever the source topic has untrusted producers.** One crafted record (a
> non-decimal `cesium-delay-ms`, an over-`delay.max` value, a wrong-length binary value) fatally
> stops the ingest loop, tears down both loops in the process, and exits non-zero.
> Because the aborted batch never advanced the source offset and `auto.offset.reset=none` is locked,
> the next start re-polls the same poison record and re-fails. The result is a pipeline-wide outage
> that survives restarts until an operator seeks the group past the offset or changes the policy.
> Use only `DLQ` (default) or `RELAY_IMMEDIATE` for multi-tenant / untrusted ingress (L5; see
> [operations.md §5](operations.md) and [SECURITY.md](../SECURITY.md)).

---

## 6. Headers (`headers.*`)

| Key | Default | Meaning |
|---|---|---|
| `headers.stamp-provenance` | `true` | Stamp `cesium-relayed-at`, `cesium-source-*`, and `cesium-scheduled-for` provenance headers on relayed records. |

---

## 7. Scheduler store

There are no `store.*` keys: since 2.0.0 the tracker store is part of the engine, and `store.type`
and `store.properties` are unknown keys (exit 78; ADR-0021). Its sidecar budget is
`dispatch.cursor.sidecar-max-bytes` (§9), its recovery ceiling uses the same 25%-of-heap budget as
`dispatch.max-pending-total`, and its index maintenance floors are fixed.

---

## 8. Ingest loop

There are no `ingest.*` keys: each instance runs at most one ingest loop and one dispatch loop
(selected by `roles`), and you scale either half by adding instances. `ingest.workers`,
`dispatch.workers` and `ingest.max-batch` were removed in 2.0.0 (exit 78 if set; ADR-0021).

---

## 9. Dispatch loop (`dispatch.*`)

| Key | Default | Meaning |
|---|---|---|
| `dispatch.batch.max-entries` | `10000` | Maximum entries per dispatch transaction (D8). |
| `dispatch.batch.max-bytes` | `33554432` (32 MiB) | Decompressed payload byte budget per dispatch transaction, enforced in the fetch pass with truncate-and-carry-over (R8). |
| `dispatch.drain.max-slice` | `PT1M` | Maximum back-to-back transaction time before the loop returns to a real `poll()`. Always validated `≤ max.poll.interval.ms / 3`, using the tracker consumer's effective `max.poll.interval.ms` (overlay, then `kafka.properties`, else the client default 300000 ms), so membership survives due-storms (§6). |
| `dispatch.idle-cursor-interval` | `PT30S` | How long a partition may go untouched before its cursor advances in a records-free transaction (§3.5). |
| `dispatch.cursor.sidecar-max-bytes` | `3072` | Pinned-entry sidecar budget in the offset metadata (§3.5), in `[128, 1048576]` (outside is exit 78). Checked against broker `offset.metadata.max.bytes` at startup: a larger budget logs a warning and is clamped to the broker cap (never a startup failure). ~200–300 pinned entries. |
| `dispatch.fetch.timeout` | `PT30S` | Overall seek-fetch deadline per batch (§7). |
| `dispatch.fetch.partition-time-floor` | `PT2S` | Minimum per-partition fetch time slice, so one slow partition cannot consume the whole deadline. |
| `dispatch.fetch.penalty.backoff` | `PT0.05S` | Initial per-source-partition penalty-box backoff after a TRANSIENT fetch outcome (§7.3, D22). |
| `dispatch.fetch.penalty.backoff-max` | `PT10S` | Penalty-box backoff ceiling under consecutive failures. |
| `dispatch.on-unfetchable-payload` | `DLQ` | Policy for provably-expired payloads: `DLQ` (loss notice + COMPLETE, atomic), `DROP` (COMPLETE only + metric), or `FAIL`. In all non-FAIL modes the COMPLETE is always written, so a poison entry never replays forever. |
| `dispatch.max-pending-per-partition` | `2000000` | Backpressure high-water mark per ACTIVE shard; pauses the tracker consumer above it, resumes below half. A RECOVERING shard is never paused. Backlog accumulates durably in the tracker topic. |
| `dispatch.max-pending-total` | `0` (AUTO) | Process-wide pending cap across every ACTIVE shard this instance owns. `0` (or unset) derives it from the heap budget: ≈ 25% of `Xmx` ÷ 64 B/entry. There is no writable `AUTO` literal; `0` is the sentinel. A non-zero cap whose `cap × 64 B` exceeds the heap budget is reported as a warning at load. This cap, together with the store's recovery ceiling, is what bounds the index; there is no separate startup footprint check (ADR-0021). |

---

## 10. Observability (`observability.*`)

| Key | Default | Meaning |
|---|---|---|
| `observability.port` | `8081` | HTTP port serving `/metrics`, `/health/live`, `/health/ready`, `/info` (`§9`). Range 1-65535. |
| `observability.bind-address` | `0.0.0.0` | Interface the HTTP server binds. Must be an IP literal or `localhost` (no DNS lookup). The endpoints are unauthenticated: set `127.0.0.1` for a loopback-only scrape, or network-restrict the port. |
| `observability.detailed-info` | `false` | When `true`, `/info` also returns `applicationId`, `roles`, and acknowledgments. By default it returns only version and git commit. |

See [operations.md](operations.md) for the metric inventory and alert rules.

---

## 11. Startup checks (`startup-checks.*`)

The strictness knobs below apply at startup and to the runtime re-validation rounds. At runtime only
an ERROR finding degrades; a `WARN`-mode finding is logged once when it appears.

| Key | Default | Meaning |
|---|---|---|
| `startup-checks.retention` | `FAIL` | Validate source `retention.ms` against `delay.max` plus a 1-hour margin (`retention.ms=-1` passes): `FAIL`, `WARN`, or `SKIP`. |
| `startup-checks.size-based-retention` | `FAIL` | When `retention.bytes != -1` or remote/tiered storage is enabled on the source, time-based validation cannot bound payload lifetime. Startup fails unless set to the explicit, named acceptance `ACKNOWLEDGED` (R13). |
| `startup-checks.max-tolerated-outage` | `P7D` | The longest outage after which committed offsets must still exist; checked against broker `offsets.retention.minutes` (D18, R6). |
| `startup-checks.outage-check` | `FAIL` | Strictness of the `max-tolerated-outage` vs broker offsets-retention check: `FAIL` or `WARN` only (`SKIP` is rejected). |
| `startup-checks.revalidation-interval` | `PT5M` | How often a running engine re-runs these checks on the `cesium-admin` thread (D23, ADR-0020); must be at least `PT0.001S`. A source or tracker topic id change exits non-zero; every other error is logged at ERROR, counted in `cesium_revalidation_errors` and sets the readiness degraded flag, and dispatch continues. It never creates a topic or applies an ACL. |
| `startup-checks.tracker-acl` | `WARN` | Strictness of the R12 tracker write-ACL verification: `FAIL`, `WARN`, or `SKIP`. `FAIL` refuses to start unless only the cesium principal can write the tracker (it also fails on an unset `acl-principal` or a cluster without an authorizer). `FAIL` is recommended for production. |

Operator-acknowledged escape hatches (`size-based-retention: ACKNOWLEDGED`, any check set to `SKIP`
or `WARN`) shift responsibility to you. They are named and logged at startup. `/info` lists
`size-based-retention` under `acknowledgments` when it is acknowledged, and only when
`observability.detailed-info: true`.

---

## 12. Durations and the schema

All durations are ISO-8601 (`PT30S`, `PT2S`, `PT1M`, `P1D`, `P7D`). cesium uses them in whole
milliseconds, so each must be at least `PT0.001S`; a shorter one fails startup with exit 78.
Numeric byte budgets are plain integers (`33554432` for 32 MiB). Enum values are case-insensitive
(`DLQ`, `dlq`, and `Dlq` all bind); this reference writes them as shown (`DLQ`, `CLAMP`,
`DISPATCH`, `BY_KEY`, `FAIL`, `ACKNOWLEDGED`, `classic`, ...). The `instance-id` literal `random`
is case-sensitive.

The schema is strict: any key not in the schema, at any nesting level, from YAML or the env
overlay, fails startup with exit 78 and names the offending path. The exceptions are the open maps
(`kafka.*.properties`), which accept any key; locked Kafka keys are still rejected there.

An unknown key gets a did-you-mean hint when it differs from a real key only by case or hyphens
(`maxEntries` → `max-entries`). Two older top-level spellings are pointed at their real homes:
`transactions` → `kafka.transactions` and `relay` → `route.relay`.
