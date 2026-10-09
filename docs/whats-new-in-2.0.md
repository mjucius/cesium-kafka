# What's new in 2.0

2.0 is a cleanup release. The relay does the same job with the same exactly-once guarantees and
writes the same bytes to Kafka. What changes is the surface around it: fewer config keys, one
built-in store, one loop of each kind per process, a smaller build, and a batch of fixes. A rolling
upgrade from 1.1.5 is safe because 1.1.5 and 2.0 instances can run side by side.

This page is the overview. The [CHANGELOG](../CHANGELOG.md) has the complete list of changes.

---

## 1. What stays the same

- **Guarantees.** Exactly-once relay under `read_committed`, the replay barrier, and the in-doubt
  commit handling ([delivery-semantics.md](delivery-semantics.md)).
- **Architecture.** Two consumer groups, payloads left on the source topic, schedules in the
  compacted tracker topic ([architecture.md](architecture.md)).
- **Everything on the wire, byte for byte.**
  - The control headers ([header-protocol.md](header-protocol.md)).
  - The tracker record format.
  - The committed-cursor offset metadata.
  - Group ids, and the `transactional.id` and `group.instance.id` of each instance.
- **Performance.** The hot-path benchmarks and the per-entry memory footprint are unchanged
  ([performance.md](performance.md)).

## 2. What's new

- **Runtime re-validation** ([ADR-0020](adr/0020-runtime-revalidation.md)). A running engine re-runs
  its startup checks every `startup-checks.revalidation-interval` (default `PT5M`). If the source or
  tracker topic is recreated, or the cluster changes, the engine exits non-zero. Other findings set
  the `/health/ready` `degraded` flag and the new `cesium_revalidation_errors` gauge, and the relay
  keeps running.
- **New metrics.**
  - `cesium_revalidation_errors`.
  - `cesium_shard_state{partition}`.
  - `cesium_replay_remaining_records{partition}`.
- **Recovery detail in `/health/ready`.** It now reports per-shard recovery progress; in 1.x this
  was always empty.
- **Log context.** Log lines carry `applicationId`, `loop` and `txn`. In JSON output they appear as
  an `mdc` object.
- **Sidecar budget clamp.** Startup reads the broker's `offset.metadata.max.bytes`. If
  `dispatch.cursor.sidecar-max-bytes` exceeds it, startup warns and clamps the budget instead of
  failing at commit time.

## 3. What's simpler

- **One store, built in** ([ADR-0021](adr/0021-store-spi-folded-into-core.md)). The pluggable store
  SPI, `store.type` and `store.properties` are gone, and the Kafka tracker store is part of the
  engine. Alternative index or store implementations remain possible as code changes inside this
  repository.
- **One ingest loop and one dispatch loop per process**, selected by `roles`. To add capacity, add
  instances. The 1.x `*.workers` settings misbehaved above 1: extra dispatch loops raced each other
  on the shared store, and extra ingest loops fenced each other.
- **Fewer settings.** Eight config keys and the `-Dcesium.*` system-property layer are removed (see
  §4.2). Config precedence is now `defaults < YAML < CESIUM_* environment`.
- **cesium no longer grants ACLs.** `CREATE` bootstrap creates the tracker topic but not its write
  ACL. The cesium principal no longer needs `Cluster` ALTER. The ACL check at startup and during
  re-validation is unchanged.
- **Four build modules instead of seven:** core, app, it, benchmarks. None of the removed modules
  was ever published.

## 4. Upgrading from 1.x

### 4.1 Before you start

- **Be on 1.1.5.** Side-by-side safety is stated for 1.1.5. If you run an earlier 1.x, roll to 1.1.5
  first.
- **Save your 1.x config.** You'll need it if you roll back (§4.7).

### 4.2 Update the config

2.0 rejects removed keys at startup with exit code 78 and names the key. This applies to the
`CESIUM_*` environment form of each key as well.

| 1.x setting | In 2.0 |
|---|---|
| `store.type`, `store.properties` | Remove. Move `store.properties.cursor.sidecar-max-bytes` to `dispatch.cursor.sidecar-max-bytes`. The other store keys have no replacement: the heap budget comes from `-Xmx`, and the index floors are fixed. |
| `ingest.workers`, `dispatch.workers` | Remove. Run more instances instead (§4.6). |
| `ingest.max-batch` | Remove. The ingest consumer's `max.poll.records` keeps its default of 2000. To change it, set `max.poll.records` under `kafka.ingest-consumer.properties`. |
| `dispatch.coalesce` | Remove. It never had any effect. |
| `startup-checks.heap-budget` | Remove. Size the heap from [performance.md](performance.md). An explicit `dispatch.max-pending-total` above 25% of the heap is now a warning, not an error. |
| `headers.accept-binary-long-values` | Remove. Control headers must be ASCII decimal; see §4.3. |
| `-Dcesium.*` JVM properties | Move them to `CESIUM_*` environment variables or YAML. For example, `-Dcesium.instance-id=x` becomes `CESIUM_INSTANCE_ID=x`. **2.0 ignores these properties without any error**, so check for them explicitly. |

Two ranges are now enforced (exit 78 when outside them):
- `dispatch.cursor.sidecar-max-bytes` must be between 128 and 1048576. 1.x ignored this key entirely.
- `kafka.transactions.commit-retry` must be at least 1.

### 4.3 Check producers (only if you enabled binary headers)

If you set `headers.accept-binary-long-values: true`, every producer must now send
`cesium-delay-ms` and `cesium-deliver-at` as ASCII decimal. An 8-byte binary value is treated as
malformed and handled by `delay.on-malformed-header` (default `DLQ`).

### 4.4 Tracker write ACL

- **Existing trackers** keep the ACL that 1.x granted. Nothing to do.
- **New deployments** using `route.tracker.bootstrap: CREATE`: create the tracker write ACL yourself.
  A literal ACL can be created before the topic exists. See
  [operations.md §5, "Tracker write ACL"](operations.md#tracker-write-acl-the-r12-control).
- You can drop `Cluster` ALTER from the cesium principal.

### 4.5 Update dashboards and alerts

- **Log-based alerts.** The fatal log line changed:
  - Old: `fatal {role} loop failure — initiating engine shutdown`.
  - New: `fatal engine failure ({source}) — initiating engine shutdown`, where `{source}` is, for
    example, `ingest loop` or `re-validation`.
  - The exit line is now `engine exited on a fatal engine failure`.
- **`/health/ready`.** Each loop's detail loses its `assigned` field.
- **`/info`.** It loses its `store` object.
- **`cesium_dlq_records_total`.**
  - New `reason="payload-expired"` value.
  - `reason="unrelayable"` now also counts DLQ copies written by dispatch.
  - Expect higher counts than 1.x reported.
- **New alert.** Alert on `cesium_revalidation_errors > 0`. Runtime re-validation can set `degraded`
  for conditions 1.x only checked at startup ([operations.md §14.5](operations.md#145-re-validation-errors-not-an-exit)).

### 4.6 Roll out

1. Roll instances one at a time. Each 2.0 instance keeps its 1.x `transactional.id` and
   `group.instance.id`, so it fences its predecessor and rejoins as the same static member.
2. **If you ran `workers > 1`**, add instances to replace that capacity.
3. **If you set an explicit `dispatch.max-pending-total` while running several dispatch workers**,
   re-check it. It is now a per-process limit; 1.x applied it per worker.

### 4.7 Rolling back

2.0 writes the same bytes as 1.1.5, so rolling back is another rolling restart, onto 1.1.5 with your
saved 1.x config. Don't take the 2.0 config back to 1.1.5:
- 1.x rejects `startup-checks.revalidation-interval` as an unknown key.
- Without the `startup-checks.heap-budget` setting you removed, 1.x's heap check defaults to
  `FAIL`.

## 5. For contributors and source builds

- **Modules.** The `cesium-kafka-api`, `cesium-kafka-store-kafka` and `cesium-kafka-store-testkit`
  modules are folded into `cesium-kafka-core` with their package names unchanged. The store contract
  tests run with core's tests.
- **Extending the store.** The ingest and dispatch loops depend only on the engine-internal
  `TrackerBackedStore` interface. An alternative store or index:
  1. Implements that interface, or replaces `TrackerIndex` inside `KafkaTrackerStore`.
  2. Is wired in `CesiumEngine`.
  3. Must pass `TrackerBackedStoreContract`.
  4. Must publish the shard-state gauges the health sampler reads.

  See [ADR-0021](adr/0021-store-spi-folded-into-core.md).
- **Internal API changes.** `ClusterAdmin` gains `offsetsAtOrAfter`, so out-of-tree implementations
  must add it.
