# Changelog

All notable changes to this project will be documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.1.1] - 2026-10-23

Patch release. kafka-clients moves to 4.4.0, lz4-java and snappy-java move past two CVEs, and
configuration.md covers two new kafka-clients settings. Apache Kafka had tagged 4.4.0 and published
it to Maven Central and Docker Hub, but had not yet announced it, when 2.1.1 shipped. Config keys,
headers and the tracker wire format are unchanged from 2.1.0.

### Added
- configuration.md §3 has a new "Passthrough keys that need care" subsection for two kafka-clients
  4.4 settings. `bootstrap.resolve.timeout.ms` should stay at its default `0`: a positive value makes
  a DNS failure that outlasts the timeout permanent for that client, and cesium does not treat the
  resulting `BootstrapResolutionException` as fatal, so the owning loop retries with backoff until
  the process is restarted. Classic-protocol consumers now log a WARN when `connections.max.idle.ms`
  is below `max.poll.interval.ms`; the defaults don't trigger it, but raising `max.poll.interval.ms`
  past 9 minutes, for example to allow a `dispatch.drain.max-slice` above `PT3M`, does unless
  `connections.max.idle.ms` is raised with it.

### Changed
- Runtime dependency (Dependabot PR [#34]): kafka-clients 4.3.1 → 4.4.0. It brings zstd-jni 1.5.6-10
  → 1.5.7-17 and lz4-java 1.10.2 → 1.11.2 into the distribution archives, and it bundles a shaded
  Apache Commons Net 3.13.0, which NOTICE now records. Per
  [ADR-0017](docs/adr/0017-kafka-4-floor-and-repo-only-publishing.md) only the distribution is
  published, so for cesium this is a patch-level change. The jar on Maven Central is the voted
  4.4.0-rc4 build. Its embedded commit id is `1156b27`, and its sha512 and the release manager's
  signature check out. Upstream had not yet pushed a final `4.4.0` git tag or announced the release
  when this merged; the `4.4.0` tag that followed points at the same commit.
- The §3.8 error taxonomy was re-checked against the 4.4.0 source. Every exception class it names
  keeps its superclass chain, the `Producer` and `MockProducer` APIs are unchanged, and the
  transaction manager's begin, commit, abort and init paths are untouched. Two transactional changes
  reach cesium. `sendOffsetsToTransaction` now waits for producer metadata on the offsets' topics
  before it sends, inside the same `max.block.ms` budget; a timeout there is a failure before the
  commit call, so the transaction aborts as before. Against a 4.4 broker, which finalizes
  `transaction.version=2` by default, the offset commit uses TxnOffsetCommit v6 (KIP-1319). Its new
  `GROUP_ID_NOT_FOUND` and `STALE_MEMBER_EPOCH` errors surface as the abortable
  `CommitFailedException` that `ILLEGAL_GENERATION` already produces. Under
  `group.protocol=consumer` the classic-only keys are still rejected, and the member epoch still
  reaches `ConsumerGroupMetadata` the same way
  ([ADR-0006](docs/adr/0006-classic-protocol-default-and-kip-848-promotion.md)). Every lane in
  `ci.yml` and `nightly.yml` passed locally against the `apache/kafka:4.3.0` broker the ITs pin, and
  the broker-backed lanes (Batches A–C, macro perf, KIP-848, `SoakPerfIT`, `CrashLoopSoakIT`) also
  passed against `apache/kafka:4.4.0-rc4`, which negotiates TxnOffsetCommit v6.
  **Operator note:** kafka-clients 4.4 logs
  `Connection to node … is throttled` at WARN instead of TRACE when a principal hits a broker quota.
- Test and quickstart broker: `apache/kafka:4.3.0` → `4.4.0`, in the integration tests and the
  digest-pinned `config/docker-compose.yaml`. Kafka published the `4.4.0` image as the same image as
  `4.4.0-rc4` (index digest `833b867a`), and every lane in `ci.yml` and `nightly.yml` passed locally
  against it.
- Build/test only ([#34]): jqwik 1.10.1 → 1.10.2 and Spotless 8.10.3 → 8.10.4. Apart from the
  version string in `module-info`, jqwik's engine classes are byte-identical (the release adds Error
  Prone's `@Keep` to `@Provide`), so generation, shrinking and the `@Example` regression cases
  behave as before. Spotless's new default formatter versions don't apply here, because
  palantir-java-format stays pinned at 2.68.0; `spotlessCheck` passes without reformatting.
- SHA-pinned `actions/setup-java` v6.0.1 → v6.1.0 (Dependabot PR [#35]). The lightweight `v6.1.0`
  tag and the floating `v6` both resolve to `b843ebf`, a nine-commit fast-forward from v6.0.1.
  Nothing in it reaches this repo's usage: Temurin 21 and 25 from the hosted tool cache, 21
  installed last so it stays `JAVA_HOME`, and no `cache:` input. The PR's CI lanes resolved the same
  JDKs as main (21.0.12+1 and 25.0.4+1). `release.yml`'s `verify-signature: true` runs only when a
  JDK is downloaded, and that code is unchanged; the comment above it now reads "Since v6.0.1".
- Dependabot now waits before proposing a release (`cooldown` in `.github/dependabot.yml`): 21 days
  for gradle minor and major bumps, 7 for gradle patches and GitHub Actions. For Maven artifacts
  Dependabot dates a release by its Maven Central directory listing, which for Apache projects is
  the day the release candidate was staged. kafka-clients 4.4.0 is listed 2026-10-06, was promoted
  2026-10-13 and was still unannounced when [#34] opened on 2026-10-18, so a 7-day window would not
  have held it. Security fixes wait too, because Dependabot security updates are disabled on this
  repository; bump those by hand.
- Dependabot ignores minor and major bumps of lz4-java and snappy-java while `cesium-kafka-core`
  constrains them, so the codecs stay on the lines Kafka itself ships; patch releases still arrive.
  Dependabot PR [#36] (lz4-java 1.11.4 → 1.12.0) was closed for this reason: 1.12.0 tightens input
  validation, and Kafka has not adopted it. The ignore entries go when the constraints do.

### Security
- lz4-java and snappy-java, the compression codecs kafka-clients pulls in, are now required at
  1.11.4 and 1.1.10.12, above the 1.11.2 and 1.1.10.7 that kafka-clients 4.4.0 declares. lz4-java
  before 1.11.4 extracts its JNI library to a predictable path in the temp directory, so another
  local user who can write there can race the extraction and get native code loaded into cesium
  (CVE-2026-106451). cesium's producers compress with lz4, so every deployment loads that library.
  The Docker image runs as the single `cesium` user with a container-private `/tmp`; a distribution
  archive on a shared host is the exposed case. snappy-java before 1.1.10.9 lets a few crafted bytes
  force allocations of up to 2 GB through `SnappyInputStream` (CVE-2026-108106), which kafka-clients
  uses to decompress snappy batches that other producers write to a source topic. Neither came with
  the 4.4.0 bump: 2.1.0 shipped lz4-java 1.10.2 and the same snappy-java 1.1.10.7. Kafka chose these
  versions on its own branches (lz4-java 1.11.4 in KAFKA-21174, snappy-java 1.1.10.12 on 4.4). The
  constraints sit in `cesium-kafka-core` and set a floor, so a later kafka-clients that declares
  newer versions wins; the catalog comment says when to drop them. zstd, lz4, snappy and gzip
  batches written by the 4.3.1 and 4.4.0 client jars decode with the pinned codecs on macOS and in
  the `eclipse-temurin:21-jre` runtime image on linux/arm64 and linux/amd64, and every lane in
  `ci.yml` and `nightly.yml` passed locally.

## [2.1.0] - 2026-10-12

Minor release. One new metric, two fixes for bugs present since 1.0, and a nightly crash-loop soak
that kills real processes. Config keys, headers and the tracker wire format are unchanged from
2.0.0.

### Added
- `cesium_pending_oldest_deadline_seconds`, an untagged gauge: how long the most overdue pending
  entry on an ACTIVE shard has been due, or `0` when nothing is due. `cesium_dispatch_lag_seconds`
  only times entries that went out; this gauge sees due entries held back by a slow destination,
  a source partition in the penalty box, or a stalled dispatch loop. Penalty-boxed entries count;
  RECOVERING shards are left out, so restarts and takeovers don't spike it. operations.md §13 adds
  an "Overdue backlog" alert.
- Test infrastructure: `CrashLoopSoakIT`, the design §11.3-12 crash-loop soak that was never built.
  It runs three app processes, each in its own JVM, under delayed load for 6 minutes, SIGKILLs them
  and halts some at armed §3.9 crash points. Then it checks that every record arrived exactly once
  and never early, and that the DLQ is empty. It runs nightly in the `soak` job, which now calls
  `soakPerf` once per soak IT. `SoakPerfIT` shares its invariant checker. Commands and knobs are in
  performance.md §8.
- Test infrastructure: `SubSecondDelayIT` measures sub-second delays where a consumer sees them.
  A trickle asks for `cesium-delay-ms` 0, 50, 300 and 900, and a `read_committed` consumer stamps
  when it first sees each relay. The test asserts exactly-once and never-early, logs p50/p99/max
  lateness per delay, and asserts no latency bound. It is untagged, so it runs on the PR lane;
  performance.md §3.4 has the numbers.

### Changed
- Design §9 drops four metrics that were specified but never emitted: `cesium_lso_lag`,
  `cesium_store_recovery_duration_seconds`, `cesium_store_replay_records_total` and
  `cesium_index_bytes_estimate`; each row gives its reason (revision 6). operations.md §13 loses
  its "Not yet emitted" block and its promise of proxy metrics, and gains a note on confirming a
  tracker LSO stall by hand.
- Build only (Dependabot PR [#31]): Gradle 9.8.0 → 9.8.1. The distribution and the committed
  `gradle-wrapper.jar` match Gradle's published SHA-256 checksums. Dependabot left the
  `gradle-wrapper.properties` comment describing 9.8.0; corrected.
- SHA-pinned `actions/upload-artifact` v7.0.1 → v7.0.2 (Dependabot PR [#32]). The pin `cf430e0` is
  the commit the `v7.0.2` tag resolves to.
- Core (internal): `DispatchAdmin.groupHasCommittedOffsets` is removed, and `KafkaDispatchAdmin`
  no longer takes the dispatch group id.
- Design open questions 3, 5 and 6 are resolved (revision 6). There is no cancellation API for now;
  the `CANCEL 0x02` reservation stays, and design.md records a minimal design for later.
  Sub-second delays are best-effort and never early, with no early-relay threshold:
  delivery-semantics.md, performance.md §3.4, migration-from-poc.md, configuration.md and
  header-protocol.md now say so, with measured lateness in place of the unmeasured ~100–500 ms
  estimate. There is no admin endpoint; operations stays metrics-only. design.md §8 and §9 no
  longer list store type or store capabilities in `/info`.
- performance.md traces the `dispatch_lag` p99 miss (~480 ms against the 250 ms SLO) to the seek
  consumer's `fetch.max.wait.ms`, which defaults to 500 ms, instead of commit, fsync or GC stalls.
  With it at 50 ms, two diagnostic runs measured p99 at 53–55 ms. The default is unchanged. The
  operations.md precision alert now names this cause first.

### Fixed
- Since 1.0, setting `CESIUM_KAFKA_OPTS`, which the Dockerfile and operations.md advertise for JVM
  flags, made startup exit 78. The environment overlay read it as an override of an unknown key
  `kafka-opts`. The overlay now skips the variable, as it already skipped `CESIUM_CONFIG`.
- Since 1.0, a dispatch loop exited when it was assigned a tracker partition with no committed
  cursor while the dispatch group had committed on another partition. On a fresh multi-instance
  deployment under live traffic, a cooperative rebalance that moved partitions before their first
  cursor commit could stop every dispatch instance until an operator seeded the dispatch group.
  Tracker growth stopped dispatch the same way. The loop now replays such a partition from its
  beginning with a WARN (ADR-0022). Behavior change: deleting the dispatch group's offsets for
  specific tracker partitions no longer stops dispatch; those partitions replay from the beginning.
  The growth runbook (operations.md §9) no longer seeds the dispatch group and ends by checking
  that every tracker partition has an owner; the ingest side is unchanged.

## [2.0.0] - 2026-10-11

Cleanup and fixes from an over-engineering audit and a docs-versus-code audit, released as 2.0.0
(ADR-0019): with no external users yet, the fixes take the clean form even where that breaks the
api, testkit, config keys, metrics or log shape. Every break is listed under "Breaking changes".
Wire formats are unchanged, so a mixed 1.1.5/2.0.0 rolling restart is safe.
HTTP bodies are the same JSON, though an escaped control character there would now use uppercase hex.

### Upgrading from 1.x
[docs/whats-new-in-2.0.md](https://github.com/mjucius/cesium-kafka/blob/v2.0.0/docs/whats-new-in-2.0.md) walks through these steps, plus dashboards and
rollback.
1. Remove keys 2.0.0 rejects at startup (exit 78): `dispatch.coalesce`,
   `startup-checks.heap-budget`, `headers.accept-binary-long-values`, and the whole `store` block
   (`store.type`, `store.properties`). Move a `store.properties.cursor.sidecar-max-bytes` value to
   `dispatch.cursor.sidecar-max-bytes`. Remove `ingest.workers`, `ingest.max-batch` and
   `dispatch.workers` (the whole `ingest` block); scale either loop by adding instances. Move any
   `-Dcesium.*` JVM properties to `CESIUM_*` environment variables; 2.0.0 ignores them.
2. Roll instances one at a time. The tracker wire format, committed-cursor offset metadata and
   worker-0 `group.instance.id`s are unchanged, so 1.1.5 and 2.0.0 instances can run side by side.
3. Expect runtime re-validation (every `startup-checks.revalidation-interval`, default PT5M) to set
   `/health` degraded for conditions 1.x only checked at startup; see operations.md §14.5.
4. `CREATE` bootstrap no longer grants the tracker write ACL. A tracker 1.x created keeps its ACL;
   for new deployments, create it yourself first (operations.md, "Tracker write ACL").
5. Store implementers: there is no store SPI any more (ADR-0021); the store is part of the engine.

### Breaking changes
- The release is 2.0.0 (ADR-0019). Every Java module is internal, with no compatibility promise:
  the pluggable store SPI is retired and `KafkaTrackerStore` is the only store (ADR-0021).
- Build: the `cesium-kafka-api`, `cesium-kafka-store-kafka` and `cesium-kafka-store-testkit`
  modules are folded into `cesium-kafka-core`, with their package names unchanged; the store
  contract kit is now part of core's test sources. None of them was ever published as an artifact
  (ADR-0017), so this only affects source builds. The build has four modules: core, app, it,
  benchmarks.
- SPI: `ExternalSchedulerStore`, its contract kit (`ExternalSchedulerStoreContract`,
  `ExternalStoreHarness`) and the testkit's `InMemoryExternalSchedulerStore` are removed. The engine
  never had a dispatch loop for the external archetype and rejected such a store at startup.
- SPI: `StoreCapabilities` and `SchedulerStore.capabilities()` are removed. The engine never branched
  on them. `/info` (with `observability.detailed-info: true`) loses its `store.capabilities` object.
- SPI: `OwnershipEpoch`, `StoreContext.epoch()` and `StoreContext.clock()` are removed; no store read
  them. The testkit's `MutableClock` is removed with them.
- SPI: the headers-blind `TrackerBackedStore.onTrackerRecord(int, long, byte[], byte[])` overload is
  removed. The engine always called the headers variant, and the overload applied reason-less
  tombstones that the engine's path counts and skips (R12).
- `ingest.workers` and `dispatch.workers` are removed (exit 78 if set): each instance runs at most
  one ingest loop and one dispatch loop, selected by `roles`; scale either by adding instances.
  1.x let `dispatch.workers` exceed 1, but every dispatch loop in a process shared one
  single-threaded store and `drainDue` did not filter by owned partition, so a second worker raced
  the first; extra ingest workers shared one `group.instance.id`, so the broker fenced all but one,
  and they overwrote each other's degraded and heartbeat gauges. The transactional ids keep their 1.x
  worker-0 form (`cesium.<app>.<role>.<instance>.0`) and the thread names stay
  `cesium-ingest-0`/`cesium-dispatch-0`, so a rolling upgrade still fences its predecessor.
- `dispatch.coalesce` is removed (YAML and `CESIUM_DISPATCH__COALESCE`).
  Setting it is now an unknown-key error (exit 78). It never had any effect.
- Config: `store.type` and `store.properties` are removed; setting either is an unknown-key error
  (exit 78). The store's keys go with them: `cursor.sidecar-max-bytes` is now
  `dispatch.cursor.sidecar-max-bytes` (already the documented key), `heap-budget-bytes` is derived
  from 25% of `-Xmx` like `dispatch.max-pending-total`, `index.stale-rebuild-floor` and
  `index.completed-sweep-floor` are fixed at their defaults, and `max-pending-per-partition` was
  already a duplicate of `dispatch.max-pending-per-partition`.
- `dispatch.cursor.sidecar-max-bytes` must be in [128, 1048576] (exit 78). 1.x accepted any value
  of at least 1 and silently ignored it.
- SPI: `SchedulerStore`, `SchedulerStoreProvider` and its `META-INF/services` registration,
  `StoreContext`, `ConfigView`/`MapConfigView` and `RouteDescriptor` are removed.
  `TrackerBackedStore` is an engine-internal interface, and `KafkaTrackerStore` takes its identity,
  budgets and meter registry in its constructor instead of `configure → validate → start`.
- `/info` drops its `store` object (`store.type`).
- `startup-checks.heap-budget` is removed, together with the startup check it controlled; setting
  it is an unknown-key error (exit 78). That check required `source partitions ×
  dispatch.max-pending-per-partition × 64 B` to fit in 25% of the heap, so at the 2,000,000 default
  it needed `-Xmx` of at least 512 MiB per source partition. The runtime bounds already cover it: the
  AUTO `dispatch.max-pending-total` pauses ACTIVE shards at the same budget, and the store's H1
  ceiling bounds RECOVERING shards. An explicit `dispatch.max-pending-total` larger than the budget
  is now a warning instead of an error. The store's own repeat of the check is gone too.
- `kafka.transactions.commit-retry` must be at least 1 (exit 78). It counts `commitTransaction`
  attempts, so `0` behaved like `1`; it passed config load and then failed engine construction
  with exit 1.
- Every duration key must be at least `PT0.001S` (exit 78). The engine uses durations in whole
  milliseconds; 1.x accepted a positive value below 1 ms and truncated it to 0 ms. Without the
  check, a sub-millisecond `startup-checks.revalidation-interval` (new in 2.0) would crash startup.
- `headers.accept-binary-long-values` is removed (exit 78 if set). ASCII decimal is the only
  control-header encoding, and an 8-byte big-endian value is malformed. The flag was compatibility
  with the retired proof-of-concept and was never used outside a unit test.
- The `-Dcesium.*` system-property config overlay is removed; precedence is now
  `defaults < YAML < CESIUM_* environment`. Set the same keys through the environment
  (`-Dcesium.instance-id=x` becomes `CESIUM_INSTANCE_ID=x`) or YAML. `-Dcesium.*` properties are now
  ignored rather than rejected. This also removes the clash between the overlay prefix and the
  test-only `cesium.testing.crashpoints` switch, which an app JVM would have rejected as an unknown key.
- `route.tracker.bootstrap: CREATE` no longer grants the tracker write ACL to
  `route.tracker.acl-principal`. Create the ACL yourself; a literal ACL can exist before the topic.
  `startup-checks.tracker-acl` still verifies it at startup and on every re-validation round, and
  the cesium principal no longer needs `Cluster:ALTER`. A failed grant can no longer fail startup.
  The startup finding for a missing ACL now says to create it yourself rather than suggesting `CREATE`.
- The startup log line `cesium engine started` loses its `ingestWorkers`/`dispatchWorkers` fields.
- `ingest.max-batch` is removed (exit 78 if set). It only set the ingest consumer's
  `max.poll.records`, which keeps its tuned default of 2000; override it through
  `kafka.ingest-consumer.properties`.
- `/health/ready` loop detail loses its `assigned` field, and readiness no longer has an
  "assignment" term. The engine only ever set it alongside the heartbeat, so it never changed the
  verdict.
- `dispatch.max-pending-total` is process-wide; 1.x enforced it once per dispatch worker.
- `cesium_dlq_records_total` gains `reason="payload-expired"`, and `reason="unrelayable"` now
  includes dispatch-side DLQ copies. Names and tags are unchanged; only the values move.
- Logs: JSON lines from the loop threads (`cesium-ingest-0`, `cesium-dispatch-0`) now carry an
  `mdc` object with `applicationId`, `loop` and `txn`, and lines from the main, `cesium-shutdown`
  and `cesium-admin` threads carry `{applicationId}`. The never-populated `partition` key is removed from the documented
  shape, and `txn` is the loop's transactional id, which includes the instance id.
- Logs: the fatal log changes from `fatal {role} loop failure — initiating engine shutdown` to
  `fatal engine failure ({source}) — initiating engine shutdown` (for example `(ingest loop)`), and
  the exit line from `engine exited on a fatal loop failure` to
  `engine exited on a fatal engine failure`.
- Core (internal): `ClusterAdmin` gains `offsetsAtOrAfter`, and `StartupValidator` gains
  `revalidate` and a `(ClusterAdmin, Clock)` constructor. Out-of-tree `ClusterAdmin` implementations
  must add the method.
- A running engine now exits non-zero within `startup-checks.revalidation-interval` (default
  `PT5M`) when the source or tracker topic id (or the cluster id) changes (R17, ADR-0020). The log is
  `fatal engine failure (re-validation)`.
- Other runtime re-validation errors set the `/health/ready` `degraded` flag with a short cause and
  `cesium_revalidation_errors`; readiness status is unchanged (D21) and dispatch continues. That
  covers the D14 floor and committed-cursor age, partition drift, the tracker `cleanup.policy` and
  other §2.1 settings, source retention and the tracker ACL under `FAIL`, a missing topic, and a
  round that cannot complete. Partition drift at runtime no longer fails fast in the spec (R-7);
  ingest's I-7 fail-fast on a source partition with no tracker partition is unchanged. Growing
  partitions still needs a stop (see Fixed).
- D14's runtime term is now the dispatch group's committed-cursor age against
  `delete.retention.ms / 2`, and a violation degrades and alerts instead of refusing
  (ADR-0020). The oldest-pending term is dropped as subsumed.
- Core (internal): `StartupChecks` gains `revalidationInterval` and loses `heapBudget`.

### Added
- Runtime re-validation (D23, ADR-0020): a `cesium-admin` thread re-runs the environment checks every
  `startup-checks.revalidation-interval` (new key, default `PT5M`, must be at least `PT0.001S`),
  plus the topic identity, D14 committed-cursor age (dispatch instances only) and, under
  acknowledged size or tier eviction, an R-11 earliest-retained-record check. It never creates
  topics or applies ACLs, and needs no new ACL grants.
- Metric `cesium_revalidation_errors`: errors found by the last re-validation round; `0` when clean.
- Metrics `cesium_shard_state{partition}` (0 = ASSIGNED, 1 = RECOVERING, 2 = ACTIVE) and
  `cesium_replay_remaining_records{partition}` (barrier − position while recovering), previously
  listed as "not yet emitted".

### Fixed
- The growing-partitions runbook (operations.md §9) had operators grow the tracker, then the source,
  on a running engine, and the 2.0 text added that re-validation only degrades. A running engine
  stops instead, because a new partition has no committed offset and, with `auto.offset.reset=none`
  locked, the loop assigned it exits (dispatch on a new tracker partition, ingest on a new source
  partition).
  The runbook is now offline: stop every instance, wait for both groups to empty, grow the tracker
  and then the source, seed only the new partitions to earliest in both groups, and restart.
  `PartitionGrowthIT` follows it end to end.
- After an ambiguous (in-doubt) dispatch commit, the dropped-and-readopted partitions kept two
  pieces of loop state that a revocation clears: `cesium_shard_paused{partition}` could stay at 1
  after the shard recovered below the backpressure threshold, and the partition's penalty streak
  stayed counted in `cesium_fetch_penalized_partitions`. The in-doubt path now resets shard state
  through the same code as a revocation.
- `dispatch.cursor.sidecar-max-bytes` had no effect: the store read its own
  `store.properties.cursor.sidecar-max-bytes` instead. It is now the store's only sidecar budget.
- The documented startup check against broker `offset.metadata.max.bytes` did not exist. Startup
  now reads it and, when the sidecar budget exceeds it, warns and clamps the budget so cursor commits
  pin fewer entries instead of failing at commit time. It only ever warns.
- `/health/ready` always reported `"recovery": []`. The per-shard recovery detail (state,
  records remaining, ETA) is now fed from the store's shard-state gauges. Recovery still never affects
  readiness (D21).
- `cesium_pinned_entries`, `cesium_cursor_sidecar_bytes` and `cesium_pending_entries` raced the
  dispatch thread. Scrapes read the store's unsynchronized per-partition map and the index directly,
  so a scrape during a rebalance could report a wrong value or NaN. The gauges now read volatile
  per-partition snapshots; `cesium_pending_entries` is refreshed every dispatch-loop iteration.
- Docs claimed a `past_due` relay reason that was never emitted anywhere; reworded.
- Docs and javadoc described startup admin calls as running on a `cesium-admin` thread; they run on
  the thread that calls `start()` (`cesium-admin` now exists only for re-validation, see Added). They
  also claimed the integration suite runs cesium instances as separate JVMs (they run in-JVM; the
  SIGKILL crash-loop soak is not built). The DLQ reason lists in
  `CesiumHeaders.ERROR_REASON`, `RouteConfig`, design §2.1 and the header-protocol §5.1 table now
  include `unrelayable`.
- The perf ITs ignored their `-Dperf.*` knobs: Gradle set them only in its own JVM, not in the forked
  test JVM, so every run used the CI defaults. The `cesium-kafka-it` build now forwards every
  `-Dperf.*` and `-Dsoak.*` property to its test tasks as a configuration-cache input.
- `cesium_dlq_records_total` counted only ingest-side DLQ writes. The dispatch loop's
  `payload-expired` loss notices and its `unrelayable` DLQ copies are now counted too, once per
  committed transaction (an aborted attempt counts nothing).
- The documented log MDC keys (`applicationId`, `loop`, `txn`) were never set, so JSON lines had no
  `mdc` object and plain lines no context. The engine now sets them once per loop thread, and
  `applicationId` on the main and shutdown threads.
- The `startup-checks.size-based-retention: ACKNOWLEDGED` warning pointed operators at
  `cesium_retention_margin_seconds`, a metric that was never emitted. It now cites
  `cesium_fetch_unfetchable_total`, `dispatch.on-unfetchable-payload` and broker-side monitoring of
  the source's earliest-record age.

### Removed
- `dispatch.coalesce`: validated but never read by the dispatch loop (see Breaking changes).
- `store.type`, `store.properties` and the store-selection SPI (see Breaking changes).
- `startup-checks.heap-budget` and the startup footprint check, in both the engine and the store
  (see Breaking changes).
- `ExternalSchedulerStore` and its contract kit, `StoreCapabilities`, `OwnershipEpoch`,
  `StoreContext.epoch()`/`clock()`, the testkit's `MutableClock`, and the headers-blind
  `onTrackerRecord` overload (see Breaking changes).
- `docs/store-spi.md`: there is no store SPI to implement against (ADR-0021).
- From the spec (never emitted): `cesium_retention_margin_seconds` and its earliest-age probe, the
  periodic tracker-size monitor, and `cesium_tracker_cursor_lag` /
  `cesium_tracker_cursor_age_seconds`. Watch tracker size and source earliest-record age with
  broker-side metrics (ADR-0020).

### Changed
- The ingest and dispatch loops share one implementation of the transactional producer mechanics
  (`LoopTransactions`: send with failure capture, the bounded commit retry and in-doubt
  classification, abort-or-replace, producer replacement) instead of two copies that had drifted.
  One behavior change follows: a `TransactionAbortableException` (KIP-890) after an ambiguous ingest
  commit now aborts and rewinds the batch, as dispatch already did, instead of running the heavier
  in-doubt procedure. Both are exactly-once; the broker has affirmed the transaction did not commit.
- Internal simplification, about −470 lines net with the fixes included. Highlights:
  - The observability server is started directly instead of through a single-provider `ServiceLoader`.
  - The three HTTP handlers are merged into one.
  - The hand-rolled JSON writer is replaced by the Jackson already shipped; log lines stay
    byte-identical.
  - Transaction helpers duplicated between the ingest and dispatch loops are shared, as is one
    cause-chain walker.
  - The two loop-fatal exception types are merged.
  - Dead fetch summaries and test-only tracker-index surface are removed.
- Plain log lines now end with the thread's MDC (`%X`), for example
  `applicationId=orders-delay, loop=ingest, txn=cesium.orders-delay.ingest.cesium-0.0`.
- The startup checks now also run on a schedule, so their ERROR findings appear in a running
  engine's log as `re-validation finding <path>: …`. ADR-0002, ADR-0005 and ADR-0018 are amended by
  ADR-0020.
- The integration suite now waits up to 5 minutes for a container image pull instead of
  Testcontainers' 2-minute default (`pull.timeout=300` in the `cesium-kafka-it` test resources).
  The 2026-10-11 nightly failed when ghcr.io was unreachable for about 3 minutes. Test
  infrastructure only.

## [1.1.5] - 2026-10-06

Dependency refresh. No engine source changed. The runtime libraries that ship inside the
distribution archives (jackson, slf4j, logback) move to current patch releases, so unlike 1.1.4 the
archives' contents differ; the build moves to Gradle 9.8.0.

### Changed
- Runtime dependencies (grouped Dependabot PR [#28]): jackson 2.22.2 → 2.22.3, slf4j 2.0.19 →
  2.0.20, logback 1.6.3 → 1.6.5.
- Build/test only ([#28]): Gradle 9.7.1 → 9.8.0, NullAway 0.14.1 → 0.14.2, Spotless 8.10.2 →
  8.10.3. The Gradle distribution was verified against Gradle's published SHA-256, and the
  regenerated `gradle-wrapper.jar` against the published 9.8.0 wrapper-jar checksum. Dependabot left
  the `gradle-wrapper.properties` comment describing 9.7.1; corrected.
- SHA-pinned `gradle/actions/setup-gradle` v6.3.0 → v6.4.0 (Dependabot PR [#29]). The pin was
  dereferenced through the annotated `v6.4.0` tag to `3f5f9ad`, a clean fast-forward from v6.3.0.

## [1.1.4] - 2026-09-25

CI infrastructure only. No executable line of the engine and no shipped dependency changed, so the
distribution archives are functionally identical to 1.1.3. The SHA-pinned `actions/setup-java` moves
to v6.0.1, and the release lane now enforces JDK signature verification explicitly.

### Changed
- SHA-pinned `actions/setup-java` v6.0.0 → v6.0.1 (Dependabot PR [#25]). The pin was dereferenced to
  its release tag (`v6.0.1` is a lightweight ref straight to `de7274f`; the floating `v6` tag points
  there too), and the upstream compare is a clean eight-commit fast-forward matching the release
  notes. The one behaviour change is to Temurin signature verification: v6.0.0 checked a downloaded
  archive's GPG signature by default and failed the step on a bad signature, a missing signature URL
  or a missing `gpg`; v6.0.1 still checks by default but only warns on those failures unless
  `verify-signature: true` is set explicitly. That has no effect today: every lane resolves JDK 21/25
  from the hosted runner tool cache (the PR's JDK 25 lane logs `Resolved Java 25.0.4+1 from
  tool-cache` / `Resolved Java 21.0.12+1 from tool-cache`), and setup-java verifies nothing on that
  path. Even so, `release.yml` now sets `verify-signature: true` so that a JDK download in the lane that
  builds shipped artifacts fails closed. The CI and nightly lanes keep the warn-only default so an
  Adoptium signing-key rotation the action has not yet picked up cannot break routine builds. The
  unit lane's `JAVA_HOME` ordering is unchanged: 21 is still installed last and remains the default.

## [1.1.3] - 2026-09-08

Dependency and build infrastructure only. No executable line of the engine changed, and behaviour
is the same. The runtime libraries that ship inside the distribution archives (jackson, logback,
slf4j, micrometer) move to current patch releases, and a latent Gradle plugin-classloader defect
that had been misdiagnosed as a Spotless incompatibility is fixed, unpinning the formatter.

### Fixed
- The benchmarks module was splitting the Gradle plugin classloader, which had held Spotless at
  7.x since 2026-06-07. Gradle caches one plugin classloader per distinct plugin classpath.
  `cesium-kafka-benchmarks` was the only module applying a plugin its siblings lacked (it added
  `alias(libs.plugins.jmh)` on top of the shared `cesium.java-conventions`), so it received its own
  classloader, and `SpotlessTaskService` (a build-scoped Gradle `BuildService`) was loaded twice.
  Spotless 8.x checks for exactly this and failed the build at configuration time
  (`Cannot set the value of task ':cesium-kafka-benchmarks:spotlessJava' property 'taskService' …
  loaded with …project-cesium-kafka-benchmarks … using a provider … loaded with
  …project-cesium-kafka-api`); 7.x never performed the check, so the defect sat latent and was
  misread as a Spotless incompatibility. The JMH plugin now reaches the module through a new
  `cesium.jmh-conventions` precompiled script plugin in `build-logic`, putting it on the runtime
  classpath every subproject already shares: one classpath, one classloader, one
  `SpotlessTaskService`. The `[plugins]` block in `gradle/libs.versions.toml` is gone; every plugin
  jar is now a build-logic `implementation` dependency, and the catalog comment says why.

### Changed
- Spotless 7.0.4 → 8.10.2, and the `ignore:` rule holding it at 7.x is removed from
  `.github/dependabot.yml`; the classloader fix above was the real blocker. (That rule was not doing
  its job regardless: it was added 2026-06-07 yet Dependabot still opened a major-version Spotless PR
  on 2026-08-23.) Nothing in the 8.x breaking-change list reaches this build: the renamed
  `removeWildcardImports`, the root-only `spotlessInstallGitPrePushHook`, the `LintSuppression` path
  change and the ktfmt/ktlint changes are all unused, and the raised floors (Gradle 8.1, Java 17) sit
  below this project's. `palantirJavaFormat` stays pinned to the catalog's 2.68.0 rather than tracking
  the plugin default, so no source file was reformatted: `spotlessApply` on 8.10.2 is a no-op
  against the 7.0.4 output.
- Dependency refresh (grouped Dependabot PRs [#20], [#22] and [#24]). Runtime: micrometer 1.17.0 →
  1.17.1, jackson 2.22.0 → 2.22.2, logback 1.6.2 → 1.6.3, slf4j 2.0.18 → 2.0.19. Build/test only:
  Gradle 9.7.0 → 9.7.1, NullAway 0.13.8 → 0.14.1, and the SHA-pinned GitHub Actions (setup-java
  v5.7.0 → v6.0.0, action-gh-release v3.0.2 → v3.0.3). No API change: per
  [ADR-0017](docs/adr/0017-kafka-4-floor-and-repo-only-publishing.md) this project publishes
  distribution archives only, so micrometer's patch bump is not a transitive compile surface for any
  consumer. Every pin was verified against upstream before merge: the Gradle distribution against
  Gradle's published SHA-256, and each action SHA dereferenced to its release tag (action-gh-release
  v3.0.3 through its annotated tag object). `gradle-wrapper.jar` is byte-identical between 9.7.0 and
  9.7.1, so Dependabot was right to leave it untouched.
- `actions/setup-java` v6.0.0 is a major bump, but it preserves the `JAVA_HOME` ordering the unit
  lane depends on. `ci.yml` installs the matrix test JVM and 21, with 21 last, so Gradle (and
  therefore Spotless/palantir-java-format, which hooks javac internals that newer JDKs change)
  always runs on 21, while the suites run on the matrix JVM via `-PtestToolchain`. The JDK 25 lane of
  [#22] confirms the ordering survives v6: `JAVA_HOME` resolves to the 21 toolchain with 25 installed
  alongside. v6's split of the wrapper cache from the dependency cache does not affect this repo: no
  workflow passes setup-java a `cache:` input, since caching is `gradle/actions/setup-gradle`.

## [1.1.2] - 2026-08-25

Documentation only. No executable line changed, and behaviour is the same. Fixes three places where
the docs described intended design as though it were shipped behaviour, or stated a Kafka semantic
backwards. A reader would act on each of these claims: write an alert, implement against an SPI, or
size an offsets-retention budget.

### Fixed
- `docs/delivery-semantics.md` §8.1 stated KIP-211's offset-expiry semantics backwards. It said
  the retention clock "runs from the last commit, even for a live-but-idle group", which is the
  pre-KIP-211 behaviour the KIP removed in Kafka 2.1. For a subscribing group the clock starts when
  the group becomes empty; a group with live members does not lose its offsets however idle its
  partitions. Both cesium groups `subscribe()`, so expiry can only begin once cesium is fully down,
  which is why the guard is an outage budget (`startup-checks.max-tolerated-outage`) rather than a
  commit-interval floor. The surrounding section already assumed the correct model, so only the
  parenthetical was wrong, but an operator reading it could have concluded that a healthy, idle
  deployment was at risk of silent offset expiry.

### Changed
- The two-dispatch-loop `switch` in design §4.2 and `docs/store-spi.md` §2 (removed in 2.0.0) is
  now labelled as design intent rather than current code. Both showed
  `switch (store) { case TrackerBackedStore -> new TrackerDispatchLoop(...); case
  ExternalSchedulerStore -> new ExternalDispatchLoop(...); }`. Neither loop type exists: there is one
  `DispatchLoop`, and `CesiumEngine.resolveStore` narrows to `TrackerBackedStore` and rejects any
  other archetype at startup ("this app build orchestrates tracker-backed stores only"). The snippets
  are kept, since sealing the hierarchy ([ADR-0003](docs/adr/0003-sealed-two-archetype-store-spi.md))
  makes that the wiring when the external archetype lands, but each now carries an as-shipped note,
  and store-spi.md states that a store must implement `TrackerBackedStore` to be
  runnable by the shipped app today.
- Design §9's metric inventory now agrees with [`docs/operations.md`](docs/operations.md) §13 about
  which series exist. operations.md has always carried a "not yet emitted (deferred past M8)"
  block; design §9 listed the same nine series with no marker, so the two documents disagreed
  and the design doc read as a promise of live telemetry. The nine deferred series (`cesium_lso_lag`,
  `cesium_shard_state`, `cesium_replay_remaining_records`, `cesium_store_recovery_duration_seconds`,
  `cesium_store_replay_records_total`, `cesium_retention_margin_seconds`, `cesium_tracker_cursor_lag`
  / `_age_seconds`, `cesium_pending_oldest_deadline_seconds`, `cesium_index_bytes_estimate`) are now
  marked **[not yet emitted]** row by row, with a note naming operations.md §13 as authoritative and
  pointing at the documented proxies. `cesium_shard_paused` is emitted and is called out separately
  from `cesium_shard_state`, which is not.

## [1.1.1] - 2026-08-21

### Fixed
- The README quickstart no longer fails for a first-time visitor. "Try it — one command" opened
  straight at `make demo` with no `git clone`/`cd` ahead of it (the string `git clone` appeared
  nowhere in the file), so following the README verbatim ran `make` in whatever directory the reader
  happened to be in, and `make: *** No rule to make target 'demo'` was the first thing they saw.
- Startup no longer fails when the tracker topic it just created is not yet visible in the metadata a
  broker serves. `CreateTopics` is acknowledged by the KRaft controller, but the broker answering the
  next describe publishes that record asynchronously: a normal, usually sub-millisecond window that
  a loaded cluster can stretch. cesium now waits it out (bounded: 10 s, capped-exponential backoff)
  wherever a topic's existence is already proven, and any wait above 1 s is surfaced as a startup
  warning rather than absorbed silently. The error text no longer misdiagnoses this as a degraded
  cluster. Topics cesium did not create (source, DLQ, destination, and the `FAIL`-mode tracker)
  still fail fast on the first answer, so a genuinely missing topic is still reported immediately.
  See [ADR-0018](docs/adr/0018-bounded-wait-for-proven-topic-metadata.md).
- The nightly integration and soak lanes no longer resolve `UP-TO-DATE`/`FROM-CACHE`. Gradle's `Test`
  task is cacheable, but a broker-backed run's real inputs (Docker, the KRaft container, wall-clock)
  are not modelled, so an unchanged commit produced a guaranteed-wrong cache hit. Seven consecutive
  nightlies (2026-07-24 … 07-30) reported green in under 95 s without executing a single integration
  test.

### Changed
- `DispatchRestartRecoveryIT`'s kill-and-restart scenario no longer has a KIP-848 variant: restarting
  on the same `group.instance.id` stalls ~42.5 s under `group.protocol=consumer` (the successor is
  rejected with `UnreleasedInstanceIdException` until the killed incumbent is evicted by
  `group.consumer.session.timeout.ms`), where the classic protocol fences the incumbent immediately.
  A documented protocol incompatibility, now recorded in `docs/failure-matrix-coverage.md` gap 2
  rather than masked by a larger timeout. **Operator note:** under the consumer protocol with static
  membership, a hard pod restart pauses that partition's dispatch for up to the session timeout.
- Dependency refresh (grouped Dependabot PRs [#10] and [#17]). Runtime: kafka-clients 4.3.0 → 4.3.1,
  micrometer 1.16.5 → 1.17.0, fastutil 8.5.18 → 8.5.19, logback 1.5.34 → 1.6.2, jspecify 1.0.0 →
  1.0.1. Build/test only: Gradle 9.5.1 → 9.7.0, JUnit 6.1.0 → 6.1.3, Error Prone 2.49.0 → 2.50.0,
  NullAway 0.13.6 → 0.13.8, and the SHA-pinned GitHub Actions (checkout v6.0.3 → v7.0.1, setup-java
  v5.2.0 → v5.7.0, gradle/actions v6.1.0 → v6.3.0, action-gh-release v3.0.0 → v3.0.2). No API
  change: per [ADR-0017](docs/adr/0017-kafka-4-floor-and-repo-only-publishing.md) this project
  publishes distribution archives only, so micrometer's minor bump is not a transitive compile
  surface for any consumer, hence a patch release. Every pin was verified against upstream before
  merge: the Gradle distribution and `gradle-wrapper.jar` against Gradle's published SHA-256s, and
  each action SHA dereferenced to its release tag.
- Error Prone 2.50.0 newly reports `ReferenceEquality` at all six self-referential-cause guards
  (`t.getCause() == t`, which stops a Throwable whose cause is itself from looping forever). Identity
  is the intended comparison and the suggested `.equals()` rewrite would be incorrect, so each is
  suppressed at its method with the reasoning recorded. Annotations and comments only: no executable
  line changed, and the taxonomy classifiers behave as before.

## [1.1.0] - 2026-07-22

### Added
- One-command self-driving demo (`make demo`): submits five out-of-order delayed notifications and
  watches them arrive re-ordered and on schedule, exactly once. Needs only Docker (a `kcat`
  sidecar provides the tooling). Added a `Makefile` (`demo`/`up`/`down`/`logs`/`build`/`test`/
  `image`), `config/demo/run-demo.sh`, and a `demo`-profile service in `config/docker-compose.yaml`.
- `startup-checks.tracker-acl` config knob (`WARN`|`FAIL`|`SKIP`, default `WARN`): opt into `FAIL` to
  have cesium refuse to start unless the R12 tracker write-ACL is verifiably in force (recommended for
  production). Defaulting to `WARN` preserves the previous surface-but-proceed behaviour.
- Observability listener now caps accepted connections (`jdk.httpserver.maxConnections`, default 64)
  on top of the slow-client reaper, closing the M1 idle-socket file-descriptor exhaustion vector.

### Fixed
- The Docker image / `docker compose ... up --build` quickstart failed to configure because
  `settings.gradle.kts` included `cesium-kafka-benchmarks` while the Dockerfile (intentionally)
  omits that dev-only module; the benchmarks module is now included only when its directory is
  present, so slim build contexts configure cleanly.
- Stopped tracking stray Gradle build output (`build-logic/.iso-*-build/`) and ignored it.
- Dev/source-build version aligned to `1.0.0-SNAPSHOT` (was `0.1.0-SNAPSHOT`); release builds still
  derive the version from the git tag.

### Security
Security hardening from an internal audit. No default is changed in a backward-incompatible way:
the two controls that would break an existing deployment (the observability bind address and the
tracker-ACL strictness) are opt-in, so an upgrade with an unchanged config behaves as before. The
fixes below either add defence in depth for a normally-operating deployment or close a fail-open path
that only a non-conforming producer / external offset reset could reach.

- `delay.max` now bounds the absolute dispatch instant on the `cesium-delay-ms` path, matching the
  `cesium-deliver-at` path. A producer-controlled future-dated record timestamp with an in-range delay
  can no longer schedule a record beyond `now + delay.max`; over-bound records take the configured
  `on-over-max` policy (DLQ by default). Conforming producers are unaffected.
- Config errors no longer echo values into logs. `MapConfigView` type-mismatch messages and YAML
  parse errors now report the key/expected type and the structured source location without quoting the
  offending value or source line, which could carry a secret.
- Optional hardening (opt-in, see Added): `startup-checks.tracker-acl: FAIL` makes a missing R12
  tracker ACL a startup failure, and `observability.bind-address: 127.0.0.1` restricts the
  unauthenticated endpoints to loopback. Both are recommended for production and documented in
  [SECURITY.md](SECURITY.md); neither is the default, so existing deployments are unaffected until you
  opt in.

## [1.0.0] - 2026-06-06

First public release. cesium-kafka is a Kafka delayed-message relay: it consumes a source topic and
re-delivers each record to a destination topic at the time the producer asked for, with exactly-once
delivery as observed by `read_committed` consumers of the destination.

### Scope of this release

- The runnable application (`cesium-kafka-app`, shipped as the distribution archives on this
  Release and as a Docker image you build from the included `Dockerfile`) is the supported v1
  product.
- The store SPI (`cesium-kafka-api`) and its contract test kit (`cesium-kafka-store-testkit`) are
  stable and semver-governed for third-party store implementers from 1.0.
- The engine's programmatic API (`cesium-kafka-core`) is internal-until-1.x: usable, but not yet a
  compatibility-guaranteed surface.
- Publishing is repo-only for v1.0: this Release carries the `distTar`/`distZip` archives. There
  is no Maven Central artifact and no published container image in v1 (see ADR-0017).

### Added

- Exactly-once delayed relay. Both the ingest and dispatch loops are Kafka read-process-write
  transactions with KIP-447 group-metadata fencing, so the destination's `read_committed` consumers
  observe each record exactly once. Delivery is at or after the requested instant (never early;
  bounded lateness). Producers request a delay with one header: `cesium-delay-ms` (relative to the
  source timestamp) or `cesium-deliver-at` (absolute epoch-millis). Key, value, and all non-`cesium-*`
  headers are preserved byte-for-byte.
- Pointer-only memory model. A pending message holds only `(partition, offset, dispatchAt)` in
  the in-memory index; payloads stay in the source topic and are re-fetched at dispatch time. The
  index is a fastutil-backed primitive structure (arrival ring + binary search, no per-entry object),
  measured at ~40.5 B/entry.
- Pluggable scheduler store SPI (`cesium-kafka-api`) with two sealed archetypes (the
  tracker-backed store and the external, e.g. JDBC, store) and a published contract test kit
  (`cesium-kafka-store-testkit`) that is the executable specification a conforming store is written
  against (`TrackerBackedStoreContract`, `ExternalSchedulerStoreContract`).
- Tracker store (`cesium-kafka-store-kafka`): the main store, an internal compacted tracker
  topic holding scheduler state. Restarts and rebalances replay a bounded window via the
  committed-cursor-v2 design (consumer position + a pinned-entry sidecar in the offset-metadata
  channel), not the full history. The high-watermark replay barrier and snapshot ordering (I8)
  guarantee no duplicate or dropped delivery across recovery.
- Runnable app (`cesium-kafka-app`): YAML + environment configuration with validation and locked
  keys, graceful lifecycle, a multi-stage `Dockerfile`, and a `config/docker-compose.yaml` 5-minute
  quickstart.
- Observability: decoupled liveness/readiness health endpoints (ready can be served while
  recovery is still in progress), a Prometheus `/metrics` endpoint with the `cesium_*` inventory, a
  `/info` build-info endpoint, and one-line JSON logging.
- Hardening: cooperative rebalance + `onPartitionsLost` handling, zombie-producer fencing
  (including static-membership ids), ACTIVE-only backpressure pause/resume, a per-partition penalty
  box with enforced fetch byte/time budgets, the in-doubt-commit recovery taxonomy, and admin-time
  startup validation (topic identity/parity, retention, compaction settings, `auto.offset.reset=none`).
- Test suite: ~47 Testcontainers integration tests against Kafka 4.3.0 (representative PR lane +
  heavy nightly Toxiproxy/compaction/multi-instance/KIP-848/soak lanes), property-based index and
  store-contract suites, and a JMH hot-path benchmark module.
- Documentation: the implementation-ready [`docs/design.md`](docs/design.md) plus focused
  guides (architecture, delivery semantics, header protocol, operations, configuration, store SPI,
  performance, migration-from-PoC), ADRs 0001–0017, and the failure-matrix coverage map.

### Known limitations

- Not all performance numbers meet their targets. Per
  [`docs/performance.md`](docs/performance.md), dev-box numbers measured on a single localhost broker
  miss two classes of design `§11.4` target: the `drainDue`/ring-binary-search JMH hot paths
  (memory-latency bound) and the dispatch `dispatch_lag` p99 + scattered-due throughput
  (seek/fetch-I/O bound). A server-class, multi-broker re-measurement is owed before any
  dedicated-hardware or 100M-scale throughput claim; the original targets are retained as projections.
- The JMH >10% regression gate is aspirational and non-blocking in v1 (it needs a low-variance
  dedicated runner before it can gate CI).
- The KIP-848 consumer protocol lane runs nightly as non-blocking (`continue-on-error`); the
  classic protocol is the default until the ADR-0006 promotion criteria are met.
- `/metrics` exposes the engine's `cesium_*` inventory only. The design `§9` Micrometer binders
  (`KafkaClientMetrics` per producer/consumer, the JVM/process binders, and the `application_id`/`role`
  common tags) are deferred and not wired in v1, so `kafka_*` / JVM / process series are absent
  (obtain them via a JMX-to-Prometheus exporter sidecar). The deferred-metric series likewise listed
  in design `§9` (`cesium_shard_state`, `cesium_replay_remaining_records`, `cesium_retention_margin_seconds`)
  are not emitted either. See [`docs/operations.md`](docs/operations.md) §13.
- No Maven Central artifacts and no published container image in v1 (repo-only publishing,
  ADR-0017); a cancellation API and external (JDBC) store implementation are reserved for a later
  release (the SPI for both is fixed now so it cannot drift).

### Security

- The internal tracker topic **requires a write-restricting ACL**. Without it, forged ADD/tombstone
  records are duplicate-injection / data-loss primitives. See [`SECURITY.md`](SECURITY.md) and the
  operations guide.

[Unreleased]: https://github.com/mjucius/cesium-kafka/compare/v2.1.1...HEAD
[2.1.1]: https://github.com/mjucius/cesium-kafka/compare/v2.1.0...v2.1.1
[2.1.0]: https://github.com/mjucius/cesium-kafka/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.com/mjucius/cesium-kafka/compare/v1.1.5...v2.0.0
[1.1.5]: https://github.com/mjucius/cesium-kafka/compare/v1.1.4...v1.1.5
[1.1.4]: https://github.com/mjucius/cesium-kafka/compare/v1.1.3...v1.1.4
[1.1.3]: https://github.com/mjucius/cesium-kafka/compare/v1.1.2...v1.1.3
[1.1.2]: https://github.com/mjucius/cesium-kafka/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/mjucius/cesium-kafka/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/mjucius/cesium-kafka/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/mjucius/cesium-kafka/releases/tag/v1.0.0
