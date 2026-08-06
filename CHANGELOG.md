# Changelog

All notable changes to this project will be documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- Startup no longer fails when the tracker topic it just created is not yet visible in the metadata a
  broker serves. `CreateTopics` is acknowledged by the KRaft controller, but the broker answering the
  next describe publishes that record asynchronously — a normal, usually sub-millisecond window that
  a loaded cluster can stretch. cesium now waits it out (bounded: 10 s, capped-exponential backoff)
  wherever a topic's existence is already proven, and any wait above 1 s is surfaced as a startup
  warning rather than absorbed silently. The error text no longer misdiagnoses this as a degraded
  cluster. Topics cesium did **not** create — source, DLQ, destination, and the `FAIL`-mode tracker —
  still fail fast on the first answer, so a genuinely missing topic is still reported immediately.
  See [ADR-0018](docs/adr/0018-bounded-wait-for-proven-topic-metadata.md).
- The nightly integration and soak lanes no longer resolve `UP-TO-DATE`/`FROM-CACHE`. Gradle's `Test`
  task is cacheable, but a broker-backed run's real inputs (Docker, the KRaft container, wall-clock)
  are not modelled — so an unchanged commit produced a guaranteed-wrong cache hit. Seven consecutive
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

## [1.1.0] - 2026-07-22

### Added
- One-command self-driving demo (`make demo`): submits five out-of-order delayed notifications and
  watches them arrive re-ordered and on schedule, exactly once — needs only Docker (a `kcat`
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
Security hardening from an internal audit. **No default is changed in a backward-incompatible way** —
the two controls that would break an existing deployment (the observability bind address and the
tracker-ACL strictness) are opt-in, so an upgrade with an unchanged config behaves as before. The
fixes below either add defence in depth for a normally-operating deployment or close a fail-open path
that only a non-conforming producer / external offset reset could reach.

- **`delay.max` now bounds the absolute dispatch instant on the `cesium-delay-ms` path**, matching the
  `cesium-deliver-at` path. A producer-controlled future-dated record timestamp with an in-range delay
  can no longer schedule a record beyond `now + delay.max`; over-bound records take the configured
  `on-over-max` policy (DLQ by default). Conforming producers are unaffected.
- **Config errors no longer echo values into logs.** `MapConfigView` type-mismatch messages and YAML
  parse errors now report the key/expected type and the structured source location without quoting the
  offending value or source line, which could carry a secret.
- **Optional hardening (opt-in, see Added):** `startup-checks.tracker-acl: FAIL` makes a missing R12
  tracker ACL a startup failure, and `observability.bind-address: 127.0.0.1` restricts the
  unauthenticated endpoints to loopback. Both are **recommended for production** and documented in
  [SECURITY.md](SECURITY.md); neither is the default, so existing deployments are unaffected until you
  opt in.

## [1.0.0] - 2026-06-06

First public release. cesium-kafka is a Kafka delayed-message relay: it consumes a source topic and
re-delivers each record to a destination topic **at the time the producer asked for**, with
**exactly-once delivery as observed by `read_committed` consumers of the destination**.

### Scope of this release

- The **runnable application** (`cesium-kafka-app`, shipped as the distribution archives on this
  Release and as a Docker image you build from the included `Dockerfile`) is the **supported v1
  product surface**.
- The **store SPI** (`cesium-kafka-api`) and its **contract test kit** (`cesium-kafka-store-testkit`)
  are a **stable, semver-governed surface** for third-party store implementers from 1.0.
- The engine's programmatic API (`cesium-kafka-core`) is **internal-until-1.x** — usable, but not yet
  a compatibility-guaranteed surface.
- **Publishing is repo-only for v1.0**: this Release carries the `distTar`/`distZip` archives. There
  is no Maven Central artifact and no published container image in v1 (see ADR-0017).

### Added

- **Exactly-once delayed relay.** Both the ingest and dispatch loops are Kafka read-process-write
  transactions with KIP-447 group-metadata fencing, so the destination's `read_committed` consumers
  observe each record exactly once. Delivery is **at-or-after** the requested instant (never early;
  bounded lateness). Producers request a delay with one header — `cesium-delay-ms` (relative to the
  source timestamp) or `cesium-deliver-at` (absolute epoch-millis). Key, value, and all non-`cesium-*`
  headers are preserved byte-for-byte.
- **Pointer-only memory model.** A pending message holds only `(partition, offset, dispatchAt)` in
  the in-memory index; payloads stay in the source topic and are re-fetched at dispatch time. The
  index is a fastutil-backed primitive structure (arrival ring + binary search, no per-entry object),
  measured at ~40.5 B/entry.
- **Pluggable scheduler store SPI** (`cesium-kafka-api`) with two sealed archetypes — the
  tracker-backed store and the external (e.g. JDBC) store — and a published **contract test kit**
  (`cesium-kafka-store-testkit`) that is the executable specification a conforming store is written
  against (`TrackerBackedStoreContract`, `ExternalSchedulerStoreContract`).
- **Tracker store** (`cesium-kafka-store-kafka`): the flagship store, an internal compacted tracker
  topic holding scheduler state. Restarts and rebalances replay a **bounded** window via the
  committed-cursor-v2 design (consumer position + a pinned-entry sidecar in the offset-metadata
  channel), not the full history. The high-watermark replay barrier and snapshot ordering (I8)
  guarantee no duplicate or dropped delivery across recovery.
- **Runnable app** (`cesium-kafka-app`): YAML + environment configuration with validation and locked
  keys, graceful lifecycle, a multi-stage `Dockerfile`, and a `config/docker-compose.yaml` 5-minute
  quickstart.
- **Observability**: decoupled liveness/readiness health endpoints (ready can be served while
  recovery is still in progress), a Prometheus `/metrics` endpoint with the `cesium_*` inventory, a
  `/info` build-info endpoint, and one-line JSON logging.
- **Hardening**: cooperative rebalance + `onPartitionsLost` handling, zombie-producer fencing
  (including static-membership ids), ACTIVE-only backpressure pause/resume, a per-partition penalty
  box with enforced fetch byte/time budgets, the in-doubt-commit recovery taxonomy, and admin-time
  startup validation (topic identity/parity, retention, compaction settings, `auto.offset.reset=none`).
- **Test suite**: ~47 Testcontainers integration tests against Kafka 4.3.0 (representative PR lane +
  heavy nightly Toxiproxy/compaction/multi-instance/KIP-848/soak lanes), property-based index and
  store-contract suites, and a JMH hot-path benchmark module.
- **Documentation**: the implementation-ready [`docs/design.md`](docs/design.md) plus audience-targeted
  guides (architecture, delivery semantics, header protocol, operations, configuration, store SPI,
  performance, migration-from-PoC), ADRs 0001–0017, and the failure-matrix coverage map.

### Known limitations / what is owed

- **Performance numbers are honestly re-documented, not all on-target.** Per
  [`docs/performance.md`](docs/performance.md), measured-on-a-single-localhost-broker dev-box numbers
  miss two classes of design `§11.4` target — the `drainDue`/ring-binary-search JMH hot paths
  (memory-latency bound) and the dispatch `dispatch_lag` p99 + scattered-due throughput
  (seek/fetch-I/O bound). A **server-class, multi-broker re-measurement is owed** before any
  dedicated-hardware or 100M-scale throughput claim; the original targets are retained as projections.
- The JMH **>10% regression gate is aspirational/non-blocking** in v1 (it needs a low-variance
  dedicated runner before it can gate CI).
- The **KIP-848 consumer protocol** lane runs nightly as non-blocking (`continue-on-error`); the
  classic protocol is the default until the ADR-0006 promotion criteria are met.
- **`/metrics` exposes the engine's `cesium_*` inventory only.** The design `§9` Micrometer binders —
  `KafkaClientMetrics` per producer/consumer, the JVM/process binders, and the `application_id`/`role`
  common tags — are **deferred** and not wired in v1, so `kafka_*` / JVM / process series are absent
  (obtain them via a JMX-to-Prometheus exporter sidecar). The deferred-metric series likewise listed
  in design `§9` (`cesium_shard_state`, `cesium_replay_remaining_records`, `cesium_retention_margin_seconds`)
  are not emitted either. See [`docs/operations.md`](docs/operations.md) §13.
- **No Maven Central artifacts and no published container image** in v1 (repo-only publishing,
  ADR-0017); a cancellation API and external (JDBC) store implementation are reserved for a later
  release (the SPI for both is fixed now so it cannot drift).

### Security

- The internal tracker topic **requires a write-restricting ACL**: without it, forged ADD/tombstone
  records are duplicate-injection / data-loss primitives. See [`SECURITY.md`](SECURITY.md) and the
  operations guide.

[Unreleased]: https://github.com/mjucius/cesium-kafka/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/mjucius/cesium-kafka/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/mjucius/cesium-kafka/releases/tag/v1.0.0
