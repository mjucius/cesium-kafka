# Changelog

All notable changes to this project will be documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **The benchmarks module was splitting the Gradle plugin classloader**, which had held Spotless at
  7.x since 2026-06-07. Gradle caches one plugin classloader per distinct plugin classpath.
  `cesium-kafka-benchmarks` was the only module applying a plugin its siblings lacked — it added
  `alias(libs.plugins.jmh)` on top of the shared `cesium.java-conventions` — so it received its own
  classloader, and `SpotlessTaskService` (a build-scoped Gradle `BuildService`) was loaded twice.
  Spotless 8.x checks for exactly this and failed the build at configuration time
  (`Cannot set the value of task ':cesium-kafka-benchmarks:spotlessJava' property 'taskService' …
  loaded with …project-cesium-kafka-benchmarks … using a provider … loaded with
  …project-cesium-kafka-api`); 7.x never performed the check, so the defect sat latent and was
  misread as a Spotless incompatibility. The JMH plugin now reaches the module through a new
  `cesium.jmh-conventions` precompiled script plugin in `build-logic`, putting it on the runtime
  classpath every subproject already shares — one classpath, one classloader, one
  `SpotlessTaskService`. The `[plugins]` block in `gradle/libs.versions.toml` is gone; every plugin
  jar is now a build-logic `implementation` dependency, and the catalog comment says why.

### Changed
- **Spotless 7.0.4 → 8.10.2**, and the `ignore:` rule holding it at 7.x is removed from
  `.github/dependabot.yml` — the classloader fix above was the real blocker. (That rule was not doing
  its job regardless: it was added 2026-06-07 yet Dependabot still opened a major-version Spotless PR
  on 2026-08-23.) Nothing in the 8.x breaking-change list reaches this build: the renamed
  `removeWildcardImports`, the root-only `spotlessInstallGitPrePushHook`, the `LintSuppression` path
  change and the ktfmt/ktlint changes are all unused, and the raised floors (Gradle 8.1, Java 17) sit
  below this project's. `palantirJavaFormat` stays pinned to the catalog's 2.68.0 rather than tracking
  the plugin default, so **no source file was reformatted** — `spotlessApply` on 8.10.2 is a no-op
  against the 7.0.4 output.
- Dependency refresh (grouped Dependabot PRs [#20] and [#22]). Runtime: micrometer 1.17.0 → 1.17.1,
  jackson 2.22.0 → 2.22.2, logback 1.6.2 → 1.6.3. Build/test only: Gradle 9.7.0 → 9.7.1, NullAway
  0.13.8 → 0.14.0, and the SHA-pinned GitHub Actions (setup-java v5.7.0 → v6.0.0, action-gh-release
  v3.0.2 → v3.0.3). No API change: per
  [ADR-0017](docs/adr/0017-kafka-4-floor-and-repo-only-publishing.md) this project publishes
  distribution archives only, so micrometer's patch bump is not a transitive compile surface for any
  consumer. Every pin was verified against upstream before merge: the Gradle distribution against
  Gradle's published SHA-256, and each action SHA dereferenced to its release tag (action-gh-release
  v3.0.3 through its annotated tag object). `gradle-wrapper.jar` is byte-identical between 9.7.0 and
  9.7.1, so Dependabot was right to leave it untouched.
- `actions/setup-java` v6.0.0 is a major bump, but it preserves the `JAVA_HOME` ordering the unit
  lane depends on. `ci.yml` installs the matrix test JVM **and** 21 with 21 *last*, so Gradle — and
  therefore Spotless/palantir-java-format, which hooks javac internals that newer JDKs change —
  always runs on 21, while the suites run on the matrix JVM via `-PtestToolchain`. The JDK 25 lane of
  [#22] confirms the ordering survives v6: `JAVA_HOME` resolves to the 21 toolchain with 25 installed
  alongside. v6's split of the wrapper cache from the dependency cache does not reach this repo — no
  workflow passes setup-java a `cache:` input, since caching is `gradle/actions/setup-gradle`.

## [1.1.2] - 2026-08-25

Documentation only — no executable line changed, no behaviour difference. Three places where the
docs described intended design as though it were shipped behaviour, or stated a Kafka semantic
backwards. Each is the kind of claim a reader would act on: write an alert, implement against an
SPI, or size an offsets-retention budget.

### Fixed
- **`docs/delivery-semantics.md` §8.1 stated KIP-211's offset-expiry semantics backwards.** It said
  the retention clock "runs from the last commit, even for a live-but-idle group" — that is the
  *pre*-KIP-211 behaviour the KIP removed in Kafka 2.1. For a subscribing group the clock starts when
  the group becomes **empty**; a group with live members does not lose its offsets however idle its
  partitions. Both cesium groups `subscribe()`, so expiry can only begin once cesium is fully down,
  which is precisely why the guard is an outage budget (`startup-checks.max-tolerated-outage`) and
  not a commit-interval floor. The surrounding section already assumed the correct model, so only the
  parenthetical was wrong — but an operator reading it could have concluded that a healthy, idle
  deployment was at risk of silent offset expiry.

### Changed
- **The two-dispatch-loop `switch` in design §4.2 and [`docs/store-spi.md`](docs/store-spi.md) §2 is
  now labelled as design intent rather than current code.** Both showed
  `switch (store) { case TrackerBackedStore -> new TrackerDispatchLoop(...); case
  ExternalSchedulerStore -> new ExternalDispatchLoop(...); }`. Neither loop type exists: there is one
  `DispatchLoop`, and `CesiumEngine.resolveStore` narrows to `TrackerBackedStore` and rejects any
  other archetype at startup ("this app build orchestrates tracker-backed stores only"). The snippets
  are kept — sealing the hierarchy ([ADR-0003](docs/adr/0003-sealed-two-archetype-store-spi.md)) is
  what makes that the wiring when the external archetype lands — but each now carries an as-shipped
  note, and store-spi.md states plainly that a store must implement `TrackerBackedStore` to be
  runnable by the shipped app today.
- **Design §9's metric inventory now agrees with [`docs/operations.md`](docs/operations.md) §13 about
  which series exist.** operations.md has always carried the honest "not yet emitted (deferred past
  M8)" block; design §9 listed the same nine series with no marker, so the two documents disagreed
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
- Dependency refresh (grouped Dependabot PRs [#10] and [#17]). Runtime: kafka-clients 4.3.0 → 4.3.1,
  micrometer 1.16.5 → 1.17.0, fastutil 8.5.18 → 8.5.19, logback 1.5.34 → 1.6.2, jspecify 1.0.0 →
  1.0.1. Build/test only: Gradle 9.5.1 → 9.7.0, JUnit 6.1.0 → 6.1.3, Error Prone 2.49.0 → 2.50.0,
  NullAway 0.13.6 → 0.13.8, and the SHA-pinned GitHub Actions (checkout v6.0.3 → v7.0.1, setup-java
  v5.2.0 → v5.7.0, gradle/actions v6.1.0 → v6.3.0, action-gh-release v3.0.0 → v3.0.2). No API
  change: per [ADR-0017](docs/adr/0017-kafka-4-floor-and-repo-only-publishing.md) this project
  publishes distribution archives only, so micrometer's minor bump is not a transitive compile
  surface for any consumer — hence a patch release. Every pin was verified against upstream before
  merge: the Gradle distribution and `gradle-wrapper.jar` against Gradle's published SHA-256s, and
  each action SHA dereferenced to its release tag.
- Error Prone 2.50.0 newly reports `ReferenceEquality` at all six self-referential-cause guards
  (`t.getCause() == t`, which stops a Throwable whose cause is itself from looping forever). Identity
  is the intended comparison and the suggested `.equals()` rewrite would be incorrect, so each is
  suppressed at its method with the reasoning recorded. Annotations and comments only — no executable
  line changed, and the taxonomy classifiers behave exactly as before.

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

[Unreleased]: https://github.com/mjucius/cesium-kafka/compare/v1.1.2...HEAD
[1.1.2]: https://github.com/mjucius/cesium-kafka/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/mjucius/cesium-kafka/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/mjucius/cesium-kafka/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/mjucius/cesium-kafka/releases/tag/v1.0.0
