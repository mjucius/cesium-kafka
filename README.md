# cesium-kafka

[![CI](https://github.com/mjucius/cesium-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/mjucius/cesium-kafka/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A Kafka delayed-message relay: consume a source topic, deliver each record to a destination topic
**at the time the producer asked for** — with exactly-once delivery.

Producers attach one header to a record:

| Header | Value | Meaning |
|---|---|---|
| `cesium-delay-ms` | ASCII decimal millis | relay N ms after the source record's timestamp |
| `cesium-deliver-at` | ASCII decimal epoch-millis (UTC) | relay at an absolute instant |

Records without a delay header (or already due) relay immediately. Key, value, and all
non-`cesium-*` headers are preserved byte-for-byte.

## Status

**v1.0** — a ground-up rewrite of an earlier proof-of-concept. See the
[CHANGELOG](CHANGELOG.md) for the release notes and [docs/design.md](docs/design.md) for the complete
design (architecture, exactly-once transaction/fencing analysis, failure matrix, store SPI).

What is supported, and how stable, in v1:

- The **runnable application** is the supported product surface. Releases are **repo-only**: each
  tagged release publishes the distribution archives (`distTar`/`distZip`) on its
  [GitHub Release](https://github.com/mjucius/cesium-kafka/releases) — there is no Maven Central
  artifact or published container image in v1 (build the image from the included `Dockerfile`). See
  [ADR-0017](docs/adr/0017-kafka-4-floor-and-repo-only-publishing.md).
- The **store SPI** (`cesium-kafka-api`) and its **contract test kit** (`cesium-kafka-store-testkit`)
  are a **stable, semver-governed surface** for store implementers — start at
  [docs/store-spi.md](docs/store-spi.md).
- The engine's programmatic API (`cesium-kafka-core`) is **internal-until-1.x**.

## Design highlights

- **Exactly-once** as observed by `read_committed` consumers of the destination: both the ingest
  and dispatch loops are Kafka read-process-write transactions with KIP-447 group-metadata fencing.
- **Pointer-only memory**: while a message waits, cesium stores only
  `(partition, offset, dispatchAt)` — payloads stay in your source topic and are re-fetched at
  dispatch time. Millions of pending messages fit in a modest heap.
- **Durable + recoverable**: scheduler state lives in an internal compacted tracker topic; restarts
  and rebalances replay a bounded window (committed-cursor + sidecar design), never the full history.
- **Pluggable scheduler store SPI** with a published contract-test kit; the tracker-topic store is
  the flagship implementation, database-backed stores can plug in.
- **No silent failure modes**: every fault path ends in a retry, an explicit DLQ record, a degraded
  health flag + alert, or a fail-fast with a runbook entry.
- **Measured, not projected**: [docs/performance.md](docs/performance.md) has the M8 performance &
  sizing numbers — JMH hot-path benchmarks, macro throughput/latency, the memory worksheet, and the
  honest replay-cost formula, with every miss recorded against its target rather than fudged.

## Requirements

- Kafka **4.0+** brokers (KRaft)
- Destination consumers must use `isolation.level=read_committed` to observe exactly-once
- Java 21+ (to build/run from source)

## Quickstart (5 minutes)

You need [Docker](https://docs.docker.com/get-docker/) (with Compose) and
[`kcat`](https://github.com/edenhill/kcat). The Compose stack runs a single-broker KRaft Kafka, a
one-shot bootstrap step, and cesium itself built from source.

**1. Start everything** (from the repo root):

```bash
docker compose -f config/docker-compose.yaml up --build
```

The `topic-init` step creates the `orders` (source), `orders-delayed` (destination), and
`orders-dlq` topics and seeds cesium's first-run consumer offsets, then cesium starts. cesium runs
with [`config/cesium-example.yaml`](config/cesium-example.yaml) (mounted into the container), with
the broker address overridden via the environment.

> **First-run seeding.** cesium locks `auto.offset.reset=none` (delivery semantics depend on it), so
> a brand-new deployment has the `topic-init` step seed committed offsets for the source group once.
> This is a deliberate explicit-operator step — see [docs/design.md](docs/design.md) §3.6.

**2. Confirm it's healthy** (the observability port is published on `localhost:8081`):

```bash
curl -s localhost:8081/health/ready        # HTTP 200 once startup completes
curl -s localhost:8081/metrics | grep '^cesium_'
```

**3. Produce a record asking for a 30-second delay** (in a second terminal):

```bash
echo "hello, future" | kcat -b localhost:9092 -t orders -P -H cesium-delay-ms=30000 -k order-1
```

**4. Watch it arrive ~30 seconds later** on the destination. **Consume with
`isolation.level=read_committed`** — that is how you observe exactly-once; a `read_uncommitted`
consumer will see aborted "duplicates" from fencing. The consumer below **blocks and waits** (no
`-e`), so leave it running until the record appears, then press **Ctrl-C**:

```bash
kcat -b localhost:9092 -t orders-delayed -C -o beginning \
     -X isolation.level=read_committed -f '%T  key=%k  %s\n  headers: %h\n'
```

The record arrives ~30 s after you produced it, with a fresh dispatch timestamp (`%T`), the original
payload byte-for-byte, the `cesium-delay-ms` control header stripped, and `cesium-scheduled-for` /
`cesium-source-*` provenance headers stamped. A record produced **without** a `cesium-*` header
relays immediately.

**5. Tear down:**

```bash
docker compose -f config/docker-compose.yaml down -v
```

### Building the image directly

```bash
docker build -t cesium-kafka:local .        # multi-stage; final image is a JRE + the dist
```

The container reads its config from `CESIUM_CONFIG` (default `/etc/cesium/cesium.yaml`); mount your
own and override individual keys from the environment (e.g.
`CESIUM_KAFKA__PROPERTIES__BOOTSTRAP_SERVERS=...`). See
[`config/cesium-example.yaml`](config/cesium-example.yaml) for the full, commented schema.

## Exactly-once, observed by `read_committed`

cesium's delivery guarantee is **exactly-once as seen by `read_committed` consumers of the
destination**: both the ingest and dispatch loops are Kafka read-process-write transactions with
KIP-447 group-metadata fencing. A `read_uncommitted` consumer will observe aborted records (the
"duplicates" prevented by fencing) and is the wrong tool for verifying delivery. Delivery is
**at-or-after** the requested instant (never before; bounded lateness). See
[docs/design.md](docs/design.md) for the full transaction/fencing analysis and failure matrix.

## Documentation

The [design document](docs/design.md) is the deep, implementation-ready reference. The guides below
extract and refine it into focused, audience-targeted entry points.

**Run and operate it**

| Doc | What it covers |
|---|---|
| [Configuration](docs/configuration.md) | Full key reference, locked keys + rationale, the env-mapping grammar |
| [Operations](docs/operations.md) | Topic bootstrap/sizing worksheet, the tracker disk formula, ACLs, K8s rollout, JVM/GC, alert rules, runbooks |
| [Header protocol](docs/header-protocol.md) | Normative (RFC 2119) producer/consumer header contract incl. the DLQ JSON shape |
| [Migration from the PoC](docs/migration-from-poc.md) | Header renames, tracker-format break, behavioral diffs from the old proof-of-concept |

**Understand the guarantees**

| Doc | What it covers |
|---|---|
| [Architecture](docs/architecture.md) | Topology, the two-group rationale, the shard state machine, cursor v2, diagrams |
| [Delivery semantics](docs/delivery-semantics.md) | Invariants I1–I9, the replay-barrier proof, the `read_committed` requirement, store-archetype guarantees |
| [Performance](docs/performance.md) | Measured (M8) hot-path/throughput/latency numbers, the honest replay-cost formula, sizing & tuning |
| [Failure-matrix coverage](docs/failure-matrix-coverage.md) | Each failure-matrix scenario mapped to the test that proves it |

**Extend it / decisions**

| Doc | What it covers |
|---|---|
| [Store SPI guide](docs/store-spi.md) | Implementers' guide: archetype flowchart, ordering/cursor contracts, the contract test kit, packaging |
| [Architecture Decision Records](docs/adr/) | ADRs 0001–0017 — the load-bearing decisions, in Status/Context/Decision/Consequences form |
| [Design document](docs/design.md) | The full design: invariants, the complete failure matrix, the proofs |

## Building

```bash
./gradlew build                                  # compile + unit tests
./gradlew :cesium-kafka-it:integrationTest       # integration tests (requires Docker)
./gradlew :cesium-kafka-app:installDist          # unpacked distribution under build/install
```

## License

[MIT](LICENSE)
