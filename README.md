# cesium-kafka

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

🚧 **Pre-release, under active development.** This is a ground-up rewrite of an earlier
proof-of-concept; see [docs/design.md](docs/design.md) for the complete design (architecture,
exactly-once transaction/fencing analysis, failure matrix, store SPI).

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

## Building

```bash
./gradlew build                                  # compile + unit tests
./gradlew :cesium-kafka-it:integrationTest       # integration tests (requires Docker)
./gradlew :cesium-kafka-app:installDist          # unpacked distribution under build/install
```

## License

[MIT](LICENSE)
