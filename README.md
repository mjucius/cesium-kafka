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

## Requirements

- Kafka **4.0+** brokers (KRaft)
- Destination consumers must use `isolation.level=read_committed` to observe exactly-once
- Java 21+ (to build/run from source)

## Building

```bash
./gradlew build                                  # compile + unit tests
./gradlew :cesium-kafka-it:integrationTest       # integration tests (requires Docker)
```

## License

[MIT](LICENSE)
