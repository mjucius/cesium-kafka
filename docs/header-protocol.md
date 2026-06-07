# cesium-kafka header protocol

**Status:** normative · **Protocol version:** 1 · **Stability:** public wire contract (semver from
1.0)

This document specifies the Kafka record headers cesium-kafka reads from source records and stamps
on relayed and dead-lettered records, and the dead-letter (DLQ) record contract. It is the
authoritative, versioned description of the wire surface a producer or downstream consumer
integrates against. The deep design rationale is in [docs/design.md](design.md) `§2.3`–`§2.4`; the
header names and grammar are pinned in code by `com.jucius.cesium.kafka.api.headers.CesiumHeaders`
(published in `cesium-kafka-api`).

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are to be interpreted
as described in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119).

---

## 1. Control headers (consumed by cesium; stripped on relay)

A producer requests a delay by attaching **one** control header to a record.

| Header | Value grammar | Meaning |
|---|---|---|
| `cesium-delay-ms` | ASCII decimal, `^[0-9]{1,19}$` | Relay the record N milliseconds after the **source record timestamp**. |
| `cesium-deliver-at` | ASCII decimal epoch-milliseconds, UTC | Relay the record at an absolute instant. |

### 1.1 Encoding

- Control-header values **MUST** be canonical UTF-8 **ASCII decimal**, 1–19 digits
  (`^[0-9]{1,19}$` — the range that renders a non-negative Java `long`). ASCII decimal is producible
  from every client ecosystem without helper code.
- A value **MUST NOT** carry a sign, leading `+`, whitespace, decimal point, or thousands separator.
  Such a value is *malformed* (§4).
- An 8-byte big-endian `long` binary decode exists **only** behind the `headers.accept-binary-long-values`
  configuration flag (default off). When that flag is on, control-header values are decoded as 8-byte
  big-endian longs **instead of** ASCII decimal — the two modes are **mutually exclusive**, because a
  length-8 value is ambiguous between "8 ASCII digits" and "an 8-byte long". A deployment **MUST**
  choose one mode for the whole route. Unless you have a specific binary-only producer, leave the
  flag off and produce ASCII decimal.

### 1.2 `cesium-delay-ms` reference instant

- The relay time is `source-record-timestamp + N`. The source timestamp is the record's CreateTime.
  Using the producer-controlled CreateTime makes the relay time **deterministic across exactly-once
  abort/retry cycles**.
- If the source record has no timestamp (`NO_TIMESTAMP`), cesium **MUST** fall back to the ingest
  wall clock at the instant the record is first consumed.

### 1.3 `cesium-deliver-at` reference instant

- The relay time is the absolute UTC epoch-millisecond instant in the value, independent of any
  record timestamp.

### 1.4 Precedence

- A producer **SHOULD** set at most one control header.
- If **both** `cesium-delay-ms` and `cesium-deliver-at` are present, `cesium-deliver-at` **MUST**
  win. cesium increments `cesium_header_errors_total{type="conflict"}` and logs a WARN.
- If a single control header carries **multiple values**, the **last** value **MUST** win
  (`lastHeader` semantics), also counted as a conflict.

### 1.5 Validation and ranges

Let `delay.max` be the configured maximum delay (`delay.max`, default `P1D`) and `now` the ingest
clock.

- `cesium-delay-ms` **MUST** be in `[0, delay.max]`.
- `cesium-deliver-at` **MUST** be `≤ now + delay.max`. There is no lower bound.
- A value that is **past or zero** (a `cesium-delay-ms` of `0`, or a `cesium-deliver-at` already
  elapsed) is **not an error**: the record relays immediately with reason `past_due`. A past
  `cesium-deliver-at` is explicitly valid.
- A value failing the grammar of §1.1 is **malformed** and handled by the malformed-header policy
  (§4.1).
- A value exceeding the range above is **over-max** and handled by the over-max policy (§4.2).

### 1.6 Stripping on relay

- The two control headers (`cesium-delay-ms`, `cesium-deliver-at`) are the **only** headers cesium
  removes. On relay they **MUST** be stripped.
- Every other header on the source record — including any non-control `cesium-`-prefixed header a
  producer set (which it **SHOULD NOT**) — **MUST** be preserved byte-for-byte.

### 1.7 Records without a control header

- A record with neither control header relays **immediately**, unchanged except for provenance
  stamping (§2). It is never scheduled.

---

## 2. Provenance headers (stamped by cesium on relayed records)

When `headers.stamp-provenance` is `true` (default), cesium stamps the following on every relayed
record. Values are ASCII decimal or UTF-8 as noted. These headers are **delivered** (not stripped);
they are additive and **MUST NOT** collide with a producer's own headers (do not set
`cesium-`-prefixed headers other than the two control headers).

| Header | Value | Present on |
|---|---|---|
| `cesium-relayed-at` | ASCII decimal epoch-ms UTC: when the record was relayed | every relayed record |
| `cesium-source-topic` | UTF-8 source topic name | every relayed record |
| `cesium-source-partition` | ASCII decimal source partition | every relayed record |
| `cesium-source-offset` | ASCII decimal source offset | every relayed record |
| `cesium-source-timestamp` | ASCII decimal epoch-ms UTC: the source record's original timestamp | relayed records that had a timestamp |
| `cesium-scheduled-for` | ASCII decimal epoch-ms UTC: the instant the record was scheduled for | **delayed** records only |
| `cesium-clamped` | `true` | records pinned to `now + delay.max` by the `CLAMP` over-max policy |

- `cesium-source-timestamp` lets a consumer recover the original CreateTime even though the relayed
  record carries a fresh `DISPATCH` timestamp by default (`route.relay.timestamp`).
- Comparing `cesium-relayed-at` against `cesium-scheduled-for` yields the observed scheduling
  precision for a delayed record.

### 2.1 Relay timestamp

- By default the relayed record's Kafka timestamp is the **dispatch instant** (`route.relay.timestamp:
  DISPATCH`). This avoids violating the destination's `message.timestamp.difference.max.ms` and
  skewing time-based retention with an hours-old CreateTime.
- `route.relay.timestamp: SOURCE` preserves the original CreateTime instead.

---

## 3. Relay fidelity guarantees

On relay, cesium **MUST** preserve, byte-for-byte:

- the record **key**;
- the record **value**;
- **all** headers except the two stripped control headers (§1.6).

cesium **MUST NOT** alter, reorder semantically, or re-encode the payload. The destination
partition is chosen by `route.relay.partitioning` (`BY_KEY` default, or `SOURCE_PARTITION`).

> Delivery is **exactly-once as observed by `read_committed` consumers of the destination**, and
> **at-or-after** the requested instant (never before; bounded lateness). A `read_uncommitted`
> consumer will observe aborted records (the "duplicates" prevented by KIP-447 fencing) and is the
> wrong tool for verifying delivery.

---

## 4. Policies for malformed and over-max headers

Both policies are applied **inside the ingest transaction** (atomic with the source-offset advance),
validated at startup, and independent of each other.

### 4.1 Malformed header — `delay.on-malformed-header`

The winning control header failed the §1.1 grammar.

| Value | Behavior |
|---|---|
| `DLQ` (default) | Produce a header-error DLQ record (§5.1); the source record is not relayed to the destination. Requires `route.dlq.topic`. |
| `RELAY_IMMEDIATE` | Relay the record immediately, ignoring the malformed delay. |
| `FAIL` | Treat as a fatal condition (the ingest loop fails fast). |

`cesium_header_errors_total{type="malformed"}` increments.

### 4.2 Over-max delay — `delay.on-over-max`

The value parsed but exceeded the §1.5 range.

| Value | Behavior |
|---|---|
| `DLQ` (default) | Produce a header-error DLQ record (§5.1) with reason `over-max-delay`; not relayed. Requires `route.dlq.topic`. |
| `CLAMP` | Schedule at `now + delay.max` and stamp `cesium-clamped: true` on the eventual relay. |
| `FAIL` | Fail fast. |

`cesium_header_errors_total{type="over_max"}` increments.

**Default rationale (D3):** delivering early a message someone intended to delay is a business
hazard, so both policies default to `DLQ` — the violation is made explicit while the pipeline stays
alive.

---

## 5. Dead-letter (DLQ) contract

The DLQ record format is **versioned and public**. There are two shapes.

### 5.1 Header-error records (malformed / over-max)

Produced inside the **ingest** transaction. The record carries the **original** key and value and
**all** original headers (including the offending control headers — that is the point), plus:

| Header | Value |
|---|---|
| `cesium-error-reason` | `malformed-header` or `over-max-delay` |
| `cesium-error-detail` | UTF-8 human-readable detail (e.g. the offending value) |
| provenance headers | the §2 set, always stamped on DLQ records regardless of `headers.stamp-provenance` |

### 5.2 Payload-expired loss notices

Produced inside the **dispatch** transaction, in the same transaction as the COMPLETE tombstone,
when a scheduled record's payload is no longer fetchable at dispatch time (source retention,
compaction, or size/tier eviction) and `dispatch.on-unfetchable-payload: DLQ`. The payload is gone,
so only the pointer remains:

- **Key:** `null`.
- **Header:** `cesium-error-reason: payload-expired`.
- **Value:** UTF-8 JSON, exactly:

```json
{"v":1,"sourceTopic":"<topic>","sourcePartition":<int>,"sourceOffset":<long>,"scheduledFor":<epoch-ms>,"detectedAt":<epoch-ms>,"reason":"payload-expired"}
```

Field meanings:

| Field | Type | Meaning |
|---|---|---|
| `v` | int | Loss-notice schema version (currently `1`) |
| `sourceTopic` | string | Source topic the lost record came from (JSON-string-escaped) |
| `sourcePartition` | int | Source partition |
| `sourceOffset` | long | Source offset of the lost record |
| `scheduledFor` | epoch-ms | The instant the record was scheduled to be delivered |
| `detectedAt` | epoch-ms | When the missing payload was detected at dispatch time |
| `reason` | string | Always `payload-expired` |

### 5.3 DLQ reason values (wire format — never re-spelled)

| `cesium-error-reason` | Produced by | Meaning |
|---|---|---|
| `malformed-header` | ingest transaction | The winning control header failed the value grammar |
| `over-max-delay` | ingest transaction | The requested delay exceeded `delay.max` |
| `unrelayable` | ingest **or** dispatch transaction | The destination broker permanently rejected the relay on produce (record too large for `max.request.size`/`max.message.bytes`, invalid record) under `route.relay.on-unrelayable: DLQ` |
| `payload-expired` | dispatch transaction | The payload was unfetchable at dispatch time |

`cesium_dlq_records_total{reason=...}` counts records produced to the DLQ.

---

## 6. Versioning and compatibility

- This is **protocol version 1**. The header names, the value grammar, the DLQ `cesium-error-reason`
  values, and the payload-expired JSON shape are wire format and **MUST NOT** change spelling within
  a major version.
- New optional provenance or DLQ headers, and new fields appended to the payload-expired JSON (with a
  bumped `v`), are additive, backward-compatible evolutions.
- The legacy proof-of-concept's unprefixed `delay-by` / `delay-until` headers are **NOT** honored —
  this is a deliberate clean break. See [migration-from-poc.md](migration-from-poc.md).

### 6.1 Internal (non-protocol) headers

cesium's internal tracker topic uses its own headers (e.g. `cesium-completion-reason` on completion
tombstones). These are **internal store wire format**, owned by the `kafka-tracker` store, opaque to
producers and destination consumers, and out of scope for this protocol. They are documented in
[design.md](design.md) `§2.2` and the store SPI material. Producers and downstream consumers
**MUST NOT** depend on them.
