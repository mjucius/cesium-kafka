# ADR-0007: ASCII-decimal control headers

- **Status:** Accepted
- **Date:** 2026-06-06
- **Design reference:** [`../design.md`](../design.md) §2.3–2.4 (D1, D2)

## Context

Producers across every client ecosystem must be able to request a delay without a cesium helper
library — the control headers are the public producer-facing protocol. A binary "8-byte long"
encoding forces helper code and collides ambiguously with an "8 ASCII digits" value (both are 8
bytes). The delay base must also be deterministic across exactly-once abort/retry cycles, and the
PoC's unprefixed `delay-by` / `delay-until` names are a namespace-pollution and clash hazard.

## Decision

Two control headers, **canonical UTF-8 ASCII decimal**:

- `cesium-delay-ms` (`^[0-9]{1,19}$`) — relay N ms after the **source record timestamp**
  (CreateTime; deterministic across EOS retries), falling back to the ingest wall clock on
  `NO_TIMESTAMP` (D2).
- `cesium-deliver-at` — relay at the absolute epoch-millis-UTC instant.

An 8-byte big-endian long decode exists only behind `headers.accept-binary-long-values: true`
(default off); the modes are **exclusive** because length 8 is ambiguous. Precedence:
`cesium-deliver-at` wins if both are present (`cesium_header_errors_total{type="conflict"}` +
WARN); `lastHeader` wins for multi-valued headers. Validation is regex + range
(`delay-ms ∈ [0, delay.max]`, `deliver-at ∈ (−∞, now + delay.max]`); past or zero values relay
immediately (`reason="past_due"`) and are **not** errors. The PoC's unprefixed names are not
honored (a clean break, covered by the migration doc).

## Consequences

- Any producer in any language can schedule a delay by setting a plain decimal string header.
- The exclusive binary mode avoids the length-8 ambiguity entirely.
- CreateTime-relative delays are stable across EOS abort/retry, which the unique-committed-ADD
  invariant ([ADR-0001](0001-two-consumer-group-architecture.md)) and replay determinism depend on.
- Header-policy violations route to the DLQ by default (malformed / over-max), keeping the pipeline
  alive while making the violation explicit; over-max `CLAMP` stamps `cesium-clamped: true`, which
  must survive the durable round trip (the store carries it — design §2.2,
  [ADR-0005](0005-tracker-format-compaction-only-and-tombstone-retention-floor.md)).
