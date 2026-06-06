# ADR-0005: Tracker format, compaction-only, tombstone-retention floor

- **Status:** Accepted
- **Date:** 2026-06-06
- **Design reference:** [`../design.md`](../design.md) §2.1–2.2, §3.7 (D4, D14, D15)

## Context

The compacted tracker topic is the system's entire durable pending state: an ADD record per
scheduled entry and a completion record per settled entry. Its format and retention are
correctness-load-bearing. A lone pending ADD must never expire; a completion tombstone must survive
long enough for any replayer whose cursor lies below it to read it (otherwise the entry looks
pending again — a duplicate); and completion records must have null values so compaction can
reclaim the ADD. The PoC used an unversioned sign-negation hack and time-based deletion, both of
which silently lose state.

## Decision

**Wire format (store-owned, opaque to the engine; D15).** Key = 8-byte big-endian source offset
(unique forever per partition — the compaction identity). ADD value = 12 bytes
`magic 0xC5 | version 0x01 | type 0x01=ADD | flags(1) | dispatchAtMs(int64 BE)`; `flags` bit 0 is
the CLAMP marker. COMPLETE = a **null-value tombstone** with the completion *reason* (`DISPATCHED`,
`PAYLOAD_MISSING_DLQ`, `DROPPED`, `REJECTED`) in a record **header**. Type `0x02 CANCEL` is
reserved. Unknown flags are ignored; unknown versions are rejected; records failing wire-format
validation are counted (`cesium_tracker_invalid_records_total`) and skipped, never applied.

**Cleanup policy (D4).** `cleanup.policy=compact` **only** — never `compact,delete`. Time-based
delete would remove a pending ADD older than retention; compaction-only means a lone pending ADD
can never expire.

**Tombstone-retention floor (D14).** `delete.retention.ms ≥ 2 × max(delay.max, observed
oldest-pending age, committed-cursor age)`. The `delay.max` term is validated at startup (FAIL);
the observed terms are re-validated periodically and a violation **refuses** to continue silently
(degraded health + alert + refusal to advance into the unsafe regime). `delay.max` default lowered
to **`P1D`** to keep default tracker disk sane.

## Consequences

- A lone pending ADD survives indefinitely; the tombstone a fallback (overflow) replay may need —
  as old as the oldest pending entry, *including entries scheduled under a previous, larger
  `delay.max`* — survives too.
- The floor is **validated, not merely documented**, because it is the one compaction property the
  correctness of replay depends on; replay cost itself is bounded by the v2 cursor
  ([ADR-0011](0011-committed-cursor-v2-position-plus-sidecar.md)), not by compaction.
- Tracker disk is dominated by the tombstone floor: `tracker_bytes ≈ pending × ~70 B +
  completion_rate × delete.retention.ms × ~64 B + uncleaned tail` (ops worksheet). Lowering
  `delay.max` requires draining or waiting out older entries first (runbook).
- Wire-format versioning belongs to the store, not the SPI
  ([ADR-0003](0003-sealed-two-archetype-store-spi.md)); `CANCEL` reservation leaves room for a
  future cancellation API.
