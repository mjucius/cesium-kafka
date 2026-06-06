# Migrating from the proof-of-concept

cesium-kafka is a ground-up rewrite of an earlier proof-of-concept (PoC) delayed-message relay. It
is a **clean break**: the header names changed, the durable tracker format changed incompatibly, and
several behaviors the PoC handled silently (or incorrectly) are now explicit. **There is no
automated migration tool** — you cut over by draining or by accepting the cutover (§4).

This guide is for someone who ran the PoC and wants to move to cesium-kafka. For the full design see
[docs/design.md](design.md); for the new wire contract see [header-protocol.md](header-protocol.md);
for configuration see [configuration.md](configuration.md).

---

## 1. Header renames (producer-visible)

The control headers were renamed and namespaced. The PoC's unprefixed names are **not** honored —
cesium-kafka ignores them, so a record produced with only a PoC header relays **immediately** (no
delay).

| PoC header | cesium-kafka header | Notes |
|---|---|---|
| `delay-by` | `cesium-delay-ms` | Relay N ms after the source record timestamp |
| `delay-until` | `cesium-deliver-at` | Relay at an absolute epoch-ms instant |

Why the rename: the `cesium-` namespace prevents collisions with application headers, and the
explicit `-ms` / `-at` suffixes remove ambiguity. Values remain ASCII decimal, but the grammar is now
pinned (`^[0-9]{1,19}$`) and validated against `delay.max` — see
[header-protocol.md](header-protocol.md).

**Action:** update every producer to emit `cesium-delay-ms` / `cesium-deliver-at`. There is no
compatibility shim; a missing rename silently produces immediate relays, not delayed ones.

---

## 2. Tracker (durable state) format break

The PoC encoded scheduler state with an **unversioned sign-negation hack** in the record value. The
rewrite uses a **versioned binary format** (D15):

- **ADD** value is 12 bytes: `magic 0xC5 | version 0x01 | type 0x01 | flags | dispatchAtMs (int64 BE)`.
- **COMPLETE** is a proper Kafka **tombstone** (null value), with the completion reason carried in a
  record header — so the compacted tracker topic actually collapses completed entries.
- Records failing wire-format validation are counted (`cesium_tracker_invalid_records_total`),
  logged, and skipped — never applied, never crash the loop.

The two formats are **not interchangeable**. cesium-kafka cannot read a PoC tracker topic, and the
PoC cannot read cesium-kafka's. There is no in-place upgrade of the durable log.

**Action:** cesium-kafka provisions its **own** tracker topic (`route.tracker.bootstrap: CREATE`,
default name `cesium.<application-id>.tracker`) with the correct compaction settings. Do **not** point
it at a PoC tracker topic. Decommission the PoC topic after cutover (§4).

---

## 3. Behavioral differences

These are the changes you will notice operationally. Most close a correctness gap the PoC had.

### 3.1 Exactly-once instead of a duplicate window

The PoC used a per-partition `transactional.id` with manual partition assignment, which (under
KIP-447) leaves transactional offset commits **without group fencing** — a structural duplicate
window across rebalances and zombie producers. cesium-kafka uses **two consumer groups with KIP-447
group-metadata fencing** on both the ingest and dispatch loops. Delivery is **exactly-once as
observed by `read_committed` consumers of the destination**.

**Action:** destination consumers **must** set `isolation.level=read_committed`. A `read_uncommitted`
consumer will now observe aborted records (the "duplicates" that fencing prevents) and is the wrong
tool for verifying delivery.

### 3.2 No silent message loss on a missing payload

The PoC did one `seek` + one short `poll` per entry and **silently dropped** the message if the
payload was no longer available. cesium-kafka classifies each fetch as `FOUND` / `GONE` / `TRANSIENT`
and, on a provably-expired payload, produces an explicit **payload-expired DLQ loss notice** inside
the dispatch transaction (`dispatch.on-unfetchable-payload: DLQ`, the default) — resolved exactly
once, never silently lost. See the [DLQ contract](header-protocol.md).

### 3.3 Retention is validated, not assumed

Because payloads are pointer-only and re-fetched at dispatch time, a source whose retention is
shorter than the delay would lose payloads. The PoC did not check this. cesium-kafka validates
source `retention.ms` against `delay.max + margin` at startup (`startup-checks.retention: FAIL` by
default) and **refuses to start** on a misconfiguration; size-based / tiered eviction requires an
explicit `startup-checks.size-based-retention: ACKNOWLEDGED`. Monitor source earliest-available-record
age externally (the in-process `cesium_retention_margin_seconds` gauge is deferred — see
[operations.md](operations.md) §13).

### 3.4 Topic identity is bound; recreation fails fast

The PoC had no topic-identity binding, so a source topic deleted and recreated under the same name
could deliver **wrong payloads** at old offsets. cesium-kafka binds the source cluster id and topic
id into the committed-offset metadata and re-validates them, **failing fast** on a mismatch with a
runbook rather than relaying the wrong bytes (R17). The tracker topic is likewise integrity-checked
(committed offset within `[beginning, end]`, no auto-reset into an empty log).

### 3.5 Locked correctness-critical client config

The PoC's `Properties(defaults)` style let correctness-critical Kafka client settings drift.
cesium-kafka **locks** `isolation.level`, `auto.offset.reset`, `enable.idempotence`,
`enable.auto.commit`, `group.id`, `group.instance.id`, and `transactional.id`, rejecting any override
with an explanation. See [configuration.md](configuration.md). Notably `auto.offset.reset=none` means
a brand-new deployment needs a one-time explicit offset seed for the ingest group (a deliberate
operator step — [operations.md](operations.md)), rather than silently resetting after an outage.

### 3.6 Memory and the event loop (informational)

cesium-kafka stores `(partition, offset, dispatchAtMs)` pointers in a packed primitive index
(~32 B/entry nominal, ~40 B measured) instead of the PoC's `DelayQueue<DelayEntry>` (~64–80 B/entry
of pointer-chasing objects plus O(n) scans), and replaces the PoC's busy-polling and coarse
read/write lock with a single per-partition event loop. This is invisible to producers and
consumers; it just means millions of pending messages fit in a modest heap. See
[performance.md](performance.md).

### 3.7 Delivery is at-or-after, never before

cesium-kafka delivers **at or after** the requested instant (bounded lateness), never early. The
sub-second floor (ingest-commit + tracker-consume + `read_committed` visibility) is documented in
[performance.md](performance.md). If your PoC use relied on near-instant sub-second precision, measure
against the new floor before cutover.

---

## 4. Cutover (no automated migration)

There is no tool to migrate PoC pending state into cesium-kafka — the durable formats are
incompatible (§2). Choose one of:

**Option A — drain (recommended, no loss).**
1. Stop producers from adding **new** delayed records to the PoC.
2. Let the PoC drain its pending backlog to the destination (wait out the longest outstanding delay).
3. Stand up cesium-kafka against its own fresh tracker topic and destination, update producers to the
   new headers (§1), and seed the ingest group's first-run offsets (see the first-run runbook in
   [operations.md](operations.md)).
4. Cut producers over to cesium-kafka. Decommission the PoC and its tracker topic.

**Option B — accept the cutover (faster, abandons in-flight PoC delays).**
1. Stand up cesium-kafka alongside the PoC (own tracker topic, own consumer groups).
2. Cut producers over to the new headers at a chosen instant.
3. Accept that records still pending in the PoC at that instant are delivered by the PoC on its old
   schedule (run both until the PoC drains, or accept that the PoC's remaining pending entries are
   abandoned if you tear it down).

Run cesium-kafka with `delay.max` set deliberately (default `P1D`) and review the tracker sizing
worksheet in [operations.md](operations.md) before going to production — the tombstone-retention
floor that `delay.max` drives is the main capacity input, and it has no PoC equivalent.

---

## 5. Quick checklist

- [ ] Producers emit `cesium-delay-ms` / `cesium-deliver-at` (not `delay-by` / `delay-until`).
- [ ] Destination consumers set `isolation.level=read_committed`.
- [ ] Source `retention.ms` comfortably exceeds `delay.max` (or size/tier eviction is `ACKNOWLEDGED`).
- [ ] A fresh tracker topic is used (never a PoC topic); write access is restricted to the cesium
      principal (`route.tracker.acl-principal`).
- [ ] `route.dlq.topic` is configured (the default policies route to it).
- [ ] The ingest group's first-run offsets are seeded (`auto.offset.reset=none` is locked).
- [ ] PoC pending backlog is drained or its abandonment is accepted.
