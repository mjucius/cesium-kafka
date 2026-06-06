# Delivery semantics — the correctness contract

*Audience: anyone who needs to know exactly what cesium-kafka guarantees, under what conditions, and
why.* This is the **correctness contract**. It states the guarantee, the invariants the engine
enforces, the proof that the replay barrier closes the duplicate window, how ambiguous commits are
handled, what each store archetype guarantees, and — critically — **the conditions the guarantee
depends on.** For the architecture see [`architecture.md`](architecture.md); for the full proofs,
decision log, and the complete failure matrix see [`design.md`](design.md); for which test backs
each failure-matrix row see [`failure-matrix-coverage.md`](failure-matrix-coverage.md).

---

## 1. The guarantee, stated precisely

> **Every record with a valid delay header is delivered to the destination topic exactly once, at or
> after its requested time, as observed by a `read_committed` consumer of the destination — provided
> the [stated conditions](#8-the-guarantee-is-conditional) hold.**

Three load-bearing qualifications, each expanded below:

1. **"as observed by a `read_committed` consumer"** — the guarantee is defined against
   `read_committed` reads. A `read_uncommitted` consumer **will** see the aborted records that
   fencing produced (the "duplicates" fencing prevents) and is the wrong tool for verifying delivery.
   See [§2](#2-read_committed-is-mandatory).
2. **"at or after its requested time"** — delivery is never *early*; lateness is bounded but real.
   See [§9](#9-at-or-after-requested-time).
3. **"provided the stated conditions hold"** — the guarantee is **conditional** on committed-offset
   retention, broker topic config, and explicit-reset posture. See [§8](#8-the-guarantee-is-conditional).

Records **without** a delay header, or already due, relay immediately. Malformed or over-max-delay
records follow an explicit policy (default: dead-letter), never silent early delivery. Records whose
payload is no longer fetchable at dispatch time are resolved exactly once via the DLQ loss-notice
path — never silently dropped.

---

## 2. `read_committed` is mandatory

cesium achieves exactly-once by making both loops **Kafka read-process-write transactions** with
KIP-447 group-metadata fencing. Fencing works by *aborting* a zombie's transaction — which means the
zombie's destination writes are physically present in the log as **aborted** records. Only a
`read_committed` consumer filters them out.

```properties
# destination consumer — REQUIRED
isolation.level=read_committed
```

A `read_uncommitted` consumer observes aborted batches and will report "duplicates." This is not a
cesium bug; it is reading uncommitted data. The requirement is stated in the
[README](../README.md#exactly-once-observed-by-read_committed), the
[quickstart](../README.md#quickstart-5-minutes), and here because it is the single most common way to
misread the guarantee.

`read_committed` is also locked **internally**, on every cesium consumer (ingest, tracker, seek) — it
is a correctness invariant, not a tuning knob, because KIP-447's `require_stable` takeover ordering
is only set under `read_committed` (see [§5](#5-the-replay-barrier-hw-not-lso) and
[§8](#8-the-guarantee-is-conditional)).

---

## 3. Invariants I1–I9

These are enforced by the engine and asserted in unit, contract, and integration tests
([`design.md` §1.4](design.md#14-correctness-invariants-engine-enforced-asserted-in-tests)). They are
the contract a conforming store may rely on.

| | Invariant |
|---|---|
| **I1** | Every engine-produced record (destination, tracker, DLQ) is produced inside a transaction. |
| **I2** | Every transaction that produces records also calls `sendOffsetsToTransaction` with the producing loop's group metadata, covering **every partition whose scheduler state the transaction affects** — even if the offset value is unchanged. *This is the fencing hook; a transaction without it is unfenced.* |
| **I3** | A transaction never spans a `consumer.poll()` call. Rebalance callbacks run inside `poll()` on the same thread, so no transaction can be in flight when a revocation callback runs. |
| **I4** | Entries of tracker partition *p* are dispatch-eligible only while *p* is `ACTIVE` (replay complete to the barrier). |
| **I5** | The committed cursor offset per partition is monotonic, and at commit time every pending entry either has `trackerAddOffset ≥ cursorOffset` or is encoded in the cursor sidecar. A ring-resident entry's `trackerAddOffset` is never increased in place. A violation is surfaced as metric + log, never a corrupted commit. |
| **I6** | COMPLETE markers for partition *p* are only ever *committed* by the member that owned *p* in group B at commit time (a consequence of I2 + KIP-447). |
| **I7** | Replay applies only committed records (`read_committed`) and tolerates COMPLETE-without-ADD (no-op) and anomalous duplicate ADD (update `dispatchAtMs` only, keep the original `trackerAddOffset`, warn metric). |
| **I8** | `barrier(p)` is snapshotted strictly **after** the committed cursor `c(p)` for the current assignment epoch is known (i.e. after the offset fetch — which waits out `UNSTABLE_OFFSET_COMMIT` — resolves). A barrier future is discarded if *p* is revoked; re-assignment re-snapshots. |
| **I9** | After an **in-doubt** transaction commit (ambiguous outcome), the in-flight batch's in-memory state is **never** restored to the index. The engine retries the commit to a definitive outcome, or drops the affected shards and re-enters recovery. |

The two foundational consequences:

- **Unique committed ADD.** The ingest transaction commits immediate relays + ADDs + DLQ records +
  source-offset advancement atomically. A crash before commit leaves offsets unmoved and everything
  produced aborted (invisible under `read_committed`); the next owner reprocesses. **At most one
  committed ADD per `(partition, sourceOffset)` ever exists** — this underpins compaction-key
  correctness, arrival-log sortedness, and the replay rules. It depends on group A running
  `read_committed` (only then does the offset fetch set `require_stable`).
- **Atomic dispatch.** The dispatch transaction commits destination payloads + COMPLETE tombstones +
  the cursor atomically. A crash between the destination send and commit aborts everything ⇒ the
  tombstone is invisible ⇒ replay shows the entry pending ⇒ it re-dispatches; the aborted destination
  write is invisible to `read_committed`. This closes the PoC's duplicate window.

---

## 4. Fencing in one paragraph

A zombie (paused, partitioned, or pre-crash) holds a stale generation / member epoch. Its
`TxnOffsetCommit` is rejected (`ILLEGAL_GENERATION` / fenced member epoch), the engine aborts, and
**all records in that transaction — including destination writes — become invisible** under
`read_committed`. By I2 there is no code path where a zombie commits a destination write without
passing this check. When a new member takes over a partition, its offset fetch returns
`UNSTABLE_OFFSET_COMMIT` while any transaction containing offsets for that partition is pending and
retries internally — so **by the time the new owner knows its starting cursor, every prior
transaction for that partition has resolved.** That takeover ordering exists only under
`read_committed` (`require_stable`), and it is the synchronization point the barrier proof relies on.

---

## 5. The replay barrier: HW, not LSO

On takeover of a tracker partition the new owner must replay from its committed cursor up to a
**barrier** before dispatching, so it sees every COMPLETE a previous owner committed. The barrier is
the partition **high watermark** (`Admin.listOffsets(latest, READ_UNCOMMITTED)`), *not* the LSO
(`endOffsets` under `read_committed`). Two scenarios force this.

### 5.1 The LSO hazard (why the barrier is the HW)

Previous owner *O* dispatched entry *X* in committed transaction *T*: destination write *D_X*,
tombstone *C_X* at tracker offset `o_C`. Meanwhile an **ingest** transaction *T′* — open since before
*T* committed — holds an ADD in partition *p* at an offset *below* `o_C` and is still open. Then
`LSO(p) < o_C`. A replay that stops at an LSO snapshot never sees *C_X*, concludes *X* is pending,
and (being past due) re-dispatches it: **duplicate.** Only ingest transactions can create this
interleaving, and one can stay open up to `transaction.timeout.ms` — a real window.

**Why the HW is sufficient.** A `read_committed` consumer cannot pass an offset until every
transaction below it has resolved (the LSO must reach it). So `position(p) ≥ barrier(p)` implies every
record below the barrier is stable — committed records (including *C_X*) applied, aborted ones
skipped. The wait is bounded by the longest open transaction (≤ `transaction.timeout.ms`, default
30 s; typically milliseconds). No committed COMPLETE relevant to takeover can appear above the
barrier "from the past": COMPLETEs for *p* are written only by dispatch owners (I6), and by I8 every
prior owner's transaction resolved before the snapshot. A zombie can still append afterward, but its
transaction carries a group-B offset commit (I2), which is fenced ⇒ aborts ⇒ never visible.

### 5.2 The I8 snapshot-ordering scenario

The barrier must be snapshotted **strictly after** the committed-cursor fetch resolves. Consider a
*stalled* predecessor *Z*: it sends *D_X* and *C_X* (buffered client-side), its
`sendOffsetsToTransaction` is **accepted** (generation still valid), then it stalls before
`commitTransaction`; its session expires and *p* moves to new owner *N*.

- **If *N* snapshotted the barrier at callback time** (before resolving the cursor), the snapshot
  would predate *C_X*'s append. *Z* then resumes: `commitTransaction` flushes — *C_X* lands *above*
  the stale barrier — and commits successfully (*Z*'s producer was never fenced; nobody ran
  `initTransactions` on its id). *N*'s replay, gated only to the stale barrier, never applies *C_X* ⇒
  **duplicate.**
- **With I8**, *N*'s offset fetch blocks until *Z*'s transaction resolves (the offsets are pending —
  `UNSTABLE_OFFSET_COMMIT`); the barrier snapshot therefore happens *after* *C_X* and its commit
  marker are in the log, so `barrier > o_C` and *C_X* is replayed. No duplicate.

This is covered by a dedicated integration test (`BarrierOrderingI8IT`, row D-14 in the
[failure-matrix coverage](failure-matrix-coverage.md)). Getting either the HW-vs-LSO choice or the
snapshot ordering wrong silently reintroduces a duplicate window, which is why those two integration
tests are non-negotiable.

Full case analysis is in [`design.md` §3.6](design.md#36-replay-barrier-the-lso-hazard-snapshot-ordering-and-integrity-checks).

---

## 6. The committed cursor and I5

Recovery does not depend on log compaction; it depends on the committed cursor being a *sound* lower
bound on pending state. **Invariant I5** is that bound: at every commit, the cursor offset is
monotonic per partition, and every pending entry either sits at or above the cursor offset **or** is
carried in the sidecar. A lone committed ADD with no committed COMPLETE is therefore always
recoverable — it is either seeded from the sidecar or replayed from `[cursor, barrier)` — and a lone
ADD is never compacted away (the tracker is compaction-only; a key with no newer record survives).

The tombstone-retention floor (`delete.retention.ms ≥ 2 × max(delay.max, observed oldest-pending age,
committed-cursor age)`) guarantees the tombstones a replay might need are still present; it does
**not** bound replay *cost* — the cursor v2 design does that (see
[architecture § cursor v2](architecture.md#5-cursor-v2--sidecar-recovery-model)). In overflow mode the
honest residual cost is `replay_records ≈ completion_rate(p) × age(cut) + pending(p)`, observable
before it bites and tunable by raising the sidecar budget. This is the load-bearing invariant the
published contract kit makes observable — see
[store-spi § the cursor contract](store-spi.md#6-the-cursorsidecar-contract).

---

## 7. In-doubt commits and the error taxonomy

A single three-way classification governs every transactional failure
([`design.md` §3.8](design.md#38-producer-error-taxonomy--in-doubt-commits)). It was **re-verified
against the exact `kafka-clients` 4.3.0 client at M5** with MockProducer fault injection (and the
LSO/barrier integration scenarios run against an `apache/kafka:4.3.0` broker).

1. **Definitively aborted** — `CommitFailedException` at `sendOffsetsToTransaction`, a fenced-member
   offset commit, an explicit successful `abortTransaction()`, or any abortable exception
   (`InvalidProducerEpochException` is abortable under KIP-588). ⇒ abort, **restore** the popped
   entries to the heap, continue. Bounded retries with exponential backoff and a cause-tagged metric.
2. **Fatal** — `ProducerFencedException`, `OutOfOrderSequenceException`, unrecoverable
   authorization/config errors. ⇒ close clients, fail the worker, process exits non-zero (readiness
   already false). The durable log is authoritative for the successor.
3. **In-doubt** — `TimeoutException` (or any ambiguous failure) from `commitTransaction`: the broker
   may have committed (`PREPARE_COMMIT` completes as commit). **The in-memory batch is never restored
   — invariant I9** — because restoring is the duplicate vector when the durable state already
   contains the committed tombstones and destination writes. The procedure:
   - **(a)** retry `commitTransaction` (the producer supports retrying a timed-out commit) to a
     definitive outcome, bounded by `kafka.transactions.commit-retry`; then finalize or restore
     accordingly; otherwise
   - **(b)** drop the in-memory shards for every partition the transaction touched, recreate the
     producer, `initTransactions()` (which resolves the dangling transaction deterministically
     broker-side but **does not report the outcome** — documented), re-fetch the committed cursors,
     and re-enter `RECOVERING` for those partitions (cheap under cursor v2). The replay reconstructs
     the truth either way.

**Retry-exhaustion is never a crash-loop.** When definitively-abortable retries exhaust (e.g. the
destination is under-replicated for 20 minutes), the loop **parks** the batch — entries return to
pending with a penalty not-before — keeps polling (membership alive), flips a `degraded` health
detail + `cesium_degraded` gauge, and alerts. Offsets stay unmoved; the system idles out broker
degradation instead of exiting and replaying every cycle.

The contract kit proves a store survives this: an in-doubt commit drives **neither** `onBatchCommitted`
nor `onBatchAborted` — the partitions are dropped and re-recovered, and the store must converge from
durable state alone (see [store-spi](store-spi.md#7-the-correctness-checklist)).

---

## 8. The guarantee is conditional

Exactly-once holds **only while cesium's durable anchors survive.** These are not edge cases to gloss
over — they are stated so operators configure for them.

### 8.1 Committed-offset retention (the big one)

cesium's entire knowledge of "where am I" lives in **committed consumer offsets** (group A's source
position, group B's cursor + sidecar). Kafka expires committed offsets after
`offsets.retention.minutes` of group inactivity (KIP-211: the clock runs from the last commit, even
for a live-but-idle group). If a group's offsets expire — say after an outage longer than broker
retention — the next fetch finds **no committed offset.**

cesium refuses to guess. **`auto.offset.reset=none` is locked on both groups (D18).** A missing
offset surfaces as `NoOffsetForPartitionException` and is a **fail-fast with a runbook**, not a
silent reset. The alternatives are both catastrophic and silent:

- `auto.offset.reset=latest` would **skip** everything between the lost offset and the live end —
  silent loss.
- `auto.offset.reset=earliest` would **reprocess from the beginning** — mass duplication.

The operator chooses the reset point explicitly. To keep this from happening at all, a startup check
compares the broker's `offsets.retention.minutes` against
`startup-checks.max-tolerated-outage` (default `P7D`) and warns/fails on a too-short retention.

### 8.2 First-run seeding (the one legitimate explicit reset)

A brand-new deployment has no committed offsets, which is indistinguishable from expired offsets to a
machine — hence the locked `none`. So first-run is an **explicit operator step**: group A's offsets
are seeded once (the quickstart's `topic-init` step does this), and group B performs an *explicit*
seek-to-beginning **only** when it can prove a true first run (no committed offsets anywhere for the
whole group). A replay-from-beginning of the compacted tracker is *correct* — completed entries are
tombstone-paired or fully compacted — but is logged prominently and may be slow. This is the
"correct-but-slow replay from zero" case; it is sound, just not free.

### 8.3 Tombstone retention and topic identity

The tombstone-retention floor ([§6](#6-the-committed-cursor-and-i5)) must hold, or an overflow-mode
replay could miss a needed tombstone (duplicate). It is validated at startup (FAIL) and re-validated
periodically (refuses to advance into the unsafe regime). Source and tracker **topic identity**
(topic id + cluster id) is bound into every committed metadata blob and re-validated: a recreated or
truncated topic is a fail-fast, never a silent wrong-payload delivery or replay-into-empty-log. A
deliberate same-id offset reset (e.g. a cluster restore from backup) remains undetectable by design —
documented.

### 8.4 Source payload availability

Payloads are pointer-only: the source must retain a record at least until its scheduled dispatch
time. Startup validates `delay.max + margin ≤ effective source retention`, and because
`retention.ms=-1` does not bound size-based or tiered eviction, size/tier modes require an explicit
`startup-checks.size-based-retention: ACKNOWLEDGED`. A payload that expires anyway is resolved exactly
once via the DLQ loss-notice path (`reason=payload-expired`) — its *loss* is unrecoverable (pointer-
only is a locked design choice), but it is never silent.

---

## 9. At-or-after requested time

Delivery is **never early, and late by a bounded amount.** Within a partition, due entries surface in
`(dispatchAtMs, arrival)` order; across partitions, in effective-deadline order. Sources of bounded
lateness:

- The **pipeline floor** for a freshly-arrived delayed record: ingest-commit + tracker-consume +
  `read_committed` LSO visibility (~100–500 ms). For a record scheduled comfortably ahead of its due
  time this is fully absorbed; for sub-second delays it dominates (best-effort below a threshold).
- **Due-storms** drain in due order, time-sliced, late but exactly once — the loop interleaves polls
  to keep membership alive rather than dropping records.
- **Clock skew** between dispatch workers shifts firing by the skew; NTP is assumed, and the
  `cesium_dispatch_lag_seconds` histogram (actual − scheduled) surfaces it. No EOS impact.
- A **penalty-boxed** source partition (degraded payload fetches) delays its own entries with
  exponential backoff so one bad partition cannot head-of-line block healthy ones — again, late not
  lost.

Scheduling precision is **measured and documented, not contractual** (sub-100 ms precision is a
non-goal for v1). See [`performance.md`](performance.md) for the measured `dispatch_lag` distribution.

---

## 10. Store-archetype guarantees

The delivery guarantee depends on which store archetype is in use. The flagship `KafkaTrackerStore`
is exactly-once; the external archetype is at-least-once by default, upgradeable to effectively-once.
This is declared by each store via `capabilities()` and surfaced on `/info` — never discovered in
production.

| Phase | `TrackerBackedStore` (EXACTLY_ONCE) | `ExternalSchedulerStore` (AT_LEAST_ONCE → effectively-once) |
|---|---|---|
| Ingest: schedule | `encodeSchedule` bytes produced **inside** the ingest txn | `upsertScheduled` **before** the txn commit; idempotent on retry |
| Ingest crash | txn aborts ⇒ nothing visible anywhere | aborted txn ⇒ row already upserted; re-poll re-upserts (no-op) |
| Dispatch: settle | `encodeCompletions` tombstones **inside** the dispatch txn; cursor (offset + sidecar) committed via `sendOffsetsToTransaction` | `markDispatched` **after** commit (at-least-once), or a cursor in the offset metadata committed atomically (effectively-once) |
| Dispatch crash | atomic: redispatch cleanly or fully settled — exactly-once; in-doubt resolved by replay, never by restore (I9) | window between commit and `markDispatched` ⇒ possible duplicate, surfaced via `capabilities()`; eliminated by `cursorToCommit` reconciliation |
| Recovery | engine seeds the sidecar, replays cursor → barrier; store rebuilds the index | `scanPending` per partition, reconciled against the committed cursor if present |

The difference is structural: a tracker-backed store's completion facts share Kafka's transactional
atomicity (tombstones + cursor inside the dispatch transaction), so there is no window. An external
store cannot enlist its writes in the Kafka transaction, so it relies on **ordering contracts**
(idempotent upsert before ingest commit; mark-dispatched after dispatch commit) — which gives
at-least-once, closable to effectively-once by committing a reconciliation cursor in the
offset-metadata channel. See [store-spi § archetypes](store-spi.md#2-pick-an-archetype).

---

## 11. The failure matrix

The design enumerates a row for **every** crash/fault point across the ingest loop, the dispatch
loop, and recovery/environment, each mapping to a named test. Rather than reproduce ~36 rows here,
this doc states the guarantee they collectively establish and points to the authoritative sources:

> For every fault point — crash before/after each `send`, before/after `sendOffsets`, during
> `commitTransaction`, zombie commit after losing a partition, cooperative revocation mid-batch,
> in-doubt commit on a live process, an open foreign transaction holding the LSO below a committed
> COMPLETE at takeover, a predecessor that commits after takeover begins, payload expiry, broker
> degradation outlasting retries, committed-offset expiry, and topic recreation/truncation — the
> outcome is **no loss and no duplicate under `read_committed` destination consumers**, achieved by
> retry, exactly-once atomic settlement, fail-fast, or park-and-degrade. **No silent terminal
> states.**

- The full row-by-row matrix with the "why ✔" for each:
  [`design.md` §3.9](design.md#39-failure-matrix).
- The mapping from each row to the concrete test that covers it and the lane it runs in:
  [`failure-matrix-coverage.md`](failure-matrix-coverage.md).

---

## See also

- [`architecture.md`](architecture.md) — topology, shard state machine, cursor v2, threading.
- [`store-spi.md`](store-spi.md) — the archetype ordering contracts and correctness checklist a store
  must satisfy to preserve these semantics.
- [`design.md`](design.md) — the full proofs (§3.4–§3.8), decision log, and risks.
- [`performance.md`](performance.md) — measured `dispatch_lag`, the replay-cost formula.
