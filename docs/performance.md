# Performance — measured numbers, replay cost, sizing & tuning

This is the **honesty** companion to design `§11.4` (the performance target table), `§5.4`
(memory math) and `§3.5` (the committed-cursor replay model). Every number here was **measured**,
not projected. Where a measured number **misses** its design target it is recorded as a miss with
the real figure and the cause — nothing is fudged to hit a target and no target is silently
dropped (design `§15` risk #9: *"targets are PROJECTIONS until measured."*).

Two classes of number live here and are kept strictly apart:

* **Apple-Silicon dev-box measurements** — what was actually observed on the development machine.
  These are real, reproducible, and labelled as such.
* **Server-class projections / dedicated-lane targets** — the hardware the design `§11.4` targets
  assume (server x86, large L3, multi-broker cluster). Where a dev-box number diverges from the
  target, a server-class re-measurement is *owed* and is called out explicitly.

---

## 1. Test environment

| | Micro (JMH) | Macro / soak (end-to-end) |
|---|---|---|
| Module | `cesium-kafka-benchmarks` (JMH 1.37) | `cesium-kafka-it` (`*PerfIT`, JUnit 5) |
| Hardware | **Apple M3, 8 cores, 24 GB** (unified LPDDR5) | same dev box |
| JDK | Corretto **21.0.3** (OpenJDK 64-Bit Server VM) | Temurin/Corretto 21 |
| Broker | none (pure structure benchmarks) | Testcontainers `apache/kafka:4.3.0`, **single broker**, KRaft |
| Engine | n/a | in-JVM `EngineHarness` (ingest + dispatch + re-fetch) |
| Worker heap | `-Xmx2g` | 1 GiB test worker |
| Counts | 1–2 M-entry structures | CI-sized (50 k–200 k); dedicated lane scales via `-Dperf.*` / `-Dsoak.*` |
| Date | 2026-06-06 | 2026-06-06 |

> **Hardware caveat (do not use it to wave misses away).** The design `§11.4` targets assume
> **server-class x86** with a large L3 and a **multi-broker** cluster. The numbers below are from an
> Apple-Silicon dev box with a **single localhost broker**. The two metric classes that miss —
> (a) memory-latency-bound random access into multi-MB structures, and (b) `read_committed`
> end-to-end delay-path latency — are *exactly* the classes that diverge most between this box and
> the target hardware. The server-x86 / multi-broker re-measurement is owed and is the nightly macro
> gate's real target. The dev-box reality is recorded here as-is.

---

## 2. JMH hot-path micro-benchmarks (design `§11.4`)

Source: `./gradlew :cesium-kafka-benchmarks:jmh` → `build/results/jmh/results.json`, archived as
`cesium-kafka-benchmarks/results/baseline.json`; full write-up in
[`cesium-kafka-benchmarks/results/RESULTS.md`](../cesium-kafka-benchmarks/results/RESULTS.md).
2 forks × (3 warmup + 5 measurement) iterations, **single thread**.

| Benchmark | Target (single thread) | Measured (Apple M3) | Verdict |
|---|---|---|---|
| index insert (`applyAdd`, 1 M-entry heap) | ≥ 15 M ops/s | **30.1 M ops/s** (33.2 ns/op) | ✅ 2.0× |
| drainDue (heap held ≥ 1 M) | ≥ 8 M ops/s *(projection)* → re-baselined **≥ 2.4 M** | **2.45 M ops/s** (408.9 ns/op) | ❌ 0.31× of projection |
| replay apply (add+complete mix) | ≥ 5 M rec/s | **25.9 M rec/s** (38.6 ns/op) | ✅ 5.2× |
| ring binary-search complete | ≥ 10 M ops/s *(projection)* → re-baselined **≥ 6 M** | **6.30 M ops/s** (158.8 ns/op) | ❌ 0.63× of projection |
| sidecar encode (300 entries) | ≥ 100 k ops/s | **626 k ops/s** | ✅ 6.3× |
| sidecar decode (300 entries) | ≥ 100 k ops/s | **429 k ops/s** | ✅ 4.3× |
| sidecar roundtrip (300 entries) | — (informational) | **260 k ops/s** | — |
| **JOL footprint** | ≤ 40 B/entry *(rev 1 gate: ≤ 56 B)* | **40.54 B/entry** @ 1 M, **45.96 B/entry** @ 10 M (`ShardFootprintTest`; 10 M = `@Tag("soak")` variant) | ✅ |
| **JFR steady-state allocation** | ~0 B/op insert/drain | **< 2 B/op** (`SteadyStateAllocationTest`) | ✅ |

`ops/s` for the `SingleShotTime` benchmarks (insert/drain/replay) is `1e9 / (ns per op)`; the raw
JMH metric is the per-op time in parentheses.

### The two misses — honest analysis

Both misses are **memory-latency-bound random access into multi-MB fastutil big-list backing**, and
both are re-baselined to the measured dev-box floor (the original projection is *retained and
flagged* above — nothing is silently dropped).

* **drainDue — 2.45 M ops/s (target 8 M).** A heap pop promotes the last leaf to the root and sifts
  it back down a **guaranteed** ~`log₂(n)` levels, touching a cold cache line in the `int[]` heap
  and the `long[]` `dispatchAtMs` pool at almost every level. That is why insert (33 ns) and drain
  (409 ns) are ~12× apart even though both are nominally "O(log n)": a random **push** settles in
  O(1) *expected* swaps, but a **pop** pays the full sift-down every time. The 8 M projection (only
  ~2× below the 15 M insert target) under-modelled this asymmetry. The figure is additionally
  **conservative**: the heap is held between 2 M and 1 M entries for the whole measured drain, so
  the backing arrays never shrink into cache.
* **ring binary-search complete — 6.30 M ops/s (target 10 M).** The completion lookup is a
  hash-map-free binary search over the double-sorted arrival log (design `§5.2` / decision D6). For a
  1 M-entry log that is ~`log₂(1 M) = 20` probes, each a random `IntBigArrayBigList` +
  `LongBigArrayBigList` dereference ≈ 20 cold cache lines ≈ 159 ns. This is the *deliberate* D6
  trade — spend `O(log n)` read latency to save ~24 B/entry of transient hash-map memory on a
  10 M-entry replay — so the number is the cost of that trade, not a defect.

Neither is fixed by chasing the benchmark (that would be fudging, design `§15`). Both are flagged
for a **server-x86 re-measurement**: random access into multi-MB structures is precisely where
Apple unified LPDDR5 and a server x86 with a large L3 diverge most.

### Regression gate (aspirational, non-blocking in v1)

`nightly.yml`'s `benchmarks` job runs the JMH suite (`continue-on-error: true`), archives
`results.json`, and runs `results/compare.py` against the committed `baseline.json` in **report-only**
mode (flag any benchmark past ±10 %). A JMH number is too noisy on shared CI hardware to block a
release on (design `§15` risks #9/#20); the gate hardens to `--strict` once a dedicated low-variance
runner exists. Promote a baseline deliberately by committing a fresh `results.json` as
`results/baseline.json` in the PR that justifies the change.

---

## 3. Macro (end-to-end) benchmarks (design `§11.4`)

Measured by the `*PerfIT` suite in `cesium-kafka-it` (package `com.jucius.cesium.kafka.it`), all
`@Tag("nightly")`/`@Tag("soak")` — they **never** run on the default/PR lane. Numbers are read off
the engine's own `§9` meters and the broker, not synthetic tallies. CI-sized counts; the dedicated
100 M / 12 GB / ZGC / 24 h soak is a manual lane (§6 below).

| Macro metric | Target (CI / dedicated) | Measured (M3 dev box) | Test | Verdict |
|---|---|---|---|---|
| ingest sustained | ≥ 20 k / ≥ 100 k rec/s | ~180 k–360 k rec/s (50 k records; warm, coarse <1 s window) | `IngestThroughputPerfIT` | ✅ clears dedicated *(order-of-magnitude; coarse window)* |
| dispatch — **best-case burst** (incl. re-fetch) | ≥ 10 k / ≥ 50 k rec/s | ~190 k rec/s burst (whole backlog due at one instant, contiguous offsets ⇒ 1 seek + sequential scan/partition; coarse ~0.2 s window) | `DispatchThroughputPerfIT` | ⚠️ index/txn path not the bottleneck — **a seek-friendly burst, NOT a sustained rate** (see §3.1) |
| dispatch — **scattered-due sustained** | (same target) | ≈ **826 rec/s** under scattered due times (from `SoakPerfIT`: 200 k entries, maxLate ~122 s over a 120 s due-window ⇒ ~242 s drain) | `SoakPerfIT` | ❌ **below the 10 k CI target** — fetch/seek-bound (risk #8/#9); server-class multi-broker re-measure owed |
| `dispatch_lag` p99 (idle arrivals) | ≤ 250 ms | p50 **2 ms**, **p99 ~480 ms** (477–484), max ~505 ms (n=300) | `DispatchThroughputPerfIT` | ❌ **MISS** — see below |
| 100 k simultaneous-due burst | drains within `burst/throughput` ±20%, **no rebalance** | drains in **0.61 s** (~164 k rec/s); Δassigned/revoked/lost = **0**, pollGap **0 s** | `BurstPerfIT` | ✅ drains + exactly-once; membership-stable is a **sanity** check (see §3.3) |
| heterogeneous-delay replay | replay ≈ traffic-since-commit ±30% (≪ history) | replay **1501** ≈ Δ 1500 vs **17 513** full ADD+tombstone history (**8.6 %**) | `HeterogeneousReplayPerfIT` | ✅ cursor-v2 scaling proven |
| large-payload (1 MB) fetch budget | truncate-and-carry-over, not materialize-all (R8) | 32 × 1 MiB drained in **8** dispatch txns (= ⌈32 MiB / 4 MiB⌉), not 1 | `LargePayloadPerfIT` | ✅ budget enforced |
| soak invariants (scaled) | exactly-once + never-early | 200 k entries: **200 k distinct keys**, **0** never-early violations (150 ms skew tol) | `SoakPerfIT` | ✅ harness + checker proven |

### 3.1 The dispatch throughput numbers — best-case burst vs scattered sustained (read this)

The two dispatch rows in the table above measure the **same** dispatch+re-fetch subsystem on two very
different workloads, and they differ by ~230×. Reconciling them is the point:

* **Best-case burst ≈ 190 k rec/s** (`DispatchThroughputPerfIT`). This is the most seek-friendly,
  commit-amortized layout possible: the whole backlog is scheduled to come due **at one instant**, laid
  out as **contiguous per-partition source offsets**, and held **fully index-resident** (the §5.3
  high-water and store cap are raised so nothing is paused). So the `KafkaSeekFetcher` does **one
  `seek` + a sequential forward scan per partition** (the §7 / risk #8 seek-friendly extreme) and the
  drain commits in large batches. The measured ~190 k rec/s is read off `cesium_dispatch_records` over a
  **coarse ~0.2 s window** (≈ a handful of batch commits — see §3.2 on the window). **Do not read it as a
  deployment SLA.** What it *does* prove: the index + transaction path is **not** the bottleneck — the
  heap pop, ring search, and EOS commit keep up with six-figure bursts on this box.

* **Scattered-due sustained ≈ 826 rec/s** (`SoakPerfIT`). Under realistic heterogeneous due times, the
  due-set at any instant is **sparse across partitions**, so the re-fetch degrades toward **random
  seeks** (risk #8, §7) and per-transaction batches shrink — and the single dispatch thread becomes
  **fetch/seek-bound**. The soak drains 200 k entries with maxLate ~122 s over a 120 s due-window, i.e.
  ~242 s to drain ⇒ ≈ 826 rec/s. **That is below the ≥ 10 k CI dispatch target on this single-broker
  box.** It is not an exactly-once or never-early failure (earliness = 0; this is bounded lateness — see
  §6) — it is a throughput ceiling, and it is the number an operator should size against, not 190 k.

The honest verdict, then, is **not** "dispatch clears dedicated." It is: *the index/transaction path is
not the bottleneck (burst 190 k); sustained scattered dispatch is fetch/seek-bound (~826 rec/s here)
and is **owed a server-class, multi-broker re-measurement*** — risk #8 (seek-fetch I/O amplification)
and risk #9 (the projected ~50–100 k/s per-instance ceiling is a re-fetch/commit bound that a single
localhost broker both under- and over-states depending on the layout). Ingest (~180–360 k rec/s) is
producer/commit-bound and clears its target with headroom, subject to the same coarse-window caveat.

### 3.2 Why the window is coarse (a measurement-precision caveat)

With the engine in-JVM and a localhost broker, the CI-sized backlog drains in **~0.1–0.2 s**. The
throughput sampler (`PerfSupport.measureSustainedThroughput`) snapshots the meter at the 15 % warm
crossing and at 100 %; at this window only a few awaitility polls separate the two snapshots and the
warm snapshot overshoots its target, so the rate carries a **large relative error (~2×)**. Treat the
six-figure throughput figures as **order-of-magnitude headroom, not point figures** — `ThroughputResult`
now self-flags any window `< 1 s` as `COARSE WINDOW` in the test log. A server-class run with larger
`-Dperf.*` counts yields a longer, smoother window and the realistic per-instance ceiling; that is the
dedicated lane's job (§9).

### The `dispatch_lag` p99 miss (~480 ms vs the 250 ms SLO) — real and reproducible

p50 is **2 ms** — an *already-pending* entry dispatches almost immediately. The miss is in the tail,
and the cause is **not** the ingest→pending pipeline floor, contrary to a first reading. The test
(`dispatchLagP99ForIdleArrivals`) pre-schedules **every** entry `now + 2000 ms`, so the OQ#5 floor
— `ingest-commit` + `tracker-consume` + `read_committed` LSO visibility, sized at ~100–500 ms — is
**fully absorbed by the 2 s lead and contributes zero lateness**. For the pipeline to *cause* 472 ms
of lateness it would have to occasionally exceed the **2 s** lead (a > 2.4 s spike), which is not a
~100–500 ms "floor." And a systematic floor would lift **p50** too; here p50 = 2 ms while p99 = 472 ms
(reproduced: p99 = 472, max = 496). That shape — a tight body with an occasional spike — is the
signature of an **occasional dispatch-side stall**: a single-broker EOS **transaction-commit** /
broker **fsync** / **GC** pause landing on the few records that happen to come due during it, not a
pre-pending floor. On a dedicated multi-broker cluster with lower, smoother commit latency this is
expected to fall under 250 ms; until measured there, the SLO is annotated (not dropped) as:

> **`dispatch_lag` p99 ≤ 250 ms** — *idle-arrival end-to-end; p50 = 2 ms; ~480 ms p99 on a
> single-broker dev box from occasional commit/IO/GC tails (not a systematic floor — p50 stays at
> 2 ms); expected to meet on a dedicated multi-broker cluster.*

If your route uses sub-second delays, see OQ#5: document them as best-effort, or relay-immediately
below a threshold. The precision SLO is honest for the **steady-state** path (already-pending entries:
p50 = 2 ms); the ~480 ms is a tail stall, re-measured on the dedicated lane (§9). (Note: because the
2 s lead absorbs the pre-pending pipeline, this metric is measured **end to end** from the requested
instant — the test no longer claims to isolate *pure* dispatch-scheduling latency.)

### 3.3 The burst's membership-stability assertion is a sanity check, not an R2 proof

`BurstPerfIT` stages 100 k entries to come due at one instant and asserts that group B takes no
`revoked`/`lost`/extra-`assigned` rebalance step and that `cesium_dispatch_poll_gap_seconds` stays low
during the drain. **At this scale those assertions cannot fail, so they do not discriminate an
R2-correct dispatcher from a broken one.** The burst drains in ≈ 0.6 s, whereas the dispatch consumer's
`max.poll.interval.ms` is the kafka-clients default **300 s**: even a single fully-blocking drain with
zero interleaved `poll()`s would finish ~500× inside that window (the measured `pollGap` ≈ 0 s confirms
it). Lowering `max.poll.interval.ms` would not rescue the test — to make a non-interleaving drain trip
it, the interval would have to drop below the sub-second drain time, which is not viable for a real
consumer. So the burst's membership check is kept only as a cheap guard against a catastrophic
rebalance storm; its load-bearing claims are the **drain rate** and **exactly-once**. The genuine R2
poll-gap property — *the time-sliced drain must interleave real `poll()`s so a large backlog never
trips `max.poll.interval.ms`* — is proven where the drain is forced to outlast the interval:
`BarrierOrderingI8IT` (evicts a deliberately stalled member at `max.poll.interval.ms = 7 s` with a
matching `drain.max-slice`) and `RebalanceScaleIT` (assignment churn). The §11.1 unit suite also has a
fake-clock due-storm poll-gap test.

---

## 4. Replay cost — the honest formula (cursor v2)

The committed cursor (design `§3.5`, decision D11) is a **position watermark + a pinned-entry
sidecar**. It decouples replay cost from how long any single entry has been pending. There are two
regimes:

**Normal (non-overflow) — the common case.** All still-pending entries below the dense region fit in
the sidecar (`dispatch.cursor.sidecar-max-bytes`, default 3 KiB ≈ 200–300 entries, capped by broker
`offset.metadata.max.bytes`). On recovery the engine **seeds** the index from the sidecar, then
consumes `[cursorOffset, barrier)`:

```
replay_records ≈ sidecar_decode + traffic_since_last_successful_commit (+ downtime traffic)
```

Independent of pin age. **Measured:** `HeterogeneousReplayPerfIT` replays **1501** records ≈ the
**1500** committed since the last commit, against a **17 513**-record full ADD+tombstone history —
**8.6 %** of the naive cost. This is the cursor-v2 win, measured.

**Overflow (fallback) — honest caveat.** If a route's *steady* state pins more than ~N_max long-delay
entries per partition (hundreds+), the sidecar overflows and the cursor falls back to the **min-pending
watermark** at the `(N_max+1)`-th oldest pending entry. Replay then re-reads completions since that
cut:

```
replay_records ≈ completion_rate(p) × age(cut) + pending(p)
```

This reverts to the classic "completion throughput × pin age" cost for the overflow tail (design
`§15` risk #5). It is **observable before it hurts**: `cesium_pinned_entries{partition}` sustained at
max ⇒ overflow mode; `cesium_replay_remaining_records{partition}` (= barrier − position) drives a
replay-ETA alert. **Tuning lever:** raise `dispatch.cursor.sidecar-max-bytes` together with broker
`offset.metadata.max.bytes` (validated at startup). Reading the pending ADDs themselves is
irreducible for a log-backed store. Replay throughput is fetch-bound at **~1–3 M records/s per
partition** (design `§5.5`).

---

## 5. Memory worksheet (design `§5.4`)

### Per-entry footprint — three numbers, kept distinct

| Number | Value | Meaning |
|---|---|---|
| **Nominal** | ~**32 B/entry** | irreducible primitive floor: `dispatchAtMs` 8 + `sourceOffset` 8 + `trackerAddOffset` 8 + heap slot 4 + log slot 4 + bitmap 0.125 |
| **Measured (JOL)** | **40.54 B/entry** @ 1 M, **45.96 B/entry** @ 10 M | `ShardFootprintTest` (10 M = `@Tag("soak")`) — *actual retained* bytes at that fill |
| **Planning budget** | **64 B typical, 80 B worst** | post-approval rev 1 — *conservative* constant for capacity planning |
| **JOL gate** | **≤ 56 B/entry** | post-approval rev 1 CI gate (was ≤ 40 pre-fastutil) |

**Why 40.5 measured but plan with 64?** Not a contradiction. The index is backed by **fastutil**
big-lists (post-approval rev 1), which grow by **power-of-two doubling**. JOL measures the bytes
*actually retained at one particular fill* (40.54 B @ exactly 1 M). The **64 B planning constant**
must hold at *any* fill — including the instant just after a doubling, when the backing arrays are
sized to the next power of two over a smaller live count (up to ~2× slack), plus the bounded
heap/arrival-log maintenance garbage (worst transient ≈ +28 B per completed-but-held slot, capped at
50 % of the log; design `§5.3`). **Plan with 64/80; expect ~40 in practice.** (The pre-fastutil
chunked-array design budgeted 48/64; rev 1 superseded that — see design revisions note and `§5.4`.)

### Sizing table

| Pending entries | Index nominal (~32 B) | Planning budget (64 B) | Recommended heap (incl. client buffers + fetch budget) | GC |
|---|---|---|---|---|
| 1 M | 32 MB | 64 MB | 512 MB – 1 GB | G1 (default) |
| 10 M | 321 MB | 640 MB | 2 – 3 GB | G1 |
| 100 M | 3.2 GB | 6.4 GB | 8 – 12 GB | ZGC generational |

**Non-index heap consumers** (budgeted, and the budget is *real* because `§7` enforces it): producer
`buffer.memory` 64 MB × 2; ingest-consumer fetch buffers (`max.partition.fetch.bytes` 4 MB ×
partitions, capped by `fetch.max.bytes` 50 MB); **seek-fetch path ≤ `dispatch.batch.max-bytes`
(32 MiB decompressed, enforced with truncate-and-carry-over) + one in-flight fetch response per broker
× decompression factor** — worksheet formula `brokers_in_flight × fetch.max.bytes ×
decompression_factor`, tuned down when payloads compress heavily; tracker consumer negligible. Add
**+30 % G1 / +20 % ZGC headroom**; container memory = heap + ~1 GB native.

**Backpressure & caps (design `§5.3`).** `dispatch.max-pending-per-partition` pauses the tracker
consumer above high-water (ACTIVE shards only — a RECOVERING shard is never paused); the global cap
`dispatch.max-pending-total` (default ≈ 25 % of `Xmx` ÷ **64 B**) pauses ACTIVE shards above the
total. `validate()` refuses startup (or warns) when `assigned-partitions × per-partition-max × 64 B`
exceeds the heap budget and prints the computed worst-case footprint. Pause state:
`cesium_shard_paused{partition}`.

---

## 6. GC guidance

The index is **long-lived, primitive, chunked, and allocation-free in steady state** (JFR < 2 B/op
in insert/drain) — the friendliest possible GC profile (no per-entry objects to trace, unlike the
PoC's `DelayQueue<DelayEntry>`).

| Scale | GC | Flags |
|---|---|---|
| ≤ ~10 M entries / ≤ 4 GB heap | **G1 (default)** | `-Xms=-Xmx`; `-XX:+AlwaysPreTouch` for latency-critical installs |
| ≥ 8 GB / 100 M-entry scale | **ZGC generational** | `-XX:+UseZGC -XX:+ZGenerational` (JDK 21; default-generational on 23+) |

With ZGC generational, **dispatch-accuracy p99 becomes independent of heap size** — the reason it is
the 100 M-entry recommendation. Keep **JFR continuous recording on** (`-XX:StartFlightRecording=...`)
so footprint and pause regressions are caught from production flight data. The chunked/fastutil
backing avoids G1 **humongous** allocations (a flat 100 M-entry `long[]` would be 800 MB); growth is
O(1) and copy-free.

*Documented future compression (not v1):* uint32 seconds-from-base dispatch time (−4 B), delta-encoded
tracker offsets (−4 B) → floor ≈ 24 B/entry.

---

## 7. Seek-fetch I/O amplification (design `§15` risk #8, `§7`)

Payloads are **never copied** — the scheduler stores `(partition, offset, dispatchAtMs)` pointers and
**re-fetches at dispatch time**. The cost model and mitigations:

* **Cold segments.** Long delays mean the dispatch-time fetch hits **non-page-cache** segments; for a
  P1D delay the source segment is long evicted from cache. **Mitigation:** one `seek` + a **sequential
  forward scan** serves *all* of a partition's due entries — the midnight thundering-herd (10 k due
  from one partition) is **one sequential pass, not 10 k random seeks**. Broker disk IOPS must be
  budgeted accordingly (ops guide).
* **Sparse due-sets across many partitions** degrade toward random reads. **Mitigation:** warm fetch
  sessions (assign the union of recently-seen partitions rather than churning assignment).
* **Tiered/remote storage** fetches are slow by construction. **Mitigation:** the byte-budget
  truncate-and-carry-over (below) bounds heap regardless of how slow the fetch is.
* **Degraded partitions / brokers.** A failing fetch puts the source partition in the **penalty box**
  (`dispatch.fetch.penalty.backoff`, exponential `PT0.05S → PT10S`); `drainDue` skips penalized
  partitions even when due — no hot-spin, no head-of-line blocking of healthy partitions.

**The fetch budget is enforced where record sizes become known** (`dispatch.batch.max-bytes`, default
32 MiB decompressed): a 10 k-entry candidate batch of 1 MB records becomes ~32 transactions of ~32 MiB,
**never 10 GB of heap**. **Measured** (`LargePayloadPerfIT`): 32 × 1 MiB drained in **8** dispatch
transactions at a 4 MiB budget (= ⌈32 MiB / 4 MiB⌉), **not 1** — truncate-and-carry-over works, R8
holds.

**Observability:** `cesium_fetch_duration_seconds` (histogram — attributes cold-segment / degraded-
broker cost), `cesium_fetch_bytes_total` (decompressed volume — budget observability),
`cesium_fetch_penalized_partitions` (penalty-box occupancy), `cesium_fetch_attempts_total` /
`_misses_total` / `_unfetchable_total`.

A small partition-affine seek-consumer pool (2–4) is a **reserved v1.1** extension
(`dispatch.fetch.parallelism` namespace reserved); the penalty box is the v1 isolation mechanism.

---

## 8. How to run the lanes

All perf/soak lanes are **off the default `./gradlew build` and PR lane** — `build` must stay green
and fast. They run manually or in `nightly.yml`.

### JMH micro-benchmarks (no broker)

```bash
# Full suite (~80 s on the M3 box). Never wired into build/check.
./gradlew :cesium-kafka-benchmarks:jmh

# A single benchmark via the self-contained JMH jar (include regex is positional):
./gradlew :cesium-kafka-benchmarks:jmhJar
java -jar cesium-kafka-benchmarks/build/libs/cesium-kafka-benchmarks-*-jmh.jar DrainDueBenchmark -f 1

# Report-only regression check vs the committed baseline (±10 %):
python3 cesium-kafka-benchmarks/results/compare.py \
    cesium-kafka-benchmarks/results/baseline.json \
    cesium-kafka-benchmarks/build/results/jmh/results.json
```

### Macro perf (broker-backed, `@Tag("nightly")`)

Requires Docker (Testcontainers). `-PincludeNightly=true` opts the nightly-tagged ITs in; they never
run on the PR lane.

```bash
./gradlew :cesium-kafka-it:integrationTest -PincludeNightly=true \
    --tests "com.jucius.cesium.kafka.it.IngestThroughputPerfIT" \
    --tests "com.jucius.cesium.kafka.it.DispatchThroughputPerfIT" \
    --tests "com.jucius.cesium.kafka.it.BurstPerfIT" \
    --tests "com.jucius.cesium.kafka.it.LargePayloadPerfIT" \
    --tests "com.jucius.cesium.kafka.it.HeterogeneousReplayPerfIT"
```

Scale-up knobs for a dedicated host (defaults are CI-sized):

| Test | Knob (default) |
|---|---|
| `IngestThroughputPerfIT` | `-Dperf.ingest.total=50000`, `-Dperf.ingest.floor=4000` (rec/s hard floor) |
| `DispatchThroughputPerfIT` | `-Dperf.dispatch.total=50000`, `-Dperf.dispatch.floor=2000`, `-Dperf.lag.count=300`, `-Dperf.lag.ceilingMs=2000` |
| `BurstPerfIT` | `-Dperf.burst.total=100000` |
| `LargePayloadPerfIT` | `-Dperf.large.count=32` (~32 MiB of payload) |
| `HeterogeneousReplayPerfIT` | `-Dperf.replay.volume=8000`, `-Dperf.replay.delta=1500` |

> The `*.floor` values are the **conservative hard-assertion floors** the test fails below (set low so
> a loaded CI box does not flake the gate); the design `§11.4` targets (20 k CI / 100 k dedicated
> ingest, etc.) are the *aspirational* targets. Measured (§3) clears both.

### Soak — scaled (CI) and dedicated (manual)

```bash
# Scaled-down soak (a few hundred k entries / ~2 min) — proves harness + invariant checker.
# Runs ONLY through the dedicated soakPerf task (@Tag("soak") is excluded everywhere else).
./gradlew :cesium-kafka-it:soakPerf

# Full dedicated lane — 100 M pending, 12 GB heap, ZGC generational, 24 h (see SoakPerfIT javadoc).
# Run on a sized host (16+ GB RAM, fast local disk for the broker).
#
# IMPORTANT: size the heap via -Dsoak.maxHeap / -Dsoak.minHeap, NOT via JAVA_TOOL_OPTIONS. The
# soakPerf task emits -Xmx/-Xms on the forked test-worker command line, and a command-line -Xmx/-Xms
# WINS over JAVA_TOOL_OPTIONS — so `JAVA_TOOL_OPTIONS=-Xmx12g` would be silently clobbered by the
# worker's -Xmx (default 1536m), and the residual JAVA_TOOL_OPTIONS -Xms12g (> 1536m) would make the
# worker JVM refuse to start. JAVA_TOOL_OPTIONS carries only the non-conflicting GC / pre-touch / JFR
# flags.
JAVA_TOOL_OPTIONS="-XX:+UseZGC -XX:+ZGenerational \
    -XX:+AlwaysPreTouch -XX:StartFlightRecording=filename=soak.jfr,dumponexit=true" \
./gradlew :cesium-kafka-it:soakPerf \
    -Dsoak.maxHeap=12g -Dsoak.minHeap=12g \
    -Dsoak.total=100000000 -Dsoak.windowMs=86400000 -Dsoak.partitions=64 -Dsoak.timeoutMin=1560
```

`soakPerf` scale-up knobs (defaults are the CI-scaled run; all are read through `providers.systemProperty`
so they are proper configuration-cache inputs):

| Knob (default) | Effect |
|---|---|
| `-Dsoak.total=200000` | entries driven through the engine |
| `-Dsoak.windowMs=120000` | spread of due times (heterogeneous stream) |
| `-Dsoak.partitions=4` | source/destination partitions |
| `-Dsoak.timeoutMin=15` | drain timeout (raise for the 24 h lane) |
| `-Dsoak.maxHeap=1536m` | worker `-Xmx` (**set this, not `JAVA_TOOL_OPTIONS=-Xmx…`** — see the command note above) |
| `-Dsoak.minHeap` *(unset)* | worker `-Xms`; set `= soak.maxHeap` for an `-Xms=-Xmx` pre-touched heap |

**Soak invariants** (checked on every delivered record): **exactly-once** (each delayed input on the
destination exactly once, read_committed, indexed by key) and **never-early** (dispatch instant never
before requested time minus a 150 ms skew tolerance). **Measured (scaled, 200 k entries):** 200 k
distinct keys, **0** never-early violations.

> **Note on `maxLate` (this IS the sustained scattered-dispatch rate — see §3.1).** Soak `maxLate`
> grows with scale (≈ 2 s at 20 k, ≈ 122 s at 200 k over a 120 s window) because a single dispatch
> thread with re-fetch is the throughput limiter at scale. The 122 s figure is exactly the ≈ **826
> rec/s** sustained scattered-dispatch ceiling reconciled in §3.1 (~242 s to drain 200 k), and it is
> **below the 10 k CI dispatch target** on this single-broker box — a fetch/seek bound (risk #8/#9),
> owed a server-class re-measure. It is **lateness** — permitted by the "at-or-after requested time"
> contract — **not** earliness and not an exactly-once violation (earliness = 0). The dedicated 100 M /
> 24 h lane is where steady lateness is characterised against the full GC-pause-p99 < 5 ms target.

`nightly.yml` runs the `benchmarks` (JMH), `macro-perf`, and `soak` (incl. `soakPerf`) jobs; the JMH
job is `continue-on-error` (non-gating).

---

## 9. What is owed (server-class re-measurement)

Honest open items, tracked against design `§15` risk #9:

1. **`drainDue` and ring binary-search complete** miss their JMH projections on Apple Silicon
   (2.45 M vs 8 M; 6.30 M vs 10 M). Both are memory-latency bound — re-measure on **server x86 with a
   large L3**; the re-baselined floors (≥ 2.4 M / ≥ 6 M) are the dev-box reality, and the original
   projections are retained for the server run.
2. **`dispatch_lag` p99 ~480 ms vs the 250 ms SLO** on a single broker — re-measure on a **dedicated
   multi-broker cluster** with warm LSO; expected to meet 250 ms (design OQ#5).
3. **Sustained scattered-due dispatch ≈ 826 rec/s is below the 10 k CI target** (§3.1). The
   single-thread re-fetch goes random-seek under sparse due-sets (risk #8) and per-transaction batches
   shrink. Re-measure on a **dedicated multi-broker cluster** (warm fetch sessions, real disk IOPS) and
   characterise whether a partition-affine seek-consumer pool (the reserved v1.1 `dispatch.fetch.parallelism`)
   is needed. The 190 k best-case burst is **not** this number.
4. **Throughput windows are coarse** (~0.1–0.2 s) because localhost I/O is fast — re-run with larger
   `-Dperf.*` counts on a server-class host for a smooth, representative per-instance ceiling (the
   risk-#9 ~50–100 k/s re-fetch/commit bound is not stressed by localhost). The six-figure burst
   figures are order-of-magnitude, not point SLAs (§3.2).
5. **Full soak** (100 M / 12 GB / ZGC / 24 h, GC pause p99 < 5 ms) has only been run **scaled**;
   the dedicated lane is documented above and owed before any 100 M-scale claim.
