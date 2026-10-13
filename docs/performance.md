# Performance: measured numbers, replay cost, sizing and tuning

This page records measured results against design `§11.4` (the performance target table), `§5.4`
(memory math) and `§3.5` (the committed-cursor replay model). Where a measurement misses its design
target, the table shows the measured figure next to the target and the text gives the cause. Design
`§15` risk #9 says the targets are projections until measured.

There are two classes of number, kept separate:

* Apple-Silicon dev-box measurements: what was observed on the development machine. These are
  reproducible and labelled as dev-box numbers.
* Server-class projections and dedicated-lane targets: the hardware the design `§11.4` targets
  assume (server x86, large L3, multi-broker cluster). Where a dev-box number diverges from the
  target, it needs a server-class re-measurement, listed in §9.

---

## 1. Test environment

| | Micro (JMH) | Macro / soak (end-to-end) |
|---|---|---|
| Module | `cesium-kafka-benchmarks` (JMH 1.37) | `cesium-kafka-it` (`*PerfIT`, JUnit 5) |
| Hardware | Apple M3, 8 cores, 24 GB (unified LPDDR5) | same dev box |
| JDK | Corretto 21.0.3 (OpenJDK 64-Bit Server VM) | Temurin/Corretto 21 |
| Broker | none (pure structure benchmarks) | Testcontainers `apache/kafka:4.3.0`, single broker, KRaft |
| Engine | n/a | in-JVM `EngineHarness` (ingest + dispatch + re-fetch) |
| Worker heap | `-Xmx2g` | 1 GiB test worker |
| Counts | 1–2 M-entry structures | CI-sized (50 k–200 k); dedicated lane scales via `-Dperf.*` / `-Dsoak.*` |
| Date | 2026-06-06 | 2026-06-06 |

> **Hardware caveat.** The design `§11.4` targets assume server-class x86 with a large L3 and a
> multi-broker cluster. The numbers below are from an Apple-Silicon dev box with a single localhost
> broker. The two metric classes that miss are (a) memory-latency-bound random access into multi-MB
> structures and (b) `read_committed` end-to-end delay-path latency. These are the classes that
> diverge most between this box and the target hardware. The nightly macro gate targets the
> server-x86, multi-broker setup, and that re-measurement is still pending. The misses below are
> reported at their dev-box values. §3 has since traced the delay-path miss to a Kafka client
> setting.

---

## 2. JMH hot-path micro-benchmarks (design `§11.4`)

Source: `./gradlew :cesium-kafka-benchmarks:jmh` → `build/results/jmh/results.json`, archived as
`cesium-kafka-benchmarks/results/baseline.json`; full write-up in
[`cesium-kafka-benchmarks/results/RESULTS.md`](../cesium-kafka-benchmarks/results/RESULTS.md).
2 forks × (3 warmup + 5 measurement) iterations, single thread.

| Benchmark | Target (single thread) | Measured (Apple M3) | Verdict |
|---|---|---|---|
| index insert (`applyAdd`, 1 M-entry heap) | ≥ 15 M ops/s | 30.1 M ops/s (33.2 ns/op) | ✅ 2.0× |
| drainDue (heap held ≥ 1 M) | ≥ 8 M ops/s *(projection)* → re-baselined ≥ 2.4 M | 2.45 M ops/s (408.9 ns/op) | ❌ 0.31× of projection |
| replay apply (add+complete mix) | ≥ 5 M rec/s | 25.9 M rec/s (38.6 ns/op) | ✅ 5.2× |
| ring binary-search complete | ≥ 10 M ops/s *(projection)* → re-baselined ≥ 6 M | 6.30 M ops/s (158.8 ns/op) | ❌ 0.63× of projection |
| sidecar encode (300 entries) | ≥ 100 k ops/s | 626 k ops/s | ✅ 6.3× |
| sidecar decode (300 entries) | ≥ 100 k ops/s | 429 k ops/s | ✅ 4.3× |
| sidecar roundtrip (300 entries) | none (informational) | 260 k ops/s | n/a |
| JOL footprint | ≤ 40 B/entry *(rev 1 gate: ≤ 56 B)* | 40.54 B/entry @ 1 M, 45.96 B/entry @ 10 M (`ShardFootprintTest`; 10 M = `@Tag("soak")` variant) | ✅ |
| Steady-state allocation (`ThreadMXBean`) | ~0 B/op insert/drain | < 2 B/op (`SteadyStateAllocationTest`) | ✅ |

`ops/s` for the `SingleShotTime` benchmarks (insert/drain/replay) is `1e9 / (ns per op)`; the raw
JMH metric is the per-op time in parentheses.

### The two misses

Both misses are memory-latency-bound random access into multi-MB fastutil big-list backing. Both
targets are re-baselined to the measured dev-box floor, and the table keeps the original projection
next to the new floor.

* **drainDue: 2.45 M ops/s (target 8 M).** A heap pop promotes the last leaf to the root and sifts
  it back down a guaranteed ~`log₂(n)` levels, touching a cold cache line in the `int[]` heap and
  the `long[]` `dispatchAtMs` pool at almost every level. That is why insert (33 ns) and drain
  (409 ns) are ~12× apart even though both are nominally "O(log n)": a random push settles in O(1)
  expected swaps, while a pop pays the full sift-down every time. The 8 M projection (only ~2× below
  the 15 M insert target) under-modelled this asymmetry. The figure is also conservative: the heap
  is held between 2 M and 1 M entries for the whole measured drain, so the backing arrays never
  shrink into cache.
* **ring binary-search complete: 6.30 M ops/s (target 10 M).** The completion lookup is a
  hash-map-free binary search over the double-sorted arrival log (design `§5.2` / decision D6). For a
  1 M-entry log that is ~`log₂(1 M) = 20` probes, each a random `IntBigArrayBigList` +
  `LongBigArrayBigList` dereference ≈ 20 cold cache lines ≈ 159 ns. D6 trades `O(log n)` read
  latency for ~24 B/entry less transient hash-map memory on a 10 M-entry replay, and 159 ns is the
  cost of that trade.

Neither miss is fixed by tuning the code to the benchmark (design `§15`). Both need a server-x86
re-measurement, because random access into multi-MB structures is where Apple unified LPDDR5 and a
server x86 with a large L3 diverge most.

### Regression gate (aspirational, non-blocking)

`nightly.yml`'s `benchmarks` job runs the JMH suite (`continue-on-error: true`), archives
`results.json`, and runs `results/compare.py` against the committed `baseline.json` in report-only
mode (flag any benchmark past ±10 %). A JMH number is too noisy on shared CI hardware to block a
release on (design `§15` risks #9/#20); the gate hardens to `--strict` once a dedicated low-variance
runner exists. To promote a baseline, commit a fresh `results.json` as `results/baseline.json` in
the PR that justifies the change.

---

## 3. Macro (end-to-end) benchmarks (design `§11.4`)

Measured by the `*PerfIT` suite in `cesium-kafka-it` (package `com.jucius.cesium.kafka.it`), all
`@Tag("nightly")`/`@Tag("soak")`, so they never run on the default/PR lane. Numbers are read off
the engine's own `§9` meters and the broker. Counts are CI-sized; the dedicated
100 M / 12 GB / ZGC / 24 h soak is a manual lane (§6 below).

| Macro metric | Target (CI / dedicated) | Measured (M3 dev box) | Test | Verdict |
|---|---|---|---|---|
| ingest sustained | ≥ 20 k / ≥ 100 k rec/s | ~180 k–360 k rec/s (50 k records; warm, coarse <1 s window) | `IngestThroughputPerfIT` | ✅ clears dedicated *(order-of-magnitude; coarse window)* |
| dispatch, best-case burst (incl. re-fetch) | ≥ 10 k / ≥ 50 k rec/s | ~190 k rec/s burst (whole backlog due at one instant, contiguous offsets ⇒ 1 seek + sequential scan/partition; coarse ~0.2 s window) | `DispatchThroughputPerfIT` | ⚠️ index/txn path is not the bottleneck; this is a seek-friendly burst, not a sustained rate (see §3.1) |
| dispatch, scattered-due sustained | (same target) | ≈ 826 rec/s under scattered due times (from `SoakPerfIT`: 200 k entries, maxLate ~122 s over a 120 s due-window ⇒ ~242 s drain) | `SoakPerfIT` | ❌ below the 10 k CI target; fetch/seek-bound (risk #8/#9); needs a server-class multi-broker re-measure |
| `dispatch_lag` p99 (idle arrivals) | ≤ 250 ms | p50 2 ms, p99 ~480 ms (477–484), max ~505 ms (n=300) | `DispatchThroughputPerfIT` | ❌ miss, see below |
| 100 k simultaneous-due burst | drains within `burst/throughput` ±20%, no rebalance | drains in 0.61 s (~164 k rec/s); Δassigned/revoked/lost = 0, pollGap 0 s | `BurstPerfIT` | ✅ drains + exactly-once; membership stability is only a sanity check (see §3.3) |
| heterogeneous-delay replay | replay ≈ traffic-since-commit ±30% (≪ history) | replay 1501 ≈ Δ 1500 vs 17 513 full ADD+tombstone history (8.6 %) | `HeterogeneousReplayPerfIT` | ✅ cursor-v2 scaling confirmed |
| large-payload (1 MB) fetch budget | truncate-and-carry-over, not materialize-all (R8) | 32 × 1 MiB drained in 8 dispatch txns (= ⌈32 MiB / 4 MiB⌉), not 1 | `LargePayloadPerfIT` | ✅ budget enforced |
| soak invariants (scaled) | exactly-once + never-early | 200 k entries: 200 k distinct keys, 0 never-early violations (150 ms skew tol) | `SoakPerfIT` | ✅ harness + checker work |

### 3.1 Dispatch throughput: best-case burst vs scattered sustained

The two dispatch rows in the table above measure the same dispatch+re-fetch subsystem on two very
different workloads, and they differ by ~230×.

* **Best-case burst ≈ 190 k rec/s** (`DispatchThroughputPerfIT`). This is the most seek-friendly,
  commit-amortized layout possible: the whole backlog comes due at one instant, laid out as
  contiguous per-partition source offsets, and held fully index-resident (the §5.3 high-water and
  store cap are raised so nothing is paused). The `KafkaSeekFetcher` does one `seek` + a sequential
  forward scan per partition (the §7 / risk #8 seek-friendly extreme), and the drain commits in large
  batches. The ~190 k rec/s is read off `cesium_dispatch_records_total` over a coarse ~0.2 s window (a
  handful of batch commits; see §3.2). It is not a deployment SLA. It shows that the index and
  transaction path are not the bottleneck: the heap pop, ring search, and EOS commit keep up with
  six-figure bursts on this box.

* **Scattered-due sustained ≈ 826 rec/s** (`SoakPerfIT`). Under realistic heterogeneous due times,
  the due-set at any instant is sparse across partitions, so the re-fetch degrades toward random
  seeks (risk #8, §7), per-transaction batches shrink, and the single dispatch thread becomes
  fetch/seek-bound. The soak drains 200 k entries with maxLate ~122 s over a 120 s due-window, i.e.
  ~242 s to drain ⇒ ≈ 826 rec/s. That is below the ≥ 10 k CI dispatch target on this single-broker
  box. It is a throughput ceiling, with no exactly-once or never-early failure (earliness = 0; this
  is bounded lateness, see §6). Size deployments against this number, not 190 k.

So the index/transaction path is not the bottleneck (burst 190 k), while sustained scattered
dispatch is fetch/seek-bound (~826 rec/s here) and needs a server-class, multi-broker
re-measurement. The relevant risks are risk #8 (seek-fetch I/O amplification) and risk #9 (the
projected ~50–100 k/s per-instance ceiling is a re-fetch/commit bound that a single localhost broker
both under- and over-states depending on the layout). Ingest (~180–360 k rec/s) is
producer/commit-bound and clears its target with headroom, subject to the same coarse-window caveat.

### 3.2 Why the window is coarse (a measurement-precision caveat)

With the engine in-JVM and a localhost broker, the CI-sized backlog drains in ~0.1–0.2 s. The
throughput sampler (`PerfSupport.measureSustainedThroughput`) snapshots the meter at the 15 % warm
crossing and at 100 %. In a window that short only a few awaitility polls separate the two
snapshots and the warm snapshot overshoots its target, so the rate carries a large relative error
(~2×). Treat the six-figure throughput figures as order-of-magnitude headroom rather than point
figures. `ThroughputResult` flags any window `< 1 s` as `COARSE WINDOW` in the test log. A
server-class run with larger `-Dperf.*` counts gives a longer, smoother window and the realistic
per-instance ceiling; that is the dedicated lane's job (§9).

### The `dispatch_lag` p99 miss (~480 ms vs the 250 ms SLO)

p50 is 2 ms: an already-pending entry dispatches almost immediately. The miss is in the tail, and
the ingest→pending pipeline does not cause it. The test (`dispatchLagP99ForIdleArrivals`)
pre-schedules every entry at `now + 2000 ms`, so ingest commit, tracker consume and
`read_committed` LSO visibility (in §3.4, records with only a 50 ms lead were visible 5–18 ms late
at p50) are absorbed by the 2 s lead and add no lateness. A systematic floor would also lift p50;
here p50 = 2 ms while p99 = 472 ms (reproduced: p99 = 472, max = 496; on 2026-10-12, p99 = 474,
max = 498).

The tail is the payload fetch. After a due batch's fetch, the seek consumer leaves a fetch in flight
at the end of the source partition, and the broker holds it until new data arrives or
`fetch.max.wait.ms` (Kafka default 500 ms) passes. While the source still receives writes, that
takes milliseconds. Once it goes quiet, as it does at the end of the test's trickle, the next due
batch waits up to 500 ms before its own fetch can go out. Two diagnostic runs with
`fetch.max.wait.ms` at 50 ms on the seek consumer (`kafka.seek-consumer.properties`) brought p99
down to 53–55 ms (max 62–63 ms), inside the SLO. The default is unchanged, so the SLO carries this
annotation:

> **`dispatch_lag` p99 ≤ 250 ms**: *idle-arrival; p50 = 2 ms; ~480 ms p99 at the default
> seek-consumer `fetch.max.wait.ms` (500 ms), from batches that come due after the source partition
> goes quiet; ~55 ms p99 with it at 50 ms.*

`cesium_dispatch_lag_seconds`, and the relay timestamp this test reads, are taken when a dispatch
batch starts, before the payload fetch and the transaction commit. They leave out that batch's fetch
and commit and the destination's `read_committed` visibility; a batch that waited behind a slow
fetch still shows the wait. §3.4 measures lateness where a consumer sees it.

### 3.3 The burst's membership-stability assertion is a sanity check, not an R2 proof

`BurstPerfIT` stages 100 k entries to come due at one instant and asserts that group B takes no
`revoked`/`lost`/extra-`assigned` rebalance step and that `cesium_dispatch_poll_gap_seconds` stays low
during the drain. At this scale those assertions cannot fail, so they do not distinguish an
R2-correct dispatcher from a broken one. The burst drains in ≈ 0.6 s, and the dispatch consumer's
`max.poll.interval.ms` is the kafka-clients default 300 s: even a single fully-blocking drain with
zero interleaved `poll()`s would finish ~500× inside that window (the measured `pollGap` ≈ 0 s confirms
it). Lowering `max.poll.interval.ms` would not help. For a non-interleaving drain to trip it, the
interval would have to drop below the sub-second drain time, which is not viable for a real
consumer. The burst's membership check stays only as a cheap guard against a catastrophic
rebalance storm; what the test verifies is the drain rate and exactly-once. The R2
poll-gap property (*the time-sliced drain must interleave real `poll()`s so a large backlog never
trips `max.poll.interval.ms`*) is tested where the drain is forced to outlast the interval:
`BarrierOrderingI8IT` (evicts an intentionally stalled member at `max.poll.interval.ms = 7 s` with a
matching `drain.max-slice`) and `RebalanceScaleIT` (assignment churn). The §11.1 unit suite also has a
fake-clock due-storm poll-gap test.

### 3.4 Sub-second delays: best-effort, never early

`SubSecondDelayIT` measures what a downstream consumer sees. Each round produces one record each
with `cesium-delay-ms` 0, 50, 300 and 900, and rounds are at least 15 ms apart. A `read_committed`
consumer stamps the moment it first sees each relay. Lateness is that moment minus the requested
time (source record timestamp + delay), so for a scheduled record it includes ingest, tracker
consume, the payload fetch, both transaction commits and the consumer's own fetch. The test asserts
exactly-once and that no record is visible before its requested time. It asserts no latency bound
and is untagged, so it runs on the PR lane. To run it alone, use
`./gradlew :cesium-kafka-it:integrationTest --tests "com.jucius.cesium.kafka.it.SubSecondDelayIT"`
(`-Dperf.subsecond.rounds` changes the 200 measured rounds).

Four runs on 2026-10-12, on the Apple M3 dev box (8 cores, 24 GB) with Corretto 21.0.3 and one
`apache/kafka:4.3.0` broker under Testcontainers, 200 measured records per delay. Three runs landed
at the low end of each range; the fourth gave the high p50s and maxima.

| Delay | p50 | p99 | max | relayed by ingest |
|---|---|---|---|---|
| 0 ms | 9–19 ms | 19–217 ms | 90–280 ms | 200 of 200 |
| 50 ms | 5–18 ms | 16–158 ms | 36–215 ms | 1–30 of 200 |
| 300 ms | 5–16 ms | 18–74 ms | 19–143 ms | 0 |
| 900 ms | 5–15 ms | 21–80 ms | 24–137 ms | 0 |

No record was early. A record already due when ingest reads it, which is every 0 ms record and a
few 50 ms records that ingest read late, is relayed straight from ingest; at any delay, nothing is
relayed before its requested time. Sub-second delays are best-effort, and cesium makes no latency
promise for them.

The percentiles leave out two stretches that the test still checks:

* **Warm-up**: the first 25 rounds, about the first second after the engine starts. Worst
  lateness 0.53–0.79 s.
* **Drain**: the rounds after the measured ones, which keep the source busy until the last measured
  record is due. Their last records come due after the source goes quiet, so each due batch waits
  for the seek consumer's in-flight fetch (the `dispatch_lag` p99 section above). Worst lateness
  1.00–1.01 s. One diagnostic run with the seek consumer's `fetch.max.wait.ms` at 50 ms cut it to
  0.11 s.

---

## 4. Replay cost formula (cursor v2)

The committed cursor (design `§3.5`, decision D11) is a position watermark plus a pinned-entry
sidecar. It decouples replay cost from how long any single entry has been pending. There are two
regimes:

**Normal (non-overflow), the common case.** All still-pending entries below the dense region fit in
the sidecar (`dispatch.cursor.sidecar-max-bytes`, default 3 KiB ≈ 200–300 entries, capped by broker
`offset.metadata.max.bytes`). On recovery the engine seeds the index from the sidecar, then
consumes `[cursorOffset, barrier)`:

```
replay_records ≈ sidecar_decode + traffic_since_last_successful_commit (+ downtime traffic)
```

This is independent of pin age. `HeterogeneousReplayPerfIT` replays 1501 records ≈ the 1500
committed since the last commit, against a 17 513-record full ADD+tombstone history, which is
8.6 % of the naive cost.

**Overflow (fallback).** If a route's *steady* state pins more than ~N_max long-delay entries per
partition (hundreds+), the sidecar overflows and the cursor falls back to the min-pending
watermark at the `(N_max+1)`-th oldest pending entry. Replay then re-reads completions since that
cut:

```
replay_records ≈ completion_rate(p) × age(cut) + pending(p)
```

This reverts to the classic "completion throughput × pin age" cost for the overflow tail (design
`§15` risk #5). It shows up in metrics before it hurts: `cesium_pinned_entries{partition}` sustained
at max ⇒ overflow mode; `cesium_replay_remaining_records{partition}` (= barrier − position) drives a
replay-ETA alert. To tune it, raise `dispatch.cursor.sidecar-max-bytes` together with broker
`offset.metadata.max.bytes` (a budget above the broker cap is clamped to it at startup). Reading the
pending ADDs themselves is irreducible for a log-backed store. Replay throughput is fetch-bound at
~1–3 M records/s per partition (design `§5.5`).

---

## 5. Memory worksheet (design `§5.4`)

### Per-entry footprint: three numbers

| Number | Value | Meaning |
|---|---|---|
| Nominal | ~32 B/entry | irreducible primitive floor: `dispatchAtMs` 8 + `sourceOffset` 8 + `trackerAddOffset` 8 + heap slot 4 + log slot 4 + bitmap 0.125 |
| Measured (JOL) | 40.54 B/entry @ 1 M, 45.96 B/entry @ 10 M | `ShardFootprintTest` (10 M = `@Tag("soak")`): retained bytes at that fill |
| Planning budget | 64 B typical, 80 B worst | post-approval rev 1: conservative constant for capacity planning |
| JOL gate | ≤ 56 B/entry | post-approval rev 1 CI gate (was ≤ 40 pre-fastutil) |

JOL measures 40.5 B/entry, but capacity planning uses 64 B. The index is backed by fastutil
big-lists (post-approval rev 1), which grow by power-of-two doubling. JOL counts the bytes retained
at one particular fill (40.54 B @ exactly 1 M). The 64 B planning constant must hold at any fill, including the instant just after a doubling, when the
backing arrays are sized to the next power of two over a smaller live count (up to ~2× slack), plus
the bounded heap/arrival-log maintenance garbage (worst transient ≈ +28 B per completed-but-held
slot, capped at 50 % of the log; design `§5.3`). **Plan with 64/80; expect ~40 in practice.** (The
pre-fastutil chunked-array design budgeted 48/64; rev 1 superseded that. See the design revisions
note and `§5.4`.)

### Sizing table

| Pending entries | Index nominal (~32 B) | Planning budget (64 B) | Recommended heap (incl. client buffers + fetch budget) | GC |
|---|---|---|---|---|
| 1 M | 32 MB | 64 MB | 512 MB – 1 GB | G1 (default) |
| 10 M | 321 MB | 640 MB | 2 – 3 GB | G1 |
| 100 M | 3.2 GB | 6.4 GB | 8 – 12 GB | ZGC generational |

**Non-index heap consumers** (budgeted; `§7` enforces the budget):

* producer `buffer.memory` 64 MB × 2;
* ingest-consumer fetch buffers (`max.partition.fetch.bytes` 4 MB × partitions, capped by
  `fetch.max.bytes` 50 MB);
* seek-fetch path ≤ `dispatch.batch.max-bytes` (32 MiB decompressed, enforced with
  truncate-and-carry-over) + one in-flight fetch response per broker × decompression factor.
  Worksheet formula: `brokers_in_flight × fetch.max.bytes × decompression_factor`, tuned down when
  payloads compress heavily;
* tracker consumer: negligible.

Add +30 % G1 / +20 % ZGC headroom; container memory = heap + ~1 GB native.

**Backpressure and caps (design `§5.3`).** `dispatch.max-pending-per-partition` pauses the tracker
consumer above high-water (ACTIVE shards only; a RECOVERING shard is never paused). The process-wide cap
`dispatch.max-pending-total` (default ≈ 25 % of `Xmx` ÷ 64 B) pauses ACTIVE shards above the
total, and the store's recovery ceiling bounds RECOVERING shards at the same budget. Startup prints
the effective cap. Pause state: `cesium_shard_paused{partition}`.

---

## 6. GC guidance

The index is long-lived, primitive, chunked, and allocation-free in steady state (< 2 B/op in
insert/drain, `SteadyStateAllocationTest`). There are no per-entry objects for the GC to trace, unlike the PoC's
`DelayQueue<DelayEntry>`.

| Scale | GC | Flags |
|---|---|---|
| ≤ ~10 M entries / ≤ 4 GB heap | G1 (default) | `-Xms=-Xmx`; `-XX:+AlwaysPreTouch` for latency-critical installs |
| ≥ 8 GB / 100 M-entry scale | ZGC generational | `-XX:+UseZGC -XX:+ZGenerational` (JDK 21; default-generational on 23+) |

With ZGC generational, dispatch-accuracy p99 becomes independent of heap size, which is why it is
the 100 M-entry recommendation. Keep JFR continuous recording on (`-XX:StartFlightRecording=...`)
so footprint and pause regressions are caught from production flight data. The chunked/fastutil
backing avoids G1 humongous allocations (a flat 100 M-entry `long[]` would be 800 MB); growth is
O(1) and copy-free.

*Documented future compression (not implemented):* uint32 seconds-from-base dispatch time (−4 B), delta-encoded
tracker offsets (−4 B) → floor ≈ 24 B/entry.

---

## 7. Seek-fetch I/O amplification (design `§15` risk #8, `§7`)

Payloads are never copied. The scheduler stores `(partition, offset, dispatchAtMs)` pointers and
re-fetches at dispatch time. The cost model and mitigations:

* **Cold segments.** Long delays mean the dispatch-time fetch hits segments that are no longer in
  page cache; for a P1D delay the source segment was evicted long ago. Mitigation: one `seek` + a
  sequential forward scan serves all of a partition's due entries, so the midnight thundering herd
  (10 k due from one partition) is one sequential pass instead of 10 k random seeks. Budget broker
  disk IOPS accordingly (ops guide).
* **Sparse due-sets across many partitions** degrade toward random reads. Mitigation: warm fetch
  sessions (assign the union of recently-seen partitions rather than churning assignment).
* **Tiered/remote storage** fetches are slow by construction. Mitigation: the byte-budget
  truncate-and-carry-over (below) bounds heap regardless of how slow the fetch is.
* **Degraded partitions / brokers.** A failing fetch puts the source partition in the penalty box
  (`dispatch.fetch.penalty.backoff`, exponential `PT0.05S → PT10S`); `drainDue` skips penalized
  partitions even when due, so there is no hot-spin and no head-of-line blocking of healthy
  partitions.

The fetch budget is enforced where record sizes become known (`dispatch.batch.max-bytes`, default
32 MiB decompressed): a 10 k-entry candidate batch of 1 MB records becomes ~32 transactions of
~32 MiB instead of 10 GB of heap. In `LargePayloadPerfIT`, 32 × 1 MiB drained in 8 dispatch
transactions at a 4 MiB budget (= ⌈32 MiB / 4 MiB⌉) rather than 1, so truncate-and-carry-over works
and R8 holds.

**Observability:** `cesium_fetch_duration_seconds` (histogram; attributes cold-segment / degraded-
broker cost), `cesium_fetch_bytes_total` (decompressed volume, for the budget),
`cesium_fetch_penalized_partitions` (penalty-box occupancy), `cesium_fetch_attempts_total` /
`_misses_total` / `_unfetchable_total`.

A small partition-affine seek-consumer pool (2–4) is a possible future extension, not implemented
(`dispatch.fetch.parallelism` is not an accepted key today; unknown keys fail startup). The penalty
box is the isolation mechanism.

---

## 8. How to run the lanes

All perf/soak lanes are off the default `./gradlew build` and PR lane, so `build` stays green and
fast. They run manually or in `nightly.yml`.

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

Scale-up knobs for a dedicated host (defaults are CI-sized). Pass them on the Gradle command line;
the build forwards every `-Dperf.*` and `-Dsoak.*` property to the forked test JVM:

```bash
./gradlew :cesium-kafka-it:integrationTest -PincludeNightly=true \
    --tests "com.jucius.cesium.kafka.it.IngestThroughputPerfIT" -Dperf.ingest.total=500000
```


| Test | Knob (default) |
|---|---|
| `IngestThroughputPerfIT` | `-Dperf.ingest.total=50000`, `-Dperf.ingest.floor=4000` (rec/s hard floor) |
| `DispatchThroughputPerfIT` | `-Dperf.dispatch.total=50000`, `-Dperf.dispatch.floor=2000`, `-Dperf.lag.count=300`, `-Dperf.lag.ceilingMs=2000` |
| `BurstPerfIT` | `-Dperf.burst.total=100000` |
| `LargePayloadPerfIT` | `-Dperf.large.count=32` (~32 MiB of payload) |
| `HeterogeneousReplayPerfIT` | `-Dperf.replay.volume=8000`, `-Dperf.replay.delta=1500` |

> The `*.floor` values are conservative hard-assertion floors the test fails below (set low so a
> loaded CI box does not flake the gate). The design `§11.4` targets (20 k CI / 100 k dedicated
> ingest, etc.) are the aspirational targets. The §3 measurements clear both.

### Soak: scaled (CI) and dedicated (manual)

```bash
# Scaled-down soak (a few hundred k entries / ~2 min) — proves harness + invariant checker.
# Runs ONLY through the dedicated soakPerf task (@Tag("soak") is excluded everywhere else).
./gradlew :cesium-kafka-it:soakPerf --tests '*SoakPerfIT'

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
./gradlew :cesium-kafka-it:soakPerf --tests '*SoakPerfIT' \
    -Dsoak.maxHeap=12g -Dsoak.minHeap=12g \
    -Dsoak.total=100000000 -Dsoak.windowMs=86400000 -Dsoak.partitions=64 -Dsoak.timeoutMin=1560
```

`soakPerf` scale-up knobs (defaults are the CI-scaled run). Every `-Dsoak.*` property is forwarded to
the forked test JVM and is a configuration-cache input, so changing one is never served from a stale
cache entry. `soak.maxHeap` and `soak.minHeap` also size the worker heap:

| Knob (default) | Effect |
|---|---|
| `-Dsoak.total=200000` | entries driven through the engine |
| `-Dsoak.windowMs=120000` | spread of due times (heterogeneous stream) |
| `-Dsoak.partitions=4` | source/destination partitions |
| `-Dsoak.timeoutMin=15` | drain timeout (raise for the 24 h lane) |
| `-Dsoak.maxHeap=1536m` | worker `-Xmx` (**set this, not `JAVA_TOOL_OPTIONS=-Xmx…`**; see the command note above) |
| `-Dsoak.minHeap` *(unset)* | worker `-Xms`; set `= soak.maxHeap` for an `-Xms=-Xmx` pre-touched heap |

**Soak invariants** (checked on every delivered record): exactly-once (each delayed input appears
on the destination once, read_committed, indexed by key) and never-early (dispatch instant never
before requested time minus a 150 ms skew tolerance). Scaled run, 200 k entries: 200 k distinct
keys, 0 never-early violations.

> **Note on `maxLate`.** Soak `maxLate` reflects the sustained scattered-dispatch rate from §3.1. It grows
> with scale (≈ 2 s at 20 k, ≈ 122 s at 200 k over a 120 s window) because a single dispatch thread
> with re-fetch is the throughput limiter at scale. The 122 s figure corresponds to the ≈ 826 rec/s
> sustained scattered-dispatch ceiling in §3.1 (~242 s to drain 200 k). That is below the 10 k CI
> dispatch target on this single-broker box, a fetch/seek bound (risk #8/#9) that needs a
> server-class re-measure. It is lateness, which the "at-or-after requested time" contract permits;
> there is no earliness and no exactly-once violation (earliness = 0). The dedicated 100 M / 24 h
> lane is where steady lateness is characterised against the full GC-pause-p99 < 5 ms target.

### Crash-loop soak (design §11.3-12)

```bash
# Three app JVMs under delayed load, SIGKILLed and crash-point-halted; ~7.5 min at the defaults.
./gradlew :cesium-kafka-it:soakPerf --tests '*CrashLoopSoakIT'
```

`CrashLoopSoakIT` forks three engine processes (`CrashLoopChild`: the production `CesiumApp.main`
with an optional crash point armed) on the `integrationTest` runtime classpath, which `soakPerf`
passes in as `soak.childClasspath`. Every 5–20 s it SIGKILLs one and restarts it after 0–10 s;
half the restarts arm a random §3.9 crash point that halts the child (exit 86). All three boot
together on a fresh deployment under live load, and the kill loop starts without waiting for
dispatch to commit. When the load and the kills stop, it drains the destination, checks the same
two invariants as `SoakPerfIT`, and checks that the DLQ is empty. If a child the test did not
kill exits with anything but its armed crash point's code, the run fails and names that child's
log under `cesium-kafka-it/build/reports/tests/soak-children/`.

| Knob (default) | Effect | Measured at the defaults |
|---|---|---|
| `-Dsoak.crashloop.rate=200` | records produced per second, each due 2–60 s later | 72.1 k–74.7 k records |
| `-Dsoak.crashloop.minutes=6` | how long the kill loop runs under load | 20–25 SIGKILLs, 24–32 restarts |
| `-Dsoak.crashloop.crashProb=0.02` | chance an armed crash point halts the child each time it fires | 4–9 halts from 8–15 armed restarts |
| `-Dsoak.crashloop.drainMin=5` | bound on the final drain once the load and kills stop | drained in 66–67 s |

Five default runs on the §1 dev box, 2026-10-12, all green:

| Run | Records | SIGKILLs | Crash-point halts | Restarts (armed) | Took | `maxLate` |
|---|---|---|---|---|---|---|
| 1 | 74,660 | 25 | 7 | 32 (15) | 439 s | 94 s |
| 2 | 73,800 | 20 | 4 | 24 (8) | 435 s | 132 s |
| 3 | 72,820 | 22 | 7 | 29 (11) | 430 s | 77 s |
| 4 | 72,100 | 23 | 5 | 28 (9) | 427 s | 72 s |
| 5 | 74,460 | 22 | 9 | 31 (13) | 439 s | 78 s |

In run 1 a rebalance moved two tracker partitions before their first cursor commit, and their new
owner replayed them from the beginning
([ADR-0022](adr/0022-tracker-partition-without-cursor-replays-from-beginning.md)). `maxLate` is
lateness, which the at-or-after-requested-time contract permits.

`nightly.yml` runs the `benchmarks` (JMH), `macro-perf`, and `soak` (incl. both `soakPerf` ITs, as
separate invocations) jobs; the JMH job is `continue-on-error` (non-gating).

---

## 9. Open item: server-class re-measurement

Open items, tracked against design `§15` risk #9:

1. `drainDue` and ring binary-search complete miss their JMH projections on Apple Silicon
   (2.45 M vs 8 M; 6.30 M vs 10 M). Both are memory-latency bound. Re-measure on server x86 with a
   large L3. The re-baselined floors (≥ 2.4 M / ≥ 6 M) are the dev-box values, and the original
   projections are kept for the server run.
2. `dispatch_lag` p99 ~480 ms vs the 250 ms SLO. It comes from the seek consumer's default
   `fetch.max.wait.ms` (500 ms) once a source partition goes quiet (§3). Decide whether cesium should set a lower default; at 50 ms, p99 was ~55 ms.
3. Sustained scattered-due dispatch ≈ 826 rec/s is below the 10 k CI target (§3.1). The
   single-thread re-fetch goes random-seek under sparse due-sets (risk #8) and per-transaction
   batches shrink. Re-measure on a dedicated multi-broker cluster (warm fetch sessions, real disk
   IOPS) and determine whether a partition-affine seek-consumer pool (a possible future
   `dispatch.fetch.parallelism`) is needed. The 190 k best-case burst does not measure this.
4. Throughput windows are coarse (~0.1–0.2 s) because localhost I/O is fast. Re-run with larger
   `-Dperf.*` counts on a server-class host for a smooth, representative per-instance ceiling
   (localhost does not stress the risk-#9 ~50–100 k/s re-fetch/commit bound). The six-figure burst
   figures are order-of-magnitude, not point SLAs (§3.2).
5. The full soak (100 M / 12 GB / ZGC / 24 h, GC pause p99 < 5 ms) has only been run scaled. The
   dedicated lane is documented above and must run before any 100 M-scale claim.
