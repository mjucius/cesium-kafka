# JMH hot-path benchmark results (design §11.4)

Honest measured numbers for the `KafkaTrackerStore` hot paths. Per design §15 risk #9, the §11.4
targets are **projections until measured** — this is the measurement.

## Run metadata

| | |
|---|---|
| Date | 2026-06-06 |
| Hardware | **Apple M3, 8 cores, 24 GB** (Apple Silicon, unified LPDDR5) |
| JDK | Corretto **21.0.3** (OpenJDK 64-Bit Server VM, 21.0.3+9-LTS) |
| JMH | 1.37 — 2 forks × (3 warmup + 5 measurement) iterations, **single thread**, `-Xmx2g` |
| Source | `./gradlew :cesium-kafka-benchmarks:jmh` → `build/results/jmh/results.json` (archived as `results/baseline.json`) |

> **Hardware caveat.** The design §11.4 targets assume **server-class x86**. These numbers are from
> an Apple Silicon **dev box**. The two metrics that miss (below) are memory-latency-bound random
> access into multi-MB structures — exactly the class of number that diverges most between Apple
> unified memory and a server x86 with a large L3 — so the server-x86 re-measurement (the hardware
> the nightly macro gate actually targets) is owed and is expected to differ. We do **not** use the
> caveat to wave the misses away: the dev-box reality is recorded as-is.

## Results vs §11.4 targets

| Benchmark | Target (single thread) | Measured (Apple M3) | Verdict |
|---|---|---|---|
| index insert (applyAdd, 1 M-entry heap) | ≥ 15 M ops/s | **30.1 M ops/s** (33.2 ns/op) | ✅ 2.0× |
| drainDue (heap held ≥ 1 M) | ≥ 8 M ops/s | **2.45 M ops/s** (408.9 ns/op) | ❌ 0.31× |
| replay apply (add+complete mix) | ≥ 5 M records/s | **25.9 M rec/s** (38.6 ns/op) | ✅ 5.2× |
| ring binary-search complete | ≥ 10 M ops/s | **6.30 M ops/s** (158.8 ns/op) | ❌ 0.63× |
| sidecar encode (300 entries) | ≥ 100 k ops/s | **626 k ops/s** | ✅ 6.3× |
| sidecar decode (300 entries) | ≥ 100 k ops/s | **429 k ops/s** | ✅ 4.3× |
| sidecar roundtrip (300 entries) | — (informational) | **260 k ops/s** | — |

`ops/s` for the SingleShotTime benchmarks (insert/drain/replay) is `1e9 / (ns per op)`; the raw
metric JMH reports is the per-op time shown in parentheses.

Two companion §11.4 gates live in the **store-kafka** test suite (not re-implemented here, to avoid
duplication):

| Gate | Target | Measured | Where |
|---|---|---|---|
| JOL footprint | ≤ 40 B/entry (post-approval rev: ≤ 56) | **40.54 B/entry** @ 1 M, **45.96 B/entry** @ 10 M | `ShardFootprintTest` (10 M = `@Tag("soak")`) |
| JFR / steady-state allocation | ~0 B/op insert/drain | **< 2 B/op** | `SteadyStateAllocationTest` |

## The two misses — honest analysis

Both misses are **memory-latency-bound random access into multi-MB fastutil big-list backing**, and
both are re-baselined in §11.4 to the measured dev-box floor (the original projection is retained
and flagged — nothing is silently dropped).

* **drainDue — 2.45 M ops/s (target 8 M).** A heap pop promotes the last leaf to the root and sifts
  it back down a **guaranteed** ~`log₂(n)` levels, touching a cold cache line in the `int[]` heap
  and the `long[]` `dispatchAtMs` pool at almost every level. That is why insert (33 ns) and drain
  (409 ns) are ~12× apart even though both are "O(log n)": a random **push** settles in O(1)
  *expected* swaps, but a **pop** pays the full sift-down every time. The 8 M target (only ~2×
  below the 15 M insert target) under-modelled this asymmetry. The measured figure is additionally
  **conservative**: the heap is held between 2 M and 1 M entries for the whole measured drain
  (build 2 M, drain 1 M), so the backing arrays never shrink into cache.

* **ring binary-search complete — 6.30 M ops/s (target 10 M).** The completion lookup is a
  hash-map-free binary search over the double-sorted arrival log (design §5.2 / D6). For a 1 M-entry
  log that is ~`log₂(1 M) = 20` probes, each a random `IntBigArrayBigList` + `LongBigArrayBigList`
  dereference = ~20 cold cache lines per search ≈ 159 ns. This is the deliberate design trade — D6
  spends `O(log n)` read latency to save ~24 B/entry of transient hash-map memory on a 10 M-entry
  replay — so the number is the cost of that trade, not a defect.

Neither miss is fixed by chasing the benchmark (that would be fudging, design §15). They are
re-baselined honestly and flagged for a server-x86 macro re-measurement.

### Replay-apply scope caveat (honest)

`ReplayApplyBenchmark` measures the **raw apply path**: it never calls `maintenance()` (the §5.3 heap
rebuild / arrival-log sweep). Production recovery replay interleaves periodic `maintenance()` to bound
memory, paying an amortized O(n) rebuild/sweep this benchmark omits — so **25.9 M rec/s slightly
overstates replay-with-maintenance throughput**. (It is not purely optimistic: the lazy-deleted heap
copies and completed-held log slots also grow unbounded here, so late pushes sift through an
ever-larger backing list — a cost a maintained shard would not pay.) The verdict is unaffected — replay
clears its 5 M target by ~5.2×, and would still clear with maintenance folded in. The maintained-replay
cost is exercised end-to-end by `HeterogeneousReplayPerfIT` (macro suite) and is owed a server-class
re-measure.

## How to run

```bash
./gradlew :cesium-kafka-benchmarks:jmh            # full suite, ~80 s on the M3 box

# A subset — either set `jmh { includes = listOf(".*DrainDueBenchmark.*") }` in build.gradle.kts,
# or build the self-contained JMH jar once and use the standard JMH CLI (include regex is positional):
./gradlew :cesium-kafka-benchmarks:jmhJar
java -jar cesium-kafka-benchmarks/build/libs/cesium-kafka-benchmarks-*-jmh.jar DrainDueBenchmark -f 1
```

The JMH task is **never** wired into `build`/`check` — benchmarks are manual / nightly only.

## Regression gate (aspirational, non-blocking in v1)

`nightly.yml`'s `benchmarks` job runs `:cesium-kafka-benchmarks:jmh` (`continue-on-error: true`) and
archives `results.json`. `results/compare.py` diffs a run against the committed
[`baseline.json`](baseline.json) and flags any benchmark past ±10 % (throughput drop / latency rise):

```bash
python3 results/compare.py results/baseline.json build/results/jmh/results.json
```

It is **report-only** in v1 (`--strict` flips it to a hard gate). A JMH number is too noisy on
shared CI hardware to block a release on (design §15 risk #9 / #20); the gate becomes hard once a
dedicated low-variance runner exists. Promote a new baseline deliberately by committing a fresh
`results.json` as `results/baseline.json` in the PR that justifies the change.
