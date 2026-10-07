# JMH hot-path benchmark results (design §11.4)

Measured numbers for the `KafkaTrackerStore` hot paths. Per design §15 risk #9, the §11.4 targets
are projections until measured; this file records the measurement. Analysis of the misses and how
these numbers fit with the macro results is in [`docs/performance.md`](../../docs/performance.md).

## Run metadata

| | |
|---|---|
| Date | 2026-06-06 |
| Hardware | Apple M3, 8 cores, 24 GB (Apple Silicon, unified LPDDR5) |
| JDK | Corretto 21.0.3 (OpenJDK 64-Bit Server VM, 21.0.3+9-LTS) |
| JMH | 1.37; 2 forks × (3 warmup + 5 measurement) iterations, single thread, `-Xmx2g` |
| Source | `./gradlew :cesium-kafka-benchmarks:jmh` → `build/results/jmh/results.json` (archived as `results/baseline.json`) |

> **Hardware caveat.** The design §11.4 targets assume server-class x86. These numbers are from an
> Apple Silicon dev box. The two metrics that miss (below) are memory-latency-bound random access
> into multi-MB structures, the class of number that diverges most between Apple unified memory and
> a server x86 with a large L3. A server-x86 re-measurement (the hardware the nightly macro gate
> targets) is still pending and is expected to differ. The misses are recorded at their dev-box
> values.

## Results vs §11.4 targets

| Benchmark | Target (single thread) | Measured (Apple M3) | Verdict |
|---|---|---|---|
| index insert (applyAdd, 1 M-entry heap) | ≥ 15 M ops/s | 30.1 M ops/s (33.2 ns/op) | ✅ 2.0× |
| drainDue (heap held ≥ 1 M) | ≥ 8 M ops/s | 2.45 M ops/s (408.9 ns/op) | ❌ 0.31× |
| replay apply (add+complete mix) | ≥ 5 M records/s | 25.9 M rec/s (38.6 ns/op) | ✅ 5.2× |
| ring binary-search complete | ≥ 10 M ops/s | 6.30 M ops/s (158.8 ns/op) | ❌ 0.63× |
| sidecar encode (300 entries) | ≥ 100 k ops/s | 626 k ops/s | ✅ 6.3× |
| sidecar decode (300 entries) | ≥ 100 k ops/s | 429 k ops/s | ✅ 4.3× |
| sidecar roundtrip (300 entries) | none (informational) | 260 k ops/s | n/a |

`ops/s` for the SingleShotTime benchmarks (insert/drain/replay) is `1e9 / (ns per op)`; the raw
metric JMH reports is the per-op time shown in parentheses.

Two companion §11.4 gates live in the store-kafka test suite and are not duplicated here:

| Gate | Target | Measured | Where |
|---|---|---|---|
| JOL footprint | ≤ 40 B/entry (post-approval rev: ≤ 56) | 40.54 B/entry @ 1 M, 45.96 B/entry @ 10 M | `ShardFootprintTest` (10 M = `@Tag("soak")`) |
| JFR / steady-state allocation | ~0 B/op insert/drain | < 2 B/op | `SteadyStateAllocationTest` |

## The two misses

Both misses are memory-latency-bound random access into multi-MB fastutil big-list backing, and
both are re-baselined in §11.4 to the measured dev-box floor; the original projections stay in the
design table. In short:

* **drainDue: 2.45 M ops/s (target 8 M).** Every pop pays a full ~`log₂(n)` sift-down through cold
  cache lines in the `int[]` heap and the `long[]` `dispatchAtMs` pool, while a random push settles
  in O(1) expected swaps, hence 409 ns vs 33 ns for insert (~12× apart).
  The run holds the heap between 2 M and 1 M entries (build 2 M, drain 1 M), so the figure is
  conservative.
* **ring binary-search complete: 6.30 M ops/s (target 10 M).** ~`log₂(1 M) = 20` random
  `IntBigArrayBigList` + `LongBigArrayBigList` probes ≈ 159 ns. This is the cost of the D6 trade
  (`O(log n)` read latency for ~24 B/entry less hash-map memory on a 10 M-entry replay).

Full analysis is in [`docs/performance.md`](../../docs/performance.md) §2. Neither miss is fixed by
tuning the code to the benchmark (design §15); both are flagged for a server-x86 re-measurement.

### Replay-apply scope caveat

`ReplayApplyBenchmark` measures the raw apply path: it never calls `maintenance()` (the §5.3 heap
rebuild / arrival-log sweep). Production recovery replay interleaves periodic `maintenance()` to bound
memory and pays an amortized O(n) rebuild/sweep that this benchmark omits, so 25.9 M rec/s slightly
overstates replay-with-maintenance throughput. The error also runs the other way: the lazy-deleted
heap copies and completed-held log slots grow unbounded here, so late pushes sift through an
ever-larger backing list, a cost a maintained shard would not pay. The verdict holds either way:
replay clears its 5 M target by ~5.2×, and would still clear with maintenance folded in.
`HeterogeneousReplayPerfIT` (macro suite) exercises the maintained-replay cost end to end; it still
needs a server-class re-measure.

## How to run

```bash
./gradlew :cesium-kafka-benchmarks:jmh            # full suite, ~80 s on the M3 box

# A subset — either set `jmh { includes = listOf(".*DrainDueBenchmark.*") }` in build.gradle.kts,
# or build the self-contained JMH jar once and use the standard JMH CLI (include regex is positional):
./gradlew :cesium-kafka-benchmarks:jmhJar
java -jar cesium-kafka-benchmarks/build/libs/cesium-kafka-benchmarks-*-jmh.jar DrainDueBenchmark -f 1
```

The JMH task is not wired into `build`/`check`; benchmarks run manually or nightly.

## Regression gate (aspirational, non-blocking in v1)

`nightly.yml`'s `benchmarks` job runs `:cesium-kafka-benchmarks:jmh` (`continue-on-error: true`) and
archives `results.json`. `results/compare.py` diffs a run against the committed
[`baseline.json`](baseline.json) and flags any benchmark past ±10 % (throughput drop / latency rise):

```bash
python3 results/compare.py results/baseline.json build/results/jmh/results.json
```

It is report-only in v1 (`--strict` flips it to a hard gate). A JMH number is too noisy on
shared CI hardware to block a release on (design §15 risk #9 / #20); the gate becomes hard once a
dedicated low-variance runner exists. To promote a new baseline, commit a fresh `results.json` as
`results/baseline.json` in the PR that justifies the change.
