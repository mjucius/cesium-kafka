# cesium-kafka-benchmarks

JMH micro-benchmarks for the `KafkaTrackerStore` hot paths (design §11.4). They use no broker and
measure the in-memory index structures (`PartitionShard` / `TrackerIndex`) and the `SidecarCodec`
directly. The broker-backed *macro* perf lane lives in `cesium-kafka-it` (nightly `macro-perf` job).

This module is not wired into `build` / `check`. Run it on demand:

```bash
./gradlew :cesium-kafka-benchmarks:jmh
```

Results land in `build/results/jmh/{results.json,human.txt}`. The committed reference run is in
[`results/`](results/RESULTS.md); measured-vs-target analysis is in
[`docs/performance.md`](../docs/performance.md).

## Benchmarks (design §11.4 table)

| Class | Path measured | Mode |
|---|---|---|
| `IndexInsertBenchmark` | `applyAdd` live-tail append into a 1 M-entry heap | SingleShotTime |
| `DrainDueBenchmark` | `drainDue` heap pop (sift-down), heap held ≥ 1 M | SingleShotTime |
| `ReplayApplyBenchmark` | recovery replay: add+complete record stream from empty | SingleShotTime |
| `RingSearchCompleteBenchmark` | `applyComplete` arrival-log binary search (D6) | Throughput |
| `SidecarCodecBenchmark` | sidecar varint/Base64 encode + decode, ~300 entries | Throughput |

### Methodology notes

* **Destructive / growing paths use `SingleShotTime`.** Insert grows the heap and drain empties it,
  so they cannot be measured one-call-per-JMH-op without either ballooning or depleting into
  no-op-drain noise. SingleShotTime measures one invocation and excludes the heavy
  `@Setup(Level.Invocation)` that rebuilds the ~1 M-entry base shard, so setup cost stays out of the
  per-op time. `@OperationsPerInvocation` normalizes the inner loop to per-op. Insert and drain are
  conservative (heap held at or above 1 M throughout the measured run).
* **Non-destructive paths use `Throughput`.** The ring search completes a guaranteed-miss odd
  offset against an even-keyed log, so it runs the full-depth binary search with zero mutation, and
  one pre-built shard serves the whole run. Sidecar encode/decode are pure functions of a pre-built
  entry set.
* **Randomization is in `@Setup`, never the measured loop.** Dispatch times, probe offsets, and the
  replay record stream are precomputed; the measured loops only read primitive arrays and call the
  index, consuming results through a `Blackhole` (or returning them) to defeat dead-code elimination.

## Targets and regressions

Per design §15 risk #9, the §11.4 targets are projections until measured. Measured results are in
[`results/RESULTS.md`](results/RESULTS.md). Two benchmarks, `drainDue` and ring binary-search
complete, miss their projections and are re-baselined in §11.4; the cause analysis is in
§2 of [`docs/performance.md`](../docs/performance.md). The nightly
`benchmarks` job is non-gating. `results/compare.py` compares a run against `results/baseline.json`
and flags regressions >10 %. It is report-only in v1, because a flaky perf gate is worse than none.

## Build wiring

* Applies `cesium.java-conventions` + `me.champeau.jmh` (version from the `jmh-plugin` catalog entry).
* `NullAway` is disabled for the `jmh` source set only (benchmarks are not production code); the repo
  default `NullAway=ERROR` applies everywhere else.
* The forked benchmark JVM is pinned to the project's JDK 21 toolchain.
