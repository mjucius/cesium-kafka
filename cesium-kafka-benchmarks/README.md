# cesium-kafka-benchmarks

JMH micro-benchmarks for the `KafkaTrackerStore` hot paths (design §11.4). **No broker** — these
measure the in-memory index structures (`PartitionShard` / `TrackerIndex`) and the `SidecarCodec`
directly. The broker-backed *macro* perf lane lives in `cesium-kafka-it` (nightly `macro-perf` job).

This module is **never** wired into `build` / `check`. Run it on demand:

```bash
./gradlew :cesium-kafka-benchmarks:jmh
```

Results land in `build/results/jmh/{results.json,human.txt}`. The committed reference run and the
honest measured-vs-target analysis are in [`results/`](results/RESULTS.md).

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
  no-op-drain noise. SingleShotTime measures one invocation and **excludes** the heavy
  `@Setup(Level.Invocation)` that rebuilds the ~1 M-entry base shard, so the per-op time is honest.
  `@OperationsPerInvocation` normalizes the inner loop to per-op. Insert and drain are deliberately
  **conservative** (heap held at or above 1 M throughout the measured run).
* **Non-destructive paths use `Throughput`.** The ring search completes a guaranteed-**miss** odd
  offset against an even-keyed log, so it runs the full-depth binary search with **zero mutation** —
  one pre-built shard serves the whole run. Sidecar encode/decode are pure functions of a pre-built
  entry set.
* **Randomization is in `@Setup`, never the measured loop.** Dispatch times, probe offsets, and the
  replay record stream are precomputed; the measured loops only read primitive arrays and call the
  index, consuming results through a `Blackhole` (or returning them) to defeat dead-code elimination.

## Honesty posture

Per design §15 risk #9, the §11.4 targets are **projections until measured**. The measured reality
(including two honest misses — `drainDue` and ring binary-search complete — re-baselined in §11.4)
is in [`results/RESULTS.md`](results/RESULTS.md). The nightly `benchmarks` job is **non-gating**;
`results/compare.py` provides an **aspirational** >10 % regression comparator against
`results/baseline.json` (report-only in v1 — a flaky perf gate is worse than none).

## Build wiring

* Applies `cesium.java-conventions` + `me.champeau.jmh` (version from the `jmh-plugin` catalog entry).
* `NullAway` is scope-disabled for the `jmh` source set **only** (benchmarks are not production; the
  repo default `NullAway=ERROR` is untouched everywhere else).
* The forked benchmark JVM is pinned to the project's JDK 21 toolchain.
