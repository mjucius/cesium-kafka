package events.cesium.kafka.benchmarks;

import events.cesium.kafka.store.tracker.index.PartitionShard;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Design §11.4: <em>ring binary-search complete — target ≥ 10 M ops/s</em>.
 *
 * <p>Isolates the arrival-log binary search that backs {@link PartitionShard#applyComplete(long)}
 * (design §5.2 / D6: a hash-map-free {@code O(log n)} source-offset lookup over the double-sorted
 * arrival log). This is the dominant, allocation-free cost of every completion.
 *
 * <p><strong>Why a guaranteed miss keeps it Throughput + non-destructive.</strong> A real
 * completion mutates (marks the slot completed, advances the head), which would deplete a 1 M-entry
 * shard and make it un-reusable across the millions of JMH ops a Throughput run needs. The shard is
 * built with <em>even</em> source offsets and the benchmark completes a randomized <em>odd</em>
 * interior offset: {@code findBySourceOffset} traverses the full {@code log2(1 M) ≈ 20} levels and
 * returns {@code -1}, so {@code applyComplete} takes the R2 no-op path — the exact binary search,
 * with zero mutation, so one pre-built shard serves the whole run. The randomized probe offsets are
 * precomputed; the measured method only does an array load + the search.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class RingSearchCompleteBenchmark {

    /** Pending entries the binary search ranges over (the "1 M-entry" ring). */
    static final int ENTRIES = 1_000_000;

    /** Number of distinct precomputed probe offsets, cycled through to defeat trivial caching. */
    static final int PROBES = 1 << 16; // 65 536

    private static final long EPOCH_BASE_MS = 1_700_000_000_000L;
    private static final long DELAY_SPREAD_MS = 7L * 24 * 3600 * 1000;

    private PartitionShard shard;
    private long[] missOffsets;
    private int cursor;

    @Setup(Level.Trial)
    public void build() {
        shard = new PartitionShard();
        SplittableRandom random = new SplittableRandom(0x81A5_12L);
        for (int i = 0; i < ENTRIES; i++) {
            // EVEN source offsets (0, 2, 4, …); tracker offset strictly increasing.
            shard.applyAdd(2L * i, EPOCH_BASE_MS + random.nextLong(DELAY_SPREAD_MS), i);
        }
        missOffsets = new long[PROBES];
        for (int k = 0; k < PROBES; k++) {
            // ODD interior offset → always absent → full-depth search → -1 → no-op.
            missOffsets[k] = 2L * random.nextInt(ENTRIES) + 1;
        }
    }

    @Benchmark
    public boolean searchMiss() {
        long key = missOffsets[cursor++ & (PROBES - 1)];
        return shard.applyComplete(key); // always false; pure binary search, no mutation
    }
}
