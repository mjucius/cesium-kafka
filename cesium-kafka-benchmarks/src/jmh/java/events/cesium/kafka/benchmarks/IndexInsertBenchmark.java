package events.cesium.kafka.benchmarks;

import events.cesium.kafka.store.tracker.index.PartitionShard;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Design §11.4: <em>index insert (1 M-entry heap) — target ≥ 15 M ops/s</em>.
 *
 * <p>Measures {@link PartitionShard#applyAdd(long, long, long)} on the live-tail append path
 * (strictly increasing source/tracker offsets, above the shard watermark — the common case): an
 * {@code O(log n)} min-heap sift-up keyed on a randomized {@code dispatchAtMs} plus an {@code O(1)}
 * arrival-log append, zero allocation in steady state.
 *
 * <p><strong>Why SingleShotTime + a per-invocation rebuild.</strong> Insert is a <em>growing</em>
 * operation, so it cannot be measured one-call-per-JMH-op without either depleting nothing
 * (impossible) or letting the heap balloon arbitrarily. Throughput/AverageTime would also fold the
 * heavy {@link Level#Invocation} setup that rebuilds the 1 M-entry base shard <em>into</em> the
 * measured time. SingleShotTime measures exactly one invocation and excludes {@code @Setup}, so the
 * reported number is the cost of {@value #INSERTS} real inserts into a heap that grows from 1 M to
 * 2 M entries during the measured run. That over-sizes the heap relative to the "1 M-entry" target
 * (deeper sift-ups, more cache misses), so the number is mildly <strong>conservative</strong> —
 * the honest direction. {@code dispatchAtMs} values are randomized in {@code @Setup}, never in the
 * measured loop.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class IndexInsertBenchmark {

    /** Pre-existing pending entries the inserts land on top of (the "1 M-entry heap"). */
    static final int BASE_ENTRIES = 1_000_000;

    /** Inserts measured per invocation. */
    static final int INSERTS = 1_000_000;

    private static final long EPOCH_BASE_MS = 1_700_000_000_000L;
    private static final long DELAY_SPREAD_MS = 7L * 24 * 3600 * 1000; // up to a week out

    /** Randomized dispatch times for the measured inserts — computed once, reused every invocation. */
    private long[] insertDispatchAt;

    private PartitionShard shard;
    private long nextSourceOffset;
    private long nextTrackerOffset;

    @Setup(Level.Trial)
    public void precomputeDispatchTimes() {
        SplittableRandom random = new SplittableRandom(0xCE51_01L);
        insertDispatchAt = new long[INSERTS];
        for (int i = 0; i < INSERTS; i++) {
            insertDispatchAt[i] = EPOCH_BASE_MS + random.nextLong(DELAY_SPREAD_MS);
        }
    }

    @Setup(Level.Invocation)
    public void buildBaseShard() {
        shard = new PartitionShard();
        SplittableRandom random = new SplittableRandom(0xBA5E_01L);
        for (int i = 0; i < BASE_ENTRIES; i++) {
            shard.applyAdd(i, EPOCH_BASE_MS + random.nextLong(DELAY_SPREAD_MS), i);
        }
        // The measured inserts continue strictly above the watermark and the log tail.
        nextSourceOffset = BASE_ENTRIES;
        nextTrackerOffset = BASE_ENTRIES;
    }

    @Benchmark
    @OperationsPerInvocation(INSERTS)
    public void insert(Blackhole bh) {
        long src = nextSourceOffset;
        long trk = nextTrackerOffset;
        final long[] dispatchAt = insertDispatchAt;
        final PartitionShard s = shard;
        for (int i = 0; i < INSERTS; i++) {
            bh.consume(s.applyAdd(src++, dispatchAt[i], trk++));
        }
        nextSourceOffset = src;
        nextTrackerOffset = trk;
    }
}
