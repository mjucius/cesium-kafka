package events.cesium.kafka.benchmarks;

import events.cesium.kafka.store.tracker.index.DueEntrySink;
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
 * Design §11.4: <em>drainDue — target ≥ 8 M ops/s</em>.
 *
 * <p>Measures {@link PartitionShard#drainDue(long, int, DueEntrySink)} popping due entries: per
 * entry an {@code O(log n)} min-heap pop (sift-down) plus a {@code PENDING → IN_FLIGHT} state flip
 * and a primitive sink callback, zero allocation.
 *
 * <p><strong>Why SingleShotTime + a per-invocation rebuild.</strong> Draining is destructive — it
 * empties the heap — so it cannot be measured one-call-per-JMH-op without the heap exhausting and
 * the benchmark degenerating into measuring empty no-op drains (which would dishonestly inflate the
 * number). Each invocation rebuilds a fresh {@value #HEAP_ENTRIES}-entry all-due shard in
 * {@code @Setup} (excluded from the SingleShotTime measurement) and then drains exactly
 * {@value #DRAIN} entries, so the measured heap is held between 2 M and 1 M entries throughout —
 * i.e. always ≥ 1 M, mildly <strong>conservative</strong> versus a steady 1 M-entry heap. Dispatch
 * times are randomized in setup so the pop order (and the resulting sift-down paths / cache misses)
 * is realistic, never sorted.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class DrainDueBenchmark {

    /** Entries built per invocation; the heap is held above 1 M for the whole measured drain. */
    static final int HEAP_ENTRIES = 2_000_000;

    /** Entries drained (and measured) per invocation. */
    static final int DRAIN = 1_000_000;

    /** Pop batch handed to {@code drainDue} per call — a realistic transactional dispatch batch. */
    static final int POP_BATCH = 4096;

    private static final long EPOCH_BASE_MS = 1_700_000_000_000L;
    private static final long DELAY_SPREAD_MS = 7L * 24 * 3600 * 1000;

    /** All entries are due at this clock (built strictly below it), so every drain pops. */
    private static final long NOW_MS = EPOCH_BASE_MS + DELAY_SPREAD_MS + 1;

    private PartitionShard shard;
    private final BlackholeSink sink = new BlackholeSink();

    @Setup(Level.Invocation)
    public void buildAllDueShard() {
        shard = new PartitionShard();
        SplittableRandom random = new SplittableRandom(0xD7A1_07L);
        for (int i = 0; i < HEAP_ENTRIES; i++) {
            shard.applyAdd(i, EPOCH_BASE_MS + random.nextLong(DELAY_SPREAD_MS), i);
        }
    }

    @Benchmark
    @OperationsPerInvocation(DRAIN)
    public void drain(Blackhole bh) {
        sink.bh = bh;
        final PartitionShard s = shard;
        int drained = 0;
        while (drained < DRAIN) {
            int n = s.drainDue(NOW_MS, Math.min(POP_BATCH, DRAIN - drained), sink);
            if (n == 0) {
                break; // unreachable: 2 M due entries, only 1 M drained
            }
            drained += n;
        }
    }

    /** Reused, allocation-free sink that funnels each popped entry's identity into the Blackhole. */
    private static final class BlackholeSink implements DueEntrySink {
        @SuppressWarnings("NullAway.Init")
        Blackhole bh;

        @Override
        public void accept(int slotId, long sourceOffset, long dispatchAtMs, long trackerAddOffset) {
            bh.consume(slotId);
            bh.consume(sourceOffset ^ dispatchAtMs ^ trackerAddOffset);
        }
    }
}
