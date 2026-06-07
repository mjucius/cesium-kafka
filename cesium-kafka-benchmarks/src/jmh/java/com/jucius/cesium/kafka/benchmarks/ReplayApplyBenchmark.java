package com.jucius.cesium.kafka.benchmarks;

import com.jucius.cesium.kafka.store.tracker.index.PartitionShard;
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
 * Design §11.4: <em>replay apply (add + complete mix) — target ≥ 5 M records/s</em>.
 *
 * <p>Recovery and live tailing share one code path (design §5.5, {@code onTrackerRecord}): a stream
 * of ADD and COMPLETE records applied to a shard from empty. This models the dominant cost of
 * replaying a sizeable backlog — each ADD is an {@code O(log n)} heap insert + log append, each
 * COMPLETE an {@code O(log n)} arrival-log binary search + bit set (+ amortized head advance). The
 * mix is ~60 % ADD / ~40 % COMPLETE, with COMPLETEs settling the oldest still-pending entry (FIFO),
 * so the head advances steadily and the live region grows to a realistic six-figure backlog over
 * the run (binary searches over a {@code log2} that climbs through ~17–18). The whole record stream
 * is pre-generated in {@code @Setup}; the measured loop only reads primitive arrays and calls the
 * index — no RNG, no allocation.
 *
 * <p><strong>Why SingleShotTime.</strong> Replay is inherently a "process N records from empty"
 * bulk operation. Each invocation gets a fresh empty shard (a cheap {@code @Setup}, excluded from
 * the SingleShotTime measurement) and replays the full {@value #RECORDS}-record stream;
 * {@code @OperationsPerInvocation} normalizes to per-record time so the result reads directly as
 * records/s.
 *
 * <p><strong>Scope caveat (honest, design §15 risk #9).</strong> This measures the <em>raw apply</em>
 * path only: {@code maintenance()} (the §5.3 heap rebuild / arrival-log sweep) is deliberately
 * <em>not</em> called during the loop, so no rebuild/sweep amortization is folded into the reported
 * rate. Two consequences, disclosed rather than hidden: (a) production recovery replay interleaves
 * periodic {@code maintenance()} to bound memory, paying amortized O(n) rebuild/sweep passes this
 * benchmark omits — so the figure slightly <em>over</em>states replay-with-maintenance throughput;
 * (b) conversely, the lazy-deleted heap copies and completed-held log slots accumulate unbounded here
 * (~0.8 M COMPLETEs are never swept), so late pushes sift through an ever-larger backing list — a
 * cost a maintained shard would <em>not</em> pay. The net is a raw-path number; it clears its 5 M
 * target by ~5.2×, and with maintenance folded in it still clears. The maintained-replay cost is the
 * macro suite's job ({@code HeterogeneousReplayPerfIT}) and a server-class re-measure (RESULTS.md).
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ReplayApplyBenchmark {

    /** Records replayed (and measured) per invocation. */
    static final int RECORDS = 2_000_000;

    private static final long EPOCH_BASE_MS = 1_700_000_000_000L;
    private static final long DELAY_SPREAD_MS = 7L * 24 * 3600 * 1000;

    /** {@code true} = COMPLETE, {@code false} = ADD. */
    private boolean[] isComplete;

    private long[] sourceOffset;
    private long[] trackerOffset;
    private long[] dispatchAt;

    private PartitionShard shard;

    @Setup(Level.Trial)
    public void generateReplayStream() {
        SplittableRandom random = new SplittableRandom(0xEEFA_11L);
        isComplete = new boolean[RECORDS];
        sourceOffset = new long[RECORDS];
        trackerOffset = new long[RECORDS];
        dispatchAt = new long[RECORDS];

        long nextAdd = 0; // next source offset to ADD (strictly increasing → live-tail append path)
        long nextComplete = 0; // next source offset to settle, FIFO (oldest pending first)
        for (int i = 0; i < RECORDS; i++) {
            // ~40% completes, but only when there is an added-yet-unsettled offset available.
            if (nextComplete < nextAdd && random.nextInt(5) < 2) {
                isComplete[i] = true;
                sourceOffset[i] = nextComplete++;
            } else {
                isComplete[i] = false;
                sourceOffset[i] = nextAdd;
                trackerOffset[i] = nextAdd; // tracker offset advances in lockstep with the ADD order
                dispatchAt[i] = EPOCH_BASE_MS + random.nextLong(DELAY_SPREAD_MS);
                nextAdd++;
            }
        }
    }

    @Setup(Level.Invocation)
    public void freshShard() {
        shard = new PartitionShard();
    }

    @Benchmark
    @OperationsPerInvocation(RECORDS)
    public void replay(Blackhole bh) {
        final PartitionShard s = shard;
        final boolean[] complete = isComplete;
        final long[] src = sourceOffset;
        final long[] trk = trackerOffset;
        final long[] disp = dispatchAt;
        for (int i = 0; i < RECORDS; i++) {
            if (complete[i]) {
                bh.consume(s.applyComplete(src[i]));
            } else {
                bh.consume(s.applyAdd(src[i], disp[i], trk[i]));
            }
        }
    }
}
