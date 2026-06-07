package com.jucius.cesium.kafka.store.tracker.index;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import org.junit.jupiter.api.Test;

/**
 * Allocation-discipline gate (design §1.1: zero per-entry allocation in the steady-state hot
 * path). Runs a 1M-iteration add/complete/drain/finalize/maintenance loop: the first pass sizes
 * every structure (geometric growth, batch arrays, free stack), further passes warm the JIT, and
 * the measured passes assert average allocation below 2 bytes/op via
 * {@link com.sun.management.ThreadMXBean#getCurrentThreadAllocatedBytes()}. The structures
 * themselves allocate zero in steady state; the 2 B/op budget is generous slack for residual JIT
 * activity on the measuring thread. The minimum over several measured passes is used so a stray
 * late compilation cannot flake the gate.
 */
class SteadyStateAllocationTest {

    private static final int ITERATIONS = 1_000_000;

    /** Mutable loop state kept in plain fields so the loop itself allocates nothing. */
    private static final class LoopState {
        long nowMs = 1_000_000;
        long nextSourceOffset;
        long nextTrackerOffset;
    }

    @Test
    void steadyStateLoopAllocatesUnderTwoBytesPerOp() {
        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        assertTrue(threads.isThreadAllocatedMemorySupported(), "JVM must support allocation accounting");

        TrackerIndex index = new TrackerIndex();
        index.assignPartition(0);
        LoopState state = new LoopState();

        runLoop(index, state); // sizing pass: grows every structure to steady-state capacity
        runLoop(index, state); // JIT warmup
        runLoop(index, state);

        double bestBytesPerOp = Double.MAX_VALUE;
        for (int round = 0; round < 3; round++) {
            long before = threads.getCurrentThreadAllocatedBytes();
            long ops = runLoop(index, state);
            long after = threads.getCurrentThreadAllocatedBytes();
            bestBytesPerOp = Math.min(bestBytesPerOp, (after - before) / (double) ops);
        }
        System.out.printf("steady-state allocation: %.4f bytes/op (gate 2.0)%n", bestBytesPerOp);
        assertTrue(bestBytesPerOp < 2.0, "steady-state allocation " + bestBytesPerOp + " B/op exceeds 2 B/op");
    }

    /**
     * One steady-state pass: every iteration adds an entry due 64–127 ms out while the clock
     * advances 1 ms/iteration; every 8th iteration completes a recent (usually still-pending)
     * entry pre-dispatch (exercising lazy deletion); every 16th drains due entries and finalizes
     * the committed batch; maintenance runs periodically. The pass ends by draining the index
     * empty so every pass starts and ends in an identical shape (no growth between passes).
     *
     * @return number of individual index operations performed
     */
    private static long runLoop(TrackerIndex index, LoopState state) {
        long ops = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            index.applyAdd(0, state.nextSourceOffset, state.nowMs + 64 + (i & 63), state.nextTrackerOffset++);
            state.nextSourceOffset += 2;
            ops++;
            if ((i & 7) == 0 && state.nextSourceOffset >= 16) {
                index.applyComplete(0, state.nextSourceOffset - 8);
                ops++;
            }
            if ((i & 15) == 15) {
                state.nowMs += 16;
                IndexDueBatch batch = index.drainDue(state.nowMs, 1_024);
                index.onBatchCommitted(batch);
                ops += 2L * batch.size() + 1;
            }
            if ((i & 65_535) == 65_535) {
                index.maintenance();
                ops++;
            }
        }
        // Drain to empty so the next pass reuses identical capacity everywhere.
        state.nowMs += 1_000_000;
        while (true) {
            IndexDueBatch batch = index.drainDue(state.nowMs, 8_192);
            if (batch.size() == 0) {
                break;
            }
            index.onBatchCommitted(batch);
            ops += 2L * batch.size();
        }
        return ops;
    }
}
