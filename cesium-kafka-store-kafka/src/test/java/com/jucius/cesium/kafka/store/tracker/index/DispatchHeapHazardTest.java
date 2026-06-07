package com.jucius.cesium.kafka.store.tracker.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the duplicate-ADD (R1) heap hazard handling documented on {@link DispatchHeap}:
 * in-place {@code dispatchAtMs} updates against a live-value indirect comparator.
 */
class DispatchHeapHazardTest {

    private static LongArrayList drainSourceOffsets(PartitionShard shard, long nowMs, int max) {
        LongArrayList sources = new LongArrayList();
        shard.drainDue(nowMs, max, (slot, src, at, trk) -> sources.add(src));
        return sources;
    }

    /**
     * The analytically derived masking counterexample (heap layout
     * {@code [40, 50, 130, 80, 135, 140, 145]}, then 50 → 500 in place): the stale copy of the
     * moved entry sits above the still-due 80 and a naive lazy heap breaks with top=130 while 80
     * is masked beneath the misplaced 500. The suspect-rebuild must surface {40, 80} at now=90.
     */
    @Test
    void duplicateAddMovedLaterDoesNotMaskDueEntriesBelowIt() {
        PartitionShard shard = new PartitionShard();
        shard.applyAdd(0, 40, 0);
        shard.applyAdd(1, 50, 1); // will be moved to 500
        shard.applyAdd(2, 130, 2);
        shard.applyAdd(3, 80, 3); // placed under (1); masked after the move in a naive heap
        shard.applyAdd(4, 135, 4);
        shard.applyAdd(5, 140, 5);
        shard.applyAdd(6, 145, 6);
        assertEquals(7, shard.pendingCount());

        assertEquals(false, shard.applyAdd(1, 500, 7)); // R1 duplicate, moved later
        assertEquals(1, shard.anomalies());

        LongArrayList drained = drainSourceOffsets(shard, 90, 10);
        assertEquals(LongArrayList.of(0, 3), drained, "due entries must not be masked by the moved copy");
        assertEquals(130, shard.nextDeadlineMs(), "next pending deadline after the drain");
    }

    /** A deadline moved LATER must never dispatch early (rule b + suspect rebuild). */
    @Test
    void deadlineMovedLaterNeverDispatchesEarly() {
        PartitionShard shard = new PartitionShard();
        shard.applyAdd(0, 100, 0);
        shard.applyAdd(1, 500, 1);
        shard.applyAdd(0, 1_000, 2); // R1: 100 → 1000

        assertEquals(0, drainSourceOffsets(shard, 100, 10).size(), "must not dispatch at the OLD deadline");
        assertEquals(500, shard.nextDeadlineMs(), "other entry's deadline is unaffected");
        assertEquals(
                LongArrayList.of(1), drainSourceOffsets(shard, 999, 10), "only the unmoved entry is due before 1000");

        LongArrayList at1000 = drainSourceOffsets(shard, 1_000, 10);
        assertEquals(LongArrayList.of(0), at1000, "the moved entry dispatches at the NEW deadline");
    }

    /** A deadline moved EARLIER dispatches at the new time via the freshly pushed duplicate. */
    @Test
    void deadlineMovedEarlierDispatchesAtNewTime() {
        PartitionShard shard = new PartitionShard();
        shard.applyAdd(0, 1_000, 0);
        shard.applyAdd(1, 2_000, 1);
        shard.applyAdd(0, 100, 2); // R1: 1000 → 100

        assertEquals(100, shard.nextDeadlineMs());
        LongArrayList drained = drainSourceOffsets(shard, 100, 10);
        assertEquals(LongArrayList.of(0), drained, "dispatches at the moved-earlier time");
        assertEquals(1, shard.pendingCount());

        // The stale (too-deep) copy of src=0 must be skipped once it surfaces, not re-dispatched.
        assertEquals(LongArrayList.of(1), drainSourceOffsets(shard, 2_000, 10));
        assertEquals(0, shard.pendingCount());
        assertEquals(Long.MAX_VALUE, shard.nextDeadlineMs());
    }

    /** Repeated reschedules of the same entry: only the final deadline dispatches, exactly once. */
    @Test
    void repeatedReschedulesDispatchExactlyOnce() {
        PartitionShard shard = new PartitionShard();
        shard.applyAdd(0, 1_000, 0);
        long trk = 1;
        long[] moves = {200, 5_000, 50, 4_000, 300};
        for (long at : moves) {
            shard.applyAdd(0, at, trk++);
        }
        assertEquals(1, shard.pendingCount());
        assertEquals(300, shard.nextDeadlineMs());
        assertEquals(0, drainSourceOffsets(shard, 299, 10).size());
        assertEquals(LongArrayList.of(0), drainSourceOffsets(shard, 300, 10));
        assertEquals(0, drainSourceOffsets(shard, 10_000, 10).size(), "stale copies never re-dispatch");
    }

    /** I5: a duplicate ADD keeps the slot's ORIGINAL trackerAddOffset, never the duplicate's. */
    @Test
    void duplicateAddPreservesOriginalTrackerAddOffset() {
        PartitionShard shard = new PartitionShard();
        shard.applyAdd(7, 500, 42);
        shard.applyAdd(7, 900, 77); // duplicate with a later tracker offset

        long[] visited = new long[2];
        shard.oldestPending((slot, src, at, trk) -> {
            visited[0] = trk;
            visited[1] = at;
            return true;
        });
        assertEquals(42, visited[0], "original trackerAddOffset preserved (I5)");
        assertEquals(900, visited[1], "dispatchAtMs updated");

        LongArrayList trks = new LongArrayList();
        shard.drainDue(900, 10, (slot, src, at, trk) -> trks.add(trk));
        assertEquals(LongArrayList.of(42), trks, "drained batch carries the original offset");
    }

    /** The suspect rebuild purges duplicates and stale entries and resets the stale estimate. */
    @Test
    void suspectRebuildPurgesDuplicatesAndStaleEntries() {
        PartitionShard shard = new PartitionShard();
        for (int i = 0; i < 16; i++) {
            shard.applyAdd(i, 1_000 + i, i);
        }
        for (int i = 0; i < 4; i++) {
            shard.applyAdd(i, 5_000 + i, 16 + i); // four moved-later duplicates
        }
        assertEquals(4, shard.staleHeapEntries());
        assertEquals(0, shard.heapRebuilds());

        // First read after the in-place increases triggers exactly one coalesced rebuild.
        assertEquals(1_004, shard.nextDeadlineMs());
        assertEquals(1, shard.heapRebuilds());
        assertEquals(0, shard.staleHeapEntries());
        assertEquals(16, shard.heapSize(), "rebuild deduplicates to one copy per pending slot");
        assertTrue(drainSourceOffsets(shard, 999, 10).isEmpty());
        assertEquals(1, shard.heapRebuilds(), "no further rebuilds without new increases");
    }
}
