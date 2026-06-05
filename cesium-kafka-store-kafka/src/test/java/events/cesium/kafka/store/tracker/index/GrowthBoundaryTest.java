package events.cesium.kafka.store.tracker.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.junit.jupiter.api.Test;

/**
 * Drives a single shard across many internal geometric-growth boundaries (~3M entries: the
 * fastutil big lists, the heap array, the metadata chunks and the free stack all re-size multiple
 * times) and verifies counts, order and binary-search integrity end to end.
 */
class GrowthBoundaryTest {

    private static final int N = 3_000_000;

    @Test
    void threeMillionEntriesSurviveGrowthDrainAndReuse() {
        PartitionShard shard = new PartitionShard();
        long base = 1_000_000;
        for (int i = 0; i < N; i++) {
            shard.applyAdd(i, base + i, i);
        }
        assertEquals(N, shard.pendingCount());
        assertEquals(N, shard.allocatedSlotCount());

        // Binary search deep inside the grown log.
        assertTrue(shard.applyComplete(1_500_000), "binary search across segment boundaries");
        assertTrue(shard.applyComplete(2_999_999));
        assertTrue(shard.applyComplete(0));
        assertEquals(N - 3, shard.pendingCount());

        // Drain everything in batches, asserting global due order, then finalize each batch.
        long[] lastAt = {Long.MIN_VALUE};
        long drained = 0;
        IntArrayList slots = new IntArrayList();
        DueEntrySink sink = (slot, src, at, trk) -> {
            assertTrue(at >= lastAt[0], "drain order");
            lastAt[0] = at;
            slots.add(slot);
        };
        while (true) {
            slots.clear();
            int got = shard.drainDue(base + N, 8_192, sink);
            if (got == 0) {
                break;
            }
            drained += got;
            shard.finalizeCommitted(slots.elements(), slots.size());
        }
        assertEquals(N - 3, drained);
        assertEquals(0, shard.pendingCount());
        assertEquals(0, shard.inFlightCount());

        // Everything left the log: every slot is recyclable (directly free or zombie-parked).
        assertEquals(N, shard.freeSlotCount() + shard.zombieSlotCount());
        assertEquals(0, shard.completedHeldInLog());
        assertEquals(0, shard.logTotalSize());

        // Reuse after mass-free: adds recycle slots without growing the pool.
        for (int i = 0; i < 1_000; i++) {
            shard.applyAdd(N + i, base + 2L * N + i, N + i);
        }
        assertEquals(N, shard.allocatedSlotCount(), "slots recycled, pool did not grow");
        assertEquals(1_000, shard.pendingCount());
    }
}
