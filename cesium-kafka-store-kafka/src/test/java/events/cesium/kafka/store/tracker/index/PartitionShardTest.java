package events.cesium.kafka.store.tracker.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.jupiter.api.Test;

/** Edge-case units for {@link PartitionShard}: slot lifecycle, sweep, rebuild thresholds, R2. */
class PartitionShardTest {

    private static IntArrayList drainSlots(PartitionShard shard, long nowMs, int max) {
        IntArrayList slots = new IntArrayList();
        shard.drainDue(nowMs, max, (slot, src, at, trk) -> slots.add(slot));
        return slots;
    }

    private static LongArrayList pendingSources(PartitionShard shard) {
        LongArrayList sources = new LongArrayList();
        shard.oldestPending((slot, src, at, trk) -> sources.add(src));
        return sources;
    }

    @Test
    void slotsAreFreedOnlyOnLogExitNeverAtDispatch() {
        PartitionShard shard = new PartitionShard();
        shard.applyAdd(0, 100, 0);
        shard.applyAdd(1, 110, 1);
        shard.applyAdd(2, 9_000, 2);
        assertEquals(3, shard.allocatedSlotCount());

        IntArrayList drained = drainSlots(shard, 200, 10);
        assertEquals(2, drained.size());
        assertEquals(0, shard.freeSlotCount(), "dispatch must not free slots");
        assertEquals(2, shard.inFlightCount());

        // Finalize: both slots complete, the head advances past them, both leave the log → freed.
        shard.finalizeCommitted(drained.elements(), drained.size());
        assertEquals(2, shard.freeSlotCount(), "slots free exactly when they leave the log");
        assertEquals(0, shard.zombieSlotCount(), "drained slots had no leftover heap copies");
        assertEquals(0, shard.inFlightCount());

        // Reuse: the next adds recycle the freed slots instead of growing the pool.
        shard.applyAdd(3, 9_100, 3);
        shard.applyAdd(4, 9_200, 4);
        assertEquals(3, shard.allocatedSlotCount(), "freed slots are reused");
        assertEquals(0, shard.freeSlotCount());
    }

    /**
     * A completion that arrives before dispatch leaves the slot's heap copy live (lazy deletion),
     * so on log exit the slot parks as a zombie and is recycled only after a heap rebuild — the
     * guard against rebinding a heap-resident slot id (see EntryPool javadoc).
     */
    @Test
    void preDispatchCompletedSlotParksAsZombieUntilHeapRebuild() {
        PartitionShard shard = new PartitionShard(1, 1_000_000);
        assertFalse(shard.applyComplete(0), "empty shard: R2 no-op");
        shard.applyAdd(0, 100, 0);
        shard.applyAdd(1, 200, 1);

        assertTrue(shard.applyComplete(0)); // head entry completes pre-dispatch → leaves the log
        assertEquals(1, shard.pendingCount());
        assertEquals(0, shard.freeSlotCount(), "heap copy still live: must not be reusable yet");
        assertEquals(1, shard.zombieSlotCount());

        shard.applyAdd(2, 300, 2);
        assertEquals(3, shard.allocatedSlotCount(), "zombie must NOT be reused");

        shard.maintenance(); // stale=1 > max(floor=1, heapSize/4)? 1 > 1 is false → no rebuild yet
        assertEquals(1, shard.zombieSlotCount());
        assertTrue(shard.applyComplete(1));
        shard.maintenance(); // stale=2 > max(1, ~0) → rebuild purges copies, reclaims zombies
        assertTrue(shard.heapRebuilds() >= 1);
        assertEquals(0, shard.staleHeapEntries());
        assertEquals(0, shard.zombieSlotCount(), "rebuild reclaims zombies");
        assertTrue(shard.freeSlotCount() >= 1);

        shard.applyAdd(3, 400, 3);
        assertEquals(3, shard.allocatedSlotCount(), "reclaimed slot is reused after the rebuild");
    }

    @Test
    void sweepPreservesOrderAndBinarySearchStaysValid() {
        PartitionShard shard = new PartitionShard(4, 2);
        for (int i = 0; i < 10; i++) {
            shard.applyAdd(i, 1_000 + i, i);
        }
        // Keep src=0 pending so the head stays pinned; complete six mid-log entries.
        for (long src : new long[] {1, 2, 3, 5, 7, 9}) {
            assertTrue(shard.applyComplete(src));
        }
        assertEquals(6, shard.completedHeldInLog());
        assertEquals(4, shard.pendingCount());

        shard.maintenance(); // rebuild (stale 6 > max(4, 10/4)) then sweep (6 > max(2, 10/2))
        assertEquals(1, shard.logSweeps());
        assertEquals(0, shard.completedHeldInLog());
        assertEquals(6, shard.freeSlotCount(), "swept slots freed copy-free after the rebuild");
        assertEquals(0, shard.zombieSlotCount());

        // Order preserved → oldestPending still in trackerAddOffset order.
        assertEquals(LongArrayList.of(0, 4, 6, 8), pendingSources(shard));
        // Binary search still valid over the compacted log.
        assertTrue(shard.applyComplete(4));
        assertFalse(shard.applyComplete(3), "swept offset: R2 no-op");
        assertEquals(3, shard.pendingCount());
    }

    @Test
    void heapRebuildThresholdHonored() {
        PartitionShard shard = new PartitionShard(4, 1_000_000);
        for (int i = 0; i < 12; i++) {
            shard.applyAdd(i, 100_000 + i, i);
        }
        for (long src = 0; src < 3; src++) {
            shard.applyComplete(src);
        }
        assertEquals(3, shard.staleHeapEntries());
        shard.maintenance(); // 3 > max(4, 12/4=3) = 4 → below threshold, no rebuild
        assertEquals(0, shard.heapRebuilds(), "threshold not crossed");

        shard.applyComplete(3);
        shard.applyComplete(4);
        assertEquals(5, shard.staleHeapEntries());
        shard.maintenance(); // 5 > max(4, 12/4=3) = 4 → rebuild
        assertEquals(1, shard.heapRebuilds(), "threshold crossed");
        assertEquals(0, shard.staleHeapEntries());
        assertEquals(7, shard.heapSize(), "only live pending entries survive the rebuild");
    }

    @Test
    void restoreAfterAbortRedispatchesSameEntries() {
        PartitionShard shard = new PartitionShard();
        shard.applyAdd(0, 100, 0);
        shard.applyAdd(1, 100, 1);
        shard.applyAdd(2, 5_000, 2);

        IntArrayList batch = drainSlots(shard, 200, 10);
        assertEquals(2, batch.size());
        assertEquals(1, shard.pendingCount());
        assertEquals(2, shard.inFlightCount());
        assertEquals(0, drainSlots(shard, 200, 10).size(), "in-flight entries are excluded");

        shard.restoreAfterAbort(batch.elements(), batch.size());
        assertEquals(3, shard.pendingCount());
        assertEquals(0, shard.inFlightCount());
        assertEquals(100, shard.nextDeadlineMs());

        IntArrayList again = drainSlots(shard, 200, 10);
        IntArrayList sortedBatch = new IntArrayList(batch);
        IntArrayList sortedAgain = new IntArrayList(again);
        sortedBatch.sort(null);
        sortedAgain.sort(null);
        assertEquals(sortedBatch, sortedAgain, "abort restores exactly the popped entries");
        shard.finalizeCommitted(again.elements(), again.size());
        assertEquals(0, shard.inFlightCount());
        assertEquals(2, shard.freeSlotCount());
    }

    /**
     * The pollDue contract "(dispatchAtMs, then arrival)": a same-millisecond burst (the midnight
     * thundering herd, design §7.1) drains in arrival == ascending-source-offset order, feeding
     * the one-seek forward-scan fetch — and the order survives an abort/restore cycle.
     */
    @Test
    void equalDeadlineEntriesDrainInArrivalOrder() {
        PartitionShard shard = new PartitionShard();
        for (int i = 0; i < 8; i++) {
            shard.applyAdd(i, 1_000, i);
        }
        LongArrayList sources = new LongArrayList();
        IntArrayList slots = new IntArrayList();
        shard.drainDue(1_000, 16, (slot, src, at, trk) -> {
            sources.add(src);
            slots.add(slot);
        });
        assertEquals(LongArrayList.of(0, 1, 2, 3, 4, 5, 6, 7), sources, "(dispatchAtMs, then arrival)");

        shard.restoreAfterAbort(slots.elements(), slots.size());
        LongArrayList again = new LongArrayList();
        IntArrayList slotsAgain = new IntArrayList();
        shard.drainDue(1_000, 16, (slot, src, at, trk) -> {
            again.add(src);
            slotsAgain.add(slot);
        });
        assertEquals(sources, again, "arrival-order ties survive restore-after-abort");
        shard.finalizeCommitted(slotsAgain.elements(), slotsAgain.size());
    }

    /**
     * Independent zombie-pressure maintenance trigger: replay-style ADD+COMPLETE bursts complete
     * entries pre-dispatch, so their slots exit the log with live heap copies (zombies); later
     * drains skim the stale copies (draining the stale estimate toward zero) while the zombies
     * stay parked. Without the zombie trigger nothing ever rebuilds in this pattern and parked
     * slots accumulate monotonically across recovery episodes.
     */
    @Test
    void zombiePressureTriggersRebuildAcrossRecoveryEpisodes() {
        int staleFloor = 8;
        PartitionShard shard = new PartitionShard(staleFloor, 1_000_000);
        long src = 0;
        long trk = 0;
        for (int episode = 0; episode < 10; episode++) {
            for (int i = 0; i < 6; i++) {
                shard.applyAdd(src, 5_000 + src, trk++);
                assertTrue(shard.applyComplete(src), "replay-style pre-dispatch complete");
                src++;
            }
            // Live-phase drains skim the stale copies; the parked zombies remain.
            drainSlots(shard, Long.MAX_VALUE - 1, 1_000);
            assertEquals(0, shard.staleHeapEntries(), "stale debt fully skimmed by the drain");
            shard.maintenance();
            long bound = Math.max(staleFloor, shard.allocatedSlotCount() / 4);
            assertTrue(
                    shard.zombieSlotCount() <= bound,
                    "parked zombies " + shard.zombieSlotCount() + " exceed the maintenance bound " + bound);
        }
        assertTrue(shard.heapRebuilds() >= 1, "zombie pressure must fire rebuilds");
        assertTrue(
                shard.allocatedSlotCount() <= 24,
                "pool growth must stay bounded across episodes, got " + shard.allocatedSlotCount());
    }

    /**
     * A duplicate ADD arriving after the arrival log has fully emptied must be dropped and
     * counted exactly like the non-empty-log duplicate — never resurrected as a fresh pending
     * entry (the high-watermark gate; a resurrected entry would re-dispatch).
     */
    @Test
    void duplicateAddAfterLogEmptiesIsDroppedAndCounted() {
        PartitionShard shard = new PartitionShard();
        shard.applyAdd(0, 100, 0);
        IntArrayList batch = drainSlots(shard, 100, 10);
        shard.finalizeCommitted(batch.elements(), batch.size());
        assertEquals(0, shard.logTotalSize(), "log fully emptied");

        assertFalse(shard.applyAdd(0, 500, 1), "duplicate of a departed entry");
        assertEquals(1, shard.anomalies(), "counted like the non-empty-log duplicate");
        assertEquals(0, shard.pendingCount(), "never resurrected");
        assertEquals(0, drainSlots(shard, Long.MAX_VALUE - 1, 10).size(), "nothing to re-dispatch");

        assertTrue(shard.applyAdd(1, 600, 2), "the next genuinely-new offset still inserts");
        assertEquals(1, shard.pendingCount());
    }

    /**
     * Defensive branch: a COMPLETE landing on an IN_FLIGHT slot is impossible from a correct
     * engine (our own echo is only readable after the commit that finalized the slot) — it must
     * count an anomaly and never corrupt in-flight state, leaving both resolution paths intact.
     */
    @Test
    void completeOnInFlightCountsAnomalyAndPreservesBothResolutionPaths() {
        PartitionShard shard = new PartitionShard();
        shard.applyAdd(0, 100, 0);
        shard.applyAdd(1, 100, 1);
        IntArrayList batch = drainSlots(shard, 100, 10);
        assertEquals(2, batch.size());

        assertFalse(shard.applyComplete(0), "complete-on-in-flight is rejected");
        assertFalse(shard.applyComplete(1), "complete-on-in-flight is rejected");
        assertEquals(2, shard.anomalies());
        assertEquals(2, shard.inFlightCount(), "in-flight state untouched");
        assertEquals(0, shard.pendingCount());

        shard.finalizeSlot(batch.getInt(0)); // commit path still works
        shard.restoreSlot(batch.getInt(1)); // abort path still works
        assertEquals(0, shard.inFlightCount());
        assertEquals(1, shard.pendingCount());
        assertEquals(LongArrayList.of(1), pendingSources(shard));

        IntArrayList redrained = drainSlots(shard, 100, 10);
        assertEquals(1, redrained.size(), "restored entry re-dispatches");
        shard.finalizeCommitted(redrained.elements(), redrained.size());
        assertEquals(2, shard.freeSlotCount());
    }

    /**
     * Defensive branch: a duplicate ADD landing on a COMPLETED-held slot (behind a far-future
     * head pin) updates {@code dispatchAtMs} harmlessly, counts the anomaly, never resurrects the
     * entry, and its stale heap copy is purged by the suspect rebuild on the next read.
     */
    @Test
    void duplicateAddOnCompletedHeldSlotDoesNotResurrect() {
        PartitionShard shard = new PartitionShard(1_000_000, 1_000_000);
        shard.applyAdd(0, 100_000, 0); // far-future head pin
        shard.applyAdd(1, 200, 1);
        assertTrue(shard.applyComplete(1), "behind-head complete: held in log, heap copy resident");
        assertEquals(1, shard.completedHeldInLog());
        assertEquals(1, shard.pendingCount());

        assertFalse(shard.applyAdd(1, 50, 2), "duplicate ADD on a COMPLETED-held slot");
        assertEquals(1, shard.anomalies());
        assertEquals(1, shard.pendingCount(), "no resurrection");

        // The in-place update behind a resident copy marks the order suspect; the next read
        // rebuilds, purging the held slot's stale copy.
        assertEquals(100_000, shard.nextDeadlineMs(), "only the pinned head entry is pending");
        assertEquals(1, shard.heapRebuilds(), "suspect rebuild fired on the read");
        assertEquals(0, shard.staleHeapEntries());
        assertEquals(1, shard.heapSize(), "stale copy of the completed slot purged");

        assertEquals(0, drainSlots(shard, 200, 10).size(), "nothing due before the head pin");
        IntArrayList batch = drainSlots(shard, 100_000, 10);
        assertEquals(1, batch.size(), "a full drain surfaces only the pending head");
        shard.finalizeCommitted(batch.elements(), batch.size());
        assertEquals(0, shard.pendingCount());
        assertEquals(0, shard.completedHeldInLog(), "the held slot left with the head advance");
    }

    @Test
    void unknownCompleteIsSilentNoOpPerR2() {
        PartitionShard shard = new PartitionShard();
        shard.applyAdd(10, 100, 0);
        assertFalse(shard.applyComplete(5), "below range: no-op");
        assertFalse(shard.applyComplete(11), "above range: no-op");
        assertFalse(shard.applyComplete(7), "gap: no-op");
        assertEquals(1, shard.pendingCount());
        assertEquals(0, shard.anomalies(), "R2 no-ops are expected, not anomalies");
    }

    @Test
    void outOfOrderNonDuplicateAddIsCountedAndDropped() {
        PartitionShard shard = new PartitionShard();
        shard.applyAdd(10, 100, 5);
        assertFalse(shard.applyAdd(4, 200, 6), "source offset below the tail, not a duplicate");
        assertEquals(1, shard.pendingCount(), "dropped, not inserted");
        assertEquals(1, shard.anomalies());
        assertEquals(LongArrayList.of(10), pendingSources(shard));
    }

    @Test
    void nonMonotonicTrackerOffsetOnFreshAppendIsRejected() {
        PartitionShard shard = new PartitionShard();
        shard.applyAdd(10, 100, 5);
        assertThrows(IllegalStateException.class, () -> shard.applyAdd(11, 200, 5));
    }

    @Test
    void resolvingANonInFlightSlotThrows() {
        PartitionShard shard = new PartitionShard();
        shard.applyAdd(0, 100, 0);
        IntArrayList batch = drainSlots(shard, 100, 1);
        shard.finalizeSlot(batch.getInt(0));
        assertThrows(IllegalStateException.class, () -> shard.finalizeSlot(batch.getInt(0)));
        assertThrows(IllegalStateException.class, () -> shard.restoreSlot(batch.getInt(0)));
    }

    @Test
    void oldestPendingStopsWhenVisitorReturnsFalse() {
        PartitionShard shard = new PartitionShard();
        for (int i = 0; i < 5; i++) {
            shard.applyAdd(i, 100 + i, i);
        }
        int[] visits = {0};
        shard.oldestPending((slot, src, at, trk) -> {
            visits[0]++;
            return visits[0] < 2;
        });
        assertEquals(2, visits[0]);
    }

    @Test
    void estimatedRetainedBytesTracksGrowthAndRelease() {
        PartitionShard shard = new PartitionShard();
        long empty = shard.estimatedRetainedBytes();
        for (int i = 0; i < 1_000; i++) {
            shard.applyAdd(i, 1_000 + i, i);
        }
        long full = shard.estimatedRetainedBytes();
        assertTrue(full >= empty + 1_000L * 32, "estimate reflects ~32 B/entry of nominal state");

        IntArrayList batch = drainSlots(shard, 10_000, 1_000);
        shard.finalizeCommitted(batch.elements(), batch.size());
        assertEquals(0, shard.pendingCount());
    }
}
