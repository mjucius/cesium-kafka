package events.cesium.kafka.store.tracker.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Units for {@link TrackerIndex}: cross-partition scheduling, penalty box, partition lifecycle. */
class TrackerIndexTest {

    @Test
    void drainsAcrossPartitionsInDeadlineOrder() {
        TrackerIndex index = new TrackerIndex();
        index.assignPartition(0);
        index.assignPartition(1);
        index.applyAdd(0, 0, 100, 0);
        index.applyAdd(0, 1, 300, 1);
        index.applyAdd(1, 0, 200, 0);

        assertEquals(100, index.nextDeadlineMs());
        IndexDueBatch batch = index.drainDue(400, 10);
        assertEquals(3, batch.size());
        assertEquals(100, batch.dispatchAtMs(0));
        assertEquals(0, batch.sourcePartition(0));
        assertEquals(200, batch.dispatchAtMs(1));
        assertEquals(1, batch.sourcePartition(1));
        assertEquals(300, batch.dispatchAtMs(2));
        assertEquals(0, batch.sourcePartition(2));
        index.onBatchCommitted(batch);
        assertEquals(0, index.totalPendingCount());
        assertEquals(0, index.totalInFlightCount());
        assertEquals(Long.MAX_VALUE, index.nextDeadlineMs());
    }

    @Test
    void penalizedPartitionIsSkippedAndDrivesNextDeadline() {
        TrackerIndex index = new TrackerIndex();
        index.assignPartition(0);
        index.assignPartition(1);
        index.applyAdd(0, 0, 100, 0); // due, but will be penalized
        index.applyAdd(1, 0, 200, 0);

        index.penalizeSourcePartition(0, 500);
        // A penalized-but-due entry must never drive a zero poll timeout: p0 contributes
        // max(100, 500) = 500, p1 contributes 200.
        assertEquals(200, index.nextDeadlineMs());

        IndexDueBatch batch = index.drainDue(300, 10);
        assertEquals(1, batch.size(), "penalized partition skipped even though due");
        assertEquals(1, batch.sourcePartition(0));
        index.onBatchCommitted(batch);
        assertEquals(500, index.nextDeadlineMs(), "the penalty deadline drives the poll timeout");

        assertEquals(0, index.drainDue(499, 10).size(), "still penalized");
        IndexDueBatch after = index.drainDue(500, 10);
        assertEquals(1, after.size(), "penalty expires at its deadline (inclusive)");
        assertEquals(0, after.sourcePartition(0));
        index.onBatchCommitted(after);
    }

    @Test
    void penaltyClearedByStampingPastDeadline() {
        TrackerIndex index = new TrackerIndex();
        index.assignPartition(0);
        index.applyAdd(0, 0, 100, 0);
        index.penalizeSourcePartition(0, 10_000);
        assertEquals(10_000, index.nextDeadlineMs());
        assertEquals(0, index.drainDue(5_000, 10).size());

        index.penalizeSourcePartition(0, 0); // clear-by-past-deadline, per the SchedulerStore javadoc
        assertEquals(100, index.nextDeadlineMs());
        IndexDueBatch batch = index.drainDue(5_000, 10);
        assertEquals(1, batch.size());
        index.onBatchCommitted(batch);
    }

    @Test
    void replaceOnStampSemantics() {
        TrackerIndex index = new TrackerIndex();
        index.assignPartition(0);
        index.applyAdd(0, 0, 100, 0);
        index.penalizeSourcePartition(0, 10_000);
        index.penalizeSourcePartition(0, 300); // a new stamp always replaces the previous one
        assertEquals(300, index.nextDeadlineMs());
        IndexDueBatch batch = index.drainDue(300, 10);
        assertEquals(1, batch.size());
        index.onBatchCommitted(batch);
    }

    /**
     * The I4 recovery gate (design §4.2 pollDue "empty while recovering"; §4.4 item 5): an
     * ineligible shard's entries are undrainable and contribute no deadline, however overdue,
     * while the replay-side methods stay fully ungated.
     */
    @Test
    void ineligiblePartitionContributesNothingHoweverOverdue() {
        TrackerIndex index = new TrackerIndex();
        index.assignPartition(0);
        index.assignPartition(1);
        assertTrue(index.isDispatchEligible(0), "a fresh shard starts dispatch-eligible");

        index.setDispatchEligible(0, false); // M3 beginRecovery
        index.applyAdd(0, 0, 100, 0); // replay populates the gated shard with overdue entries
        index.applyAdd(0, 1, 150, 1);
        index.applyAdd(1, 0, 200, 0);
        assertTrue(index.applyComplete(0, 1), "replay-side applyComplete stays ungated");
        assertEquals(1, index.pendingCount(0), "pendingCount stays ungated (cursor math needs it)");
        long[] visited = {0};
        index.oldestPending(0, (slot, src, at, trk) -> {
            visited[0]++;
            return true;
        });
        assertEquals(1, visited[0], "oldestPending stays ungated (sidecar encoding needs it)");

        assertEquals(200, index.nextDeadlineMs(), "recovering shard drives no (zero) poll timeout");
        IndexDueBatch batch = index.drainDue(10_000, 10);
        assertEquals(1, batch.size(), "a recovering partition contributes nothing, however overdue");
        assertEquals(1, batch.sourcePartition(0));
        index.onBatchCommitted(batch);
        assertEquals(Long.MAX_VALUE, index.nextDeadlineMs(), "only the gated shard has entries left");

        index.setDispatchEligible(0, true); // promotion: position >= barrier (design §3.6 step 5)
        assertEquals(100, index.nextDeadlineMs());
        IndexDueBatch promoted = index.drainDue(10_000, 10);
        assertEquals(1, promoted.size());
        assertEquals(0, promoted.sourcePartition(0));
        index.onBatchCommitted(promoted);
        assertEquals(0, index.totalPendingCount());
    }

    @Test
    void eligibilityLifecycleFollowsTheShard() {
        TrackerIndex index = new TrackerIndex();
        assertFalse(index.isDispatchEligible(0), "an unassigned partition is not eligible");
        assertThrows(IllegalStateException.class, () -> index.setDispatchEligible(0, true));

        index.assignPartition(0);
        index.setDispatchEligible(0, false);
        assertFalse(index.isDispatchEligible(0));
        index.assignPartition(0); // idempotent re-assignment must not reset the gate
        assertFalse(index.isDispatchEligible(0));

        index.revokePartition(0);
        assertFalse(index.isDispatchEligible(0));
        index.assignPartition(0);
        assertTrue(index.isDispatchEligible(0), "a fresh shard after revoke starts eligible");
    }

    @Test
    void revokeDropsStateAndReassignStartsClean() {
        TrackerIndex index = new TrackerIndex();
        index.assignPartition(0);
        index.assignPartition(1);
        for (int i = 0; i < 100; i++) {
            index.applyAdd(0, i, 1_000 + i, i);
        }
        index.applyAdd(1, 0, 50, 0);
        assertEquals(101, index.totalPendingCount());

        index.revokePartition(0);
        assertFalse(index.isAssigned(0));
        assertEquals(1, index.assignedPartitionCount());
        assertEquals(0, index.pendingCount(0));
        assertEquals(1, index.totalPendingCount(), "other shards unaffected");
        assertEquals(50, index.nextDeadlineMs());
        assertThrows(IllegalStateException.class, () -> index.applyAdd(0, 200, 1, 200));

        index.assignPartition(0);
        assertTrue(index.isAssigned(0));
        assertEquals(0, index.pendingCount(0), "re-assignment starts clean");
        // The new shard accepts a fresh replay from the cursor — offsets may legitimately restart
        // below the old shard's tail because the old state is gone.
        assertTrue(index.applyAdd(0, 5, 2_000, 5));
        assertEquals(1, index.pendingCount(0));

        index.lostPartition(1);
        assertEquals(0, index.pendingCount(1));
        assertEquals(1, index.totalPendingCount());
    }

    @Test
    void assignmentIsIdempotent() {
        TrackerIndex index = new TrackerIndex();
        index.assignPartition(7);
        index.applyAdd(7, 0, 100, 0);
        index.assignPartition(7); // re-assignment of a live partition is a no-op
        assertEquals(1, index.pendingCount(7));
        assertEquals(1, index.assignedPartitionCount());
    }

    @Test
    void batchResolutionForARevokedPartitionIsANoOp() {
        TrackerIndex index = new TrackerIndex();
        index.assignPartition(0);
        index.assignPartition(1);
        index.applyAdd(0, 0, 100, 0);
        index.applyAdd(1, 0, 100, 0);
        IndexDueBatch batch = index.drainDue(100, 10);
        assertEquals(2, batch.size());

        index.revokePartition(0); // I9-style drop while the batch is in flight
        index.onBatchCommitted(batch); // partition 0's entry is silently skipped
        assertEquals(0, index.totalInFlightCount());
        assertEquals(0, index.totalPendingCount());
    }

    @Test
    void emptyAndUnassignedIndexBehaves() {
        TrackerIndex index = new TrackerIndex();
        assertEquals(Long.MAX_VALUE, index.nextDeadlineMs());
        assertEquals(0, index.drainDue(Long.MAX_VALUE, 10).size());
        assertEquals(0, index.totalPendingCount());
        assertEquals(0, index.pendingCount(3), "unassigned partition reports zero pending");
        assertThrows(IllegalStateException.class, () -> index.applyAdd(3, 0, 1, 0));
        assertThrows(IllegalStateException.class, () -> index.applyComplete(3, 0));
    }

    @Test
    void maxBatchTruncatesAcrossPartitions() {
        TrackerIndex index = new TrackerIndex();
        index.assignPartition(0);
        index.assignPartition(1);
        for (int i = 0; i < 10; i++) {
            index.applyAdd(i % 2, i / 2, 100 + i, i / 2);
        }
        IndexDueBatch batch = index.drainDue(1_000, 4);
        assertEquals(4, batch.size());
        for (int i = 0; i < 4; i++) {
            assertEquals(100 + i, batch.dispatchAtMs(i), "global deadline order under truncation");
        }
        index.onBatchAborted(batch);
        assertEquals(10, index.totalPendingCount(), "aborted batch restored");
        IndexDueBatch all = index.drainDue(1_000, 100);
        assertEquals(10, all.size());
        index.onBatchCommitted(all);
    }

    @Test
    void aggregatedCountersSumOverShards() {
        TrackerIndex index = new TrackerIndex(1, 1);
        index.assignPartition(0);
        index.assignPartition(1);
        index.applyAdd(0, 0, 100, 0);
        index.applyAdd(0, 0, 200, 1); // duplicate ADD → anomaly on shard 0
        index.applyAdd(1, 0, 100, 0);
        index.applyAdd(1, 0, 50, 1); // duplicate ADD → anomaly on shard 1
        assertEquals(2, index.anomalies());
        assertTrue(index.staleHeapEntries() >= 2);
        index.maintenance();
        assertTrue(index.heapRebuilds() >= 1);
        assertTrue(index.estimatedRetainedBytes() > 0);
    }
}
