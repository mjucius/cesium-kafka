package events.cesium.kafka.store.tracker;

import static events.cesium.kafka.store.tracker.StoreFixtures.EMPTY_BATCH;
import static events.cesium.kafka.store.tracker.StoreFixtures.feedComplete;
import static events.cesium.kafka.store.tracker.StoreFixtures.feedSchedule;
import static events.cesium.kafka.store.tracker.StoreFixtures.startedStore;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.api.store.DueBatch;
import events.cesium.kafka.api.store.ScheduledRef;
import events.cesium.kafka.api.store.TrackerCursor;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Recovery protocol units (design §3.5/§3.6, §4.4 items 5–6): sidecar seeding order and offsets,
 * the I4 gate while replaying, the barrier flip, idempotent re-recovery, and the fail-fast
 * posture for corrupted or mismatched committed cursors.
 */
class KafkaTrackerStoreRecoveryTest {

    private static final long T0 = 1_000_000;

    /** Runs a first store through a partial cycle and returns its committed cursor. */
    private TrackerCursor cursorFromFirstIncarnation() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = startedStore(ctx);
        store.onPartitionsAssigned(Set.of(0));
        store.beginRecovery(0, new TrackerCursor(0, ""), 0);

        feedSchedule(store, 0, new ScheduledRef(0, 10, T0 + 10, false));
        feedSchedule(store, 1, new ScheduledRef(0, 11, T0 + 20, true)); // clamped long-delay pin
        feedSchedule(store, 2, new ScheduledRef(0, 12, T0 + 900_000, false));
        feedSchedule(store, 3, new ScheduledRef(0, 13, T0 + 900_001, true));
        feedSchedule(store, 4, new ScheduledRef(0, 14, T0 + 900_002, false));

        DueBatch batch = store.pollDue(T0 + 100, 10); // src 10 and 11 are due
        assertEquals(2, batch.size());
        store.onBatchCommitted(batch);
        feedComplete(store, 0, 5, 10); // own COMPLETE echoes
        feedComplete(store, 0, 6, 11);

        TrackerCursor cursor = store.committedCursor(0, EMPTY_BATCH);
        store.onBatchCommitted(EMPTY_BATCH);
        assertEquals(7, cursor.offset(), "all pending fit: position-tracking cursor");
        assertEquals(3, SidecarCodec.decode(cursor.metadata()).entries().size());
        store.close();
        return cursor;
    }

    @Test
    void sidecarSeedingReplayRangeAndBarrierFlip() {
        TrackerCursor committed = cursorFromFirstIncarnation();

        // New incarnation (e.g. after a rebalance): fresh store, same route identity.
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = startedStore(ctx);
        store.onPartitionsAssigned(Set.of(0));
        assertTrue(store.isRecovering(0), "ASSIGNED shards are gated before recovery begins");
        assertEquals(0, store.pollDue(Long.MAX_VALUE - 1, 10).size(), "I4: gated while ASSIGNED");

        store.beginRecovery(0, committed, 11); // barrier: replay [7, 11)
        assertTrue(store.isRecovering(0));
        assertEquals(3, store.pendingCount(0), "sidecar entries are seeded FIRST");
        assertEquals(0, store.pollDue(Long.MAX_VALUE - 1, 10).size(), "I4: gated while RECOVERING, however overdue");
        assertEquals(Long.MAX_VALUE, store.nextDeadlineMs(), "a recovering shard drives no poll timeout");

        // Replay range: a new ADD, a COMPLETE for a seeded entry, another ADD, an echo no-op.
        feedSchedule(store, 7, new ScheduledRef(0, 20, T0 + 50, false));
        assertTrue(store.isRecovering(0));
        feedComplete(store, 0, 8, 12); // seeded entry settles during replay (R2 removes it)
        feedSchedule(store, 9, new ScheduledRef(0, 21, T0 + 60, true));
        feedComplete(store, 0, 10, 999); // COMPLETE-without-ADD: silent no-op
        assertFalse(store.isRecovering(0), "position 11 reached the barrier: ACTIVE");
        assertEquals(4, store.pendingCount(0));

        DueBatch batch = store.pollDue(Long.MAX_VALUE - 1, 10);
        assertEquals(4, batch.size());
        // Due order: 20@T0+50, 21@T0+60, then the seeded pins 13@T0+900_001, 14@T0+900_002.
        assertEquals(20, batch.sourceOffset(0));
        assertEquals(21, batch.sourceOffset(1));
        assertTrue(batch.clamped(1));
        assertEquals(13, batch.sourceOffset(2));
        assertTrue(batch.clamped(2), "the clamped bit survives the sidecar round trip");
        assertEquals(3, batch.trackerOffset(2), "seeded entries keep their ORIGINAL tracker ADD offsets");
        assertEquals(14, batch.sourceOffset(3));
        assertFalse(batch.clamped(3));
        store.onBatchCommitted(batch);
        assertEquals(0, store.pendingCount(0));
    }

    @Test
    void commitMarkerBelowTheBarrierPromotesViaThePositionReport() {
        // Regression (M3 review, critical): every tracker write is transactional (I1), so the
        // offsets directly below the HW barrier are almost always the previous transaction's
        // commit marker -- never delivered to a read_committed consumer. The engine reports
        // consumer.position() through onTrackerPosition; the store must promote on it instead of
        // waiting forever for a delivered record at the barrier (the idle-partition wedge).
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = startedStore(ctx);
        store.onPartitionsAssigned(Set.of(0));

        // Durable log: ADD@0 (committed), commit-marker@1 (undelivered). HW = barrier = 2.
        store.beginRecovery(0, new TrackerCursor(0, ""), 2);
        feedSchedule(store, 0, new ScheduledRef(0, 10, T0, false));
        assertTrue(store.isRecovering(0), "record-derived position is 1, the barrier is 2");
        assertEquals(0, store.pollDue(T0 + 1_000_000, 10).size(), "the overdue entry stays gated (I4)");
        assertEquals(Long.MAX_VALUE, store.nextDeadlineMs(), "and drives no wakeup");

        store.onTrackerPosition(0, 2); // the consumer's position passed the marker
        assertFalse(store.isRecovering(0), "the reported position reached the barrier: ACTIVE");
        DueBatch batch = store.pollDue(T0 + 1_000_000, 10);
        assertEquals(1, batch.size());
        assertEquals(10, batch.sourceOffset(0));
        store.onBatchCommitted(batch);

        store.onTrackerPosition(0, 1); // position reports are a high-water mark
        assertFalse(store.isRecovering(0), "a stale, lower report must never regress the shard");
    }

    @Test
    void beginRecoveryIsIdempotentAndConverges() {
        TrackerCursor committed = cursorFromFirstIncarnation();
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = startedStore(ctx);
        store.onPartitionsAssigned(Set.of(0));

        store.beginRecovery(0, committed, 11);
        feedSchedule(store, 7, new ScheduledRef(0, 20, T0 + 50, false)); // crash mid-replay (D-6)...
        assertEquals(4, store.pendingCount(0));

        store.beginRecovery(0, committed, 11); // ...recovery re-runs from the same cursor
        assertEquals(3, store.pendingCount(0), "re-seeding converges to the same pending set");
        assertTrue(store.isRecovering(0));

        feedSchedule(store, 7, new ScheduledRef(0, 20, T0 + 50, false));
        feedComplete(store, 0, 8, 12);
        feedSchedule(store, 9, new ScheduledRef(0, 21, T0 + 60, true));
        feedComplete(store, 0, 10, 999);
        assertFalse(store.isRecovering(0));
        assertEquals(4, store.pendingCount(0), "identical converged pending set after the re-run");
    }

    @Test
    void barrierAtOrBelowCursorPromotesImmediately() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = startedStore(ctx);
        store.onPartitionsAssigned(Set.of(0));
        store.beginRecovery(0, new TrackerCursor(5, ""), 5); // barrier <= cursor: nothing to replay
        assertFalse(store.isRecovering(0));
    }

    @Test
    void identityMismatchFailsFast() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = startedStore(ctx);
        store.onPartitionsAssigned(Set.of(0));

        // Same topic ids, different cluster.
        String wrongCluster = SidecarCodec.encoder(
                        "other-cluster",
                        FixedIdentityStoreContext.SOURCE_TOPIC_ID,
                        FixedIdentityStoreContext.TRACKER_TOPIC_ID,
                        3072)
                .finish(5)
                .metadata();
        IllegalStateException cluster = assertThrows(
                IllegalStateException.class, () -> store.beginRecovery(0, new TrackerCursor(5, wrongCluster), 10));
        assertTrue(cluster.getMessage().contains("identity mismatch"), cluster.getMessage());

        // Recreated source topic: a different source topic id.
        String wrongSource = SidecarCodec.encoder(
                        FixedIdentityStoreContext.CLUSTER_ID,
                        FixedIdentityStoreContext.DESTINATION_TOPIC_ID,
                        FixedIdentityStoreContext.TRACKER_TOPIC_ID,
                        3072)
                .finish(5)
                .metadata();
        assertThrows(IllegalStateException.class, () -> store.beginRecovery(0, new TrackerCursor(5, wrongSource), 10));

        // Recreated tracker topic: a different tracker topic id.
        String wrongTracker = SidecarCodec.encoder(
                        FixedIdentityStoreContext.CLUSTER_ID,
                        FixedIdentityStoreContext.SOURCE_TOPIC_ID,
                        FixedIdentityStoreContext.DESTINATION_TOPIC_ID,
                        3072)
                .finish(5)
                .metadata();
        assertThrows(IllegalStateException.class, () -> store.beginRecovery(0, new TrackerCursor(5, wrongTracker), 10));
    }

    @Test
    void corruptedCursorsFailFastNeverSilentlyIgnored() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = startedStore(ctx);
        store.onPartitionsAssigned(Set.of(0));

        // Unknown sidecar version.
        assertThrows(IllegalArgumentException.class, () -> store.beginRecovery(0, new TrackerCursor(5, "AgE"), 10));
        // Garbage metadata.
        assertThrows(
                IllegalArgumentException.class,
                () -> store.beginRecovery(0, new TrackerCursor(5, "!!not-base64!!"), 10));

        // Structurally valid sidecar whose seeded entry violates I5 (entry offset >= cursor).
        SidecarCodec.Encoder encoder = SidecarCodec.encoder(
                FixedIdentityStoreContext.CLUSTER_ID,
                FixedIdentityStoreContext.SOURCE_TOPIC_ID,
                FixedIdentityStoreContext.TRACKER_TOPIC_ID,
                3072);
        assertTrue(encoder.offer(7, 100, T0, false));
        String metadata = encoder.finish(5).metadata();
        IllegalStateException corrupt = assertThrows(
                IllegalStateException.class, () -> store.beginRecovery(0, new TrackerCursor(5, metadata), 10));
        assertTrue(corrupt.getMessage().contains("not below the cursor offset"), corrupt.getMessage());
    }

    @Test
    void firstRunWithEmptyMetadataSkipsSeedingAndIdentityCheck() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = startedStore(ctx);
        store.onPartitionsAssigned(Set.of(0));
        store.beginRecovery(0, new TrackerCursor(0, ""), 0); // §3.6 explicit first-run seek shape
        assertFalse(store.isRecovering(0));
        assertEquals(0, store.pendingCount(0));

        // The first committed cursor then binds the identity for all future incarnations.
        TrackerCursor first = store.committedCursor(0, EMPTY_BATCH);
        SidecarCodec.DecodedSidecar decoded = SidecarCodec.decode(first.metadata());
        assertEquals(FixedIdentityStoreContext.CLUSTER_ID, decoded.clusterId());
        assertEquals(FixedIdentityStoreContext.SOURCE_TOPIC_ID, decoded.sourceTopicId());
        assertEquals(FixedIdentityStoreContext.TRACKER_TOPIC_ID, decoded.trackerTopicId());
    }

    @Test
    void activeShardsKeepDispatchingWhileOthersRecover() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(2);
        KafkaTrackerStore store = startedStore(ctx);
        store.onPartitionsAssigned(Set.of(0, 1));
        store.beginRecovery(0, new TrackerCursor(0, ""), 0); // partition 0 immediately ACTIVE
        store.beginRecovery(1, new TrackerCursor(0, ""), 100); // partition 1 replays to barrier 100

        feedSchedule(store, 0, new ScheduledRef(0, 10, T0, false));
        feedSchedule(store, 0, new ScheduledRef(1, 10, T0, false)); // overdue but RECOVERING

        DueBatch batch = store.pollDue(T0 + 10, 10);
        assertEquals(1, batch.size(), "no global recovery gate: per-partition I4 only");
        assertEquals(0, batch.sourcePartition(0));
        store.onBatchCommitted(batch);
        assertTrue(store.isRecovering(1));
        assertEquals(1, store.pendingCount(1), "the recovering shard's pending state is intact");
    }
}
