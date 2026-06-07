package com.jucius.cesium.kafka.store.tracker;

import static com.jucius.cesium.kafka.store.tracker.StoreFixtures.feedSchedule;
import static com.jucius.cesium.kafka.store.tracker.StoreFixtures.startedStore;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.store.DueBatch;
import com.jucius.cesium.kafka.api.store.ScheduledRef;
import com.jucius.cesium.kafka.api.store.TrackerCursor;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Security finding H1: a recovery whose durable backlog exceeds the heap budget must fail in a
 * <em>controlled, attributable</em> way (a clear cause + the {@code cesium_recovery_over_budget}
 * metric + a dedicated exception), <strong>not</strong> with an uncontrolled
 * {@link OutOfMemoryError} crash-loop — while a within-budget recovery is unaffected.
 *
 * <p>The store is configured with a deliberately tiny heap budget so the resident-pending ceiling
 * ({@code heap-budget-bytes / }{@value KafkaTrackerStore#WORST_CASE_BYTES_PER_ENTRY}{@code  B}) is a
 * handful of entries — proving the trip without allocating gigabytes (design §5.3/§5.4).
 */
class KafkaTrackerStoreRecoveryBudgetTest {

    private static final long T0 = 1_000_000;

    /** A {@code heap-budget-bytes} of {@code 64 * n} yields a resident-pending ceiling of exactly {@code n}. */
    private static FixedIdentityStoreContext contextWithCeiling(int ceilingEntries) {
        long heapBudget = KafkaTrackerStore.WORST_CASE_BYTES_PER_ENTRY * ceilingEntries;
        return new FixedIdentityStoreContext(
                1,
                Map.of(
                        // Tiny budget => tiny ceiling. The validator's worst-case footprint check
                        // (partitions x max-pending-per-partition x 64 B) must still pass, so cap the
                        // per-partition max at the ceiling: 1 x ceiling x 64 == heapBudget, not over.
                        KafkaTrackerStore.HEAP_BUDGET_BYTES_KEY,
                        Long.toString(heapBudget),
                        KafkaTrackerStore.MAX_PENDING_PER_PARTITION_KEY,
                        Long.toString(ceilingEntries)));
    }

    private static double overBudgetCount(FixedIdentityStoreContext ctx) {
        return ctx.registry()
                .get(KafkaTrackerStore.RECOVERY_OVER_BUDGET_METRIC)
                .counter()
                .count();
    }

    @Test
    void replayOverHeapBudgetFailsControlledNotOom() {
        FixedIdentityStoreContext ctx = contextWithCeiling(10);
        KafkaTrackerStore store = startedStore(ctx);
        store.onPartitionsAssigned(Set.of(0));
        // Barrier far above the replay range, so the shard stays RECOVERING throughout (the uncapped
        // path the H1 bug exploits — a RECOVERING shard is never backpressure-paused).
        store.beginRecovery(0, new TrackerCursor(0, ""), 1_000_000);

        // Replay exactly the ceiling's worth of distinct ADDs — all admitted.
        for (int i = 0; i < 10; i++) {
            feedSchedule(store, i, new ScheduledRef(0, 100 + i, T0 + i, false));
        }
        assertEquals(10, store.pendingCount(0), "the ceiling's worth of entries replay fine");
        assertTrue(store.isRecovering(0), "still below the barrier");
        assertEquals(0.0, overBudgetCount(ctx), "no trip while within budget");

        // The next replayed ADD would push resident pending over the ceiling: controlled fail.
        RecoveryHeapBudgetExceededException thrown = assertThrows(
                RecoveryHeapBudgetExceededException.class,
                () -> feedSchedule(store, 10, new ScheduledRef(0, 110, T0 + 10, false)));
        assertTrue(thrown.getMessage().contains("heap budget"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains("H1"), thrown.getMessage());

        // The cause is unmistakable (metric), nothing leaked into the index, and the shard was NOT
        // promoted before its barrier (I4 preserved): the durable log stays authoritative.
        assertEquals(1.0, overBudgetCount(ctx), "the controlled-fail metric fired exactly once");
        assertEquals(10, store.pendingCount(0), "the over-budget ADD was never applied");
        assertTrue(store.isRecovering(0), "still RECOVERING — never promoted ACTIVE on the failure path");
        store.close();
    }

    @Test
    void sidecarSeedOverHeapBudgetAlsoFailsControlled() {
        FixedIdentityStoreContext ctx = contextWithCeiling(3);
        KafkaTrackerStore store = startedStore(ctx);
        store.onPartitionsAssigned(Set.of(0));

        // A committed cursor whose sidecar pins more entries than the heap budget admits.
        SidecarCodec.Encoder encoder = SidecarCodec.encoder(
                FixedIdentityStoreContext.CLUSTER_ID,
                FixedIdentityStoreContext.SOURCE_TOPIC_ID,
                FixedIdentityStoreContext.TRACKER_TOPIC_ID,
                3072);
        for (int i = 0; i < 5; i++) {
            assertTrue(encoder.offer(i, 100 + i, T0 + i, false));
        }
        String metadata = encoder.finish(5).metadata();

        RecoveryHeapBudgetExceededException thrown = assertThrows(
                RecoveryHeapBudgetExceededException.class,
                () -> store.beginRecovery(0, new TrackerCursor(5, metadata), 1_000));
        assertTrue(thrown.getMessage().contains("heap budget"), thrown.getMessage());
        assertEquals(1.0, overBudgetCount(ctx), "the seed path trips the same controlled fail");
        assertEquals(3, store.pendingCount(0), "seeding stopped exactly at the ceiling");
        store.close();
    }

    @Test
    void withinBudgetRecoveryIsUnaffectedAndPromotes() {
        FixedIdentityStoreContext ctx = contextWithCeiling(10);
        KafkaTrackerStore store = startedStore(ctx);
        store.onPartitionsAssigned(Set.of(0));
        // Barrier == 10: the tenth replayed record carries position to the barrier and promotes.
        store.beginRecovery(0, new TrackerCursor(0, ""), 10);

        for (int i = 0; i < 10; i++) {
            feedSchedule(store, i, new ScheduledRef(0, 100 + i, T0 + i, false));
        }
        assertFalse(store.isRecovering(0), "position reached the barrier: ACTIVE");
        assertEquals(10, store.pendingCount(0));
        assertEquals(0.0, overBudgetCount(ctx), "a within-budget recovery never trips the guard");

        DueBatch batch = store.pollDue(T0 + 100, 100);
        assertEquals(10, batch.size(), "all entries are dispatch-eligible after promotion");
        store.onBatchCommitted(batch);

        // Once ACTIVE the live path is governed by dispatch-side backpressure, NOT the store's hard
        // recovery ceiling: an ADD that pushes resident pending over the ceiling must NOT fail-fast
        // here (that case is the dispatch loop's pause/resume, §5.3) — proving the gating is correct.
        for (int i = 10; i < 22; i++) {
            feedSchedule(store, i, new ScheduledRef(0, 100 + i, T0 + 10_000 + i, false));
        }
        assertEquals(12, store.pendingCount(0), "ACTIVE-path ADDs above the ceiling are not store-rejected");
        assertEquals(0.0, overBudgetCount(ctx), "the recovery guard never fires on the live path");
        store.close();
    }
}
