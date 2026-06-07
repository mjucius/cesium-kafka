package com.jucius.cesium.kafka.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.store.CompletionReason;
import com.jucius.cesium.kafka.api.store.DueBatch;
import com.jucius.cesium.kafka.api.store.ScheduledRef;
import com.jucius.cesium.kafka.api.store.StoreContext;
import com.jucius.cesium.kafka.api.store.TrackerBackedStore;
import com.jucius.cesium.kafka.api.store.TrackerCursor;
import com.jucius.cesium.kafka.api.store.TrackerRecordData;
import com.jucius.cesium.kafka.testkit.TrackerStoreHarness.PendingEntry;
import com.jucius.cesium.kafka.testkit.TrackerStoreHarness.ReasonGroup;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Executable specification for {@link TrackerBackedStore} implementations (design §11.2, enforcing
 * the §4.4 correctness checklist). Subclass with a factory and run: every test and property here
 * must pass for a conforming store.
 *
 * <p><strong>How to use.</strong> Implement {@link #createStore(StoreContext)} to return a
 * configured, validated, started instance; override {@link #storeProperties()} for any
 * configuration your store requires, and {@link #overflowForcingProperties()} to unlock the
 * sidecar-overflow test. The contract builds {@link FakeStoreContext}s (random route identity per
 * context — pass an existing {@code route()} through the builder to model a restart of the same
 * route, which {@link TrackerStoreHarness#migrateTo} does for you).
 *
 * <p><strong>What is asserted — and deliberately no more.</strong> Drain order is asserted exactly
 * as specified: within a partition ({@code dispatchAtMs}, then arrival); across partitions,
 * effective-deadline order (which collapses to non-decreasing {@code dispatchAtMs} when no
 * penalty stamps are involved); cross-partition <em>ties</em> are unspecified and never asserted.
 * Replay rule R1 is asserted as written ({@code dispatchAtMs} updated, original
 * {@code trackerAddOffset} kept) without constraining how a store treats other schedule-payload
 * markers on the duplicate.
 *
 * <p><strong>Engine obligations modelled by the harness</strong> (documented here because a store
 * is entitled to rely on them): one transaction at a time (I3); {@code committedCursor} requested
 * only for cursors riding the very next commit; resolution callbacks only for <em>definitive</em>
 * outcomes — after an in-doubt commit neither {@code onBatchCommitted} nor
 * {@code onBatchAborted} is ever invoked (I9), the partitions are dropped and re-recovered
 * instead; completion echoes are consumed only after their commit ({@code read_committed});
 * records are fed through the headers-carrying {@code onTrackerRecord} variant and the consumer
 * position is reported via {@code onTrackerPosition} — across the <em>undelivered</em> offsets
 * (transaction control records, aborted batches) every transactional tracker log carries (I1),
 * which the harness models after every committed transaction. Truncate-and-carry-over commits
 * (§3.2/§7) settle a strict sub-batch view and restore the remainder; the store must honor the
 * {@code committedCursor} parameter, not its in-flight state.
 *
 * <p>The {@code @Tag("soak")} test is excluded from default test lanes by the build conventions
 * and routed to a dedicated {@code soakTest} task (design §11.2); kit consumers should do the
 * same, or tune {@link #soakEntryCount()} / {@link #soakMemoryCeilingBytes()}.
 */
public abstract class TrackerBackedStoreContract {

    /** Base instant for all virtual time in the contract. */
    protected static final long T0 = 1_700_000_000_000L;

    /**
     * Cluster id used by {@link #overflowFallbackEqualsTheMinPendingWatermark()}: long enough
     * (60 bytes) that a store binding identity into its metadata blob has almost no entry room
     * left under the smallest budget {@link #overflowForcingProperties()} configures.
     */
    protected static final String OVERFLOW_CLUSTER_ID = "o".repeat(60);

    // ----------------------------------------------------------------------------------------
    // Implementer surface
    // ----------------------------------------------------------------------------------------

    /**
     * Creates a ready-to-use store: {@code configure(context) → validate() → start()} already
     * performed. Called many times per test run; instances must be independent.
     */
    protected abstract TrackerBackedStore createStore(StoreContext context);

    /** {@code store.properties} merged into every context the contract builds. */
    protected Map<String, String> storeProperties() {
        return Map.of();
    }

    /**
     * Properties that bound the committed-cursor metadata budget so tightly that not even one
     * pinned entry fits (the contract pairs them with {@link #OVERFLOW_CLUSTER_ID}). Empty (the
     * default) skips the min-pending-watermark overflow test via an assumption — override it for
     * any store with a configurable sidecar budget.
     */
    protected Optional<Map<String, String>> overflowForcingProperties() {
        return Optional.empty();
    }

    /** Partition count of the routes the contract builds. */
    protected int partitionCount() {
        return 4;
    }

    /** Key bytes of a record violating the store's wire format (any sane format rejects these). */
    protected byte[] invalidRecordKey() {
        return new byte[] {0x00, 0x01, 0x02};
    }

    /** Value bytes of a record violating the store's wire format. */
    protected byte @Nullable [] invalidRecordValue() {
        return new byte[] {(byte) 0xDE, (byte) 0xAD};
    }

    /**
     * Reads the store's invalid-tracker-record counter (count-and-skip posture, design §2.2).
     * Defaults to the flagship store's meter name; override to match your store's metric.
     */
    protected double invalidRecordCount(MeterRegistry registry) {
        Counter counter = registry.find("cesium.tracker.invalid.records").counter();
        return counter == null ? 0.0 : counter.count();
    }

    /** Entry count of the {@code @Tag("soak")} test. */
    protected int soakEntryCount() {
        return 1_000_000;
    }

    /** Retained-heap ceiling asserted by the soak test after loading {@link #soakEntryCount()}. */
    protected long soakMemoryCeilingBytes() {
        return 256L << 20;
    }

    // ----------------------------------------------------------------------------------------
    // Encode / replay round trip, ordering, dueness
    // ----------------------------------------------------------------------------------------

    @Test
    public void scheduleRoundTripsThroughTheTrackerEncoding() {
        Fixture f = fixture(0, 1);
        long o0 = f.harness().schedule(ref(0, 100, T0 + 50));
        long o1 = f.harness().schedule(refClamped(0, 101, T0 + 10));
        long o2 = f.harness().schedule(ref(1, 7, T0 + 30));

        DueBatch batch = f.store().pollDue(T0 + 100, 10);
        assertEquals(3, batch.size());
        // Distinct deadlines: effective-deadline order is fully deterministic here.
        assertEntry(batch, 0, 0, 101, T0 + 10, o1, true);
        assertEntry(batch, 1, 1, 7, T0 + 30, o2, false);
        assertEntry(batch, 2, 0, 100, T0 + 50, o0, false);
        f.harness().commitDispatch(batch, CompletionReason.DISPATCHED);
    }

    @Test
    public void pollDueReturnsOnlyDueEntriesInDueOrder() {
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0 + 100));
        f.harness().schedule(ref(0, 2, T0 + 50));
        f.harness().schedule(ref(0, 3, T0 + 200));
        f.harness().schedule(ref(0, 4, T0 + 150));

        DueBatch due = f.store().pollDue(T0 + 100, 10);
        assertEquals(2, due.size(), "entries beyond nowMs are not due");
        assertEquals(2, due.sourceOffset(0));
        assertEquals(1, due.sourceOffset(1));
        f.harness().commitDispatch(due, CompletionReason.DISPATCHED);

        assertEquals(0, f.store().pollDue(T0 + 100, 10).size(), "nothing further is due yet");

        DueBatch capped = f.store().pollDue(T0 + 200, 1);
        assertEquals(1, capped.size(), "maxBatch bounds the pop");
        assertEquals(4, capped.sourceOffset(0), "the earliest deadline pops first");
        f.harness().commitDispatch(capped, CompletionReason.DISPATCHED);

        DueBatch rest = f.store().pollDue(T0 + 200, 10);
        assertEquals(1, rest.size());
        assertEquals(3, rest.sourceOffset(0));
        f.harness().commitDispatch(rest, CompletionReason.DISPATCHED);
    }

    @Test
    public void equalDeadlinesDrainInArrivalOrder() {
        Fixture f = fixture(0);
        long[] offsets = new long[5];
        for (int i = 0; i < 5; i++) {
            offsets[i] = f.harness().schedule(ref(0, 10 + i, T0)); // one same-millisecond burst
        }
        DueBatch batch = f.store().pollDue(T0, 10);
        assertEquals(5, batch.size());
        for (int i = 0; i < 5; i++) {
            assertEquals(10 + i, batch.sourceOffset(i), "ties break by arrival within a partition");
            assertEquals(offsets[i], batch.trackerOffset(i));
        }
        f.harness().commitDispatch(batch, CompletionReason.DISPATCHED);
    }

    @Test
    public void crossPartitionDrainFollowsEffectiveDeadlineOrder() {
        Fixture f = fixture(0, 1, 2);
        f.harness().schedule(ref(0, 1, T0 + 30));
        f.harness().schedule(ref(0, 2, T0 + 10));
        f.harness().schedule(ref(1, 1, T0 + 20));
        f.harness().schedule(ref(1, 2, T0 + 40));
        f.harness().schedule(ref(2, 1, T0 + 25));

        DueBatch batch = f.store().pollDue(T0 + 100, 10);
        assertEquals(5, batch.size());
        for (int i = 1; i < batch.size(); i++) {
            assertTrue(
                    batch.dispatchAtMs(i - 1) <= batch.dispatchAtMs(i),
                    "with no penalties, the cross-partition drain is deadline-ordered");
        }
        // Within partition 0 the (dispatchAtMs, arrival) order must hold as a subsequence.
        List<Long> partition0 = new ArrayList<>();
        for (int i = 0; i < batch.size(); i++) {
            if (batch.sourcePartition(i) == 0) {
                partition0.add(batch.sourceOffset(i));
            }
        }
        assertEquals(List.of(2L, 1L), partition0);
        f.harness().commitDispatch(batch, CompletionReason.DISPATCHED);
    }

    @Test
    public void clampedMarkerSurvivesTheDurableRoundTrip() {
        Fixture f = fixture(0);
        f.harness().schedule(refClamped(0, 1, T0));
        f.harness().schedule(ref(0, 2, T0));
        DueBatch batch = f.store().pollDue(T0, 10);
        assertEquals(2, batch.size());
        assertTrue(batch.clamped(0), "the CLAMP policy marker must survive ingest → tracker → index → batch");
        assertFalse(batch.clamped(1));
        f.harness().commitDispatch(batch, CompletionReason.DISPATCHED);
    }

    // ----------------------------------------------------------------------------------------
    // Replay rules (R1 / R2), invalid records, tombstone posture
    // ----------------------------------------------------------------------------------------

    @Test
    public void duplicateAddUpdatesTheScheduleButKeepsTheOriginalTrackerOffset() {
        Fixture f = fixture(0);
        long original = f.harness().schedule(ref(0, 10, T0 + 500_000));
        f.harness().schedule(ref(0, 11, T0 + 600_000));
        f.harness().schedule(ref(0, 10, T0 + 100)); // anomalous duplicate ADD, moved much earlier
        assertEquals(2, f.store().pendingCount(0), "a duplicate ADD must not create a second entry");

        DueBatch batch = f.store().pollDue(T0 + 200, 10);
        assertEquals(1, batch.size());
        assertEquals(10, batch.sourceOffset(0));
        assertEquals(T0 + 100, batch.dispatchAtMs(0), "R1: dispatchAtMs follows the duplicate");
        assertEquals(original, batch.trackerOffset(0), "I5: the ORIGINAL tracker ADD offset is kept");
        f.harness().commitDispatch(batch, CompletionReason.DISPATCHED);
    }

    @Test
    public void completionForAnUnknownKeyIsASilentNoOp() {
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 5, T0 + 100));
        assertEquals(1, f.store().pendingCount(0));

        // A tombstone for a key this store never saw — e.g. settled below the cursor, or the
        // asymmetric-compaction shape (§3.7). R2: silently tolerated, never an error.
        TrackerRecordData tombstone = encodeOneCompletion(f.store(), 0, 999, CompletionReason.DISPATCHED);
        f.harness().appendRecord(0, tombstone);
        assertEquals(1, f.store().pendingCount(0), "R2: complete-before-add must be a silent no-op");

        f.harness().schedule(ref(0, 6, T0 + 200)); // the shard stays fully functional
        DueBatch batch = f.store().pollDue(T0 + 1_000, 10);
        assertEquals(2, batch.size());
        f.harness().commitDispatch(batch, CompletionReason.DISPATCHED);
        assertEquals(0, f.store().pendingCount(0));
    }

    @Test
    public void completionTombstonesApplyRegardlessOfReasonMetadata() {
        // The correctness payload of a tombstone is "this key completed": even a reason constant
        // unknown to this store version must still settle the entry (skipping would re-dispatch a
        // completed key — a read_committed-visible duplicate). Reason metadata is informational.
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 5, T0 + 100));
        f.harness().schedule(ref(0, 6, T0 + 200));

        TrackerRecordData tombstone = encodeOneCompletion(f.store(), 0, 5, CompletionReason.REJECTED);
        f.harness().appendRecord(0, tombstone);
        assertEquals(1, f.store().pendingCount(0), "the tombstone must apply the completion");

        DueBatch batch = f.store().pollDue(T0 + 1_000, 10);
        assertEquals(1, batch.size());
        assertEquals(6, batch.sourceOffset(0));
        f.harness().commitDispatch(batch, CompletionReason.DISPATCHED);
    }

    @Test
    public void invalidRecordsAreCountedSkippedAndNeverBlockRecovery() {
        FakeStoreContext ctx = contextBuilder().build();
        TrackerBackedStore store = createStore(ctx);
        TrackerStoreHarness harness = new TrackerStoreHarness(store);
        harness.startPartition(0);

        double before = invalidRecordCount(ctx.meterRegistry());
        harness.schedule(ref(0, 1, T0 + 10));
        harness.appendRaw(0, invalidRecordKey(), invalidRecordValue());
        harness.schedule(ref(0, 2, T0 + 20));
        harness.appendRaw(0, invalidRecordKey(), invalidRecordValue());
        assertEquals(2, store.pendingCount(0), "invalid records are never applied");
        assertTrue(
                invalidRecordCount(ctx.meterRegistry()) >= before + 2,
                "invalid records must be counted (count-and-skip, design §2.2)");

        // Replaying across the same invalid records — one of them the LAST record below the
        // barrier — must still reach the barrier: position advances on invalid records too.
        TrackerBackedStore fresh =
                createStore(contextBuilder().route(ctx.route()).build());
        TrackerStoreHarness freshHarness = harness.migrateTo(fresh);
        freshHarness.startPartition(0);
        assertFalse(fresh.isRecovering(0), "an invalid record below the barrier must not wedge recovery");
        assertEquals(2, fresh.pendingCount(0));
    }

    @Test
    public void replayingTheSameRecordsTwiceLeavesThePendingSetUnchanged() {
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 10, T0 + 10));
        f.harness().schedule(ref(0, 11, T0 + 9_000_000)); // a pin that stays pending
        f.harness().schedule(ref(0, 12, T0 + 20));
        DueBatch due = f.store().pollDue(T0 + 100, 10);
        assertEquals(2, due.size());
        TrackerCursor committed = cursorOf(f.harness().commitDispatch(due, CompletionReason.DISPATCHED), 0);

        // New incarnation; crash mid-replay (D-6) re-runs recovery from the same cursor.
        Fixture next = sibling(f);
        TrackerBackedStore store = next.store();
        TrackerStoreHarness harness = next.harness();
        store.onPartitionsAssigned(Set.of(0));
        long barrier = harness.logEndOffset(0);

        store.beginRecovery(0, committed, barrier);
        harness.replayRange(0, committed.offset(), committed.offset() + 1); // partial, then "crash"
        store.beginRecovery(0, committed, barrier); // idempotent re-recovery, same cursor
        harness.replayRange(0, committed.offset(), barrier);
        assertFalse(store.isRecovering(0));
        long pendingAfterFirst = store.pendingCount(0);

        store.beginRecovery(0, committed, barrier); // and once more, fully
        harness.replayRange(0, committed.offset(), barrier);
        assertFalse(store.isRecovering(0));
        assertEquals(pendingAfterFirst, store.pendingCount(0), "recovery must converge, not accumulate");
        assertEquals(1, store.pendingCount(0));

        DueBatch pin = store.pollDue(Long.MAX_VALUE, 10);
        assertEquals(11, pin.sourceOffset(0));
        harness.commitDispatch(pin, CompletionReason.DISPATCHED);
    }

    // ----------------------------------------------------------------------------------------
    // Recovery gating (I4), barrier mechanics, R7
    // ----------------------------------------------------------------------------------------

    @Test
    public void recoveringPartitionContributesNothingUntilTheBarrier() {
        Fixture f = fixture(0);
        // Three ingest transactions: the log is ADD@0, marker@1, ADD@2, marker@3, ADD@4,
        // marker@5 — every committed transaction leaves a control record (I1).
        f.harness().schedule(ref(0, 10, T0 - 5_000)); // already overdue
        f.harness().schedule(ref(0, 11, T0 - 4_000));
        f.harness().schedule(ref(0, 12, T0 - 3_000));

        Fixture next = sibling(f);
        TrackerBackedStore store = next.store();
        store.onPartitionsAssigned(Set.of(0));
        TrackerCursor cursor = next.harness().committedCursor(0);
        long barrier = next.harness().logEndOffset(0);
        assertEquals(6, barrier, "the barrier (HW) includes the trailing commit marker");
        store.beginRecovery(0, cursor, barrier);

        assertTrue(store.isRecovering(0));
        assertEquals(0, store.pollDue(T0 + 60_000, 100).size(), "I4: a recovering partition is empty, however overdue");
        assertEquals(Long.MAX_VALUE, store.nextDeadlineMs(), "a recovering shard drives no poll timeout");

        next.harness().replayRange(0, 0, 2); // partial replay: still below the barrier
        assertTrue(store.isRecovering(0));
        assertEquals(0, store.pollDue(T0 + 60_000, 100).size());
        assertEquals(Long.MAX_VALUE, store.nextDeadlineMs());

        next.harness().replayRange(0, 2, 5); // every remaining RECORD, but not the trailing marker
        assertTrue(store.isRecovering(0), "all records delivered, position below the barrier: still gated");
        assertEquals(0, store.pollDue(T0 + 60_000, 100).size());

        next.harness().replayRange(0, 5, 6); // the trailing marker: position-report only
        assertFalse(store.isRecovering(0), "the reported position reached the barrier: ACTIVE");
        assertEquals(T0 - 5_000, store.nextDeadlineMs());
        DueBatch batch = store.pollDue(T0, 100);
        assertEquals(3, batch.size());
        next.harness().commitDispatch(batch, CompletionReason.DISPATCHED);
    }

    @Test
    public void recoveryGateIsPerPartition() {
        Fixture f = fixture(0, 1);
        f.harness().schedule(ref(1, 50, T0 - 1_000));
        f.harness().schedule(ref(1, 51, T0 - 900));

        Fixture next = sibling(f);
        TrackerBackedStore store = next.store();
        TrackerStoreHarness harness = next.harness();
        harness.startPartition(0); // empty log: immediately ACTIVE
        assertFalse(store.isRecovering(0));

        store.onPartitionsAssigned(Set.of(1));
        long barrier = harness.logEndOffset(1); // ADD@0, marker@1, ADD@2, marker@3
        store.beginRecovery(1, harness.committedCursor(1), barrier);
        harness.replayRange(1, 0, 1); // partial: partition 1 stays gated
        assertTrue(store.isRecovering(1));

        harness.schedule(ref(0, 7, T0 - 500)); // live entry on the ACTIVE partition
        DueBatch batch = store.pollDue(T0, 100);
        assertEquals(1, batch.size(), "no global gate: ACTIVE partitions dispatch while others recover");
        assertEquals(0, batch.sourcePartition(0));
        assertEquals(7, batch.sourceOffset(0));
        harness.commitDispatch(batch, CompletionReason.DISPATCHED);

        harness.replayRange(1, 1, barrier);
        assertFalse(store.isRecovering(1));
        DueBatch rest = store.pollDue(T0, 100);
        assertEquals(2, rest.size());
        harness.commitDispatch(rest, CompletionReason.DISPATCHED);
    }

    @Test
    public void recoveryCompletesWhenPendingVolumeIsHuge() {
        // R7: backpressure applies to ACTIVE shards only — a store must reach the barrier no
        // matter how much genuine pending state the replay accumulates.
        Fixture f = fixture(0);
        int entries = 30_000;
        for (int i = 0; i < entries; i++) {
            f.harness().schedule(ref(0, i, T0 + (i % 1_000)));
        }

        Fixture next = sibling(f);
        next.harness().startPartition(0); // full replay
        assertFalse(next.store().isRecovering(0), "recovery must complete through any pending volume");
        assertEquals(entries, next.store().pendingCount(0));

        DueBatch batch = next.store().pollDue(T0 + 2_000, 1_000);
        assertEquals(1_000, batch.size());
        next.harness().commitDispatch(batch, CompletionReason.DISPATCHED);
    }

    @Test
    public void trailingCommitMarkersBelowTheBarrierNeverWedgeRecovery() {
        // Every cesium tracker write is transactional (I1), so after ANY committed transaction
        // the highest offsets below the HW barrier are control records a read_committed consumer
        // never delivers. Promotion must ride the engine's onTrackerPosition reports across
        // them: a store that waits for a delivered record at the barrier wedges idle partitions
        // in RECOVERING forever (pollDue empty per I4, nextDeadlineMs MAX_VALUE — no wakeup).
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0));
        long pin = f.harness().schedule(ref(0, 2, T0 + 9_000_000));
        DueBatch due = f.store().pollDue(T0, 10);
        assertEquals(1, due.size());
        f.harness().commitDispatch(due, CompletionReason.DISPATCHED); // tombstone + trailing marker

        Fixture next = sibling(f);
        next.harness().startPartition(0); // no new traffic: only the position report can promote
        assertFalse(
                next.store().isRecovering(0),
                "the offsets directly below the barrier are undelivered control records — the"
                        + " store must promote on the reported consumer position, without new traffic");
        assertEquals(1, next.store().pendingCount(0));
        DueBatch rest = next.store().pollDue(Long.MAX_VALUE, 10);
        assertEquals(1, rest.size());
        assertEquals(2, rest.sourceOffset(0));
        assertEquals(pin, rest.trackerOffset(0));
        next.harness().commitDispatch(rest, CompletionReason.DISPATCHED);
    }

    @Test
    public void barrierAtOrBelowTheCursorPromotesImmediately() {
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 10, T0 + 9_000_000));
        TrackerCursor cursor = f.harness().commitIdle(0);

        Fixture next = sibling(f);
        next.store().onPartitionsAssigned(Set.of(0));
        next.store().beginRecovery(0, cursor, cursor.offset()); // barrier == cursor: nothing to replay
        assertFalse(next.store().isRecovering(0), "barrier ≤ cursor short-circuits to ACTIVE (§3.6 step 3)");
        assertEquals(1, next.store().pendingCount(0), "the sidecar seed alone is complete state");
    }

    // ----------------------------------------------------------------------------------------
    // Cursor invariants (I5), sidecar round trips, overflow fallback
    // ----------------------------------------------------------------------------------------

    @Test
    public void committedCursorOffsetsAreMonotonicPerPartition() {
        Fixture f = fixture(0);
        List<Long> committed = new ArrayList<>();
        for (int cycle = 0; cycle < 5; cycle++) {
            f.harness().schedule(ref(0, cycle * 2L, T0 + cycle));
            f.harness().schedule(ref(0, cycle * 2L + 1, T0 + 9_000_000 + cycle)); // one pin per cycle
            DueBatch due = f.store().pollDue(T0 + 1_000_000, 10);
            if (cycle == 2) {
                f.harness().abortDispatch(due); // an aborted transaction between commits
            } else {
                committed.add(cursorOf(f.harness().commitDispatch(due, CompletionReason.DISPATCHED), 0)
                        .offset());
            }
        }
        committed.add(f.harness().commitIdle(0).offset());
        for (int i = 1; i < committed.size(); i++) {
            assertTrue(
                    committed.get(i - 1) <= committed.get(i),
                    "I5: the committed cursor offset must be monotonic per partition, got " + committed);
        }
    }

    @Test
    public void committedCursorRoundTripsIntoAFreshStore() {
        Fixture f = fixture(0, 1);
        f.harness().schedule(ref(0, 100, T0 + 10));
        f.harness().schedule(refClamped(0, 101, T0 + 9_000_000));
        f.harness().schedule(ref(0, 102, T0 + 20));
        f.harness().schedule(ref(1, 7, T0 + 15));
        f.harness().schedule(ref(1, 8, T0 + 8_000_000));

        DueBatch due = f.store().pollDue(T0 + 1_000, 100);
        assertEquals(3, due.size());
        f.harness().commitDispatch(due, CompletionReason.DISPATCHED);
        f.harness().commitIdle(0);
        f.harness().commitIdle(1);

        List<PendingEntry> expected = f.harness().pendingSnapshot();
        assertEquals(2, expected.size());

        Fixture next = sibling(f);
        next.harness().startPartitions(0, 1);
        assertFalse(next.store().isRecovering(0));
        assertFalse(next.store().isRecovering(1));
        assertEquals(
                expected,
                next.harness().pendingSnapshot(),
                "cursor + sidecar + replay must reproduce the exact pending set (I5)");
    }

    @Test
    public void sidecarPinnedEntriesRecoverWithOriginalOffsetsAndClampBits() {
        Fixture f = fixture(0);
        long pin1 = f.harness().schedule(refClamped(0, 1, T0 + 9_000_000));
        long pin2 = f.harness().schedule(ref(0, 2, T0 + 9_500_000));
        f.harness().schedule(ref(0, 3, T0 + 10));
        f.harness().schedule(ref(0, 4, T0 + 20));
        f.harness().schedule(ref(0, 5, T0 + 30));
        long positionAtCommit = f.harness().logEndOffset(0);

        DueBatch due = f.store().pollDue(T0 + 100, 10);
        assertEquals(3, due.size());
        TrackerCursor cursor = cursorOf(f.harness().commitDispatch(due, CompletionReason.DISPATCHED), 0);
        assertEquals(positionAtCommit, cursor.offset(), "all pending encoded ⇒ position-tracking cursor (§3.5)");
        assertFalse(cursor.metadata().isEmpty(), "committed metadata must be self-describing (§4.4 item 1)");
        assertTrue(cursor.offset() > pin2, "the pins lie below the cursor: only the sidecar can carry them (I5)");

        Fixture next = sibling(f);
        next.harness().startPartition(0);
        assertEquals(2, next.store().pendingCount(0));
        DueBatch pins = next.store().pollDue(Long.MAX_VALUE, 10);
        assertEntry(pins, 0, 0, 1, T0 + 9_000_000, pin1, true);
        assertEntry(pins, 1, 0, 2, T0 + 9_500_000, pin2, false);
        next.harness().commitDispatch(pins, CompletionReason.DISPATCHED);
    }

    @Test
    public void overflowFallbackEqualsTheMinPendingWatermark() {
        Optional<Map<String, String>> overflowProps = overflowForcingProperties();
        Assumptions.assumeTrue(
                overflowProps.isPresent(),
                "store exposes no metadata-budget knob: overflow fallback not testable through the SPI");

        Map<String, String> forcing = overflowProps.orElseThrow();
        FakeStoreContext ctx = FakeStoreContext.builder()
                .partitions(1)
                .clusterId(OVERFLOW_CLUSTER_ID)
                .properties(storeProperties())
                .properties(forcing)
                .build();
        TrackerBackedStore store = createStore(ctx);
        TrackerStoreHarness harness = new TrackerStoreHarness(store);
        harness.startPartition(0);

        long oldest = harness.schedule(ref(0, 100, T0 + 10));
        long second = harness.schedule(ref(0, 101, T0 + 9_000_000));
        harness.schedule(ref(0, 102, T0 + 9_000_001));

        TrackerCursor cursor = harness.commitIdle(0);
        assertEquals(oldest, cursor.offset(), "nothing fits the sidecar: the cursor degenerates to min-pending");

        DueBatch due = store.pollDue(T0 + 100, 10);
        assertEquals(1, due.size());
        TrackerCursor advanced = cursorOf(harness.commitDispatch(due, CompletionReason.DISPATCHED), 0);
        assertEquals(second, advanced.offset(), "the cut advances to the first (= oldest) non-encoded pending entry");

        List<PendingEntry> expected = harness.pendingSnapshot();
        TrackerBackedStore fresh = createStore(FakeStoreContext.builder()
                .route(ctx.route())
                .properties(storeProperties())
                .properties(forcing)
                .build());
        TrackerStoreHarness freshHarness = harness.migrateTo(fresh);
        freshHarness.startPartition(0);
        assertEquals(expected, freshHarness.pendingSnapshot(), "the fallback cursor must still recover exactly");
    }

    @Test
    public void largePendingSetsRoundTripThroughRecovery() {
        Fixture f = fixture(0);
        int entries = 600; // larger than typical sidecar budgets: exercises seeding + replay mixes
        for (int i = 0; i < entries; i++) {
            f.harness().schedule(new ScheduledRef(0, i, T0 + 9_000_000 + i, i % 3 == 0));
        }
        f.harness().commitIdle(0);
        List<PendingEntry> expected = f.harness().pendingSnapshot();
        assertEquals(entries, expected.size());

        Fixture next = sibling(f);
        next.harness().startPartition(0);
        assertEquals(
                expected,
                next.harness().pendingSnapshot(),
                "overflow mode or not, recovery must reproduce the exact pending set");
    }

    @Test
    public void idleCommitsAdvanceThePositionTrackingCursor() {
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0));
        f.harness().schedule(ref(0, 2, T0));
        DueBatch batch = f.store().pollDue(T0, 10);
        TrackerCursor atDispatch = cursorOf(f.harness().commitDispatch(batch, CompletionReason.DISPATCHED), 0);

        TrackerCursor idle = f.harness().commitIdle(0);
        assertTrue(idle.offset() >= atDispatch.offset());
        assertEquals(
                f.harness().logEndOffset(0),
                idle.offset(),
                "nothing pending, echoes consumed: the idle cursor tracks the live position (§3.5)");

        Fixture next = sibling(f);
        next.harness().startPartition(0);
        assertFalse(next.store().isRecovering(0));
        assertEquals(0, next.store().pendingCount(0));
    }

    @Test
    public void recoveryFailsFastOnAForeignIdentityCursor() {
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0 + 9_000_000));
        TrackerCursor cursor = f.harness().commitIdle(0);
        assertFalse(cursor.metadata().isEmpty(), "metadata must carry identity material (R17, §4.4 item 1)");
        long barrier = f.harness().logEndOffset(0);

        // A context built independently models recreated topics / another cluster: same partition
        // numbers, different identity. The committed cursor must be refused, never replayed into.
        TrackerBackedStore foreign = createStore(contextBuilder().build());
        foreign.onPartitionsAssigned(Set.of(0));
        assertThrows(
                RuntimeException.class,
                () -> foreign.beginRecovery(0, cursor, barrier),
                "a cursor committed against a different identity must fail fast (§3.6, R-9/R-10)");
    }

    // ----------------------------------------------------------------------------------------
    // Transaction staging: commit / definitive abort / in-doubt (I9)
    // ----------------------------------------------------------------------------------------

    @Test
    public void stagedThenAbortedBatchesLeaveNoDurableTrace() {
        Fixture f = fixture(0);
        for (int i = 0; i < 4; i++) {
            f.harness().schedule(ref(0, i, T0));
        }
        DueBatch staged = f.store().pollDue(T0, 10);
        assertEquals(4, staged.size());
        ArrayDueBatch snapshot = ArrayDueBatch.snapshot(staged);
        // The engine legally encodes completions before the transaction fails.
        assertEquals(
                4,
                f.store()
                        .encodeCompletions(snapshot, CompletionReason.DISPATCHED)
                        .size());
        f.harness().abortDispatch(staged);
        assertEquals(4, f.store().pendingCount(0), "a definitive abort restores every entry to pending");

        DueBatch again = f.store().pollDue(T0, 10);
        assertEquals(4, again.size(), "restored entries are re-polled");
        f.harness().abortDispatch(again);

        List<PendingEntry> expected = f.harness().pendingSnapshot();
        Fixture next = sibling(f);
        next.harness().startPartition(0);
        assertEquals(expected, next.harness().pendingSnapshot(), "aborted effects must be invisible to recovery");
    }

    @Test
    public void committedBatchesAreDurablySettled() {
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0));
        f.harness().schedule(ref(0, 2, T0 + 9_000_000));
        f.harness().schedule(ref(0, 3, T0));
        f.harness().schedule(ref(0, 4, T0 + 9_500_000));

        DueBatch due = f.store().pollDue(T0, 10);
        assertEquals(2, due.size());
        f.harness().commitDispatch(due, CompletionReason.DISPATCHED);
        assertEquals(2, f.store().pendingCount(0));

        List<PendingEntry> expected = f.harness().pendingSnapshot();
        Fixture next = sibling(f);
        next.harness().startPartition(0);
        assertEquals(expected, next.harness().pendingSnapshot(), "committed completions must never resurface");
    }

    @Test
    public void perReasonSubBatchViewsEncodeCompletionsCorrectly() {
        Fixture f = fixture(0);
        for (int i = 1; i <= 4; i++) {
            f.harness().schedule(ref(0, i, T0));
        }
        DueBatch batch = f.store().pollDue(T0, 10);
        assertEquals(4, batch.size());

        // §3.2 mixes FOUND→DISPATCHED relays with GONE→PAYLOAD_MISSING_DLQ notices in one
        // transaction; the engine groups per reason and encodes each group separately. The store
        // must treat encodeCompletions as a pure function of ANY batch view.
        ArrayDueBatch relayed = ArrayDueBatch.select(batch, 0, 2);
        ArrayDueBatch lost = ArrayDueBatch.select(batch, 1, 3);
        assertEquals(
                2,
                f.store()
                        .encodeCompletions(relayed, CompletionReason.DISPATCHED)
                        .size(),
                "any sub-batch view must encode");
        f.harness()
                .commitDispatch(
                        batch,
                        List.of(
                                new ReasonGroup(relayed, CompletionReason.DISPATCHED),
                                new ReasonGroup(lost, CompletionReason.PAYLOAD_MISSING_DLQ)));
        assertEquals(0, f.store().pendingCount(0));

        Fixture next = sibling(f);
        next.harness().startPartition(0);
        assertEquals(0, next.store().pendingCount(0), "the concatenated groups settled the full batch durably");
    }

    @Test
    public void truncatedDispatchCommitPreservesCarryOverEntriesAcrossACrash() {
        // §3.2/§7 step 2c (D8) and D-8: the fetch byte budget (or a TRANSIENT exclusion) makes
        // the transaction settle only a strict subset of the drained batch; the carry-over
        // returns to pending. The committed cursor must treat as complete exactly the SETTLED
        // entries — a cursor derived from in-flight state instead drops the carry-over from
        // durable truth, and the first crash after the commit makes the loss permanent.
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0));
        f.harness().schedule(ref(0, 2, T0 + 10));
        long o3 = f.harness().schedule(ref(0, 3, T0 + 20));
        long o4 = f.harness().schedule(ref(0, 4, T0 + 30));

        DueBatch drained = f.store().pollDue(T0 + 100, 10);
        assertEquals(4, drained.size());
        // The fetch pass trips the byte budget after two entries: settle {1, 2}, carry over {3, 4}.
        f.harness().commitDispatchPartial(drained, new int[] {0, 1}, CompletionReason.DISPATCHED);

        assertEquals(2, f.store().pendingCount(0), "the carry-over returns to pending");
        List<PendingEntry> live = f.harness().pendingSnapshot();
        assertEquals(
                List.of(new PendingEntry(0, 3, T0 + 20, o3, false), new PendingEntry(0, 4, T0 + 30, o4, false)),
                live,
                "the carry-over keeps its original tracker ADD offsets (I5)");

        // Crash: memory dies; only the committed cursor and the log survive — and so must the
        // carry-over (every unsettled entry >= cursor offset or in the sidecar).
        Fixture next = sibling(f);
        next.harness().startPartition(0);
        assertEquals(
                live,
                next.harness().pendingSnapshot(),
                "carry-over entries must survive crash + recovery — losing them violates I5"
                        + " against durable truth at the moment of commit");
    }

    @Test
    public void inDoubtCommitThatActuallyCommittedConvergesByReplayNeverByRestore() {
        Fixture f = fixture(0);
        for (int i = 1; i <= 3; i++) {
            f.harness().schedule(ref(0, i, T0));
        }
        long future = f.harness().schedule(ref(0, 4, T0 + 9_000_000));

        DueBatch batch = f.store().pollDue(T0, 10);
        assertEquals(3, batch.size());
        // commitTransaction outcome ambiguous; the broker DID commit. I9: the engine resolves
        // neither callback — it drops the shard and lets the durable log decide.
        f.harness().inDoubtCommit(batch, CompletionReason.DISPATCHED, true);
        f.harness().dropAndRecover(0);

        assertFalse(f.store().isRecovering(0));
        assertEquals(1, f.store().pendingCount(0), "broker committed: settled entries must NOT resurface (duplicate)");
        DueBatch rest = f.store().pollDue(Long.MAX_VALUE, 10);
        assertEquals(1, rest.size());
        assertEquals(4, rest.sourceOffset(0));
        assertEquals(future, rest.trackerOffset(0));
        f.harness().abortDispatch(rest);
    }

    @Test
    public void inDoubtCommitThatActuallyAbortedRedispatchesAfterReplay() {
        Fixture f = fixture(0);
        for (int i = 1; i <= 3; i++) {
            f.harness().schedule(ref(0, i, T0));
        }
        f.harness().schedule(ref(0, 4, T0 + 9_000_000));

        DueBatch batch = f.store().pollDue(T0, 10);
        assertEquals(3, batch.size());
        // Ambiguous outcome again — but this time the broker aborted: nothing became durable.
        f.harness().inDoubtCommit(batch, CompletionReason.DISPATCHED, false);
        f.harness().dropAndRecover(0);

        assertFalse(f.store().isRecovering(0));
        assertEquals(4, f.store().pendingCount(0), "broker aborted: every entry remains pending (no loss)");
    }

    // ----------------------------------------------------------------------------------------
    // Partition lifecycle: revoke, lost, idempotent assignment
    // ----------------------------------------------------------------------------------------

    @Test
    public void revokeDropsInMemoryStateAndReassignmentRecoversFromTheCursor() {
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0));
        f.harness().schedule(ref(0, 2, T0 + 9_000_000));
        f.harness().schedule(ref(0, 3, T0 + 9_500_000));
        DueBatch due = f.store().pollDue(T0, 10);
        f.harness().commitDispatch(due, CompletionReason.DISPATCHED);
        f.harness().commitIdle(0);
        List<PendingEntry> expected = f.harness().pendingSnapshot();

        f.store().onPartitionsRevoked(Set.of(0));
        assertEquals(0, f.store().pendingCount(0), "revocation drops state in O(1): memory is a cache");

        f.store().onPartitionsAssigned(Set.of(0));
        f.harness().recover(0);
        assertFalse(f.store().isRecovering(0));
        assertEquals(expected, f.harness().pendingSnapshot(), "the durable cursor + log rebuild the dropped state");
    }

    @Test
    public void lostPartitionsAreDroppedWithoutFlushing() {
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0));
        f.harness().schedule(ref(0, 2, T0 + 9_000_000));
        f.harness().schedule(ref(0, 3, T0));
        DueBatch inFlight = f.store().pollDue(T0, 10);
        assertEquals(2, inFlight.size());

        long logEnd = f.harness().logEndOffset(0);
        // Fenced mid-transaction: a new owner may already be live. Drop, never write.
        f.store().onPartitionsLost(Set.of(0));
        assertEquals(logEnd, f.harness().logEndOffset(0), "lost is a drop, never a flush");
        assertEquals(0, f.store().pendingCount(0));

        f.store().onPartitionsAssigned(Set.of(0));
        f.harness().recover(0);
        assertEquals(3, f.store().pendingCount(0), "the abandoned in-flight batch was never settled: it resurfaces");
    }

    @Test
    public void assignmentIsIdempotentAndIncremental() {
        FakeStoreContext ctx = contextBuilder().build();
        TrackerBackedStore store = createStore(ctx);
        TrackerStoreHarness harness = new TrackerStoreHarness(store);
        store.onPartitionsAssigned(Set.of(0));
        store.onPartitionsAssigned(Set.of(0, 1)); // incremental delivery (cooperative / KIP-848)
        harness.recover(0);
        harness.recover(1);
        harness.schedule(ref(0, 1, T0));

        store.onPartitionsAssigned(Set.of(0)); // duplicate delivery of an already-owned partition
        assertTrue(
                store.pendingCount(0) == 1 || store.isRecovering(0),
                "re-assignment must be a no-op or a clean re-recovery — never silent state loss");
        if (store.isRecovering(0)) {
            harness.recover(0);
        }
        DueBatch batch = store.pollDue(T0, 10);
        assertEquals(1, batch.size());
        harness.commitDispatch(batch, CompletionReason.DISPATCHED);

        store.close();
        store.close(); // close is idempotent and never throws
    }

    @Test
    public void pendingCountTracksScheduleSettleAndRestore() {
        Fixture f = fixture(0);
        assertEquals(0, f.store().pendingCount(0));
        f.harness().schedule(ref(0, 1, T0));
        f.harness().schedule(ref(0, 2, T0));
        f.harness().schedule(ref(0, 3, T0 + 9_000_000));
        assertEquals(3, f.store().pendingCount(0));

        DueBatch due = f.store().pollDue(T0, 10);
        f.harness().commitDispatch(due, CompletionReason.DISPATCHED);
        assertEquals(1, f.store().pendingCount(0));

        f.harness().schedule(ref(0, 4, T0));
        DueBatch aborted = f.store().pollDue(T0, 10);
        assertEquals(1, aborted.size());
        f.harness().abortDispatch(aborted);
        assertEquals(2, f.store().pendingCount(0), "restored entries count as pending again");
    }

    // ----------------------------------------------------------------------------------------
    // Penalty box (§7.3, D22, R9 — the M1 obligation)
    // ----------------------------------------------------------------------------------------

    @Test
    public void penaltyBoxSkipsDueEntriesAndShapesTheDeadline() {
        Fixture f = fixture(0, 1);
        f.harness().schedule(ref(0, 1, T0 - 100)); // overdue on the degraded partition
        f.harness().schedule(ref(1, 1, T0)); // due on the healthy partition

        f.store().penalizeSourcePartition(0, T0 + 5_000);
        DueBatch healthy = f.store().pollDue(T0 + 100, 10);
        assertEquals(1, healthy.size(), "a penalized partition must not head-of-line block healthy ones");
        assertEquals(1, healthy.sourcePartition(0));
        f.harness().commitDispatch(healthy, CompletionReason.DISPATCHED);

        assertEquals(
                T0 + 5_000,
                f.store().nextDeadlineMs(),
                "a penalized-but-due entry contributes its penalty deadline, never a zero poll timeout");
        assertEquals(0, f.store().pollDue(T0 + 4_999, 10).size(), "skipped while the stamp is unexpired");

        DueBatch afterExpiry = f.store().pollDue(T0 + 5_000, 10);
        assertEquals(1, afterExpiry.size(), "an expired stamp has no effect");
        f.harness().abortDispatch(afterExpiry); // keep it pending for the explicit-clear leg

        f.store().penalizeSourcePartition(0, T0 + 60_000); // re-stamp far out...
        assertEquals(0, f.store().pollDue(T0 + 100, 10).size());
        f.store().penalizeSourcePartition(0, 0); // ...then the engine clears with a past deadline
        assertEquals(T0 - 100, f.store().nextDeadlineMs());
        DueBatch cleared = f.store().pollDue(T0 + 100, 10);
        assertEquals(1, cleared.size());
        assertEquals(0, cleared.sourcePartition(0));
        f.harness().commitDispatch(cleared, CompletionReason.DISPATCHED);
    }

    // ----------------------------------------------------------------------------------------
    // Pre-compacted logs (§3.7 backstop)
    // ----------------------------------------------------------------------------------------

    @Test
    public void compactedLogWithOrphanTombstonesConverges() {
        TrackerEventScript script = TrackerEventScript.forPartition(0)
                .add(1, T0 + 10)
                .complete(1)
                .add(2, T0 + 20, true)
                .add(3, T0 + 30)
                .complete(3)
                .add(4, T0 + 40);
        assertScriptConverges(script.compactAddsOfCompletedPairs());
    }

    @Test
    public void compactedLogWithVanishedPairsConverges() {
        TrackerEventScript script = TrackerEventScript.forPartition(0)
                .add(1, T0 + 10)
                .complete(1)
                .add(2, T0 + 20)
                .add(3, T0 + 30, true)
                .complete(3)
                .add(4, T0 + 40);
        assertScriptConverges(script.compactCompletedPairs());
    }

    // ----------------------------------------------------------------------------------------
    // Properties (jqwik)
    // ----------------------------------------------------------------------------------------

    /**
     * THE killer property (I5 made observable through the SPI): after any interleaving of adds,
     * duplicates, drains, full and <em>partial</em> (truncate-and-carry-over) commits, definitive
     * aborts, idle commits and re-recoveries, committing the cursor and recovering a brand-new
     * store instance from it (sidecar seeding + replay of everything at or above the cursor
     * offset, across the transactional log's control-marker gaps) reproduces the exact pending
     * set — which is only possible when every pending entry either has
     * {@code trackerAddOffset >= cursor.offset()} or rides the sidecar. Cursor offsets are also
     * asserted monotonic at every commit.
     */
    @Property(tries = 30)
    public void cursorInvariantHoldsUnderRandomInterleavings(@ForAll("opSequences") List<StoreOp> ops) {
        FakeStoreContext ctx = contextBuilder().build();
        TrackerBackedStore store = createStore(ctx);
        TrackerStoreHarness harness = new TrackerStoreHarness(store);
        int partitions = partitionCount();
        for (int p = 0; p < partitions; p++) {
            harness.startPartition(p);
        }

        Map<Integer, Map<Long, ModelEntry>> model = new HashMap<>();
        Map<Integer, Long> nextSourceOffset = new HashMap<>();
        Map<Integer, Long> cursorFloor = new HashMap<>();
        long now = T0;

        for (StoreOp op : ops) {
            switch (op) {
                case StoreOp.Add(int partition, int gap, long delayMs, boolean clamped) -> {
                    long sourceOffset = nextSourceOffset.getOrDefault(partition, 0L);
                    nextSourceOffset.put(partition, sourceOffset + gap);
                    long at = now + delayMs;
                    long trackerOffset = harness.schedule(new ScheduledRef(partition, sourceOffset, at, clamped));
                    modelOf(model, partition).put(sourceOffset, new ModelEntry(at, trackerOffset, clamped));
                }
                case StoreOp.DuplicateAdd(int partition, int pickIndex, long delayMs) -> {
                    Map<Long, ModelEntry> entries = modelOf(model, partition);
                    if (entries.isEmpty()) {
                        continue;
                    }
                    List<Long> keys = List.copyOf(entries.keySet());
                    long key = keys.get(pickIndex % keys.size());
                    ModelEntry entry = entries.get(key);
                    if (entry == null) {
                        throw new AssertionError("model key vanished: " + key);
                    }
                    long at = now + delayMs;
                    // The duplicate carries the entry's current clamp marker, so the property is
                    // agnostic to whether a store updates or keeps non-schedule markers on R1.
                    harness.schedule(new ScheduledRef(partition, key, at, entry.clamped()));
                    entries.put(key, new ModelEntry(at, entry.trackerAddOffset(), entry.clamped()));
                }
                case StoreOp.AdvanceTime(long deltaMs) -> now += deltaMs;
                case StoreOp.DrainCommit(int maxBatch, CompletionReason reason) -> {
                    DueBatch batch = store.pollDue(now, maxBatch);
                    if (batch.size() == 0) {
                        continue;
                    }
                    ArrayDueBatch snapshot = ArrayDueBatch.snapshot(batch);
                    Map<Integer, TrackerCursor> cursors = harness.commitDispatch(batch, reason);
                    for (int i = 0; i < snapshot.size(); i++) {
                        modelOf(model, snapshot.sourcePartition(i)).remove(snapshot.sourceOffset(i));
                    }
                    assertCursorsMonotonic(cursorFloor, cursors);
                }
                case StoreOp.DrainCommitPartial(int maxBatch, int settleSeed, CompletionReason reason) -> {
                    DueBatch batch = store.pollDue(now, maxBatch);
                    if (batch.size() == 0) {
                        continue;
                    }
                    if (batch.size() < 2) {
                        harness.abortDispatch(batch); // no strict subset exists — restore
                        continue;
                    }
                    ArrayDueBatch snapshot = ArrayDueBatch.snapshot(batch);
                    int settledCount = 1 + settleSeed % (snapshot.size() - 1);
                    int[] settledIndices = new int[settledCount];
                    for (int i = 0; i < settledCount; i++) {
                        settledIndices[i] = i;
                    }
                    Map<Integer, TrackerCursor> cursors = harness.commitDispatchPartial(batch, settledIndices, reason);
                    for (int i = 0; i < settledCount; i++) {
                        modelOf(model, snapshot.sourcePartition(i)).remove(snapshot.sourceOffset(i));
                    }
                    assertCursorsMonotonic(cursorFloor, cursors);
                }
                case StoreOp.DrainAbort(int maxBatch) -> {
                    DueBatch batch = store.pollDue(now, maxBatch);
                    if (batch.size() > 0) {
                        harness.abortDispatch(batch);
                    }
                }
                case StoreOp.IdleCommit(int partition) -> {
                    TrackerCursor cursor = harness.commitIdle(partition);
                    assertCursorsMonotonic(cursorFloor, Map.of(partition, cursor));
                }
                case StoreOp.Rerecover(int partition) -> {
                    harness.dropAndRecover(partition);
                    assertFalse(store.isRecovering(partition));
                }
            }
        }

        for (int p = 0; p < partitions; p++) {
            assertCursorsMonotonic(cursorFloor, Map.of(p, harness.commitIdle(p)));
        }
        List<PendingEntry> expected = expectedFromModel(model);
        assertEquals(expected, harness.pendingSnapshot(), "the live store diverged from the reference model");

        TrackerBackedStore fresh =
                createStore(contextBuilder().route(ctx.route()).build());
        TrackerStoreHarness freshHarness = harness.migrateTo(fresh);
        for (int p = 0; p < partitions; p++) {
            freshHarness.startPartition(p);
            assertFalse(fresh.isRecovering(p));
        }
        assertEquals(
                expected,
                freshHarness.pendingSnapshot(),
                "killer property: a fresh store recovered from the committed cursor must reproduce"
                        + " the exact pending set (every pending entry >= cursor offset or in the sidecar — I5)");
    }

    /** Both §3.7 compaction asymmetries (and the uncompacted baseline) converge to script truth. */
    @Property(tries = 30)
    public void compactedLogsConvergeToScriptTruth(@ForAll("compactionScripts") TrackerEventScript script) {
        assertScriptConverges(script);
        assertScriptConverges(script.compactAddsOfCompletedPairs());
        assertScriptConverges(script.compactCompletedPairs());
    }

    @Provide
    Arbitrary<List<StoreOp>> opSequences() {
        return StoreOpArbitraries.sequences(partitionCount());
    }

    @Provide
    Arbitrary<TrackerEventScript> compactionScripts() {
        return StoreOpArbitraries.compactionScripts(0);
    }

    // ----------------------------------------------------------------------------------------
    // Soak (§11.2: memory ceiling + complexity sanity)
    // ----------------------------------------------------------------------------------------

    /**
     * Excluded from default test lanes by the {@code @Tag("soak")} routing (the repo conventions
     * exclude the tag from {@code test} and provide a dedicated {@code soakTest} task for the
     * nightly lane, §11.2) — GC-delta memory measurement is not a PR-gating-quality signal on
     * shared CI workers. Kit consumers should route the tag the same way, or tune
     * {@link #soakEntryCount()} / {@link #soakMemoryCeilingBytes()}.
     */
    @Test
    @Tag("soak")
    public void soakMillionEntriesStayWithinTheMemoryCeiling() {
        FakeStoreContext ctx = contextBuilder().build();
        TrackerBackedStore store = createStore(ctx);
        try {
            store.onPartitionsAssigned(Set.of(0));
            store.beginRecovery(0, new TrackerCursor(0, ""), 0);
            int entries = soakEntryCount();

            long before = usedHeapAfterGc();
            for (int i = 0; i < entries; i++) {
                ScheduledRef ref = new ScheduledRef(0, i, T0 + (i % 86_400_000), (i & 7) == 0);
                TrackerRecordData data = store.encodeSchedule(ref);
                store.onTrackerRecord(0, i, data.key(), data.value());
            }
            long retained = usedHeapAfterGc() - before;
            assertEquals(entries, store.pendingCount(0));
            assertTrue(
                    retained <= soakMemoryCeilingBytes(),
                    "retained " + retained + " B for " + entries + " entries exceeds the ceiling "
                            + soakMemoryCeilingBytes() + " B");

            // Complexity sanity: drains against a million-entry index must stay cheap (O(log n)
            // per pop); a generous wall-clock bound catches accidental O(n) scans only.
            long start = System.nanoTime();
            long drained = 0;
            while (drained < 100_000) {
                DueBatch batch = store.pollDue(Long.MAX_VALUE, 10_000);
                if (batch.size() == 0) {
                    break;
                }
                drained += batch.size();
                store.onBatchCommitted(batch);
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertEquals(100_000, drained);
            assertTrue(elapsedMs < 10_000, "draining 100k of 1M entries took " + elapsedMs + " ms");
        } finally {
            store.close();
        }
    }

    // ----------------------------------------------------------------------------------------
    // Shared plumbing
    // ----------------------------------------------------------------------------------------

    /** Context builder pre-loaded with the contract's partition count and the store's properties. */
    protected FakeStoreContext.Builder contextBuilder() {
        return FakeStoreContext.builder().partitions(partitionCount()).properties(storeProperties());
    }

    /** An unclamped {@link ScheduledRef}. */
    protected static ScheduledRef ref(int partition, long sourceOffset, long dispatchAtMs) {
        return new ScheduledRef(partition, sourceOffset, dispatchAtMs, false);
    }

    /** A clamped {@link ScheduledRef}. */
    protected static ScheduledRef refClamped(int partition, long sourceOffset, long dispatchAtMs) {
        return new ScheduledRef(partition, sourceOffset, dispatchAtMs, true);
    }

    private Fixture fixture(int... partitions) {
        FakeStoreContext ctx = contextBuilder().build();
        TrackerBackedStore store = createStore(ctx);
        TrackerStoreHarness harness = new TrackerStoreHarness(store);
        for (int partition : partitions) {
            harness.startPartition(partition);
        }
        return new Fixture(ctx, store, harness);
    }

    /** A fresh incarnation of the same route: new store, shared durable state. */
    private Fixture sibling(Fixture previous) {
        FakeStoreContext ctx =
                contextBuilder().route(previous.context().route()).build();
        TrackerBackedStore store = createStore(ctx);
        return new Fixture(ctx, store, previous.harness().migrateTo(store));
    }

    private void assertScriptConverges(TrackerEventScript script) {
        TrackerBackedStore store = createStore(contextBuilder().build());
        try {
            store.onPartitionsAssigned(Set.of(script.partition()));
            store.beginRecovery(script.partition(), new TrackerCursor(0, ""), script.replayBarrier());
            script.replayInto(store);
            assertFalse(store.isRecovering(script.partition()), "replay must reach the barrier");

            List<TrackerEventScript.PendingTruth> truth = new ArrayList<>(script.expectedPending());
            truth.sort(Comparator.comparingLong(TrackerEventScript.PendingTruth::sourceOffset));
            DueBatch batch = store.pollDue(Long.MAX_VALUE, Integer.MAX_VALUE);
            List<TrackerEventScript.PendingTruth> observed = new ArrayList<>(batch.size());
            for (int i = 0; i < batch.size(); i++) {
                observed.add(new TrackerEventScript.PendingTruth(
                        batch.sourceOffset(i), batch.dispatchAtMs(i), batch.trackerOffset(i), batch.clamped(i)));
            }
            observed.sort(Comparator.comparingLong(TrackerEventScript.PendingTruth::sourceOffset));
            assertEquals(truth, observed, "no duplicates, no losses: replay converges to the script truth");
            store.onBatchAborted(batch);
        } finally {
            store.close();
        }
    }

    private static TrackerRecordData encodeOneCompletion(
            TrackerBackedStore store, int partition, long sourceOffset, CompletionReason reason) {
        ArrayDueBatch view = ArrayDueBatch.builder()
                .add(partition, sourceOffset, 0, -1, false)
                .build();
        List<TrackerRecordData> tombstones = store.encodeCompletions(view, reason);
        assertEquals(1, tombstones.size(), "one tombstone per entry");
        return tombstones.get(0);
    }

    private static void assertEntry(
            DueBatch batch,
            int index,
            int partition,
            long sourceOffset,
            long dispatchAtMs,
            long trackerOffset,
            boolean clamped) {
        assertEquals(partition, batch.sourcePartition(index), "sourcePartition[" + index + "]");
        assertEquals(sourceOffset, batch.sourceOffset(index), "sourceOffset[" + index + "]");
        assertEquals(dispatchAtMs, batch.dispatchAtMs(index), "dispatchAtMs[" + index + "]");
        assertEquals(trackerOffset, batch.trackerOffset(index), "trackerOffset[" + index + "]");
        assertEquals(clamped, batch.clamped(index), "clamped[" + index + "]");
    }

    private static TrackerCursor cursorOf(Map<Integer, TrackerCursor> cursors, int partition) {
        TrackerCursor cursor = cursors.get(partition);
        if (cursor == null) {
            throw new AssertionError("no cursor committed for partition " + partition + "; got " + cursors.keySet());
        }
        return cursor;
    }

    private static Map<Long, ModelEntry> modelOf(Map<Integer, Map<Long, ModelEntry>> model, int partition) {
        // LinkedHashMap keeps the DuplicateAdd pick deterministic per generated sequence.
        return model.computeIfAbsent(partition, ignored -> new LinkedHashMap<>());
    }

    private static List<PendingEntry> expectedFromModel(Map<Integer, Map<Long, ModelEntry>> model) {
        List<PendingEntry> expected = new ArrayList<>();
        model.forEach((partition, entries) -> entries.forEach((sourceOffset, entry) -> expected.add(new PendingEntry(
                partition, sourceOffset, entry.dispatchAtMs(), entry.trackerAddOffset(), entry.clamped()))));
        expected.sort(null);
        return expected;
    }

    private static void assertCursorsMonotonic(Map<Integer, Long> floor, Map<Integer, TrackerCursor> committed) {
        committed.forEach((partition, cursor) -> {
            long previous = floor.getOrDefault(partition, Long.MIN_VALUE);
            assertTrue(
                    cursor.offset() >= previous,
                    "cursor offset regressed for partition " + partition + ": " + previous + " -> " + cursor.offset());
            floor.put(partition, cursor.offset());
        });
    }

    private static long usedHeapAfterGc() {
        Runtime runtime = Runtime.getRuntime();
        for (int i = 0; i < 3; i++) {
            System.gc();
        }
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private record Fixture(FakeStoreContext context, TrackerBackedStore store, TrackerStoreHarness harness) {}

    private record ModelEntry(long dispatchAtMs, long trackerAddOffset, boolean clamped) {}
}
