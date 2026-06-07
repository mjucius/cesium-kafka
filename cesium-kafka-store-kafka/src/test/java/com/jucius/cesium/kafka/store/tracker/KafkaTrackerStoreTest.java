package com.jucius.cesium.kafka.store.tracker;

import static com.jucius.cesium.kafka.store.tracker.StoreFixtures.EMPTY_BATCH;
import static com.jucius.cesium.kafka.store.tracker.StoreFixtures.feedComplete;
import static com.jucius.cesium.kafka.store.tracker.StoreFixtures.feedSchedule;
import static com.jucius.cesium.kafka.store.tracker.StoreFixtures.startedStore;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.store.CompletionReason;
import com.jucius.cesium.kafka.api.store.DueBatch;
import com.jucius.cesium.kafka.api.store.ScheduledRef;
import com.jucius.cesium.kafka.api.store.StoreCapabilities;
import com.jucius.cesium.kafka.api.store.TrackerCursor;
import com.jucius.cesium.kafka.api.store.TrackerRecordData;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

/**
 * Store-level units for {@link KafkaTrackerStore}: full
 * schedule→tracker-record→pollDue→encodeCompletions→commit→cursor cycles through the real wire
 * format, replay rules R1/R2 via the store path, the clamped bit end-to-end, in-flight exclusion
 * ("as if complete", §3.5), the cursor monotonic-guard safe-degrade, and metrics lifecycle.
 */
class KafkaTrackerStoreTest {

    private static final long T0 = 1_000_000;

    private static KafkaTrackerStore activeStore(FixedIdentityStoreContext ctx, int partition) {
        KafkaTrackerStore store = startedStore(ctx);
        store.onPartitionsAssigned(Set.of(partition));
        // First run: no committed cursor, empty tracker — barrier 0 promotes immediately.
        store.beginRecovery(partition, new TrackerCursor(0, ""), 0);
        return store;
    }

    @Test
    void capabilitiesDeclareTheTrackerArchetype() {
        KafkaTrackerStore store = new KafkaTrackerStore();
        StoreCapabilities capabilities = store.capabilities();
        assertEquals(StoreCapabilities.TransactionAffinity.KAFKA_TRANSACTIONAL, capabilities.affinity());
        assertEquals(StoreCapabilities.DispatchGuarantee.EXACTLY_ONCE, capabilities.dispatchGuarantee());
        assertTrue(capabilities.requiresTrackerTopic());
        assertFalse(capabilities.supportsCancellation());
    }

    @Test
    void fullDispatchCycleThroughTheRealWireFormat() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(3);
        KafkaTrackerStore store = activeStore(ctx, 0);

        feedSchedule(store, 0, new ScheduledRef(0, 100, T0 + 1_000, false));
        feedSchedule(store, 1, new ScheduledRef(0, 101, T0 + 500, true)); // CLAMP-policy schedule
        feedSchedule(store, 2, new ScheduledRef(0, 102, T0 + 2_000, false));
        assertEquals(3, store.pendingCount(0));
        assertEquals(T0 + 500, store.nextDeadlineMs());

        DueBatch batch = store.pollDue(T0 + 1_000, 10);
        assertEquals(2, batch.size());
        assertEquals(101, batch.sourceOffset(0));
        assertEquals(T0 + 500, batch.dispatchAtMs(0));
        assertEquals(1, batch.trackerOffset(0));
        assertTrue(batch.clamped(0), "the CLAMP marker survives ingest -> tracker -> index -> DueBatch");
        assertEquals(100, batch.sourceOffset(1));
        assertFalse(batch.clamped(1));

        List<TrackerRecordData> tombstones = store.encodeCompletions(batch, CompletionReason.DISPATCHED);
        assertEquals(2, tombstones.size());
        for (int i = 0; i < tombstones.size(); i++) {
            TrackerRecordData tombstone = tombstones.get(i);
            assertNull(tombstone.value(), "completions are null-value tombstones (D15)");
            assertEquals(batch.sourceOffset(i), TrackerWireFormat.decodeKey(tombstone.key()));
            Header reason = tombstone.headers().lastHeader(TrackerWireFormat.COMPLETION_REASON_HEADER);
            assertNotNull(reason);
            assertEquals("DISPATCHED", new String(reason.value(), StandardCharsets.US_ASCII));
        }

        // Cursor "as if the in-flight batch were complete": only src=102 remains pending.
        TrackerCursor cursor = store.committedCursor(0, batch);
        assertEquals(3, cursor.offset(), "all pending encoded => cursor = live position");
        SidecarCodec.DecodedSidecar sidecar = SidecarCodec.decode(cursor.metadata());
        assertEquals(
                List.of(new SidecarCodec.SidecarEntry(2, 102, T0 + 2_000, false)),
                sidecar.entries(),
                "in-flight entries are excluded from the sidecar");

        store.onBatchCommitted(batch);
        assertEquals(1, store.pendingCount(0));

        // The store's own COMPLETE echoes arrive via the tracker tail: R2 no-ops.
        feedComplete(store, 0, 3, 101);
        feedComplete(store, 0, 4, 100);
        assertEquals(1, store.pendingCount(0));

        TrackerCursor after = store.committedCursor(0, EMPTY_BATCH);
        assertEquals(5, after.offset(), "position advanced past the echoes");
        assertTrue(after.offset() >= cursor.offset(), "cursor offsets are monotonic");
    }

    @Test
    void encodeCompletionsHandlesArbitrarySubBatchViews() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = activeStore(ctx, 0);
        feedSchedule(store, 0, new ScheduledRef(0, 10, T0, false));
        feedSchedule(store, 1, new ScheduledRef(0, 11, T0, false));
        feedSchedule(store, 2, new ScheduledRef(0, 12, T0, false));
        DueBatch batch = store.pollDue(T0, 10);
        assertEquals(3, batch.size());

        // Per-reason sub-batch view (the M1-review obligation: the engine groups by reason and
        // calls once per group with a view; the store must handle any subset).
        DueBatch view = subBatch(batch, 1, 3);
        List<TrackerRecordData> tombstones = store.encodeCompletions(view, CompletionReason.PAYLOAD_MISSING_DLQ);
        assertEquals(2, tombstones.size());
        assertEquals(11, TrackerWireFormat.decodeKey(tombstones.get(0).key()));
        assertEquals(12, TrackerWireFormat.decodeKey(tombstones.get(1).key()));
        Header reason = tombstones.get(0).headers().lastHeader(TrackerWireFormat.COMPLETION_REASON_HEADER);
        assertEquals("PAYLOAD_MISSING_DLQ", new String(reason.value(), StandardCharsets.US_ASCII));

        store.onBatchCommitted(batch); // the FULL batch resolves, never the views
        assertEquals(0, store.pendingCount(0));
    }

    @Test
    void abortRestoresEntriesAndDiscardsTheProposedCursor() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = activeStore(ctx, 0);
        feedSchedule(store, 0, new ScheduledRef(0, 10, T0, false));
        feedSchedule(store, 1, new ScheduledRef(0, 11, T0 + 50, true));

        DueBatch batch = store.pollDue(T0 + 100, 10);
        assertEquals(2, batch.size());
        TrackerCursor proposed = store.committedCursor(0, batch);
        assertEquals(2, proposed.offset());

        store.onBatchAborted(batch);
        assertEquals(2, store.pendingCount(0), "definitive abort restores the entries");

        DueBatch again = store.pollDue(T0 + 100, 10);
        assertEquals(2, again.size(), "restored entries re-dispatch");
        assertTrue(again.clamped(1), "clamped survives restore-after-abort");
        store.onBatchCommitted(again);
        assertEquals(0, store.pendingCount(0));
    }

    @Test
    void idleCursorAdvancementPromotesTheFloorOnEmptyCommit() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = activeStore(ctx, 0);
        feedSchedule(store, 0, new ScheduledRef(0, 10, T0, false));
        feedComplete(store, 0, 1, 10);
        assertEquals(0, store.pendingCount(0));

        TrackerCursor cursor = store.committedCursor(0, EMPTY_BATCH);
        assertEquals(2, cursor.offset());
        store.onBatchCommitted(EMPTY_BATCH); // records-free idle-advancement transaction

        // The promoted floor now guards: a (bug-injected) position regression degrades safely.
        store.onTrackerRecord(0, 0, TrackerWireFormat.encodeKey(99), TrackerWireFormat.encodeAddValue(T0, false));
        TrackerCursor degraded = store.committedCursor(0, EMPTY_BATCH);
        assertEquals(2, degraded.offset(), "the promoted floor is authoritative");
    }

    @Test
    void truncatedBatchCursorCarriesCarryOverEntriesAndSurvivesACrash() {
        // Regression (M3 review, critical): committedCursor must honor its inFlight PARAMETER.
        // Under truncate-and-carry-over (design section 3.2 / section 7 step 2c, D8) the
        // transaction settles only the fetched subset of the drained batch; the carry-over
        // entries are still pending durable truth at commit time and must ride the sidecar (or
        // bound the overflow cut) -- deriving the as-if-complete set from in-flight slot state
        // instead loses them permanently on the first crash after the commit.
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = activeStore(ctx, 0);
        feedSchedule(store, 0, new ScheduledRef(0, 10, T0, false)); // E1, due
        feedSchedule(store, 1, new ScheduledRef(0, 11, T0, false)); // E2, due

        DueBatch drained = store.pollDue(T0, 10);
        assertEquals(2, drained.size());

        // The fetch byte budget trips after E1: the transaction settles only E1's view.
        DueBatch fetched = subBatch(drained, 0, 1);
        TrackerCursor cursor = store.committedCursor(0, fetched);
        assertEquals(2, cursor.offset(), "every unsettled entry fits the sidecar: position-tracking cursor");
        assertEquals(
                List.of(new SidecarCodec.SidecarEntry(1, 11, T0, false)),
                SidecarCodec.decode(cursor.metadata()).entries(),
                "the carry-over entry E2 must ride the sidecar -- no tombstone for it commits");

        // Commit: tombstone(E1)@2 + cursor, atomically; the engine resolves the split views
        // (settled view committed, carry-over view restored -- both foreign DueBatch views).
        store.onBatchCommitted(fetched);
        store.onBatchAborted(subBatch(drained, 1, 2));
        assertEquals(1, store.pendingCount(0), "the carry-over returned to pending");

        // Crash before E2 re-dispatches: memory dies, the cursor + log survive.
        KafkaTrackerStore fresh = startedStore(FixedIdentityStoreContext.withPartitions(1));
        fresh.onPartitionsAssigned(Set.of(0));
        fresh.beginRecovery(0, cursor, 3); // log: ADD@0, ADD@1, tombstone(E1)@2; barrier = 3
        feedComplete(fresh, 0, 2, 10); // replay [2, 3): only E1's tombstone
        assertFalse(fresh.isRecovering(0));

        assertEquals(1, fresh.pendingCount(0), "E2 must survive the crash (I5 against durable truth)");
        DueBatch recovered = fresh.pollDue(T0, 10);
        assertEquals(11, recovered.sourceOffset(0));
        assertEquals(1, recovered.trackerOffset(0), "E2 keeps its original tracker ADD offset");
        fresh.onBatchCommitted(recovered);
    }

    @Test
    void reasonlessTombstonesAreCountedAndSkippedOnTheHeadersPath() {
        // R12: a tombstone missing the completion-reason header matches the forged-tombstone
        // shape -- the data-loss primitive section 2.2's count-and-skip posture exists for. The
        // engine's headers-carrying call path must enforce it.
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = activeStore(ctx, 0);
        feedSchedule(store, 0, new ScheduledRef(0, 10, T0 + 100, false));

        store.onTrackerRecord(0, 1, TrackerWireFormat.encodeKey(10), null, new RecordHeaders());
        assertEquals(1, store.pendingCount(0), "the forged tombstone must NOT apply");
        assertEquals(
                1.0,
                ctx.registry().get("cesium.tracker.invalid.records").counter().count());

        TrackerCursor cursor = store.committedCursor(0, EMPTY_BATCH);
        assertEquals(2, cursor.offset(), "the skipped record was still consumed: position advanced");
    }

    @Test
    void unknownReasonTombstonesStillApplyAndSurfaceTheNovelty() {
        // The M4 decision: an unrecognized reason constant must still settle the entry (a
        // skipped completion re-dispatches a completed key -- a read_committed-visible
        // duplicate); the novelty is surfaced under a dedicated counter, never as Invalid.
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = activeStore(ctx, 0);
        feedSchedule(store, 0, new ScheduledRef(0, 10, T0 + 100, false));

        RecordHeaders headers = new RecordHeaders();
        headers.add(
                TrackerWireFormat.COMPLETION_REASON_HEADER, "SOME_FUTURE_REASON".getBytes(StandardCharsets.US_ASCII));
        store.onTrackerRecord(0, 1, TrackerWireFormat.encodeKey(10), null, headers);
        assertEquals(0, store.pendingCount(0), "the completion must apply");
        assertEquals(
                1.0,
                ctx.registry()
                        .get("cesium.tracker.unknown.reason.records")
                        .counter()
                        .count());
        assertEquals(
                0.0,
                ctx.registry().get("cesium.tracker.invalid.records").counter().count());
    }

    @Test
    void headersBlindTombstonesKeepTheLenientApplyPosture() {
        // The legacy 4-arg overload cannot see headers, so it cannot distinguish a forged
        // reason-less tombstone from a legitimate one whose headers were simply not plumbed: it
        // fails safe toward applying (skipping would be the duplicate vector). The engine's
        // call path is the 5-arg variant, where R12 is enforced.
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = activeStore(ctx, 0);
        feedSchedule(store, 0, new ScheduledRef(0, 10, T0 + 100, false));

        store.onTrackerRecord(0, 1, TrackerWireFormat.encodeKey(10), null);
        assertEquals(0, store.pendingCount(0), "the headers-blind path applies valid tombstones");
        assertEquals(
                0.0,
                ctx.registry().get("cesium.tracker.invalid.records").counter().count());
    }

    @Test
    void anomalyCounterIsMonotonicAcrossInPlaceReRecovery() {
        // Regression (M3 review, minor): beginRecovery's idempotent-reset branch replaces the
        // shard, whose plain counters restart at zero -- the monotonic baselines must restart
        // with them, or the new shard's first N anomalies are silently swallowed (exactly the
        // post-incident moment the R1/R16 anomaly canary matters most).
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = activeStore(ctx, 0);
        feedSchedule(store, 0, new ScheduledRef(0, 10, T0, false));
        feedSchedule(store, 1, new ScheduledRef(0, 10, T0 + 1, false)); // duplicate -> anomaly
        store.maintenance();
        assertEquals(
                1.0,
                ctx.registry().get("cesium.store.index.anomalies").counter().count());

        store.beginRecovery(0, new TrackerCursor(0, ""), 0); // in-place re-recovery (D-6/I9)
        feedSchedule(store, 0, new ScheduledRef(0, 10, T0, false));
        feedSchedule(store, 1, new ScheduledRef(0, 10, T0 + 1, false)); // anomaly in the NEW shard
        store.maintenance();
        assertEquals(
                2.0,
                ctx.registry().get("cesium.store.index.anomalies").counter().count(),
                "a stale baseline must not swallow the first anomaly after an in-place re-recovery");
    }

    @Test
    void duplicateAddViaStorePathUpdatesScheduleButKeepsOriginalTrackerOffset() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = activeStore(ctx, 0);
        feedSchedule(store, 0, new ScheduledRef(0, 100, T0 + 1_000, false));
        feedSchedule(store, 1, new ScheduledRef(0, 100, T0 + 2_000, true)); // anomalous duplicate (R1)
        assertEquals(1, store.pendingCount(0));

        assertEquals(0, store.pollDue(T0 + 1_500, 10).size(), "the updated deadline governs");
        DueBatch batch = store.pollDue(T0 + 2_000, 10);
        assertEquals(1, batch.size());
        assertEquals(100, batch.sourceOffset(0));
        assertEquals(T0 + 2_000, batch.dispatchAtMs(0));
        assertEquals(0, batch.trackerOffset(0), "the ORIGINAL trackerAddOffset is kept (I5)");
        assertTrue(batch.clamped(0), "the schedule value (dispatchAtMs + clamped) is updated");
        store.onBatchCommitted(batch);

        store.maintenance();
        assertEquals(
                1.0,
                ctx.registry().get("cesium.store.index.anomalies").counter().count(),
                "the duplicate is surfaced as a warn metric");
    }

    @Test
    void completeForUnknownOffsetIsASilentNoOp() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = activeStore(ctx, 0);
        feedSchedule(store, 0, new ScheduledRef(0, 100, T0, false));

        feedComplete(store, 0, 1, 999); // R2: COMPLETE-without-ADD
        assertEquals(1, store.pendingCount(0));
        assertEquals(
                0.0,
                ctx.registry().get("cesium.tracker.invalid.records").counter().count());
        store.maintenance();
        assertEquals(
                0.0,
                ctx.registry().get("cesium.store.index.anomalies").counter().count(),
                "R2 is expected");
    }

    @Test
    void invalidRecordsAreCountedSkippedAndStillAdvancePosition() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = activeStore(ctx, 0);

        store.onTrackerRecord(0, 7, TrackerWireFormat.encodeKey(5), new byte[] {1, 2, 3}); // bad magic
        store.onTrackerRecord(0, 8, new byte[] {1, 2}, null); // tombstone with a bad key length
        assertEquals(0, store.pendingCount(0));
        assertEquals(
                2.0,
                ctx.registry().get("cesium.tracker.invalid.records").counter().count());

        TrackerCursor cursor = store.committedCursor(0, EMPTY_BATCH);
        assertEquals(9, cursor.offset(), "invalid records were still consumed: position advanced");
    }

    @Test
    void reservedCancelRecordsAreCountedUnsupportedNoOps() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = activeStore(ctx, 0);
        byte[] cancel = {
            TrackerWireFormat.MAGIC, TrackerWireFormat.VERSION, TrackerWireFormat.TYPE_CANCEL, 0, 0, 0, 0, 0, 0, 0, 0, 0
        };
        store.onTrackerRecord(0, 0, TrackerWireFormat.encodeKey(5), cancel);
        assertEquals(0, store.pendingCount(0));
        assertEquals(
                1.0,
                ctx.registry().get("cesium.tracker.cancel.records").counter().count());
        assertEquals(
                0.0,
                ctx.registry().get("cesium.tracker.invalid.records").counter().count());
    }

    @Test
    void cursorGuardDegradesToTheLastSafeCursorAndRecovers() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = startedStore(ctx);
        store.onPartitionsAssigned(Set.of(0));
        store.beginRecovery(0, new TrackerCursor(100, ""), 100); // committed floor at offset 100

        // Bug injection: the engine misfeeds a record BELOW the committed cursor; position
        // regresses to 51 and the computed cursor would violate monotonicity.
        store.onTrackerRecord(0, 50, TrackerWireFormat.encodeKey(10), TrackerWireFormat.encodeAddValue(T0, false));
        TrackerCursor degraded = store.committedCursor(0, EMPTY_BATCH);
        assertEquals(100, degraded.offset(), "the last safe cursor is returned, never a corrupted one");
        assertEquals("", degraded.metadata());
        assertEquals(
                1.0,
                ctx.registry().get("cesium.cursor.guard.violations").counter().count());

        // Once position passes the floor again, normal cursors resume; the stray pending entry
        // rides in the sidecar (I5 holds: in the sidecar OR >= cursor offset).
        store.onTrackerRecord(0, 150, TrackerWireFormat.encodeKey(20), TrackerWireFormat.encodeAddValue(T0, false));
        TrackerCursor recovered = store.committedCursor(0, EMPTY_BATCH);
        assertEquals(151, recovered.offset());
        SidecarCodec.DecodedSidecar sidecar = SidecarCodec.decode(recovered.metadata());
        assertEquals(2, sidecar.entries().size());
        assertEquals(50, sidecar.entries().get(0).trackerAddOffset());
        assertEquals(
                1.0,
                ctx.registry().get("cesium.cursor.guard.violations").counter().count());
    }

    @Test
    void overflowFallbackCursorAtTheStoreLevel() {
        // Budget admits the header (46 raw for "test-cluster") plus ~2 small entries.
        FixedIdentityStoreContext ctx =
                new FixedIdentityStoreContext(1, Map.of(KafkaTrackerStore.SIDECAR_MAX_BYTES_KEY, "128"));
        KafkaTrackerStore store = activeStore(ctx, 0);
        for (int i = 0; i < 40; i++) {
            feedSchedule(store, i, new ScheduledRef(0, 100 + i, T0 + i, false));
        }
        TrackerCursor cursor = store.committedCursor(0, EMPTY_BATCH);
        assertTrue(cursor.offset() < 40, "overflow: the cursor falls back below the live position");
        SidecarCodec.DecodedSidecar sidecar = SidecarCodec.decode(cursor.metadata());
        assertTrue(sidecar.entries().size() < 40);
        assertEquals(
                sidecar.entries().size(),
                cursor.offset(),
                "the cut is the first NON-encoded pending entry's tracker ADD offset");
        long pinned = (long) ctx.registry()
                .get("cesium.pinned.entries")
                .tag("partition", "0")
                .gauge()
                .value();
        assertEquals(sidecar.entries().size(), pinned);
    }

    @Test
    void penalizeSourcePartitionIsDelegatedToTheIndex() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = activeStore(ctx, 0);
        feedSchedule(store, 0, new ScheduledRef(0, 10, T0, false));

        store.penalizeSourcePartition(0, T0 + 10_000);
        assertEquals(0, store.pollDue(T0 + 5_000, 10).size(), "penalized entries are skipped even when due");
        assertEquals(T0 + 10_000, store.nextDeadlineMs(), "the penalty deadline drives the poll timeout");

        store.penalizeSourcePartition(0, 0); // clear-by-past-deadline
        DueBatch batch = store.pollDue(T0 + 5_000, 10);
        assertEquals(1, batch.size());
        store.onBatchCommitted(batch);
    }

    @Test
    void gaugesRegisterOnAssignAndDeregisterOnRevoke() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(2);
        KafkaTrackerStore store = activeStore(ctx, 0);
        feedSchedule(store, 0, new ScheduledRef(0, 10, T0, false));

        assertEquals(
                1.0,
                ctx.registry()
                        .get("cesium.pending.entries")
                        .tag("partition", "0")
                        .gauge()
                        .value());
        assertNotNull(ctx.registry()
                .find("cesium.cursor.sidecar.bytes")
                .tag("partition", "0")
                .gauge());

        store.onPartitionsRevoked(Set.of(0));
        assertNull(
                ctx.registry()
                        .find("cesium.pending.entries")
                        .tag("partition", "0")
                        .gauge(),
                "gauges must be deregistered on revoke — they would bind dead state");
        assertNull(ctx.registry()
                .find("cesium.pinned.entries")
                .tag("partition", "0")
                .gauge());
        assertNull(ctx.registry()
                .find("cesium.cursor.sidecar.bytes")
                .tag("partition", "0")
                .gauge());
    }

    @Test
    void anomalyCounterIsMonotonicAcrossRevocations() {
        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = activeStore(ctx, 0);
        feedSchedule(store, 0, new ScheduledRef(0, 10, T0, false));
        feedSchedule(store, 1, new ScheduledRef(0, 10, T0 + 1, false)); // duplicate -> shard anomaly
        store.maintenance();
        assertEquals(
                1.0,
                ctx.registry().get("cesium.store.index.anomalies").counter().count());

        store.onPartitionsRevoked(Set.of(0)); // the shard's plain counter drops with it
        assertEquals(
                1.0,
                ctx.registry().get("cesium.store.index.anomalies").counter().count(),
                "the monotonic counter survives the revocation");

        store.onPartitionsAssigned(Set.of(0));
        store.beginRecovery(0, new TrackerCursor(0, ""), 0);
        feedSchedule(store, 0, new ScheduledRef(0, 10, T0, false));
        feedSchedule(store, 1, new ScheduledRef(0, 10, T0 + 1, false));
        store.onPartitionsRevoked(Set.of(0)); // final delta drained at drop time, without maintenance()
        assertEquals(
                2.0,
                ctx.registry().get("cesium.store.index.anomalies").counter().count());
    }

    @Test
    void lifecycleGuards() {
        KafkaTrackerStore unconfigured = new KafkaTrackerStore();
        assertThrows(IllegalStateException.class, unconfigured::validate);
        assertThrows(IllegalStateException.class, () -> unconfigured.pollDue(0, 1));

        FixedIdentityStoreContext ctx = FixedIdentityStoreContext.withPartitions(1);
        KafkaTrackerStore store = new KafkaTrackerStore();
        store.configure(ctx);
        assertThrows(IllegalStateException.class, () -> store.configure(ctx), "configure is once-only");
        assertThrows(IllegalStateException.class, store::start, "start requires successful validate");

        store.validate();
        store.start();
        assertThrows(IllegalStateException.class, () -> store.beginRecovery(5, new TrackerCursor(0, ""), 0));
        assertThrows(
                IllegalStateException.class, () -> store.onTrackerRecord(5, 0, TrackerWireFormat.encodeKey(1), null));

        store.onPartitionsAssigned(Set.of(0));
        assertThrows(
                IllegalStateException.class,
                () -> store.committedCursor(0, EMPTY_BATCH),
                "the cursor position is undefined before beginRecovery");
        store.close();
        store.close(); // idempotent
    }

    @Test
    void validateRejectsBadConfiguration() {
        assertThrows(
                IllegalArgumentException.class,
                () -> validateWith(Map.of("no-such-key", "1")),
                "unknown keys are startup errors");
        assertThrows(
                IllegalArgumentException.class,
                () -> validateWith(Map.of(KafkaTrackerStore.SIDECAR_MAX_BYTES_KEY, "64")));
        assertThrows(
                IllegalArgumentException.class,
                () -> validateWith(Map.of(KafkaTrackerStore.MAX_PENDING_PER_PARTITION_KEY, "0")));
        assertThrows(
                IllegalArgumentException.class,
                () -> validateWith(Map.of(KafkaTrackerStore.STALE_REBUILD_FLOOR_KEY, "0")));

        // Worst-case footprint vs heap budget (§4.4 item 7): 3 x 10^12 x 64 B >> 1 GiB.
        IllegalArgumentException footprint = assertThrows(
                IllegalArgumentException.class,
                () -> validateWith(Map.of(KafkaTrackerStore.MAX_PENDING_PER_PARTITION_KEY, "1000000000000")));
        assertTrue(footprint.getMessage().contains("worst-case index footprint"), footprint.getMessage());

        // The tracker topic is required (requiresTrackerTopic = true).
        KafkaTrackerStore noTracker = new KafkaTrackerStore();
        noTracker.configure(FixedIdentityStoreContext.withoutTrackerTopicId(3));
        assertThrows(IllegalArgumentException.class, noTracker::validate);
    }

    private static void validateWith(Map<String, String> properties) {
        KafkaTrackerStore store = new KafkaTrackerStore();
        store.configure(new FixedIdentityStoreContext(3, properties));
        store.validate();
    }

    /** A read-only sub-range view, mimicking the engine's per-reason grouping views. */
    private static DueBatch subBatch(DueBatch batch, int from, int to) {
        return new DueBatch() {
            @Override
            public int size() {
                return to - from;
            }

            @Override
            public int sourcePartition(int i) {
                return batch.sourcePartition(from + i);
            }

            @Override
            public long sourceOffset(int i) {
                return batch.sourceOffset(from + i);
            }

            @Override
            public long dispatchAtMs(int i) {
                return batch.dispatchAtMs(from + i);
            }

            @Override
            public long trackerOffset(int i) {
                return batch.trackerOffset(from + i);
            }

            @Override
            public boolean clamped(int i) {
                return batch.clamped(from + i);
            }
        };
    }
}
