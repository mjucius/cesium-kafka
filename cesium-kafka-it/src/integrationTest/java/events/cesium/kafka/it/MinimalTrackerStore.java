package events.cesium.kafka.it;

import events.cesium.kafka.api.store.CompletionReason;
import events.cesium.kafka.api.store.DueBatch;
import events.cesium.kafka.api.store.ScheduledRef;
import events.cesium.kafka.api.store.StoreCapabilities;
import events.cesium.kafka.api.store.StoreContext;
import events.cesium.kafka.api.store.TrackerBackedStore;
import events.cesium.kafka.api.store.TrackerCursor;
import events.cesium.kafka.api.store.TrackerRecordData;
import events.cesium.kafka.store.tracker.TrackerWireFormat;
import java.util.List;
import java.util.Set;

/**
 * Minimal ingest-side {@link TrackerBackedStore} adapter for the M4 integration harness: the only
 * method the ingest loop ever calls is {@link #encodeSchedule}, which delegates to the REAL
 * {@link TrackerWireFormat} so durable ADD bytes on the tracker topic are exactly the production
 * format (key = BE source offset, 12-byte versioned value, CLAMP flag bit preserved).
 *
 * <p>Every dispatch-side and lifecycle method throws {@link UnsupportedOperationException}: M3's
 * {@code KafkaTrackerStore} replaces this adapter once the dispatch loop (M5) joins the harness.
 */
final class MinimalTrackerStore implements TrackerBackedStore {

    @Override
    public TrackerRecordData encodeSchedule(ScheduledRef ref) {
        return TrackerWireFormat.encodeAdd(ref.sourceOffset(), ref.dispatchAtMs(), ref.clamped());
    }

    @Override
    public void close() {
        // Nothing to release: this adapter owns no resources.
    }

    // --- dispatch-side surface: M3's KafkaTrackerStore replaces this adapter -------------------

    @Override
    public void configure(StoreContext context) {
        throw notImplemented();
    }

    @Override
    public StoreCapabilities capabilities() {
        throw notImplemented();
    }

    @Override
    public void validate() {
        throw notImplemented();
    }

    @Override
    public void start() {
        throw notImplemented();
    }

    @Override
    public void onPartitionsAssigned(Set<Integer> partitions) {
        throw notImplemented();
    }

    @Override
    public void onPartitionsRevoked(Set<Integer> partitions) {
        throw notImplemented();
    }

    @Override
    public void onPartitionsLost(Set<Integer> partitions) {
        throw notImplemented();
    }

    @Override
    public DueBatch pollDue(long nowMs, int maxBatch) {
        throw notImplemented();
    }

    @Override
    public long nextDeadlineMs() {
        throw notImplemented();
    }

    @Override
    public long pendingCount(int partition) {
        throw notImplemented();
    }

    @Override
    public void onBatchCommitted(DueBatch batch) {
        throw notImplemented();
    }

    @Override
    public void onBatchAborted(DueBatch batch) {
        throw notImplemented();
    }

    @Override
    public void maintenance() {
        throw notImplemented();
    }

    @Override
    public List<TrackerRecordData> encodeCompletions(DueBatch dispatched, CompletionReason reason) {
        throw notImplemented();
    }

    @Override
    public void beginRecovery(int partition, TrackerCursor committed, long barrierOffset) {
        throw notImplemented();
    }

    @Override
    public void onTrackerRecord(int partition, long trackerOffset, byte[] key, byte[] value) {
        throw notImplemented();
    }

    @Override
    public boolean isRecovering(int partition) {
        throw notImplemented();
    }

    @Override
    public TrackerCursor committedCursor(int partition, DueBatch inFlight) {
        throw notImplemented();
    }

    private static UnsupportedOperationException notImplemented() {
        return new UnsupportedOperationException(
                "dispatch-side store surface is not part of the M4 harness; M3's KafkaTrackerStore replaces this"
                        + " adapter");
    }
}
