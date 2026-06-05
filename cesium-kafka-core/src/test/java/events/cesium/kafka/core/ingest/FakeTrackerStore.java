package events.cesium.kafka.core.ingest;

import events.cesium.kafka.api.store.CompletionReason;
import events.cesium.kafka.api.store.DueBatch;
import events.cesium.kafka.api.store.ScheduledRef;
import events.cesium.kafka.api.store.SchedulerStore;
import events.cesium.kafka.api.store.StoreCapabilities;
import events.cesium.kafka.api.store.StoreContext;
import events.cesium.kafka.api.store.TrackerBackedStore;
import events.cesium.kafka.api.store.TrackerCursor;
import events.cesium.kafka.api.store.TrackerRecordData;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * Ingest-side stand-in for the tracker store: {@link #encodeSchedule} records the refs the loop
 * handed over and returns a deterministic test encoding; everything the ingest loop must never
 * touch throws — the loop talks to the {@link SchedulerStore} SPI through this one method only.
 */
final class FakeTrackerStore implements TrackerBackedStore {

    final List<ScheduledRef> encoded = new ArrayList<>();

    /** Test encoding: key = BE source offset; value = {@code clamped(1) | dispatchAtMs(8)}. */
    @Override
    public TrackerRecordData encodeSchedule(ScheduledRef ref) {
        encoded.add(ref);
        return new TrackerRecordData(key(ref.sourceOffset()), value(ref), new RecordHeaders());
    }

    static byte[] key(long sourceOffset) {
        return ByteBuffer.allocate(Long.BYTES).putLong(sourceOffset).array();
    }

    static byte[] value(ScheduledRef ref) {
        return ByteBuffer.allocate(1 + Long.BYTES)
                .put(ref.clamped() ? (byte) 1 : (byte) 0)
                .putLong(ref.dispatchAtMs())
                .array();
    }

    @Override
    public void close() {}

    // --- everything below is dispatch/lifecycle surface the ingest loop must never call ---

    @Override
    public void configure(StoreContext context) {
        throw offLimits();
    }

    @Override
    public StoreCapabilities capabilities() {
        throw offLimits();
    }

    @Override
    public void validate() {
        throw offLimits();
    }

    @Override
    public void start() {
        throw offLimits();
    }

    @Override
    public void onPartitionsAssigned(Set<Integer> partitions) {
        throw offLimits();
    }

    @Override
    public void onPartitionsRevoked(Set<Integer> partitions) {
        throw offLimits();
    }

    @Override
    public void onPartitionsLost(Set<Integer> partitions) {
        throw offLimits();
    }

    @Override
    public DueBatch pollDue(long nowMs, int maxBatch) {
        throw offLimits();
    }

    @Override
    public long nextDeadlineMs() {
        throw offLimits();
    }

    @Override
    public long pendingCount(int partition) {
        throw offLimits();
    }

    @Override
    public void onBatchCommitted(DueBatch batch) {
        throw offLimits();
    }

    @Override
    public void onBatchAborted(DueBatch batch) {
        throw offLimits();
    }

    @Override
    public void maintenance() {
        throw offLimits();
    }

    @Override
    public List<TrackerRecordData> encodeCompletions(DueBatch dispatched, CompletionReason reason) {
        throw offLimits();
    }

    @Override
    public void beginRecovery(int partition, TrackerCursor committed, long barrierOffset) {
        throw offLimits();
    }

    @Override
    public void onTrackerRecord(int partition, long trackerOffset, byte[] recordKey, byte[] recordValue) {
        throw offLimits();
    }

    @Override
    public boolean isRecovering(int partition) {
        throw offLimits();
    }

    @Override
    public TrackerCursor committedCursor(int partition, DueBatch inFlight) {
        throw offLimits();
    }

    private static UnsupportedOperationException offLimits() {
        return new UnsupportedOperationException("the ingest loop must only call encodeSchedule on the store");
    }
}
