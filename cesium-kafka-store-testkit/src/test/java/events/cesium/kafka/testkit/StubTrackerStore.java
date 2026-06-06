package events.cesium.kafka.testkit;

import events.cesium.kafka.api.store.CompletionReason;
import events.cesium.kafka.api.store.DueBatch;
import events.cesium.kafka.api.store.ScheduledRef;
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
import org.jspecify.annotations.Nullable;

/**
 * A codec-only {@link TrackerBackedStore} for fixture self-tests: a trivial wire format (8-byte BE
 * source-offset key; 9-byte value {@code dispatchAtMs(8) | clamped(1)}; null-value tombstones)
 * with every non-encoding method unsupported.
 */
final class StubTrackerStore implements TrackerBackedStore {

    @Override
    public TrackerRecordData encodeSchedule(ScheduledRef ref) {
        byte[] value = ByteBuffer.allocate(9)
                .putLong(ref.dispatchAtMs())
                .put(ref.clamped() ? (byte) 1 : 0)
                .array();
        return new TrackerRecordData(key(ref.sourceOffset()), value, new RecordHeaders());
    }

    @Override
    public List<TrackerRecordData> encodeCompletions(DueBatch dispatched, CompletionReason reason) {
        List<TrackerRecordData> tombstones = new ArrayList<>(dispatched.size());
        for (int i = 0; i < dispatched.size(); i++) {
            tombstones.add(new TrackerRecordData(key(dispatched.sourceOffset(i)), null, new RecordHeaders()));
        }
        return tombstones;
    }

    static byte[] key(long sourceOffset) {
        return ByteBuffer.allocate(8).putLong(sourceOffset).array();
    }

    static long sourceOffsetOf(byte[] key) {
        return ByteBuffer.wrap(key).getLong();
    }

    // Everything below is out of scope for a codec stub.

    @Override
    public void configure(StoreContext context) {
        throw new UnsupportedOperationException("codec stub");
    }

    @Override
    public StoreCapabilities capabilities() {
        throw new UnsupportedOperationException("codec stub");
    }

    @Override
    public void validate() {
        throw new UnsupportedOperationException("codec stub");
    }

    @Override
    public void start() {
        throw new UnsupportedOperationException("codec stub");
    }

    @Override
    public void onPartitionsAssigned(Set<Integer> partitions) {
        throw new UnsupportedOperationException("codec stub");
    }

    @Override
    public void onPartitionsRevoked(Set<Integer> partitions) {
        throw new UnsupportedOperationException("codec stub");
    }

    @Override
    public void onPartitionsLost(Set<Integer> partitions) {
        throw new UnsupportedOperationException("codec stub");
    }

    @Override
    public DueBatch pollDue(long nowMs, int maxBatch) {
        throw new UnsupportedOperationException("codec stub");
    }

    @Override
    public long nextDeadlineMs() {
        throw new UnsupportedOperationException("codec stub");
    }

    @Override
    public long pendingCount(int partition) {
        throw new UnsupportedOperationException("codec stub");
    }

    @Override
    public void onBatchCommitted(DueBatch batch) {
        throw new UnsupportedOperationException("codec stub");
    }

    @Override
    public void onBatchAborted(DueBatch batch) {
        throw new UnsupportedOperationException("codec stub");
    }

    @Override
    public void maintenance() {
        throw new UnsupportedOperationException("codec stub");
    }

    @Override
    public void close() {
        // releasing nothing is fine for a stub
    }

    @Override
    public void beginRecovery(int partition, TrackerCursor committed, long barrierOffset) {
        throw new UnsupportedOperationException("codec stub");
    }

    @Override
    public void onTrackerRecord(int partition, long trackerOffset, byte[] key, byte @Nullable [] value) {
        throw new UnsupportedOperationException("codec stub");
    }

    @Override
    public boolean isRecovering(int partition) {
        throw new UnsupportedOperationException("codec stub");
    }

    @Override
    public TrackerCursor committedCursor(int partition, DueBatch inFlight) {
        throw new UnsupportedOperationException("codec stub");
    }
}
