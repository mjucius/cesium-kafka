package events.cesium.kafka.store.tracker.index;

import events.cesium.kafka.api.store.DueBatch;
import java.util.Arrays;

/**
 * Reusable, parallel-array {@link DueBatch} produced by {@link TrackerIndex#drainDue} (design
 * §4.1 force 2: primitive accessors, no per-entry boxing). Additionally carries the pool
 * {@link #slotId(int) slot id} of every entry, which {@link TrackerIndex#onBatchCommitted} and
 * {@link TrackerIndex#onBatchAborted} use to resolve the batch without any lookup.
 *
 * <p>The instance is owned and reused by its {@code TrackerIndex}: a batch is valid until the
 * next {@code drainDue} call and must be resolved (committed/aborted) or abandoned (I9 drop)
 * before then. Arrays grow geometrically and are retained, so steady-state drains allocate
 * nothing.
 */
public final class IndexDueBatch implements DueBatch {

    private int[] partitions = new int[16];
    private long[] sourceOffsets = new long[16];
    private long[] dispatchAts = new long[16];
    private long[] trackerOffsets = new long[16];
    private int[] slotIds = new int[16];
    private int size;

    IndexDueBatch() {}

    void reset() {
        size = 0;
    }

    void add(int partition, long sourceOffset, long dispatchAtMs, long trackerOffset, int slotId) {
        if (size == partitions.length) {
            grow();
        }
        partitions[size] = partition;
        sourceOffsets[size] = sourceOffset;
        dispatchAts[size] = dispatchAtMs;
        trackerOffsets[size] = trackerOffset;
        slotIds[size] = slotId;
        size++;
    }

    private void grow() {
        int cap = partitions.length * 2;
        partitions = Arrays.copyOf(partitions, cap);
        sourceOffsets = Arrays.copyOf(sourceOffsets, cap);
        dispatchAts = Arrays.copyOf(dispatchAts, cap);
        trackerOffsets = Arrays.copyOf(trackerOffsets, cap);
        slotIds = Arrays.copyOf(slotIds, cap);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int sourcePartition(int i) {
        return partitions[i];
    }

    @Override
    public long sourceOffset(int i) {
        return sourceOffsets[i];
    }

    @Override
    public long dispatchAtMs(int i) {
        return dispatchAts[i];
    }

    @Override
    public long trackerOffset(int i) {
        return trackerOffsets[i];
    }

    /** Pool slot id of entry {@code i} — the in-flight handle for batch resolution. */
    public int slotId(int i) {
        return slotIds[i];
    }
}
