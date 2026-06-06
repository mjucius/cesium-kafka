package events.cesium.kafka.testkit;

import events.cesium.kafka.api.store.DueBatch;
import java.util.Arrays;

/**
 * Immutable, array-backed {@link DueBatch} for engine-synthesized views: the records-free
 * idle-advancement transaction's {@linkplain #EMPTY empty batch}, the per-{@code CompletionReason}
 * {@linkplain #select(DueBatch, int...) sub-batch views} the engine passes to
 * {@code encodeCompletions}, and {@linkplain #snapshot(DueBatch) snapshots} of a store's (often
 * reused) batch instance taken before the batch is resolved.
 *
 * <p>Deliberately a <em>foreign</em> implementation from any store's point of view: the SPI
 * obliges stores to treat {@code encodeCompletions} as a pure function of the batch <em>view</em>
 * and to tolerate empty engine-synthesized batches in resolution callbacks, so the contract kit
 * always hands stores this type where the engine would hand them its own views.
 */
public final class ArrayDueBatch implements DueBatch {

    /** The records-free idle-advancement transaction's batch shape. */
    public static final ArrayDueBatch EMPTY = builder().build();

    private final int[] partitions;
    private final long[] sourceOffsets;
    private final long[] dispatchAts;
    private final long[] trackerOffsets;
    private final boolean[] clampedFlags;
    private final int size;

    private ArrayDueBatch(
            int[] partitions,
            long[] sourceOffsets,
            long[] dispatchAts,
            long[] trackerOffsets,
            boolean[] clampedFlags,
            int size) {
        this.partitions = partitions;
        this.sourceOffsets = sourceOffsets;
        this.dispatchAts = dispatchAts;
        this.trackerOffsets = trackerOffsets;
        this.clampedFlags = clampedFlags;
        this.size = size;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Copies every entry of {@code batch} — safe to hold across the source batch's resolution. */
    public static ArrayDueBatch snapshot(DueBatch batch) {
        Builder builder = builder();
        for (int i = 0; i < batch.size(); i++) {
            builder.add(
                    batch.sourcePartition(i),
                    batch.sourceOffset(i),
                    batch.dispatchAtMs(i),
                    batch.trackerOffset(i),
                    batch.clamped(i));
        }
        return builder.build();
    }

    /**
     * Copies the entries of {@code batch} at {@code indices}, in the given order — the shape of
     * the engine's per-reason sub-batch views.
     */
    public static ArrayDueBatch select(DueBatch batch, int... indices) {
        Builder builder = builder();
        for (int index : indices) {
            builder.add(
                    batch.sourcePartition(index),
                    batch.sourceOffset(index),
                    batch.dispatchAtMs(index),
                    batch.trackerOffset(index),
                    batch.clamped(index));
        }
        return builder.build();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int sourcePartition(int i) {
        checkIndex(i);
        return partitions[i];
    }

    @Override
    public long sourceOffset(int i) {
        checkIndex(i);
        return sourceOffsets[i];
    }

    @Override
    public long dispatchAtMs(int i) {
        checkIndex(i);
        return dispatchAts[i];
    }

    @Override
    public long trackerOffset(int i) {
        checkIndex(i);
        return trackerOffsets[i];
    }

    @Override
    public boolean clamped(int i) {
        checkIndex(i);
        return clampedFlags[i];
    }

    private void checkIndex(int i) {
        if (i < 0 || i >= size) {
            throw new IndexOutOfBoundsException("index " + i + " out of [0, " + size + ")");
        }
    }

    /** Builder; entries are appended in iteration order. */
    public static final class Builder {

        private int[] partitions = new int[8];
        private long[] sourceOffsets = new long[8];
        private long[] dispatchAts = new long[8];
        private long[] trackerOffsets = new long[8];
        private boolean[] clampedFlags = new boolean[8];
        private int size;

        private Builder() {}

        public Builder add(int partition, long sourceOffset, long dispatchAtMs, long trackerOffset, boolean clamped) {
            if (size == partitions.length) {
                int cap = size * 2;
                partitions = Arrays.copyOf(partitions, cap);
                sourceOffsets = Arrays.copyOf(sourceOffsets, cap);
                dispatchAts = Arrays.copyOf(dispatchAts, cap);
                trackerOffsets = Arrays.copyOf(trackerOffsets, cap);
                clampedFlags = Arrays.copyOf(clampedFlags, cap);
            }
            partitions[size] = partition;
            sourceOffsets[size] = sourceOffset;
            dispatchAts[size] = dispatchAtMs;
            trackerOffsets[size] = trackerOffset;
            clampedFlags[size] = clamped;
            size++;
            return this;
        }

        public ArrayDueBatch build() {
            return new ArrayDueBatch(partitions, sourceOffsets, dispatchAts, trackerOffsets, clampedFlags, size);
        }
    }
}
