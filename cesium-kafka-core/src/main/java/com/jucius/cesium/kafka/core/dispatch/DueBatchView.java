package com.jucius.cesium.kafka.core.dispatch;

import com.jucius.cesium.kafka.api.store.DueBatch;

/**
 * Immutable, array-backed {@link DueBatch} for the engine-synthesized views of design §3.2/§7:
 * the per-{@code CompletionReason} sub-batch views handed to {@code encodeCompletions}, the
 * settled/carry-over split of truncate-and-carry-over, and the {@linkplain #EMPTY empty batch} of
 * the records-free idle-advancement transaction.
 *
 * <p>The published testkit ships an equivalent type ({@code ArrayDueBatch}); this one lives in
 * {@code core} because production code cannot depend on the testkit. Views reference entries by
 * copied primitives — safe to hold across the source batch's resolution.
 */
final class DueBatchView implements DueBatch {

    /** The records-free idle-advancement transaction's batch shape (§3.5 "Advancement"). */
    static final DueBatchView EMPTY =
            new DueBatchView(new int[0], new long[0], new long[0], new long[0], new boolean[0]);

    private final int[] partitions;
    private final long[] sourceOffsets;
    private final long[] dispatchAts;
    private final long[] trackerOffsets;
    private final boolean[] clampedFlags;

    private DueBatchView(
            int[] partitions, long[] sourceOffsets, long[] dispatchAts, long[] trackerOffsets, boolean[] clampedFlags) {
        this.partitions = partitions;
        this.sourceOffsets = sourceOffsets;
        this.dispatchAts = dispatchAts;
        this.trackerOffsets = trackerOffsets;
        this.clampedFlags = clampedFlags;
    }

    /**
     * Copies the entries of {@code batch} at the first {@code count} indices of {@code indices},
     * in the given order — the shape of the engine's sub-batch views.
     */
    static DueBatchView select(DueBatch batch, int[] indices, int count) {
        int[] partitions = new int[count];
        long[] sourceOffsets = new long[count];
        long[] dispatchAts = new long[count];
        long[] trackerOffsets = new long[count];
        boolean[] clampedFlags = new boolean[count];
        for (int i = 0; i < count; i++) {
            int index = indices[i];
            partitions[i] = batch.sourcePartition(index);
            sourceOffsets[i] = batch.sourceOffset(index);
            dispatchAts[i] = batch.dispatchAtMs(index);
            trackerOffsets[i] = batch.trackerOffset(index);
            clampedFlags[i] = batch.clamped(index);
        }
        return new DueBatchView(partitions, sourceOffsets, dispatchAts, trackerOffsets, clampedFlags);
    }

    @Override
    public int size() {
        return partitions.length;
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

    @Override
    public boolean clamped(int i) {
        return clampedFlags[i];
    }
}
