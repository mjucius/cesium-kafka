package events.cesium.kafka.core.fetch;

import events.cesium.kafka.api.store.DueBatch;

/**
 * Array-backed {@link DueBatch} for fetch tests: entries are {@code (sourcePartition,
 * sourceOffset)} pairs in the given order — the fetcher only reads those two accessors.
 */
final class FakeDueBatch implements DueBatch {

    private final int[] partitions;
    private final long[] offsets;

    private FakeDueBatch(int[] partitions, long[] offsets) {
        this.partitions = partitions;
        this.offsets = offsets;
    }

    /** One entry per {@code (partitions[i], offsets[i])} pair, in array order. */
    static FakeDueBatch of(int[] partitions, long[] offsets) {
        if (partitions.length != offsets.length) {
            throw new IllegalArgumentException("parallel arrays must match");
        }
        return new FakeDueBatch(partitions.clone(), offsets.clone());
    }

    static FakeDueBatch empty() {
        return new FakeDueBatch(new int[0], new long[0]);
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
        return offsets[i];
    }

    @Override
    public long dispatchAtMs(int i) {
        return 0;
    }

    @Override
    public long trackerOffset(int i) {
        return -1;
    }
}
