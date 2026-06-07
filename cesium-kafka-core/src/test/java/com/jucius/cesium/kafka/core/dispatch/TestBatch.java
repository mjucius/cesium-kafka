package com.jucius.cesium.kafka.core.dispatch;

import com.jucius.cesium.kafka.api.store.DueBatch;
import java.util.ArrayList;
import java.util.List;

/** Simple list-backed {@link DueBatch} for scripting {@code pollDue} results in tests. */
final class TestBatch implements DueBatch {

    private final List<long[]> rows; // {partition, sourceOffset, dispatchAtMs, trackerOffset, clamped}

    private TestBatch(List<long[]> rows) {
        this.rows = rows;
    }

    static Builder builder() {
        return new Builder();
    }

    /** A batch of unclamped entries on one partition: offsets with {@code dispatchAtMs = dueAt}. */
    static TestBatch onPartition(int partition, long dueAtMs, long... sourceOffsets) {
        Builder builder = builder();
        for (long offset : sourceOffsets) {
            builder.add(partition, offset, dueAtMs, offset * 10, false);
        }
        return builder.build();
    }

    @Override
    public int size() {
        return rows.size();
    }

    @Override
    public int sourcePartition(int i) {
        return (int) rows.get(i)[0];
    }

    @Override
    public long sourceOffset(int i) {
        return rows.get(i)[1];
    }

    @Override
    public long dispatchAtMs(int i) {
        return rows.get(i)[2];
    }

    @Override
    public long trackerOffset(int i) {
        return rows.get(i)[3];
    }

    @Override
    public boolean clamped(int i) {
        return rows.get(i)[4] != 0;
    }

    static final class Builder {
        private final List<long[]> rows = new ArrayList<>();

        Builder add(int partition, long sourceOffset, long dispatchAtMs, long trackerOffset, boolean clamped) {
            rows.add(new long[] {partition, sourceOffset, dispatchAtMs, trackerOffset, clamped ? 1 : 0});
            return this;
        }

        TestBatch build() {
            return new TestBatch(rows);
        }
    }
}
