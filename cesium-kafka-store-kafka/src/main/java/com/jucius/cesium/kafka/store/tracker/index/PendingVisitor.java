package com.jucius.cesium.kafka.store.tracker.index;

/**
 * Primitive visitor for {@link PartitionShard#oldestPending} — pending entries surface from the
 * arrival-log head in {@code trackerAddOffset} order, which is exactly the greedy
 * sidecar-encoding order the cursor computation of design §3.5 needs.
 */
@FunctionalInterface
public interface PendingVisitor {

    /**
     * Visits one pending entry.
     *
     * @param slotId pool slot id
     * @param sourceOffset source-topic offset
     * @param dispatchAtMs requested delivery time, epoch milliseconds
     * @param trackerAddOffset tracker offset of the entry's durable ADD record (the original
     *     offset — preserved across anomalous duplicate ADDs, invariant I5)
     * @return {@code true} to continue visiting, {@code false} to stop
     */
    boolean visit(int slotId, long sourceOffset, long dispatchAtMs, long trackerAddOffset);
}
