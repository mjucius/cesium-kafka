package events.cesium.kafka.store.tracker.index;

/**
 * Primitive sink for entries popped by {@link PartitionShard#drainDue}. Receives each popped
 * entry's identity without boxing or per-entry allocation; implementations are expected to be
 * long-lived and reused across drains.
 */
@FunctionalInterface
public interface DueEntrySink {

    /**
     * Accepts one popped (now in-flight) entry.
     *
     * @param slotId pool slot id — the handle later passed to
     *     {@link PartitionShard#finalizeSlot(int)} or {@link PartitionShard#restoreSlot(int)}
     * @param sourceOffset source-topic offset (durable identity)
     * @param dispatchAtMs requested delivery time, epoch milliseconds
     * @param trackerAddOffset tracker offset of the entry's durable ADD record
     */
    void accept(int slotId, long sourceOffset, long dispatchAtMs, long trackerAddOffset);
}
