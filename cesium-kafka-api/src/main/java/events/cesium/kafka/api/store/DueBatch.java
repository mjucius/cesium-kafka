package events.cesium.kafka.api.store;

/**
 * Primitive-friendly view of one batch of due entries popped by {@link SchedulerStore#pollDue}.
 *
 * <p>Implementations are expected to be backed by parallel {@code long[]} arrays — accessors take
 * an index {@code i} in {@code [0, size())} and return primitives, so iterating a 10,000-entry
 * batch allocates nothing per entry (design §4.1 force 2).
 *
 * <p><strong>Lifecycle.</strong> A batch represents in-flight entries inside exactly one engine
 * transaction. The engine resolves every batch by calling exactly one of
 * {@link SchedulerStore#onBatchCommitted} or {@link SchedulerStore#onBatchAborted} — except after
 * an <em>in-doubt</em> commit (invariant I9), when neither is called and the affected partitions
 * are dropped and re-recovered. A batch instance must not be read after it has been resolved.
 *
 * <p><strong>Threading.</strong> Produced and consumed on the dispatch thread only, like every
 * hot-path store method.
 */
public interface DueBatch {

    /** Number of entries in this batch; accessor indexes range over {@code [0, size())}. */
    int size();

    /** Source-topic partition of entry {@code i} (see {@link ScheduledRef#sourcePartition}). */
    int sourcePartition(int i);

    /** Source-topic offset of entry {@code i} — the durable identity of the entry. */
    long sourceOffset(int i);

    /** Requested delivery instant of entry {@code i}, epoch milliseconds UTC. */
    long dispatchAtMs(int i);

    /**
     * Tracker-topic offset of the entry's durable ADD record, captured at consume time. This is
     * the per-entry recovery position a {@link TrackerBackedStore} needs to compute a sound
     * committed cursor (invariant I5; design §4.4 item 2).
     *
     * @return the tracker ADD offset, or {@code -1} for {@link ExternalSchedulerStore external}
     *     stores, which have no tracker topic
     */
    long trackerOffset(int i);
}
