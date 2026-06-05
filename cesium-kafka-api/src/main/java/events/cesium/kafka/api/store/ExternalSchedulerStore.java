package events.cesium.kafka.api.store;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Archetype 2 — external scheduler store (e.g. JDBC; design §4.2). No implementation ships in v1,
 * but the SPI is fixed now so the contract cannot drift.
 *
 * <p>External stores cannot enlist their writes in the engine's Kafka transactions, so correctness
 * rests on the <em>ordering contracts</em> documented per method: idempotent upserts strictly
 * before the ingest transaction commits, dispatch marking strictly after the dispatch transaction
 * commits. The resulting baseline guarantee is
 * {@link StoreCapabilities.DispatchGuarantee#AT_LEAST_ONCE}, upgradeable to effectively-once via
 * {@link #cursorToCommit cursor reconciliation} (design §4.3).
 *
 * <p><strong>Threading.</strong> {@link #upsertScheduled} is called on the ingest thread; the
 * remaining methods follow the {@link SchedulerStore} dispatch-thread contract.
 */
public non-sealed interface ExternalSchedulerStore extends SchedulerStore {

    /**
     * Idempotent upsert keyed by {@code (sourcePartition, sourceOffset)} (ingest thread).
     *
     * <p><strong>ORDERING CONTRACT: called BEFORE the ingest transaction commits.</strong> If the
     * transaction aborts, source offsets were not committed, the batch re-polls, and the upsert
     * repeats — idempotency is what makes scheduling state exactly-once despite the write being
     * outside the transaction. Implementations SHOULD use {@link StoreContext#epoch} for
     * conditional writes so a zombie ingest worker cannot overwrite state owned by a newer
     * generation (design §4.4 item 4).
     */
    void upsertScheduled(List<ScheduledRef> refs);

    /**
     * Marks a dispatched batch as settled in the external system (dispatch thread; invoked from
     * the store's {@link SchedulerStore#onBatchCommitted} handling).
     *
     * <p><strong>ORDERING CONTRACT: called strictly AFTER the dispatch transaction commits.</strong>
     * A crash between the commit and this call means recovery sees the entries as pending and
     * re-dispatches them: the archetype's dispatch guarantee is
     * {@link StoreCapabilities.DispatchGuarantee#AT_LEAST_ONCE} unless the store also implements
     * {@link #cursorToCommit cursor reconciliation}, which closes exactly this window.
     */
    void markDispatched(DueBatch dispatched);

    /**
     * Recovery: streams every pending ref for {@code partition} into the in-memory time index
     * (dispatch thread). Must be idempotent — recovery may run repeatedly and must converge to the
     * same pending set (design §4.4 item 6). When a committed reconciliation cursor exists, rows
     * at or below it are reconciled as delivered rather than trusted as pending.
     */
    void scanPending(int partition, Consumer<ScheduledRef> sink);

    /**
     * OPTIONAL upgrade path to effectively-once dispatch.
     *
     * <p>Per-partition dispatch proceeds in the deterministic total order
     * {@code (dispatchAtMs, sourceOffset)}; the store returns a cursor string encoding "all
     * entries {@code <=} cursor dispatched", computed as if {@code inFlight} were complete. The
     * engine commits it atomically in the Kafka offset-metadata channel — the one durable
     * completion-fact channel that shares the dispatch transaction's atomicity (design §4.4
     * item 1). Recovery then reconciles rows {@code <=} cursor as delivered instead of trusting
     * row status, closing the commit/markDispatched crash window.
     *
     * @return the cursor to commit, or {@link Optional#empty()} (the default) when the store does
     *     not implement reconciliation and accepts the at-least-once window
     */
    default Optional<String> cursorToCommit(int partition, DueBatch inFlight) {
        return Optional.empty();
    }

    /**
     * How dispatch ownership of partitions is decided for this store. The v1 engine implements
     * only {@link PartitionCoordination#FOLLOW_INGEST_GROUP};
     * {@link PartitionCoordination#STORE_MANAGED} (e.g. database leases) is reserved for a later
     * release and is rejected at startup by the v1 engine.
     */
    default PartitionCoordination coordination() {
        return PartitionCoordination.FOLLOW_INGEST_GROUP;
    }

    /** Source of partition-ownership decisions for an external store's dispatch side. */
    enum PartitionCoordination {
        /**
         * Dispatch ownership mirrors the engine's Kafka consumer-group membership: the member
         * that ingests a source partition dispatches its entries. The only mode the v1 engine
         * implements.
         */
        FOLLOW_INGEST_GROUP,
        /**
         * The store arbitrates ownership itself (e.g. DB leases). Reserved; not implemented by
         * the v1 engine.
         */
        STORE_MANAGED
    }
}
