package events.cesium.kafka.api.store;

import java.util.Set;

/**
 * Root SPI for scheduler-state stores (design §4.2).
 *
 * <p><strong>Sealed by design (D7).</strong> The two permitted archetypes are the only
 * transaction-participation models the engine knows how to orchestrate, and the engine wires its
 * dispatch loop with an exhaustive Java 21 {@code switch} over them. Concrete stores implement
 * exactly one of {@link TrackerBackedStore} (durable writes enlisted in the engine's Kafka
 * transactions — exactly-once) or {@link ExternalSchedulerStore} (ordered out-of-band writes to an
 * external system).
 *
 * <p><strong>Thread-confinement contract (design §4.1 force 3).</strong> Lifecycle methods
 * ({@link #configure}, {@link #capabilities}, {@link #validate}, {@link #start}) are called from
 * the engine's startup thread before any loop runs. Partition-lifecycle callbacks and every
 * hot-path method — {@link #pollDue}, {@link #nextDeadlineMs}, {@link #pendingCount},
 * {@link #penalizeSourcePartition}, {@link #onBatchCommitted}, {@link #onBatchAborted},
 * {@link #maintenance}, and the archetypes' index-maintenance methods — are confined to the
 * single dispatch thread owning the affected
 * partitions. A conforming engine never calls them concurrently, so a store needs no locking on
 * these paths; archetype-specific ingest-side methods are confined to the ingest thread and
 * documented on the archetype.
 *
 * <p><strong>Lifecycle order:</strong> {@code configure → capabilities → validate → start →
 * (partition callbacks / hot path)* → close}.
 */
public sealed interface SchedulerStore extends AutoCloseable permits TrackerBackedStore, ExternalSchedulerStore {

    /**
     * Injects the engine-provided context (route identity, typed config, clock, metrics,
     * ownership epochs). Called exactly once, before any other method.
     */
    void configure(StoreContext context);

    /**
     * Declares the store's transaction affinity and guarantees. Must be constant for the life of
     * the instance and consistent with the implemented archetype; the engine selects its
     * orchestration from this declaration (design §4.3).
     */
    StoreCapabilities capabilities();

    /**
     * Store-declared preconditions, checked at startup: partition parity, the tombstone-retention
     * floor, broker {@code offset.metadata.max.bytes} vs the cursor sidecar budget, worst-case
     * index footprint vs the heap budget, external schema presence, unknown config keys — whatever
     * this store needs to be safe (design §4.4 item 7).
     *
     * <p>MUST fail fast by throwing; the engine refuses to start when validation fails. A store
     * that defers a checkable precondition to dispatch time is non-conforming.
     */
    void validate();

    /** Transitions the store to running; called once, after {@link #validate()} succeeds. */
    void start();

    /**
     * Partitions granted to this instance by the engine's coordination source (consumer group B
     * for tracker-backed stores; per {@link ExternalSchedulerStore#coordination()} otherwise).
     *
     * <p>Must be <em>idempotent</em> and tolerate <em>incremental</em> delivery: under
     * cooperative/KIP-848 protocols an assignment epoch may surface as several calls, each with a
     * subset, and re-assignment of an already-known partition must be a no-op or a clean
     * re-recovery. Runs inside the engine's rebalance handling — do O(1) bookkeeping only, no
     * I/O and no heavy work (recovery is driven separately by the dispatch loop, design §3.6).
     */
    void onPartitionsAssigned(Set<Integer> partitions);

    /**
     * Partitions cooperatively revoked. Drop all in-memory state for each partition in O(1) —
     * pending entries are durable (in the tracker topic or the external system); memory is a
     * cache. Never flush, never write: the new owner rebuilds from durable state, and a revoked
     * owner writing afterward is exactly the zombie the fencing design excludes.
     */
    void onPartitionsRevoked(Set<Integer> partitions);

    /**
     * Partitions lost without a clean revocation (member fenced or session expired). Same as
     * {@link #onPartitionsRevoked} — drop state, no flush — but the store must assume a new owner
     * may already be active elsewhere.
     */
    void onPartitionsLost(Set<Integer> partitions);

    /**
     * Pops entries due at {@code nowMs} into a batch, up to {@code maxBatch} entries. The popped
     * entries become in-flight: excluded from future polls until the batch is resolved via
     * {@link #onBatchCommitted} or {@link #onBatchAborted}.
     *
     * <p>Contract:
     *
     * <ul>
     *   <li><strong>Never blocks</strong> — this is the dispatch loop's hot path.
     *   <li><strong>Empty while recovering (I4):</strong> entries of a partition are
     *       dispatch-eligible only once its recovery has reached the replay barrier; a recovering
     *       partition contributes nothing, however overdue its entries.
     *   <li><strong>Honors per-source-partition penalty deadlines (design §7, R9):</strong>
     *       entries of a source partition carrying an unexpired
     *       {@link #penalizeSourcePartition(int, long) not-before deadline} are skipped even when
     *       due, so one degraded source partition cannot head-of-line block the rest or hot-spin
     *       the loop.
     *   <li>Within a partition, entries surface in due order ({@code dispatchAtMs}, then
     *       arrival); the engine documents delivery as "at or after the requested time".
     * </ul>
     *
     * @param nowMs current time, epoch milliseconds, from the engine's clock
     * @param maxBatch maximum entries to pop (the engine's byte budget is enforced later, in the
     *     payload-fetch pass)
     * @return the popped batch; possibly empty, never {@code null}
     */
    DueBatch pollDue(long nowMs, int maxBatch);

    /**
     * Earliest {@code dispatchAtMs} over all pending, dispatch-eligible entries, or
     * {@link Long#MAX_VALUE} when nothing is pending. Drives the dispatch loop's poll timeout —
     * the engine sleeps inside {@code poll()} until this deadline, so there is no busy-polling
     * (design §6).
     *
     * <p>Must be consistent with the penalty box: an entry skipped by an unexpired
     * {@link #penalizeSourcePartition(int, long) penalty deadline} contributes that deadline
     * instead of its (possibly past) dispatch time — a penalized-but-due entry must never drive a
     * zero poll timeout.
     */
    long nextDeadlineMs();

    /**
     * Stamps a not-before deadline on a source partition whose payload fetches failed transiently
     * (design §7.3, D22). Dispatch thread only. The engine calls this after a {@code TRANSIENT}
     * fetch outcome with an exponential-backoff deadline; a new stamp always replaces the previous
     * one, and the engine clears a partition's penalty after a successful fetch by stamping a
     * deadline in the past ({@code 0}).
     *
     * <p>While {@code notBeforeMs} lies in the future, {@link #pollDue} skips the partition's
     * entries and {@link #nextDeadlineMs} reports the penalty deadline for them — one degraded
     * source partition must neither head-of-line block healthy partitions nor hot-spin the
     * dispatch loop.
     *
     * <p>The default implementation ignores the stamp. A store inheriting it forfeits the §7
     * fetch isolation: the engine will re-poll penalized entries immediately and retry their
     * fetches without backoff.
     *
     * @param sourcePartition the source partition whose fetches are degraded
     * @param notBeforeMs epoch milliseconds before which the partition's entries must not surface
     *     from {@link #pollDue}
     */
    default void penalizeSourcePartition(int sourcePartition, long notBeforeMs) {}

    /** Number of pending (not yet resolved) entries for {@code partition}; feeds backpressure and gauges. */
    long pendingCount(int partition);

    /**
     * The engine's transaction containing {@code batch} committed <em>definitively</em>. Finalize
     * the batch: mark entries completed, advance cursors/ring heads, free slots. After this call
     * the batch's entries must never resurface from {@link #pollDue} or recovery from the cursor
     * this store subsequently reports.
     */
    void onBatchCommitted(DueBatch batch);

    /**
     * The engine's transaction containing {@code batch} aborted <em>definitively</em>. Restore the
     * entries to pending — they remain due and will be re-polled.
     *
     * <p><strong>Never called after an in-doubt commit (I9).</strong> When a
     * {@code commitTransaction} outcome is ambiguous, the broker may have committed; restoring the
     * batch would be the duplicate vector. The engine instead drops the affected partitions
     * ({@link #onPartitionsLost}-style) and re-enters recovery, letting the durable log decide
     * (design §3.8). Store implementations must therefore keep committed-batch effects recoverable
     * purely from durable state.
     */
    void onBatchAborted(DueBatch batch);

    /**
     * Amortized housekeeping — heap rebuilds, ring sweeps, gauge refresh. Called periodically by
     * the dispatch loop between transactions; must bound its work per call so it never threatens
     * {@code max.poll.interval.ms}.
     */
    void maintenance();

    /** Releases all resources. Idempotent; never throws checked exceptions. */
    @Override
    void close();
}
