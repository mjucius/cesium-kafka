package events.cesium.kafka.testkit;

import events.cesium.kafka.api.store.DueBatch;
import events.cesium.kafka.api.store.ExternalSchedulerStore;
import events.cesium.kafka.api.store.ScheduledRef;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * A simulated engine around one {@link ExternalSchedulerStore}: it drives the §4.3 ordering
 * contracts of the external archetype through the public SPI, the way the real engine would on a
 * co-located ({@code FOLLOW_INGEST_GROUP}) instance.
 *
 * <p><strong>What the harness models</strong> (the contract documents the obligations; the harness
 * enforces the call order):
 *
 * <ul>
 *   <li><strong>Ingest (before commit):</strong> {@link #schedule} calls
 *       {@link ExternalSchedulerStore#upsertScheduled} — the idempotent upsert that runs strictly
 *       <em>before</em> the ingest transaction commits, so a re-poll after an aborted ingest
 *       transaction simply repeats it.
 *   <li><strong>Dispatch (after commit):</strong> {@link #commitDispatch} models the engine
 *       committing the Kafka dispatch transaction and then performing the out-of-band durable
 *       settle: it captures the {@linkplain ExternalSchedulerStore#cursorToCommit reconciliation
 *       cursor} (committed in the offset-metadata channel), finalizes the in-memory batch via
 *       {@code onBatchCommitted}, and only <em>then</em> calls {@code markDispatched}.
 *   <li><strong>The at-least-once crash window:</strong> {@link #commitDispatchLosingMarkDispatched}
 *       captures the cursor and finalizes in memory but <em>omits</em> {@code markDispatched} — the
 *       crash between the dispatch-transaction commit and the after-commit durable write. The
 *       entry's durable row stays pending, which is exactly the
 *       {@link events.cesium.kafka.api.store.StoreCapabilities.DispatchGuarantee#AT_LEAST_ONCE}
 *       window the cursor reconciliation closes.
 *   <li><strong>Recovery:</strong> {@link #recover} assigns the partition and streams
 *       {@code scanPending} into the store's dispatch index. Cursor reconciliation (when the store
 *       implements it) is delivered by the contract <em>between</em> {@code onPartitionsAssigned}
 *       and the scan, since the engine reads the committed cursor from the offset-metadata channel
 *       at recovery.
 * </ul>
 *
 * <p>The committed reconciliation cursors live in this harness (modelling the offset-metadata
 * channel they are durably committed to) and survive a {@linkplain #migrateTo restart}. The
 * durable scheduler rows live in the store's own external system; a conforming reference store
 * shares them across incarnations of the <em>same route</em>, the way a real database does.
 *
 * <p>Single-threaded, like the co-located loop it stands in for.
 */
public final class ExternalStoreHarness {

    private final ExternalSchedulerStore store;
    private final Map<Integer, String> committedCursors;

    /** A harness over a fresh store with no committed cursors (a brand-new route). */
    public ExternalStoreHarness(ExternalSchedulerStore store) {
        this(store, new HashMap<>());
    }

    private ExternalStoreHarness(ExternalSchedulerStore store, Map<Integer, String> committedCursors) {
        this.store = store;
        this.committedCursors = committedCursors;
    }

    /**
     * A new harness around {@code freshStore} sharing this harness's durably-committed reconciliation
     * cursors — the restart model for the offset-metadata channel. The durable scheduler rows are
     * shared by the store itself (same route ⇒ same external system); the previous store must not be
     * used afterwards.
     */
    public ExternalStoreHarness migrateTo(ExternalSchedulerStore freshStore) {
        return new ExternalStoreHarness(freshStore, new HashMap<>(committedCursors));
    }

    /** The store under test, for assertions a test makes directly against the SPI. */
    public ExternalSchedulerStore store() {
        return store;
    }

    /** Ingest of one entry: the idempotent upsert that runs before the ingest transaction commits. */
    public void schedule(ScheduledRef ref) {
        store.upsertScheduled(List.of(ref));
    }

    /** Ingest of a batch of entries (one ingest transaction's worth). */
    public void scheduleAll(List<ScheduledRef> refs) {
        store.upsertScheduled(refs);
    }

    /** Grants {@code partition} to the store (recovery is driven separately, via {@link #recover}). */
    public void assign(int partition) {
        store.onPartitionsAssigned(Set.of(partition));
    }

    /**
     * Streams every durable pending ref for {@code partition} into the store via
     * {@code scanPending}, also collecting them for assertions. When the partition has just been
     * assigned (and not yet scanned), a conforming store loads its dispatch index here.
     */
    public List<ScheduledRef> scan(int partition) {
        List<ScheduledRef> collected = new ArrayList<>();
        store.scanPending(partition, collected::add);
        return collected;
    }

    /** Assigns {@code partition} and runs recovery (scanPending → dispatch index) with no cursor. */
    public void recover(int partition) {
        assign(partition);
        scan(partition);
    }

    /**
     * The normal dispatch settle: capture the reconciliation cursor (offset-metadata channel),
     * finalize the in-memory batch ({@code onBatchCommitted}), then perform the after-commit durable
     * write ({@code markDispatched}).
     */
    public void commitDispatch(DueBatch batch) {
        captureCursors(batch);
        store.onBatchCommitted(batch);
        store.markDispatched(batch);
    }

    /**
     * The at-least-once crash window: the Kafka dispatch transaction committed (cursor captured,
     * in-memory batch finalized) but the process died before the after-commit
     * {@code markDispatched}. The durable rows therefore stay pending and reappear on recovery
     * unless cursor reconciliation closes the window.
     */
    public void commitDispatchLosingMarkDispatched(DueBatch batch) {
        captureCursors(batch);
        store.onBatchCommitted(batch);
        // markDispatched intentionally NOT called.
    }

    /** A definitively aborted dispatch transaction: nothing settled, entries restored to pending. */
    public void abortDispatch(DueBatch batch) {
        store.onBatchAborted(batch);
    }

    /** The reconciliation cursor durably committed for {@code partition}, if the store produced one. */
    public Optional<String> committedCursor(int partition) {
        return Optional.ofNullable(committedCursors.get(partition));
    }

    private void captureCursors(DueBatch batch) {
        Set<Integer> touched = new TreeSet<>();
        for (int i = 0; i < batch.size(); i++) {
            touched.add(batch.sourcePartition(i));
        }
        for (int partition : touched) {
            store.cursorToCommit(partition, batch).ifPresent(cursor -> committedCursors.put(partition, cursor));
        }
    }
}
