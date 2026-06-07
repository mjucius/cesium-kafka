package com.jucius.cesium.kafka.testkit;

import com.jucius.cesium.kafka.api.store.DueBatch;
import com.jucius.cesium.kafka.api.store.ExternalSchedulerStore;
import com.jucius.cesium.kafka.api.store.RouteDescriptor;
import com.jucius.cesium.kafka.api.store.ScheduledRef;
import com.jucius.cesium.kafka.api.store.StoreCapabilities;
import com.jucius.cesium.kafka.api.store.StoreContext;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

/**
 * A minimal in-memory reference {@link ExternalSchedulerStore} for the testkit's self-tests: it
 * proves {@link ExternalSchedulerStoreContract} is satisfiable and the external archetype is
 * implementable, in both the at-least-once and the effectively-once (cursor reconciliation) modes.
 * It is the executable spec a real DB-backed store mirrors — read it alongside the contract.
 *
 * <p><strong>Durable rows vs in-memory index.</strong> The "external system" is a process-global,
 * route-keyed map of rows (so two incarnations of the same route share durable state, the way a
 * database connection does). Each owned partition also has an in-memory dispatch index that
 * {@code scanPending} rebuilds at recovery and {@code upsertScheduled} feeds in steady state
 * (modelling a co-located {@code FOLLOW_INGEST_GROUP} instance).
 *
 * <p><strong>Ordering, exactly as the SPI documents it.</strong> {@code upsertScheduled} writes the
 * durable row (idempotent on {@code (partition, sourceOffset)}); {@code markDispatched} flips the
 * row's dispatched flag <em>after</em> the dispatch commit. A row stays in {@code scanPending}
 * output until it is marked or reconciled — hence at-least-once. {@code cursorToCommit} encodes the
 * high-water {@code (dispatchAtMs, sourceOffset)} of the settled batch; {@link #reconcileFrom}
 * applies a committed cursor at recovery, flipping rows at or below it to dispatched and closing the
 * window (effectively-once).
 *
 * <p>Reconciliation is enabled by default; set {@code store.properties} key
 * {@value #RECONCILIATION_KEY} to {@code false} to exercise the pure at-least-once contract.
 */
final class InMemoryExternalSchedulerStore implements ExternalSchedulerStore {

    /** {@code store.properties} key toggling cursor reconciliation (default {@code true}). */
    static final String RECONCILIATION_KEY = "reference.reconciliation";

    /** The shared "external systems", keyed by route identity (cluster id + source topic id). */
    private static final Map<String, Map<Integer, TreeMap<Long, Row>>> REGISTRY = new ConcurrentHashMap<>();

    private static final Comparator<Entry> WITHIN_PARTITION =
            Comparator.comparingLong(Entry::dispatchAtMs).thenComparingLong(Entry::sourceOffset);
    private static final Comparator<Entry> BY_DEADLINE = Comparator.comparingLong(Entry::dispatchAtMs)
            .thenComparingInt(Entry::partition)
            .thenComparingLong(Entry::sourceOffset);

    // Durable, shared by route.
    private Map<Integer, TreeMap<Long, Row>> durable = new HashMap<>();
    private boolean reconciliation = true;

    // In-memory, per instance.
    private final Map<Integer, PartitionIndex> active = new HashMap<>();
    private final Set<Integer> recovering = new HashSet<>();
    private final Map<Integer, Map<Long, Entry>> inFlight = new HashMap<>();
    private final Map<Integer, long[]> reconcileHighWater = new HashMap<>();

    @Override
    public void configure(StoreContext context) {
        RouteDescriptor route = context.route();
        String identity = route.clusterId() + "|" + route.sourceTopicId();
        this.durable = REGISTRY.computeIfAbsent(identity, key -> new HashMap<>());
        this.reconciliation = context.config().getBoolean(RECONCILIATION_KEY, true);
    }

    @Override
    public StoreCapabilities capabilities() {
        return new StoreCapabilities(
                StoreCapabilities.TransactionAffinity.EXTERNAL,
                reconciliation
                        ? StoreCapabilities.DispatchGuarantee.EXACTLY_ONCE
                        : StoreCapabilities.DispatchGuarantee.AT_LEAST_ONCE,
                false,
                false);
    }

    @Override
    public void validate() {
        // The reference store has no external preconditions to check.
    }

    @Override
    public void start() {
        // Nothing to start.
    }

    // ---- Ingest side -----------------------------------------------------------------------

    @Override
    public void upsertScheduled(List<ScheduledRef> refs) {
        for (ScheduledRef ref : refs) {
            NavigableMap<Long, Row> rows = durablePartition(ref.sourcePartition());
            Row row = rows.get(ref.sourceOffset());
            if (row == null) {
                rows.put(ref.sourceOffset(), new Row(ref.dispatchAtMs(), ref.clamped(), false));
            } else {
                // Idempotent: a re-poll after an aborted ingest transaction repeats the same
                // (deterministic) schedule; never a second row.
                row.dispatchAtMs = ref.dispatchAtMs();
                row.clamped = ref.clamped();
            }
            // Co-located steady state: a freshly scheduled entry on an ACTIVE partition becomes
            // dispatch-eligible without waiting for a re-scan.
            PartitionIndex index = active.get(ref.sourcePartition());
            Row current = rows.get(ref.sourceOffset());
            if (index != null
                    && current != null
                    && !current.dispatched
                    && !isInFlight(ref.sourcePartition(), ref.sourceOffset())) {
                index.upsert(new Entry(ref.sourcePartition(), ref.sourceOffset(), ref.dispatchAtMs(), ref.clamped()));
            }
        }
    }

    // ---- Dispatch / recovery side ----------------------------------------------------------

    @Override
    public void markDispatched(DueBatch dispatched) {
        for (int i = 0; i < dispatched.size(); i++) {
            NavigableMap<Long, Row> rows = durablePartition(dispatched.sourcePartition(i));
            Row row = rows.get(dispatched.sourceOffset(i));
            if (row != null) {
                row.dispatched = true;
            }
        }
    }

    @Override
    public void scanPending(int partition, Consumer<ScheduledRef> sink) {
        NavigableMap<Long, Row> rows = durablePartition(partition);
        boolean load = recovering.contains(partition);
        PartitionIndex index = load ? new PartitionIndex() : null;
        for (Map.Entry<Long, Row> e : rows.entrySet()) {
            Row row = e.getValue();
            if (row.dispatched) {
                continue;
            }
            long sourceOffset = e.getKey();
            sink.accept(new ScheduledRef(partition, sourceOffset, row.dispatchAtMs, row.clamped));
            if (index != null) {
                index.upsert(new Entry(partition, sourceOffset, row.dispatchAtMs, row.clamped));
            }
        }
        if (index != null) {
            active.put(partition, index);
            recovering.remove(partition);
        }
    }

    @Override
    public Optional<String> cursorToCommit(int partition, DueBatch inFlightBatch) {
        if (!reconciliation) {
            return Optional.empty();
        }
        long @Nullable [] highWater = reconcileHighWater.get(partition);
        long bestAt = highWater == null ? Long.MIN_VALUE : highWater[0];
        long bestOffset = highWater == null ? Long.MIN_VALUE : highWater[1];
        boolean any = highWater != null;
        for (int i = 0; i < inFlightBatch.size(); i++) {
            if (inFlightBatch.sourcePartition(i) != partition) {
                continue;
            }
            long at = inFlightBatch.dispatchAtMs(i);
            long offset = inFlightBatch.sourceOffset(i);
            if (!any || greater(at, offset, bestAt, bestOffset)) {
                bestAt = at;
                bestOffset = offset;
                any = true;
            }
        }
        if (!any) {
            return Optional.empty();
        }
        reconcileHighWater.put(partition, new long[] {bestAt, bestOffset});
        return Optional.of(bestAt + ":" + bestOffset);
    }

    /**
     * Applies a committed reconciliation cursor at recovery (the engine read it from the
     * offset-metadata channel it was committed to): durable rows whose {@code (dispatchAtMs,
     * sourceOffset)} is at or below the cursor are reconciled as delivered, so the subsequent
     * {@code scanPending} excludes them. Not part of the SPI — the v1 SPI has no standard channel for
     * the engine to hand a committed cursor back to an external store, so the contract routes it here.
     */
    void reconcileFrom(int partition, String cursor) {
        int separator = cursor.indexOf(':');
        long cursorAt = Long.parseLong(cursor.substring(0, separator));
        long cursorOffset = Long.parseLong(cursor.substring(separator + 1));
        NavigableMap<Long, Row> rows = durablePartition(partition);
        for (Map.Entry<Long, Row> e : rows.entrySet()) {
            Row row = e.getValue();
            if (!row.dispatched && lessOrEqual(row.dispatchAtMs, e.getKey(), cursorAt, cursorOffset)) {
                row.dispatched = true;
            }
        }
        reconcileHighWater.put(partition, new long[] {cursorAt, cursorOffset});
    }

    // ---- Partition lifecycle ---------------------------------------------------------------

    @Override
    public void onPartitionsAssigned(Set<Integer> partitions) {
        for (int partition : partitions) {
            if (!active.containsKey(partition)) {
                recovering.add(partition); // RECOVERING until scanPending loads the index
            }
        }
    }

    @Override
    public void onPartitionsRevoked(Set<Integer> partitions) {
        dropPartitions(partitions);
    }

    @Override
    public void onPartitionsLost(Set<Integer> partitions) {
        dropPartitions(partitions);
    }

    private void dropPartitions(Set<Integer> partitions) {
        for (int partition : partitions) {
            active.remove(partition);
            recovering.remove(partition);
            inFlight.remove(partition);
        }
    }

    // ---- Hot path --------------------------------------------------------------------------

    @Override
    public DueBatch pollDue(long nowMs, int maxBatch) {
        List<Entry> candidates = new ArrayList<>();
        for (PartitionIndex index : active.values()) {
            for (Entry entry : index.due) {
                if (entry.dispatchAtMs() > nowMs) {
                    break; // due is sorted by (dispatchAtMs, sourceOffset)
                }
                candidates.add(entry);
            }
        }
        candidates.sort(BY_DEADLINE);
        ArrayDueBatch.Builder builder = ArrayDueBatch.builder();
        int count = Math.min(maxBatch, candidates.size());
        for (int i = 0; i < count; i++) {
            Entry entry = candidates.get(i);
            PartitionIndex index = active.get(entry.partition());
            if (index != null) {
                index.remove(entry);
            }
            inFlight.computeIfAbsent(entry.partition(), key -> new HashMap<>()).put(entry.sourceOffset(), entry);
            builder.add(entry.partition(), entry.sourceOffset(), entry.dispatchAtMs(), -1L, entry.clamped());
        }
        return builder.build();
    }

    @Override
    public long nextDeadlineMs() {
        long min = Long.MAX_VALUE;
        for (PartitionIndex index : active.values()) {
            if (!index.due.isEmpty()) {
                min = Math.min(min, index.due.first().dispatchAtMs());
            }
        }
        return min;
    }

    @Override
    public long pendingCount(int partition) {
        PartitionIndex index = active.get(partition);
        if (index == null) {
            return 0;
        }
        Map<Long, Entry> partitionInFlight = inFlight.get(partition);
        return (long) index.size() + (partitionInFlight == null ? 0 : partitionInFlight.size());
    }

    @Override
    public void onBatchCommitted(DueBatch batch) {
        for (int i = 0; i < batch.size(); i++) {
            Map<Long, Entry> partitionInFlight = inFlight.get(batch.sourcePartition(i));
            if (partitionInFlight != null) {
                partitionInFlight.remove(batch.sourceOffset(i));
            }
        }
    }

    @Override
    public void onBatchAborted(DueBatch batch) {
        for (int i = 0; i < batch.size(); i++) {
            int partition = batch.sourcePartition(i);
            Map<Long, Entry> partitionInFlight = inFlight.get(partition);
            Entry entry = partitionInFlight == null ? null : partitionInFlight.remove(batch.sourceOffset(i));
            PartitionIndex index = active.get(partition);
            if (entry != null && index != null) {
                index.upsert(entry); // restore to pending
            }
        }
    }

    @Override
    public void maintenance() {
        // No amortized housekeeping in the reference store.
    }

    @Override
    public void close() {
        active.clear();
        recovering.clear();
        inFlight.clear();
        reconcileHighWater.clear();
    }

    // ---- Internals -------------------------------------------------------------------------

    private NavigableMap<Long, Row> durablePartition(int partition) {
        return durable.computeIfAbsent(partition, key -> new TreeMap<>());
    }

    private boolean isInFlight(int partition, long sourceOffset) {
        Map<Long, Entry> partitionInFlight = inFlight.get(partition);
        return partitionInFlight != null && partitionInFlight.containsKey(sourceOffset);
    }

    private static boolean greater(long at, long offset, long otherAt, long otherOffset) {
        return at > otherAt || (at == otherAt && offset > otherOffset);
    }

    private static boolean lessOrEqual(long at, long offset, long otherAt, long otherOffset) {
        return at < otherAt || (at == otherAt && offset <= otherOffset);
    }

    /** One durable scheduler row (mutable, like a database record). */
    private static final class Row {
        private long dispatchAtMs;
        private boolean clamped;
        private boolean dispatched;

        Row(long dispatchAtMs, boolean clamped, boolean dispatched) {
            this.dispatchAtMs = dispatchAtMs;
            this.clamped = clamped;
            this.dispatched = dispatched;
        }
    }

    /** One in-memory dispatch-index entry. */
    private record Entry(int partition, long sourceOffset, long dispatchAtMs, boolean clamped) {}

    /** A partition's dispatch index: a due-ordered set plus an offset lookup. */
    private static final class PartitionIndex {
        private final TreeSet<Entry> due = new TreeSet<>(WITHIN_PARTITION);
        private final Map<Long, Entry> byOffset = new HashMap<>();

        void upsert(Entry entry) {
            Entry existing = byOffset.get(entry.sourceOffset());
            if (existing != null) {
                due.remove(existing);
            }
            due.add(entry);
            byOffset.put(entry.sourceOffset(), entry);
        }

        void remove(Entry entry) {
            due.remove(entry);
            byOffset.remove(entry.sourceOffset());
        }

        int size() {
            return due.size();
        }
    }
}
