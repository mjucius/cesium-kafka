package com.jucius.cesium.kafka.testkit;

import com.jucius.cesium.kafka.api.store.CompletionReason;
import com.jucius.cesium.kafka.api.store.DueBatch;
import com.jucius.cesium.kafka.api.store.ScheduledRef;
import com.jucius.cesium.kafka.api.store.TrackerBackedStore;
import com.jucius.cesium.kafka.api.store.TrackerCursor;
import com.jucius.cesium.kafka.api.store.TrackerRecordData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jspecify.annotations.Nullable;

/**
 * A simulated engine around one {@link TrackerBackedStore}: per-partition synthetic tracker logs,
 * the §3.6 recovery protocol (cursor → barrier → replay → promote), and the §3.2/§4.3 dispatch
 * transaction shapes (commit, truncate-and-carry-over partial commit, definitive abort, in-doubt)
 * — all through the public SPI, with the store's own encodings as the only bytes that ever reach
 * {@code onTrackerRecord}.
 *
 * <p><strong>Transactional log shape.</strong> Every cesium tracker write is transactional (I1),
 * so a real tracker partition's offsets are <em>not dense</em>: each committed transaction's
 * records are followed by a control record (the commit marker) the {@code read_committed}
 * consumer never delivers, and aborted transactions occupy undelivered offsets too. The harness
 * models this faithfully: every committed ingest transaction ({@link #schedule}) and dispatch
 * transaction ({@link #commitDispatch}, {@link #commitDispatchPartial},
 * {@link #inDoubtCommit in-doubt with broker commit}) appends a marker cell after its records;
 * an in-doubt transaction the broker <em>aborted</em> appends only undelivered cells. Replay
 * skips the undelivered offsets and reports the consumer position through
 * {@code TrackerBackedStore.onTrackerPosition} — exactly like the engine — so a conforming store
 * must promote across trailing markers rather than wait for a delivered record at the barrier.
 *
 * <p><strong>Modelled engine obligations</strong> (the contract documents them; the harness
 * enforces the call order):
 *
 * <ul>
 *   <li>{@code committedCursor} is requested only for cursors that ride the very next commit
 *       (I3: one transaction at a time), and the batch's tombstones land at offsets at or above
 *       the position the cursor was computed from (§3.5).
 *   <li>Records are fed through the headers-carrying {@code onTrackerRecord} variant (the
 *       engine's call path; the default delegates for headers-blind stores) and the consumer
 *       position is reported after every delivery and across every undelivered offset.
 *   <li>{@code onBatchCommitted}/{@code onBatchAborted} resolve only <em>definitive</em>
 *       outcomes; after an {@linkplain #inDoubtCommit in-doubt} commit neither is invoked (I9) —
 *       the affected partitions are dropped and re-recovered, and the durable log decides.
 *   <li>Under truncate-and-carry-over ({@link #commitDispatchPartial}, §3.2/§7 step 2c, D-8) the
 *       settled subset commits as a sub-batch view and the carry-over subset is restored via
 *       {@code onBatchAborted}; the cursor is computed for the <em>settled</em> view while the
 *       carry-over is still in flight, so the store must honor the parameter, not its in-flight
 *       state.
 *   <li>Completion-tombstone echoes are consumed (fed back through {@code onTrackerRecord}) only
 *       after the commit that made them visible ({@code read_committed}, D17).
 * </ul>
 *
 * <p>Single-threaded, like the dispatch loop it stands in for.
 */
public final class TrackerStoreHarness {

    private static final byte[] NO_KEY = new byte[0];
    private static final Headers NO_HEADERS = new RecordHeaders();

    /** One undelivered offset: a transaction control record or an aborted transaction's record. */
    private static final LogRecord UNDELIVERED = new LogRecord(NO_KEY, null, NO_HEADERS, true);

    private final TrackerBackedStore store;
    private final Map<Integer, List<LogRecord>> logs;
    private final Map<Integer, TrackerCursor> lastCommitted;
    private final Set<Integer> started = new TreeSet<>();

    /** A harness over a fresh store with empty logs (a brand-new route). */
    public TrackerStoreHarness(TrackerBackedStore store) {
        this(store, new HashMap<>(), new HashMap<>());
    }

    private TrackerStoreHarness(
            TrackerBackedStore store, Map<Integer, List<LogRecord>> logs, Map<Integer, TrackerCursor> lastCommitted) {
        this.store = store;
        this.logs = logs;
        this.lastCommitted = lastCommitted;
    }

    /**
     * A new harness around {@code freshStore} sharing this harness's durable state (tracker logs
     * and committed cursors) — the restart/rebalance model: memory dies, the log survives. The
     * previous store must not be used afterwards.
     */
    public TrackerStoreHarness migrateTo(TrackerBackedStore freshStore) {
        return new TrackerStoreHarness(freshStore, logs, new HashMap<>(lastCommitted));
    }

    /** The store under test, for assertions a test makes directly against the SPI. */
    public TrackerBackedStore store() {
        return store;
    }

    /** Assigns {@code partition} and runs full recovery from the last committed cursor. */
    public void startPartition(int partition) {
        store.onPartitionsAssigned(Set.of(partition));
        recover(partition);
    }

    /** Assigns and recovers each of {@code partitions} (a batch {@link #startPartition}). */
    public void startPartitions(int... partitions) {
        for (int partition : partitions) {
            startPartition(partition);
        }
    }

    /**
     * The §3.6 recovery protocol for an already-assigned partition: resolve the committed cursor,
     * snapshot the barrier (log end — the HW, including trailing control records),
     * {@code beginRecovery}, replay {@code [cursor, barrier)}.
     */
    public void recover(int partition) {
        TrackerCursor cursor = committedCursor(partition);
        long barrier = logEndOffset(partition);
        store.beginRecovery(partition, cursor, barrier);
        replayRange(partition, cursor.offset(), barrier);
        started.add(partition);
    }

    /** The I9 in-doubt epilogue (and the {@code lost}-rebalance model): drop, re-assign, recover. */
    public void dropAndRecover(int partition) {
        store.onPartitionsLost(Set.of(partition));
        store.onPartitionsAssigned(Set.of(partition));
        recover(partition);
    }

    /**
     * Feeds log records {@code [fromOffset, toOffsetExclusive)} through the headers-carrying
     * {@code onTrackerRecord}, skipping undelivered offsets (control records, aborted batches)
     * and reporting the consumer position after every offset — the engine's replay shape.
     */
    public void replayRange(int partition, long fromOffset, long toOffsetExclusive) {
        List<LogRecord> log = log(partition);
        for (long offset = fromOffset; offset < toOffsetExclusive; offset++) {
            LogRecord record = log.get((int) offset);
            if (!record.marker()) {
                store.onTrackerRecord(partition, offset, record.key(), record.value(), record.headers());
            }
            store.onTrackerPosition(partition, offset + 1);
        }
    }

    /**
     * One live scheduled entry: encoded by the store ({@code encodeSchedule}), committed to the
     * tracker log by the simulated ingest transaction — whose commit marker occupies the next
     * offset (I1) — and applied through the dispatch thread's own tailing consumer, the only path
     * by which ADDs ever reach the index (design §5.1).
     *
     * @return the ADD record's tracker offset
     */
    public long schedule(ScheduledRef ref) {
        TrackerRecordData data = store.encodeSchedule(ref);
        int partition = ref.sourcePartition();
        long offset = append(partition, new LogRecord(data.key(), data.value(), data.headers(), false));
        append(partition, UNDELIVERED); // the ingest transaction's commit marker
        deliver(partition, offset);
        reportPosition(partition); // the consumer position passes the marker
        return offset;
    }

    /**
     * Appends raw, headerless bytes (e.g. a non-transactional foreign writer's record, R12) and
     * feeds them to the store.
     */
    public long appendRaw(int partition, byte[] key, byte @Nullable [] value) {
        long offset = append(partition, new LogRecord(key, value, NO_HEADERS, false));
        deliver(partition, offset);
        return offset;
    }

    /**
     * Appends one store-encoded record — headers included, the way {@code encodeCompletions}
     * output reaches the log — and feeds it to the store.
     */
    public long appendRecord(int partition, TrackerRecordData data) {
        long offset = append(partition, new LogRecord(data.key(), data.value(), data.headers(), false));
        deliver(partition, offset);
        return offset;
    }

    /**
     * A homogeneous dispatch transaction: every entry of {@code batch} settles with
     * {@code reason}.
     */
    public Map<Integer, TrackerCursor> commitDispatch(DueBatch batch, CompletionReason reason) {
        return commitDispatch(batch, List.of(new ReasonGroup(batch, reason)));
    }

    /**
     * A dispatch transaction with per-reason sub-batch views (the engine's grouping obligation):
     * {@code encodeCompletions} once per group; cursors computed for every touched partition
     * "as if" the batch were complete; tombstones and cursors committed atomically (each touched
     * tracker partition gaining a trailing commit marker); {@code onBatchCommitted} with the FULL
     * batch; echoes consumed afterwards.
     *
     * @return the cursor committed per touched partition
     */
    public Map<Integer, TrackerCursor> commitDispatch(DueBatch batch, List<ReasonGroup> groups) {
        List<StagedRecord> staged = stageCompletions(batch, groups);
        Map<Integer, TrackerCursor> cursors = cursorsForTouched(batch);
        // commitTransaction: tombstones and cursors become durable atomically.
        List<long[]> appended = appendCommitted(staged);
        lastCommitted.putAll(cursors);
        store.onBatchCommitted(batch);
        deliverEchoes(appended);
        return cursors;
    }

    /**
     * The §3.2/§7 truncate-and-carry-over (and D-8 TRANSIENT-exclusion) transaction shape: of the
     * entries drained into {@code drained}, only {@code settledIndices} were fetched and proceed
     * to the transaction — their tombstones and cursors commit — while the rest (the carry-over)
     * returns to pending via {@code onBatchAborted} and must survive any subsequent crash.
     *
     * <p>The cursor is deliberately computed for the <em>settled sub-batch view</em> while the
     * carry-over entries are still in flight: per the {@code committedCursor} contract the store
     * must treat as complete exactly the given batch's entries, encoding the carry-over into the
     * sidecar (or bounding the overflow cut) like any pending entry — deriving the
     * as-if-complete set from in-flight state instead durably loses the carry-over on the first
     * crash after the commit.
     *
     * @param drained the batch returned by {@code pollDue}
     * @param settledIndices indices into {@code drained} of the entries the transaction settles;
     *     must be a non-empty strict subset
     * @param reason the settled entries' completion reason
     * @return the cursor committed per partition touched by the settled subset
     */
    public Map<Integer, TrackerCursor> commitDispatchPartial(
            DueBatch drained, int[] settledIndices, CompletionReason reason) {
        if (settledIndices.length == 0 || settledIndices.length >= drained.size()) {
            throw new IllegalArgumentException("settledIndices must be a non-empty strict subset of the drained"
                    + " batch: got " + settledIndices.length + " of " + drained.size());
        }
        boolean[] settledAt = new boolean[drained.size()];
        for (int index : settledIndices) {
            if (settledAt[index]) {
                throw new IllegalArgumentException("duplicate settled index " + index);
            }
            settledAt[index] = true;
        }
        int[] carryIndices = new int[drained.size() - settledIndices.length];
        int next = 0;
        for (int i = 0; i < drained.size(); i++) {
            if (!settledAt[i]) {
                carryIndices[next++] = i;
            }
        }
        ArrayDueBatch settled = ArrayDueBatch.select(drained, settledIndices);
        ArrayDueBatch carryOver = ArrayDueBatch.select(drained, carryIndices);

        List<StagedRecord> staged = stageCompletions(settled, List.of(new ReasonGroup(settled, reason)));
        Map<Integer, TrackerCursor> cursors = cursorsForTouched(settled);
        List<long[]> appended = appendCommitted(staged);
        lastCommitted.putAll(cursors);
        store.onBatchCommitted(settled);
        store.onBatchAborted(carryOver); // the carry-over returns to pending (§3.2/§7 step 2c)
        deliverEchoes(appended);
        return cursors;
    }

    /** A definitively aborted dispatch transaction: nothing durable, entries restored. */
    public void abortDispatch(DueBatch batch) {
        store.onBatchAborted(batch);
    }

    /**
     * An in-doubt {@code commitTransaction} outcome (I9, design §3.8): the broker either
     * committed ({@code brokerCommitted}) — tombstones, commit markers and cursors durable — or
     * aborted, in which case the transaction's records still occupy undelivered offsets (aborted
     * batches consume offsets, plus the abort marker); the engine cannot know, so it resolves
     * NEITHER callback. Callers follow with {@link #dropAndRecover} for every touched partition,
     * exactly like the engine.
     */
    public void inDoubtCommit(DueBatch batch, CompletionReason reason, boolean brokerCommitted) {
        List<StagedRecord> staged = stageCompletions(batch, List.of(new ReasonGroup(batch, reason)));
        Map<Integer, TrackerCursor> cursors = cursorsForTouched(batch);
        if (brokerCommitted) {
            appendCommitted(staged);
            lastCommitted.putAll(cursors);
        } else {
            // Aborted records occupy offsets a read_committed consumer never delivers.
            Set<Integer> touched = new TreeSet<>();
            for (StagedRecord record : staged) {
                append(record.partition(), UNDELIVERED);
                touched.add(record.partition());
            }
            for (int partition : touched) {
                append(partition, UNDELIVERED); // the abort marker
            }
        }
        // I9: neither onBatchCommitted nor onBatchAborted — the durable log is authoritative.
    }

    /** The records-free idle-advancement transaction for one partition (§3.5 "Advancement"). */
    public TrackerCursor commitIdle(int partition) {
        TrackerCursor cursor = store.committedCursor(partition, ArrayDueBatch.EMPTY);
        lastCommitted.put(partition, cursor);
        store.onBatchCommitted(ArrayDueBatch.EMPTY);
        return cursor;
    }

    /** The last durably committed cursor, or the provable-first-run cursor {@code (0, "")}. */
    public TrackerCursor committedCursor(int partition) {
        return lastCommitted.getOrDefault(partition, new TrackerCursor(0, ""));
    }

    /** Log end offset == high watermark == the recovery barrier for {@code partition}. */
    public long logEndOffset(int partition) {
        return log(partition).size();
    }

    /**
     * Snapshot of every dispatch-eligible pending entry, taken non-destructively (drain
     * everything, then definitively abort — restoring is exact for a conforming store). Sorted by
     * {@code (partition, sourceOffset)} for comparisons.
     *
     * @throws IllegalStateException if a started partition is still recovering (its entries would
     *     be invisible, making the snapshot a lie)
     */
    public List<PendingEntry> pendingSnapshot() {
        for (int partition : started) {
            if (store.isRecovering(partition)) {
                throw new IllegalStateException("pendingSnapshot while partition " + partition + " is recovering");
            }
        }
        DueBatch batch = store.pollDue(Long.MAX_VALUE, Integer.MAX_VALUE);
        List<PendingEntry> entries = new ArrayList<>(batch.size());
        for (int i = 0; i < batch.size(); i++) {
            entries.add(new PendingEntry(
                    batch.sourcePartition(i),
                    batch.sourceOffset(i),
                    batch.dispatchAtMs(i),
                    batch.trackerOffset(i),
                    batch.clamped(i)));
        }
        store.onBatchAborted(batch);
        entries.sort(null);
        return entries;
    }

    private List<StagedRecord> stageCompletions(DueBatch batch, List<ReasonGroup> groups) {
        List<StagedRecord> staged = new ArrayList<>(batch.size());
        int covered = 0;
        for (ReasonGroup group : groups) {
            DueBatch view = group.view();
            List<TrackerRecordData> tombstones = store.encodeCompletions(view, group.reason());
            if (tombstones.size() != view.size()) {
                throw new IllegalStateException("encodeCompletions must return one tombstone per entry: got "
                        + tombstones.size() + " for " + view.size());
            }
            for (int i = 0; i < view.size(); i++) {
                staged.add(new StagedRecord(view.sourcePartition(i), tombstones.get(i)));
            }
            covered += view.size();
        }
        if (covered != batch.size()) {
            throw new IllegalArgumentException(
                    "reason groups cover " + covered + " entries, batch has " + batch.size());
        }
        return staged;
    }

    /**
     * Appends a committed transaction's records, then one commit marker per touched partition
     * (the control record a read_committed consumer never sees).
     *
     * @return the {@code (partition, offset)} of every appended <em>record</em>, in append order
     */
    private List<long[]> appendCommitted(List<StagedRecord> staged) {
        List<long[]> appended = new ArrayList<>(staged.size());
        Set<Integer> touched = new TreeSet<>();
        for (StagedRecord record : staged) {
            TrackerRecordData data = record.data();
            long offset = append(record.partition(), new LogRecord(data.key(), data.value(), data.headers(), false));
            appended.add(new long[] {record.partition(), offset});
            touched.add(record.partition());
        }
        for (int partition : touched) {
            append(partition, UNDELIVERED); // the dispatch transaction's commit marker
        }
        return appended;
    }

    /** The tailing consumer reads the store's own committed completion echoes (R2 no-ops). */
    private void deliverEchoes(List<long[]> appended) {
        Set<Integer> touched = new TreeSet<>();
        for (long[] partitionAndOffset : appended) {
            int partition = (int) partitionAndOffset[0];
            deliver(partition, partitionAndOffset[1]);
            touched.add(partition);
        }
        for (int partition : touched) {
            reportPosition(partition); // the consumer position passes the trailing marker
        }
    }

    private void deliver(int partition, long offset) {
        LogRecord record = log(partition).get((int) offset);
        store.onTrackerRecord(partition, offset, record.key(), record.value(), record.headers());
        store.onTrackerPosition(partition, offset + 1);
    }

    private void reportPosition(int partition) {
        store.onTrackerPosition(partition, logEndOffset(partition));
    }

    private Map<Integer, TrackerCursor> cursorsForTouched(DueBatch batch) {
        Set<Integer> touched = new TreeSet<>();
        for (int i = 0; i < batch.size(); i++) {
            touched.add(batch.sourcePartition(i));
        }
        Map<Integer, TrackerCursor> cursors = new LinkedHashMap<>();
        for (int partition : touched) {
            cursors.put(partition, store.committedCursor(partition, batch));
        }
        return cursors;
    }

    private List<LogRecord> log(int partition) {
        return logs.computeIfAbsent(partition, ignored -> new ArrayList<>());
    }

    private long append(int partition, LogRecord record) {
        List<LogRecord> log = log(partition);
        long offset = log.size();
        log.add(record);
        return offset;
    }

    /** One per-reason sub-batch view paired with its {@link CompletionReason}. */
    public record ReasonGroup(DueBatch view, CompletionReason reason) {}

    /** One observed pending entry, comparable for set equality across store incarnations. */
    public record PendingEntry(
            int partition, long sourceOffset, long dispatchAtMs, long trackerAddOffset, boolean clamped)
            implements Comparable<PendingEntry> {

        @Override
        public int compareTo(PendingEntry other) {
            int byPartition = Integer.compare(partition, other.partition);
            if (byPartition != 0) {
                return byPartition;
            }
            return Long.compare(sourceOffset, other.sourceOffset);
        }
    }

    // ArrayRecordComponent: zero-copy log cell mirroring TrackerRecordData; never compared.
    // marker == true models an undelivered offset (control record / aborted batch).
    @SuppressWarnings("ArrayRecordComponent")
    private record LogRecord(byte[] key, byte @Nullable [] value, Headers headers, boolean marker) {}

    private record StagedRecord(int partition, TrackerRecordData data) {}
}
