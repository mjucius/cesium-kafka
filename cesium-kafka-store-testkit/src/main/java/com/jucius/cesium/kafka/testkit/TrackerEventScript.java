package com.jucius.cesium.kafka.testkit;

import com.jucius.cesium.kafka.api.store.CompletionReason;
import com.jucius.cesium.kafka.api.store.ScheduledRef;
import com.jucius.cesium.kafka.api.store.TrackerBackedStore;
import com.jucius.cesium.kafka.api.store.TrackerRecordData;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jspecify.annotations.Nullable;

/**
 * A small DSL for synthetic tracker logs: a sequence of ADD / COMPLETE / invalid /
 * {@linkplain #commitMarker() undelivered control} records at explicit tracker offsets, with the
 * two §3.7 compaction asymmetries available as transforms.
 *
 * <p><strong>Encoding is store-owned.</strong> The script records <em>intents</em>; record bytes
 * are produced on demand by the store under test ({@code encodeSchedule} /
 * {@code encodeCompletions}), so the kit never assumes a wire format and the replayed bytes —
 * headers included — are exactly what the store would have produced through the engine.
 *
 * <p><strong>Compaction transforms</strong> (design §3.7; replay rules R1/R2):
 *
 * <ul>
 *   <li>{@link #compactAddsOfCompletedPairs()} — every ADD that has a later COMPLETE for the same
 *       key is removed while the tombstone remains: the cleaner removed the ADD because a newer
 *       same-key record exists. Replay sees orphan tombstones, which R2 must silently no-op.
 *   <li>{@link #compactCompletedPairs()} — completed pairs vanish entirely (the tombstone aged
 *       past {@code delete.retention.ms}; the cleaner removed the ADD in the same pass that
 *       started the tombstone's deletion clock, so ADD-present/tombstone-deleted cannot exist
 *       below the cleaner's reach — design §3.6 step 2). Orphan tombstones are removed too.
 * </ul>
 *
 * <p>Surviving records keep their <em>original</em> tracker offsets (compaction never renumbers a
 * log), control markers always survive (the cleaner does not remove control records), and neither
 * transform changes {@link #expectedPending() the script's truth}.
 *
 * <p><strong>Replay barrier.</strong> {@link #replayBarrier()} is the last surviving offset + 1,
 * <em>including trailing {@linkplain #commitMarker() markers}</em>: a real barrier is a
 * high-watermark snapshot, and on a transactional log (I1) the offsets directly below the HW are
 * typically the last transaction's commit marker — an undelivered offset the store can only cross
 * via the engine's {@code onTrackerPosition} report, which {@link #replayInto} models. (Compacted
 * <em>record</em> gaps immediately below the barrier cannot occur — the active segment is beyond
 * the cleaner's reach — but control-record gaps are universal there.)
 *
 * <p><strong>Limitations (by design).</strong> Scripts model healthy §3.1 logs plus compaction:
 * at most one ADD per source offset, and no ADD after its own COMPLETE (offsets never recycle).
 * Duplicate-ADD anomalies (R1) are exercised by the contract through the live path instead.
 */
public final class TrackerEventScript {

    private static final Headers NO_HEADERS = new RecordHeaders();

    private final int partition;
    private final List<OffsetOp> ops;
    private final long endOffset;

    private TrackerEventScript(int partition, List<OffsetOp> ops, long endOffset) {
        this.partition = partition;
        this.ops = ops;
        this.endOffset = endOffset;
    }

    /** A new, empty script for tracker (= source) partition {@code partition}. */
    public static TrackerEventScript forPartition(int partition) {
        return new TrackerEventScript(partition, new ArrayList<>(), 0);
    }

    /** Appends an unclamped ADD at the next tracker offset. */
    public TrackerEventScript add(long sourceOffset, long dispatchAtMs) {
        return add(sourceOffset, dispatchAtMs, false);
    }

    /** Appends an ADD at the next tracker offset. */
    public TrackerEventScript add(long sourceOffset, long dispatchAtMs, boolean clamped) {
        return append(new Op.Add(sourceOffset, dispatchAtMs, clamped));
    }

    /** Appends a {@code DISPATCHED} COMPLETE tombstone at the next tracker offset. */
    public TrackerEventScript complete(long sourceOffset) {
        return complete(sourceOffset, CompletionReason.DISPATCHED);
    }

    /** Appends a COMPLETE tombstone at the next tracker offset. */
    public TrackerEventScript complete(long sourceOffset, CompletionReason reason) {
        return append(new Op.Complete(sourceOffset, reason));
    }

    /** Appends a record with raw, store-format-violating bytes at the next tracker offset. */
    public TrackerEventScript invalid(byte[] key, byte @Nullable [] value) {
        return append(new Op.Invalid(key.clone(), value == null ? null : value.clone()));
    }

    /**
     * Appends an <em>undelivered</em> offset at the next tracker offset: a transaction control
     * record (commit/abort marker) or an aborted transaction's record — every cesium tracker
     * write is transactional (I1), so real logs carry one after every committed transaction's
     * records, including directly below the high-watermark barrier. {@link #replayInto} reports
     * the position across it without delivering a record, exactly like the engine's
     * {@code read_committed} consumer.
     */
    public TrackerEventScript commitMarker() {
        return append(Op.Marker.INSTANCE);
    }

    private TrackerEventScript append(Op op) {
        List<OffsetOp> next = new ArrayList<>(ops);
        next.add(new OffsetOp(endOffset, op));
        return new TrackerEventScript(partition, next, endOffset + 1);
    }

    /** First §3.7 asymmetry: ADDs of completed pairs removed, tombstones present. */
    public TrackerEventScript compactAddsOfCompletedPairs() {
        List<OffsetOp> survivors = new ArrayList<>();
        for (OffsetOp candidate : ops) {
            if (candidate.op() instanceof Op.Add add && hasLaterComplete(add.sourceOffset(), candidate.offset())) {
                continue;
            }
            survivors.add(candidate);
        }
        return new TrackerEventScript(partition, survivors, endOffset);
    }

    /** Second §3.7 asymmetry: completed pairs (and orphan tombstones) vanish entirely. */
    public TrackerEventScript compactCompletedPairs() {
        List<OffsetOp> survivors = new ArrayList<>();
        for (OffsetOp candidate : ops) {
            if (candidate.op() instanceof Op.Complete) {
                continue;
            }
            if (candidate.op() instanceof Op.Add add && hasLaterComplete(add.sourceOffset(), candidate.offset())) {
                continue;
            }
            survivors.add(candidate);
        }
        return new TrackerEventScript(partition, survivors, endOffset);
    }

    private boolean hasLaterComplete(long sourceOffset, long addOffset) {
        for (OffsetOp other : ops) {
            if (other.offset() > addOffset
                    && other.op() instanceof Op.Complete complete
                    && complete.sourceOffset() == sourceOffset) {
                return true;
            }
        }
        return false;
    }

    /** The single source partition this script's ops belong to. */
    public int partition() {
        return partition;
    }

    /** Offset one past the last op of the ORIGINAL script (preserved by the transforms). */
    public long endOffset() {
        return endOffset;
    }

    /**
     * The replay barrier for this (possibly compacted) log: last surviving offset + 1, trailing
     * {@linkplain #commitMarker() undelivered offsets} included (see the class javadoc).
     */
    public long replayBarrier() {
        return ops.isEmpty() ? 0 : ops.get(ops.size() - 1).offset() + 1;
    }

    /**
     * Materializes the surviving <em>deliverable</em> records at their original offsets, encoded
     * by {@code codec} ({@code encodeSchedule} for ADDs, {@code encodeCompletions} of a one-entry
     * view for tombstones, raw bytes for invalid ops). {@linkplain #commitMarker() Undelivered
     * offsets} are not materialized — prefer {@link #replayInto}, which also reports positions
     * across them.
     *
     * <p>Synthesized tombstone views carry {@code trackerOffset = -1} (and {@code dispatchAtMs =
     * 0} when no matching ADD exists in the script): completion encodings must be derivable from
     * the {@code (partition, sourceOffset)} identity alone (see
     * {@code TrackerBackedStore.encodeCompletions}).
     */
    public List<ScriptRecord> records(TrackerBackedStore codec) {
        List<ScriptRecord> records = new ArrayList<>(ops.size());
        for (OffsetOp offsetOp : ops) {
            ScriptRecord record = materialize(codec, addsBySource(), offsetOp);
            if (record != null) {
                records.add(record);
            }
        }
        return records;
    }

    /**
     * Feeds the surviving records into {@code store} via the headers-carrying
     * {@code onTrackerRecord}, encoding with the same store, and reports the consumer position
     * after every offset — undelivered control offsets included, exactly like the engine's
     * replay. The caller drives {@code beginRecovery} (cursor + {@link #replayBarrier()}) around
     * this call.
     */
    public void replayInto(TrackerBackedStore store) {
        Map<Long, Op.Add> addsBySource = addsBySource();
        for (OffsetOp offsetOp : ops) {
            ScriptRecord record = materialize(store, addsBySource, offsetOp);
            if (record != null) {
                store.onTrackerRecord(
                        partition, record.trackerOffset(), record.key(), record.value(), record.headers());
            }
            store.onTrackerPosition(partition, offsetOp.offset() + 1);
        }
    }

    private Map<Long, Op.Add> addsBySource() {
        Map<Long, Op.Add> addsBySource = new LinkedHashMap<>();
        for (OffsetOp offsetOp : ops) {
            if (offsetOp.op() instanceof Op.Add add) {
                addsBySource.putIfAbsent(add.sourceOffset(), add);
            }
        }
        return addsBySource;
    }

    private @Nullable ScriptRecord materialize(
            TrackerBackedStore codec, Map<Long, Op.Add> addsBySource, OffsetOp offsetOp) {
        return switch (offsetOp.op()) {
            case Op.Add(long sourceOffset, long dispatchAtMs, boolean clamped) -> {
                TrackerRecordData data =
                        codec.encodeSchedule(new ScheduledRef(partition, sourceOffset, dispatchAtMs, clamped));
                yield new ScriptRecord(offsetOp.offset(), data.key(), data.value(), data.headers());
            }
            case Op.Complete(long sourceOffset, CompletionReason reason) -> {
                Op.Add matching = addsBySource.get(sourceOffset);
                ArrayDueBatch view = ArrayDueBatch.builder()
                        .add(
                                partition,
                                sourceOffset,
                                matching == null ? 0 : matching.dispatchAtMs(),
                                -1,
                                matching != null && matching.clamped())
                        .build();
                List<TrackerRecordData> tombstones = codec.encodeCompletions(view, reason);
                if (tombstones.size() != 1) {
                    throw new IllegalStateException("encodeCompletions of a one-entry view must return exactly"
                            + " one record, got " + tombstones.size());
                }
                TrackerRecordData data = tombstones.get(0);
                yield new ScriptRecord(offsetOp.offset(), data.key(), data.value(), data.headers());
            }
            case Op.Invalid(byte[] key, byte @Nullable [] value) ->
                new ScriptRecord(offsetOp.offset(), key, value, NO_HEADERS);
            case Op.Marker ignored -> null;
        };
    }

    /**
     * The script's truth: source offsets with an ADD and no later COMPLETE, in ADD (tracker
     * offset) order — invariant under both compaction transforms. Invalid records and control
     * markers contribute nothing.
     */
    public List<PendingTruth> expectedPending() {
        Map<Long, PendingTruth> pending = new LinkedHashMap<>();
        for (OffsetOp offsetOp : ops) {
            switch (offsetOp.op()) {
                case Op.Add(long sourceOffset, long dispatchAtMs, boolean clamped) ->
                    pending.putIfAbsent(
                            sourceOffset, new PendingTruth(sourceOffset, dispatchAtMs, offsetOp.offset(), clamped));
                case Op.Complete(long sourceOffset, CompletionReason ignored) -> pending.remove(sourceOffset);
                case Op.Invalid ignored -> {
                    // Counted and skipped by conforming stores; never part of the truth.
                }
                case Op.Marker ignored -> {
                    // Undelivered offsets carry no state.
                }
            }
        }
        return List.copyOf(pending.values());
    }

    /** One materialized tracker record at its original offset, headers included. */
    // ArrayRecordComponent: zero-copy test transport mirroring TrackerRecordData; never compared.
    @SuppressWarnings("ArrayRecordComponent")
    public record ScriptRecord(long trackerOffset, byte[] key, byte @Nullable [] value, Headers headers) {}

    /** One expected-pending entry: the script's truth for comparisons. */
    public record PendingTruth(long sourceOffset, long dispatchAtMs, long trackerAddOffset, boolean clamped) {}

    private record OffsetOp(long offset, Op op) {}

    private sealed interface Op {
        record Add(long sourceOffset, long dispatchAtMs, boolean clamped) implements Op {}

        record Complete(long sourceOffset, CompletionReason reason) implements Op {}

        // ArrayRecordComponent: raw malformed bytes by definition; cloned on entry, never compared.
        @SuppressWarnings("ArrayRecordComponent")
        record Invalid(byte[] key, byte @Nullable [] value) implements Op {}

        /** An undelivered offset: a transaction control record or an aborted record (I1). */
        record Marker() implements Op {
            static final Marker INSTANCE = new Marker();
        }
    }
}
