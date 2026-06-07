package com.jucius.cesium.kafka.api.store;

import java.util.List;
import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;

/**
 * Archetype 1 — tracker-backed, exactly-once (design §4.2; v1 production implementation:
 * {@code KafkaTrackerStore}).
 *
 * <p>The store owns the tracker-topic wire format; the <em>engine</em> owns the Kafka clients and
 * transactions. The store only ever encodes bytes — the engine produces them with its own
 * transactional producer, inside the ingest or dispatch transaction, on the same partition number
 * as the source record. Scheduler-state mutations therefore commit or abort atomically with
 * destination writes and consumer offsets (design §4.3).
 *
 * <p><strong>Threading.</strong> {@link #encodeSchedule} is called on the ingest thread; all other
 * methods of this interface are confined to the dispatch thread that owns the partition, per the
 * {@link SchedulerStore} contract. Ingest and dispatch never share mutable state: ADDs reach the
 * in-memory index only via the tracker topic and the dispatch thread's own consumer.
 */
public non-sealed interface TrackerBackedStore extends SchedulerStore {

    /**
     * Encodes the durable ADD record for one scheduled entry (ingest thread).
     *
     * <p>The engine sends the returned bytes with its transactional producer inside the ingest
     * transaction, to the tracker partition with the same number as
     * {@link ScheduledRef#sourcePartition()}. If the transaction aborts, nothing becomes visible
     * and the source batch is reprocessed — at most one <em>committed</em> ADD per
     * {@code (partition, sourceOffset)} ever exists (design §3.1), which is what makes the record
     * key a sound compaction identity.
     *
     * <p>The encoding must preserve {@link ScheduledRef#clamped()}: the CLAMP marker is stamped on
     * the relay at <em>dispatch</em> time, after the index has been rebuilt purely from these
     * bytes, so the durable record is its only carrier (design §2.3).
     */
    TrackerRecordData encodeSchedule(ScheduledRef ref);

    /**
     * Encodes the completion tombstones for a dispatched batch (dispatch thread). The engine sends
     * them inside the dispatch transaction, atomically with the destination writes and the
     * committed cursor. Completion records must be tombstones ({@code value == null}) so log
     * compaction can eventually reclaim the ADD; the {@code reason} rides in a record header,
     * which survives tombstoning (design D15).
     *
     * <p><strong>Per-reason grouping (engine obligation).</strong> One dispatch transaction
     * legitimately settles entries with different completion reasons — §3.2 mixes
     * {@code FOUND → DISPATCHED} relays with {@code GONE → PAYLOAD_MISSING_DLQ} loss notices
     * (D-9). The engine groups the transaction's settled entries by reason and calls this method
     * once per group, each call receiving a sub-batch view whose entries all completed for
     * {@code reason}; the concatenation of the calls' results covers the settled set.
     * {@link SchedulerStore#onBatchCommitted} and {@link SchedulerStore#onBatchAborted} receive
     * the transaction's resolution batches (the full drained batch, or the engine's
     * settled/carry-over split views under §3.2/§7 truncate-and-carry-over) — never the
     * per-reason views.
     *
     * <p><strong>Identity-only encoding (store obligation).</strong> A completion record must be
     * derivable from the entry's {@code (sourcePartition, sourceOffset)} identity alone — the
     * compaction-key identity (design §2.2). The engine (and the published contract kit)
     * legitimately synthesizes completion views whose {@code dispatchAtMs} and
     * {@code trackerOffset} carry placeholder values ({@code 0} / {@code -1}); an encoding that
     * consulted those fields would emit tombstones that fail to apply.
     *
     * @param dispatched the in-flight entries being settled with {@code reason} — the full batch
     *     when the reasons are homogeneous, otherwise the engine's per-reason sub-batch view
     * @param reason why every entry in {@code dispatched} completed; one tombstone per entry
     */
    List<TrackerRecordData> encodeCompletions(DueBatch dispatched, CompletionReason reason);

    /**
     * Starts recovery of {@code partition} from a full committed cursor (dispatch thread).
     *
     * <p>The store decodes {@link TrackerCursor#metadata() the sidecar} and <em>seeds</em> the
     * index with its pinned entries — they carry their original tracker ADD offsets, all below
     * {@code committed.offset()}, in arrival order — before the engine streams the tracker range
     * {@code [committed.offset(), barrierOffset)} through {@link #onTrackerRecord}. Recovery must
     * be idempotent: re-running from the same cursor (e.g. after a crash mid-replay, D-6)
     * converges to the same pending set, including sidecar re-seeding (design §4.4 item 6).
     *
     * <p>While recovering, the partition contributes nothing to {@link #pollDue} (I4); the engine
     * owns the ACTIVE gate and never pauses a recovering partition for backpressure (design §4.4
     * item 5).
     *
     * @param partition the tracker (= source) partition being recovered
     * @param committed the committed cursor: position-tracking offset plus pinned-entry sidecar
     * @param barrierOffset the replay barrier — the partition's high watermark snapshotted by the
     *     engine strictly after the committed-cursor fetch resolved (I8, design §3.6)
     */
    void beginRecovery(int partition, TrackerCursor committed, long barrierOffset);

    /**
     * Applies one tracker record to the index (dispatch thread). The engine feeds replayed and
     * live records through this same callback — recovery and live tailing are one code path, so
     * recovery correctness is exercised continuously.
     *
     * <p><strong>Tracker offsets are not dense.</strong> Every cesium tracker write is
     * transactional (I1), so transaction control records (commit/abort markers) and the records
     * of aborted transactions occupy offsets that a {@code read_committed} consumer never
     * delivers through this callback. The store must never assume consecutive
     * {@code trackerOffset} values, and must not derive its replay position from delivered
     * records alone — the engine reports the consumer position separately via
     * {@link #onTrackerPosition} (which is how recovery reaches a barrier whose preceding offsets
     * are control records).
     *
     * <p>The engine calls the {@link #onTrackerRecord(int, long, byte[], byte[], Headers)
     * headers-carrying variant}; this overload exists as the delegation target for stores that do
     * not consult record headers.
     *
     * <p>The store applies the replay rules (design §3.5, invariant I7):
     *
     * <ul>
     *   <li><strong>R1 (ADD, {@code value != null}):</strong> insert the entry. An anomalous
     *       duplicate ADD for an already-pending source offset updates {@code dispatchAtMs} only
     *       and <strong>keeps the original tracker ADD offset</strong> — increasing it in place
     *       could carry the committed cursor past other pending entries (I5); warn via metric.
     *   <li><strong>R2 (COMPLETE, {@code value == null}):</strong> remove the entry if present;
     *       <strong>silently no-op if absent</strong> — expected whenever the ADD lies below the
     *       cursor and outside the sidecar, the pair was asymmetrically compacted, or the record
     *       is the store's own completion echo.
     *   <li>Records failing wire-format validation are counted and skipped — never applied, never
     *       crash the loop.
     * </ul>
     *
     * <p>Only committed records are ever delivered (the engine's tracker consumer is locked to
     * {@code read_committed}, D17).
     *
     * @param partition the tracker partition the record arrived on
     * @param trackerOffset the record's own tracker offset — the per-entry recovery position the
     *     committed cursor is computed from; never stored in the value
     * @param key the record key (store-owned wire format; for the v1 format, the big-endian source
     *     offset)
     * @param value the record value, or {@code null} for a completion tombstone
     */
    void onTrackerRecord(int partition, long trackerOffset, byte[] key, byte @Nullable [] value);

    /**
     * Applies one tracker record <em>with its headers</em> (dispatch thread). This is the variant
     * the engine calls: record headers are part of the store-owned wire format (the v1 format
     * carries the completion reason in a header, D15), and a headers-blind callback cannot
     * distinguish a legitimate completion tombstone from the R12 forged-tombstone shape (a
     * reason-less tombstone — the data-loss primitive §2.2's count-and-skip posture exists for).
     *
     * <p>The default delegates to the {@linkplain #onTrackerRecord(int, long, byte[], byte[])
     * headers-blind overload} so stores written against that signature keep working unchanged
     * (additive SPI evolution, design §4.5). Stores that enforce header-dependent validation
     * override this variant.
     *
     * @param partition the tracker partition the record arrived on
     * @param trackerOffset the record's own tracker offset (not dense — see the headers-blind
     *     overload)
     * @param key the record key (store-owned wire format)
     * @param value the record value, or {@code null} for a completion tombstone
     * @param headers the record's headers, as delivered by the engine's tracker consumer
     */
    default void onTrackerRecord(
            int partition, long trackerOffset, byte[] key, byte @Nullable [] value, Headers headers) {
        onTrackerRecord(partition, trackerOffset, key, value);
    }

    /**
     * Reports the engine's tracker-consumer position for {@code partition} (dispatch thread).
     *
     * <p>Tracker offsets are not dense (see {@link #onTrackerRecord(int, long, byte[], byte[])}):
     * the offsets immediately below the §3.6 replay barrier are almost always the previous
     * transaction's commit marker, which is never delivered as a record. A store that derived its
     * replay position from delivered records alone would therefore stall strictly below the
     * barrier and wedge an idle partition in recovery forever (the I4 gate never lifts). The
     * engine closes the gap by reporting {@code consumer.position(partition)} — which advances
     * past control records and aborted batches — after every poll that may have moved it, and
     * after the replay stream for a recovering partition reaches the barrier.
     *
     * <p>The store should treat the reported position as a high-water mark (combine with its
     * record-derived position via {@code max}; the engine may report positions out of order with
     * record delivery), promote the shard ACTIVE when the position reaches the barrier (design
     * §3.6 step 5), and use it as the live position of the all-encoded committed cursor (§3.5).
     *
     * <p>The default is a no-op for additive-evolution compatibility (design §4.5), but a
     * tracker-backed store that ignores this hook cannot recover idle partitions on a real
     * transactional log — the contract kit models control-record gaps and fails such stores.
     *
     * @param partition the tracker partition the position belongs to
     * @param position the consumer's next-fetch offset for the partition
     */
    default void onTrackerPosition(int partition, long position) {}

    /**
     * Whether {@code partition} is still replaying toward its barrier. The engine drives
     * promotion by streaming records through {@link #onTrackerRecord} and reporting the consumer
     * position through {@link #onTrackerPosition}; the store flips the shard ACTIVE — making its
     * entries dispatch-eligible — when its position reaches the barrier (design §3.6 step 5).
     */
    boolean isRecovering(int partition);

    /**
     * Computes the cursor the engine commits for {@code partition} via
     * {@code sendOffsetsToTransaction}: a position-tracking offset plus the pinned-entry sidecar
     * (versioned metadata blob, at most the validated sidecar byte budget), computed <em>as if</em>
     * {@code inFlight} were already complete — sound because the batch's tombstones commit
     * atomically with this cursor (design §3.5).
     *
     * <p><strong>Invariant I5 (must hold at every commit):</strong> the offset is monotonic per
     * partition, and every pending entry either has {@code trackerAddOffset >= offset} or is
     * encoded in the sidecar. A violation must surface as metric + log — never as a corrupted
     * commit. When the pending set does not fit the sidecar budget, the offset falls back to the
     * tracker ADD offset of the first non-encoded pending entry (the min-pending overflow
     * fallback).
     *
     * <p><strong>Sub-batch commits (truncate-and-carry-over).</strong> {@code inFlight} is the
     * set of entries the next transaction actually settles with tombstones — under the §7
     * decompressed-byte budget (D8) or the D-8 TRANSIENT fetch exclusion it is a <em>strict
     * subset</em> of the entries drained by the last {@link SchedulerStore#pollDue}. The store
     * must treat as complete exactly the entries of the <em>given</em> batch: a drained entry NOT
     * in {@code inFlight} (the carry-over) is still pending durable truth at the moment this
     * cursor commits — it must be encoded into the sidecar or bound the overflow cut exactly like
     * a pending entry, whether the engine has already restored it via
     * {@link SchedulerStore#onBatchAborted} or still holds it in flight. Deriving the
     * as-if-complete set from in-flight state instead of this parameter durably loses carry-over
     * entries on the first crash after such a commit.
     *
     * @param partition the partition to compute the cursor for
     * @param inFlight the batch about to commit; exactly its entries are treated as complete
     */
    TrackerCursor committedCursor(int partition, DueBatch inFlight);
}
