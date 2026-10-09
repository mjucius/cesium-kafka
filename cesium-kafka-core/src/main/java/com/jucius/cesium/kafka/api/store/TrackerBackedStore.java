package com.jucius.cesium.kafka.api.store;

import java.util.List;
import java.util.Set;
import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;

/**
 * The dispatch and ingest loops' view of the scheduler store (design §4–§5). The only production
 * implementation is {@code KafkaTrackerStore}; the interface stays as the seam the loop unit tests
 * script, and carries no compatibility promise (ADR-0021).
 *
 * <p>The store owns the tracker-topic wire format; the <em>engine</em> owns the Kafka clients and
 * transactions. The store only ever encodes bytes — the engine produces them with its own
 * transactional producer, inside the ingest or dispatch transaction, on the same partition number
 * as the source record. Scheduler-state mutations therefore commit or abort atomically with
 * destination writes and consumer offsets (design §4.3).
 *
 * <p><strong>Threading.</strong> {@link #encodeSchedule} is called on the ingest thread. Every
 * other method is confined to the single dispatch thread owning the affected partitions, so a
 * store needs no locking. Ingest and dispatch never share mutable state: ADDs reach the in-memory
 * index only via the tracker topic and the dispatch thread's own consumer.
 */
public interface TrackerBackedStore extends AutoCloseable {

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
     * {@link #onBatchCommitted} and {@link #onBatchAborted} receive
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
     * <p>Record headers are part of the store-owned wire format (the v1 format carries the
     * completion reason in a header, D15): without them a legitimate completion tombstone cannot be
     * told apart from the R12 forged-tombstone shape (a reason-less tombstone, the data-loss
     * primitive §2.2's count-and-skip posture exists for).
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
     * @param headers the record's headers, as delivered by the engine's tracker consumer
     */
    void onTrackerRecord(int partition, long trackerOffset, byte[] key, byte @Nullable [] value, Headers headers);

    /**
     * Reports the engine's tracker-consumer position for {@code partition} (dispatch thread).
     *
     * <p>Tracker offsets are not dense (see {@link #onTrackerRecord}):
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
     * <p>A store that ignored this hook could not recover idle partitions on a real transactional
     * log — the contract kit models control-record gaps.
     *
     * @param partition the tracker partition the position belongs to
     * @param position the consumer's next-fetch offset for the partition
     */
    void onTrackerPosition(int partition, long position);

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
     * subset</em> of the entries drained by the last {@link #pollDue}. The store
     * must treat as complete exactly the entries of the <em>given</em> batch: a drained entry NOT
     * in {@code inFlight} (the carry-over) is still pending durable truth at the moment this
     * cursor commits — it must be encoded into the sidecar or bound the overflow cut exactly like
     * a pending entry, whether the engine has already restored it via
     * {@link #onBatchAborted} or still holds it in flight. Deriving the
     * as-if-complete set from in-flight state instead of this parameter durably loses carry-over
     * entries on the first crash after such a commit.
     *
     * @param partition the partition to compute the cursor for
     * @param inFlight the batch about to commit; exactly its entries are treated as complete
     */
    TrackerCursor committedCursor(int partition, DueBatch inFlight);

    /**
     * Partitions granted to this instance by the engine's coordination source (consumer group B,
     * which subscribes to the tracker topic).
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
     * pending entries are durable in the tracker topic; memory is a
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
     *   <li><strong>Across partitions, entries surface in non-decreasing <em>effective
     *       deadline</em> order</strong> — {@code max(dispatchAtMs, the source partition's
     *       penalty not-before)}, which collapses to {@code dispatchAtMs} order when no penalty
     *       stamps are involved. The engine's poll-timeout and due-order design relies on this
     *       (design §5.5, D-12). Cross-partition <em>ties</em> are unspecified.
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
     * @param sourcePartition the source partition whose fetches are degraded
     * @param notBeforeMs epoch milliseconds before which the partition's entries must not surface
     *     from {@link #pollDue}
     */
    void penalizeSourcePartition(int sourcePartition, long notBeforeMs);

    /**
     * Number of pending (not yet resolved) entries for {@code partition}; feeds backpressure and
     * gauges. Returns {@code 0} for partitions not currently owned (never assigned, or already
     * revoked/lost) — dropped in-memory state is not an error, it is the §5.1 "memory is a cache"
     * posture, and the engine's gauges may race a rebalance.
     */
    long pendingCount(int partition);

    /**
     * The engine's transaction containing {@code batch} committed <em>definitively</em>. Finalize
     * the batch: mark entries completed, advance cursors/ring heads, free slots. After this call
     * the batch's entries must never resurface from {@link #pollDue} or recovery from the cursor
     * this store subsequently reports.
     *
     * <p><strong>Sub-batch views (truncate-and-carry-over).</strong> {@code batch} is either the
     * instance returned by {@link #pollDue} or an engine-synthesized view of a <em>subset</em> of
     * it: under the §7 byte budget (D8) or the D-8 TRANSIENT fetch exclusion the transaction
     * settles only the fetched entries, which the engine commits as a sub-batch view while
     * restoring the carry-over remainder via {@link #onBatchAborted}. Views reference entries by
     * {@code (sourcePartition, sourceOffset)}; across the views of one drained batch, every
     * drained entry is resolved exactly once.
     */
    void onBatchCommitted(DueBatch batch);

    /**
     * The engine's transaction containing {@code batch} aborted <em>definitively</em>. Restore the
     * entries to pending — they remain due and will be re-polled.
     *
     * <p><strong>Also the carry-over path of a truncated batch.</strong> Entries drained by
     * {@link #pollDue} but excluded from the transaction — the §7 byte-budget truncation (D8) or
     * the D-8 TRANSIENT fetch exclusion — are returned to pending through this callback, possibly
     * as an engine-synthesized sub-batch view while the settled remainder commits via
     * {@link #onBatchCommitted}. Views reference entries by
     * {@code (sourcePartition, sourceOffset)}.
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
