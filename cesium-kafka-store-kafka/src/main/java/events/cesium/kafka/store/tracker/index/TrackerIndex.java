package events.cesium.kafka.store.tracker.index;

import events.cesium.kafka.api.store.DueBatch;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Arrays;

/**
 * Multi-shard container: one {@link PartitionShard} per owned tracker partition, plus
 * cross-partition scheduling (design §5.5) and the per-source-partition penalty box (design §7.3,
 * D22, R9). This is the M2 data-structure layer the M3 {@code KafkaTrackerStore} builds on; it
 * has no Kafka clients, no wire format and no metrics wiring — only plain counters.
 *
 * <p><strong>Partition selection.</strong> Tracker partition <em>p</em> always carries source
 * partition <em>p</em> (design §2.1), so shard keys and penalty keys share one number space.
 * {@link #drainDue} and {@link #nextDeadlineMs} recompute the minimum effective deadline over the
 * assigned shards on every read instead of maintaining an incremental partition heap — design
 * §5.5 sizes that structure at "owned partitions", so an O(P) scan is cache-friendly, trivially
 * correct as shard deadlines move (every {@code applyAdd}/duplicate-ADD/complete/restore can
 * change a shard's minimum), and allocation-free. A partition's <em>effective</em> deadline is
 * {@code max(shard.nextDeadlineMs(), penaltyNotBefore)} — a penalized-but-due entry must never
 * drive a zero poll timeout (see {@code SchedulerStore.nextDeadlineMs}).
 *
 * <p><strong>Recovery gating (I4, design §4.2/§4.4 item 5).</strong> Each shard carries a
 * {@link #setDispatchEligible(int, boolean) dispatch-eligibility} flag consulted by both
 * selection loops: an ineligible shard contributes nothing to {@link #drainDue} or
 * {@link #nextDeadlineMs}, however overdue its entries — the primitive M3 uses to keep a
 * RECOVERING partition's replayed entries undrainable until its position reaches the replay
 * barrier (dispatching mid-replay is the §3.6 duplicate vector: a COMPLETE tombstone between the
 * cursor and the barrier may not have been applied yet). The replay-side methods —
 * {@link #applyAdd}, {@link #applyComplete}, {@link #oldestPending}, {@link #pendingCount} — are
 * deliberately ungated: replay must populate the shard and the cursor computation needs it. The
 * flag is deliberately separate from the penalty box, whose replace-on-stamp/clear-by-past
 * semantics are owned by the engine's §7 fetch-isolation logic.
 *
 * <p><strong>Penalty box semantics</strong> (mirrors {@code SchedulerStore.penalizeSourcePartition}):
 * a new stamp always replaces the previous one; the engine clears a penalty by stamping a past
 * deadline (e.g. {@code 0}) — an expired stamp has no effect because only {@code max(deadline,
 * notBefore)} is ever consulted. Penalties are keyed by partition, not by shard: they survive
 * revoke/re-assign cycles, matching the engine's view of source-partition health.
 *
 * <p><strong>Lifecycle.</strong> {@link #revokePartition}/{@link #lostPartition} drop the shard
 * reference in O(1) — pending entries are durable in the tracker topic; memory is a cache.
 * Resolving a batch whose partition was dropped meanwhile (the I9 drop path) is a silent no-op
 * for those entries <em>while the partition remains unassigned</em>; once the partition is
 * re-assigned (the I9 procedure re-recovers immediately), the new shard carries a fresh
 * drain-generation stamp and resolving a stale batch is detected per entry — counted as an
 * anomaly and skipped, never dereferenced against the new shard's pool (whose slot ids may
 * collide with the abandoned batch's). The engine is still obliged to abandon batches across
 * drops (see {@link IndexDueBatch}); the stamp turns that obligation's violation into a metric
 * instead of silent corruption.
 *
 * <p><strong>Threading/allocation.</strong> Single dispatch thread; all hot paths allocate
 * nothing in steady state (the returned {@link IndexDueBatch} is reused across drains).
 */
public final class TrackerIndex {

    private final Int2ObjectOpenHashMap<PartitionShard> shardByPartition = new Int2ObjectOpenHashMap<>();
    private final Int2LongOpenHashMap penaltyNotBefore = new Int2LongOpenHashMap(); // default 0

    // Dense parallel views of shardByPartition for iterator-free, allocation-free scans.
    private int[] partitionIds = new int[8];
    private PartitionShard[] shards = new PartitionShard[8];
    private boolean[] dispatchEligible = new boolean[8];
    private int shardCount;

    private final IndexDueBatch batch = new IndexDueBatch();
    private final BatchSink sink = new BatchSink();

    private final int staleRebuildFloor;
    private final int completedSweepFloor;

    /** Source of the per-shard drain-generation stamps (see {@link PartitionShard#epoch()}). */
    private long nextShardEpoch = 1;

    public TrackerIndex() {
        this(PartitionShard.DEFAULT_STALE_REBUILD_FLOOR, PartitionShard.DEFAULT_COMPLETED_SWEEP_FLOOR);
    }

    /** Maintenance floors are per shard; see {@link PartitionShard#PartitionShard(int, int)}. */
    public TrackerIndex(int staleRebuildFloor, int completedSweepFloor) {
        this.staleRebuildFloor = staleRebuildFloor;
        this.completedSweepFloor = completedSweepFloor;
    }

    /**
     * Creates an empty shard for {@code partition}; idempotent (re-assignment is a no-op). A
     * fresh shard starts {@link #setDispatchEligible(int, boolean) dispatch-eligible}; gating it
     * for recovery (I4) is the caller's responsibility (M3's {@code beginRecovery}).
     */
    public void assignPartition(int partition) {
        if (shardByPartition.containsKey(partition)) {
            return;
        }
        PartitionShard shard = new PartitionShard(staleRebuildFloor, completedSweepFloor);
        shard.epoch(nextShardEpoch++);
        shardByPartition.put(partition, shard);
        if (shardCount == shards.length) {
            partitionIds = Arrays.copyOf(partitionIds, shardCount * 2);
            shards = Arrays.copyOf(shards, shardCount * 2);
            dispatchEligible = Arrays.copyOf(dispatchEligible, shardCount * 2);
        }
        partitionIds[shardCount] = partition;
        shards[shardCount] = shard;
        dispatchEligible[shardCount] = true;
        shardCount++;
    }

    /** Drops the shard in O(1); a subsequent {@link #assignPartition} starts clean (and eligible). */
    public void revokePartition(int partition) {
        PartitionShard removed = shardByPartition.remove(partition);
        if (removed == null) {
            return;
        }
        for (int i = 0; i < shardCount; i++) {
            if (partitionIds[i] == partition) {
                shardCount--;
                partitionIds[i] = partitionIds[shardCount];
                shards[i] = shards[shardCount];
                dispatchEligible[i] = dispatchEligible[shardCount];
                shards[shardCount] = null;
                break;
            }
        }
    }

    /** Same as {@link #revokePartition}: drop state, no flush (a new owner may be live). */
    public void lostPartition(int partition) {
        revokePartition(partition);
    }

    public boolean isAssigned(int partition) {
        return shardByPartition.containsKey(partition);
    }

    /**
     * Gates or ungates {@code partition}'s entries for dispatch — the I4 recovery primitive (see
     * class javadoc). While ineligible, the shard contributes nothing to {@link #drainDue} or
     * {@link #nextDeadlineMs}; the replay-side methods stay ungated. M3's {@code beginRecovery}
     * sets {@code false}; promotion to ACTIVE ({@code position >= barrier}, design §3.6 step 5)
     * sets {@code true}. A fresh assignment starts eligible.
     *
     * @throws IllegalStateException if {@code partition} is not assigned
     */
    public void setDispatchEligible(int partition, boolean eligible) {
        for (int i = 0; i < shardCount; i++) {
            if (partitionIds[i] == partition) {
                dispatchEligible[i] = eligible;
                return;
            }
        }
        throw new IllegalStateException("partition " + partition + " is not assigned");
    }

    /** Whether {@code partition}'s entries are dispatch-eligible; an unassigned partition is not. */
    public boolean isDispatchEligible(int partition) {
        for (int i = 0; i < shardCount; i++) {
            if (partitionIds[i] == partition) {
                return dispatchEligible[i];
            }
        }
        return false;
    }

    public int assignedPartitionCount() {
        return shardCount;
    }

    /** Unclamped variant of {@link #applyAdd(int, long, long, long, boolean)}. */
    public boolean applyAdd(int partition, long sourceOffset, long dispatchAtMs, long trackerAddOffset) {
        return applyAdd(partition, sourceOffset, dispatchAtMs, trackerAddOffset, false);
    }

    /** Routes an ADD to the owning shard; see {@link PartitionShard#applyAdd(long, long, long, boolean)}. */
    public boolean applyAdd(
            int partition, long sourceOffset, long dispatchAtMs, long trackerAddOffset, boolean clamped) {
        return requireShard(partition).applyAdd(sourceOffset, dispatchAtMs, trackerAddOffset, clamped);
    }

    /** Routes a COMPLETE to the owning shard; see {@link PartitionShard#applyComplete}. */
    public boolean applyComplete(int partition, long sourceOffset) {
        return requireShard(partition).applyComplete(sourceOffset);
    }

    /**
     * Drains due entries across partitions in effective-deadline order, skipping penalized and
     * dispatch-ineligible partitions, up to {@code maxBatch}. Each iteration re-selects the
     * minimum-effective-deadline shard (O(owned partitions), see class javadoc) and pops one
     * entry, so the batch is globally ordered by <em>effective</em> deadline —
     * {@code max(dispatchAtMs, the partition's penalty stamp)} — which equals {@code dispatchAtMs}
     * order only when no penalty stamps are involved (an entry behind an expired-but-nonzero
     * stamp legally drains after younger entries of healthy partitions). Ties across shards
     * surface in unspecified order; within a partition, entries surface in
     * ({@code dispatchAtMs}, then arrival) order per the {@code SchedulerStore.pollDue} contract.
     *
     * @return the reused batch — valid until the next call; resolve via
     *     {@link #onBatchCommitted}/{@link #onBatchAborted} (or abandon after an I9 drop)
     */
    public IndexDueBatch drainDue(long nowMs, int maxBatch) {
        batch.reset();
        while (batch.size() < maxBatch) {
            PartitionShard best = null;
            long bestDeadline = Long.MAX_VALUE;
            int bestPartition = -1;
            for (int i = 0; i < shardCount; i++) {
                if (!dispatchEligible[i]) {
                    continue; // I4: a recovering shard contributes nothing, however overdue
                }
                long deadline = shards[i].nextDeadlineMs();
                if (deadline == Long.MAX_VALUE) {
                    continue;
                }
                long effective = Math.max(deadline, penaltyNotBefore.get(partitionIds[i]));
                if (effective < bestDeadline) {
                    bestDeadline = effective;
                    best = shards[i];
                    bestPartition = partitionIds[i];
                }
            }
            if (best == null || bestDeadline > nowMs) {
                break;
            }
            sink.partition = bestPartition;
            sink.shard = best;
            if (best.drainDue(nowMs, 1, sink) == 0) {
                break; // unreachable with a consistent shard; defensive against infinite loops
            }
        }
        return batch;
    }

    /**
     * Minimum effective deadline over all assigned, dispatch-eligible shards, or
     * {@link Long#MAX_VALUE} when nothing is pending. A penalized partition contributes
     * {@code max(itsDeadline, itsPenaltyNotBefore)} so a penalized-but-due entry never drives a
     * zero poll timeout; an ineligible (recovering) shard contributes nothing, so an overdue
     * mid-replay entry never drives one either.
     */
    public long nextDeadlineMs() {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < shardCount; i++) {
            if (!dispatchEligible[i]) {
                continue; // I4: see drainDue
            }
            long deadline = shards[i].nextDeadlineMs();
            if (deadline == Long.MAX_VALUE) {
                continue;
            }
            long effective = Math.max(deadline, penaltyNotBefore.get(partitionIds[i]));
            if (effective < min) {
                min = effective;
            }
        }
        return min;
    }

    /**
     * Stamps the penalty-box not-before deadline for a source partition (replace-on-stamp; clear
     * by stamping a past deadline — matching the {@code SchedulerStore} javadoc).
     */
    public void penalizeSourcePartition(int sourcePartition, long notBeforeMs) {
        penaltyNotBefore.put(sourcePartition, notBeforeMs);
    }

    /**
     * Finalizes a committed batch: every entry {@code IN_FLIGHT → COMPLETED}, heads advance,
     * departing slots free. Entries of partitions dropped since the drain are silent no-ops;
     * entries whose shard was dropped <em>and re-assigned</em> since the drain fail the
     * drain-generation check and are counted + skipped (see the class javadoc).
     */
    public void onBatchCommitted(IndexDueBatch committed) {
        for (int i = 0; i < committed.size(); i++) {
            PartitionShard shard = shardByPartition.get(committed.sourcePartition(i));
            if (shard == null) {
                continue;
            }
            if (shard.epoch() != committed.shardEpoch(i)) {
                shard.noteStaleResolution();
                continue;
            }
            shard.finalizeSlot(committed.slotId(i));
        }
    }

    /**
     * Restores a definitively aborted batch: every entry {@code IN_FLIGHT → PENDING}, re-pushed.
     * Dropped and stale-generation entries are handled exactly like
     * {@link #onBatchCommitted(IndexDueBatch)}.
     */
    public void onBatchAborted(IndexDueBatch aborted) {
        for (int i = 0; i < aborted.size(); i++) {
            PartitionShard shard = shardByPartition.get(aborted.sourcePartition(i));
            if (shard == null) {
                continue;
            }
            if (shard.epoch() != aborted.shardEpoch(i)) {
                shard.noteStaleResolution();
                continue;
            }
            shard.restoreSlot(aborted.slotId(i));
        }
    }

    /**
     * Finalizes a committed engine-synthesized batch view (truncate-and-carry-over, §3.2/§7):
     * entries are resolved by {@code (partition, sourceOffset)} via ring binary search instead of
     * slot ids, since foreign views carry none. Entries of dropped partitions are silent no-ops;
     * absent or not-in-flight entries are counted + skipped per
     * {@link PartitionShard#finalizeBySourceOffset}.
     */
    public void onForeignBatchCommitted(DueBatch committed) {
        for (int i = 0; i < committed.size(); i++) {
            PartitionShard shard = shardByPartition.get(committed.sourcePartition(i));
            if (shard != null) {
                shard.finalizeBySourceOffset(committed.sourceOffset(i));
            }
        }
    }

    /**
     * Restores an aborted (or carried-over) engine-synthesized batch view; the by-identity
     * counterpart of {@link #onBatchAborted(IndexDueBatch)} — see
     * {@link #onForeignBatchCommitted}.
     */
    public void onForeignBatchAborted(DueBatch aborted) {
        for (int i = 0; i < aborted.size(); i++) {
            PartitionShard shard = shardByPartition.get(aborted.sourcePartition(i));
            if (shard != null) {
                shard.restoreBySourceOffset(aborted.sourceOffset(i));
            }
        }
    }

    /** Visits a partition's pending entries oldest-first; see {@link PartitionShard#oldestPending}. */
    public void oldestPending(int partition, PendingVisitor visitor) {
        requireShard(partition).oldestPending(visitor);
    }

    /**
     * Visits a partition's pending <em>and in-flight</em> entries oldest-first — the cursor
     * computation's input; see {@link PartitionShard#oldestUnsettled}.
     */
    public void oldestUnsettled(int partition, PendingVisitor visitor) {
        requireShard(partition).oldestUnsettled(visitor);
    }

    public long pendingCount(int partition) {
        PartitionShard shard = shardByPartition.get(partition);
        return shard == null ? 0 : shard.pendingCount();
    }

    public long totalPendingCount() {
        long total = 0;
        for (int i = 0; i < shardCount; i++) {
            total += shards[i].pendingCount();
        }
        return total;
    }

    public long inFlightCount(int partition) {
        PartitionShard shard = shardByPartition.get(partition);
        return shard == null ? 0 : shard.inFlightCount();
    }

    public long totalInFlightCount() {
        long total = 0;
        for (int i = 0; i < shardCount; i++) {
            total += shards[i].inFlightCount();
        }
        return total;
    }

    /** Amortized housekeeping over all shards (heap rebuilds + log sweeps past thresholds). */
    public void maintenance() {
        for (int i = 0; i < shardCount; i++) {
            shards[i].maintenance();
        }
    }

    public long estimatedRetainedBytes() {
        long total = 0;
        for (int i = 0; i < shardCount; i++) {
            total += shards[i].estimatedRetainedBytes();
        }
        return total;
    }

    // Aggregated plain counters (M3 wires Micrometer on top). Counts of revoked shards drop with
    // the shard — the engine snapshots/aggregates externally if it needs monotonic totals.

    public long anomalies() {
        long total = 0;
        for (int i = 0; i < shardCount; i++) {
            total += shards[i].anomalies();
        }
        return total;
    }

    public long heapRebuilds() {
        long total = 0;
        for (int i = 0; i < shardCount; i++) {
            total += shards[i].heapRebuilds();
        }
        return total;
    }

    public long logSweeps() {
        long total = 0;
        for (int i = 0; i < shardCount; i++) {
            total += shards[i].logSweeps();
        }
        return total;
    }

    public long staleHeapEntries() {
        long total = 0;
        for (int i = 0; i < shardCount; i++) {
            total += shards[i].staleHeapEntries();
        }
        return total;
    }

    public long completedHeldInLog() {
        long total = 0;
        for (int i = 0; i < shardCount; i++) {
            total += shards[i].completedHeldInLog();
        }
        return total;
    }

    /** The shard for {@code partition}, for M3 per-partition work (cursor computation). */
    public PartitionShard shard(int partition) {
        return requireShard(partition);
    }

    private PartitionShard requireShard(int partition) {
        PartitionShard shard = shardByPartition.get(partition);
        if (shard == null) {
            throw new IllegalStateException("partition " + partition + " is not assigned");
        }
        return shard;
    }

    private final class BatchSink implements DueEntrySink {
        int partition;

        @SuppressWarnings("NullAway.Init") // assigned in drainDue immediately before every drain call
        PartitionShard shard;

        @Override
        public void accept(int slotId, long sourceOffset, long dispatchAtMs, long trackerAddOffset) {
            batch.add(
                    partition,
                    sourceOffset,
                    dispatchAtMs,
                    trackerAddOffset,
                    slotId,
                    shard.epoch(),
                    shard.clamped(slotId));
        }
    }
}
