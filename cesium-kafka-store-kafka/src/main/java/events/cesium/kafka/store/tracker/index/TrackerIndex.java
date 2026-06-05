package events.cesium.kafka.store.tracker.index;

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
 * <p><strong>Penalty box semantics</strong> (mirrors {@code SchedulerStore.penalizeSourcePartition}):
 * a new stamp always replaces the previous one; the engine clears a penalty by stamping a past
 * deadline (e.g. {@code 0}) — an expired stamp has no effect because only {@code max(deadline,
 * notBefore)} is ever consulted. Penalties are keyed by partition, not by shard: they survive
 * revoke/re-assign cycles, matching the engine's view of source-partition health.
 *
 * <p><strong>Lifecycle.</strong> {@link #revokePartition}/{@link #lostPartition} drop the shard
 * reference in O(1) — pending entries are durable in the tracker topic; memory is a cache.
 * Resolving a batch whose partition was dropped meanwhile (the I9 drop path) is a silent no-op
 * for those entries.
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
    private int shardCount;

    private final IndexDueBatch batch = new IndexDueBatch();
    private final BatchSink sink = new BatchSink();

    private final int staleRebuildFloor;
    private final int completedSweepFloor;

    public TrackerIndex() {
        this(PartitionShard.DEFAULT_STALE_REBUILD_FLOOR, PartitionShard.DEFAULT_COMPLETED_SWEEP_FLOOR);
    }

    /** Maintenance floors are per shard; see {@link PartitionShard#PartitionShard(int, int)}. */
    public TrackerIndex(int staleRebuildFloor, int completedSweepFloor) {
        this.staleRebuildFloor = staleRebuildFloor;
        this.completedSweepFloor = completedSweepFloor;
    }

    /** Creates an empty shard for {@code partition}; idempotent (re-assignment is a no-op). */
    public void assignPartition(int partition) {
        if (shardByPartition.containsKey(partition)) {
            return;
        }
        PartitionShard shard = new PartitionShard(staleRebuildFloor, completedSweepFloor);
        shardByPartition.put(partition, shard);
        if (shardCount == shards.length) {
            partitionIds = Arrays.copyOf(partitionIds, shardCount * 2);
            shards = Arrays.copyOf(shards, shardCount * 2);
        }
        partitionIds[shardCount] = partition;
        shards[shardCount] = shard;
        shardCount++;
    }

    /** Drops the shard in O(1); a subsequent {@link #assignPartition} starts clean. */
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

    public int assignedPartitionCount() {
        return shardCount;
    }

    /** Routes an ADD to the owning shard; see {@link PartitionShard#applyAdd}. */
    public boolean applyAdd(int partition, long sourceOffset, long dispatchAtMs, long trackerAddOffset) {
        return requireShard(partition).applyAdd(sourceOffset, dispatchAtMs, trackerAddOffset);
    }

    /** Routes a COMPLETE to the owning shard; see {@link PartitionShard#applyComplete}. */
    public boolean applyComplete(int partition, long sourceOffset) {
        return requireShard(partition).applyComplete(sourceOffset);
    }

    /**
     * Drains due entries across partitions in effective-deadline order, skipping penalized
     * partitions, up to {@code maxBatch}. Each iteration re-selects the minimum-effective-
     * deadline shard (O(owned partitions), see class javadoc) and pops one entry, so the batch is
     * globally ordered by {@code dispatchAtMs} (ties across shards in unspecified order).
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
            if (best.drainDue(nowMs, 1, sink) == 0) {
                break; // unreachable with a consistent shard; defensive against infinite loops
            }
        }
        return batch;
    }

    /**
     * Minimum effective deadline over all assigned shards, or {@link Long#MAX_VALUE} when nothing
     * is pending. A penalized partition contributes {@code max(itsDeadline, itsPenaltyNotBefore)}
     * so a penalized-but-due entry never drives a zero poll timeout.
     */
    public long nextDeadlineMs() {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < shardCount; i++) {
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

    /** Finalizes a committed batch: every entry {@code IN_FLIGHT → COMPLETED}, heads advance, departing slots free. */
    public void onBatchCommitted(IndexDueBatch committed) {
        for (int i = 0; i < committed.size(); i++) {
            PartitionShard shard = shardByPartition.get(committed.sourcePartition(i));
            if (shard != null) {
                shard.finalizeSlot(committed.slotId(i));
            }
        }
    }

    /** Restores a definitively aborted batch: every entry {@code IN_FLIGHT → PENDING}, re-pushed. */
    public void onBatchAborted(IndexDueBatch aborted) {
        for (int i = 0; i < aborted.size(); i++) {
            PartitionShard shard = shardByPartition.get(aborted.sourcePartition(i));
            if (shard != null) {
                shard.restoreSlot(aborted.slotId(i));
            }
        }
    }

    /** Visits a partition's pending entries oldest-first; see {@link PartitionShard#oldestPending}. */
    public void oldestPending(int partition, PendingVisitor visitor) {
        requireShard(partition).oldestPending(visitor);
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

        @Override
        public void accept(int slotId, long sourceOffset, long dispatchAtMs, long trackerAddOffset) {
            batch.add(partition, sourceOffset, dispatchAtMs, trackerAddOffset, slotId);
        }
    }
}
