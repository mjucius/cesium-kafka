package events.cesium.kafka.store.tracker.index;

/**
 * The in-memory scheduler index for ONE tracker partition (design §5.2/§5.3): an
 * {@link EntryPool}, a {@link DispatchHeap} and an {@link ArrivalLog} composed behind the slot
 * lifecycle {@code FREE → PENDING → IN_FLIGHT → COMPLETED → FREE}.
 *
 * <p><strong>Ownership/threading (design §5.1).</strong> A shard is owned by exactly one dispatch
 * thread; no method is safe for concurrent use. <strong>Allocation.</strong> All hot-path methods
 * are allocation-free in steady state.
 *
 * <p><strong>Replay rules (design §3.5).</strong>
 *
 * <ul>
 *   <li><strong>R1 (ADD):</strong> a source offset above the log tail appends a new entry. A
 *       source offset at or below the tail is an anomaly (unique-committed-ADD makes it
 *       impossible in a healthy system) and is resolved by binary search: on a hit, ONLY
 *       {@code dispatchAtMs} is updated — the stored {@code trackerAddOffset} is never modified
 *       in place (invariant I5: the original offset remains a valid, conservative replay bound;
 *       increasing it could carry the cursor past other pending entries). On a miss the record is
 *       counted and dropped: inserting mid-log would corrupt the double sortedness that backs
 *       binary search, and the durable log — not this cache — is authoritative.
 *   <li><strong>R2 (COMPLETE):</strong> a completion for an unknown source offset is a silent
 *       no-op (expected whenever the ADD sits below the cursor, or the pair was asymmetrically
 *       compacted, or it is our own committed echo for an entry that already left the log).
 * </ul>
 *
 * <p><strong>Slot lifetime == log residency.</strong> Dispatch ({@link #drainDue}) and completion
 * mark states only; slots are freed exclusively when they leave the log (head advance on
 * finalize/complete, or sweep). See {@link EntryPool#free(int)} for the reuse guard.
 */
public final class PartitionShard {

    /** Default stale-entry floor before a heap rebuild: 64k (design §5.2 item 2). */
    public static final int DEFAULT_STALE_REBUILD_FLOOR = 64 * 1024;

    /** Default completed-held floor before a log sweep: 64k (design §5.3). */
    public static final int DEFAULT_COMPLETED_SWEEP_FLOOR = 64 * 1024;

    /** Nominal per-slot pool bytes: 3 longs + 1 metadata byte (design §5.4). */
    private static final long POOL_BYTES_PER_SLOT = 3 * Long.BYTES + 1;

    private final EntryPool pool = new EntryPool();
    private final DispatchHeap heap = new DispatchHeap(pool);
    private final ArrivalLog log = new ArrivalLog(pool);
    private final int staleRebuildFloor;
    private final int completedSweepFloor;

    private long pendingCount;
    private long inFlightCount;
    private long anomalies;

    public PartitionShard() {
        this(DEFAULT_STALE_REBUILD_FLOOR, DEFAULT_COMPLETED_SWEEP_FLOOR);
    }

    /**
     * @param staleRebuildFloor minimum stale-heap-entry count before {@link #maintenance()}
     *     rebuilds the heap (production default {@link #DEFAULT_STALE_REBUILD_FLOOR}; tests lower
     *     it to exercise rebuilds)
     * @param completedSweepFloor minimum completed-held count before {@link #maintenance()}
     *     sweeps the log
     */
    public PartitionShard(int staleRebuildFloor, int completedSweepFloor) {
        this.staleRebuildFloor = staleRebuildFloor;
        this.completedSweepFloor = completedSweepFloor;
    }

    /**
     * Applies an ADD record (live tail or replay — same code path, design §1.2).
     *
     * @return {@code true} if a new entry was inserted; {@code false} for the anomalous
     *     duplicate/out-of-order paths (R1), which are counted in {@link #anomalies()}
     */
    public boolean applyAdd(long sourceOffset, long dispatchAtMs, long trackerAddOffset) {
        if (log.liveSize() > 0 && sourceOffset <= log.lastSourceOffset()) {
            anomalies++;
            int slot = log.findBySourceOffset(sourceOffset);
            if (slot < 0) {
                return false; // out-of-order non-duplicate: dropped, see class javadoc
            }
            long old = pool.dispatchAtMs(slot);
            if (dispatchAtMs == old) {
                return false; // value unchanged — nothing to reorder
            }
            pool.setDispatchAtMs(slot, dispatchAtMs);
            if (pool.copies(slot) > 0) {
                // Any in-place change behind resident heap copies can break heap order — an
                // increase masks due entries beneath the stale copy, and a decrease can strand
                // the new minimum beneath a larger parent (the fresh duplicate's sift-up stops
                // at the slot's own equal-valued stale copy). Rebuild before the next read
                // (see DispatchHeap javadoc; pinned in TrackerIndexOracleTest).
                heap.suspectOrder();
            }
            if (pool.state(slot) == EntryPool.PENDING) {
                heap.push(slot); // fresh, correctly placed copy; the old copy becomes stale
                heap.markStale(1);
            }
            // trackerAddOffset deliberately NOT touched (I5).
            return false;
        }
        int slot = pool.alloc(sourceOffset, dispatchAtMs, trackerAddOffset);
        log.append(slot);
        heap.push(slot);
        pendingCount++;
        return true;
    }

    /**
     * Applies a COMPLETE record. Unknown source offsets are silent no-ops (R2).
     *
     * @return {@code true} if a pending entry was completed
     */
    public boolean applyComplete(long sourceOffset) {
        if (log.liveSize() == 0) {
            return false;
        }
        int slot = log.findBySourceOffset(sourceOffset);
        if (slot < 0) {
            return false; // R2: silently no-op
        }
        byte state = pool.state(slot);
        if (state == EntryPool.PENDING) {
            pool.setState(slot, EntryPool.COMPLETED);
            pendingCount--;
            heap.markStale(pool.copies(slot)); // its live heap copies are now lazy-delete debt
            log.noteCompleted();
            log.advanceHead();
            return true;
        }
        if (state == EntryPool.IN_FLIGHT) {
            // Impossible from a correct engine: our own COMPLETE echo is only readable after the
            // commit that already finalized the slot. Count it, never corrupt in-flight state.
            anomalies++;
        }
        return false; // COMPLETED: duplicate tombstone for a held slot — no-op (R2)
    }

    /**
     * Pops entries due at {@code nowMs} (up to {@code maxEntries}) into {@code sink}, in due
     * order, transitioning each {@code PENDING → IN_FLIGHT}. Popped slots stay in the log and are
     * resolved later via {@link #finalizeSlot}/{@link #restoreSlot}.
     *
     * <p>Pop protocol (see {@link DispatchHeap} for the soundness argument): (a) non-pending
     * tops are discarded (lazy deletion); (b) a pending top whose CURRENT {@code dispatchAtMs}
     * lies beyond {@code nowMs} is not actually due — it is re-pushed once (rule b) and, when the
     * surfaced minimum is still not due, the drain stops: a deadline moved later never dispatches
     * early, and the suspect-rebuild guarantees the surfaced top is a true minimum.
     *
     * @return number of entries popped
     */
    public int drainDue(long nowMs, int maxEntries, DueEntrySink sink) {
        heap.rebuildIfSuspect();
        int popped = 0;
        boolean repositioned = false;
        while (popped < maxEntries) {
            int top = heap.peekPendingTop();
            if (top < 0) {
                break;
            }
            long at = pool.dispatchAtMs(top);
            if (at <= nowMs) {
                heap.popTop();
                pool.setState(top, EntryPool.IN_FLIGHT);
                pendingCount--;
                inFlightCount++;
                sink.accept(top, pool.sourceOffset(top), at, pool.trackerAddOffset(top));
                popped++;
                repositioned = false;
                continue;
            }
            if (repositioned) {
                break; // minimum pending deadline is in the future — nothing more is due
            }
            heap.repositionTop(); // rule (b)
            repositioned = true;
        }
        return popped;
    }

    /** Restores one in-flight slot to pending after a definitive transaction abort. */
    public void restoreSlot(int slotId) {
        requireState(slotId, EntryPool.IN_FLIGHT, "restore");
        pool.setState(slotId, EntryPool.PENDING);
        heap.push(slotId);
        inFlightCount--;
        pendingCount++;
    }

    /** Batch form of {@link #restoreSlot}. */
    public void restoreAfterAbort(int[] slotIds, int count) {
        for (int i = 0; i < count; i++) {
            restoreSlot(slotIds[i]);
        }
    }

    /**
     * Finalizes one in-flight slot after a definitive transaction commit: marks it completed and
     * advances the log head, freeing every departing slot (the slot-lifetime invariant's only
     * head-side exit).
     */
    public void finalizeSlot(int slotId) {
        requireState(slotId, EntryPool.IN_FLIGHT, "finalize");
        pool.setState(slotId, EntryPool.COMPLETED);
        inFlightCount--;
        log.noteCompleted();
        log.advanceHead();
    }

    /** Batch form of {@link #finalizeSlot}. */
    public void finalizeCommitted(int[] slotIds, int count) {
        for (int i = 0; i < count; i++) {
            finalizeSlot(slotIds[i]);
        }
    }

    /**
     * Visits pending entries from the log head in {@code trackerAddOffset} order — the greedy
     * sidecar-encoding order for the M3 cursor computation. Amortized O(1) per visit (skipped
     * completed slots are bounded by the sweep threshold).
     */
    public void oldestPending(PendingVisitor visitor) {
        log.forEachPending(visitor);
    }

    /**
     * Earliest {@code dispatchAtMs} over pending entries, or {@link Long#MAX_VALUE} when none.
     * In-flight entries do not contribute. Performs the suspect-rebuild and stale skim, so the
     * reported value is exact.
     */
    public long nextDeadlineMs() {
        heap.rebuildIfSuspect();
        int top = heap.peekPendingTop();
        return top < 0 ? Long.MAX_VALUE : pool.dispatchAtMs(top);
    }

    public long pendingCount() {
        return pendingCount;
    }

    public long inFlightCount() {
        return inFlightCount;
    }

    /**
     * Amortized housekeeping: a pending suspect rebuild (taking the deferred R1 repair off the
     * next read), a heap rebuild above {@code max(staleRebuildFloor, 25% of heap)}, and a log
     * sweep above {@code max(completedSweepFloor, 50% of the live region)}. Heap work runs first
     * so swept slots are freed copy-free (immediately reusable) in the same pass.
     */
    public void maintenance() {
        heap.rebuildIfSuspect();
        heap.maintain(staleRebuildFloor);
        log.maintain(completedSweepFloor);
    }

    /**
     * Nominal retained bytes (design §5.4 accounting: pool cells + heap/log/free-list ints).
     * Excludes geometric growth slack and fixed object overheads; capacity planning multiplies by
     * the documented 64 B typical / 80 B worst budget instead.
     */
    public long estimatedRetainedBytes() {
        return pool.allocatedSlots() * POOL_BYTES_PER_SLOT
                + heap.size() * 4L
                + log.totalSize() * 4L
                + (pool.freeCount() + pool.zombieCount()) * 4L;
    }

    /** Anomalous-record count: duplicate ADDs (R1), out-of-order drops, impossible completes. */
    public long anomalies() {
        return anomalies;
    }

    public long heapRebuilds() {
        return heap.rebuilds();
    }

    public long logSweeps() {
        return log.sweeps();
    }

    public long staleHeapEntries() {
        return heap.staleEstimate();
    }

    public long completedHeldInLog() {
        return log.completedHeld();
    }

    // Test observability (package-private; not part of the M3 surface).

    int allocatedSlotCount() {
        return pool.allocatedSlots();
    }

    int freeSlotCount() {
        return pool.freeCount();
    }

    int zombieSlotCount() {
        return pool.zombieCount();
    }

    int heapSize() {
        return heap.size();
    }

    long logTotalSize() {
        return log.totalSize();
    }

    private void requireState(int slotId, byte expected, String op) {
        if (pool.state(slotId) != expected) {
            throw new IllegalStateException(
                    op + " of slot " + slotId + " in state " + pool.state(slotId) + " (expected " + expected + ")");
        }
    }
}
