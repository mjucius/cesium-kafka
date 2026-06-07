package com.jucius.cesium.kafka.store.tracker.index;

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
 *   <li><strong>R1 (ADD):</strong> a source offset above every offset this shard has ever
 *       appended (a per-shard high watermark that survives head advances, sweeps and the log
 *       emptying — unlike the log tail, it never forgets) appends a new entry. A source offset at
 *       or below the watermark is an anomaly (unique-committed-ADD plus Kafka's per-partition
 *       ordering make it impossible in a healthy system) and is resolved by binary search over
 *       the live log: on a hit, ONLY {@code dispatchAtMs} is updated — the stored
 *       {@code trackerAddOffset} is never modified in place (invariant I5: the original offset
 *       remains a valid, conservative replay bound; increasing it could carry the cursor past
 *       other pending entries). On a miss the record is counted and dropped: inserting mid-log
 *       would corrupt the double sortedness that backs binary search, the entry's durable truth
 *       is already settled (a duplicate committed ADD is a forged-write/replay-bug signal, R12),
 *       and the durable log — not this cache — is authoritative. The watermark dies with the
 *       shard, so revoke→reassign recovery replays — which legitimately re-add lower offsets into
 *       a fresh shard — are unaffected.
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

    /**
     * Drain-generation stamp set by {@link TrackerIndex#assignPartition}: every shard incarnation
     * gets a fresh value, and {@link TrackerIndex#drainDue} stamps it into each drained entry so
     * batch resolution can detect a batch drained from a <em>previous</em> incarnation of the
     * partition (the I9 drop-and-re-recover path) instead of dereferencing its slot ids against
     * this shard's pool. {@code 0} for shards constructed outside a {@code TrackerIndex}.
     */
    private long epoch;

    /**
     * Highest source offset ever appended; never reset by head advances, sweeps or the log
     * emptying (it dies with the shard). Gates the R1 anomaly path so a duplicate committed ADD
     * arriving after its entry left the log is dropped and counted like any other duplicate,
     * never silently resurrected as a fresh pending entry (which would re-dispatch).
     */
    private long maxSourceOffsetSeen = Long.MIN_VALUE;

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

    /** Unclamped variant of {@link #applyAdd(long, long, long, boolean)}. */
    public boolean applyAdd(long sourceOffset, long dispatchAtMs, long trackerAddOffset) {
        return applyAdd(sourceOffset, dispatchAtMs, trackerAddOffset, false);
    }

    /**
     * Applies an ADD record (live tail or replay — same code path, design §1.2).
     *
     * @param clamped the ADD's CLAMP marker (wire-format flag bit 0, design §2.3) — pass-through
     *     schedule payload surfaced on the drained {@code DueBatch}; never ordering-relevant
     * @return {@code true} if a new entry was inserted; {@code false} for the anomalous
     *     duplicate/out-of-order paths (R1), which are counted in {@link #anomalies()}
     */
    public boolean applyAdd(long sourceOffset, long dispatchAtMs, long trackerAddOffset, boolean clamped) {
        if (sourceOffset <= maxSourceOffsetSeen) {
            anomalies++;
            int slot = log.findBySourceOffset(sourceOffset);
            if (slot < 0) {
                return false; // duplicate of a departed entry, or out-of-order: dropped (javadoc)
            }
            // The clamp marker travels with dispatchAtMs (they are one schedule value on the
            // wire): a duplicate ADD updates both. Unlike trackerAddOffset (I5) the bit has no
            // replay-position meaning, and unlike dispatchAtMs it has no heap-order effect.
            pool.setClamped(slot, clamped);
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
        int slot = pool.alloc(sourceOffset, dispatchAtMs, trackerAddOffset, clamped);
        log.append(slot);
        maxSourceOffsetSeen = sourceOffset;
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
     * Finalizes the in-flight entry for {@code sourceOffset} after a committed engine-synthesized
     * batch view (truncate-and-carry-over, design §3.2/§7): foreign views carry no slot ids, so
     * the entry is resolved by binary search over the live log (D6). An absent or
     * not-in-flight entry is counted as an anomaly and skipped — it signals a stale batch
     * resolved after an I9 drop, which the engine is obliged to abandon.
     *
     * @return {@code true} if an in-flight entry was finalized
     */
    public boolean finalizeBySourceOffset(long sourceOffset) {
        int slot = log.liveSize() == 0 ? -1 : log.findBySourceOffset(sourceOffset);
        if (slot < 0 || pool.state(slot) != EntryPool.IN_FLIGHT) {
            anomalies++;
            return false;
        }
        finalizeSlot(slot);
        return true;
    }

    /**
     * Restores the in-flight entry for {@code sourceOffset} to pending after a definitively
     * aborted (or carried-over, design §3.2/§7) engine-synthesized batch view; the by-identity
     * counterpart of {@link #restoreSlot}. Absent or not-in-flight entries are counted as
     * anomalies and skipped, exactly like {@link #finalizeBySourceOffset}.
     *
     * @return {@code true} if an in-flight entry was restored
     */
    public boolean restoreBySourceOffset(long sourceOffset) {
        int slot = log.liveSize() == 0 ? -1 : log.findBySourceOffset(sourceOffset);
        if (slot < 0 || pool.state(slot) != EntryPool.IN_FLIGHT) {
            anomalies++;
            return false;
        }
        restoreSlot(slot);
        return true;
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
     * Visits pending <em>and in-flight</em> entries from the log head in {@code trackerAddOffset}
     * order — the cursor-computation input once in-flight entries must be classified against the
     * committing batch (an in-flight entry the transaction does <em>not</em> settle is still
     * pending durable truth and must reach the sidecar encoder; design §3.5,
     * {@code TrackerBackedStore.committedCursor}). Same amortized cost as
     * {@link #oldestPending}.
     */
    public void oldestUnsettled(PendingVisitor visitor) {
        log.forEachUnsettled(visitor);
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
     * The CLAMP marker of a live slot (design §2.3) — valid for any slot id surfaced by
     * {@link #drainDue} or {@link #oldestPending} while the slot remains log-resident. Callers
     * read it alongside the sink/visitor callback (the slot id is the handle), keeping those
     * functional interfaces stable.
     */
    public boolean clamped(int slotId) {
        return pool.clamped(slotId);
    }

    /**
     * Amortized housekeeping: a pending suspect rebuild (taking the deferred R1 repair off the
     * next read), a heap rebuild above {@code max(staleRebuildFloor, 25% of heap)} stale entries
     * OR above {@code max(staleRebuildFloor, 25% of allocated slots)} parked zombies, and a log
     * sweep above {@code max(completedSweepFloor, 50% of the live region)}. Heap work runs first
     * so swept slots are freed copy-free (immediately reusable) in the same pass.
     *
     * <p><strong>Why the independent zombie trigger:</strong> parked zombies are reclaimed only
     * by a heap rebuild, but their stale heap copies are also skimmed away by ordinary drains
     * ({@code peekPendingTop} decrements the stale estimate while the zombie stays parked), so a
     * replay-style ADD+COMPLETE burst per recovery episode could otherwise accumulate up to a
     * stale-floor's worth of permanently unreusable slots per episode, monotonic across episodes,
     * without ever crossing the stale threshold. The zombie trigger bounds the parked backlog at
     * the same amortized envelope as the stale backlog (design §5.4's capped completed-held
     * budget) for one extra comparison per call.
     */
    public void maintenance() {
        heap.rebuildIfSuspect();
        boolean rebuilt = heap.maintain(staleRebuildFloor);
        if (!rebuilt && pool.zombieCount() > Math.max((long) staleRebuildFloor, pool.allocatedSlots() / 4L)) {
            heap.rebuild(); // purges every stale copy, then reclaimZombies() recycles the parked slots
        }
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

    /**
     * Anomalous-event count: duplicate ADDs (R1), out-of-order drops, impossible completes, and
     * stale-batch resolutions (foreign-view misses and post-I9-drop epoch mismatches).
     */
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

    // Intra-package wiring (TrackerIndex) and test observability; not part of the M3 surface.

    /** The drain-generation stamp; see the field javadoc. */
    long epoch() {
        return epoch;
    }

    /** Stamped once by {@link TrackerIndex#assignPartition}; never changes for a shard's lifetime. */
    void epoch(long epoch) {
        this.epoch = epoch;
    }

    /** Counts a stale-batch resolution attempt detected by the epoch check (see {@link #epoch()}). */
    void noteStaleResolution() {
        anomalies++;
    }

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
