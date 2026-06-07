package com.jucius.cesium.kafka.store.tracker.index;

import it.unimi.dsi.fastutil.ints.IntBigArrayBigList;

/**
 * Append-only log of slot ids in tracker-arrival order with a head pointer (design §5.2 item 3,
 * revision 1: {@link IntBigArrayBigList} instead of a ring, no wraparound).
 *
 * <p><strong>Double sortedness (design §5.2, D6).</strong> Ingest appends ADDs in source-offset
 * order and Kafka preserves per-partition order, so the live region {@code [head, size)} is
 * simultaneously sorted by {@code trackerAddOffset} and {@code sourceOffset}. {@link #append}
 * enforces both with a hard check; the documented exception — anomalous duplicate ADD (R1) — is
 * resolved by {@link PartitionShard#applyAdd} via {@link #findBySourceOffset binary search}
 * <em>before</em> anything reaches {@code append}. The sortedness is what makes completion lookup
 * a hash-map-free O(log n) binary search.
 *
 * <p><strong>Slot lifetime == log residency (design §5.3, critical invariant).</strong> A slot is
 * freed (returned to the pool) ONLY here — when the head advances past it or a sweep drops it —
 * never at dispatch or completion time. That guarantees every slot id inside {@code [head, size)}
 * always reads live pool cells, keeping the binary search valid.
 *
 * <p>{@link #advanceHead()} walks past completed slots; one far-future pending entry at the head
 * can pin completed slots behind it, so {@link #maintain(int)} sweeps (an order-preserving O(n)
 * in-place rewrite, after which binary search remains valid) once the completed-held count
 * crosses {@code max(sweepFloor, 50% of the live region)}.
 */
final class ArrivalLog {

    private final EntryPool pool;
    private final IntBigArrayBigList log = new IntBigArrayBigList();
    private long head;
    private long completedHeld;
    private long sweeps;

    ArrivalLog(EntryPool pool) {
        this.pool = pool;
    }

    /** Number of slots in the live region {@code [head, size)}. */
    long liveSize() {
        return log.size64() - head;
    }

    /** Source offset of the newest entry; callers must check {@code liveSize() > 0} first. */
    long lastSourceOffset() {
        return pool.sourceOffset(log.getInt(log.size64() - 1));
    }

    /**
     * Appends a freshly allocated slot, enforcing the double-sortedness invariant. Duplicate
     * ADDs are handled by the caller via binary search before append (the documented exception).
     */
    void append(int slot) {
        long size = log.size64();
        if (size > head) {
            int tail = log.getInt(size - 1);
            if (pool.sourceOffset(slot) <= pool.sourceOffset(tail)
                    || pool.trackerAddOffset(slot) <= pool.trackerAddOffset(tail)) {
                throw new IllegalStateException("arrival log order violation: appending (sourceOffset="
                        + pool.sourceOffset(slot) + ", trackerAddOffset=" + pool.trackerAddOffset(slot)
                        + ") after (sourceOffset=" + pool.sourceOffset(tail) + ", trackerAddOffset="
                        + pool.trackerAddOffset(tail) + ")");
            }
        }
        log.add(slot);
    }

    /** Binary search over the live region by source offset; returns the slot id or {@code -1}. */
    int findBySourceOffset(long srcOffset) {
        long lo = head;
        long hi = log.size64();
        while (lo < hi) {
            long mid = (lo + hi) >>> 1;
            int slot = log.getInt(mid);
            long v = pool.sourceOffset(slot);
            if (v < srcOffset) {
                lo = mid + 1;
            } else if (v > srcOffset) {
                hi = mid;
            } else {
                return slot;
            }
        }
        return -1;
    }

    /** Records that a live-region slot transitioned to {@link EntryPool#COMPLETED}. */
    void noteCompleted() {
        completedHeld++;
    }

    /**
     * Advances the head past completed slots, freeing each departing slot (the only other free
     * site is {@link #sweep()}). Clears the backing list once the live region empties, so freed
     * prefixes never linger and "log empty" implies {@code size == 0}.
     */
    void advanceHead() {
        long size = log.size64();
        while (head < size && pool.state(log.getInt(head)) == EntryPool.COMPLETED) {
            pool.free(log.getInt(head));
            completedHeld--;
            head++;
        }
        if (head == size && size != 0) {
            log.clear();
            head = 0;
        }
    }

    /** Sweeps when completed-held exceeds {@code max(sweepFloor, 50% of the live region)}. */
    boolean maintain(int sweepFloor) {
        if (completedHeld > Math.max((long) sweepFloor, liveSize() / 2)) {
            sweep();
            return true;
        }
        return false;
    }

    /**
     * O(n) compaction: rewrites live (non-completed) slot ids in order to the front — order is
     * preserved, so binary search stays valid — and frees the departing completed slots.
     */
    void sweep() {
        long size = log.size64();
        long write = 0;
        for (long i = head; i < size; i++) {
            int slot = log.getInt(i);
            if (pool.state(slot) == EntryPool.COMPLETED) {
                pool.free(slot);
                completedHeld--;
            } else {
                log.set(write++, slot);
            }
        }
        log.size(write);
        head = 0;
        sweeps++;
    }

    /**
     * Visits pending entries from the head in {@code trackerAddOffset} order — the greedy
     * sidecar-encoding order of design §3.5. Completed and in-flight slots are skipped via state
     * lookups; the skip cost is bounded by the sweep threshold (amortized O(1) per visit).
     */
    void forEachPending(PendingVisitor visitor) {
        long size = log.size64();
        for (long i = head; i < size; i++) {
            int slot = log.getInt(i);
            if (pool.state(slot) == EntryPool.PENDING
                    && !visitor.visit(
                            slot, pool.sourceOffset(slot), pool.dispatchAtMs(slot), pool.trackerAddOffset(slot))) {
                return;
            }
        }
    }

    /**
     * Visits pending <em>and in-flight</em> entries from the head in {@code trackerAddOffset}
     * order — the cursor-computation order of design §3.5 once in-flight entries must be
     * classified against the committing batch rather than blanket-skipped (an in-flight entry
     * outside the committing batch is still pending durable truth). Completed slots are skipped.
     */
    void forEachUnsettled(PendingVisitor visitor) {
        long size = log.size64();
        for (long i = head; i < size; i++) {
            int slot = log.getInt(i);
            byte state = pool.state(slot);
            if ((state == EntryPool.PENDING || state == EntryPool.IN_FLIGHT)
                    && !visitor.visit(
                            slot, pool.sourceOffset(slot), pool.dispatchAtMs(slot), pool.trackerAddOffset(slot))) {
                return;
            }
        }
    }

    long completedHeld() {
        return completedHeld;
    }

    long sweeps() {
        return sweeps;
    }

    long totalSize() {
        return log.size64();
    }
}
