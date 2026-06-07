package com.jucius.cesium.kafka.store.tracker.index;

import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;
import it.unimi.dsi.fastutil.ints.IntHeaps;
import java.util.BitSet;

/**
 * Min-heap of slot ids ordered by the pool's <em>live</em> ({@code dispatchAtMs}, then
 * {@code sourceOffset}) (design §5.2 item 2): a fastutil {@link IntHeapPriorityQueue} with an
 * indirect primitive comparator, plus lazy deletion and threshold/anomaly rebuilds. Extends the
 * fastutil heap (rather than wrapping) for direct access to the backing array, which makes
 * rebuilds in-place and allocation-free.
 *
 * <p><strong>Equal-deadline tie-break.</strong> {@code sourceOffset} is immutable for the life of
 * a slot and arrival-ordered within a partition (the arrival log's double sortedness, design
 * §5.2), so the tie-break makes the order total and deterministic over live slots and realizes
 * the {@code SchedulerStore.pollDue} contract "({@code dispatchAtMs}, then arrival)" — a
 * same-millisecond burst drains in source-offset order (ascending offsets feed §7.1's one-seek
 * forward-scan fetch). Copies of the SAME slot still compare equal (same live values), so every
 * soundness argument below — lazy deletion, rule (b), the suspect rebuild — carries over
 * unchanged.
 *
 * <p><strong>Lazy deletion.</strong> Completion and dispatch never search the heap: a slot whose
 * state is no longer {@link EntryPool#PENDING} is simply discarded when it surfaces at the top
 * ({@link #peekPendingTop()}). The stale backlog is tracked as an estimate and a rebuild
 * re-heapifies once it crosses {@code max(staleFloor, 25% of heap size)}.
 *
 * <p><strong>Duplicate-ADD hazard (replay rule R1, design §3.5/R16) and why this heap is
 * sound.</strong> R1 updates a slot's {@code dispatchAtMs} <em>in place</em>, which the indirect
 * comparator observes retroactively at every position holding that slot id — an already placed
 * copy can violate heap order in BOTH directions:
 *
 * <ul>
 *   <li><strong>Deadline moved later</strong> (live value increased): the old copy is now too
 *       high and can <em>mask</em> still-due entries that were placed beneath it before the
 *       update; they stay hidden even though the surfaced top looks correctly not-due (a
 *       pop-time re-push of the top alone cannot repair a violation that is not at the root —
 *       pinned in {@code DispatchHeapHazardTest}).
 *   <li><strong>Deadline moved earlier</strong> (live value decreased): the freshly pushed
 *       duplicate is NOT guaranteed to surface, because its sift-up can stop at the slot's own
 *       stale copy (which reads the same, equal live value) stranded beneath a larger parent —
 *       the true minimum stays buried and {@code nextDeadlineMs} over-reports, dispatching the
 *       moved entry late (found by the jqwik oracle; pinned in {@code TrackerIndexOracleTest}).
 * </ul>
 *
 * <p>Handling, in concert with {@link PartitionShard#applyAdd}:
 *
 * <ul>
 *   <li><strong>Every R1 update of a pending slot pushes the slot id again</strong> (duplicates
 *       are allowed and counted as stale); sift operations compare live values, so the fresh
 *       copy is placed consistently at push time.
 *   <li><strong>Any in-place value change behind resident copies marks the order
 *       {@link #suspectOrder() suspect}</strong>; the next read ({@code drainDue} /
 *       {@code nextDeadlineMs} / {@code maintenance}) performs one coalesced O(n) in-place
 *       {@link #rebuild()} that drops stale and duplicate copies and re-heapifies against live
 *       values. Duplicate-ADD is an anomaly path (impossible under §3.1's unique-committed-ADD
 *       invariant, warn metric), so the amortized cost is nil; anomaly storms between reads
 *       coalesce into a single rebuild.
 *   <li><strong>Pop-time guards.</strong> (a) Non-{@code PENDING} tops are discarded (lazy
 *       deletion — the first due pop transitions a slot {@code PENDING → IN_FLIGHT}, so any
 *       remaining duplicate is skipped by the state check); (b) a surfaced {@code PENDING} top
 *       whose CURRENT {@code dispatchAtMs} is later than now is re-pushed before the drain
 *       concludes nothing is due — a deadline moved later never dispatches early, even if a
 *       future change introduced an unforeseen mutation vector.
 *   <li><strong>Slot reuse</strong> would also change the comparator value behind an already
 *       placed heap copy; {@link EntryPool} prevents it by never recycling a slot that still has
 *       live heap copies (zombie list, reclaimed after {@link #rebuild()}).
 * </ul>
 *
 * <p>Between a rebuild and the next in-place change, every push/pop maintains heap order against
 * live values (nothing mutates behind the comparator's back), so the skimmed top is a true
 * minimum: the not-due break in the drain loop is sound and {@code nextDeadlineMs} is exact.
 */
final class DispatchHeap extends IntHeapPriorityQueue {

    private final EntryPool pool;
    private long stale;
    private boolean orderSuspect;
    private long rebuilds;

    /** Rebuild-only scratch for first-occurrence dedup; bits are cleared after every rebuild. */
    private final BitSet rebuildSeen = new BitSet();

    DispatchHeap(EntryPool pool) {
        super(16, (a, b) -> {
            int byDeadline = Long.compare(pool.dispatchAtMs(a), pool.dispatchAtMs(b));
            return byDeadline != 0 ? byDeadline : Long.compare(pool.sourceOffset(a), pool.sourceOffset(b));
        });
        this.pool = pool;
    }

    /** Pushes a slot id and counts the heap copy (duplicates allowed). */
    void push(int slot) {
        enqueue(slot);
        pool.incrementCopies(slot);
    }

    /**
     * Skims stale (non-{@code PENDING}) entries off the top and returns the surfaced pending slot
     * id, or {@code -1} when the heap is exhausted. Does not remove the returned slot.
     */
    int peekPendingTop() {
        while (size > 0) {
            int top = heap[0];
            if (pool.state(top) == EntryPool.PENDING) {
                return top;
            }
            dequeueInt();
            pool.decrementCopies(top);
            if (stale > 0) {
                stale--;
            }
        }
        return -1;
    }

    /** Removes and returns the current top (callers pair this with {@link #peekPendingTop()}). */
    int popTop() {
        int top = dequeueInt();
        pool.decrementCopies(top);
        return top;
    }

    /** Rule (b): removes the top and re-inserts it so it sifts to its live-value position. */
    void repositionTop() {
        enqueue(dequeueInt()); // copy count is net unchanged
    }

    /** Records {@code n} heap entries as stale (lazy-deletion backlog estimate). */
    void markStale(long n) {
        stale += n;
    }

    /** Flags a possible heap-order violation from an in-place comparator-value increase. */
    void suspectOrder() {
        orderSuspect = true;
    }

    /** Rebuilds if an in-place increase was recorded since the last rebuild. Read-path guard. */
    void rebuildIfSuspect() {
        if (orderSuspect) {
            rebuild();
        }
    }

    /** Threshold rebuild: stale backlog above {@code max(staleFloor, 25% of heap size)}. */
    boolean maintain(int staleFloor) {
        if (stale > Math.max((long) staleFloor, size / 4L)) {
            rebuild();
            return true;
        }
        return false;
    }

    /**
     * In-place O(n) rebuild: drops every stale and duplicate entry, recomputes exact heap-copy
     * counts (healing saturated counters), re-heapifies the survivors against live values, and
     * lets the pool recycle zombie slots (now provably copy-free). Allocation-free after the
     * scratch bitset has grown once.
     */
    void rebuild() {
        int kept = 0;
        for (int i = 0; i < size; i++) {
            int slot = heap[i];
            pool.zeroCopies(slot);
            if (pool.state(slot) == EntryPool.PENDING && !rebuildSeen.get(slot)) {
                rebuildSeen.set(slot);
                heap[kept++] = slot;
            }
        }
        for (int i = 0; i < kept; i++) {
            pool.setCopiesOne(heap[i]);
            rebuildSeen.clear(heap[i]);
        }
        size = kept;
        IntHeaps.makeHeap(heap, kept, c);
        stale = 0;
        orderSuspect = false;
        rebuilds++;
        pool.reclaimZombies();
    }

    long staleEstimate() {
        return stale;
    }

    long rebuilds() {
        return rebuilds;
    }
}
