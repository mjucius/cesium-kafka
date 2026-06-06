package events.cesium.kafka.store.tracker.index;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import java.util.Arrays;
import java.util.BitSet;

/**
 * Slot storage for one partition shard (design §5.2 item 1, revision 1).
 *
 * <p>Three parallel {@link LongBigArrayBigList}s hold the 24 bytes of real state per entry —
 * {@code dispatchAtMs}, {@code sourceOffset}, {@code trackerAddOffset} — indexed by an {@code int}
 * slot id. Freed slot ids are recycled through an {@link IntArrayList} stack. Per-slot metadata
 * (the {@code FREE → PENDING → IN_FLIGHT → COMPLETED → FREE} state machine plus a heap-copy
 * count) packs into one byte per slot, stored in 64 KiB chunks so growth never reallocates and a
 * shard with few entries never pre-pays.
 *
 * <p><strong>Slot-reuse hazard (design §5.3).</strong> A slot id may still be referenced by stale
 * entries in the {@link DispatchHeap} (lazy deletion) when it leaves the {@link ArrivalLog}.
 * Reusing it while heap-resident would silently rebind those stale heap entries to the new entry
 * — and, worse, could re-introduce a comparator-value <em>increase</em> at an already-placed heap
 * position, breaking the heap-order argument documented on {@link DispatchHeap}. The pool
 * therefore tracks an exact (saturating) count of live heap copies per slot: {@link #free(int)}
 * recycles a slot immediately only when its count is zero; otherwise the slot parks on a
 * <em>zombie</em> list and is recycled by {@link #reclaimZombies()} after the next heap rebuild
 * (which purges every stale copy and recomputes exact counts, also healing saturated counters).
 *
 * <p>The count saturates at {@value #MAX_COPIES}; a saturated counter sticks (it can no longer be
 * decremented exactly) until a heap rebuild recomputes it. Saturation only delays slot reuse,
 * never correctness.
 */
final class EntryPool {

    static final byte FREE = 0;
    static final byte PENDING = 1;
    static final byte IN_FLIGHT = 2;
    static final byte COMPLETED = 3;

    private static final int STATE_MASK = 0x03;
    private static final int COPIES_SHIFT = 2;
    private static final int MAX_COPIES = 63;

    private static final int META_CHUNK_BITS = 16;
    private static final int META_CHUNK_SIZE = 1 << META_CHUNK_BITS;
    private static final int META_CHUNK_MASK = META_CHUNK_SIZE - 1;

    private final LongBigArrayBigList dispatchAtMs = new LongBigArrayBigList();
    private final LongBigArrayBigList sourceOffset = new LongBigArrayBigList();
    private final LongBigArrayBigList trackerAddOffset = new LongBigArrayBigList();

    /** Chunked per-slot metadata: bits 0-1 state, bits 2-7 saturating heap-copy count. */
    private byte[][] meta = new byte[1][];

    /**
     * Per-slot CLAMP marker (1 bit/slot, design §2.3): pass-through schedule payload carried from
     * the tracker ADD's flags byte to the {@code DueBatch} so the dispatch-time relay can stamp
     * {@code cesium-clamped: true}. Never consulted by ordering or lifecycle logic.
     */
    private final BitSet clamped = new BitSet();

    private final IntArrayList freeStack = new IntArrayList();
    private final IntArrayList zombies = new IntArrayList();
    private int slotCount;

    /** Allocates a slot in state {@link #PENDING} with zero heap copies. */
    int alloc(long srcOffset, long atMs, long trkAddOffset, boolean clampedFlag) {
        final int slot;
        if (!freeStack.isEmpty()) {
            slot = freeStack.popInt();
            dispatchAtMs.set(slot, atMs);
            sourceOffset.set(slot, srcOffset);
            trackerAddOffset.set(slot, trkAddOffset);
        } else {
            if (slotCount == Integer.MAX_VALUE) {
                throw new IllegalStateException("slot id space exhausted");
            }
            slot = slotCount++;
            dispatchAtMs.add(atMs);
            sourceOffset.add(srcOffset);
            trackerAddOffset.add(trkAddOffset);
            ensureMetaCapacity(slot);
        }
        metaSet(slot, PENDING);
        clamped.set(slot, clampedFlag);
        return slot;
    }

    /**
     * Releases a slot that is leaving the arrival log — the ONLY call site that may free (slot
     * lifetime == log residency, design §5.3). Recycles immediately when no heap copies remain;
     * otherwise parks the slot as a zombie until {@link #reclaimZombies()}.
     */
    void free(int slot) {
        if (state(slot) != COMPLETED) {
            throw new IllegalStateException("freeing slot " + slot + " in state " + state(slot));
        }
        if (copies(slot) == 0) {
            metaSet(slot, FREE);
            freeStack.push(slot);
        } else {
            setState(slot, FREE);
            zombies.add(slot);
        }
    }

    /**
     * Recycles all zombie slots. MUST be called only immediately after a heap rebuild: the
     * rebuild removed every stale heap copy, so each zombie's true copy count is definitively
     * zero (regardless of a possibly saturated counter, which is healed here).
     */
    void reclaimZombies() {
        for (int i = 0; i < zombies.size(); i++) {
            int slot = zombies.getInt(i);
            metaSet(slot, FREE); // state FREE, copies 0 — exact again
            freeStack.push(slot);
        }
        zombies.clear();
    }

    long dispatchAtMs(int slot) {
        return dispatchAtMs.getLong(slot);
    }

    void setDispatchAtMs(int slot, long atMs) {
        dispatchAtMs.set(slot, atMs);
    }

    long sourceOffset(int slot) {
        return sourceOffset.getLong(slot);
    }

    long trackerAddOffset(int slot) {
        return trackerAddOffset.getLong(slot);
    }

    boolean clamped(int slot) {
        return clamped.get(slot);
    }

    void setClamped(int slot, boolean clampedFlag) {
        clamped.set(slot, clampedFlag);
    }

    byte state(int slot) {
        return (byte) (metaRaw(slot) & STATE_MASK);
    }

    void setState(int slot, byte state) {
        metaSet(slot, (metaRaw(slot) & ~STATE_MASK) | state);
    }

    int copies(int slot) {
        return metaRaw(slot) >>> COPIES_SHIFT;
    }

    void incrementCopies(int slot) {
        int raw = metaRaw(slot);
        if ((raw >>> COPIES_SHIFT) < MAX_COPIES) {
            metaSet(slot, raw + (1 << COPIES_SHIFT));
        }
    }

    void decrementCopies(int slot) {
        int raw = metaRaw(slot);
        int c = raw >>> COPIES_SHIFT;
        if (c > 0 && c < MAX_COPIES) { // saturated counters stick until a rebuild recomputes
            metaSet(slot, raw - (1 << COPIES_SHIFT));
        }
    }

    void zeroCopies(int slot) {
        metaSet(slot, metaRaw(slot) & STATE_MASK);
    }

    void setCopiesOne(int slot) {
        metaSet(slot, (metaRaw(slot) & STATE_MASK) | (1 << COPIES_SHIFT));
    }

    int allocatedSlots() {
        return slotCount;
    }

    int freeCount() {
        return freeStack.size();
    }

    int zombieCount() {
        return zombies.size();
    }

    private int metaRaw(int slot) {
        return meta[slot >>> META_CHUNK_BITS][slot & META_CHUNK_MASK] & 0xFF;
    }

    private void metaSet(int slot, int raw) {
        meta[slot >>> META_CHUNK_BITS][slot & META_CHUNK_MASK] = (byte) raw;
    }

    private void ensureMetaCapacity(int slot) {
        int chunk = slot >>> META_CHUNK_BITS;
        if (chunk >= meta.length) {
            meta = Arrays.copyOf(meta, Math.max(meta.length * 2, chunk + 1));
        }
        if (meta[chunk] == null) {
            meta[chunk] = new byte[META_CHUNK_SIZE];
        }
    }
}
