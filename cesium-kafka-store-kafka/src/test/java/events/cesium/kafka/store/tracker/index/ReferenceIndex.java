package events.cesium.kafka.store.tracker.index;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Naive oracle model for {@link TrackerIndex}: boxed {@link TreeMap}/{@link HashMap} structures,
 * O(n) scans, zero cleverness. The jqwik stateful property executes identical op sequences
 * against both and asserts observable equivalence.
 *
 * <p>Modeling notes (mirroring the real semantics the generator exercises):
 *
 * <ul>
 *   <li>Duplicate ADD (R1) updates {@code dispatchAtMs} only and keeps the ORIGINAL
 *       {@code trackerAddOffset} (invariant I5).
 *   <li>COMPLETE of an unknown offset is a silent no-op (R2); completes only remove pending
 *       entries.
 *   <li>Penalties are keyed by partition and survive revoke/re-assign (they live in the index
 *       container, not the shard).
 * </ul>
 */
final class ReferenceIndex {

    static final class RefEntry {
        final int partition;
        final long sourceOffset;
        long dispatchAtMs;
        final long trackerAddOffset;

        RefEntry(int partition, long sourceOffset, long dispatchAtMs, long trackerAddOffset) {
            this.partition = partition;
            this.sourceOffset = sourceOffset;
            this.dispatchAtMs = dispatchAtMs;
            this.trackerAddOffset = trackerAddOffset;
        }
    }

    static final class RefShard {
        final TreeMap<Long, RefEntry> pending = new TreeMap<>();
    }

    final TreeMap<Integer, RefShard> shards = new TreeMap<>();
    final Map<Integer, Long> penaltyNotBefore = new HashMap<>();

    void assign(int partition) {
        shards.putIfAbsent(partition, new RefShard());
    }

    void revoke(int partition) {
        shards.remove(partition);
    }

    boolean applyAdd(int partition, long sourceOffset, long dispatchAtMs, long trackerAddOffset) {
        RefShard shard = shards.get(partition);
        RefEntry existing = shard.pending.get(sourceOffset);
        if (existing != null) {
            existing.dispatchAtMs = dispatchAtMs; // R1: trackerAddOffset KEPT (I5)
            return false;
        }
        shard.pending.put(sourceOffset, new RefEntry(partition, sourceOffset, dispatchAtMs, trackerAddOffset));
        return true;
    }

    boolean applyComplete(int partition, long sourceOffset) {
        return shards.get(partition).pending.remove(sourceOffset) != null; // R2: absent = no-op
    }

    /** Effective deadline: an expired penalty still floors the partition's drain priority. */
    long effectiveDeadline(RefEntry entry) {
        return Math.max(entry.dispatchAtMs, penaltyNotBefore.getOrDefault(entry.partition, 0L));
    }

    /**
     * Eligible due entries (penalty-respecting), sorted by EFFECTIVE deadline — the real index
     * drains partitions by {@code max(shardDeadline, penaltyNotBefore)}, so an entry behind an
     * expired penalty legally drains after younger entries of healthy partitions.
     */
    List<RefEntry> eligibleDue(long nowMs) {
        List<RefEntry> due = new ArrayList<>();
        for (Map.Entry<Integer, RefShard> e : shards.entrySet()) {
            if (penaltyNotBefore.getOrDefault(e.getKey(), 0L) > nowMs) {
                continue;
            }
            for (RefEntry entry : e.getValue().pending.values()) {
                if (entry.dispatchAtMs <= nowMs) {
                    due.add(entry);
                }
            }
        }
        due.sort(Comparator.comparingLong(this::effectiveDeadline)
                .thenComparingInt(r -> r.partition)
                .thenComparingLong(r -> r.sourceOffset));
        return due;
    }

    long nextDeadlineMs() {
        long min = Long.MAX_VALUE;
        for (Map.Entry<Integer, RefShard> e : shards.entrySet()) {
            long shardMin = Long.MAX_VALUE;
            for (RefEntry entry : e.getValue().pending.values()) {
                shardMin = Math.min(shardMin, entry.dispatchAtMs);
            }
            if (shardMin == Long.MAX_VALUE) {
                continue;
            }
            min = Math.min(min, Math.max(shardMin, penaltyNotBefore.getOrDefault(e.getKey(), 0L)));
        }
        return min;
    }

    /** Pending entries of one partition in trackerAddOffset order (oldestPending oracle). */
    List<RefEntry> pendingByTrackerOffset(int partition) {
        List<RefEntry> list = new ArrayList<>(shards.get(partition).pending.values());
        list.sort(Comparator.comparingLong(r -> r.trackerAddOffset));
        return list;
    }

    long pendingCount(int partition) {
        RefShard shard = shards.get(partition);
        return shard == null ? 0 : shard.pending.size();
    }

    long totalPendingCount() {
        long total = 0;
        for (RefShard shard : shards.values()) {
            total += shard.pending.size();
        }
        return total;
    }
}
