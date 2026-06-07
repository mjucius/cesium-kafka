package com.jucius.cesium.kafka.store.tracker.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Drives identical operation sequences against the real {@link TrackerIndex} and the naive
 * {@link ReferenceIndex}, asserting observable equivalence after every step.
 *
 * <p>Sequencing rules that keep the naive model honest:
 *
 * <ul>
 *   <li>New ADDs always use fresh, per-partition-monotonic EVEN source offsets, so the model
 *       never needs to reason about the real shard's completed-held log contents.
 *   <li>Duplicate ADDs (R1) target currently-pending entries; unknown completes use ODD offsets
 *       (never issued), exercising the R2 binary-search miss without colliding with future adds.
 *   <li>Every drained batch is resolved (commit or abort) before the next operation, matching
 *       the engine's one-transaction-at-a-time dispatch loop (I3), so the model needs no
 *       in-flight bookkeeping between ops.
 * </ul>
 *
 * <p>Drain comparison: the batch must be globally ordered by effective deadline, and within each
 * partition the exact ({@code dispatchAtMs}, then arrival) sequence of the {@code pollDue}
 * contract is asserted (the drained entries of a partition are always a prefix of its due list in
 * that order). Only ACROSS partitions are equal effective deadlines tie-tolerant — the API leaves
 * cross-partition tie order unspecified — so tie groups are additionally compared as
 * timestamp-grouped multisets, and a batch truncated by {@code maxBatch} inside a tie group may
 * contain any subset of that group.
 */
final class OracleHarness {

    record DrainedKey(int partition, long sourceOffset, long dispatchAtMs, long trackerAddOffset)
            implements Comparable<DrainedKey> {
        @Override
        public int compareTo(DrainedKey o) {
            int c = Long.compare(dispatchAtMs, o.dispatchAtMs);
            if (c != 0) {
                return c;
            }
            c = Integer.compare(partition, o.partition);
            if (c != 0) {
                return c;
            }
            return Long.compare(sourceOffset, o.sourceOffset);
        }
    }

    final TrackerIndex real;
    final ReferenceIndex model = new ReferenceIndex();
    final int partitions;
    long nowMs = 1_000_000;
    private final long[] nextSourceOffset;
    private final long[] nextTrackerOffset;

    OracleHarness(int partitions) {
        this(partitions, 8, 8); // low floors so property runs exercise rebuilds and sweeps
    }

    OracleHarness(int partitions, int staleFloor, int sweepFloor) {
        this.partitions = partitions;
        this.real = new TrackerIndex(staleFloor, sweepFloor);
        this.nextSourceOffset = new long[partitions];
        this.nextTrackerOffset = new long[partitions];
        for (int p = 0; p < partitions; p++) {
            real.assignPartition(p);
            model.assign(p);
        }
    }

    void addNew(int partitionSel, long dispatchAtMs) {
        addNew(partitionSel, dispatchAtMs, false);
    }

    void addNew(int partitionSel, long dispatchAtMs, boolean clamped) {
        int p = Math.floorMod(partitionSel, partitions);
        long src = nextSourceOffset[p];
        nextSourceOffset[p] += 2; // even offsets; odd ones are reserved for unknown completes
        long trk = nextTrackerOffset[p]++;
        boolean realNew = real.applyAdd(p, src, dispatchAtMs, trk, clamped);
        boolean modelNew = model.applyAdd(p, src, dispatchAtMs, trk, clamped);
        assertEquals(modelNew, realNew, "applyAdd(new) insertion disagreement");
        assertTrue(realNew, "fresh offsets must insert");
    }

    /** Duplicate ADD (R1) against a currently-pending entry; no-op when none pending. */
    void dupAdd(int partitionSel, int entrySel, long newDispatchAtMs) {
        dupAdd(partitionSel, entrySel, newDispatchAtMs, false);
    }

    void dupAdd(int partitionSel, int entrySel, long newDispatchAtMs, boolean clamped) {
        int p = Math.floorMod(partitionSel, partitions);
        List<ReferenceIndex.RefEntry> pending = model.pendingByTrackerOffset(p);
        if (pending.isEmpty()) {
            return;
        }
        ReferenceIndex.RefEntry target = pending.get(Math.floorMod(entrySel, pending.size()));
        long trk = nextTrackerOffset[p]++; // the duplicate record consumes a tracker offset...
        real.applyAdd(p, target.sourceOffset, newDispatchAtMs, trk, clamped);
        model.applyAdd(p, target.sourceOffset, newDispatchAtMs, trk, clamped); // ...but I5 keeps the original
    }

    void completeKnown(int partitionSel, int entrySel) {
        int p = Math.floorMod(partitionSel, partitions);
        List<ReferenceIndex.RefEntry> pending = model.pendingByTrackerOffset(p);
        if (pending.isEmpty()) {
            return;
        }
        ReferenceIndex.RefEntry target = pending.get(Math.floorMod(entrySel, pending.size()));
        boolean realHit = real.applyComplete(p, target.sourceOffset);
        boolean modelHit = model.applyComplete(p, target.sourceOffset);
        assertEquals(modelHit, realHit, "applyComplete(known) disagreement");
    }

    void completeUnknown(int partitionSel, long offsetSel) {
        int p = Math.floorMod(partitionSel, partitions);
        long bound = Math.max(2, nextSourceOffset[p] + 2);
        long src = Math.floorMod(offsetSel, bound) | 1L; // odd → never issued
        boolean realHit = real.applyComplete(p, src);
        assertEquals(false, realHit, "unknown complete must be a no-op (R2)");
    }

    void drainAndResolve(long advanceMs, int maxBatch, boolean commit) {
        nowMs += advanceMs;
        IndexDueBatch batch = real.drainDue(nowMs, maxBatch);
        List<ReferenceIndex.RefEntry> due = model.eligibleDue(nowMs);
        int expected = Math.min(maxBatch, due.size());
        assertEquals(expected, batch.size(), "drained count at now=" + nowMs);
        verifyDrainGroups(batch, due);

        // Model bookkeeping: drained entries leave pending; commit keeps them out, abort returns
        // them. The real index is resolved through the same batch object.
        for (int i = 0; i < batch.size(); i++) {
            ReferenceIndex.RefEntry e =
                    model.shards.get(batch.sourcePartition(i)).pending.remove(batch.sourceOffset(i));
            assertNotNull(e, "real drained an entry the model does not consider pending");
            assertEquals(e.dispatchAtMs, batch.dispatchAtMs(i), "drained dispatchAtMs");
            assertEquals(e.trackerAddOffset, batch.trackerOffset(i), "drained trackerAddOffset (I5)");
            assertEquals(e.clamped, batch.clamped(i), "drained clamped pass-through");
        }
        if (commit) {
            real.onBatchCommitted(batch);
        } else {
            real.onBatchAborted(batch);
            for (int i = 0; i < batch.size(); i++) {
                int p = batch.sourcePartition(i);
                model.applyAdd(
                        p, batch.sourceOffset(i), batch.dispatchAtMs(i), batch.trackerOffset(i), batch.clamped(i));
            }
        }
    }

    private long effectiveDeadline(IndexDueBatch batch, int i) {
        return Math.max(batch.dispatchAtMs(i), model.penaltyNotBefore.getOrDefault(batch.sourcePartition(i), 0L));
    }

    private void verifyDrainGroups(IndexDueBatch batch, List<ReferenceIndex.RefEntry> due) {
        // The real batch must be globally ordered by EFFECTIVE deadline (an expired penalty
        // floors a partition's drain priority even though its entries' dispatchAtMs are older).
        for (int i = 1; i < batch.size(); i++) {
            assertTrue(
                    effectiveDeadline(batch, i) >= effectiveDeadline(batch, i - 1),
                    "drain not in effective-deadline order at index " + i);
        }
        boolean truncated = batch.size() < due.size();
        verifyWithinPartitionOrder(batch, due, truncated);
        int ri = 0;
        int di = 0;
        while (ri < batch.size()) {
            long ts = effectiveDeadline(batch, ri);
            List<DrainedKey> realGroup = new ArrayList<>();
            while (ri < batch.size() && effectiveDeadline(batch, ri) == ts) {
                realGroup.add(new DrainedKey(
                        batch.sourcePartition(ri),
                        batch.sourceOffset(ri),
                        batch.dispatchAtMs(ri),
                        batch.trackerOffset(ri)));
                ri++;
            }
            assertTrue(
                    di < due.size() && model.effectiveDeadline(due.get(di)) == ts,
                    "effective-deadline group mismatch at ts=" + ts);
            List<DrainedKey> refGroup = new ArrayList<>();
            while (di < due.size() && model.effectiveDeadline(due.get(di)) == ts) {
                ReferenceIndex.RefEntry e = due.get(di++);
                refGroup.add(new DrainedKey(e.partition, e.sourceOffset, e.dispatchAtMs, e.trackerAddOffset));
            }
            Collections.sort(realGroup);
            Collections.sort(refGroup);
            if (ri == batch.size() && truncated) {
                // maxBatch cut inside this tie group: any subset of the right size is legal.
                assertTrue(refGroup.containsAll(realGroup), "truncated tie group is not a subset");
            } else {
                assertEquals(refGroup, realGroup, "timestamp group multiset mismatch at ts=" + ts);
            }
        }
    }

    /**
     * Exact within-partition order — the {@code pollDue} contract "({@code dispatchAtMs}, then
     * arrival)". Within one partition the shard's heap pops in (dispatchAtMs, sourceOffset)
     * order regardless of penalties or truncation, so the drained subsequence of every partition
     * must be a prefix of that partition's due list in that order — and the full list when the
     * batch was not truncated.
     */
    private void verifyWithinPartitionOrder(IndexDueBatch batch, List<ReferenceIndex.RefEntry> due, boolean truncated) {
        Map<Integer, List<DrainedKey>> realByPartition = new TreeMap<>();
        for (int i = 0; i < batch.size(); i++) {
            realByPartition
                    .computeIfAbsent(batch.sourcePartition(i), p -> new ArrayList<>())
                    .add(new DrainedKey(
                            batch.sourcePartition(i),
                            batch.sourceOffset(i),
                            batch.dispatchAtMs(i),
                            batch.trackerOffset(i)));
        }
        Map<Integer, List<DrainedKey>> refByPartition = new TreeMap<>();
        for (ReferenceIndex.RefEntry e : due) {
            refByPartition
                    .computeIfAbsent(e.partition, p -> new ArrayList<>())
                    .add(new DrainedKey(e.partition, e.sourceOffset, e.dispatchAtMs, e.trackerAddOffset));
        }
        refByPartition.values().forEach(Collections::sort); // (dispatchAtMs, partition, sourceOffset)
        for (Map.Entry<Integer, List<DrainedKey>> e : realByPartition.entrySet()) {
            List<DrainedKey> realSeq = e.getValue();
            List<DrainedKey> refSeq = refByPartition.get(e.getKey());
            assertNotNull(refSeq, "drained entries for a partition with nothing due: " + e.getKey());
            assertTrue(realSeq.size() <= refSeq.size(), "partition " + e.getKey() + " over-drained");
            assertEquals(
                    refSeq.subList(0, realSeq.size()),
                    realSeq,
                    "within-partition (dispatchAtMs, then arrival) order of partition " + e.getKey());
        }
        if (!truncated) {
            assertEquals(
                    refByPartition.keySet(),
                    realByPartition.keySet(),
                    "an untruncated drain must cover every partition with due entries");
            for (Map.Entry<Integer, List<DrainedKey>> e : refByPartition.entrySet()) {
                assertEquals(
                        e.getValue().size(),
                        realByPartition.get(e.getKey()).size(),
                        "untruncated drain missed due entries of partition " + e.getKey());
            }
        }
    }

    void penalize(int partitionSel, long notBeforeMs) {
        int p = Math.floorMod(partitionSel, partitions);
        real.penalizeSourcePartition(p, notBeforeMs);
        model.penaltyNotBefore.put(p, notBeforeMs);
    }

    /** Toggles the I4 dispatch-eligibility gate (M3: beginRecovery=false, ACTIVE promotion=true). */
    void setEligible(int partitionSel, boolean eligible) {
        int p = Math.floorMod(partitionSel, partitions);
        real.setDispatchEligible(p, eligible);
        model.shards.get(p).dispatchEligible = eligible;
    }

    void maintenance() {
        real.maintenance();
    }

    /** Revoke + immediate re-assign: the shard starts clean; the penalty box survives. */
    void revokeAssign(int partitionSel) {
        int p = Math.floorMod(partitionSel, partitions);
        real.revokePartition(p);
        real.assignPartition(p);
        model.revoke(p);
        model.assign(p);
        // Source/tracker offset counters keep increasing — the durable log does not rewind.
    }

    /** Full observable-equivalence check; run after every op. */
    void checkInvariants() {
        assertEquals(model.totalPendingCount(), real.totalPendingCount(), "total pending");
        assertEquals(model.nextDeadlineMs(), real.nextDeadlineMs(), "nextDeadlineMs (penalty-adjusted)");
        for (int p = 0; p < partitions; p++) {
            assertEquals(model.pendingCount(p), real.pendingCount(p), "pending count of partition " + p);
            List<ReferenceIndex.RefEntry> expected = model.pendingByTrackerOffset(p);
            List<DrainedKey> visited = new ArrayList<>();
            List<Boolean> visitedClamped = new ArrayList<>();
            int fp = p;
            PartitionShard shard = real.shard(p);
            real.oldestPending(p, (slotId, src, at, trk) -> {
                visited.add(new DrainedKey(fp, src, at, trk));
                visitedClamped.add(shard.clamped(slotId));
                return true;
            });
            assertEquals(expected.size(), visited.size(), "oldestPending size of partition " + p);
            for (int i = 0; i < expected.size(); i++) {
                ReferenceIndex.RefEntry e = expected.get(i);
                DrainedKey k = visited.get(i);
                // Visitation order == trackerAddOffset order, and I5: the ORIGINAL trackerAddOffset
                // survives duplicate ADDs, so min-pending-trackerAddOffset is well defined.
                assertEquals(e.trackerAddOffset, k.trackerAddOffset(), "visitation order / I5 at " + i);
                assertEquals(e.sourceOffset, k.sourceOffset(), "visited sourceOffset at " + i);
                assertEquals(e.dispatchAtMs, k.dispatchAtMs(), "visited dispatchAtMs at " + i);
                assertEquals(e.clamped, visitedClamped.get(i), "visited clamped pass-through at " + i);
            }
        }
    }

    /** Drains everything far in the future and verifies the index empties out like the model. */
    void finalDrain() {
        for (int p = 0; p < partitions; p++) {
            setEligible(p, true); // every shard promoted to ACTIVE before the terminal drain
        }
        drainAndResolve(30L * 24 * 3600 * 1000, Integer.MAX_VALUE, true);
        assertEquals(0, real.totalPendingCount(), "index should be empty after the final drain");
        assertEquals(0, real.totalInFlightCount(), "no in-flight after resolution");
    }
}
