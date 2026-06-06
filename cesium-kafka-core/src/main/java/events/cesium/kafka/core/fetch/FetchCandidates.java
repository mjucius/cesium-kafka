package events.cesium.kafka.core.fetch;

import events.cesium.kafka.api.store.DueBatch;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A drained {@link DueBatch} regrouped for the §7.2 seek-fetch shape: one {@link PartitionRun} per
 * source partition, wanted offsets ascending, each entry remembering its original batch index so
 * outcomes land back on the candidate the dispatch loop drained.
 *
 * <p><strong>Why regroup at all.</strong> The index's heap output is per-partition grouped and
 * typically nearly-sorted (§7.1), but the {@link DueBatch} contract orders entries by
 * <em>effective deadline</em>, and within a partition due order ({@code dispatchAtMs}, then
 * arrival) is not offset order — a later-offset record with an earlier deliver-at legitimately
 * pops first. The scan needs strictly ascending offsets ({@code seek(minWanted)} + one forward
 * pass per partition, never one seek per entry), so the runs are normalized here: grouping is a
 * single pass, and the per-partition sort is adaptive (TimSort) on the nearly-sorted common case.
 *
 * <p>Runs surface in <em>first-appearance order</em> of the batch, which approximates due order
 * across partitions — when the overall fetch deadline exhausts mid-pass, the entries forfeited to
 * {@code TRANSIENT} are the ones that were due latest.
 *
 * <p>Built and consumed on the dispatch thread only, like the batch it views.
 */
public final class FetchCandidates {

    private final DueBatch batch;
    private final List<PartitionRun> runs;

    private FetchCandidates(DueBatch batch, List<PartitionRun> runs) {
        this.batch = batch;
        this.runs = runs;
    }

    /** Groups {@code batch} into per-source-partition runs with ascending wanted offsets. */
    public static FetchCandidates of(DueBatch batch) {
        Objects.requireNonNull(batch, "batch");
        Map<Integer, List<Integer>> byPartition = new LinkedHashMap<>();
        for (int i = 0; i < batch.size(); i++) {
            byPartition
                    .computeIfAbsent(batch.sourcePartition(i), partition -> new ArrayList<>())
                    .add(i);
        }
        List<PartitionRun> runs = new ArrayList<>(byPartition.size());
        for (Map.Entry<Integer, List<Integer>> entry : byPartition.entrySet()) {
            List<Integer> indices = entry.getValue();
            // Stable adaptive sort: duplicate offsets (impossible under the unique-committed-ADD
            // invariant, but tolerated) keep their batch order and stay adjacent for the scan.
            indices.sort(Comparator.comparingLong(batch::sourceOffset));
            long[] wantedOffsets = new long[indices.size()];
            int[] batchIndices = new int[indices.size()];
            for (int j = 0; j < indices.size(); j++) {
                batchIndices[j] = indices.get(j);
                wantedOffsets[j] = batch.sourceOffset(batchIndices[j]);
            }
            runs.add(new PartitionRun(entry.getKey(), wantedOffsets, batchIndices));
        }
        return new FetchCandidates(batch, List.copyOf(runs));
    }

    /** The drained batch these runs view. */
    public DueBatch batch() {
        return batch;
    }

    /** Total candidate count — equal to {@code batch().size()}. */
    public int size() {
        return batch.size();
    }

    /** The per-partition runs, in first-appearance (≈ due) order; empty for an empty batch. */
    public List<PartitionRun> runs() {
        return runs;
    }

    /**
     * One source partition's candidates, wanted offsets ascending — the unit the fetcher serves
     * with a single {@code seek(minWantedOffset)} + forward scan (§7.2). Accessors take a run-local
     * index {@code j} in {@code [0, entryCount())} and return primitives.
     */
    public static final class PartitionRun {

        private final int sourcePartition;
        private final long[] wantedOffsets;
        private final int[] batchIndices;

        private PartitionRun(int sourcePartition, long[] wantedOffsets, int[] batchIndices) {
            this.sourcePartition = sourcePartition;
            this.wantedOffsets = wantedOffsets;
            this.batchIndices = batchIndices;
        }

        /** The source partition this run scans. */
        public int sourcePartition() {
            return sourcePartition;
        }

        /** Number of candidates in this run; always ≥ 1. */
        public int entryCount() {
            return wantedOffsets.length;
        }

        /** The wanted source offset of run entry {@code j}; non-decreasing in {@code j}. */
        public long wantedOffset(int j) {
            return wantedOffsets[j];
        }

        /** The original {@link DueBatch} index of run entry {@code j} — where its outcome lands. */
        public int batchIndex(int j) {
            return batchIndices[j];
        }

        /** The scan's seek target: the smallest wanted offset. */
        public long minWantedOffset() {
            return wantedOffsets[0];
        }

        /** The scan's stop bound: the largest wanted offset. */
        public long maxWantedOffset() {
            return wantedOffsets[wantedOffsets.length - 1];
        }
    }
}
