package com.jucius.cesium.kafka.core.fetch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/** §7.2 run normalization: per-partition grouping, ascending offsets, index round-trip. */
class FetchCandidatesTest {

    @Test
    void groupsByPartitionInFirstAppearanceOrderAndSortsOffsetsAscending() {
        // Due order is not offset order: a later-offset entry with an earlier deliver-at pops
        // first, so within-partition offsets arrive unsorted (§7.1 "typically nearly-sorted").
        FakeDueBatch batch = FakeDueBatch.of(new int[] {1, 0, 1, 0}, new long[] {9, 7, 3, 8});
        FetchCandidates candidates = FetchCandidates.of(batch);

        assertSame(batch, candidates.batch());
        assertEquals(4, candidates.size());
        List<FetchCandidates.PartitionRun> runs = candidates.runs();
        assertEquals(2, runs.size());

        FetchCandidates.PartitionRun first = runs.get(0);
        assertEquals(1, first.sourcePartition(), "partition 1 appeared first in the batch");
        assertEquals(2, first.entryCount());
        assertEquals(3, first.wantedOffset(0));
        assertEquals(9, first.wantedOffset(1));
        assertEquals(2, first.batchIndex(0), "offset 3 came from batch index 2");
        assertEquals(0, first.batchIndex(1), "offset 9 came from batch index 0");
        assertEquals(3, first.minWantedOffset());
        assertEquals(9, first.maxWantedOffset());

        FetchCandidates.PartitionRun second = runs.get(1);
        assertEquals(0, second.sourcePartition());
        assertEquals(7, second.wantedOffset(0));
        assertEquals(8, second.wantedOffset(1));
        assertEquals(1, second.batchIndex(0));
        assertEquals(3, second.batchIndex(1));
    }

    @Test
    void duplicateOffsetsStayAdjacentAndKeepBatchOrder() {
        FakeDueBatch batch = FakeDueBatch.of(new int[] {0, 0, 0}, new long[] {5, 5, 4});
        FetchCandidates.PartitionRun run = FetchCandidates.of(batch).runs().get(0);
        assertEquals(4, run.wantedOffset(0));
        assertEquals(5, run.wantedOffset(1));
        assertEquals(5, run.wantedOffset(2));
        assertEquals(0, run.batchIndex(1), "stable sort keeps the first duplicate first");
        assertEquals(1, run.batchIndex(2));
    }

    @Test
    void emptyBatchYieldsNoRuns() {
        FetchCandidates candidates = FetchCandidates.of(FakeDueBatch.empty());
        assertEquals(0, candidates.size());
        assertTrue(candidates.runs().isEmpty());
    }
}
