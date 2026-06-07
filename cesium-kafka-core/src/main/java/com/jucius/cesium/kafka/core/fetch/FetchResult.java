package com.jucius.cesium.kafka.core.fetch;

import com.jucius.cesium.kafka.api.store.DueBatch;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Outcome of one {@link SeekFetcher#fetch} pass over a drained {@link DueBatch}: a parallel view
 * indexed exactly like the candidate batch ({@code outcome(i)} classifies candidate {@code i}),
 * the retrieved records for {@link FetchOutcome#FOUND} entries, the
 * {@linkplain #carryOverIndices() carry-over indices} the byte budget excluded, and
 * {@linkplain #partitionSummaries() per-source-partition summaries} feeding the penalty box and
 * the §7 fetch metrics.
 *
 * <p>Produced and consumed on the dispatch thread only; instances are valid until the next fetch
 * pass and must not be retained across it (implementations may reuse buffers).
 */
public interface FetchResult {

    /** Number of classified entries — always equal to the candidate batch's {@code size()}. */
    int size();

    /** Disposition of candidate {@code i} (index into the candidate {@link DueBatch}). */
    FetchOutcome outcome(int i);

    /**
     * The source record retrieved for candidate {@code i}.
     *
     * @throws IllegalStateException when {@code outcome(i) != FOUND}
     */
    ConsumerRecord<byte[], byte[]> record(int i);

    /**
     * Per-source-partition outcome counts and decompressed byte volume, for the §7 observability
     * surface ({@code cesium_fetch_*}) and the loop's penalty-box stamping.
     */
    List<PartitionSummary> partitionSummaries();

    /**
     * Indices of the candidates the byte budget excluded ({@link FetchOutcome#CARRY_OVER}),
     * ascending. Derived from {@link #outcome(int)} by default; implementations that already know
     * the cut may override.
     */
    default int[] carryOverIndices() {
        int count = 0;
        for (int i = 0; i < size(); i++) {
            if (outcome(i) == FetchOutcome.CARRY_OVER) {
                count++;
            }
        }
        int[] indices = new int[count];
        int next = 0;
        for (int i = 0; i < size(); i++) {
            if (outcome(i) == FetchOutcome.CARRY_OVER) {
                indices[next++] = i;
            }
        }
        return indices;
    }

    /**
     * One source partition's slice of the pass.
     *
     * @param sourcePartition the source partition the run targeted
     * @param found entries retrieved
     * @param gone entries provably expired
     * @param transientFailures entries left unresolved (slice/deadline/transport, §7.3) — when
     *     positive the loop penalty-boxes the partition
     * @param carriedOver entries never attempted because the byte budget tripped (§7.2)
     * @param fetchedBytes decompressed payload bytes retrieved from this partition
     */
    record PartitionSummary(
            int sourcePartition, int found, int gone, int transientFailures, int carriedOver, long fetchedBytes) {}
}
