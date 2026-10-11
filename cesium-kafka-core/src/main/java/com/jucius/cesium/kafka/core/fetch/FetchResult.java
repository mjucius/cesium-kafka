package com.jucius.cesium.kafka.core.fetch;

import com.jucius.cesium.kafka.api.store.DueBatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Outcome of one {@link SeekFetcher#fetch} pass over a drained {@link DueBatch}: a parallel view
 * indexed exactly like the candidate batch ({@code outcome(i)} classifies candidate {@code i}),
 * and the retrieved records for {@link FetchOutcome#FOUND} entries.
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
}
