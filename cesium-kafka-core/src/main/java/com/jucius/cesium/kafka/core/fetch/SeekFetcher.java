package com.jucius.cesium.kafka.core.fetch;

import com.jucius.cesium.kafka.api.store.DueBatch;

/**
 * The payload re-fetch boundary the dispatch loop drives <em>outside</em> its transaction (design
 * §7, §3.2): given the drained due candidates, retrieve their source records under the three
 * budgets — per-partition time slice, overall deadline, decompressed-byte budget — and classify
 * every candidate exactly once ({@link FetchOutcome}).
 *
 * <p><strong>Contract.</strong>
 *
 * <ul>
 *   <li>One classification per candidate: {@code result.size() == candidates.size()} and
 *       {@code outcome(i)} is never unset. Entries are never silently dropped — an unresolved
 *       entry is {@code TRANSIENT}, not absent (§7.3).
 *   <li>Per source partition, one {@code assign} + {@code seek(minWantedOffset)} + forward
 *       sequential scan serves all of that partition's candidates (§7.2) — never one seek per
 *       entry.
 *   <li>{@code GONE} requires proof: {@code beginningOffsets > wantedOffset} or the scan passed
 *       the offset; a deadline expiry is {@code TRANSIENT} and is re-checked against
 *       {@code beginningOffsets} before classification.
 *   <li>The decompressed-byte budget is enforced as records arrive; once it trips, the remaining
 *       candidates are {@code CARRY_OVER} (truncate-and-carry-over, D8).
 *   <li>Transport failures are classified, not thrown: a throwing fetch pass forfeits per-entry
 *       resolution and forces the loop to treat the whole batch as transient.
 * </ul>
 *
 * <p>Implementations own a group-less {@code assign()}/{@code seek()} consumer locked to
 * {@code read_committed} (D17) and are confined to the dispatch thread that owns them (§6).
 */
public interface SeekFetcher extends AutoCloseable {

    /**
     * Fetches the payloads of {@code candidates} from the source topic.
     *
     * @param candidates the batch drained by {@code pollDue} (grouped per source partition,
     *     offsets ascending — the heap's natural output)
     * @param maxBytes the decompressed payload byte budget for this pass (§7.2, D8)
     * @param deadlineMs absolute wall-clock deadline (epoch ms) for the whole pass
     * @return the per-candidate classification; valid until the next {@code fetch} call
     */
    FetchResult fetch(DueBatch candidates, long maxBytes, long deadlineMs);

    /** Releases the seek consumer. Idempotent; never throws checked exceptions. */
    @Override
    void close();
}
