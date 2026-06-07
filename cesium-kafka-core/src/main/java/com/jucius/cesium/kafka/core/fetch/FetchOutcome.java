package com.jucius.cesium.kafka.core.fetch;

/**
 * Per-entry disposition of one seek-fetch pass (design §7.3 tri-state, plus the §7.2 byte-budget
 * truncation). The dispatch loop settles {@link #FOUND} and {@link #GONE} entries inside the next
 * transaction; {@link #TRANSIENT} and {@link #CARRY_OVER} entries are excluded and returned to
 * pending (the carry-over sub-view of the drained batch) — never silently dropped.
 */
public enum FetchOutcome {

    /** The record at the wanted offset was retrieved; it proceeds to the dispatch transaction. */
    FOUND,

    /**
     * Provably expired: {@code beginningOffsets(partition) > wantedOffset}, or the scan passed the
     * offset without yielding it — the payload was lost to retention/compaction/size-eviction.
     * Settled in the same transaction under the unfetchable-payload policy (D-9).
     */
    GONE,

    /**
     * Unresolved within the slice/deadline, or a transport error — re-checked against
     * {@code beginningOffsets} before classification (§7.3). The entry stays pending and its
     * source partition enters the penalty box (D22).
     */
    TRANSIENT,

    /**
     * Never attempted: the decompressed-byte budget tripped first (§7.2 truncate-and-carry-over,
     * D8). The entry stays pending — still due, the next drain slice picks it up — with no
     * penalty: nothing failed.
     */
    CARRY_OVER
}
