package com.jucius.cesium.kafka.core.policy;

/**
 * What to do when a due entry's payload is provably gone from the source (retention, compaction,
 * size/tier eviction) at dispatch time; config {@code dispatch.on-unfetchable-payload} (design §7,
 * default {@link #DLQ}). Applied by the dispatch loop: in all non-{@link #FAIL} modes the COMPLETE
 * tombstone is still written in the same transaction — the entry is resolved exactly once and
 * never replays forever.
 */
public enum UnfetchablePayloadPolicy {
    /** Produce the §2.4 payload-expired loss notice to the DLQ. Default. Requires a DLQ topic. */
    DLQ,
    /** Complete the entry with reason {@code DROPPED} and a metric; no notice. */
    DROP,
    /** Stop the dispatch loop: payload loss is an environment failure to surface. */
    FAIL
}
