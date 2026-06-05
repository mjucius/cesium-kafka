package events.cesium.kafka.api.store;

/**
 * Why a scheduled entry was settled (design §2.2). Carried on the completion record's
 * <em>header</em> — completion records are null-value tombstones for compaction, and headers
 * survive tombstoning where values cannot (design D15).
 *
 * <p>Every settle path produces a completion with a reason: entries are resolved exactly once and
 * never silently lost, whatever the outcome.
 */
public enum CompletionReason {
    /** The payload was relayed to the destination topic — the normal outcome. */
    DISPATCHED,
    /**
     * The payload was no longer fetchable from the source (retention, compaction, or size/tier
     * eviction) and a loss notice was produced to the DLQ in the same transaction (design §2.4,
     * D-9).
     */
    PAYLOAD_MISSING_DLQ,
    /** The entry was discarded by explicit policy without a DLQ notice. */
    DROPPED,
    /** The entry was rejected by policy at settle time (e.g. unfetchable-payload policy FAIL path). */
    REJECTED
}
