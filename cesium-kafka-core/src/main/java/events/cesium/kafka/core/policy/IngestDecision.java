package events.cesium.kafka.core.policy;

/**
 * Final ingest action for one source record after policies are applied (design §2.3, §3.1) — what
 * the ingest loop actually does inside the transaction.
 *
 * <p>Produced by {@link IngestPolicyEngine} from a
 * {@link events.cesium.kafka.core.headers.HeaderParseResult}; an exhaustive sealed hierarchy so
 * the loop's {@code switch} cannot silently miss a disposition. Maps 1:1 onto the
 * {@code cesium_ingest_records_total} outcome tags.
 */
public sealed interface IngestDecision {

    /** Relay to the destination now, inside the ingest transaction. */
    record RelayNow() implements IngestDecision {
        /** Shared instance for the hot path. */
        public static final RelayNow INSTANCE = new RelayNow();
    }

    /**
     * Schedule for later dispatch: write the tracker ADD (or external-store upsert).
     *
     * @param dispatchAtMs validated delivery instant, epoch milliseconds UTC
     */
    record Schedule(long dispatchAtMs) implements IngestDecision {}

    /**
     * Dead-letter the record inside the ingest transaction.
     *
     * @param reason wire value for {@code cesium-error-reason}
     *     ({@link events.cesium.kafka.core.headers.DlqReasons})
     * @param detail human-readable diagnostic for {@code cesium-error-detail}
     */
    record ToDlq(String reason, String detail) implements IngestDecision {}

    /**
     * Schedule at the clamp target {@code now + delay.max} per {@code on-over-max: CLAMP}; the
     * relay must stamp {@code cesium-clamped: true} (design §2.3).
     *
     * @param dispatchAtMs the clamp target, overflow-saturated
     */
    record Clamp(long dispatchAtMs) implements IngestDecision {}

    /**
     * Stop the loop: a {@code FAIL} policy fired. The record is not consumed (the transaction
     * aborts) and the violation surfaces to the operator.
     *
     * @param detail human-readable diagnostic for the failure log
     */
    record Fail(String detail) implements IngestDecision {}
}
