package events.cesium.kafka.core.policy;

/**
 * What to do with a control header value failing the grammar; config
 * {@code delay.on-malformed-header} (design §2.3, default {@link #DLQ} per D3).
 */
public enum MalformedHeaderPolicy {
    /** Dead-letter the record with reason {@code malformed-header}. Default. Requires a DLQ topic. */
    DLQ,
    /** Relay immediately as if no control header were present (the PoC behavior, now opt-in). */
    RELAY_IMMEDIATE,
    /** Stop the ingest loop. */
    FAIL
}
