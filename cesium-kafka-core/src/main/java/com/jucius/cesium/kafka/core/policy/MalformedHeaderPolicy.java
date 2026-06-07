package com.jucius.cesium.kafka.core.policy;

/**
 * What to do with a control header value failing the grammar; config
 * {@code delay.on-malformed-header} (design §2.3, default {@link #DLQ} per D3).
 */
public enum MalformedHeaderPolicy {
    /** Dead-letter the record with reason {@code malformed-header}. Default. Requires a DLQ topic. */
    DLQ,
    /** Relay immediately as if no control header were present (the PoC behavior, now opt-in). */
    RELAY_IMMEDIATE,
    /**
     * Stop the ingest loop.
     *
     * <p><strong>Unsafe for untrusted ingress (L5):</strong> one crafted record (a malformed
     * {@code cesium-delay-ms}) fatally stops ingest and, since the offset never commits under the
     * locked {@code auto.offset.reset=none}, re-fails on every restart — a pipeline-wide,
     * restart-persistent outage. Use only when the SOURCE topic has exclusively trusted producers;
     * startup WARNs.
     */
    FAIL
}
