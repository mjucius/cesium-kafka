package com.jucius.cesium.kafka.core.policy;

/**
 * What to do with a well-formed request exceeding {@code delay.max}; config
 * {@code delay.on-over-max} (design §2.3, default {@link #DLQ} per D3 — delivering early a message
 * someone intended to delay is a business hazard, so the default keeps the pipeline alive while
 * making the violation explicit).
 */
public enum OverMaxPolicy {
    /** Dead-letter the record with reason {@code over-max-delay}. Default. Requires a DLQ topic. */
    DLQ,
    /** Schedule at {@code now + delay.max} and stamp {@code cesium-clamped: true} on relay. */
    CLAMP,
    /**
     * Stop the ingest loop: the violation is an operator problem, not a data problem.
     *
     * <p><strong>Unsafe for untrusted ingress (L5):</strong> one crafted record (a delay above
     * {@code delay.max}) fatally stops ingest and, since the offset never commits under the locked
     * {@code auto.offset.reset=none}, re-fails on every restart — a pipeline-wide, restart-persistent
     * outage. Use only when the SOURCE topic has exclusively trusted producers; startup WARNs.
     */
    FAIL
}
