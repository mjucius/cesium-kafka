package com.jucius.cesium.kafka.core.loop;

/**
 * A fatal loop failure per the §3.8 taxonomy: the ingest or dispatch loop has closed (or is about
 * to close) its clients and will not continue — producer fenced, out-of-order sequence,
 * authentication or authorization failure, a {@code FAIL} policy firing, or a committed-offset /
 * cursor integrity failure ({@code NoOffsetForPartitionException} under the locked {@code
 * auto.offset.reset=none}, design I-9). The process is expected to exit non-zero; the durable log
 * is authoritative for the successor.
 */
public final class LoopFatalException extends RuntimeException {

    /** @param message what failed and, where one exists, the runbook pointer */
    public LoopFatalException(String message) {
        super(message);
    }

    /** @param message what failed; {@code cause} the classifying exception */
    public LoopFatalException(String message, Throwable cause) {
        super(message, cause);
    }
}
