package com.jucius.cesium.kafka.core.ingest;

/**
 * A fatal ingest-loop failure per the §3.8 taxonomy: the loop has closed (or is about to close)
 * its clients and will not continue — producer fenced, out-of-order sequence, authentication or
 * authorization failure, a {@code FAIL} delay policy firing, or a committed-offset integrity
 * failure ({@code NoOffsetForPartitionException} under the locked {@code auto.offset.reset=none},
 * design I-9). The process is expected to exit non-zero; the durable log is authoritative for the
 * successor.
 */
public final class IngestLoopFatalException extends RuntimeException {

    /** @param message what failed and, where one exists, the runbook pointer */
    public IngestLoopFatalException(String message) {
        super(message);
    }

    /** @param message what failed; {@code cause} the classifying exception */
    public IngestLoopFatalException(String message, Throwable cause) {
        super(message, cause);
    }
}
