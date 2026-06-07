package com.jucius.cesium.kafka.core.dispatch;

/**
 * A fatal dispatch-loop failure per the §3.8 taxonomy: the loop has closed (or is about to close)
 * its clients and will not continue — producer fenced, out-of-order sequence,
 * authentication/authorization failure, a tracker-integrity fail-fast (committed cursor outside
 * {@code [beginning, end]}, {@code OffsetOutOfRangeException}, a non-first-run partition with no
 * committed offset under the locked {@code auto.offset.reset=none} — §3.6/R11), or the
 * unfetchable-payload {@code FAIL} policy firing. The process is expected to exit non-zero; the
 * durable log is authoritative for the successor.
 */
public final class DispatchLoopFatalException extends RuntimeException {

    /** @param message what failed and, where one exists, the runbook pointer */
    public DispatchLoopFatalException(String message) {
        super(message);
    }

    /** @param message what failed; {@code cause} the classifying exception */
    public DispatchLoopFatalException(String message, Throwable cause) {
        super(message, cause);
    }
}
