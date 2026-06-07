package com.jucius.cesium.kafka.store.tracker;

/**
 * Controlled fail-fast raised when replaying the durable tracker backlog into the in-memory index
 * would push the global resident pending-entry count past the heap budget the store was sized for
 * (security finding H1; design §5.3/§5.4).
 *
 * <p><strong>Why this exists.</strong> The in-memory index cap is enforced only via dispatch-side
 * backpressure, which is deliberately skipped for {@code RECOVERING} shards — they must replay all
 * the way to the barrier (I4, §3.6), and a paused-forever recovery would wedge the relay. A large
 * durable tracker backlog (a producer flood of far-future delays, or a benign ingest-outruns-dispatch
 * period after a destination outage) is therefore replayed into the otherwise-uncapped index. Without
 * this guard that replay allocates until the JVM throws {@link OutOfMemoryError}, and since recovery
 * re-reads the same committed backlog on the next boot, the result is a self-perpetuating OOM
 * crash-loop. This exception replaces that silent OOM with a deterministic, attributable failure
 * carrying the exact budget arithmetic and an operator action.
 *
 * <p><strong>Disposition (fatal).</strong> The store throws this from the recovery seed
 * ({@code beginRecovery}) and replay ({@code onTrackerRecord}) paths; it propagates out of the
 * dispatch loop as a fatal worker failure (the process exits non-zero), which is the correct
 * outcome because the alternative is an {@code OutOfMemoryError} crash anyway. Nothing is committed
 * — no tracker record is skipped-and-committed-past and no shard is promoted ACTIVE before its
 * barrier — so the durable log stays authoritative and exactly-once semantics are preserved (I4/I8).
 * The operator must raise the heap (or {@code store.properties heap-budget-bytes}) and/or shrink the
 * durable backlog (lower {@code delay.max}, drain the source, shorten source retention) before the
 * restart can make progress. The companion {@code cesium_recovery_over_budget} counter and the
 * runbook-style ERROR log line make the cause unmistakable — it is never a generic OOM.
 */
public final class RecoveryHeapBudgetExceededException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    RecoveryHeapBudgetExceededException(String message) {
        super(message);
    }
}
