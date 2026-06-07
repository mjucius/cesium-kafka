package com.jucius.cesium.kafka.testkit;

import com.jucius.cesium.kafka.api.store.CompletionReason;

/**
 * One abstract operation of a randomized store-interaction sequence (see
 * {@link StoreOpArbitraries}). Operations carry <em>relative</em> values (offset gaps, delays,
 * pick indexes) so any sequence is executable against any starting state; the executor in
 * {@code TrackerBackedStoreContract} resolves them against its running model.
 */
public sealed interface StoreOp {

    /** Schedule a new entry: next source offset advances by {@code sourceOffsetGap} (≥ 1). */
    record Add(int partition, int sourceOffsetGap, long delayMs, boolean clamped) implements StoreOp {}

    /**
     * Re-schedule an already-pending source offset (the R1 duplicate-ADD anomaly): the executor
     * picks the {@code pickIndex mod pendingCount}-th pending key of {@code partition} and feeds
     * a second ADD with a new delay. Skipped when nothing is pending.
     */
    record DuplicateAdd(int partition, int pickIndex, long delayMs) implements StoreOp {}

    /** Advance virtual time by {@code deltaMs} (≥ 0). */
    record AdvanceTime(long deltaMs) implements StoreOp {}

    /** Poll up to {@code maxBatch} due entries and commit them with {@code reason}. */
    record DrainCommit(int maxBatch, CompletionReason reason) implements StoreOp {}

    /**
     * Poll up to {@code maxBatch} due entries and commit only a strict prefix of them — the
     * §3.2/§7 truncate-and-carry-over shape: the executor settles
     * {@code 1 + settleSeed % (size - 1)} entries via
     * {@code TrackerStoreHarness.commitDispatchPartial} and the remainder returns to pending
     * (drains of fewer than two entries fall back to a definitive abort, since no strict subset
     * exists).
     */
    record DrainCommitPartial(int maxBatch, int settleSeed, CompletionReason reason) implements StoreOp {}

    /** Poll up to {@code maxBatch} due entries and definitively abort the transaction. */
    record DrainAbort(int maxBatch) implements StoreOp {}

    /** The records-free idle-advancement transaction for {@code partition}. */
    record IdleCommit(int partition) implements StoreOp {}

    /** Drop {@code partition} (lost-style) and re-recover it from the last committed cursor. */
    record Rerecover(int partition) implements StoreOp {}
}
