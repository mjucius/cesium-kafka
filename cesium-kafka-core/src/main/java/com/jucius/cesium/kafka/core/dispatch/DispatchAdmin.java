package com.jucius.cesium.kafka.core.dispatch;

import java.util.concurrent.CompletableFuture;

/**
 * The narrow admin-plane boundary the dispatch loop talks through (design §6's
 * {@code cesium-admin} client): the §3.6 replay-barrier snapshot. An interface so unit tests
 * inject deterministic futures; production wires {@link KafkaDispatchAdmin} around the real
 * {@code Admin} client (whose own thread services the futures — the loop never blocks on them).
 */
public interface DispatchAdmin {

    /**
     * Snapshots tracker partition {@code partition}'s <em>high watermark</em> via
     * {@code Admin.listOffsets(latest)} with {@code READ_UNCOMMITTED} isolation (design §3.6 step
     * 3, D5) — never the consumer's {@code endOffsets}, which returns the LSO under
     * {@code read_committed} and would reopen the §3.6 LSO hazard. The loop epoch-tags the
     * returned future and discards it if the partition is revoked before it completes (I8).
     */
    CompletableFuture<Long> trackerHighWatermark(int partition);
}
