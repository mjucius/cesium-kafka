package com.jucius.cesium.kafka.core.dispatch;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

/**
 * Production {@link DispatchAdmin} over the shared {@code Admin} client (design §6's
 * {@code cesium-admin} plane). The barrier snapshot is {@code Admin.listOffsets(latest)} with
 * <strong>explicit</strong> {@code READ_UNCOMMITTED} isolation — the option default already is
 * read_uncommitted, but the isolation byte is correctness-load-bearing (D5: HW, not LSO), so it is
 * spelled out rather than inherited.
 *
 * <p>Thread-safe ({@code Admin} is); the futures complete on the admin client's own thread and the
 * dispatch loop only ever inspects them non-blockingly.
 */
public final class KafkaDispatchAdmin implements DispatchAdmin {

    private final Admin admin;
    private final String trackerTopic;

    /**
     * @param admin the shared admin client; the caller owns its lifecycle
     * @param trackerTopic the resolved tracker topic name barriers are snapshotted on
     */
    public KafkaDispatchAdmin(Admin admin, String trackerTopic) {
        this.admin = Objects.requireNonNull(admin, "admin");
        this.trackerTopic = Objects.requireNonNull(trackerTopic, "trackerTopic");
    }

    @Override
    public CompletableFuture<Long> trackerHighWatermark(int partition) {
        TopicPartition tp = new TopicPartition(trackerTopic, partition);
        return admin.listOffsets(
                        Map.of(tp, OffsetSpec.latest()), new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED))
                .partitionResult(tp)
                .toCompletionStage()
                .toCompletableFuture()
                .thenApply(ListOffsetsResult.ListOffsetsResultInfo::offset);
    }
}
