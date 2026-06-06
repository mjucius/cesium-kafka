package events.cesium.kafka.core.dispatch;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
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
    private final String groupId;

    /**
     * @param admin the shared admin client; the caller owns its lifecycle
     * @param trackerTopic the resolved tracker topic name barriers are snapshotted on
     * @param groupId the dispatch group id ({@code cesium.<applicationId>.dispatch}) probed for
     *     the first-run proof
     */
    public KafkaDispatchAdmin(Admin admin, String trackerTopic, String groupId) {
        this.admin = Objects.requireNonNull(admin, "admin");
        this.trackerTopic = Objects.requireNonNull(trackerTopic, "trackerTopic");
        this.groupId = Objects.requireNonNull(groupId, "groupId");
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

    @Override
    public boolean groupHasCommittedOffsets() {
        final Map<TopicPartition, OffsetAndMetadata> offsets;
        try {
            offsets = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaException("interrupted while probing committed offsets for group " + groupId, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new KafkaException("could not probe committed offsets for group " + groupId, cause);
        }
        // The broker returns null metadata entries for partitions it knows without offsets.
        return offsets.values().stream().anyMatch(Objects::nonNull);
    }
}
