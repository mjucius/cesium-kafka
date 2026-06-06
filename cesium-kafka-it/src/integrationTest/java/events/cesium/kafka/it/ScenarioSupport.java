package events.cesium.kafka.it;

import events.cesium.kafka.api.headers.CesiumHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Shared producers and metric readers for the M6 fencing/rebalance scenario suites
 * ({@link ZombieFencingIT}, {@link CooperativeRevocationIT}, {@link PartitionsLostIT}). Kept tiny
 * and dependency-free so each scenario class reads as its own story; the heavy lifting lives in the
 * M6 harness ({@link ToxiproxySupport}, {@link MultiInstanceSupport}, {@link FencingProbe}).
 */
final class ScenarioSupport {

    private ScenarioSupport() {}

    /**
     * Produces {@code count} immediate (no control header) records round-robin across
     * {@code partitions} of {@code topic} via the (fault-immune) {@code bootstrap}; ingest relays
     * each straight to the destination.
     */
    static void produceImmediate(String bootstrap, String topic, int partitions, int count, String keyPrefix) {
        produce(bootstrap, topic, partitions, count, keyPrefix, null);
    }

    /**
     * Produces {@code count} records, each carrying {@code cesium-deliver-at = deliverAtMs} so it is
     * scheduled rather than relayed at ingest. Round-robin across {@code partitions} so a
     * multi-partition source actually splits work across group members.
     */
    static void produceDeliverAt(
            String bootstrap, String topic, int partitions, int count, String keyPrefix, long deliverAtMs) {
        produce(bootstrap, topic, partitions, count, keyPrefix, deliverAtMs);
    }

    private static void produce(
            String bootstrap, String topic, int partitions, int count, String keyPrefix, Long deliverAtMs) {
        try (Producer<byte[], byte[]> producer = ToxiproxySupport.newProducer(bootstrap)) {
            for (int i = 0; i < count; i++) {
                List<Header> headers = deliverAtMs == null
                        ? List.of()
                        : List.of(new RecordHeader(
                                CesiumHeaders.DELIVER_AT,
                                Long.toString(deliverAtMs).getBytes(StandardCharsets.UTF_8)));
                producer.send(new ProducerRecord<>(
                        topic,
                        i % partitions,
                        System.currentTimeMillis(),
                        (keyPrefix + i).getBytes(StandardCharsets.UTF_8),
                        ("v" + i).getBytes(StandardCharsets.UTF_8),
                        headers));
            }
            producer.flush();
        }
    }

    /** The group-B static membership id group B assigns to {@code instanceId} (mirrors group A's). */
    static String dispatchGroupInstanceId(MultiInstanceSupport group, String instanceId) {
        return group.dispatchGroupId() + "." + instanceId;
    }

    /**
     * Joins {@code groupId} as a static member with {@code groupInstanceId}, returning once it has an
     * assignment. When that id already belongs to a live engine member, this is the deterministic
     * <em>duplicate-{@code group.instance.id}</em> fencing primitive (§3.4 / §11.3-3): the broker
     * treats the newcomer as the static member returning and fences the prior holder
     * ({@code FencedInstanceIdException}). A plain consumer (no transactional producer) is used
     * deliberately so the fence is purely group-membership — it never creates a second producer under
     * the prior member's {@code transactional.id}. The caller closes the returned consumer.
     */
    static Consumer<byte[], byte[]> joinAsStaticImpostor(
            String bootstrap, String groupId, String groupInstanceId, String topic, Duration timeout) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1500");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));
        long deadline = System.nanoTime() + timeout.toNanos();
        while (consumer.assignment().isEmpty() && System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(200));
        }
        return consumer;
    }

    /**
     * The live value of a loop's rebalance counter for {@code event}
     * ({@code assigned|revoked|lost}). The loop owns this meter on the harness's registry; reading
     * the same name+tags returns the same {@code Counter}, so this observes the loop's own count
     * (0.0 if the loop never fired the event).
     */
    static double rebalanceEvents(EngineHarness harness, String loop, String event) {
        return harness.meterRegistry()
                .counter("cesium." + loop + ".rebalances", "event", event)
                .count();
    }
}
