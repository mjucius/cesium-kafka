package events.cesium.kafka.core.kafka;

import events.cesium.kafka.core.config.CesiumConfig;
import events.cesium.kafka.core.config.InstanceId;
import events.cesium.kafka.core.config.KafkaConfig;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Builds the engine's Kafka client {@link Properties} from the frozen {@link CesiumConfig}
 * (design §8): tuned defaults first, then the common {@code kafka.properties} map, then the
 * per-client overlay, then the <strong>locked keys re-asserted last</strong> so they always win.
 *
 * <p>The validator already rejects locked-key overrides in passthrough maps
 * ({@code LockedKafkaKeys}); the factory re-asserts them anyway — defense in depth against any
 * properties source the validator did not see. This is the structural fix for the PoC's
 * {@code Properties(defaults)} misuse class: correctness-critical client config cannot drift.
 *
 * <p><strong>Naming schemes</strong> (design §3.3, D10, D21):
 *
 * <ul>
 *   <li>group A id: {@code cesium.<applicationId>.ingest}
 *   <li>static membership: {@code group.instance.id = cesium.<applicationId>.ingest.<instanceId>},
 *       omitted when the operator opted into {@code instance-id: random} (static membership is
 *       pointless for a non-stable id)
 *   <li>transactional id: {@code cesium.<applicationId>.ingest.<instanceId>.<workerOrdinal>} —
 *       stable across restarts so {@code initTransactions()} immediately fences a predecessor's
 *       dangling transaction instead of waiting out {@code transaction.timeout.ms}; under the
 *       {@code random} opt-in the instance component is a per-process random token (unique among
 *       live producers, at the documented cost of crash-failover latency)
 * </ul>
 *
 * <p>One factory instance per process: the random instance token is drawn once at construction so
 * every client of this process shares it.
 */
public final class KafkaClientFactory {

    /** Tuned producer default: {@code linger.ms} (§8). */
    static final String PRODUCER_LINGER_MS = "10";

    /** Tuned producer default: {@code batch.size} 256 KiB (§8). */
    static final String PRODUCER_BATCH_SIZE = "262144";

    /** Tuned producer default: {@code compression.type} (§8). */
    static final String PRODUCER_COMPRESSION = "lz4";

    /** Tuned producer default: {@code buffer.memory} 64 MiB (§8). */
    static final String PRODUCER_BUFFER_MEMORY = "67108864";

    /** Tuned ingest-consumer default: {@code max.partition.fetch.bytes} 4 MiB (§8). */
    static final String INGEST_MAX_PARTITION_FETCH_BYTES = "4194304";

    private final CesiumConfig config;
    private final String effectiveInstanceId;

    /** @param config a validated configuration (the factory assumes a clean ValidationReport) */
    public KafkaClientFactory(CesiumConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        InstanceId instanceId = config.instanceId();
        this.effectiveInstanceId =
                instanceId.isRandom() ? InstanceId.RANDOM_LITERAL + "-" + UUID.randomUUID() : instanceId.value();
    }

    /**
     * Group A's consumer group id for an application: {@code cesium.<applicationId>.ingest}
     * (design §3.3). Static so non-client collaborators (e.g. the startup validator reading the
     * group's committed-offset identity blobs, R-10) derive the same name without building a
     * factory.
     */
    public static String ingestGroupId(String applicationId) {
        return "cesium." + applicationId + ".ingest";
    }

    /** Group A's consumer group id: {@code cesium.<applicationId>.ingest}. */
    public String ingestGroupId() {
        return ingestGroupId(config.applicationId());
    }

    /**
     * Group A's static membership id, or empty under the {@code instance-id: random} opt-in
     * (design D21: static membership is default on, keyed by the stable deployment-slot id).
     */
    public Optional<String> ingestGroupInstanceId() {
        if (config.instanceId().isRandom()) {
            return Optional.empty();
        }
        return Optional.of(ingestGroupId() + "." + effectiveInstanceId);
    }

    /**
     * The ingest transactional id for one worker:
     * {@code cesium.<applicationId>.ingest.<instanceId>.<workerOrdinal>} (design §3.3, D10).
     */
    public String ingestTransactionalId(int workerOrdinal) {
        return ingestGroupId() + "." + effectiveInstanceId + "." + workerOrdinal;
    }

    /**
     * Properties for the group-A source consumer: tuned defaults ({@code max.poll.records} from
     * {@code ingest.max-batch}, 4 MiB partition fetch), common map, ingest-consumer overlay, then
     * the locked keys — byte-array deserialization, auto-commit off,
     * {@code isolation.level=read_committed} (D17), {@code auto.offset.reset=none} (D18), the
     * derived group id and static-membership id.
     */
    public Properties ingestConsumerProperties() {
        Properties props = new Properties();
        // 1. Tuned defaults (§8) — overridable by operator maps.
        props.setProperty(
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                String.valueOf(config.ingest().maxBatch()));
        props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, INGEST_MAX_PARTITION_FETCH_BYTES);
        // 2. Common passthrough, then 3. the per-client overlay (overlay wins).
        putAll(props, config.kafka().properties());
        putAll(props, config.kafka().ingestConsumer().properties());
        // The typed kafka.group-protocol knob outranks a raw group.protocol in a passthrough map
        // (mirrors transactions.timeout): the engine's rebalance-listener semantics and the §11
        // CI matrix key off the typed value, so an overlay must never silently flip the real
        // protocol while config.kafka().groupProtocol() reports otherwise.
        if (config.kafka().groupProtocol() == KafkaConfig.GroupProtocol.CONSUMER) {
            props.setProperty(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        } else {
            props.remove(ConsumerConfig.GROUP_PROTOCOL_CONFIG);
        }
        // 4. Locked keys last — they always win (D17/D18, §8).
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ingestGroupId());
        Optional<String> groupInstanceId = ingestGroupInstanceId();
        if (groupInstanceId.isPresent()) {
            props.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId.get());
        } else {
            // random opt-in: a leaked static-membership id would fence the next process (D21).
            props.remove(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
        }
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return props;
    }

    /**
     * Properties for one ingest worker's transactional producer: tuned defaults (linger 10 ms,
     * 256 KiB batches, lz4, 64 MiB buffer), common map, ingest-producer overlay, the typed
     * {@code kafka.transactions.timeout}, then the locked keys — the derived transactional id,
     * idempotence on, byte-array serialization.
     */
    public Properties ingestProducerProperties(int workerOrdinal) {
        Properties props = new Properties();
        // 1. Tuned defaults (§8) — overridable by operator maps.
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, PRODUCER_LINGER_MS);
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, PRODUCER_BATCH_SIZE);
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, PRODUCER_COMPRESSION);
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, PRODUCER_BUFFER_MEMORY);
        // 2. Common passthrough, then 3. the per-client overlay (overlay wins).
        putAll(props, config.kafka().properties());
        putAll(props, config.kafka().ingestProducer().properties());
        // The typed kafka.transactions.timeout key is the documented knob (D9); it outranks a raw
        // transaction.timeout.ms in a passthrough map so commit-retry math never desyncs from it.
        props.setProperty(
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                String.valueOf(config.kafka().transactions().timeout().toMillis()));
        // 4. Locked keys last — they always win (§8).
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, ingestTransactionalId(workerOrdinal));
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return props;
    }

    /** Creates the group-A source consumer. The caller's thread owns it for its whole life (§6). */
    public Consumer<byte[], byte[]> newIngestConsumer() {
        return new KafkaConsumer<>(ingestConsumerProperties());
    }

    /** Creates one ingest worker's transactional producer. The caller's thread owns it (§6). */
    public Producer<byte[], byte[]> newIngestProducer(int workerOrdinal) {
        return new KafkaProducer<>(ingestProducerProperties(workerOrdinal));
    }

    private static void putAll(Properties props, Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            props.setProperty(entry.getKey(), entry.getValue());
        }
    }
}
