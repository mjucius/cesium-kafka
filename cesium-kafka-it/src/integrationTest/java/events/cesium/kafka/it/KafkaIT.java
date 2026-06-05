package events.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Reusable integration-test base (M4; M5/M6 extend it): one KRaft Kafka container shared by every
 * IT class in the JVM, an {@link Admin} helper, byte-array test producers, read_committed verifier
 * and read_uncommitted counter consumers, awaitility-based waits, and unique per-test naming.
 *
 * <p><strong>Container lifecycle.</strong> The container is a singleton started on first class
 * load and intentionally never stopped from test code — Testcontainers' Ryuk reaper removes it
 * when the JVM exits. Sharing one broker across classes is safe because every test uses unique
 * topic and application-id names; it is dramatically faster and less flaky than per-class
 * restarts. {@code auto.create.topics.enable=false} so missing-topic startup checks observe a
 * deterministic broker.
 */
abstract class KafkaIT {

    /** Latest 4.x KRaft image (verified available; matches the kafka-clients 4.3.0 in the catalog). */
    static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("apache/kafka:4.3.0");

    static final KafkaContainer KAFKA =
            new KafkaContainer(KAFKA_IMAGE).withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    /** Shared admin client over the singleton broker; closed by the JVM exiting (test-only). */
    static final Admin ADMIN;

    static {
        KAFKA.start();
        ADMIN = Admin.create(Map.<String, Object>of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap()));
    }

    private static final AtomicLong UNIQUE = new AtomicLong();

    /** Generous-but-bounded default wait for asynchronous broker-side effects. */
    static final Duration WAIT = Duration.ofSeconds(60);

    static String bootstrap() {
        return KAFKA.getBootstrapServers();
    }

    /** Unique per-test name: {@code prefix-<counter>-<random>} (topics, app ids, group ids). */
    static String unique(String prefix) {
        return prefix + "-" + UNIQUE.incrementAndGet() + "-"
                + UUID.randomUUID().toString().substring(0, 8);
    }

    /** Creates a uniquely named topic with broker-default configs and returns its name. */
    static String createTopic(String prefix, int partitions) {
        return createTopic(prefix, partitions, Map.of());
    }

    /** Creates a uniquely named topic with the given topic configs and returns its name. */
    static String createTopic(String prefix, int partitions, Map<String, String> configs) {
        String name = unique(prefix);
        createNamedTopic(name, partitions, configs);
        return name;
    }

    /** Creates an exactly named topic (e.g. a pre-provisioned tracker for mismatch scenarios). */
    static void createNamedTopic(String name, int partitions, Map<String, String> configs) {
        NewTopic topic = new NewTopic(name, partitions, (short) 1).configs(configs);
        get(ADMIN.createTopics(List.of(topic)).all());
    }

    /** A plain (non-transactional) byte-array test producer; caller closes it. */
    static Producer<byte[], byte[]> newProducer() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    /** Synchronously produces one record and returns its metadata (offset assertions need it). */
    static RecordMetadata produce(
            Producer<byte[], byte[]> producer,
            String topic,
            Integer partition,
            Long timestamp,
            String key,
            String value,
            Header... headers) {
        ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(topic, partition, timestamp, bytes(key), bytes(value), List.of(headers));
        try {
            return producer.send(record).get();
        } catch (Exception e) {
            throw new AssertionError("produce to " + topic + " failed", e);
        }
    }

    static Header h(String name, String value) {
        return new RecordHeader(name, bytes(value));
    }

    static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    static String utf8(byte[] raw) {
        return new String(raw, StandardCharsets.UTF_8);
    }

    /** Last value of a header as UTF-8, or null when absent. */
    static String headerValue(Headers headers, String name) {
        Header header = headers.lastHeader(name);
        return header == null || header.value() == null ? null : utf8(header.value());
    }

    // ------------------------------------------------------------------ verifier consumers

    /**
     * The read_committed verifier: reads {@code topic} from the beginning until exactly
     * {@code expected} committed records are visible, then keeps polling for a one-second grace
     * window and asserts no extras arrived — "each exactly once" under read_committed.
     */
    static List<ConsumerRecord<byte[], byte[]>> readExactlyCommitted(String topic, int expected, Duration timeout) {
        try (Consumer<byte[], byte[]> consumer = newAssignedConsumer(topic, "read_committed")) {
            List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
            await("exactly " + expected + " read_committed record(s) on " + topic)
                    .atMost(timeout)
                    .pollInSameThread()
                    .until(() -> {
                        consumer.poll(Duration.ofMillis(200)).forEach(out::add);
                        return out.size() >= expected;
                    });
            long graceDeadline = System.nanoTime() + Duration.ofSeconds(1).toNanos();
            while (System.nanoTime() < graceDeadline) {
                consumer.poll(Duration.ofMillis(100)).forEach(out::add);
            }
            assertEquals(
                    expected,
                    out.size(),
                    () -> "read_committed verifier on " + topic + " saw extra records: " + describe(out));
            return out;
        }
    }

    /**
     * The read_uncommitted counter: drains {@code topic} from the beginning until two quiet
     * seconds pass (bounded at 30 s) and returns the total record count — committed plus aborted.
     * Comparing it with the committed count proves aborted duplicates exist in the log.
     */
    static int countReadUncommitted(String topic) {
        return drainCount(topic, "read_uncommitted");
    }

    /** Drain-and-count under an explicit isolation level. */
    static int drainCount(String topic, String isolation) {
        try (Consumer<byte[], byte[]> consumer = newAssignedConsumer(topic, isolation)) {
            int count = 0;
            long quietNanos = Duration.ofSeconds(2).toNanos();
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            long lastNew = System.nanoTime();
            while (System.nanoTime() < deadline && System.nanoTime() - lastNew < quietNanos) {
                int polled = consumer.poll(Duration.ofMillis(200)).count();
                if (polled > 0) {
                    count += polled;
                    lastNew = System.nanoTime();
                }
            }
            return count;
        }
    }

    /**
     * Asserts that a read_committed consumer observes <em>no</em> records on {@code topic} within
     * the bounded observation window — the negative assertion for dangling-transaction gating.
     */
    static void assertNoCommittedRecords(String topic, Duration window) {
        try (Consumer<byte[], byte[]> consumer = newAssignedConsumer(topic, "read_committed")) {
            long deadline = System.nanoTime() + window.toNanos();
            while (System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(200));
                assertTrue(
                        records.isEmpty(),
                        () -> "expected no read_committed records on " + topic + " but saw " + records.count());
            }
        }
    }

    private static Consumer<byte[], byte[]> newAssignedConsumer(String topic, String isolation) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> assignment = consumer.partitionsFor(topic).stream()
                .map(info -> new TopicPartition(topic, info.partition()))
                .toList();
        consumer.assign(assignment);
        consumer.seekToBeginning(assignment);
        return consumer;
    }

    /** Indexes verifier output by record key, asserting every key appears exactly once. */
    static Map<String, ConsumerRecord<byte[], byte[]>> byKey(List<ConsumerRecord<byte[], byte[]>> records) {
        Map<String, ConsumerRecord<byte[], byte[]>> out = new LinkedHashMap<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            String key = record.key() == null ? "<null>" : utf8(record.key());
            ConsumerRecord<byte[], byte[]> previous = out.put(key, record);
            if (previous != null) {
                throw new AssertionError("key '" + key + "' delivered more than once: " + describe(records));
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ admin-side observations

    /** Sum of all partitions' log-end offsets (READ_UNCOMMITTED): 0 means a truly empty topic. */
    static long logEndOffsetTotal(String topic) {
        TopicDescription description =
                get(ADMIN.describeTopics(List.of(topic)).allTopicNames()).get(topic);
        Map<TopicPartition, OffsetSpec> query = new HashMap<>();
        for (var partition : description.partitions()) {
            query.put(new TopicPartition(topic, partition.partition()), OffsetSpec.latest());
        }
        var offsets = get(ADMIN.listOffsets(query).all());
        return offsets.values().stream().mapToLong(info -> info.offset()).sum();
    }

    /** The group's committed offsets and metadata, as the broker reports them. */
    static Map<TopicPartition, OffsetAndMetadata> committedOffsets(String groupId) {
        return get(ADMIN.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata());
    }

    /** Awaits the group's committed offset for {@code tp} reaching {@code expected}. */
    static OffsetAndMetadata awaitCommittedOffset(String groupId, TopicPartition tp, long expected) {
        await("committed offset " + expected + " for " + groupId + " on " + tp)
                .atMost(WAIT)
                .untilAsserted(() -> {
                    OffsetAndMetadata committed = committedOffsets(groupId).get(tp);
                    assertNotNull(committed, "no committed offset yet for " + tp);
                    assertEquals(expected, committed.offset());
                });
        return committedOffsets(groupId).get(tp);
    }

    static <T> T get(KafkaFuture<T> future) {
        try {
            return future.get(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("admin operation failed", e);
        }
    }

    private static String describe(List<ConsumerRecord<byte[], byte[]>> records) {
        StringBuilder sb = new StringBuilder();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            sb.append("\n  ")
                    .append(record.topic())
                    .append('-')
                    .append(record.partition())
                    .append('@')
                    .append(record.offset())
                    .append(" key=")
                    .append(record.key() == null ? "null" : utf8(record.key()));
        }
        return sb.toString();
    }
}
