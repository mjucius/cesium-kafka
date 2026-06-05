package events.cesium.kafka.core.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.core.config.CesiumConfig;
import events.cesium.kafka.core.config.IngestConfig;
import events.cesium.kafka.core.config.InstanceId;
import events.cesium.kafka.core.config.KafkaConfig;
import events.cesium.kafka.core.config.RouteConfig;
import events.cesium.kafka.core.config.TopicRef;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Design §8: locked keys, tuned defaults, overlay precedence, and the §3.3 naming schemes. */
class KafkaClientFactoryTest {

    private static CesiumConfig config(InstanceId instanceId, KafkaConfig kafka, IngestConfig ingest) {
        RouteConfig route =
                new RouteConfig(new TopicRef("orders"), new TopicRef("orders-out"), null, Optional.empty(), null);
        return new CesiumConfig(
                "orders-delay", instanceId, null, kafka, route, null, null, null, ingest, null, null, null);
    }

    private static CesiumConfig defaults() {
        return config(new InstanceId("slot-0"), null, null);
    }

    @Test
    void namingSchemesFollowDesign() {
        KafkaClientFactory factory = new KafkaClientFactory(defaults());
        assertEquals("cesium.orders-delay.ingest", factory.ingestGroupId());
        assertEquals(Optional.of("cesium.orders-delay.ingest.slot-0"), factory.ingestGroupInstanceId());
        assertEquals("cesium.orders-delay.ingest.slot-0.0", factory.ingestTransactionalId(0));
        assertEquals("cesium.orders-delay.ingest.slot-0.3", factory.ingestTransactionalId(3));
    }

    @Test
    void consumerLockedKeysAlwaysWin() {
        // The validator rejects these; the factory re-asserts them defensively anyway (§8).
        KafkaConfig kafka = new KafkaConfig(
                null,
                Map.of("isolation.level", "read_uncommitted", "enable.auto.commit", "true"),
                null,
                new KafkaConfig.ClientOverrides(Map.of(
                        "auto.offset.reset", "earliest",
                        "group.id", "evil",
                        "key.deserializer", "com.example.Evil")),
                null,
                null,
                null,
                null,
                null);
        Properties props =
                new KafkaClientFactory(config(new InstanceId("slot-0"), kafka, null)).ingestConsumerProperties();
        assertEquals("read_committed", props.getProperty("isolation.level"));
        assertEquals("none", props.getProperty("auto.offset.reset"));
        assertEquals("false", props.getProperty("enable.auto.commit"));
        assertEquals("cesium.orders-delay.ingest", props.getProperty("group.id"));
        assertEquals("cesium.orders-delay.ingest.slot-0", props.getProperty("group.instance.id"));
        assertEquals(
                "org.apache.kafka.common.serialization.ByteArrayDeserializer", props.getProperty("key.deserializer"));
        assertEquals(
                "org.apache.kafka.common.serialization.ByteArrayDeserializer", props.getProperty("value.deserializer"));
    }

    @Test
    void consumerTunedDefaultsComeFromConfigAndYieldToOverlays() {
        IngestConfig ingest = new IngestConfig(null, 1234);
        Properties defaults =
                new KafkaClientFactory(config(new InstanceId("slot-0"), null, ingest)).ingestConsumerProperties();
        assertEquals("1234", defaults.getProperty("max.poll.records"));
        assertEquals(
                KafkaClientFactory.INGEST_MAX_PARTITION_FETCH_BYTES, defaults.getProperty("max.partition.fetch.bytes"));

        KafkaConfig overlay = new KafkaConfig(
                null,
                null,
                null,
                new KafkaConfig.ClientOverrides(Map.of("max.partition.fetch.bytes", "999")),
                null,
                null,
                null,
                null,
                null);
        Properties overlaid =
                new KafkaClientFactory(config(new InstanceId("slot-0"), overlay, ingest)).ingestConsumerProperties();
        assertEquals("999", overlaid.getProperty("max.partition.fetch.bytes"), "tuned defaults are overridable");
    }

    @Test
    void commonPropertiesFlowThroughAndOverlayWinsOverCommon() {
        KafkaConfig kafka = new KafkaConfig(
                null,
                Map.of("bootstrap.servers", "common:9092", "request.timeout.ms", "11000"),
                null,
                new KafkaConfig.ClientOverrides(Map.of("request.timeout.ms", "22000")),
                null,
                null,
                new KafkaConfig.ClientOverrides(Map.of("bootstrap.servers", "producer:9092")),
                null,
                null);
        KafkaClientFactory factory = new KafkaClientFactory(config(new InstanceId("slot-0"), kafka, null));
        Properties consumer = factory.ingestConsumerProperties();
        assertEquals("common:9092", consumer.getProperty("bootstrap.servers"));
        assertEquals("22000", consumer.getProperty("request.timeout.ms"), "overlay wins over common");
        Properties producer = factory.ingestProducerProperties(0);
        assertEquals("producer:9092", producer.getProperty("bootstrap.servers"));
        assertEquals("11000", producer.getProperty("request.timeout.ms"), "common reaches every client");
    }

    @Test
    void producerLockedKeysAndTunedDefaults() {
        KafkaConfig kafka = new KafkaConfig(
                null,
                null,
                new KafkaConfig.Transactions(Duration.ofSeconds(45), null),
                null,
                null,
                null,
                new KafkaConfig.ClientOverrides(Map.of(
                        "enable.idempotence", "false",
                        "transactional.id", "evil",
                        "transaction.timeout.ms", "1",
                        "linger.ms", "5")),
                null,
                null);
        Properties props =
                new KafkaClientFactory(config(new InstanceId("slot-0"), kafka, null)).ingestProducerProperties(2);
        // Locked keys win over the overlay.
        assertEquals("true", props.getProperty("enable.idempotence"));
        assertEquals("cesium.orders-delay.ingest.slot-0.2", props.getProperty("transactional.id"));
        assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", props.getProperty("key.serializer"));
        assertEquals(
                "org.apache.kafka.common.serialization.ByteArraySerializer", props.getProperty("value.serializer"));
        // The typed kafka.transactions.timeout outranks a raw passthrough value (D9).
        assertEquals("45000", props.getProperty("transaction.timeout.ms"));
        // Tuned defaults yield to the overlay.
        assertEquals("5", props.getProperty("linger.ms"));
        assertEquals(KafkaClientFactory.PRODUCER_BATCH_SIZE, props.getProperty("batch.size"));
        assertEquals(KafkaClientFactory.PRODUCER_COMPRESSION, props.getProperty("compression.type"));
        assertEquals(KafkaClientFactory.PRODUCER_BUFFER_MEMORY, props.getProperty("buffer.memory"));
    }

    @Test
    void randomInstanceIdDropsStaticMembershipAndStaysUniquePerProcess() {
        CesiumConfig random = config(new InstanceId(InstanceId.RANDOM_LITERAL), null, null);
        KafkaClientFactory factory = new KafkaClientFactory(random);
        assertEquals(Optional.empty(), factory.ingestGroupInstanceId(), "D21: no static membership for random ids");
        assertNull(factory.ingestConsumerProperties().getProperty("group.instance.id"));
        String txnId = factory.ingestTransactionalId(0);
        assertTrue(txnId.startsWith("cesium.orders-delay.ingest.random-"), txnId);
        assertNotEquals(
                txnId,
                new KafkaClientFactory(random).ingestTransactionalId(0),
                "each process draws its own random token (D10)");
    }

    @Test
    void randomInstanceIdRemovesLeakedGroupInstanceIdFromOverlays() {
        KafkaConfig kafka =
                new KafkaConfig(null, Map.of("group.instance.id", "leaked"), null, null, null, null, null, null, null);
        Properties props = new KafkaClientFactory(config(new InstanceId(InstanceId.RANDOM_LITERAL), kafka, null))
                .ingestConsumerProperties();
        assertNull(props.getProperty("group.instance.id"));
    }

    @Test
    void groupProtocolConsumerIsApplied() {
        KafkaConfig kafka =
                new KafkaConfig(KafkaConfig.GroupProtocol.CONSUMER, null, null, null, null, null, null, null, null);
        Properties props =
                new KafkaClientFactory(config(new InstanceId("slot-0"), kafka, null)).ingestConsumerProperties();
        assertEquals("consumer", props.getProperty("group.protocol"));
        assertFalse(
                new KafkaClientFactory(defaults()).ingestConsumerProperties().containsKey("group.protocol"),
                "classic default leaves the client default untouched");
    }

    @Test
    void groupProtocolTypedKeyOutranksPassthroughOverlays() {
        // A raw group.protocol in a passthrough map must never flip the real protocol while the
        // typed kafka.group-protocol reports otherwise (mirrors the transactions.timeout rule):
        // the engine's rebalance semantics and the CI matrix key off the typed value.
        KafkaConfig classicWithOverlay = new KafkaConfig(
                null,
                Map.of("group.protocol", "consumer"),
                null,
                new KafkaConfig.ClientOverrides(Map.of("group.protocol", "consumer")),
                null,
                null,
                null,
                null,
                null);
        Properties classic = new KafkaClientFactory(config(new InstanceId("slot-0"), classicWithOverlay, null))
                .ingestConsumerProperties();
        assertNull(classic.getProperty("group.protocol"), "typed CLASSIC removes an overlaid group.protocol");

        KafkaConfig consumerWithOverlay = new KafkaConfig(
                KafkaConfig.GroupProtocol.CONSUMER,
                Map.of("group.protocol", "classic"),
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        Properties consumer = new KafkaClientFactory(config(new InstanceId("slot-0"), consumerWithOverlay, null))
                .ingestConsumerProperties();
        assertEquals("consumer", consumer.getProperty("group.protocol"), "typed CONSUMER outranks the overlay");
    }
}
