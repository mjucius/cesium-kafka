package com.jucius.cesium.kafka.core.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.IngestConfig;
import com.jucius.cesium.kafka.core.config.InstanceId;
import com.jucius.cesium.kafka.core.config.KafkaConfig;
import com.jucius.cesium.kafka.core.config.RouteConfig;
import com.jucius.cesium.kafka.core.config.TopicRef;
import java.time.Duration;
import java.util.List;
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
    void dispatchNamingSchemesFollowDesign() {
        KafkaClientFactory factory = new KafkaClientFactory(defaults());
        assertEquals("cesium.orders-delay.dispatch", factory.dispatchGroupId());
        assertEquals(Optional.of("cesium.orders-delay.dispatch.slot-0"), factory.dispatchGroupInstanceId());
        assertEquals("cesium.orders-delay.dispatch.slot-0.0", factory.dispatchTransactionalId(0));
        assertEquals("cesium.orders-delay.dispatch.slot-0.3", factory.dispatchTransactionalId(3));
    }

    @Test
    void trackerConsumerLockedKeysAlwaysWin() {
        KafkaConfig kafka = new KafkaConfig(
                null,
                Map.of("isolation.level", "read_uncommitted", "enable.auto.commit", "true"),
                null,
                null,
                new KafkaConfig.ClientOverrides(Map.of(
                        "auto.offset.reset", "earliest",
                        "group.id", "evil",
                        "group.instance.id", "evil-instance")),
                null,
                null,
                null,
                null);
        Properties props =
                new KafkaClientFactory(config(new InstanceId("slot-0"), kafka, null)).trackerConsumerProperties();
        assertEquals("read_committed", props.getProperty("isolation.level"), "D17: locked on every cesium consumer");
        assertEquals("none", props.getProperty("auto.offset.reset"), "D18: resets are explicit operator decisions");
        assertEquals("false", props.getProperty("enable.auto.commit"));
        assertEquals("cesium.orders-delay.dispatch", props.getProperty("group.id"));
        assertEquals("cesium.orders-delay.dispatch.slot-0", props.getProperty("group.instance.id"), "D21");
        assertEquals(
                "org.apache.kafka.common.serialization.ByteArrayDeserializer", props.getProperty("key.deserializer"));
        assertEquals(
                "org.apache.kafka.common.serialization.ByteArrayDeserializer", props.getProperty("value.deserializer"));
    }

    @Test
    void trackerConsumerTunedDefaultsAndAssignor() {
        Properties props = new KafkaClientFactory(defaults()).trackerConsumerProperties();
        assertEquals(KafkaClientFactory.TRACKER_MAX_POLL_RECORDS, props.getProperty("max.poll.records"));
        assertEquals(
                KafkaClientFactory.TRACKER_ASSIGNOR,
                props.getProperty("partition.assignment.strategy"),
                "classic protocol defaults to CooperativeStickyAssignor (§3.4.5)");

        KafkaConfig kip848 =
                new KafkaConfig(KafkaConfig.GroupProtocol.CONSUMER, null, null, null, null, null, null, null, null);
        Properties consumerProtocol =
                new KafkaClientFactory(config(new InstanceId("slot-0"), kip848, null)).trackerConsumerProperties();
        assertEquals("consumer", consumerProtocol.getProperty("group.protocol"));
        assertNull(
                consumerProtocol.getProperty("partition.assignment.strategy"),
                "KIP-848 assignors are broker-side; a classic assignor list is rejected by the client");
    }

    @Test
    void trackerConsumerRandomInstanceIdDropsStaticMembership() {
        Properties props = new KafkaClientFactory(config(new InstanceId(InstanceId.RANDOM_LITERAL), null, null))
                .trackerConsumerProperties();
        assertNull(props.getProperty("group.instance.id"), "D21: no static membership for random ids");
    }

    @Test
    void dispatchProducerLockedKeysAndTunedDefaults() {
        KafkaConfig kafka = new KafkaConfig(
                null,
                null,
                new KafkaConfig.Transactions(Duration.ofSeconds(45), null),
                null,
                null,
                null,
                null,
                new KafkaConfig.ClientOverrides(Map.of(
                        "enable.idempotence", "false",
                        "transactional.id", "evil",
                        "transaction.timeout.ms", "1",
                        "linger.ms", "5")),
                null);
        Properties props =
                new KafkaClientFactory(config(new InstanceId("slot-0"), kafka, null)).dispatchProducerProperties(2);
        assertEquals("true", props.getProperty("enable.idempotence"));
        assertEquals("cesium.orders-delay.dispatch.slot-0.2", props.getProperty("transactional.id"));
        assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", props.getProperty("key.serializer"));
        assertEquals(
                "org.apache.kafka.common.serialization.ByteArraySerializer", props.getProperty("value.serializer"));
        assertEquals("45000", props.getProperty("transaction.timeout.ms"), "typed kafka.transactions.timeout (D9)");
        assertEquals("5", props.getProperty("linger.ms"), "tuned defaults yield to the overlay");
        assertEquals(KafkaClientFactory.PRODUCER_BATCH_SIZE, props.getProperty("batch.size"));
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
    void groupProtocolConsumerStripsClassicOnlyConsumerKeys() {
        // KIP-848: the assignor and the session/heartbeat timers are broker-side; kafka-clients 4.x
        // throws ConfigException if they reach a group.protocol=consumer client. The engine strips a
        // passthrough (or the §8 tuning / K8s static-membership window) that legitimately sets them
        // under the classic protocol, so the consumer-protocol lane builds valid clients.
        KafkaConfig kip848 = new KafkaConfig(
                KafkaConfig.GroupProtocol.CONSUMER,
                Map.of(
                        "session.timeout.ms", "6000",
                        "heartbeat.interval.ms", "1500",
                        "partition.assignment.strategy", "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"),
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        KafkaClientFactory factory = new KafkaClientFactory(config(new InstanceId("slot-0"), kip848, null));
        for (Properties props : List.of(factory.ingestConsumerProperties(), factory.trackerConsumerProperties())) {
            assertEquals("consumer", props.getProperty("group.protocol"));
            assertNull(props.getProperty("session.timeout.ms"), "session.timeout.ms is broker-side under KIP-848");
            assertNull(
                    props.getProperty("heartbeat.interval.ms"), "heartbeat.interval.ms is broker-side under KIP-848");
            assertNull(props.getProperty("partition.assignment.strategy"), "assignors are broker-side under KIP-848");
        }

        // The classic protocol keeps them (the operator's tuning / K8s static-membership window).
        KafkaConfig classic = new KafkaConfig(
                null,
                Map.of("session.timeout.ms", "6000", "heartbeat.interval.ms", "1500"),
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        Properties classicProps =
                new KafkaClientFactory(config(new InstanceId("slot-0"), classic, null)).trackerConsumerProperties();
        assertEquals("6000", classicProps.getProperty("session.timeout.ms"));
        assertEquals("1500", classicProps.getProperty("heartbeat.interval.ms"));
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
