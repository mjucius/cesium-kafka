package com.jucius.cesium.kafka.core.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.InstanceId;
import com.jucius.cesium.kafka.core.config.KafkaConfig;
import com.jucius.cesium.kafka.core.config.RouteConfig;
import com.jucius.cesium.kafka.core.config.TopicRef;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Design §7/§8: the group-less payload seek consumer — locked keys, group-key removal, and the
 * bulk-fetch tuned defaults. (Separate from {@link KafkaClientFactoryTest} because this client is
 * the only group-less one: its locked-key posture is removal, not derivation.)
 */
class SeekConsumerPropertiesTest {

    private static CesiumConfig config(KafkaConfig kafka) {
        RouteConfig route =
                new RouteConfig(new TopicRef("orders"), new TopicRef("orders-out"), null, Optional.empty(), null);
        return new CesiumConfig(
                "orders-delay", new InstanceId("slot-0"), null, kafka, route, null, null, null, null, null, null, null);
    }

    @Test
    void groupLessLockedKeysAlwaysWin() {
        // The validator rejects these; the factory re-asserts/removes them defensively anyway (§8).
        KafkaConfig kafka = new KafkaConfig(
                null,
                Map.of("isolation.level", "read_uncommitted", "group.id", "leaked-common"),
                null,
                null,
                null,
                new KafkaConfig.ClientOverrides(Map.of(
                        "group.id", "evil",
                        "group.instance.id", "evil-instance",
                        "auto.offset.reset", "latest",
                        "enable.auto.commit", "true",
                        "value.deserializer", "com.example.Evil")),
                null,
                null,
                null);
        Properties props = new KafkaClientFactory(config(kafka)).seekConsumerProperties();
        assertNull(props.getProperty("group.id"), "group-less by design (§7): removed, never derived");
        assertNull(props.getProperty("group.instance.id"), "a leaked static-membership id could fence a real member");
        assertEquals("read_committed", props.getProperty("isolation.level"), "D17: LSO-bounded, no aborted records");
        assertEquals("none", props.getProperty("auto.offset.reset"), "D18: out-of-range is the §7.3 expiry signal");
        assertEquals("false", props.getProperty("enable.auto.commit"));
        assertEquals(
                "org.apache.kafka.common.serialization.ByteArrayDeserializer", props.getProperty("key.deserializer"));
        assertEquals(
                "org.apache.kafka.common.serialization.ByteArrayDeserializer", props.getProperty("value.deserializer"));
    }

    @Test
    void seekTuningDefaultsApplyAndYieldToOverlays() {
        Properties defaults = new KafkaClientFactory(config(KafkaConfig.defaults())).seekConsumerProperties();
        assertEquals(KafkaClientFactory.SEEK_FETCH_MAX_BYTES, defaults.getProperty("fetch.max.bytes"));
        assertEquals(
                KafkaClientFactory.SEEK_MAX_PARTITION_FETCH_BYTES, defaults.getProperty("max.partition.fetch.bytes"));

        KafkaConfig overlaid = new KafkaConfig(
                null,
                Map.of("fetch.max.bytes", "1048576"),
                null,
                null,
                null,
                new KafkaConfig.ClientOverrides(Map.of("max.partition.fetch.bytes", "2097152")),
                null,
                null,
                null);
        Properties props = new KafkaClientFactory(config(overlaid)).seekConsumerProperties();
        assertEquals("1048576", props.getProperty("fetch.max.bytes"), "tuned defaults are overridable (§8)");
        assertEquals("2097152", props.getProperty("max.partition.fetch.bytes"), "overlay wins over common");
    }

    @Test
    void groupProtocolNeverReachesTheGroupLessConsumer() {
        // group semantics are meaningless without a group: neither the typed knob nor a raw
        // passthrough may select a consumer implementation by accident.
        KafkaConfig kafka = new KafkaConfig(
                KafkaConfig.GroupProtocol.CONSUMER,
                Map.of("group.protocol", "consumer"),
                null,
                null,
                null,
                new KafkaConfig.ClientOverrides(Map.of("group.protocol", "consumer")),
                null,
                null,
                null);
        Properties props = new KafkaClientFactory(config(kafka)).seekConsumerProperties();
        assertNull(props.getProperty("group.protocol"));
    }
}
