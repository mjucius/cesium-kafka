package events.cesium.kafka.core.config;

import java.util.Map;
import java.util.Optional;

/**
 * Builders for validator tests. NullAway is disabled for test sources, so absent components are
 * passed as {@code null} and the compact constructors materialize the §8 defaults — exactly what
 * a reflective binding layer does.
 */
final class TestConfigs {

    /** A heap context generous enough that default configs pass the budget check. */
    static final ValidationContext ROOMY_CONTEXT = new ValidationContext(4L * 1024 * 1024 * 1024, 1);

    private TestConfigs() {}

    /** A minimal valid config: required fields set, DLQ present (default policies route to it). */
    static CesiumConfig valid() {
        return new CesiumConfig(
                "orders-delay",
                new InstanceId("slot-0"),
                null,
                null,
                route(),
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    static RouteConfig route() {
        return new RouteConfig(
                new TopicRef("orders"),
                new TopicRef("orders-out"),
                null,
                Optional.of(new TopicRef("orders-dlq")),
                null);
    }

    /** Valid config with the given common {@code kafka.properties} map. */
    static CesiumConfig withKafkaProperties(Map<String, String> properties) {
        return withKafka(kafka(properties, null));
    }

    /** Valid config with the given overlay properties on one client ({@code position} 0-5). */
    static CesiumConfig withClientOverlay(int position, Map<String, String> properties) {
        KafkaConfig.ClientOverrides overlay = new KafkaConfig.ClientOverrides(properties);
        KafkaConfig.ClientOverrides[] overlays = new KafkaConfig.ClientOverrides[6];
        overlays[position] = overlay;
        return withKafka(new KafkaConfig(
                null, null, null, overlays[0], overlays[1], overlays[2], overlays[3], overlays[4], overlays[5]));
    }

    static KafkaConfig kafka(Map<String, String> properties, KafkaConfig.ClientOverrides trackerConsumer) {
        return new KafkaConfig(null, properties, null, null, trackerConsumer, null, null, null, null);
    }

    static CesiumConfig withKafka(KafkaConfig kafka) {
        return new CesiumConfig(
                "orders-delay",
                new InstanceId("slot-0"),
                null,
                kafka,
                route(),
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    static CesiumConfig withDispatch(DispatchConfig dispatch) {
        return withDispatch(dispatch, null);
    }

    static CesiumConfig withDispatch(DispatchConfig dispatch, StartupChecks startupChecks) {
        return new CesiumConfig(
                "orders-delay",
                new InstanceId("slot-0"),
                null,
                null,
                route(),
                null,
                null,
                null,
                null,
                dispatch,
                null,
                startupChecks);
    }
}
