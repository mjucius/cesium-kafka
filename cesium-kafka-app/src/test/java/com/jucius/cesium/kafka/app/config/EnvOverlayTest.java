package com.jucius.cesium.kafka.app.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.ConfigValidationException;
import com.jucius.cesium.kafka.core.config.Role;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Environment and system-property overlays: the {@code __} path grammar, precedence
 * {@code YAML < env < -D}, and unknown-env-key rejection (design §8, §11.1).
 */
class EnvOverlayTest {

    @TempDir
    Path dir;

    @Test
    void envOverridesYaml() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML + "delay:\n  max: PT1H\n");
        CesiumConfig config = LoaderTestSupport.loader(
                        Map.of("CESIUM_ROUTE__SOURCE__TOPIC", "env-topic", "CESIUM_DELAY__MAX", "PT2H"))
                .load(file)
                .config();
        assertEquals("env-topic", config.route().source().topic());
        assertEquals(Duration.ofHours(2), config.delay().max());
    }

    @Test
    void systemPropertiesOverrideEnv() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        Properties properties = new Properties();
        properties.setProperty("cesium.route.source.topic", "prop-topic");
        properties.setProperty("cesium.delay.on-over-max", "CLAMP");
        CesiumConfig config = LoaderTestSupport.loader(Map.of("CESIUM_ROUTE__SOURCE__TOPIC", "env-topic"), properties)
                .load(file)
                .config();
        assertEquals("prop-topic", config.route().source().topic());
        assertEquals(
                com.jucius.cesium.kafka.core.policy.OverMaxPolicy.CLAMP,
                config.delay().onOverMax());
    }

    @Test
    void multiWordSegmentsMapUnderscoreToKebab() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        CesiumConfig config = LoaderTestSupport.loader(Map.of(
                        "CESIUM_DISPATCH__MAX_PENDING_PER_PARTITION", "500000",
                        "CESIUM_DISPATCH__BATCH__MAX_ENTRIES", "1234",
                        "CESIUM_STARTUP_CHECKS__MAX_TOLERATED_OUTAGE", "P2D"))
                .load(file)
                .config();
        assertEquals(500_000L, config.dispatch().maxPendingPerPartition());
        assertEquals(1234, config.dispatch().batch().maxEntries());
        assertEquals(Duration.ofDays(2), config.startupChecks().maxToleratedOutage());
    }

    @Test
    void mapSubtreeSegmentsMapUnderscoreToDots() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        CesiumConfig config = LoaderTestSupport.loader(Map.of(
                        "CESIUM_KAFKA__PROPERTIES__MAX_POLL_INTERVAL_MS", "300000",
                        "CESIUM_KAFKA__TRACKER_CONSUMER__PROPERTIES__MAX_POLL_RECORDS", "9000"))
                .load(file)
                .config();
        assertEquals("300000", config.kafka().properties().get("max.poll.interval.ms"));
        assertEquals("9000", config.kafka().trackerConsumer().properties().get("max.poll.records"));
    }

    @Test
    void collectionLeavesSplitOnCommas() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        assertEquals(
                Set.of(Role.INGEST),
                LoaderTestSupport.loader(Map.of("CESIUM_ROLES", "ingest"))
                        .load(file)
                        .config()
                        .roles());
        assertEquals(
                Set.of(Role.INGEST, Role.DISPATCH),
                LoaderTestSupport.loader(Map.of("CESIUM_ROLES", "ingest,dispatch"))
                        .load(file)
                        .config()
                        .roles());
    }

    @Test
    void instanceIdIsSettableViaEnv() {
        // D10/D21: the instance id is exactly the per-deployment-slot value (e.g. a StatefulSet
        // ordinal) operators inject via env — the scalar-bound record must resolve as a leaf.
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        CesiumConfig config = LoaderTestSupport.loader(Map.of("CESIUM_INSTANCE_ID", "blue-1"))
                .load(file)
                .config();
        assertEquals("blue-1", config.instanceId().value());
    }

    @Test
    void instanceIdIsSettableViaSystemProperty() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        Properties properties = new Properties();
        properties.setProperty("cesium.instance-id", "slot-9");
        CesiumConfig config =
                LoaderTestSupport.loader(Map.of(), properties).load(file).config();
        assertEquals("slot-9", config.instanceId().value());
    }

    @Test
    void instanceIdValueSubpathIsRejectedAsUnknown() {
        // The record component is not addressable below the scalar leaf: CESIUM_INSTANCE_ID__VALUE
        // would write {instance-id: {value: ...}}, which the scalar deserializer cannot bind.
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(
                                Map.of("CESIUM_INSTANCE_ID__VALUE", "blue-1"))
                        .load(file));
        assertTrue(exception.getMessage().contains("CESIUM_INSTANCE_ID__VALUE"), exception::getMessage);
    }

    @Test
    void unknownEnvKeyUnderThePrefixFails() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        ConfigValidationException exception = assertThrows(
                ConfigValidationException.class, () -> LoaderTestSupport.loader(Map.of("CESIUM_DELAY__MAXIMUM", "PT1H"))
                        .load(file));
        assertTrue(exception.getMessage().contains("CESIUM_DELAY__MAXIMUM"), exception::getMessage);
        assertTrue(exception.getMessage().contains("delay.maximum"), exception::getMessage);
    }

    @Test
    void unknownSystemPropertyUnderThePrefixFails() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        Properties properties = new Properties();
        properties.setProperty("cesium.delay.maximum", "PT1H");
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(Map.of(), properties)
                        .load(file));
        assertTrue(exception.getMessage().contains("cesium.delay.maximum"), exception::getMessage);
    }

    @Test
    void reservedConfigPathEnvIsNotTreatedAsAnOverride() {
        // CESIUM_CONFIG names the config FILE (the --config fallback); it shares the CESIUM_ prefix
        // but is not a config-override path. The container sets it by default, so the loader must skip
        // it rather than reject 'config' as an unknown key (regression: the docker quickstart).
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        CesiumConfig config = LoaderTestSupport.loader(
                        Map.of(CesiumConfigLoader.CONFIG_PATH_ENV, file.toString(), "CESIUM_ROLES", "ingest"))
                .load(file)
                .config();
        assertEquals("orders", config.route().source().topic());
        assertEquals(Set.of(Role.INGEST), config.roles());
    }

    @Test
    void envVarsOutsideThePrefixAreIgnored() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        CesiumConfig config = LoaderTestSupport.loader(Map.of("PATH", "/usr/bin", "CESIUM", "not-prefixed"))
                .load(file)
                .config();
        assertEquals("orders", config.route().source().topic());
    }

    @Test
    void pathIntoAScalarLeafIsUnknown() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        ConfigValidationException exception = assertThrows(
                ConfigValidationException.class, () -> LoaderTestSupport.loader(Map.of("CESIUM_DELAY__MAX__EXTRA", "1"))
                        .load(file));
        assertTrue(exception.getMessage().contains("CESIUM_DELAY__MAX__EXTRA"), exception::getMessage);
    }

    @Test
    void pathEndingOnARecordNodeIsUnknown() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        ConfigValidationException exception = assertThrows(
                ConfigValidationException.class,
                () -> LoaderTestSupport.loader(Map.of("CESIUM_DELAY", "PT1H")).load(file));
        assertTrue(exception.getMessage().contains("CESIUM_DELAY"), exception::getMessage);
    }

    @Test
    void envOverlayValuesStillFlowThroughValidation() {
        // An env override that breaks a cross-field rule must be caught by the validator.
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(
                                Map.of("CESIUM_KAFKA__PROPERTIES__MAX_POLL_INTERVAL_MS", "60000"))
                        .load(file));
        assertTrue(exception.getMessage().contains("dispatch.drain.max-slice"), exception::getMessage);
    }

    @Test
    void lockedKafkaKeyInjectedViaEnvIsRejectedWithTheExplanation() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        ConfigValidationException exception =
                assertThrows(ConfigValidationException.class, () -> LoaderTestSupport.loader(
                                Map.of("CESIUM_KAFKA__PROPERTIES__ISOLATION_LEVEL", "read_uncommitted"))
                        .load(file));
        assertTrue(exception.getMessage().contains("KIP-447"), exception::getMessage);
        assertTrue(exception.getMessage().contains("require_stable"), exception::getMessage);
    }
}
