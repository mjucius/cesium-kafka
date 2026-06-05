package events.cesium.kafka.app.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.core.config.CesiumConfig;
import events.cesium.kafka.core.config.DispatchConfig;
import events.cesium.kafka.core.config.KafkaConfig;
import events.cesium.kafka.core.config.Role;
import events.cesium.kafka.core.config.TrackerConfig;
import events.cesium.kafka.core.headers.RelayPartitioning;
import events.cesium.kafka.core.headers.RelayTimestampPolicy;
import events.cesium.kafka.core.policy.MalformedHeaderPolicy;
import events.cesium.kafka.core.policy.OverMaxPolicy;
import events.cesium.kafka.core.policy.UnfetchablePayloadPolicy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** YAML → records binding happy path (design §8, §11.1). */
class YamlBindingTest {

    @TempDir
    Path dir;

    @Test
    void fullYamlBindsToRecords() {
        Path file = LoaderTestSupport.write(
                dir,
                """
                application-id: orders-delay
                instance-id: slot-3
                roles: [ingest]
                kafka:
                  group-protocol: classic
                  properties:
                    bootstrap.servers: localhost:9092
                    max.poll.interval.ms: 300000
                  tracker-consumer:
                    properties:
                      max.poll.records: 5000
                  transactions:
                    timeout: PT20S
                    commit-retry: 3
                route:
                  source:
                    topic: orders
                  destination:
                    topic: orders-out
                  dlq:
                    topic: orders-dlq
                  tracker:
                    bootstrap: FAIL
                    acl-principal: User:cesium
                  relay:
                    timestamp: SOURCE
                    partitioning: source_partition
                delay:
                  max: PT6H
                  on-over-max: CLAMP
                  on-malformed-header: relay_immediate
                headers:
                  stamp-provenance: false
                store:
                  type: kafka-tracker
                  properties:
                    hmac.key: secret
                ingest:
                  workers: 2
                  max-batch: 1000
                dispatch:
                  batch:
                    max-entries: 5000
                    max-bytes: 1048576
                  drain:
                    max-slice: PT30S
                  cursor:
                    sidecar-max-bytes: 2048
                  fetch:
                    timeout: PT10S
                    penalty:
                      backoff: PT0.1S
                  on-unfetchable-payload: DROP
                  max-pending-per-partition: 100000
                  max-pending-total: 500000
                observability:
                  port: 9090
                startup-checks:
                  retention: WARN
                  max-tolerated-outage: P3D
                """);

        CesiumConfig config = LoaderTestSupport.loader(Map.of()).load(file).config();

        assertEquals("orders-delay", config.applicationId());
        assertEquals("slot-3", config.instanceId().value());
        assertFalse(config.instanceId().isRandom());
        assertEquals(Set.of(Role.INGEST), config.roles());

        assertEquals(KafkaConfig.GroupProtocol.CLASSIC, config.kafka().groupProtocol());
        assertEquals("localhost:9092", config.kafka().properties().get("bootstrap.servers"));
        assertEquals("300000", config.kafka().properties().get("max.poll.interval.ms"));
        assertEquals("5000", config.kafka().trackerConsumer().properties().get("max.poll.records"));
        assertEquals(Duration.ofSeconds(20), config.kafka().transactions().timeout());
        assertEquals(3, config.kafka().transactions().commitRetry());

        assertEquals("orders", config.route().source().topic());
        assertEquals("orders-out", config.route().destination().topic());
        assertEquals(Optional.of("orders-dlq"), config.route().dlq().map(d -> d.topic()));
        assertEquals(TrackerConfig.Bootstrap.FAIL, config.route().tracker().bootstrap());
        assertEquals(Optional.of("User:cesium"), config.route().tracker().aclPrincipal());
        assertEquals(RelayTimestampPolicy.SOURCE, config.route().relay().timestamp());
        assertEquals(RelayPartitioning.SOURCE_PARTITION, config.route().relay().partitioning());

        assertEquals(Duration.ofHours(6), config.delay().max());
        assertEquals(OverMaxPolicy.CLAMP, config.delay().onOverMax());
        assertEquals(MalformedHeaderPolicy.RELAY_IMMEDIATE, config.delay().onMalformedHeader());

        assertFalse(config.headers().stampProvenance());
        assertEquals("secret", config.store().properties().get("hmac.key"));
        assertEquals(2, config.ingest().workers());
        assertEquals(1000, config.ingest().maxBatch());

        DispatchConfig dispatch = config.dispatch();
        assertEquals(5000, dispatch.batch().maxEntries());
        assertEquals(1_048_576L, dispatch.batch().maxBytes());
        assertEquals(Duration.ofSeconds(30), dispatch.drain().maxSlice());
        assertEquals(2048, dispatch.cursor().sidecarMaxBytes());
        assertEquals(Duration.ofSeconds(10), dispatch.fetch().timeout());
        assertEquals(Duration.ofMillis(100), dispatch.fetch().penalty().backoff());
        assertEquals(UnfetchablePayloadPolicy.DROP, dispatch.onUnfetchablePayload());
        assertEquals(100_000L, dispatch.maxPendingPerPartition());
        assertEquals(500_000L, dispatch.maxPendingTotal());

        assertEquals(9090, config.observability().port());
        assertEquals(Duration.ofDays(3), config.startupChecks().maxToleratedOutage());

        // Untouched keys keep their §8 defaults.
        assertEquals(Duration.ofSeconds(2), dispatch.fetch().partitionTimeFloor());
        assertEquals(Duration.ofSeconds(10), dispatch.fetch().penalty().backoffMax());
        assertEquals(Duration.ZERO, dispatch.coalesce());
        assertFalse(config.headers().acceptBinaryLongValues());
    }

    @Test
    void minimalYamlMaterializesAllDefaults() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        CesiumConfig config = LoaderTestSupport.loader(Map.of()).load(file).config();
        assertEquals(Set.of(Role.INGEST, Role.DISPATCH), config.roles());
        assertEquals(Duration.ofDays(1), config.delay().max());
        assertEquals(10_000, config.dispatch().batch().maxEntries());
        assertEquals(32L * 1024 * 1024, config.dispatch().batch().maxBytes());
        assertEquals(3072, config.dispatch().cursor().sidecarMaxBytes());
        assertEquals("kafka-tracker", config.store().type());
        assertEquals(8081, config.observability().port());
    }

    @Test
    void randomInstanceIdOptInBinds() {
        Path file = LoaderTestSupport.write(
                dir, LoaderTestSupport.MINIMAL_YAML.replace("instance-id: slot-0", "instance-id: random"));
        CesiumConfig config = LoaderTestSupport.loader(Map.of()).load(file).config();
        assertTrue(config.instanceId().isRandom());
    }
}
