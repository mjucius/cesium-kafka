package com.jucius.cesium.kafka.core.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.core.headers.RelayPartitioning;
import com.jucius.cesium.kafka.core.headers.RelayTimestampPolicy;
import com.jucius.cesium.kafka.core.policy.MalformedHeaderPolicy;
import com.jucius.cesium.kafka.core.policy.OverMaxPolicy;
import com.jucius.cesium.kafka.core.policy.UnfetchablePayloadPolicy;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Asserts the §8 defaults table verbatim: a config constructed with every optional component
 * absent (null, as a reflective binder would pass) materializes exactly the documented defaults.
 */
class ConfigDefaultsTest {

    private static CesiumConfig allDefaults() {
        return new CesiumConfig(
                "app", new InstanceId("slot-0"), null, null, null, null, null, null, null, null, null, null);
    }

    @Test
    void rolesDefaultToBothLoops() {
        assertEquals(Set.of(Role.INGEST, Role.DISPATCH), allDefaults().roles());
    }

    @Test
    void rolesAreFrozen() {
        CesiumConfig config = new CesiumConfig(
                "app",
                new InstanceId("slot-0"),
                EnumSet.of(Role.INGEST),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        assertEquals(Set.of(Role.INGEST), config.roles());
        assertThrows(UnsupportedOperationException.class, () -> config.roles().add(Role.DISPATCH));
    }

    @Test
    void explicitlyEmptyRolesArePreservedForValidation() {
        CesiumConfig config = new CesiumConfig(
                "app", new InstanceId("slot-0"), Set.of(), null, null, null, null, null, null, null, null, null);
        assertTrue(config.roles().isEmpty());
    }

    @Test
    void delayDefaults() {
        DelayConfig delay = allDefaults().delay();
        assertEquals(Duration.ofDays(1), delay.max()); // P1D, lowered from P7D (R10)
        assertEquals(OverMaxPolicy.DLQ, delay.onOverMax());
        assertEquals(MalformedHeaderPolicy.DLQ, delay.onMalformedHeader());
    }

    @Test
    void routeDefaults() {
        RouteConfig route = allDefaults().route();
        assertFalse(route.source().isConfigured());
        assertFalse(route.destination().isConfigured());
        assertEquals(TrackerConfig.Bootstrap.CREATE, route.tracker().bootstrap());
        assertEquals(Optional.empty(), route.tracker().aclPrincipal());
        assertEquals("cesium.app.tracker", route.tracker().resolvedTopic("app"));
        assertEquals(Optional.empty(), route.dlq());
        assertEquals(RelayTimestampPolicy.DISPATCH, route.relay().timestamp());
        assertEquals(RelayPartitioning.BY_KEY, route.relay().partitioning());
    }

    @Test
    void trackerTopicOverrideWins() {
        TrackerConfig tracker = new TrackerConfig("my-tracker", null, Optional.empty());
        assertEquals("my-tracker", tracker.resolvedTopic("app"));
    }

    @Test
    void headersDefaults() {
        HeadersConfig headers = allDefaults().headers();
        assertTrue(headers.stampProvenance());
        assertFalse(headers.acceptBinaryLongValues());
    }

    @Test
    void storeDefaults() {
        StoreConfig store = allDefaults().store();
        assertEquals("kafka-tracker", store.type());
        assertEquals(Map.of(), store.properties());
    }

    @Test
    void ingestDefaults() {
        IngestConfig ingest = allDefaults().ingest();
        assertEquals(1, ingest.workers());
        assertEquals(2000, ingest.maxBatch()); // = max.poll.records
    }

    @Test
    void dispatchDefaults() {
        DispatchConfig dispatch = allDefaults().dispatch();
        assertEquals(1, dispatch.workers());
        assertEquals(10_000, dispatch.batch().maxEntries()); // D8
        assertEquals(32L * 1024 * 1024, dispatch.batch().maxBytes()); // 32 MiB
        assertEquals(Duration.parse("PT1M"), dispatch.drain().maxSlice());
        assertEquals(Duration.ZERO, dispatch.coalesce()); // never early, never deliberately late
        assertEquals(Duration.parse("PT30S"), dispatch.idleCursorInterval());
        assertEquals(3072, dispatch.cursor().sidecarMaxBytes());
        assertEquals(Duration.parse("PT30S"), dispatch.fetch().timeout());
        assertEquals(Duration.parse("PT2S"), dispatch.fetch().partitionTimeFloor());
        assertEquals(Duration.parse("PT0.05S"), dispatch.fetch().penalty().backoff());
        assertEquals(Duration.parse("PT10S"), dispatch.fetch().penalty().backoffMax());
        assertEquals(UnfetchablePayloadPolicy.DLQ, dispatch.onUnfetchablePayload());
        assertEquals(2_000_000L, dispatch.maxPendingPerPartition());
        assertEquals(DispatchConfig.AUTO_MAX_PENDING_TOTAL, dispatch.maxPendingTotal());
    }

    @Test
    void autoMaxPendingTotalIsQuarterHeapOver64BytesPerEntry() {
        // Post-approval revision: 64 B/entry (not 48). 4 GiB heap -> 1 GiB budget -> 16M entries.
        DispatchConfig dispatch = allDefaults().dispatch();
        long fourGib = 4L * 1024 * 1024 * 1024;
        assertEquals(16_777_216L, dispatch.resolveMaxPendingTotal(fourGib));
    }

    @Test
    void explicitMaxPendingTotalIsReturnedVerbatim() {
        DispatchConfig dispatch = new DispatchConfig(null, null, null, null, null, null, null, null, null, 123_456L);
        assertEquals(123_456L, dispatch.resolveMaxPendingTotal(4L * 1024 * 1024 * 1024));
    }

    @Test
    void kafkaDefaults() {
        KafkaConfig kafka = allDefaults().kafka();
        assertEquals(KafkaConfig.GroupProtocol.CLASSIC, kafka.groupProtocol());
        assertEquals(Map.of(), kafka.properties());
        assertEquals(Duration.parse("PT30S"), kafka.transactions().timeout()); // D9
        assertEquals(5, kafka.transactions().commitRetry());
        assertEquals(Map.of(), kafka.trackerConsumer().properties());
        assertEquals(7, kafka.clientPropertyMaps().size()); // common + six overlays
    }

    @Test
    void observabilityDefaults() {
        assertEquals(8081, allDefaults().observability().port());
    }

    @Test
    void startupCheckDefaults() {
        StartupChecks checks = allDefaults().startupChecks();
        assertEquals(CheckMode.FAIL, checks.retention());
        assertEquals(StartupChecks.SizeBasedRetention.FAIL, checks.sizeBasedRetention());
        assertEquals(Duration.ofDays(7), checks.maxToleratedOutage()); // P7D
        assertEquals(CheckMode.FAIL, checks.outageCheck());
        assertEquals(CheckMode.FAIL, checks.heapBudget());
    }

    @Test
    void mapsAreImmutable() {
        CesiumConfig config = allDefaults();
        assertThrows(
                UnsupportedOperationException.class,
                () -> config.kafka().properties().put("x", "y"));
        assertThrows(
                UnsupportedOperationException.class,
                () -> config.store().properties().put("x", "y"));
    }
}
