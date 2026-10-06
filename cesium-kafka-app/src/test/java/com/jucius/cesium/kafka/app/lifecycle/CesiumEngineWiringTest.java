package com.jucius.cesium.kafka.app.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.store.RouteDescriptor;
import com.jucius.cesium.kafka.api.store.TrackerBackedStore;
import com.jucius.cesium.kafka.app.health.ShardRecovery;
import com.jucius.cesium.kafka.app.lifecycle.CesiumEngine.LoopSpec;
import com.jucius.cesium.kafka.core.admin.IdentityBlob;
import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.DispatchConfig;
import com.jucius.cesium.kafka.core.config.IngestConfig;
import com.jucius.cesium.kafka.core.config.InstanceId;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.core.config.RouteConfig;
import com.jucius.cesium.kafka.core.config.StoreConfig;
import com.jucius.cesium.kafka.core.config.TopicRef;
import com.jucius.cesium.kafka.core.config.ValidationReport;
import com.jucius.cesium.kafka.store.tracker.KafkaTrackerStore;
import com.jucius.cesium.kafka.store.tracker.KafkaTrackerStoreProvider;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.Uuid;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The config-to-engine wiring decisions that do not need a broker (design §6, §10): role selection
 * and worker counts ({@link CesiumEngine#planLoops}), store resolution by {@code store.type}
 * ({@link CesiumEngine#resolveStore}), and the M5-obligation-#10 partition-aware heap re-validation
 * ({@link CesiumEngine#heapBudgetReport}).
 */
class CesiumEngineWiringTest {

    private static final long FOUR_GIB = 4L * 1024 * 1024 * 1024;
    private static final long SIXTY_FOUR_MIB = 64L * 1024 * 1024;

    // ------------------------------------------------------------------ role selection / workers

    @Test
    void ingestOnlyRoleStartsOnlyIngestWorkers() {
        List<LoopSpec> specs = CesiumEngine.planLoops(config(Set.of(Role.INGEST), 3, 5));
        assertEquals(
                List.of(new LoopSpec(Role.INGEST, 0), new LoopSpec(Role.INGEST, 1), new LoopSpec(Role.INGEST, 2)),
                specs,
                "ingest-only roles must start exactly ingest.workers ingest loops and no dispatch loop");
    }

    @Test
    void dispatchOnlyRoleStartsOnlyDispatchWorkers() {
        List<LoopSpec> specs = CesiumEngine.planLoops(config(Set.of(Role.DISPATCH), 4, 2));
        assertEquals(List.of(new LoopSpec(Role.DISPATCH, 0), new LoopSpec(Role.DISPATCH, 1)), specs);
    }

    @Test
    void bothRolesStartIngestThenDispatchWithDistinctOrdinals() {
        List<LoopSpec> specs = CesiumEngine.planLoops(config(Set.of(Role.INGEST, Role.DISPATCH), 2, 2));
        assertEquals(
                List.of(
                        new LoopSpec(Role.INGEST, 0),
                        new LoopSpec(Role.INGEST, 1),
                        new LoopSpec(Role.DISPATCH, 0),
                        new LoopSpec(Role.DISPATCH, 1)),
                specs);
    }

    @Test
    void defaultWorkerCountsAreOneEach() {
        List<LoopSpec> specs = CesiumEngine.planLoops(config(Set.of(Role.INGEST, Role.DISPATCH), 1, 1));
        assertEquals(2, specs.size());
        assertEquals(1, specs.stream().filter(s -> s.role() == Role.INGEST).count());
        assertEquals(1, specs.stream().filter(s -> s.role() == Role.DISPATCH).count());
    }

    // ------------------------------------------------------------------ store resolution by type

    @Test
    void resolvesTheKafkaTrackerStoreByTypeId() {
        TrackerBackedStore store = CesiumEngine.resolveStore(KafkaTrackerStoreProvider.TYPE_ID);
        assertInstanceOf(TrackerBackedStore.class, store);
    }

    @Test
    void unknownStoreTypeFailsFast() {
        EngineStartupException failure =
                assertThrows(EngineStartupException.class, () -> CesiumEngine.resolveStore("does-not-exist"));
        assertTrue(failure.getMessage().contains("does-not-exist"), failure::getMessage);
    }

    // ------------------------------------------------------------------ heap re-validation (M5 #10)

    @Test
    void heapBudgetPassesAtASinglePartitionOnAGenerousHeap() {
        ValidationReport report = CesiumEngine.heapBudgetReport(config(Set.of(Role.DISPATCH), 1, 1), 1, FOUR_GIB);
        assertFalse(report.hasErrors(), report::render);
    }

    @Test
    void heapBudgetBreachesAtTheRealPartitionCount() {
        // 100k partitions x 2,000,000 default entries x 64 B/entry far exceeds the 16 MiB budget
        // (64 MiB heap / 4): the estimate-of-1 load passed, the real partition count must fail (#10).
        ValidationReport report =
                CesiumEngine.heapBudgetReport(config(Set.of(Role.DISPATCH), 1, 1), 100_000, SIXTY_FOUR_MIB);
        assertTrue(report.hasErrors(), report::render);
        assertTrue(
                report.errors().stream()
                        .anyMatch(f -> f.path().equals("dispatch.max-pending-per-partition")
                                && f.message().contains("exceeds the heap budget")),
                report::render);
    }

    // ------------------------------------------------------------------ route descriptor

    /**
     * {@link CesiumEngine#buildRouteDescriptor} re-describes the source, destination and tracker
     * <em>after</em> startup validation already described them successfully. An unknown-topic answer
     * there is therefore broker metadata lag — possibly from a different node than validation asked —
     * not a vanished topic, so the path must wait it out rather than fail startup.
     *
     * <p>These are the regression guard for that: reverting the call to a single-shot
     * {@code describeTopic} turns them red.
     */
    @Nested
    class RecoverySnapshot {

        private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        private final Map<Integer, long[]> previous = new HashMap<>();

        private AtomicLong shard(int partition, long state, long remaining) {
            AtomicLong stateValue = new AtomicLong(state);
            AtomicLong remainingValue = new AtomicLong(remaining);
            Gauge.builder(KafkaTrackerStore.SHARD_STATE_METRIC, stateValue::get)
                    .tag("partition", Integer.toString(partition))
                    .register(registry);
            Gauge.builder(KafkaTrackerStore.REPLAY_REMAINING_METRIC, remainingValue::get)
                    .tag("partition", Integer.toString(partition))
                    .register(registry);
            return remainingValue;
        }

        @Test
        void reportsNonActiveShardsWithASlopeEta() {
            AtomicLong remaining = shard(1, 1, 1_000);
            shard(0, 0, 0);
            shard(2, 2, 0); // ACTIVE: omitted

            assertEquals(
                    List.of(
                            new ShardRecovery(0, ShardRecovery.State.ASSIGNED, 0, -1),
                            new ShardRecovery(1, ShardRecovery.State.RECOVERING, 1_000, -1)),
                    CesiumEngine.recoverySnapshot(registry, 10_000, previous),
                    "no ETA from a single sample");

            remaining.set(800); // 200 records in 1 s -> 800 records left ~ 4 s
            assertEquals(
                    new ShardRecovery(1, ShardRecovery.State.RECOVERING, 800, 4_000),
                    CesiumEngine.recoverySnapshot(registry, 11_000, previous).get(1));

            // A stalled backlog has no estimable ETA.
            assertEquals(
                    -1,
                    CesiumEngine.recoverySnapshot(registry, 12_000, previous)
                            .get(1)
                            .etaMillis());
        }

        @Test
        void emptyWithoutStoreGauges() {
            assertEquals(List.of(), CesiumEngine.recoverySnapshot(registry, 0, previous));
        }
    }

    @Nested
    class BuildRouteDescriptor {

        private final LaggingClusterAdmin admin = new LaggingClusterAdmin();
        private final CesiumConfig config = config(Set.of(Role.INGEST), 1, 1);
        private final IdentityBlob identity = new IdentityBlob("cluster-1", Uuid.randomUuid());

        private String tracker() {
            return config.route().tracker().resolvedTopic(config.applicationId());
        }

        private void givenAllTopicsExist() {
            admin.addTopic("src", 3);
            admin.addTopic("dst", 3);
            admin.addTopic(tracker(), 3);
        }

        @Test
        void aVisibleClusterCostsExactlyOneDescribePerTopic() {
            givenAllTopicsExist();

            RouteDescriptor route = CesiumEngine.buildRouteDescriptor(admin, config, identity, 3);

            assertEquals("src", route.sourceTopic());
            assertEquals("dst", route.destinationTopic());
            assertEquals(tracker(), route.trackerTopic());
            assertEquals(
                    Map.of("src", 1, "dst", 1, tracker(), 1), admin.describeCalls, "the happy path must not add RPCs");
        }

        @Test
        void aTransientlyInvisibleTopicIsWaitedOutRatherThanReportedVanished() {
            givenAllTopicsExist();
            // The tracker is the one cesium itself just created, so it is the likeliest to lag.
            admin.invisibleForDescribes(tracker(), 2);

            RouteDescriptor route = CesiumEngine.buildRouteDescriptor(admin, config, identity, 3);

            assertEquals(tracker(), route.trackerTopic());
            assertEquals(3, admin.describeCalls.get(tracker()), "two lagging answers, then the real one");
        }

        @Test
        void everyRouteTopicIsAwaitedNotJustTheTracker() {
            givenAllTopicsExist();
            admin.invisibleForDescribes("src", 1);
            admin.invisibleForDescribes("dst", 1);

            RouteDescriptor route = CesiumEngine.buildRouteDescriptor(admin, config, identity, 3);

            assertEquals("src", route.sourceTopic());
            assertEquals("dst", route.destinationTopic());
            assertEquals(2, admin.describeCalls.get("src"));
            assertEquals(2, admin.describeCalls.get("dst"));
        }

        // The terminal case — a topic that NEVER becomes visible must still fail startup rather than
        // wait forever — is deliberately not asserted here. Reaching it means spending the whole
        // TopicVisibility budget, which costs ~10 s of real sleeping in this module (there is no
        // clock/waiter seam across the module boundary, and widening TopicVisibility's API for one
        // test is not worth it). Budget expiry is already pinned in TopicVisibilityTest
        // (aTopicThatTrulyDoesNotExistReturnsEmptyAfterTheBudget); all that is left uncovered here is
        // describeOrFail's three-line orElseThrow.
    }

    // ------------------------------------------------------------------ helper

    /** A structurally valid config (route + DLQ + defaults) with the given roles and worker counts. */
    // ------------------------------------------------------------------ sidecar budget (§3.5)

    @Nested
    class SidecarBudget {

        private static final String KEY = "cursor.sidecar-max-bytes";

        private CesiumConfig withBudget(@Nullable Integer dispatchBudget, Map<String, String> storeProperties) {
            return withBudget(StoreConfig.DEFAULT_TYPE, dispatchBudget, storeProperties);
        }

        private CesiumConfig withBudget(
                String storeType, @Nullable Integer dispatchBudget, Map<String, String> storeProperties) {
            return new CesiumConfig(
                    "orders",
                    InstanceId.of("slot-0"),
                    null,
                    null,
                    new RouteConfig(
                            new TopicRef("src"), new TopicRef("dst"), null, Optional.of(new TopicRef("dlq")), null),
                    null,
                    null,
                    new StoreConfig(storeType, storeProperties),
                    null,
                    new DispatchConfig(
                            null,
                            null,
                            null,
                            null,
                            null,
                            new DispatchConfig.Cursor(dispatchBudget),
                            null,
                            null,
                            null,
                            null),
                    null,
                    null);
        }

        @Test
        void dispatchBudgetReachesTheStore() {
            Map<String, String> props = CesiumEngine.storeProperties(withBudget(2048, Map.of()), OptionalInt.of(4096));
            assertEquals("2048", props.get(KEY));
        }

        @Test
        void dispatchBudgetIsClampedToTheBrokerCap() {
            Map<String, String> props = CesiumEngine.storeProperties(withBudget(8192, Map.of()), OptionalInt.of(4096));
            assertEquals("4096", props.get(KEY));
        }

        @Test
        void unknownBrokerCapPassesTheBudgetThrough() {
            Map<String, String> props = CesiumEngine.storeProperties(withBudget(8192, Map.of()), OptionalInt.empty());
            assertEquals("8192", props.get(KEY));
        }

        @Test
        void explicitStoreValueWinsAndIsNotClamped() {
            Map<String, String> props =
                    CesiumEngine.storeProperties(withBudget(2048, Map.of(KEY, "8192")), OptionalInt.of(4096));
            assertEquals("8192", props.get(KEY));
        }

        @Test
        void outOfRangeBudgetIsLeftOutSoTheStoreKeepsItsDefault() {
            assertFalse(CesiumEngine.storeProperties(withBudget(64, Map.of()), OptionalInt.empty())
                    .containsKey(KEY));
            assertFalse(CesiumEngine.storeProperties(withBudget(3072, Map.of()), OptionalInt.of(100))
                    .containsKey(KEY));
        }

        @Test
        void otherStoreTypesAreNeverInjected() {
            Map<String, String> props =
                    CesiumEngine.storeProperties(withBudget("other-store", 2048, Map.of()), OptionalInt.of(4096));
            assertFalse(props.containsKey(KEY));
        }
    }

    private static CesiumConfig config(Set<Role> roles, int ingestWorkers, int dispatchWorkers) {
        return new CesiumConfig(
                "orders",
                InstanceId.of("slot-0"),
                roles,
                null,
                new RouteConfig(new TopicRef("src"), new TopicRef("dst"), null, Optional.of(new TopicRef("dlq")), null),
                null,
                null,
                null,
                new IngestConfig(ingestWorkers, null),
                new DispatchConfig(dispatchWorkers, null, null, null, null, null, null, null, null, null),
                null,
                null);
    }
}
