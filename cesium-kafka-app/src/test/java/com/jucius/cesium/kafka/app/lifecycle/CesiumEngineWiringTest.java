package com.jucius.cesium.kafka.app.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.store.TrackerBackedStore;
import com.jucius.cesium.kafka.app.lifecycle.CesiumEngine.LoopSpec;
import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.DispatchConfig;
import com.jucius.cesium.kafka.core.config.IngestConfig;
import com.jucius.cesium.kafka.core.config.InstanceId;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.core.config.RouteConfig;
import com.jucius.cesium.kafka.core.config.TopicRef;
import com.jucius.cesium.kafka.core.config.ValidationReport;
import com.jucius.cesium.kafka.store.tracker.KafkaTrackerStoreProvider;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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

    // ------------------------------------------------------------------ helper

    /** A structurally valid config (route + DLQ + defaults) with the given roles and worker counts. */
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
