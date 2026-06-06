package events.cesium.kafka.app.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import events.cesium.kafka.core.config.Role;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link HealthAssessor} over the real atomic-backed {@link MutableEngineHealth}
 * writer seam (design §9): heartbeat-freshness arithmetic, the per-loop JSON detail, and the
 * recovery/degraded/shutdown decoupling — all on a fixed clock, no broker.
 */
class HealthAssessorTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Instant T0 = Instant.parse("2026-06-06T00:00:00Z");
    private static final Clock ENGINE_CLOCK = Clock.fixed(T0, ZoneOffset.UTC);
    private static final Duration STALE_AFTER = Duration.ofSeconds(30);

    @Test
    void livenessUpWhenThreadAliveAndHeartbeatFresh() throws Exception {
        MutableEngineHealth health = new MutableEngineHealth(ENGINE_CLOCK);
        health.registerLoop(Role.INGEST, () -> true);
        health.heartbeat(Role.INGEST);

        HealthSnapshot live = new HealthAssessor(health, ENGINE_CLOCK, STALE_AFTER).liveness();

        assertTrue(live.ok());
        JsonNode node = JSON.readTree(live.json());
        assertEquals("UP", node.get("status").asText());
        JsonNode ingest = node.get("loops").get("ingest");
        assertTrue(ingest.get("alive").asBoolean());
        assertTrue(ingest.get("fresh").asBoolean());
        assertEquals(0, ingest.get("heartbeatAgeMillis").asLong());
    }

    @Test
    void livenessDownWhenHeartbeatStale() throws Exception {
        MutableEngineHealth health = new MutableEngineHealth(ENGINE_CLOCK);
        health.registerLoop(Role.INGEST, () -> true);
        health.heartbeat(Role.INGEST); // stamped at T0

        Clock tenSecondsLater = Clock.fixed(T0.plusSeconds(10), ZoneOffset.UTC);
        HealthSnapshot live = new HealthAssessor(health, tenSecondsLater, Duration.ofSeconds(1)).liveness();

        assertFalse(live.ok());
        JsonNode ingest = JSON.readTree(live.json()).get("loops").get("ingest");
        assertTrue(ingest.get("alive").asBoolean());
        assertFalse(ingest.get("fresh").asBoolean());
        assertEquals(10_000, ingest.get("heartbeatAgeMillis").asLong());
    }

    @Test
    void livenessDownWhenThreadDead() {
        MutableEngineHealth health = new MutableEngineHealth(ENGINE_CLOCK);
        health.registerLoop(Role.DISPATCH, () -> false);
        health.heartbeat(Role.DISPATCH);

        assertFalse(
                new HealthAssessor(health, ENGINE_CLOCK, STALE_AFTER).liveness().ok());
    }

    @Test
    void readinessRequiresStartupAssignmentAndFreshPoll() throws Exception {
        MutableEngineHealth health = healthyBothRoles();
        HealthAssessor assessor = new HealthAssessor(health, ENGINE_CLOCK, STALE_AFTER);

        // Startup not yet complete -> NOT_READY.
        assertFalse(assessor.readiness().ok());

        health.markStartupComplete();
        assertTrue(assessor.readiness().ok());

        // Losing an assignment mid-rebalance fails readiness.
        health.consumerAssigned(Role.DISPATCH, false);
        HealthSnapshot ready = assessor.readiness();
        assertFalse(ready.ok());
        assertEquals("NOT_READY", JSON.readTree(ready.json()).get("status").asText());
    }

    @Test
    void recoveryAndDegradedAreReadyDetailNotGates() throws Exception {
        MutableEngineHealth health = healthyBothRoles();
        health.markStartupComplete();
        health.setDegraded(true, "destination ISR below min.insync");
        health.updateRecovery(List.of(new ShardRecovery(2, ShardRecovery.State.RECOVERING, 5_000, 1_200)));

        HealthSnapshot ready = new HealthAssessor(health, ENGINE_CLOCK, STALE_AFTER).readiness();

        // Decoupled (D21, §3.8): recovering + degraded is still READY.
        assertTrue(ready.ok());
        JsonNode node = JSON.readTree(ready.json());
        assertEquals("READY", node.get("status").asText());
        assertTrue(node.get("degraded").asBoolean());
        assertEquals(
                "destination ISR below min.insync", node.get("degradedCause").asText());
        assertEquals(1, node.get("recovery").size());
        assertEquals(2, node.get("recovery").get(0).get("partition").asInt());
    }

    @Test
    void readinessFlipsFalseOnShutdownWhileLivenessHolds() throws Exception {
        MutableEngineHealth health = healthyBothRoles();
        health.markStartupComplete();
        HealthAssessor assessor = new HealthAssessor(health, ENGINE_CLOCK, STALE_AFTER);
        assertTrue(assessor.readiness().ok());

        health.beginShutdown();

        HealthSnapshot ready = assessor.readiness();
        assertFalse(ready.ok());
        assertTrue(JSON.readTree(ready.json()).get("shuttingDown").asBoolean());
        // preStop drain: still alive while draining.
        assertTrue(assessor.liveness().ok());
    }

    private static MutableEngineHealth healthyBothRoles() {
        MutableEngineHealth health = new MutableEngineHealth(ENGINE_CLOCK);
        for (Role role : Role.values()) {
            health.registerLoop(role, () -> true);
            health.heartbeat(role);
            health.consumerAssigned(role, true);
        }
        return health;
    }
}
