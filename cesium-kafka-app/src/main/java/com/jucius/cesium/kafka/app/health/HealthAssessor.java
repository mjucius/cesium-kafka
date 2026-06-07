package com.jucius.cesium.kafka.app.health;

import com.jucius.cesium.kafka.app.json.Json;
import com.jucius.cesium.kafka.core.config.Role;
import java.time.Clock;
import java.time.Duration;
import java.util.Locale;

/**
 * Derives the liveness and readiness verdicts (and their JSON detail bodies) from an
 * {@link EngineHealth} snapshot (design §9). Pure and side-effect-free: it reads the cached signals
 * and a {@link Clock}, computing heartbeat freshness against {@code livenessStaleAfter}. No Kafka,
 * no blocking — safe to call from an HTTP dispatcher thread on every probe.
 *
 * <p><strong>Liveness</strong> = every started loop's thread is alive <em>and</em> its heartbeat is
 * fresh (the loop is iterating, not wedged).
 *
 * <p><strong>Readiness</strong> = startup checks passed, not shutting down, and every started loop
 * is alive, holds a consumer assignment, and has a fresh heartbeat (a recent poll). <strong>Shard
 * recovery and the degraded flag are detail only</strong> (D21, §3.8): a recovering or degraded
 * instance is still ready. Readiness flips false the moment shutdown is signalled, before clients
 * close, so a preStop hook drains traffic first.
 */
public final class HealthAssessor {

    private final EngineHealth health;
    private final Clock clock;
    private final long livenessStaleMillis;

    /**
     * @param health the engine-written health signals
     * @param clock the wall clock for heartbeat-age arithmetic (same clock the engine heartbeats on)
     * @param livenessStaleAfter the maximum heartbeat age before a loop is considered wedged; must
     *     exceed the dispatch poll ceiling (a quiet loop sleeps in {@code poll()} up to that long
     *     between heartbeats, §6) to avoid false negatives during idle periods
     */
    public HealthAssessor(EngineHealth health, Clock clock, Duration livenessStaleAfter) {
        this.health = health;
        this.clock = clock;
        this.livenessStaleMillis = livenessStaleAfter.toMillis();
    }

    /** Evaluates liveness: thread liveness + heartbeat freshness for every started loop. */
    public HealthSnapshot liveness() {
        boolean ok = true;
        Json.Obj loops = Json.object();
        for (Role role : startedRoles()) {
            boolean alive = health.loopAlive(role);
            boolean fresh = heartbeatFresh(role);
            ok &= alive && fresh;
            loops.raw(
                    roleKey(role),
                    Json.object()
                            .bool("alive", alive)
                            .num("heartbeatAgeMillis", heartbeatAgeMillis(role))
                            .bool("fresh", fresh)
                            .end());
        }
        String json = Json.object()
                .str("status", ok ? "UP" : "DOWN")
                .raw("loops", loops.end())
                .end();
        return new HealthSnapshot(ok, json);
    }

    /** Evaluates readiness, with recovery and degraded surfaced as non-gating detail. */
    public HealthSnapshot readiness() {
        boolean startupComplete = health.startupComplete();
        boolean shuttingDown = health.shuttingDown();
        boolean ok = startupComplete && !shuttingDown;
        Json.Obj loops = Json.object();
        for (Role role : startedRoles()) {
            boolean alive = health.loopAlive(role);
            boolean assigned = health.consumerAssigned(role);
            boolean fresh = heartbeatFresh(role);
            ok &= alive && assigned && fresh;
            loops.raw(
                    roleKey(role),
                    Json.object()
                            .bool("alive", alive)
                            .bool("assigned", assigned)
                            .bool("fresh", fresh)
                            .end());
        }
        Json.Arr recovery = Json.array();
        for (ShardRecovery shard : health.recoveringShards()) {
            recovery.value(shard.toJson());
        }
        Json.Obj root = Json.object()
                .str("status", ok ? "READY" : "NOT_READY")
                .bool("startupComplete", startupComplete)
                .bool("shuttingDown", shuttingDown)
                .bool("degraded", health.degraded());
        String cause = health.degradedCause();
        if (cause != null) {
            root.str("degradedCause", cause);
        }
        root.raw("loops", loops.end()).raw("recovery", recovery.end());
        return new HealthSnapshot(ok, root.end());
    }

    private Iterable<Role> startedRoles() {
        // Deterministic enum order (ingest before dispatch) regardless of the Set implementation.
        java.util.List<Role> ordered = new java.util.ArrayList<>(2);
        for (Role role : Role.values()) {
            if (health.roles().contains(role)) {
                ordered.add(role);
            }
        }
        return ordered;
    }

    private boolean heartbeatFresh(Role role) {
        long heartbeat = health.lastHeartbeatMillis(role);
        if (heartbeat == Long.MIN_VALUE) {
            return false;
        }
        return clock.millis() - heartbeat <= livenessStaleMillis;
    }

    private long heartbeatAgeMillis(Role role) {
        long heartbeat = health.lastHeartbeatMillis(role);
        return heartbeat == Long.MIN_VALUE ? -1 : Math.max(0, clock.millis() - heartbeat);
    }

    private static String roleKey(Role role) {
        return role.name().toLowerCase(Locale.ROOT);
    }
}
