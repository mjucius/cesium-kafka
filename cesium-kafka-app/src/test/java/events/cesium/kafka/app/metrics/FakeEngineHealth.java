package events.cesium.kafka.app.metrics;

import events.cesium.kafka.app.health.EngineHealth;
import events.cesium.kafka.app.health.ShardRecovery;
import events.cesium.kafka.core.config.Role;
import java.util.List;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * A directly-settable {@link EngineHealth} for the observability HTTP tests: the engine's signals
 * are plain mutable fields so a test can drive live/ready/degraded/shutting-down transitions without
 * a broker. Heartbeat freshness is modelled as a boolean — {@code lastHeartbeatMillis} returns "now"
 * when fresh (so the {@code HealthAssessor}'s freshness arithmetic passes) and {@link Long#MIN_VALUE}
 * when stale (the never-heartbeated sentinel).
 */
final class FakeEngineHealth implements EngineHealth {

    private final Set<Role> roles;

    boolean loopAlive = true;
    boolean consumerAssigned = true;
    boolean heartbeatFresh = true;
    boolean startupComplete = true;
    boolean shuttingDown = false;
    boolean degraded = false;

    @Nullable String degradedCause = null;

    List<ShardRecovery> recovering = List.of();

    FakeEngineHealth(Role... roles) {
        this.roles = Set.of(roles);
    }

    @Override
    public Set<Role> roles() {
        return roles;
    }

    @Override
    public boolean loopAlive(Role role) {
        return loopAlive;
    }

    @Override
    public long lastHeartbeatMillis(Role role) {
        return heartbeatFresh ? System.currentTimeMillis() : Long.MIN_VALUE;
    }

    @Override
    public boolean consumerAssigned(Role role) {
        return consumerAssigned;
    }

    @Override
    public boolean startupComplete() {
        return startupComplete;
    }

    @Override
    public boolean shuttingDown() {
        return shuttingDown;
    }

    @Override
    public boolean degraded() {
        return degraded;
    }

    @Override
    public @Nullable String degradedCause() {
        return degradedCause;
    }

    @Override
    public List<ShardRecovery> recoveringShards() {
        return recovering;
    }
}
