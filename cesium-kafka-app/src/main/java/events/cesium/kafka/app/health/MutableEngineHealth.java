package events.cesium.kafka.app.health;

import events.cesium.kafka.core.config.Role;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.jspecify.annotations.Nullable;

/**
 * The atomic-backed implementation of the {@link EngineHealth} seam (design §9). The engine writes
 * health signals through the {@code register*}/{@code mark*}/{@code set*} methods on its own loop
 * and admin threads; the observability layer reads the {@link EngineHealth} view from the HTTP
 * dispatcher thread. All writes are to atomics/volatiles, so reads never block and never observe a
 * torn value.
 *
 * <p><strong>Writer contract.</strong> The engine:
 *
 * <ul>
 *   <li>{@link #registerLoop(Role, BooleanSupplier) registers} each started loop once, supplying a
 *       liveness probe (typically {@code thread::isAlive});
 *   <li>{@link #heartbeat(Role) heartbeats} once per loop iteration;
 *   <li>updates {@link #consumerAssigned(Role, boolean) assignment} from the rebalance callbacks;
 *   <li>{@link #markStartupComplete() marks startup complete} once the startup checks pass;
 *   <li>{@link #beginShutdown() signals shutdown} at the very start of graceful stop, before any
 *       client closes;
 *   <li>updates the {@link #setDegraded(boolean, String) degraded} flag and the
 *       {@link #updateRecovery(List) recovery} snapshot as those states change.
 * </ul>
 */
public final class MutableEngineHealth implements EngineHealth {

    private final Clock clock;
    private final ConcurrentMap<Role, Loop> loops = new ConcurrentHashMap<>();
    private final AtomicBoolean startupComplete = new AtomicBoolean();
    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private final AtomicBoolean degraded = new AtomicBoolean();
    private volatile @Nullable String degradedCause;
    private volatile List<ShardRecovery> recovering = List.of();

    /**
     * @param clock the source of heartbeat timestamps; must be the same wall clock the
     *     {@link HealthAssessor} compares against so heartbeat-freshness arithmetic is consistent
     */
    public MutableEngineHealth(Clock clock) {
        this.clock = clock;
    }

    // ------------------------------------------------------------------ writer surface (engine)

    /** Registers a started loop and its thread-liveness probe (e.g. {@code thread::isAlive}). */
    public void registerLoop(Role role, BooleanSupplier aliveProbe) {
        loops.put(role, new Loop(aliveProbe));
    }

    /** Bumps {@code role}'s heartbeat to now; called once per loop iteration (§6). No-op if unregistered. */
    public void heartbeat(Role role) {
        Loop loop = loops.get(role);
        if (loop != null) {
            loop.heartbeatMillis.set(clock.millis());
        }
    }

    /** Records whether {@code role}'s consumer currently holds an assignment. No-op if unregistered. */
    public void consumerAssigned(Role role, boolean assigned) {
        Loop loop = loops.get(role);
        if (loop != null) {
            loop.assigned.set(assigned);
        }
    }

    /** Marks startup validation + environment checks complete (a readiness gate). */
    public void markStartupComplete() {
        startupComplete.set(true);
    }

    /** Signals graceful shutdown: readiness flips false immediately, before any client closes. */
    public void beginShutdown() {
        shuttingDown.set(true);
    }

    /** Sets (or clears) the park-and-degrade flag and its cause (§3.8). */
    public void setDegraded(boolean isDegraded, @Nullable String cause) {
        // Publish the cause before the flag so a reader that sees degraded==true always sees a cause.
        this.degradedCause = isDegraded ? cause : null;
        this.degraded.set(isDegraded);
    }

    /** Replaces the recovering-shard snapshot surfaced in the readiness detail body (§3.6). */
    public void updateRecovery(List<ShardRecovery> shards) {
        this.recovering = List.copyOf(shards);
    }

    // ------------------------------------------------------------------ reader surface (EngineHealth)

    @Override
    public Set<Role> roles() {
        return Set.copyOf(loops.keySet());
    }

    @Override
    public boolean loopAlive(Role role) {
        Loop loop = loops.get(role);
        return loop != null && loop.aliveProbe.getAsBoolean();
    }

    @Override
    public long lastHeartbeatMillis(Role role) {
        Loop loop = loops.get(role);
        return loop == null ? Long.MIN_VALUE : loop.heartbeatMillis.get();
    }

    @Override
    public boolean consumerAssigned(Role role) {
        Loop loop = loops.get(role);
        return loop != null && loop.assigned.get();
    }

    @Override
    public boolean startupComplete() {
        return startupComplete.get();
    }

    @Override
    public boolean shuttingDown() {
        return shuttingDown.get();
    }

    @Override
    public boolean degraded() {
        return degraded.get();
    }

    @Override
    public @Nullable String degradedCause() {
        return degradedCause;
    }

    @Override
    public List<ShardRecovery> recoveringShards() {
        return recovering;
    }

    private static final class Loop {
        final BooleanSupplier aliveProbe;
        final AtomicLong heartbeatMillis = new AtomicLong(Long.MIN_VALUE);
        final AtomicBoolean assigned = new AtomicBoolean();

        Loop(BooleanSupplier aliveProbe) {
            this.aliveProbe = aliveProbe;
        }
    }
}
