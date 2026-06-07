package com.jucius.cesium.kafka.app.health;

import com.jucius.cesium.kafka.core.config.Role;
import java.util.List;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * The read side of the health seam between the engine and the observability layer (design §9). The
 * engine writes liveness/readiness signals into an implementation (see {@link MutableEngineHealth})
 * as it runs; the {@link HealthAssessor} and the HTTP health handlers read them. Every method must
 * be cheap and non-blocking — backed by atomics/volatiles, never a synchronous Kafka call — so a
 * {@code /health/live} or {@code /health/ready} probe never stalls behind a degraded broker.
 *
 * <p>Signals are reported per {@linkplain Role loop role}; {@link #roles()} returns only the loops
 * this instance actually runs (an ingest-only deployment has no dispatch loop, so its readiness must
 * not wait on a dispatch consumer assignment).
 */
public interface EngineHealth {

    /** The loop roles this instance started; liveness and readiness are evaluated over these only. */
    Set<Role> roles();

    /**
     * Whether {@code role}'s worker thread is alive (running, not crashed or exited). Combined with
     * {@linkplain #lastHeartbeatMillis(Role) heartbeat freshness} this is the liveness signal: a
     * thread that died, or one wedged so it no longer heartbeats, fails liveness.
     */
    boolean loopAlive(Role role);

    /**
     * The epoch-millis timestamp of {@code role}'s most recent loop iteration (bumped each {@code
     * poll()} cycle — the single poll is the loop's liveness heartbeat, §6), or {@link
     * Long#MIN_VALUE} if the loop has not yet completed its first iteration.
     */
    long lastHeartbeatMillis(Role role);

    /**
     * Whether {@code role}'s consumer currently holds a partition assignment (group A for ingest,
     * group B for dispatch). A readiness signal: an instance whose consumers hold nothing is not
     * yet doing useful work. Mid-rebalance this may briefly be false.
     */
    boolean consumerAssigned(Role role);

    /** Whether startup validation and the environment startup checks passed (a readiness gate). */
    boolean startupComplete();

    /**
     * Whether a graceful shutdown has been signalled. Readiness flips false here <em>before</em> the
     * Kafka clients close, so a preStop hook drains traffic away from this instance before it stops
     * (design §9). Never affects liveness.
     */
    boolean shuttingDown();

    /**
     * Whether any loop is parked-and-degraded (§3.8/R15): broker degradation outlasting retries, the
     * loop holding membership and retrying. Surfaced as readiness <em>detail</em> with a {@link
     * #degradedCause() cause} — it never fails a probe (the instance is alive and a member).
     */
    boolean degraded();

    /** A human-readable cause for the {@link #degraded()} state, or {@code null} when not degraded. */
    @Nullable String degradedCause();

    /**
     * A snapshot of the shards currently in recovery (design §3.6), for the {@code /health/ready}
     * detail body. <strong>Decoupled from readiness (D21):</strong> a recovering shard is reported
     * here but never makes the instance not-ready. Empty when every owned shard is {@code ACTIVE}.
     */
    List<ShardRecovery> recoveringShards();
}
