/**
 * The health model (design §9): the {@link events.cesium.kafka.app.health.EngineHealth} seam the
 * engine writes and the observability layer reads, a thread-safe atomic-backed implementation
 * ({@link events.cesium.kafka.app.health.MutableEngineHealth}), and the
 * {@link events.cesium.kafka.app.health.HealthAssessor} that derives liveness and readiness.
 *
 * <p><strong>Liveness vs readiness (decoupled from shard recovery, D21).</strong> Liveness is loop
 * heartbeat freshness plus thread liveness. Readiness additionally requires startup checks passed,
 * every loop's consumer assigned, and a recent poll — but <em>not</em> shard recovery: a healthily
 * replaying instance is ready, and recovery progress is surfaced as detail in the {@code
 * /health/ready} body rather than gating rollouts. Readiness flips false on a shutdown signal before
 * the Kafka clients close (preStop drain). A {@code degraded} flag with cause surfaces
 * park-and-degrade (§3.8) without failing probes.
 *
 * <p>Health reads are cheap and non-blocking: they consult cached atomics the engine updates and
 * never call Kafka from a request handler.
 */
@org.jspecify.annotations.NullMarked
package events.cesium.kafka.app.health;
