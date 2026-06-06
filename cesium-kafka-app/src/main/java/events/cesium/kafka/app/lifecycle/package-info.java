/**
 * Production engine assembly and process lifecycle (design §6, §10): {@link
 * events.cesium.kafka.app.lifecycle.CesiumEngine} is the hardened, production version of the
 * integration-test {@code EngineHarness} — it runs startup validation over a real admin client,
 * captures the route identity, resolves the {@code SchedulerStore} through the published
 * ServiceLoader hook, and spawns the configured {@code IngestLoop}/{@code DispatchLoop} workers on
 * named non-daemon threads, exposing {@code start()} / {@code awaitDone()} / {@code stop(Duration)}
 * with the §6 graceful-shutdown ordering (readiness flips false first).
 *
 * <p>The engine writes per-loop liveness, assignment, and degraded signals into the {@link
 * events.cesium.kafka.app.health.EngineHealth} seam (sampling each loop's
 * {@code cesium_loop_last_iteration_timestamp_seconds} gauge for heartbeat freshness, §9), which the
 * observability module reads.
 */
@org.jspecify.annotations.NullMarked
package events.cesium.kafka.app.lifecycle;
