/**
 * Production engine assembly and process lifecycle (design §6, §10): {@link
 * com.jucius.cesium.kafka.app.lifecycle.CesiumEngine} is the hardened, production version of the
 * integration-test {@code EngineHarness} — it runs startup validation over a real admin client,
 * captures the route identity, builds the {@code KafkaTrackerStore}, and spawns one
 * {@code IngestLoop} and/or one {@code DispatchLoop} (per the configured roles) on named
 * non-daemon threads, exposing {@code start()} / {@code awaitDone()} / {@code stop(Duration)}
 * with the §6 graceful-shutdown ordering (readiness flips false first).
 *
 * <p>The engine writes per-loop liveness and degraded signals into the {@link
 * com.jucius.cesium.kafka.app.health.EngineHealth} seam (sampling each loop's
 * {@code cesium_loop_last_iteration_timestamp_seconds} gauge for heartbeat freshness, §9), which the
 * observability module reads.
 */
@org.jspecify.annotations.NullMarked
package com.jucius.cesium.kafka.app.lifecycle;
