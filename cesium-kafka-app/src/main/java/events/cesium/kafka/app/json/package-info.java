/**
 * A minimal, dependency-light JSON writer (design §9): the observability HTTP endpoints and the
 * one-line JSON log encoder serialize small, fixed-shape objects without pulling a JSON binding
 * framework into the hot path. Hand-rolled escaping keeps the health handlers cheap and allocation-
 * modest; the app's Jackson dependency stays confined to YAML configuration loading.
 */
@org.jspecify.annotations.NullMarked
package events.cesium.kafka.app.json;
