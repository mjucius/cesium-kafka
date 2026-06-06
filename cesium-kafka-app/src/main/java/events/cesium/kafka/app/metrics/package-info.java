/**
 * The observability HTTP surface (design §9): a {@link events.cesium.kafka.app.metrics.ObservabilityServer}
 * on the JDK {@code com.sun.net.httpserver.HttpServer} (zero extra dependencies) serving
 * {@code /metrics} (Prometheus exposition from the engine's {@code PrometheusMeterRegistry}),
 * {@code /health/live}, {@code /health/ready} (decoupled from shard recovery, D21), and {@code /info}
 * (version, git commit, applicationId, store type + capabilities, roles, named acknowledgments).
 *
 * <p>The server is a lifecycle component: the app starts it <em>before</em> the engine (so probes
 * answer during startup) and stops it after readiness has flipped false on shutdown. Handlers are
 * cheap and non-blocking — they read cached health atomics and scrape the in-memory registry, never
 * calling Kafka synchronously.
 *
 * <p><strong>Kafka client metrics.</strong> Each loop owns its Kafka clients on its own thread and
 * self-registers their {@code KafkaClientMetrics} binders into the shared registry; this server only
 * scrapes whatever is registered, so it stays decoupled from client ownership and thread confinement.
 */
@org.jspecify.annotations.NullMarked
package events.cesium.kafka.app.metrics;
