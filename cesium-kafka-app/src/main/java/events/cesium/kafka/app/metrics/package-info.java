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
 * <p><strong>Client and JVM metrics.</strong> In v1 no {@code KafkaClientMetrics}, JVM, or process
 * binders are wired: this server scrapes only the engine's own {@code cesium_*} meters from the
 * shared registry. Obtain {@code kafka_*} / JVM / process series from a JMX-to-Prometheus exporter
 * sidecar instead (see {@code docs/operations.md} §13). The handler stays decoupled from client
 * ownership and thread confinement — it renders whatever meters are registered.
 */
@org.jspecify.annotations.NullMarked
package events.cesium.kafka.app.metrics;
