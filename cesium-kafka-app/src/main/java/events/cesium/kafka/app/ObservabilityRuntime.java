package events.cesium.kafka.app;

import events.cesium.kafka.app.health.EngineHealth;
import events.cesium.kafka.app.metrics.ServiceInfo;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * The dependencies {@link CesiumApp} hands the observability HTTP server (design §9) — the
 * read-side seam between this lifecycle module and the parallel observability module
 * ({@code events.cesium.kafka.app.metrics}). The app builds it once, after the engine has started,
 * and passes it to the discovered {@link ObservabilityServerFactory}.
 *
 * <p>Everything here is read-only and non-blocking from the server's perspective: the
 * {@link EngineHealth} is backed by atomics the engine writes, the registry is scraped in place,
 * and the {@link #serviceInfo()} supplier is re-evaluated per request so {@code /info} reflects the
 * latest store capabilities once the store is up.
 *
 * @param port the {@code observability.port} the server binds (§8 default 8081)
 * @param registry the Prometheus registry every loop self-registered its meters into; the server
 *     scrapes it for {@code /metrics}
 * @param health the engine-written health signals the server reads for {@code /health/live} and
 *     {@code /health/ready} (via a {@code HealthAssessor})
 * @param clock the wall clock the engine heartbeats on; the server's heartbeat-freshness arithmetic
 *     must use the same clock instance
 * @param livenessStaleAfter the maximum heartbeat age before a loop is considered wedged; chosen by
 *     the app to comfortably exceed the dispatch poll ceiling (§6) so an idle loop is never a false
 *     negative
 * @param serviceInfo the {@code /info} payload supplier (version, store type + capabilities, roles,
 *     acknowledgments), re-evaluated per request
 */
public record ObservabilityRuntime(
        int port,
        PrometheusMeterRegistry registry,
        EngineHealth health,
        Clock clock,
        Duration livenessStaleAfter,
        Supplier<ServiceInfo> serviceInfo) {}
