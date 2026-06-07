package com.jucius.cesium.kafka.app.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Serves {@code GET /metrics} (design §9): the Prometheus exposition scraped from the engine's
 * {@code PrometheusMeterRegistry}. The scrape is a {@link Supplier} (typically {@code
 * registry::scrape}) so the handler stays decoupled from the registry type — it renders whatever
 * meters are registered. In v1 that is the engine's {@code cesium_*} inventory only; no
 * {@code KafkaClientMetrics} / JVM / process binders are wired (see {@code docs/operations.md} §13).
 *
 * <p>The scrape reads in-memory meter state only; it never calls Kafka. A scrape failure fails the
 * request {@code 500} rather than wedging the dispatcher thread.
 */
final class MetricsHttpHandler implements HttpHandler {

    static final String PATH = "/metrics";

    private final Supplier<String> scrape;

    MetricsHttpHandler(Supplier<String> scrape) {
        this.scrape = scrape;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            if (HttpResponses.rejected(exchange, PATH)) {
                return;
            }
            HttpResponses.send(exchange, 200, HttpResponses.PROMETHEUS, scrape.get());
        } catch (RuntimeException e) {
            HttpResponses.send(exchange, 500, HttpResponses.JSON, "{\"error\":\"scrape failed\"}");
        } finally {
            exchange.close();
        }
    }
}
