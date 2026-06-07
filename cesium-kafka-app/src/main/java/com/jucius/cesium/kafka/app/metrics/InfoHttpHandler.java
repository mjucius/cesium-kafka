package com.jucius.cesium.kafka.app.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Serves {@code GET /info} (design §9): version + git commit, applicationId, roles, store type and
 * its declared capabilities, and named startup acknowledgments. Reads through a {@link Supplier} so
 * the response reflects the latest {@link ServiceInfo} snapshot — the app supplies a fuller one once
 * the store has started and its capabilities are known.
 */
final class InfoHttpHandler implements HttpHandler {

    static final String PATH = "/info";

    private final Supplier<ServiceInfo> info;

    InfoHttpHandler(Supplier<ServiceInfo> info) {
        this.info = info;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            if (HttpResponses.rejected(exchange, PATH)) {
                return;
            }
            HttpResponses.send(exchange, 200, HttpResponses.JSON, info.get().toJson());
        } catch (RuntimeException e) {
            // Mirror the sibling handlers' fail-closed posture: a thrown info supplier returns a clean
            // 500 body rather than letting the exception drop the connection with no response.
            HttpResponses.send(exchange, 500, HttpResponses.JSON, "{\"error\":\"info unavailable\"}");
        } finally {
            exchange.close();
        }
    }
}
