package com.jucius.cesium.kafka.app.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Serves {@code GET /info} (design §9): always version + git commit, plus — only when {@code
 * detailed} is enabled ({@code observability.detailed-info}, off by default, security M3) — the
 * applicationId, roles, store capabilities, and named startup acknowledgments. The endpoint is
 * unauthenticated, so applicationId (the fencing-id seed) and the operational detail are gated behind
 * the opt-in flag. Reads through a {@link Supplier} so the response reflects the latest {@link
 * ServiceInfo} snapshot — the app supplies a fuller one once the store has started and its
 * capabilities are known.
 */
final class InfoHttpHandler implements HttpHandler {

    static final String PATH = "/info";

    private final Supplier<ServiceInfo> info;
    private final boolean detailed;

    InfoHttpHandler(Supplier<ServiceInfo> info, boolean detailed) {
        this.info = info;
        this.detailed = detailed;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            if (HttpResponses.rejected(exchange, PATH)) {
                return;
            }
            HttpResponses.send(exchange, 200, HttpResponses.JSON, info.get().toJson(detailed));
        } catch (RuntimeException e) {
            // Mirror the sibling handlers' fail-closed posture: a thrown info supplier returns a clean
            // 500 body rather than letting the exception drop the connection with no response.
            HttpResponses.send(exchange, 500, HttpResponses.JSON, "{\"error\":\"info unavailable\"}");
        } finally {
            exchange.close();
        }
    }
}
