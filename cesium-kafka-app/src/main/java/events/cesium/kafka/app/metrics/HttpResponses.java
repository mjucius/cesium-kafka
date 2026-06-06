package events.cesium.kafka.app.metrics;

import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Small shared helpers for writing {@link HttpExchange} responses: content-type constants, UTF-8
 * body writing, GET-only method enforcement, and exact-path matching (the JDK server matches
 * contexts by prefix, so handlers reject anything but their exact path).
 */
final class HttpResponses {

    static final String JSON = "application/json; charset=utf-8";
    static final String PROMETHEUS = "text/plain; version=0.0.4; charset=utf-8";

    private HttpResponses() {}

    /** Writes a UTF-8 response with the given status and content type, then closes the exchange. */
    static void send(HttpExchange exchange, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    /**
     * Enforces GET (the only method k8s probes and scrapers use) and exact-path matching. Returns
     * {@code true} when the request was rejected (405 or 404 already written) and the handler should
     * stop; {@code false} when the request is a valid GET for {@code path}.
     */
    static boolean rejected(HttpExchange exchange, String path) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, JSON, "{\"error\":\"method not allowed\"}");
            return true;
        }
        if (!path.equals(exchange.getRequestURI().getPath())) {
            send(exchange, 404, JSON, "{\"error\":\"not found\"}");
            return true;
        }
        return false;
    }
}
