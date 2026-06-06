package events.cesium.kafka.app.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import events.cesium.kafka.app.health.HealthSnapshot;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Serves {@code /health/live} or {@code /health/ready} (design §9) by evaluating a
 * {@link HealthSnapshot} supplier (a {@code HealthAssessor} method reference) and mapping its verdict
 * to {@code 200} (pass) or {@code 503} (fail), with the JSON detail body. The evaluation reads cached
 * health atomics only — cheap and non-blocking, never a synchronous Kafka call.
 */
final class HealthHttpHandler implements HttpHandler {

    private final String path;
    private final Supplier<HealthSnapshot> evaluator;

    HealthHttpHandler(String path, Supplier<HealthSnapshot> evaluator) {
        this.path = path;
        this.evaluator = evaluator;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            if (HttpResponses.rejected(exchange, path)) {
                return;
            }
            HealthSnapshot snapshot = evaluator.get();
            HttpResponses.send(exchange, snapshot.ok() ? 200 : 503, HttpResponses.JSON, snapshot.json());
        } catch (RuntimeException e) {
            // A health probe must never wedge the dispatcher thread: fail closed (not-ready/down).
            HttpResponses.send(exchange, 503, HttpResponses.JSON, "{\"status\":\"ERROR\"}");
        } finally {
            exchange.close();
        }
    }
}
