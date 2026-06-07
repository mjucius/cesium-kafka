package com.jucius.cesium.kafka.app.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Smoke tests for the one-line JSON log encoder (design §9): valid JSON, MDC, no payload leakage. */
class CompactJsonEncoderTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> ALLOWED_FIELDS =
            Set.of("timestamp", "level", "logger", "thread", "message", "mdc", "exception");

    private final CompactJsonEncoder encoder = newEncoder();

    private static CompactJsonEncoder newEncoder() {
        CompactJsonEncoder encoder = new CompactJsonEncoder();
        encoder.setContext(new LoggerContext());
        encoder.start();
        return encoder;
    }

    @Test
    void encodesOneLineOfStructuredJson() throws Exception {
        LoggingEvent event = new LoggingEvent();
        event.setLevel(Level.INFO);
        event.setLoggerName("com.jucius.cesium.kafka.core.ingest.IngestLoop");
        event.setThreadName("cesium-ingest-0");
        event.setMessage("ingest transaction committed");
        event.setTimeStamp(1_700_000_000_000L);
        event.setMDCPropertyMap(Map.of(
                "applicationId", "orders-delay",
                "loop", "ingest",
                "partition", "3",
                "txn", "orders-delay-ingest-3"));

        String encoded = new String(encoder.encode(event), StandardCharsets.UTF_8);

        // One physical line: exactly one trailing line separator, no embedded newline.
        assertTrue(encoded.endsWith(System.lineSeparator()), encoded);
        String line = encoded.stripTrailing();
        assertFalse(line.contains("\n"), line);

        JsonNode node = JSON.readTree(line);
        assertEquals("INFO", node.get("level").asText());
        assertEquals(
                "com.jucius.cesium.kafka.core.ingest.IngestLoop",
                node.get("logger").asText());
        assertEquals("cesium-ingest-0", node.get("thread").asText());
        assertEquals("ingest transaction committed", node.get("message").asText());
        assertTrue(node.has("timestamp"));

        JsonNode mdc = node.get("mdc");
        assertEquals("orders-delay", mdc.get("applicationId").asText());
        assertEquals("ingest", mdc.get("loop").asText());
        assertEquals("3", mdc.get("partition").asText());
        assertEquals("orders-delay-ingest-3", mdc.get("txn").asText());
    }

    @Test
    void emitsOnlyStructuralFieldsSoPayloadsCannotLeak() throws Exception {
        LoggingEvent event = new LoggingEvent();
        event.setLevel(Level.WARN);
        event.setLoggerName("com.jucius.cesium.kafka.core.dispatch.DispatchLoop");
        event.setThreadName("cesium-dispatch-1");
        event.setMessage("dispatch parked and degraded");
        event.setTimeStamp(1_700_000_000_000L);
        event.setMDCPropertyMap(Map.of()); // a bare LoggingEvent has no LoggerContext to source MDC from

        JsonNode node = JSON.readTree(new String(encoder.encode(event), StandardCharsets.UTF_8));

        // The encoder reaches only structural fields — never into record keys/values.
        Iterator<String> fields = node.fieldNames();
        while (fields.hasNext()) {
            String field = fields.next();
            assertTrue(ALLOWED_FIELDS.contains(field), "unexpected field: " + field);
        }
    }

    @Test
    void serializesThrowableAsAnInlineEscapedString() throws Exception {
        LoggingEvent event = new LoggingEvent();
        event.setLevel(Level.ERROR);
        event.setLoggerName("com.jucius.cesium.kafka.app.metrics.ObservabilityServer");
        event.setThreadName("main");
        event.setMessage("startup failed");
        event.setTimeStamp(1_700_000_000_000L);
        event.setMDCPropertyMap(Map.of()); // a bare LoggingEvent has no LoggerContext to source MDC from
        event.setThrowableProxy(new ThrowableProxy(new IllegalStateException("boom")));

        String encoded = new String(encoder.encode(event), StandardCharsets.UTF_8);
        String line = encoded.stripTrailing();
        // Multi-line stack trace stays a single physical line (newlines JSON-escaped).
        assertFalse(line.contains("\n"), line);

        JsonNode node = JSON.readTree(line);
        String exception = node.get("exception").asText();
        assertTrue(exception.contains("IllegalStateException"), exception);
        assertTrue(exception.contains("boom"), exception);
    }
}
