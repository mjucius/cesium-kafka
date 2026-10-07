package com.jucius.cesium.kafka.app.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.encoder.EncoderBase;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

/**
 * A compact one-line-per-event JSON log encoder (design §9), activated by {@code LOG_FORMAT=json}
 * (the JSON logback profile names this class). Each event renders as a single physical line — a
 * stable, fixed-shape object suitable for log aggregation — without pulling a logging-JSON dependency
 * (e.g. logstash-encoder) into the build; it streams through the Jackson core the app already ships.
 *
 * <p>Fields: {@code timestamp} (ISO-8601 UTC), {@code level}, {@code logger}, {@code thread},
 * {@code message}, the {@code mdc} object (the {@code applicationId}/{@code loop}/{@code partition}/
 * {@code txn} context keys, emitted in stable key order), and — only when present — an {@code
 * exception} string (its stack trace, with newlines JSON-escaped so the line stays single). The
 * encoder emits only the formatted message and these structural fields; it never reaches into record
 * keys or values, so it cannot leak payloads (design §9: never log payloads).
 */
public final class CompactJsonEncoder extends EncoderBase<ILoggingEvent> {

    private static final byte[] EMPTY = new byte[0];

    /** Lower-case hex in control-char escapes, keeping lines byte-identical to earlier releases. */
    private static final JsonFactory JSON =
            JsonFactory.builder().disable(JsonWriteFeature.WRITE_HEX_UPPER_CASE).build();

    @Override
    public byte[] headerBytes() {
        return EMPTY;
    }

    @Override
    public byte[] encode(ILoggingEvent event) {
        StringWriter out = new StringWriter(256);
        try (JsonGenerator json = JSON.createGenerator(out)) {
            json.writeStartObject();
            json.writeStringField(
                    "timestamp", Instant.ofEpochMilli(event.getTimeStamp()).toString());
            json.writeStringField("level", event.getLevel().toString());
            json.writeStringField("logger", event.getLoggerName());
            json.writeStringField("thread", event.getThreadName());
            json.writeStringField("message", event.getFormattedMessage());

            Map<String, String> mdc = event.getMDCPropertyMap();
            if (mdc != null && !mdc.isEmpty()) {
                json.writeObjectFieldStart("mdc");
                // Stable (alphabetical) key order keeps the rendered line deterministic across runs.
                for (Map.Entry<String, String> entry : new TreeMap<>(mdc).entrySet()) {
                    json.writeStringField(entry.getKey(), entry.getValue());
                }
                json.writeEndObject();
            }

            IThrowableProxy throwable = event.getThrowableProxy();
            if (throwable != null) {
                json.writeStringField("exception", ThrowableProxyUtil.asString(throwable));
            }
            json.writeEndObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e); // a StringWriter never throws
        }
        return (out + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] footerBytes() {
        return EMPTY;
    }
}
