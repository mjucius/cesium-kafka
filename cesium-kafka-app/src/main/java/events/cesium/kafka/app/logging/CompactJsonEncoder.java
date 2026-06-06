package events.cesium.kafka.app.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.encoder.EncoderBase;
import events.cesium.kafka.app.json.Json;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

/**
 * A compact one-line-per-event JSON log encoder (design §9), activated by {@code LOG_FORMAT=json}
 * (the JSON logback profile names this class). Each event renders as a single physical line — a
 * stable, fixed-shape object suitable for log aggregation — without pulling a logging-JSON dependency
 * (e.g. logstash-encoder) into the build; it reuses the app's hand-rolled {@link Json} writer.
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

    @Override
    public byte[] headerBytes() {
        return EMPTY;
    }

    @Override
    public byte[] encode(ILoggingEvent event) {
        Json.Obj obj = Json.object()
                .str("timestamp", Instant.ofEpochMilli(event.getTimeStamp()).toString())
                .str("level", event.getLevel().toString())
                .str("logger", event.getLoggerName())
                .str("thread", event.getThreadName())
                .str("message", event.getFormattedMessage());

        Map<String, String> mdc = event.getMDCPropertyMap();
        if (mdc != null && !mdc.isEmpty()) {
            Json.Obj context = Json.object();
            // Stable (alphabetical) key order keeps the rendered line deterministic across runs.
            for (Map.Entry<String, String> entry : new TreeMap<>(mdc).entrySet()) {
                context.str(entry.getKey(), entry.getValue());
            }
            obj.raw("mdc", context.end());
        }

        IThrowableProxy throwable = event.getThrowableProxy();
        if (throwable != null) {
            obj.str("exception", ThrowableProxyUtil.asString(throwable));
        }

        String line = obj.end() + System.lineSeparator();
        return line.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] footerBytes() {
        return EMPTY;
    }
}
