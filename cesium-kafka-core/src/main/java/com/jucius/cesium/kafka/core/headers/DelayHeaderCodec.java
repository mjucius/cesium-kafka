package com.jucius.cesium.kafka.core.headers;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.jspecify.annotations.Nullable;

/**
 * Decodes the cesium control headers ({@code cesium-delay-ms}, {@code cesium-deliver-at}) of one
 * source record into a {@link HeaderParseResult} (design §2.3, D1–D2).
 *
 * <p><strong>Encoding (D1).</strong> Canonical UTF-8 ASCII decimal matching
 * {@value CesiumHeaders#ASCII_DECIMAL_VALUE_REGEX}; values passing the regex but overflowing a
 * signed 64-bit long (19 digits reach 10^19−1 &gt; {@link Long#MAX_VALUE}) are malformed — never
 * wrapped. Behind {@code headers.accept-binary-long-values} an exactly-8-byte big-endian long is
 * accepted <em>instead of</em> ASCII: the modes are mutually exclusive because a length-8 value is
 * ambiguous between "8 ASCII digits" and "8-byte long".
 *
 * <p><strong>{@code delay-ms} base (D2).</strong> The delay is relative to the <em>source record
 * timestamp</em> whenever one exists ({@code timestampType != NO_TIMESTAMP_TYPE} and
 * {@code timestamp >= 0}), falling back to the injected ingest clock otherwise. Rationale:
 * determinism — an aborted ingest transaction is retried, and a wall-clock base would compute a
 * different dispatch instant on every attempt, while the record timestamp is identical across
 * retries (and producer-controlled). The fallback is unavoidable for timestampless records and is
 * documented as such.
 *
 * <p><strong>Precedence.</strong> {@code cesium-deliver-at} wins when both headers are present —
 * including when its value is malformed (the losing header is ignored entirely, not used as a
 * fallback). Multiple values for one header resolve to the last value. Either case flags the
 * result {@linkplain HeaderParseResult#conflicted() conflicted} for metrics; conflicts are not
 * errors.
 *
 * <p><strong>Validation.</strong> {@code cesium-delay-ms ∈ [0, delay.max]};
 * {@code cesium-deliver-at ≤ now + delay.max}. Past or zero requests relay immediately
 * ({@code past_due}) and are not errors. All epoch arithmetic saturates at the {@code long} range
 * instead of wrapping.
 *
 * <p>Stateless and thread-safe; the clock is an argument, so every outcome is a pure function of
 * (headers, record timestamp, now) — the property the §11.1 tests rely on.
 */
public final class DelayHeaderCodec {

    private static final Pattern ASCII_DECIMAL = Pattern.compile(CesiumHeaders.ASCII_DECIMAL_VALUE_REGEX);

    /** Longest offending-value rendering quoted in a {@code Malformed} detail. */
    private static final int MAX_DETAIL_VALUE_CHARS = 40;

    private final long delayMaxMs;
    private final boolean acceptBinaryLongValues;

    /**
     * @param delayMax the configured {@code delay.max} (non-negative); requests beyond it are
     *     {@link HeaderParseResult.OverMax}. A duration whose millisecond rendering exceeds the
     *     {@code long} range saturates to {@link Long#MAX_VALUE}.
     * @param acceptBinaryLongValues {@code headers.accept-binary-long-values}: when true, control
     *     header values must be exactly-8-byte big-endian longs and ASCII decimal is rejected
     *     (exclusive modes, D1)
     */
    public DelayHeaderCodec(Duration delayMax, boolean acceptBinaryLongValues) {
        Objects.requireNonNull(delayMax, "delayMax");
        if (delayMax.isNegative()) {
            throw new IllegalArgumentException("delay.max must be non-negative: " + delayMax);
        }
        long ms;
        try {
            ms = delayMax.toMillis();
        } catch (ArithmeticException overflow) {
            ms = Long.MAX_VALUE;
        }
        this.delayMaxMs = ms;
        this.acceptBinaryLongValues = acceptBinaryLongValues;
    }

    /** Convenience overload taking the consumed record directly. */
    public HeaderParseResult parse(ConsumerRecord<?, ?> record, long ingestNowMs) {
        return parse(record.headers(), record.timestamp(), record.timestampType(), ingestNowMs);
    }

    /**
     * Decodes the control headers of one record.
     *
     * @param headers the record's headers
     * @param recordTimestampMs the source record timestamp (epoch ms; may be negative when absent)
     * @param timestampType the source record's timestamp type; {@code NO_TIMESTAMP_TYPE} forces
     *     the ingest-clock fallback for {@code delay-ms}
     * @param ingestNowMs the ingest clock reading, epoch milliseconds UTC (injected, never read
     *     internally — determinism for tests and EOS retry reasoning)
     */
    public HeaderParseResult parse(
            Headers headers, long recordTimestampMs, TimestampType timestampType, long ingestNowMs) {
        Header deliverAtHeader = headers.lastHeader(CesiumHeaders.DELIVER_AT);
        Header delayMsHeader = headers.lastHeader(CesiumHeaders.DELAY_MS);
        if (deliverAtHeader == null && delayMsHeader == null) {
            return HeaderParseResult.NoDelay.INSTANCE;
        }
        // Both headers present, or repeated values for one header, count as a conflict (§2.3):
        // precedence resolves it (deliver-at wins; lastHeader wins) but the flag keeps it observable.
        boolean conflicted = (deliverAtHeader != null && delayMsHeader != null)
                || countValues(headers, CesiumHeaders.DELIVER_AT) > 1
                || countValues(headers, CesiumHeaders.DELAY_MS) > 1;
        if (deliverAtHeader != null) {
            return parseDeliverAt(deliverAtHeader.value(), ingestNowMs, conflicted);
        }
        return parseDelayMs(delayMsHeader.value(), recordTimestampMs, timestampType, ingestNowMs, conflicted);
    }

    /** {@code cesium-deliver-at}: absolute epoch-millis instant, at most {@code now + delay.max}. */
    private HeaderParseResult parseDeliverAt(byte @Nullable [] raw, long ingestNowMs, boolean conflicted) {
        return switch (decode(CesiumHeaders.DELIVER_AT, raw)) {
            case DecodedError(String detail) -> new HeaderParseResult.Malformed(detail, conflicted);
            case DecodedValue(long deliverAtMs) -> {
                long latestAllowedMs = saturatedAddMillis(ingestNowMs, delayMaxMs);
                if (deliverAtMs > latestAllowedMs) {
                    yield new HeaderParseResult.OverMax(
                            deliverAtMs,
                            CesiumHeaders.DELIVER_AT + ": " + deliverAtMs + " exceeds now + delay.max = "
                                    + latestAllowedMs,
                            conflicted);
                }
                // Past or present instants relay immediately — explicitly not an error (§2.3).
                if (deliverAtMs <= ingestNowMs) {
                    yield new HeaderParseResult.RelayImmediately(conflicted);
                }
                yield new HeaderParseResult.Schedule(deliverAtMs, conflicted);
            }
        };
    }

    /** {@code cesium-delay-ms}: relative delay in {@code [0, delay.max]}, based per D2. */
    private HeaderParseResult parseDelayMs(
            byte @Nullable [] raw,
            long recordTimestampMs,
            TimestampType timestampType,
            long ingestNowMs,
            boolean conflicted) {
        return switch (decode(CesiumHeaders.DELAY_MS, raw)) {
            case DecodedError(String detail) -> new HeaderParseResult.Malformed(detail, conflicted);
            case DecodedValue(long delayMs) -> {
                // Only reachable in binary mode: the ASCII grammar cannot express a sign.
                if (delayMs < 0) {
                    yield new HeaderParseResult.Malformed(
                            CesiumHeaders.DELAY_MS + ": negative delay: " + delayMs, conflicted);
                }
                // D2: source record timestamp when one exists — identical across EOS abort/retry
                // cycles, so the same record always computes the same dispatch instant; ingest
                // clock only for timestampless records, where no deterministic base exists.
                boolean hasSourceTimestamp = timestampType != TimestampType.NO_TIMESTAMP_TYPE && recordTimestampMs >= 0;
                long baseMs = hasSourceTimestamp ? recordTimestampMs : ingestNowMs;
                long dispatchAtMs = saturatedAddMillis(baseMs, delayMs);
                long latestAllowedMs = saturatedAddMillis(ingestNowMs, delayMaxMs);
                // Range check before the past-due fast path: a delay over the maximum violated the
                // contract even when an old record timestamp would land the instant in the past.
                if (delayMs > delayMaxMs) {
                    yield new HeaderParseResult.OverMax(
                            dispatchAtMs,
                            CesiumHeaders.DELAY_MS + ": " + delayMs + " ms exceeds delay.max " + delayMaxMs + " ms",
                            conflicted);
                }
                // The D2 base is the producer-controlled record timestamp, so the relative check above
                // does not bound the ABSOLUTE dispatch instant: a future-dated timestamp with an
                // in-range delay would schedule far beyond now + delay.max. Bound it exactly as the
                // deliver-at path does — otherwise delay.max is not an upper bound on when a record fires.
                if (dispatchAtMs > latestAllowedMs) {
                    yield new HeaderParseResult.OverMax(
                            dispatchAtMs,
                            CesiumHeaders.DELAY_MS + ": dispatch instant " + dispatchAtMs + " (source timestamp "
                                    + baseMs + " + " + delayMs + " ms) exceeds now + delay.max = " + latestAllowedMs,
                            conflicted);
                }
                if (dispatchAtMs <= ingestNowMs) {
                    yield new HeaderParseResult.RelayImmediately(conflicted);
                }
                yield new HeaderParseResult.Schedule(dispatchAtMs, conflicted);
            }
        };
    }

    /** Decodes one header value in the configured mode; errors carry the diagnostic detail. */
    private Decoded decode(String headerName, byte @Nullable [] raw) {
        if (raw == null) {
            return new DecodedError(headerName + ": null value");
        }
        if (acceptBinaryLongValues) {
            // Exclusive binary mode (D1): exactly 8 bytes, big-endian. ASCII is NOT accepted here —
            // an 8-byte value would be ambiguous between "8 ASCII digits" and "8-byte long".
            if (raw.length != 8) {
                return new DecodedError(headerName + ": binary mode expects exactly 8 bytes, got " + raw.length);
            }
            return new DecodedValue(ByteBuffer.wrap(raw).getLong());
        }
        String text = new String(raw, StandardCharsets.UTF_8);
        if (!ASCII_DECIMAL.matcher(text).matches()) {
            return new DecodedError(headerName + ": not ASCII decimal " + CesiumHeaders.ASCII_DECIMAL_VALUE_REGEX
                    + ": \"" + sanitize(text) + "\"");
        }
        try {
            return new DecodedValue(Long.parseLong(text));
        } catch (NumberFormatException overflow) {
            // 19 digits pass the regex but can exceed Long.MAX_VALUE; overflow is malformed.
            return new DecodedError(headerName + ": exceeds signed 64-bit range: \"" + text + "\"");
        }
    }

    /**
     * Overflow-safe epoch-millis addition: saturates at the {@code long} range instead of wrapping
     * (design §2.3 — a wrapped sum would turn "absurdly far future" into "ancient past" and relay
     * a record someone intended to delay).
     */
    public static long saturatedAddMillis(long a, long b) {
        long sum = a + b;
        // Overflow iff both operands share a sign that differs from the sum's (Math.addExact logic).
        if (((a ^ sum) & (b ^ sum)) < 0) {
            return sum < 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
        }
        return sum;
    }

    private static int countValues(Headers headers, String key) {
        int n = 0;
        for (Header ignored : headers.headers(key)) {
            n++;
        }
        return n;
    }

    /** Renders an offending value for diagnostics: printable ASCII kept, rest masked, truncated. */
    private static String sanitize(String text) {
        StringBuilder sb = new StringBuilder(Math.min(text.length(), MAX_DETAIL_VALUE_CHARS + 1));
        for (int i = 0; i < text.length() && sb.length() < MAX_DETAIL_VALUE_CHARS; i++) {
            char c = text.charAt(i);
            sb.append(c >= 0x20 && c < 0x7f ? c : '?');
        }
        if (text.length() > MAX_DETAIL_VALUE_CHARS) {
            sb.append('…');
        }
        return sb.toString();
    }

    /** Internal decode result: a value, or a malformed-value diagnostic. */
    private sealed interface Decoded {}

    private record DecodedValue(long value) implements Decoded {}

    private record DecodedError(String detail) implements Decoded {}
}
