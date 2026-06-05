package events.cesium.kafka.core.headers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.api.headers.CesiumHeaders;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;
import net.jqwik.api.constraints.Size;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

/** jqwik properties for the header codec (design §11.1). */
class DelayHeaderCodecProperties {

    private static final long NOW = 1_700_000_000_000L;
    private static final long TS = NOW - 5_000;
    private static final long MAX_MS = Duration.ofDays(1).toMillis();

    private static Headers single(String key, byte[] value) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(key, value);
        return headers;
    }

    private static byte[] ascii(long value) {
        return Long.toString(value).getBytes(StandardCharsets.US_ASCII);
    }

    /** Every positive long survives the ASCII deliver-at round trip exactly. */
    @Property
    void randomAsciiDeliverAtRoundTrips(@ForAll @LongRange(min = 1, max = Long.MAX_VALUE) long deliverAtMs) {
        DelayHeaderCodec codec = new DelayHeaderCodec(Duration.ofMillis(Long.MAX_VALUE), false);
        // now = 0: the limit saturates to Long.MAX_VALUE, so every positive instant is in range
        // and strictly future — the result must be Schedule(deliverAtMs) bit-for-bit.
        HeaderParseResult result = codec.parse(
                single(CesiumHeaders.DELIVER_AT, ascii(deliverAtMs)), -1, TimestampType.NO_TIMESTAMP_TYPE, 0);
        assertEquals(new HeaderParseResult.Schedule(deliverAtMs, false), result);
    }

    /** Every in-range delay lands exactly base + delay (or relays when already due). */
    @Property
    void randomAsciiDelaysScheduleRelativeToBase(@ForAll @LongRange(min = 0, max = 86_400_000L) long delayMs) {
        DelayHeaderCodec codec = new DelayHeaderCodec(Duration.ofDays(1), false);
        HeaderParseResult result =
                codec.parse(single(CesiumHeaders.DELAY_MS, ascii(delayMs)), TS, TimestampType.CREATE_TIME, NOW);
        long expectedDispatchAt = TS + delayMs;
        if (expectedDispatchAt <= NOW) {
            assertEquals(new HeaderParseResult.RelayImmediately(false), result);
        } else {
            assertEquals(new HeaderParseResult.Schedule(expectedDispatchAt, false), result);
        }
    }

    /** The codec classifies arbitrary bytes in either header and mode; it never throws. */
    @Property
    void arbitraryBytesNeverCrashTheCodec(
            @ForAll @Size(max = 64) byte[] value, @ForAll boolean binaryMode, @ForAll boolean asDeliverAt) {
        DelayHeaderCodec codec = new DelayHeaderCodec(Duration.ofDays(1), binaryMode);
        String header = asDeliverAt ? CesiumHeaders.DELIVER_AT : CesiumHeaders.DELAY_MS;
        assertNotNull(codec.parse(single(header, value), TS, TimestampType.CREATE_TIME, NOW));
    }

    /** Even both headers populated with arbitrary bytes never crash. */
    @Property
    void arbitraryBytePairsNeverCrashTheCodec(
            @ForAll @Size(max = 16) byte[] delayValue,
            @ForAll @Size(max = 16) byte[] deliverAtValue,
            @ForAll boolean binaryMode) {
        DelayHeaderCodec codec = new DelayHeaderCodec(Duration.ofDays(1), binaryMode);
        RecordHeaders headers = new RecordHeaders();
        headers.add(CesiumHeaders.DELAY_MS, delayValue);
        headers.add(CesiumHeaders.DELIVER_AT, deliverAtValue);
        HeaderParseResult result = codec.parse(headers, TS, TimestampType.CREATE_TIME, NOW);
        assertNotNull(result);
        assertTrue(result.conflicted(), "both headers present must always flag a conflict");
    }

    /**
     * Precedence law: with both headers present the outcome equals parsing deliver-at alone —
     * whatever the delay-ms value, valid or garbage — except the conflicted flag.
     */
    @Property
    void deliverAtPrecedenceLaw(
            @ForAll long deliverAtMs, @ForAll @Size(max = 24) byte[] delayValue, @ForAll boolean binaryMode) {
        DelayHeaderCodec codec = new DelayHeaderCodec(Duration.ofDays(1), binaryMode);
        byte[] deliverAtBytes = binaryMode
                ? java.nio.ByteBuffer.allocate(8).putLong(deliverAtMs).array()
                : ascii(deliverAtMs);
        RecordHeaders both = new RecordHeaders();
        both.add(CesiumHeaders.DELAY_MS, delayValue);
        both.add(CesiumHeaders.DELIVER_AT, deliverAtBytes);
        HeaderParseResult withBoth = codec.parse(both, TS, TimestampType.CREATE_TIME, NOW);
        HeaderParseResult deliverAtAlone =
                codec.parse(single(CesiumHeaders.DELIVER_AT, deliverAtBytes), TS, TimestampType.CREATE_TIME, NOW);
        assertEquals(canonical(deliverAtAlone), canonical(withBoth));
        assertTrue(withBoth.conflicted());
    }

    /** Dispatch-instant arithmetic saturates: the result never wraps below the base. */
    @Property
    void overflowSaturatesInsteadOfWrapping(
            @ForAll @LongRange(min = 0, max = Long.MAX_VALUE) long baseMs,
            @ForAll @LongRange(min = 0, max = Long.MAX_VALUE) long delayMs) {
        // delay.max = Long.MAX ms: nothing is over-max, isolating the addition itself.
        DelayHeaderCodec codec = new DelayHeaderCodec(Duration.ofMillis(Long.MAX_VALUE), false);
        // now = -1 < every saturated sum of non-negatives, so the result is always Schedule.
        HeaderParseResult result =
                codec.parse(single(CesiumHeaders.DELAY_MS, ascii(delayMs)), baseMs, TimestampType.CREATE_TIME, -1);
        HeaderParseResult.Schedule schedule = (HeaderParseResult.Schedule) result;
        long expected = BigInteger.valueOf(baseMs)
                .add(BigInteger.valueOf(delayMs))
                .min(BigInteger.valueOf(Long.MAX_VALUE))
                .longValueExact();
        assertEquals(expected, schedule.dispatchAtMs());
        assertTrue(schedule.dispatchAtMs() >= baseMs, "saturation must never move the instant before its base");
    }

    /** saturatedAddMillis equals BigInteger addition clamped to the long range, for all inputs. */
    @Property
    void saturatedAddMatchesBigIntegerClamp(@ForAll long a, @ForAll long b) {
        BigInteger exact = BigInteger.valueOf(a).add(BigInteger.valueOf(b));
        BigInteger clamped = exact.max(BigInteger.valueOf(Long.MIN_VALUE)).min(BigInteger.valueOf(Long.MAX_VALUE));
        assertEquals(clamped.longValueExact(), DelayHeaderCodec.saturatedAddMillis(a, b));
    }

    /** Valid schedules never exceed now + delay.max — the clamping/range law. */
    @Property
    void schedulesNeverExceedTheLimit(@ForAll long deliverAtMs, @ForAll @LongRange(min = 0) long nowMs) {
        DelayHeaderCodec codec = new DelayHeaderCodec(Duration.ofDays(1), false);
        byte[] value = deliverAtMs >= 0 ? ascii(deliverAtMs) : "x".getBytes(StandardCharsets.US_ASCII);
        HeaderParseResult result =
                codec.parse(single(CesiumHeaders.DELIVER_AT, value), TS, TimestampType.CREATE_TIME, nowMs);
        if (result instanceof HeaderParseResult.Schedule(long dispatchAtMs, boolean ignored)) {
            assertTrue(dispatchAtMs > nowMs, "scheduled instants are strictly future");
            assertTrue(
                    dispatchAtMs <= DelayHeaderCodec.saturatedAddMillis(nowMs, MAX_MS),
                    "scheduled instants never exceed now + delay.max");
        }
    }

    private static String canonical(HeaderParseResult result) {
        return switch (result) {
            case HeaderParseResult.NoDelay ignored -> "no-delay";
            case HeaderParseResult.RelayImmediately ignored -> "relay-immediately";
            case HeaderParseResult.Schedule s -> "schedule:" + s.dispatchAtMs();
            case HeaderParseResult.Malformed m -> "malformed:" + m.detail();
            case HeaderParseResult.OverMax o -> "over-max:" + o.requestedDispatchAtMs();
        };
    }
}
