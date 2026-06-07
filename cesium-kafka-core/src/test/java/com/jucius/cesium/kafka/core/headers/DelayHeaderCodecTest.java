package com.jucius.cesium.kafka.core.headers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.headers.CesiumHeaders;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Design §2.3 / §11.1: parse, precedence, regex, range, timestamp base, ASCII vs binary mode. */
class DelayHeaderCodecTest {

    private static final long NOW = 1_700_000_000_000L;
    private static final long TS = NOW - 5_000;
    private static final Duration MAX = Duration.ofDays(1);
    private static final long MAX_MS = MAX.toMillis();

    private final DelayHeaderCodec ascii = new DelayHeaderCodec(MAX, false);
    private final DelayHeaderCodec binary = new DelayHeaderCodec(MAX, true);

    // --- helpers --------------------------------------------------------------------------------

    private static Headers headers(Object... keyValuePairs) {
        RecordHeaders headers = new RecordHeaders();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            headers.add((String) keyValuePairs[i], (byte[]) keyValuePairs[i + 1]);
        }
        return headers;
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] binary(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    private HeaderParseResult parse(Headers headers) {
        return ascii.parse(headers, TS, TimestampType.CREATE_TIME, NOW);
    }

    // --- no headers -----------------------------------------------------------------------------

    @Test
    void noHeaders_isNoDelay() {
        HeaderParseResult result = parse(headers());
        assertEquals(HeaderParseResult.NoDelay.INSTANCE, result);
        assertFalse(result.conflicted());
    }

    @Test
    void unrelatedHeaders_areNoDelay() {
        HeaderParseResult result = parse(headers("app-header", ascii("x"), "cesium-custom", ascii("y")));
        assertEquals(HeaderParseResult.NoDelay.INSTANCE, result);
    }

    // --- delay-ms: base and range (D2, §2.3) ----------------------------------------------------

    @Test
    void delayMs_isRelativeToSourceCreateTime() {
        HeaderParseResult result = parse(headers(CesiumHeaders.DELAY_MS, ascii("60000")));
        assertEquals(new HeaderParseResult.Schedule(TS + 60_000, false), result);
    }

    @Test
    void delayMs_logAppendTimeTimestampIsAlsoABase() {
        HeaderParseResult result =
                ascii.parse(headers(CesiumHeaders.DELAY_MS, ascii("60000")), TS, TimestampType.LOG_APPEND_TIME, NOW);
        assertEquals(new HeaderParseResult.Schedule(TS + 60_000, false), result);
    }

    @Test
    void delayMs_noTimestampType_fallsBackToIngestClock() {
        HeaderParseResult result =
                ascii.parse(headers(CesiumHeaders.DELAY_MS, ascii("60000")), -1, TimestampType.NO_TIMESTAMP_TYPE, NOW);
        assertEquals(new HeaderParseResult.Schedule(NOW + 60_000, false), result);
    }

    @Test
    void delayMs_negativeTimestamp_fallsBackToIngestClock() {
        HeaderParseResult result =
                ascii.parse(headers(CesiumHeaders.DELAY_MS, ascii("60000")), -1, TimestampType.CREATE_TIME, NOW);
        assertEquals(new HeaderParseResult.Schedule(NOW + 60_000, false), result);
    }

    @Test
    void delayMs_deterministicAcrossRetries_whenTimestampPresent() {
        // D2 rationale: an aborted ingest transaction is retried later; the record timestamp gives
        // the identical dispatch instant on every attempt regardless of the ingest clock.
        Headers headers = headers(CesiumHeaders.DELAY_MS, ascii("60000"));
        HeaderParseResult first = ascii.parse(headers, TS, TimestampType.CREATE_TIME, NOW);
        HeaderParseResult retry = ascii.parse(headers, TS, TimestampType.CREATE_TIME, NOW + 12_345);
        assertEquals(first, retry);
        assertEquals(new HeaderParseResult.Schedule(TS + 60_000, false), retry);
    }

    @Test
    void delayMs_zero_relaysImmediately_pastDue() {
        HeaderParseResult result = parse(headers(CesiumHeaders.DELAY_MS, ascii("0")));
        HeaderParseResult.RelayImmediately relay = assertInstanceOf(HeaderParseResult.RelayImmediately.class, result);
        assertEquals("past_due", relay.reason());
        assertFalse(relay.conflicted());
    }

    @Test
    void delayMs_landingInThePast_relaysImmediately() {
        // TS is 5 s before NOW; a 1 s delay is already due.
        HeaderParseResult result = parse(headers(CesiumHeaders.DELAY_MS, ascii("1000")));
        assertInstanceOf(HeaderParseResult.RelayImmediately.class, result);
    }

    @Test
    void delayMs_landingExactlyNow_relaysImmediately() {
        HeaderParseResult result = parse(headers(CesiumHeaders.DELAY_MS, ascii("5000")));
        assertInstanceOf(HeaderParseResult.RelayImmediately.class, result);
    }

    @Test
    void delayMs_exactlyMax_schedules_inclusiveBound() {
        HeaderParseResult result = ascii.parse(
                headers(CesiumHeaders.DELAY_MS, ascii(Long.toString(MAX_MS))), NOW, TimestampType.CREATE_TIME, NOW);
        assertEquals(new HeaderParseResult.Schedule(NOW + MAX_MS, false), result);
    }

    @Test
    void delayMs_overMax_isOverMax() {
        HeaderParseResult result = parse(headers(CesiumHeaders.DELAY_MS, ascii(Long.toString(MAX_MS + 1))));
        HeaderParseResult.OverMax overMax = assertInstanceOf(HeaderParseResult.OverMax.class, result);
        assertEquals(TS + MAX_MS + 1, overMax.requestedDispatchAtMs());
        assertTrue(overMax.detail().contains(CesiumHeaders.DELAY_MS));
    }

    @Test
    void delayMs_overMax_evenWhenRequestedInstantIsPast() {
        // An ancient record timestamp can land the over-max request in the past; the contract
        // violation (delay > delay.max) still wins over the past-due fast path.
        HeaderParseResult result = ascii.parse(
                headers(CesiumHeaders.DELAY_MS, ascii(Long.toString(MAX_MS + 1))), 0, TimestampType.CREATE_TIME, NOW);
        HeaderParseResult.OverMax overMax = assertInstanceOf(HeaderParseResult.OverMax.class, result);
        assertEquals(MAX_MS + 1, overMax.requestedDispatchAtMs());
    }

    @Test
    void delayMs_maxLongValue_overMaxWithSaturatedRequest() {
        // "9223372036854775807" passes the grammar; base + Long.MAX saturates instead of wrapping.
        HeaderParseResult result = parse(headers(CesiumHeaders.DELAY_MS, ascii("9223372036854775807")));
        HeaderParseResult.OverMax overMax = assertInstanceOf(HeaderParseResult.OverMax.class, result);
        assertEquals(Long.MAX_VALUE, overMax.requestedDispatchAtMs());
    }

    // --- deliver-at: range (§2.3) ----------------------------------------------------------------

    @Test
    void deliverAt_future_schedules() {
        HeaderParseResult result = parse(headers(CesiumHeaders.DELIVER_AT, ascii(Long.toString(NOW + 60_000))));
        assertEquals(new HeaderParseResult.Schedule(NOW + 60_000, false), result);
    }

    @Test
    void deliverAt_exactlyNow_relaysImmediately() {
        HeaderParseResult result = parse(headers(CesiumHeaders.DELIVER_AT, ascii(Long.toString(NOW))));
        assertInstanceOf(HeaderParseResult.RelayImmediately.class, result);
    }

    @Test
    void deliverAt_past_relaysImmediately_notAnError() {
        HeaderParseResult result = parse(headers(CesiumHeaders.DELIVER_AT, ascii(Long.toString(NOW - 60_000))));
        HeaderParseResult.RelayImmediately relay = assertInstanceOf(HeaderParseResult.RelayImmediately.class, result);
        assertEquals("past_due", relay.reason());
    }

    @Test
    void deliverAt_zero_relaysImmediately() {
        HeaderParseResult result = parse(headers(CesiumHeaders.DELIVER_AT, ascii("0")));
        assertInstanceOf(HeaderParseResult.RelayImmediately.class, result);
    }

    @Test
    void deliverAt_atLimit_schedules_inclusiveBound() {
        HeaderParseResult result = parse(headers(CesiumHeaders.DELIVER_AT, ascii(Long.toString(NOW + MAX_MS))));
        assertEquals(new HeaderParseResult.Schedule(NOW + MAX_MS, false), result);
    }

    @Test
    void deliverAt_overLimit_isOverMax() {
        HeaderParseResult result = parse(headers(CesiumHeaders.DELIVER_AT, ascii(Long.toString(NOW + MAX_MS + 1))));
        HeaderParseResult.OverMax overMax = assertInstanceOf(HeaderParseResult.OverMax.class, result);
        assertEquals(NOW + MAX_MS + 1, overMax.requestedDispatchAtMs());
        assertTrue(overMax.detail().contains(CesiumHeaders.DELIVER_AT));
    }

    // --- precedence & conflicts (§2.3) -----------------------------------------------------------

    @Test
    void bothHeaders_deliverAtWins_flaggedConflicted() {
        HeaderParseResult result = parse(headers(
                CesiumHeaders.DELAY_MS, ascii("60000"),
                CesiumHeaders.DELIVER_AT, ascii(Long.toString(NOW + 30_000))));
        assertEquals(new HeaderParseResult.Schedule(NOW + 30_000, true), result);
    }

    @Test
    void bothHeaders_deliverAtWinsEvenWhenMalformed() {
        // Precedence is resolved before validation: the losing header is never a fallback.
        HeaderParseResult result = parse(headers(
                CesiumHeaders.DELAY_MS, ascii("60000"),
                CesiumHeaders.DELIVER_AT, ascii("not-a-number")));
        HeaderParseResult.Malformed malformed = assertInstanceOf(HeaderParseResult.Malformed.class, result);
        assertTrue(malformed.conflicted());
        assertTrue(malformed.detail().contains(CesiumHeaders.DELIVER_AT));
    }

    @Test
    void bothHeaders_losingDelayMsIsNotValidated() {
        HeaderParseResult result = parse(headers(
                CesiumHeaders.DELAY_MS, ascii("garbage"),
                CesiumHeaders.DELIVER_AT, ascii(Long.toString(NOW + 30_000))));
        assertEquals(new HeaderParseResult.Schedule(NOW + 30_000, true), result);
    }

    @Test
    void repeatedDelayMs_lastValueWins_flaggedConflicted() {
        HeaderParseResult result = parse(headers(
                CesiumHeaders.DELAY_MS, ascii("60000"),
                CesiumHeaders.DELAY_MS, ascii("90000")));
        assertEquals(new HeaderParseResult.Schedule(TS + 90_000, true), result);
    }

    @Test
    void repeatedDeliverAt_lastValueWins_flaggedConflicted() {
        HeaderParseResult result = parse(headers(
                CesiumHeaders.DELIVER_AT, ascii(Long.toString(NOW + 10_000)),
                CesiumHeaders.DELIVER_AT, ascii(Long.toString(NOW + 20_000))));
        assertEquals(new HeaderParseResult.Schedule(NOW + 20_000, true), result);
    }

    // --- grammar: regex + overflow rejection (D1) ------------------------------------------------

    @ParameterizedTest
    @ValueSource(
            strings = {
                "",
                "abc",
                "-1",
                "+1",
                " 1",
                "1 ",
                "1.5",
                "0x10",
                "60_000",
                "12345678901234567890", // 20 digits: fails the {1,19} length bound
                "9999999999999999999", // 19 digits, passes the regex, overflows a signed long
                "9223372036854775808", // Long.MAX_VALUE + 1: overflow is malformed, never wrapped
                "٤٢" // non-ASCII digits: the grammar is [0-9] only
            })
    void malformedDelayMsValues(String value) {
        HeaderParseResult result = parse(headers(CesiumHeaders.DELAY_MS, value.getBytes(StandardCharsets.UTF_8)));
        HeaderParseResult.Malformed malformed = assertInstanceOf(HeaderParseResult.Malformed.class, result);
        assertTrue(malformed.detail().contains(CesiumHeaders.DELAY_MS));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "tomorrow", "-1700000000000", "9999999999999999999"})
    void malformedDeliverAtValues(String value) {
        HeaderParseResult result = parse(headers(CesiumHeaders.DELIVER_AT, value.getBytes(StandardCharsets.UTF_8)));
        HeaderParseResult.Malformed malformed = assertInstanceOf(HeaderParseResult.Malformed.class, result);
        assertTrue(malformed.detail().contains(CesiumHeaders.DELIVER_AT));
    }

    @Test
    void nullHeaderValue_isMalformed_bothModes() {
        Headers nullValue = headers(CesiumHeaders.DELAY_MS, (byte[]) null);
        assertInstanceOf(HeaderParseResult.Malformed.class, ascii.parse(nullValue, TS, TimestampType.CREATE_TIME, NOW));
        assertInstanceOf(
                HeaderParseResult.Malformed.class, binary.parse(nullValue, TS, TimestampType.CREATE_TIME, NOW));
    }

    // --- ASCII vs exclusive binary mode (D1) ------------------------------------------------------

    @Nested
    class BinaryMode {

        @Test
        void eightByteBigEndianLong_accepted() {
            HeaderParseResult result =
                    binary.parse(headers(CesiumHeaders.DELAY_MS, binary(60_000)), TS, TimestampType.CREATE_TIME, NOW);
            assertEquals(new HeaderParseResult.Schedule(TS + 60_000, false), result);
        }

        @Test
        void deliverAt_eightByteBigEndianLong_accepted() {
            HeaderParseResult result = binary.parse(
                    headers(CesiumHeaders.DELIVER_AT, binary(NOW + 60_000)), TS, TimestampType.CREATE_TIME, NOW);
            assertEquals(new HeaderParseResult.Schedule(NOW + 60_000, false), result);
        }

        @Test
        void asciiDecimal_rejected_modesAreExclusive() {
            HeaderParseResult result =
                    binary.parse(headers(CesiumHeaders.DELAY_MS, ascii("60000")), TS, TimestampType.CREATE_TIME, NOW);
            HeaderParseResult.Malformed malformed = assertInstanceOf(HeaderParseResult.Malformed.class, result);
            assertTrue(malformed.detail().contains("8 bytes"));
        }

        @Test
        void eightAsciiDigits_decodeAsBinary_theAmbiguityTheExclusivityExistsFor() {
            // "12345678" is 8 bytes: in binary mode it is the long 0x3132333435363738
            // (3,544,952,156,018,063,160 ms), not the number 12345678 — which is why the modes
            // can never be combined (D1).
            HeaderParseResult result = binary.parse(
                    headers(CesiumHeaders.DELAY_MS, ascii("12345678")), TS, TimestampType.CREATE_TIME, NOW);
            HeaderParseResult.OverMax overMax = assertInstanceOf(HeaderParseResult.OverMax.class, result);
            assertEquals(TS + 0x3132333435363738L, overMax.requestedDispatchAtMs());
        }

        @ParameterizedTest
        @ValueSource(ints = {0, 1, 7, 9, 16})
        void wrongLength_isMalformed(int length) {
            HeaderParseResult result =
                    binary.parse(headers(CesiumHeaders.DELAY_MS, new byte[length]), TS, TimestampType.CREATE_TIME, NOW);
            assertInstanceOf(HeaderParseResult.Malformed.class, result);
        }

        @Test
        void negativeBinaryDelay_isMalformed() {
            HeaderParseResult result =
                    binary.parse(headers(CesiumHeaders.DELAY_MS, binary(-1)), TS, TimestampType.CREATE_TIME, NOW);
            HeaderParseResult.Malformed malformed = assertInstanceOf(HeaderParseResult.Malformed.class, result);
            assertTrue(malformed.detail().contains("negative"));
        }

        @Test
        void negativeBinaryDeliverAt_isPastDueRelay_notAnError() {
            // deliver-at range is (−∞, now + delay.max]: a negative epoch instant is merely past.
            HeaderParseResult result =
                    binary.parse(headers(CesiumHeaders.DELIVER_AT, binary(-1_000)), TS, TimestampType.CREATE_TIME, NOW);
            assertInstanceOf(HeaderParseResult.RelayImmediately.class, result);
        }
    }

    @Test
    void asciiMode_eightByteBinary_rejected() {
        HeaderParseResult result =
                ascii.parse(headers(CesiumHeaders.DELAY_MS, binary(60_000)), TS, TimestampType.CREATE_TIME, NOW);
        assertInstanceOf(HeaderParseResult.Malformed.class, result);
    }

    // --- record overload & saturation helper -----------------------------------------------------

    @Test
    void consumerRecordOverload_usesRecordTimestampAndType() {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "source",
                3,
                17L,
                TS,
                TimestampType.CREATE_TIME,
                -1,
                -1,
                null,
                null,
                headers(CesiumHeaders.DELAY_MS, ascii("60000")),
                Optional.empty());
        assertEquals(new HeaderParseResult.Schedule(TS + 60_000, false), ascii.parse(record, NOW));
    }

    @Test
    void saturatedAddMillis_saturatesBothDirections() {
        assertEquals(Long.MAX_VALUE, DelayHeaderCodec.saturatedAddMillis(Long.MAX_VALUE, 1));
        assertEquals(Long.MAX_VALUE, DelayHeaderCodec.saturatedAddMillis(Long.MAX_VALUE, Long.MAX_VALUE));
        assertEquals(Long.MIN_VALUE, DelayHeaderCodec.saturatedAddMillis(Long.MIN_VALUE, -1));
        assertEquals(Long.MIN_VALUE, DelayHeaderCodec.saturatedAddMillis(Long.MIN_VALUE, Long.MIN_VALUE));
        assertEquals(12, DelayHeaderCodec.saturatedAddMillis(5, 7));
        assertEquals(-2, DelayHeaderCodec.saturatedAddMillis(-5, 3));
        assertEquals(-1, DelayHeaderCodec.saturatedAddMillis(Long.MIN_VALUE, Long.MAX_VALUE));
    }
}
