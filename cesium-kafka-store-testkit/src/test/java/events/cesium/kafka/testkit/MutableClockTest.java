package events.cesium.kafka.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class MutableClockTest {

    @Test
    void advancesAndSetsDeterministically() {
        MutableClock clock = MutableClock.at(1_000);
        assertEquals(1_000, clock.millis());
        assertEquals(Instant.ofEpochMilli(1_000), clock.instant());

        clock.advanceMillis(500);
        assertEquals(1_500, clock.millis());

        clock.advance(Duration.ofSeconds(2));
        assertEquals(3_500, clock.millis());

        clock.advance(Duration.ofMillis(-100)); // skew tests move backwards
        assertEquals(3_400, clock.millis());

        clock.setMillis(42);
        assertEquals(42, clock.millis());
    }

    @Test
    void fromInstant() {
        assertEquals(7_777, MutableClock.at(Instant.ofEpochMilli(7_777)).millis());
    }

    @Test
    void withZoneSharesTheInstantSource() {
        MutableClock utc = MutableClock.at(0);
        assertEquals(ZoneOffset.UTC, utc.getZone());
        assertSame(utc, utc.withZone(ZoneOffset.UTC), "same zone: same instance");

        var berlin = utc.withZone(ZoneId.of("Europe/Berlin"));
        assertNotSame(utc, berlin);
        utc.advanceMillis(123);
        assertEquals(123, berlin.millis(), "the zoned view shares the mutable instant");
        assertEquals(ZoneId.of("Europe/Berlin"), berlin.getZone());
    }
}
