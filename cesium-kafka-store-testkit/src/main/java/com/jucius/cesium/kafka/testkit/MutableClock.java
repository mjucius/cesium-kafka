package com.jucius.cesium.kafka.testkit;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link Clock} for contract tests: virtual time, advanced explicitly and deterministically.
 *
 * <p>{@code StoreContext.clock()} is injectable precisely so contract tests can drive time
 * (design §4.2); conforming stores take every time decision either from this clock or from the
 * {@code nowMs} arguments the engine derives from it, so tests never sleep.
 *
 * <p>{@link #withZone(ZoneId)} returns a view sharing the same mutable instant, matching the
 * {@link Clock} contract that the converted clock uses the same instant source.
 */
public final class MutableClock extends Clock {

    private final AtomicLong epochMilli;
    private final ZoneId zone;

    private MutableClock(AtomicLong epochMilli, ZoneId zone) {
        this.epochMilli = epochMilli;
        this.zone = zone;
    }

    /** A clock at {@code epochMilli}, UTC. */
    public static MutableClock at(long epochMilli) {
        return new MutableClock(new AtomicLong(epochMilli), ZoneOffset.UTC);
    }

    /** A clock at {@code instant}, UTC. */
    public static MutableClock at(Instant instant) {
        return at(instant.toEpochMilli());
    }

    /** Advances the clock; negative durations move it backwards (skew tests). */
    public MutableClock advance(Duration duration) {
        epochMilli.addAndGet(duration.toMillis());
        return this;
    }

    /** Advances the clock by {@code deltaMs} milliseconds. */
    public MutableClock advanceMillis(long deltaMs) {
        epochMilli.addAndGet(deltaMs);
        return this;
    }

    /** Sets the absolute time. */
    public MutableClock setMillis(long newEpochMilli) {
        epochMilli.set(newEpochMilli);
        return this;
    }

    @Override
    public long millis() {
        return epochMilli.get();
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(millis());
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId newZone) {
        return zone.equals(newZone) ? this : new MutableClock(epochMilli, newZone);
    }
}
