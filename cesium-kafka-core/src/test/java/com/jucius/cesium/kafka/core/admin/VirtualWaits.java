package com.jucius.cesium.kafka.core.admin;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A virtual clock plus a {@link TopicVisibility.Waiter} that advances it instead of sleeping.
 *
 * <p>Lets the metadata-propagation tests assert the <em>exact</em> backoff schedule and prove the
 * budget expires, while running instantly and deterministically — a real {@code Thread.sleep} would
 * make the never-visible cases take {@link TopicVisibility#BUDGET} of wall-clock each.
 */
final class VirtualWaits {

    private final AtomicLong nowMillis = new AtomicLong();
    private final List<Long> waits = new ArrayList<>();

    /** The clock to hand {@code TopicVisibility}; reads the virtual now. */
    Clock clock() {
        return new Clock() {
            @Override
            public ZoneId getZone() {
                return ZoneId.of("UTC");
            }

            @Override
            public Clock withZone(ZoneId zone) {
                return this;
            }

            @Override
            public Instant instant() {
                return Instant.ofEpochMilli(nowMillis.get());
            }

            @Override
            public long millis() {
                return nowMillis.get();
            }
        };
    }

    /** The waiter to hand {@code TopicVisibility}; records the wait and advances the clock. */
    TopicVisibility.Waiter waiter() {
        return millis -> {
            waits.add(millis);
            nowMillis.addAndGet(millis);
        };
    }

    /** Every wait requested so far, in order — the backoff schedule under test. */
    List<Long> waits() {
        return List.copyOf(waits);
    }

    /** Total virtual time elapsed. */
    long elapsedMillis() {
        return nowMillis.get();
    }
}
