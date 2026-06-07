package com.jucius.cesium.kafka.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.store.ConfigView;
import java.time.Duration;
import java.util.Map;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class FakeStoreContextTest {

    @Test
    void mintsAFreshRouteIdentityPerBuild() {
        FakeStoreContext a = FakeStoreContext.builder().partitions(3).build();
        FakeStoreContext b = FakeStoreContext.builder().partitions(3).build();
        assertEquals(3, a.route().partitionCount());
        assertNotEquals(a.route().sourceTopicId(), b.route().sourceTopicId());
        assertNotEquals(a.route().trackerTopicId(), b.route().trackerTopicId());
    }

    @Test
    void reusesAnExplicitRoute() {
        FakeStoreContext a = FakeStoreContext.builder().partitions(2).build();
        FakeStoreContext b = FakeStoreContext.builder().route(a.route()).build();
        assertSame(a.route(), b.route(), "same identity models a restart of the same route");
    }

    @Test
    void clockIsTheMutableClock() {
        MutableClock clock = MutableClock.at(123);
        FakeStoreContext ctx = FakeStoreContext.builder().clock(clock).build();
        assertSame(clock, ctx.clock());
        assertSame(clock, ctx.mutableClock());
        clock.advanceMillis(1);
        assertEquals(124, ctx.clock().millis());
    }

    @Test
    void epochIsStable() {
        FakeStoreContext ctx = FakeStoreContext.builder().build();
        assertEquals(1, ctx.epoch(0).groupGenerationId());
        assertEquals("testkit-member", ctx.epoch(7).memberId());
    }

    @Test
    void configViewFollowsEngineParsingConventions() {
        ConfigView config = new FakeStoreContext.MapConfigView(Map.of(
                "an.int", "42",
                "a.long", "9000000000",
                "a.bool", "true",
                "a.duration", "PT30S",
                "bad.int", "forty-two",
                "bad.bool", "yes"));
        assertEquals(42, config.getInt("an.int"));
        assertEquals(42, config.getInt("an.int", 7));
        assertEquals(7, config.getInt("absent", 7));
        assertEquals(9_000_000_000L, config.getLong("a.long"));
        assertTrue(config.getBoolean("a.bool"));
        assertEquals(Duration.ofSeconds(30), config.getDuration("a.duration"));
        assertEquals(Duration.ofDays(1), config.getDuration("absent", Duration.ofDays(1)));
        assertEquals("fallback", config.getString("absent", "fallback"));
        assertEquals(6, config.keys().size());

        assertThrows(NoSuchElementException.class, () -> config.getString("absent"));
        assertThrows(NoSuchElementException.class, () -> config.getInt("absent"));
        assertThrows(IllegalArgumentException.class, () -> config.getInt("bad.int"));
        assertThrows(IllegalArgumentException.class, () -> config.getBoolean("bad.bool"));
        assertThrows(IllegalArgumentException.class, () -> config.getDuration("bad.int"));
    }

    @Test
    void propertiesMergeIntoTheConfigView() {
        FakeStoreContext ctx = FakeStoreContext.builder()
                .properties(Map.of("a", "1", "b", "2"))
                .property("b", "3")
                .build();
        assertEquals(1, ctx.config().getInt("a"));
        assertEquals(3, ctx.config().getInt("b"), "later puts win per key");
    }
}
