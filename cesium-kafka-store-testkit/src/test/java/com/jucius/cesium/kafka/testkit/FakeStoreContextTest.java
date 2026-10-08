package com.jucius.cesium.kafka.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.store.ConfigView;
import java.util.Map;
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
    void configParsesLikeTheEngine() {
        ConfigView config = FakeStoreContext.builder()
                .property("flag", " TRUE ")
                .property("bad", "secret-x")
                .build()
                .config();
        assertTrue(config.getBoolean("flag"), "trimmed, case-insensitive boolean");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> config.getInt("bad"));
        assertFalse(e.getMessage().contains("secret-x"), "a parse error never echoes the value");
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
