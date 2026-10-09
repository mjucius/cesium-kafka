package com.jucius.cesium.kafka.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    void configParsesLikeTheEngine() {
        ConfigView config = FakeStoreContext.builder()
                .property("flag", " TRUE ")
                .property("bad", "secret-x")
                .build()
                .config();
        assertTrue(config.getBoolean("flag"), "trimmed, case-insensitive boolean");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> config.getInt("bad"));
        assertFalse(e.getMessage().contains("secret-x"), "a parse error never echoes the value");
        assertNull(e.getCause(), "a JDK parse exception would quote the value in a logged stack trace");
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
