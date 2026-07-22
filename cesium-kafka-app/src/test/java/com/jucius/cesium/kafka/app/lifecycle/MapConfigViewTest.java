package com.jucius.cesium.kafka.app.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.store.ConfigView;
import java.time.Duration;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** The production {@link MapConfigView}: defaulting vs required access, typed parsing, error shapes. */
class MapConfigViewTest {

    private final ConfigView view = new MapConfigView(Map.of(
            "name", "kafka",
            "count", "7",
            "size", "9000000000",
            "window", "PT30S",
            "enabled", "true",
            "bad-int", "not-a-number"));

    @Test
    void defaultedAccessorsReturnDefaultsForAbsentKeys() {
        assertEquals("fallback", view.getString("missing", "fallback"));
        assertEquals(42, view.getInt("missing", 42));
        assertEquals(42L, view.getLong("missing", 42L));
        assertEquals(Duration.ofSeconds(5), view.getDuration("missing", Duration.ofSeconds(5)));
        assertTrue(view.getBoolean("missing", true));
        assertFalse(view.getBoolean("missing", false));
    }

    @Test
    void typedAccessorsParsePresentValues() {
        assertEquals("kafka", view.getString("name"));
        assertEquals(7, view.getInt("count"));
        assertEquals(9_000_000_000L, view.getLong("size"));
        assertEquals(Duration.ofSeconds(30), view.getDuration("window"));
        assertTrue(view.getBoolean("enabled"));
    }

    @Test
    void requiredAccessorsThrowNoSuchElementWhenAbsent() {
        assertThrows(NoSuchElementException.class, () -> view.getString("missing"));
        assertThrows(NoSuchElementException.class, () -> view.getInt("missing"));
        assertThrows(NoSuchElementException.class, () -> view.getDuration("missing"));
    }

    @Test
    void unparseableValuesThrowIllegalArgumentNamingTheKey() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> view.getInt("bad-int"));
        assertTrue(e.getMessage().contains("store.properties.bad-int"), e::getMessage);
        assertThrows(IllegalArgumentException.class, () -> view.getDuration("name"));
        assertThrows(IllegalArgumentException.class, () -> view.getBoolean("name"));
    }

    @Test
    void unparseableValueIsNeverEchoedIntoTheMessage() {
        // VULN-012: a store.properties.* value may be a secret and the exception reaches log.error, so
        // the type-mismatch message must name the key + expected type but never the value itself.
        ConfigView secretView = new MapConfigView(Map.of("token-count", "s3cr3t-not-a-number"));

        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> secretView.getInt("token-count"));

        assertTrue(e.getMessage().contains("store.properties.token-count"), e::getMessage);
        assertTrue(e.getMessage().contains("int"), e::getMessage);
        assertFalse(e.getMessage().contains("s3cr3t"), () -> "value leaked into message: " + e.getMessage());
    }

    @Test
    void keysExposesEveryPresentKey() {
        assertEquals(Set.of("name", "count", "size", "window", "enabled", "bad-int"), view.keys());
    }
}
