package events.cesium.kafka.app.lifecycle;

import events.cesium.kafka.api.store.ConfigView;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * The production {@link ConfigView} over the {@code store.properties} subtree (design §8) — the
 * counterpart the engine hands a store in its {@link events.cesium.kafka.api.store.StoreContext},
 * mirroring the published test kit's {@code FakeStoreContext.MapConfigView} but living in
 * production code so the app needs no test-only dependency.
 *
 * <p>Keys are relative to the subtree and values follow the application config conventions:
 * durations are ISO-8601, booleans are {@code true}/{@code false}. A present-but-unparseable value
 * always throws {@link IllegalArgumentException} naming the fully-qualified key (so a store surfaces
 * the mistake from {@code validate()} at startup), and a required-but-absent key throws
 * {@link NoSuchElementException}.
 */
public final class MapConfigView implements ConfigView {

    private final Map<String, String> properties;

    /** @param properties the {@code store.properties} subtree, copied defensively */
    public MapConfigView(Map<String, String> properties) {
        this.properties = Map.copyOf(properties);
    }

    @Override
    public String getString(String key, String defaultValue) {
        String value = properties.get(key);
        return value == null ? defaultValue : value;
    }

    @Override
    public String getString(String key) {
        return require(key);
    }

    @Override
    public int getInt(String key, int defaultValue) {
        String value = properties.get(key);
        return value == null ? defaultValue : parseInt(key, value);
    }

    @Override
    public int getInt(String key) {
        return parseInt(key, require(key));
    }

    @Override
    public long getLong(String key, long defaultValue) {
        String value = properties.get(key);
        return value == null ? defaultValue : parseLong(key, value);
    }

    @Override
    public long getLong(String key) {
        return parseLong(key, require(key));
    }

    @Override
    public Duration getDuration(String key, Duration defaultValue) {
        String value = properties.get(key);
        return value == null ? defaultValue : parseDuration(key, value);
    }

    @Override
    public Duration getDuration(String key) {
        return parseDuration(key, require(key));
    }

    @Override
    public boolean getBoolean(String key, boolean defaultValue) {
        String value = properties.get(key);
        return value == null ? defaultValue : parseBoolean(key, value);
    }

    @Override
    public boolean getBoolean(String key) {
        return parseBoolean(key, require(key));
    }

    @Override
    public Set<String> keys() {
        return properties.keySet();
    }

    private String require(String key) {
        String value = properties.get(key);
        if (value == null) {
            throw new NoSuchElementException("store.properties." + key + " is required but absent");
        }
        return value;
    }

    private static int parseInt(String key, String value) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("store.properties." + key + " must be an int, got '" + value + "'", e);
        }
    }

    private static long parseLong(String key, String value) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("store.properties." + key + " must be a long, got '" + value + "'", e);
        }
    }

    private static Duration parseDuration(String key, String value) {
        try {
            return Duration.parse(value.trim());
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    "store.properties." + key + " must be an ISO-8601 duration, got '" + value + "'", e);
        }
    }

    private static boolean parseBoolean(String key, String value) {
        String trimmed = value.trim();
        if (trimmed.equalsIgnoreCase("true")) {
            return true;
        }
        if (trimmed.equalsIgnoreCase("false")) {
            return false;
        }
        throw new IllegalArgumentException("store.properties." + key + " must be true or false, got '" + value + "'");
    }
}
