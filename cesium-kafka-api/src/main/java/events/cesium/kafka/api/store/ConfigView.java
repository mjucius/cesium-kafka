package events.cesium.kafka.api.store;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Minimal typed, read-only view over the store's configuration subtree ({@code store.properties}
 * in the application YAML, design §8).
 *
 * <p>Keys are the property names <em>relative</em> to the subtree (e.g.
 * {@code store.properties.hmac.key} is read as {@code getString("hmac.key", ...)}). Values follow
 * the application config conventions: durations are ISO-8601 ({@code PT30S}, {@code P1D}),
 * booleans are {@code true}/{@code false}.
 *
 * <p>Two access styles per type: a defaulting variant for optional keys, and a required variant
 * that throws {@link NoSuchElementException} when the key is absent. A present-but-unparseable
 * value always throws {@link IllegalArgumentException} naming the key — stores should surface
 * configuration mistakes from {@link SchedulerStore#validate()} at startup, never at dispatch
 * time.
 */
public interface ConfigView {

    /** Returns the value of {@code key}, or {@code defaultValue} if the key is absent. */
    String getString(String key, String defaultValue);

    /**
     * Returns the value of {@code key}.
     *
     * @throws NoSuchElementException if the key is absent
     */
    String getString(String key);

    /**
     * Returns the value of {@code key} as an {@code int}, or {@code defaultValue} if absent.
     *
     * @throws IllegalArgumentException if the value is present but not a valid {@code int}
     */
    int getInt(String key, int defaultValue);

    /**
     * Returns the value of {@code key} as an {@code int}.
     *
     * @throws NoSuchElementException if the key is absent
     * @throws IllegalArgumentException if the value is not a valid {@code int}
     */
    int getInt(String key);

    /**
     * Returns the value of {@code key} as a {@code long}, or {@code defaultValue} if absent.
     *
     * @throws IllegalArgumentException if the value is present but not a valid {@code long}
     */
    long getLong(String key, long defaultValue);

    /**
     * Returns the value of {@code key} as a {@code long}.
     *
     * @throws NoSuchElementException if the key is absent
     * @throws IllegalArgumentException if the value is not a valid {@code long}
     */
    long getLong(String key);

    /**
     * Returns the value of {@code key} as an ISO-8601 {@link Duration}, or {@code defaultValue} if
     * absent.
     *
     * @throws IllegalArgumentException if the value is present but not a valid ISO-8601 duration
     */
    Duration getDuration(String key, Duration defaultValue);

    /**
     * Returns the value of {@code key} as an ISO-8601 {@link Duration}.
     *
     * @throws NoSuchElementException if the key is absent
     * @throws IllegalArgumentException if the value is not a valid ISO-8601 duration
     */
    Duration getDuration(String key);

    /**
     * Returns the value of {@code key} as a {@code boolean}, or {@code defaultValue} if absent.
     *
     * @throws IllegalArgumentException if the value is present but neither {@code true} nor
     *     {@code false}
     */
    boolean getBoolean(String key, boolean defaultValue);

    /**
     * Returns the value of {@code key} as a {@code boolean}.
     *
     * @throws NoSuchElementException if the key is absent
     * @throws IllegalArgumentException if the value is neither {@code true} nor {@code false}
     */
    boolean getBoolean(String key);

    /**
     * All keys present in this view, relative to the subtree. Lets a store reject unknown keys —
     * typos in {@code store.properties} should be startup errors, matching the engine's own
     * unknown-key policy (design §8).
     */
    Set<String> keys();
}
