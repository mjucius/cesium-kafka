package com.jucius.cesium.kafka.core.config;

import java.util.AbstractMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * An immutable {@code Map<String, String>} of Kafka client / store passthrough properties whose
 * {@link #toString()} masks secret-bearing <em>values</em> (L6). The config records hold these
 * property maps verbatim, and the compiler-generated record {@code toString} prints every value
 * recursively — so a stray {@code log.debug("config {}", config)} or an exception interpolation
 * would otherwise disclose a SASL/TLS credential. Wrapping the maps here makes redaction automatic
 * everywhere the map (or its enclosing record) is rendered, with no behavioral change for lookups:
 * {@link #get}/{@link #entrySet} return the real values, only {@code toString} masks them.
 *
 * <p>Immutability and null-rejection mirror the {@link Map#copyOf} the records previously used
 * (mutators throw {@link UnsupportedOperationException} via {@link AbstractMap}); {@link #equals}
 * and {@link #hashCode} follow the {@link Map} contract, so a {@code SecretMap} compares equal to a
 * plain map with the same entries.
 */
public final class SecretMap extends AbstractMap<String, String> {

    /** The placeholder a secret value is rendered as in {@link #toString()}. */
    public static final String REDACTED = "[redacted]";

    private final Map<String, String> delegate;

    private SecretMap(Map<String, String> delegate) {
        this.delegate = delegate;
    }

    /**
     * Wraps a defensive, unmodifiable copy of {@code source} ({@link Map#copyOf}, so null keys /
     * values are rejected exactly as before).
     */
    public static SecretMap copyOf(Map<String, String> source) {
        return new SecretMap(Map.copyOf(source));
    }

    @Override
    public Set<Map.Entry<String, String>> entrySet() {
        return delegate.entrySet();
    }

    /**
     * Renders like the standard map {@code toString} but every {@linkplain #isSecretKey
     * secret-bearing} value is replaced with {@value #REDACTED}.
     */
    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(", ", "{", "}");
        for (Map.Entry<String, String> entry : delegate.entrySet()) {
            String key = entry.getKey();
            joiner.add(key + "=" + (isSecretKey(key) ? REDACTED : entry.getValue()));
        }
        return joiner.toString();
    }

    /**
     * Whether a property key names a secret-bearing value, per the L6 denylist (case-insensitive):
     * {@code *.password} (covers {@code ssl.*.password}), {@code sasl.jaas.config}, {@code *.key},
     * {@code *.secret}, and {@code *.credentials}.
     */
    static boolean isSecretKey(String key) {
        String k = key.toLowerCase(Locale.ROOT);
        return k.equals("sasl.jaas.config")
                || k.endsWith(".password")
                || k.endsWith(".key")
                || k.endsWith(".secret")
                || k.endsWith(".credentials");
    }
}
