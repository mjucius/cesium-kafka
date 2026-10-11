package com.jucius.cesium.kafka.core.config;

/**
 * Environment facts the validator needs but the config file cannot know (design §5.3).
 *
 * <p>Injectable so tests are deterministic ({@link #runtime()} is the production path).
 *
 * @param maxHeapBytes the JVM heap ceiling; production uses
 *     {@code Runtime.getRuntime().maxMemory()}
 */
public record ValidationContext(long maxHeapBytes) {

    /** Validates that the heap ceiling is positive. */
    public ValidationContext {
        if (maxHeapBytes <= 0) {
            throw new IllegalArgumentException("maxHeapBytes must be positive, got " + maxHeapBytes);
        }
    }

    /** A context using the live JVM's {@code Runtime.getRuntime().maxMemory()}. */
    public static ValidationContext runtime() {
        return new ValidationContext(Runtime.getRuntime().maxMemory());
    }
}
