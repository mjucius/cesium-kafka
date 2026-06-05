package events.cesium.kafka.core.config;

/**
 * Environment facts the validator needs but the config file cannot know (design §5.3).
 *
 * <p>Injectable so heap-budget tests are deterministic ({@link #runtime(int)} is the production
 * path) and so the engine can re-validate with the <em>actual</em> assigned partition count once
 * topic metadata is available — at config-load time the app only has an estimate.
 *
 * @param maxHeapBytes the JVM heap ceiling; production uses
 *     {@code Runtime.getRuntime().maxMemory()}
 * @param assignedPartitionEstimate worst-case number of tracker partitions this instance may own;
 *     multiplied by {@code dispatch.max-pending-per-partition} for the footprint check
 */
public record ValidationContext(long maxHeapBytes, int assignedPartitionEstimate) {

    /** Validates that both facts are positive. */
    public ValidationContext {
        if (maxHeapBytes <= 0) {
            throw new IllegalArgumentException("maxHeapBytes must be positive, got " + maxHeapBytes);
        }
        if (assignedPartitionEstimate < 1) {
            throw new IllegalArgumentException(
                    "assignedPartitionEstimate must be at least 1, got " + assignedPartitionEstimate);
        }
    }

    /** A context using the live JVM's {@code Runtime.getRuntime().maxMemory()}. */
    public static ValidationContext runtime(int assignedPartitionEstimate) {
        return new ValidationContext(Runtime.getRuntime().maxMemory(), assignedPartitionEstimate);
    }
}
