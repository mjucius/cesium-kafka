package events.cesium.kafka.core.config;

import java.util.Objects;

/**
 * The required stable deployment-slot identifier (design D10), e.g. a StatefulSet ordinal.
 *
 * <p>The instance id makes {@code transactional.id}s stable across restarts — so
 * {@code initTransactions()} immediately fences a predecessor's dangling transaction instead of
 * waiting out {@code transaction.timeout.ms} — and seeds the default {@code group.instance.id}
 * (static membership, design D21).
 *
 * <p>The literal {@value #RANDOM_LITERAL} (case-sensitive) is the documented explicit opt-in to
 * per-process random ids, trading crash-failover latency for convenience. It is a deliberate
 * decision, never a default — which is why this is a value object rather than a bare string:
 * callers ask {@link #isRandom()} instead of comparing magic literals.
 *
 * @param value the configured id; empty when the operator omitted the required key (the validator
 *     reports it)
 */
public record InstanceId(String value) {

    /** The literal value that explicitly opts in to non-stable, per-process random ids (D10). */
    public static final String RANDOM_LITERAL = "random";

    /** Materializes an absent value as empty so validation can aggregate the missing-field error. */
    public InstanceId {
        value = Objects.requireNonNullElse(value, "");
    }

    /** Returns an {@code InstanceId} wrapping {@code value}. */
    public static InstanceId of(String value) {
        return new InstanceId(value);
    }

    /** True when the operator explicitly opted in to random ids via the {@code random} literal. */
    public boolean isRandom() {
        return RANDOM_LITERAL.equals(value);
    }

    /** True when the required key was omitted or blank. */
    public boolean isBlank() {
        return value.isBlank();
    }
}
