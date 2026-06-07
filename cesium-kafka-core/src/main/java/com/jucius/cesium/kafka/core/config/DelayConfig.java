package com.jucius.cesium.kafka.core.config;

import com.jucius.cesium.kafka.core.policy.MalformedHeaderPolicy;
import com.jucius.cesium.kafka.core.policy.OverMaxPolicy;
import java.time.Duration;
import java.util.Objects;

/**
 * Delay-protocol limits and violation policies (design §2.3, §8). The policy components are the
 * same enum types the {@link com.jucius.cesium.kafka.core.policy.IngestPolicyEngine} consumes — one
 * type per concept, so config wires into the engine without translation.
 *
 * @param max maximum accepted delay; default {@code P1D} (lowered from P7D, R10): it drives the
 *     tombstone-retention floor and tracker disk worksheet — raising it is an explicit,
 *     worksheet-reviewed decision
 * @param onOverMax policy for delays beyond {@code max}; default DLQ (D3): delivering early a
 *     message someone intended to delay is a business hazard, so the violation is made explicit
 *     while the pipeline stays alive
 * @param onMalformedHeader policy for unparseable control headers; default DLQ (D3)
 */
public record DelayConfig(Duration max, OverMaxPolicy onOverMax, MalformedHeaderPolicy onMalformedHeader) {

    /** Default maximum delay ({@code P1D}, §8 defaults table). */
    public static final Duration DEFAULT_MAX = Duration.ofDays(1);

    /** Materializes the §8 defaults for absent components. */
    public DelayConfig {
        max = Objects.requireNonNullElse(max, DEFAULT_MAX);
        onOverMax = Objects.requireNonNullElse(onOverMax, OverMaxPolicy.DLQ);
        onMalformedHeader = Objects.requireNonNullElse(onMalformedHeader, MalformedHeaderPolicy.DLQ);
    }

    /** Returns a {@code DelayConfig} populated entirely from the §8 defaults table. */
    public static DelayConfig defaults() {
        return new DelayConfig(DEFAULT_MAX, OverMaxPolicy.DLQ, MalformedHeaderPolicy.DLQ);
    }
}
