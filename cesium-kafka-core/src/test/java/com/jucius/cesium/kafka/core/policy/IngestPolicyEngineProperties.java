package com.jucius.cesium.kafka.core.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.core.headers.DelayHeaderCodec;
import com.jucius.cesium.kafka.core.headers.HeaderParseResult;
import java.time.Duration;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;

/** jqwik laws for the policy engine (design §11.1): clamping and determinism. */
class IngestPolicyEngineProperties {

    /**
     * Clamping law: for any clock reading and any non-negative {@code delay.max}, CLAMP pins to
     * exactly {@code now + delay.max}, saturated — never before now, never wrapped.
     */
    @Property
    void clampAlwaysPinsToNowPlusMaxSaturated(
            @ForAll long nowMs, @ForAll @LongRange(min = 0, max = Long.MAX_VALUE) long delayMaxMs) {
        IngestPolicyEngine engine =
                new IngestPolicyEngine(Duration.ofMillis(delayMaxMs), MalformedHeaderPolicy.DLQ, OverMaxPolicy.CLAMP);
        HeaderParseResult.OverMax overMax = new HeaderParseResult.OverMax(Long.MAX_VALUE, "detail", false);
        IngestDecision decision = engine.decide(overMax, nowMs);
        IngestDecision.Clamp clamp = (IngestDecision.Clamp) decision;
        assertEquals(DelayHeaderCodec.saturatedAddMillis(nowMs, delayMaxMs), clamp.dispatchAtMs());
        assertTrue(clamp.dispatchAtMs() >= nowMs, "a clamp target can never precede the clock");
    }

    /** The engine is a pure function: identical inputs decide identically, every time. */
    @Property
    void decisionsAreDeterministic(@ForAll long nowMs, @ForAll @LongRange(min = 0) long dispatchAtMs) {
        IngestPolicyEngine engine =
                new IngestPolicyEngine(Duration.ofDays(1), MalformedHeaderPolicy.DLQ, OverMaxPolicy.DLQ);
        HeaderParseResult schedule = new HeaderParseResult.Schedule(dispatchAtMs, false);
        assertEquals(engine.decide(schedule, nowMs), engine.decide(schedule, nowMs));
        assertEquals(new IngestDecision.Schedule(dispatchAtMs), engine.decide(schedule, nowMs));
    }
}
