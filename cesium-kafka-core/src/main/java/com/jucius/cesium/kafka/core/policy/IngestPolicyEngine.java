package com.jucius.cesium.kafka.core.policy;

import com.jucius.cesium.kafka.core.headers.DelayHeaderCodec;
import com.jucius.cesium.kafka.core.headers.DlqReasons;
import com.jucius.cesium.kafka.core.headers.HeaderParseResult;
import java.time.Duration;
import java.util.Objects;

/**
 * Combines a {@link HeaderParseResult} with the configured ingest policies into the final
 * {@link IngestDecision} (design §2.3, D3).
 *
 * <p>Pure and deterministic: no clock is read internally ({@code now} is an argument, used only to
 * compute the CLAMP target {@code now + delay.max}), no I/O, no state — every
 * {@code (parse result, policy, now)} cell of the §11.1 decision table maps to exactly one action.
 * Conflict observability stays on the parse result ({@code conflicted()}); policies never change
 * an outcome because of a conflict.
 */
public final class IngestPolicyEngine {

    private final long delayMaxMs;
    private final MalformedHeaderPolicy onMalformedHeader;
    private final OverMaxPolicy onOverMax;

    /**
     * @param delayMax the configured {@code delay.max}; only used to compute the CLAMP target
     * @param onMalformedHeader {@code delay.on-malformed-header} (default DLQ, D3)
     * @param onOverMax {@code delay.on-over-max} (default DLQ, D3)
     */
    public IngestPolicyEngine(Duration delayMax, MalformedHeaderPolicy onMalformedHeader, OverMaxPolicy onOverMax) {
        Objects.requireNonNull(delayMax, "delayMax");
        if (delayMax.isNegative()) {
            throw new IllegalArgumentException("delay.max must be non-negative: " + delayMax);
        }
        long ms;
        try {
            ms = delayMax.toMillis();
        } catch (ArithmeticException overflow) {
            ms = Long.MAX_VALUE;
        }
        this.delayMaxMs = ms;
        this.onMalformedHeader = Objects.requireNonNull(onMalformedHeader, "onMalformedHeader");
        this.onOverMax = Objects.requireNonNull(onOverMax, "onOverMax");
    }

    /**
     * Decides the final ingest action for one record.
     *
     * @param parsed the codec's result for the record's control headers
     * @param nowMs the ingest clock reading, epoch milliseconds UTC (CLAMP target base)
     */
    public IngestDecision decide(HeaderParseResult parsed, long nowMs) {
        return switch (parsed) {
            case HeaderParseResult.NoDelay ignored -> IngestDecision.RelayNow.INSTANCE;
            case HeaderParseResult.RelayImmediately ignored -> IngestDecision.RelayNow.INSTANCE;
            case HeaderParseResult.Schedule schedule -> new IngestDecision.Schedule(schedule.dispatchAtMs());
            case HeaderParseResult.Malformed malformed ->
                switch (onMalformedHeader) {
                    case DLQ -> new IngestDecision.ToDlq(DlqReasons.MALFORMED_HEADER, malformed.detail());
                    case RELAY_IMMEDIATE -> IngestDecision.RelayNow.INSTANCE;
                    case FAIL -> new IngestDecision.Fail(malformed.detail());
                };
            case HeaderParseResult.OverMax overMax ->
                switch (onOverMax) {
                    case DLQ -> new IngestDecision.ToDlq(DlqReasons.OVER_MAX_DELAY, overMax.detail());
                    // CLAMP pins to now + delay.max (saturated, never wrapped) — §2.3.
                    case CLAMP -> new IngestDecision.Clamp(DelayHeaderCodec.saturatedAddMillis(nowMs, delayMaxMs));
                    case FAIL -> new IngestDecision.Fail(overMax.detail());
                };
        };
    }
}
