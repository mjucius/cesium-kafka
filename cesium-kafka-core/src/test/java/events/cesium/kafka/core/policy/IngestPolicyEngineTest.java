package events.cesium.kafka.core.policy;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import events.cesium.kafka.core.headers.DlqReasons;
import events.cesium.kafka.core.headers.HeaderParseResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Design §11.1: the full (input-shape × policy) decision table — every cell of malformed and
 * over-max under every policy combination, plus the policy-invariant shapes.
 */
class IngestPolicyEngineTest {

    private static final long NOW = 1_700_000_000_000L;
    private static final Duration MAX = Duration.ofDays(1);
    private static final long MAX_MS = MAX.toMillis();

    private static final HeaderParseResult NO_DELAY = HeaderParseResult.NoDelay.INSTANCE;
    private static final HeaderParseResult PAST_DUE = new HeaderParseResult.RelayImmediately(false);
    private static final HeaderParseResult SCHEDULE = new HeaderParseResult.Schedule(NOW + 60_000, false);
    private static final HeaderParseResult MALFORMED = new HeaderParseResult.Malformed("malformed-detail", false);
    private static final HeaderParseResult OVER_MAX =
            new HeaderParseResult.OverMax(NOW + MAX_MS + 1, "over-max-detail", false);

    /**
     * Every (input shape × malformed policy × over-max policy) cell with its expected action,
     * written out as an explicit table — 5 shapes × 9 policy combinations = 45 cells.
     */
    static Stream<Arguments> decisionTable() {
        // Expected actions per shape, keyed by the only policy axis that may affect the shape.
        Map<HeaderParseResult, String> shapeNames = Map.of(
                NO_DELAY, "NoDelay",
                PAST_DUE, "RelayImmediately",
                SCHEDULE, "Schedule",
                MALFORMED, "Malformed",
                OVER_MAX, "OverMax");
        List<Arguments> cells = new ArrayList<>();
        for (MalformedHeaderPolicy onMalformed : MalformedHeaderPolicy.values()) {
            for (OverMaxPolicy onOverMax : OverMaxPolicy.values()) {
                for (HeaderParseResult shape : shapeNames.keySet()) {
                    IngestDecision expected =
                            switch (shapeNames.get(shape)) {
                                case "NoDelay", "RelayImmediately" -> IngestDecision.RelayNow.INSTANCE;
                                case "Schedule" -> new IngestDecision.Schedule(NOW + 60_000);
                                case "Malformed" ->
                                    switch (onMalformed) {
                                        case DLQ ->
                                            new IngestDecision.ToDlq(DlqReasons.MALFORMED_HEADER, "malformed-detail");
                                        case RELAY_IMMEDIATE -> IngestDecision.RelayNow.INSTANCE;
                                        case FAIL -> new IngestDecision.Fail("malformed-detail");
                                    };
                                case "OverMax" ->
                                    switch (onOverMax) {
                                        case DLQ ->
                                            new IngestDecision.ToDlq(DlqReasons.OVER_MAX_DELAY, "over-max-detail");
                                        case CLAMP -> new IngestDecision.Clamp(NOW + MAX_MS);
                                        case FAIL -> new IngestDecision.Fail("over-max-detail");
                                    };
                                default -> throw new AssertionError();
                            };
                    cells.add(Arguments.of(shapeNames.get(shape), shape, onMalformed, onOverMax, expected));
                }
            }
        }
        return cells.stream();
    }

    @ParameterizedTest(name = "{0} × malformed={2} × overMax={3}")
    @MethodSource("decisionTable")
    void everyCellOfTheDecisionTable(
            String shapeName,
            HeaderParseResult shape,
            MalformedHeaderPolicy onMalformed,
            OverMaxPolicy onOverMax,
            IngestDecision expected) {
        IngestPolicyEngine engine = new IngestPolicyEngine(MAX, onMalformed, onOverMax);
        assertEquals(expected, engine.decide(shape, NOW));
    }

    @Test
    void dlqReasonsAreTheWireContractStrings() {
        // §2.4: these are versioned public wire values, not internal identifiers.
        assertEquals("malformed-header", DlqReasons.MALFORMED_HEADER);
        assertEquals("over-max-delay", DlqReasons.OVER_MAX_DELAY);
        assertEquals("payload-expired", DlqReasons.PAYLOAD_EXPIRED);
    }

    @Test
    void clampTargetSaturatesNearTheEpochCeiling() {
        IngestPolicyEngine engine = new IngestPolicyEngine(MAX, MalformedHeaderPolicy.DLQ, OverMaxPolicy.CLAMP);
        IngestDecision decision = engine.decide(OVER_MAX, Long.MAX_VALUE - 5);
        assertEquals(new IngestDecision.Clamp(Long.MAX_VALUE), decision);
    }

    @Test
    void conflictedResultsKeepTheirOutcome() {
        // Conflicts are observability-only (§2.3): the same shape with the flag set decides the same.
        IngestPolicyEngine engine = new IngestPolicyEngine(MAX, MalformedHeaderPolicy.DLQ, OverMaxPolicy.DLQ);
        assertEquals(
                engine.decide(new HeaderParseResult.Schedule(NOW + 60_000, false), NOW),
                engine.decide(new HeaderParseResult.Schedule(NOW + 60_000, true), NOW));
    }

    @Test
    void unfetchablePayloadPolicy_hasTheDispatchSideCells() {
        // The dispatch loop (M5) consumes this enum; pin the §7 value set here so a rename or
        // reorder is caught at the policy layer it belongs to.
        assertArrayEquals(
                new UnfetchablePayloadPolicy[] {
                    UnfetchablePayloadPolicy.DLQ, UnfetchablePayloadPolicy.DROP, UnfetchablePayloadPolicy.FAIL
                },
                UnfetchablePayloadPolicy.values());
    }
}
