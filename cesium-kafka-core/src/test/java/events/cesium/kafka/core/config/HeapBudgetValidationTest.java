package events.cesium.kafka.core.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.core.config.ValidationReport.Finding;
import events.cesium.kafka.core.config.ValidationReport.Severity;
import org.junit.jupiter.api.Test;

/**
 * Heap-budget validation math (design §5.3, R7/R18; §11.1): worst-case footprint =
 * partitions × max-pending-per-partition × 64 B/entry (post-approval revision: 64, not 48),
 * checked against 25% of max heap, FAIL or WARN per {@code startup-checks.heap-budget}, with the
 * computed footprint always present in the report.
 */
class HeapBudgetValidationTest {

    private static final long ONE_GIB = 1024L * 1024 * 1024;

    private final CesiumConfigValidator validator = new CesiumConfigValidator();

    @Test
    void footprintWithinBudgetPasses() {
        // 1 partition x 2,000,000 entries x 64 B = 128 MiB <= 256 MiB budget (1 GiB heap / 4).
        ValidationReport report = validator.validate(TestConfigs.valid(), new ValidationContext(ONE_GIB, 1));
        assertFalse(report.hasErrors(), report::render);
    }

    @Test
    void footprintBeyondBudgetFailsByDefault() {
        // 10 partitions x 2,000,000 x 64 B = 1,280,000,000 B > 268,435,456 B budget.
        ValidationReport report = validator.validate(TestConfigs.valid(), new ValidationContext(ONE_GIB, 10));
        Finding breach = report.errors().stream()
                .filter(f -> f.path().equals("dispatch.max-pending-per-partition"))
                .findFirst()
                .orElseThrow(() -> new AssertionError(report.render()));
        assertTrue(breach.message().contains("1280000000"), breach::message);
        assertTrue(breach.message().contains("268435456"), breach::message);
    }

    @Test
    void warnModeDowngradesTheBreachToAWarning() {
        StartupChecks warnChecks = new StartupChecks(null, null, null, null, CheckMode.WARN);
        CesiumConfig config = TestConfigs.withDispatch(DispatchConfig.defaults(), warnChecks);
        ValidationReport report = validator.validate(config, new ValidationContext(ONE_GIB, 10));
        assertFalse(report.hasErrors(), report::render);
        Finding warning = report.warnings().stream()
                .filter(f -> f.path().equals("dispatch.max-pending-per-partition"))
                .findFirst()
                .orElseThrow(() -> new AssertionError(report.render()));
        assertEquals(Severity.WARNING, warning.severity());
        assertTrue(warning.message().contains("1280000000"), warning::message);
    }

    @Test
    void computedFootprintIsAlwaysInTheReport() {
        // Even a passing config reports the worst-case footprint (printed at startup, §5.3).
        ValidationReport report = validator.validate(TestConfigs.valid(), new ValidationContext(ONE_GIB, 1));
        Finding info = report.infos().stream()
                .filter(f -> f.path().equals("dispatch.max-pending-per-partition"))
                .findFirst()
                .orElseThrow(() -> new AssertionError(report.render()));
        // 1 x 2,000,000 x 64 = 128,000,000 bytes vs 268,435,456 budget.
        assertTrue(info.message().contains("128000000"), info::message);
        assertTrue(info.message().contains("268435456"), info::message);
        assertTrue(info.message().contains("64"), info::message);
    }

    @Test
    void explicitGlobalCapIsCrossCheckedAgainstTheBudget() {
        // 10,000,000 entries x 64 B = 640 MB > 256 MiB budget.
        DispatchConfig dispatch =
                new DispatchConfig(null, null, null, null, null, null, null, null, 1_000_000L, 10_000_000L);
        CesiumConfig config = TestConfigs.withDispatch(dispatch);
        ValidationReport report = validator.validate(config, new ValidationContext(ONE_GIB, 1));
        assertTrue(
                report.errors().stream().anyMatch(f -> f.path().equals("dispatch.max-pending-total")), report::render);
    }

    @Test
    void autoGlobalCapNeverBreachesTheBudget() {
        // AUTO derives the cap from the same budget, so it cannot breach it.
        DispatchConfig dispatch = new DispatchConfig(null, null, null, null, null, null, null, null, 1_000_000L, null);
        CesiumConfig config = TestConfigs.withDispatch(dispatch);
        ValidationReport report = validator.validate(config, new ValidationContext(ONE_GIB, 1));
        assertFalse(report.hasErrors(), report::render);
    }

    @Test
    void absurdConfigsSaturateInsteadOfOverflowing() {
        DispatchConfig dispatch =
                new DispatchConfig(null, null, null, null, null, null, null, null, Long.MAX_VALUE / 2, null);
        CesiumConfig config = TestConfigs.withDispatch(dispatch);
        ValidationReport report = validator.validate(config, new ValidationContext(ONE_GIB, 1000));
        assertTrue(report.hasErrors(), report::render);
        assertTrue(
                report.errors().stream().anyMatch(f -> f.message().contains(String.valueOf(Long.MAX_VALUE))),
                report::render);
    }
}
