package com.jucius.cesium.kafka.core.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.core.config.ValidationReport.Finding;
import org.junit.jupiter.api.Test;

/**
 * The process-wide pending cap vs the 25%-of-heap index budget (design §5.3, R18): the effective
 * cap is always reported, and an explicit cap that admits more than the budget is a warning. The
 * budget itself is enforced at runtime (backpressure + the store's H1 ceiling, ADR-0021).
 */
class PendingCapValidationTest {

    private static final long ONE_GIB = 1024L * 1024 * 1024;

    private final CesiumConfigValidator validator = new CesiumConfigValidator();

    @Test
    void autoCapIsReportedAndNeverWarns() {
        // AUTO: 1 GiB / 4 / 64 B = 4,194,304 entries, derived from the budget, so it cannot exceed it.
        ValidationReport report = validator.validate(TestConfigs.valid(), new ValidationContext(ONE_GIB));
        assertFalse(report.hasErrors(), report::render);
        assertFalse(capFindings(report.warnings()), report::render);
        Finding info = report.infos().stream()
                .filter(f -> f.path().equals("dispatch.max-pending-total"))
                .findFirst()
                .orElseThrow(() -> new AssertionError(report.render()));
        assertTrue(info.message().contains("4194304"), info::message);
        assertTrue(info.message().contains("268435456"), info::message);
    }

    @Test
    void explicitCapOverTheBudgetWarnsButPasses() {
        // 10,000,000 entries x 64 B = 640 MB > 256 MiB budget.
        DispatchConfig dispatch = new DispatchConfig(null, null, null, null, null, null, 1_000_000L, 10_000_000L);
        ValidationReport report =
                validator.validate(TestConfigs.withDispatch(dispatch), new ValidationContext(ONE_GIB));
        assertFalse(report.hasErrors(), report::render);
        assertTrue(capFindings(report.warnings()), report::render);
    }

    @Test
    void absurdCapsSaturateInsteadOfOverflowing() {
        DispatchConfig dispatch =
                new DispatchConfig(null, null, null, null, null, null, 1_000_000L, Long.MAX_VALUE / 2);
        ValidationReport report =
                validator.validate(TestConfigs.withDispatch(dispatch), new ValidationContext(ONE_GIB));
        assertTrue(
                report.warnings().stream().anyMatch(f -> f.message().contains(String.valueOf(Long.MAX_VALUE))),
                report::render);
    }

    private static boolean capFindings(java.util.List<Finding> findings) {
        return findings.stream().anyMatch(f -> f.path().equals("dispatch.max-pending-total"));
    }
}
