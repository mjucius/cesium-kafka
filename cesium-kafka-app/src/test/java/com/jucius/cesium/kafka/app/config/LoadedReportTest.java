package com.jucius.cesium.kafka.app.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.app.config.CesiumConfigLoader.LoadedConfig;
import com.jucius.cesium.kafka.core.config.ValidationContext;
import com.jucius.cesium.kafka.core.config.ValidationReport.Severity;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A successful load must not discard the validation report: the always-present INFO worst-case
 * index footprint (§5.3 prints it at startup) and WARNING findings (heap-budget breach under
 * {@code startup-checks.heap-budget: WARN}) need a path to the operator.
 */
class LoadedReportTest {

    @TempDir
    Path dir;

    @Test
    void successfulLoadCarriesTheInfoFootprintFinding() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        LoadedConfig loaded = LoaderTestSupport.loader(Map.of()).load(file);
        assertFalse(loaded.report().hasErrors(), loaded.report()::render);
        assertTrue(
                loaded.report().infos().stream()
                        .anyMatch(f -> f.path().equals("dispatch.max-pending-per-partition")
                                && f.message().contains("worst-case index footprint")),
                loaded.report()::render);
    }

    @Test
    void warnModeHeapBreachSurfacesOnASuccessfulLoad() {
        // 2,000,000 default entries x 64 B = 128 MB > 16 MiB budget (64 MiB heap / 4), downgraded
        // to WARNING by startup-checks.heap-budget: WARN — the load succeeds but must report it.
        Path file = LoaderTestSupport.write(
                dir,
                LoaderTestSupport.MINIMAL_YAML
                        + """
                        startup-checks:
                          heap-budget: WARN
                        """);
        ValidationContext tinyHeap = new ValidationContext(64L * 1024 * 1024, 1);
        LoadedConfig loaded = new CesiumConfigLoader(Map.of(), new Properties(), tinyHeap).load(file);
        assertFalse(loaded.report().hasErrors(), loaded.report()::render);
        var breach = loaded.report().warnings().stream()
                .filter(f -> f.path().equals("dispatch.max-pending-per-partition"))
                .findFirst()
                .orElseThrow(() -> new AssertionError(loaded.report().render()));
        assertEquals(Severity.WARNING, breach.severity());
        assertTrue(breach.message().contains("exceeds the heap budget"), breach::message);
    }
}
