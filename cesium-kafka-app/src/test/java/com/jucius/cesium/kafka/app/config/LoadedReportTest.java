package com.jucius.cesium.kafka.app.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.app.config.CesiumConfigLoader.LoadedConfig;
import com.jucius.cesium.kafka.core.config.ValidationContext;
import com.jucius.cesium.kafka.core.config.ValidationReport.Severity;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A successful load must not discard the validation report: the always-present INFO index cap
 * (§5.3 prints it at startup) and WARNING findings (an explicit cap over the heap budget) need a
 * path to the operator.
 */
class LoadedReportTest {

    @TempDir
    Path dir;

    @Test
    void successfulLoadCarriesTheInfoCapFinding() {
        Path file = LoaderTestSupport.write(dir, LoaderTestSupport.MINIMAL_YAML);
        LoadedConfig loaded = LoaderTestSupport.loader(Map.of()).load(file);
        assertFalse(loaded.report().hasErrors(), loaded.report()::render);
        assertTrue(
                loaded.report().infos().stream()
                        .anyMatch(f -> f.path().equals("dispatch.max-pending-total")
                                && f.message().contains("index cap")),
                loaded.report()::render);
    }

    @Test
    void capOverTheHeapBudgetSurfacesOnASuccessfulLoad() {
        // 1,000,000 entries x 64 B = 64 MB > 16 MiB budget (64 MiB heap / 4): the load succeeds
        // but must report it.
        Path file = LoaderTestSupport.write(
                dir,
                LoaderTestSupport.MINIMAL_YAML
                        + """
                        dispatch:
                          max-pending-total: 1000000
                        """);
        ValidationContext tinyHeap = new ValidationContext(64L * 1024 * 1024);
        LoadedConfig loaded = new CesiumConfigLoader(Map.of(), tinyHeap).load(file);
        assertFalse(loaded.report().hasErrors(), loaded.report()::render);
        var breach = loaded.report().warnings().stream()
                .filter(f -> f.path().equals("dispatch.max-pending-total"))
                .findFirst()
                .orElseThrow(() -> new AssertionError(loaded.report().render()));
        assertEquals(Severity.WARNING, breach.severity());
        assertTrue(breach.message().contains("more than the heap budget"), breach::message);
    }
}
