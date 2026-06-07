package com.jucius.cesium.kafka.app.lifecycle;

import com.jucius.cesium.kafka.core.config.ValidationReport;
import org.jspecify.annotations.Nullable;

/**
 * Thrown by {@link CesiumEngine#start()} when the engine refuses to start: a failed startup
 * validation (missing/misconfigured topics, identity mismatch, retention violations, the
 * partition-aware heap-budget breach), or a store that fails its own {@code validate()}. Carries the
 * aggregate {@link ValidationReport} when one is available so {@link com.jucius.cesium.kafka.app.CesiumApp}
 * can render every finding in one round trip. The app maps this to a non-zero (fatal) exit — distinct
 * from the {@code EX_CONFIG} (78) reserved for config-file errors caught before the engine builds.
 */
public final class EngineStartupException extends RuntimeException {

    private final transient @Nullable ValidationReport report;

    EngineStartupException(String message) {
        super(message);
        this.report = null;
    }

    EngineStartupException(String message, ValidationReport report) {
        super(message);
        this.report = report;
    }

    EngineStartupException(String message, Throwable cause) {
        super(message, cause);
        this.report = null;
    }

    /** The aggregate validation report behind this failure, when the failure was a validation one. */
    public @Nullable ValidationReport report() {
        return report;
    }
}
