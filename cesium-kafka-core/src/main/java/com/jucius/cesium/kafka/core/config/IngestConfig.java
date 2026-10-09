package com.jucius.cesium.kafka.core.config;

import java.util.Objects;

/**
 * Ingest-loop settings (design §8).
 *
 * @param workers number of ingest threads, each owning its own source consumer and transactional
 *     producer (§6); default 1
 */
public record IngestConfig(Integer workers) {

    /** Default worker count (§8 defaults table). */
    public static final int DEFAULT_WORKERS = 1;

    /** Materializes the §8 defaults for absent components. */
    public IngestConfig {
        workers = Objects.requireNonNullElse(workers, DEFAULT_WORKERS);
    }

    /** Returns an {@code IngestConfig} populated entirely from the §8 defaults table. */
    public static IngestConfig defaults() {
        return new IngestConfig(DEFAULT_WORKERS);
    }
}
