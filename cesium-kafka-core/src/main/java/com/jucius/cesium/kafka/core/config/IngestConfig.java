package com.jucius.cesium.kafka.core.config;

import java.util.Objects;

/**
 * Ingest-loop settings (design §8).
 *
 * @param workers number of ingest threads, each owning its own source consumer and transactional
 *     producer (§6); default 1
 * @param maxBatch maximum records per poll/transaction (becomes the source consumer's
 *     {@code max.poll.records}); default 2000
 */
public record IngestConfig(Integer workers, Integer maxBatch) {

    /** Default worker count (§8 defaults table). */
    public static final int DEFAULT_WORKERS = 1;

    /** Default max batch — the source consumer's {@code max.poll.records} (§8 defaults table). */
    public static final int DEFAULT_MAX_BATCH = 2000;

    /** Materializes the §8 defaults for absent components. */
    public IngestConfig {
        workers = Objects.requireNonNullElse(workers, DEFAULT_WORKERS);
        maxBatch = Objects.requireNonNullElse(maxBatch, DEFAULT_MAX_BATCH);
    }

    /** Returns an {@code IngestConfig} populated entirely from the §8 defaults table. */
    public static IngestConfig defaults() {
        return new IngestConfig(DEFAULT_WORKERS, DEFAULT_MAX_BATCH);
    }
}
