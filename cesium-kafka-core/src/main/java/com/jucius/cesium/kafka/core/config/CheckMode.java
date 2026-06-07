package com.jucius.cesium.kafka.core.config;

/**
 * Strictness of a startup check (design §7.6, §8). Not every check accepts every mode — e.g. the
 * heap-budget and offsets-retention checks are FAIL/WARN only (the validator rejects SKIP there).
 */
public enum CheckMode {
    /** A violated check refuses startup. */
    FAIL,
    /** A violated check logs and surfaces a warning but startup proceeds. */
    WARN,
    /** The check is not performed. */
    SKIP
}
