/**
 * Structured logging support (design §9). {@link com.jucius.cesium.kafka.app.logging.CompactJsonEncoder}
 * is a minimal one-line-per-event JSON logback encoder activated by {@code LOG_FORMAT=json} (selected
 * by the {@code logback.xml} profile include); it streams through the Jackson core the app already
 * ships rather than adding a logging-JSON dependency. The plain profile uses a logback {@code
 * PatternLayout} console encoder. Both render the MDC context keys ({@code applicationId}/{@code
 * loop}/{@code txn}, set by the engine per thread) and emit one-line lifecycle events; neither ever
 * logs payloads.
 */
@org.jspecify.annotations.NullMarked
package com.jucius.cesium.kafka.app.logging;
