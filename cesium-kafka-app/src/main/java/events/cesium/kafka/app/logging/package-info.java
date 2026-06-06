/**
 * Structured logging support (design §9). {@link events.cesium.kafka.app.logging.CompactJsonEncoder}
 * is a minimal one-line-per-event JSON logback encoder activated by {@code LOG_FORMAT=json} (selected
 * by the {@code logback.xml} profile include); it reuses the app's hand-rolled JSON writer rather
 * than adding a logging-JSON dependency. The plain profile uses a logback {@code PatternLayout}
 * console encoder. Both carry the MDC context keys ({@code applicationId}/{@code loop}/{@code
 * partition}/{@code txn}) and emit one-line lifecycle events; neither ever logs payloads.
 */
@org.jspecify.annotations.NullMarked
package events.cesium.kafka.app.logging;
