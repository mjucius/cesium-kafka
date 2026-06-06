package events.cesium.kafka.it;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Reads the §9 Micrometer meters an {@link EngineHarness} exposes through its
 * {@link EngineHarness#meterRegistry()}. The M6 backpressure/degradation/penalty-box scenarios
 * assert engine behaviour through these gauges (design §9 names use {@code .} separators in the
 * registry; the {@code cesium_} Prometheus form is the scrape-time rename):
 *
 * <ul>
 *   <li>{@code cesium.shard.paused}{partition} — 1 while §5.3 backpressure pauses a shard's tracker
 *       intake;
 *   <li>{@code cesium.pending.entries}{partition} — live store index size for a partition;
 *   <li>{@code cesium.degraded}{loop} — 1 while a loop is parked-and-degraded (§3.8);
 *   <li>{@code cesium.dispatch.poll.gap.seconds} — recent max inter-poll gap (group-B liveness, §6);
 *   <li>{@code cesium.fetch.penalized.partitions} — §7.3 penalty-box occupancy.
 * </ul>
 *
 * <p>A meter that has not been registered yet (e.g. a per-partition gauge before its shard is
 * assigned) reads as {@link Double#NaN} rather than throwing, so an awaitility poll can spin on it
 * without a null guard at every call site.
 */
final class EngineMetrics {

    private EngineMetrics() {}

    /** The value of a gauge by name and tag pairs, or {@link Double#NaN} if not yet registered. */
    static double gauge(MeterRegistry registry, String name, String... tags) {
        Gauge gauge = registry.find(name).tags(tags).gauge();
        return gauge == null ? Double.NaN : gauge.value();
    }

    /** True when the gauge exists and currently reads exactly {@code expected}. */
    static boolean gaugeIs(MeterRegistry registry, String name, double expected, String... tags) {
        Gauge gauge = registry.find(name).tags(tags).gauge();
        return gauge != null && gauge.value() == expected;
    }

    /** The summed count of a counter by name and tag pairs, or {@code 0.0} if not yet registered. */
    static double counter(MeterRegistry registry, String name, String... tags) {
        Counter counter = registry.find(name).tags(tags).counter();
        return counter == null ? 0.0 : counter.count();
    }

    /**
     * The total count across every tagged child of a counter (e.g. {@code cesium.ingest.records}
     * summed over all {@code outcome} tags), or {@code 0.0} when none is registered yet. The §11.4
     * macro perf throughput tests read this to total a loop's processed-record count regardless of
     * how the engine partitions it by outcome tag.
     */
    static double counterSum(MeterRegistry registry, String name) {
        return registry.find(name).counters().stream()
                .mapToDouble(Counter::count)
                .sum();
    }
}
