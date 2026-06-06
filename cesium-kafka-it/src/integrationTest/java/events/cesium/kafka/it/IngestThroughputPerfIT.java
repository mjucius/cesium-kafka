package events.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.core.config.Role;
import java.time.Duration;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Macro perf — <strong>ingest throughput</strong> (design §11.4: ingest ≥ 20 k rec/s CI, ≥ 100 k
 * dedicated; R9 "targets are projections until measured"). Pre-loads a mixed backlog (half
 * immediate-relay, half {@code cesium-delay-ms} scheduled traffic) into a multi-partition source,
 * then drives it through a real single-thread {@link events.cesium.kafka.core.ingest.IngestLoop} and
 * reads the sustained steady-state rate off the engine's own {@code cesium_ingest_records} counter.
 *
 * <p>Sized for this dev box; the steady-state measurement excludes the consumer-join / first-poll
 * ramp ({@link PerfSupport#measureSustainedThroughput}). The hard assertion is a conservative
 * regression floor — the headline is the logged measured number against the §11.4 targets, captured
 * honestly per R9 (a miss is reported, never fudged). {@code @Tag("nightly")}: never on the PR lane.
 */
@Tag("nightly")
class IngestThroughputPerfIT extends KafkaIT {

    private static final Logger log = LoggerFactory.getLogger(IngestThroughputPerfIT.class);

    /** Source records driven through ingest (half immediate, half scheduled). Tunable for a bigger box. */
    private static final int TOTAL = Integer.getInteger("perf.ingest.total", 50_000);

    private static final int PARTITIONS = 4;
    private static final int VALUE_BYTES = 256;

    /** Conservative regression floor (rec/s): catches a stall/gross regression without flaking on a slow box. */
    private static final double FLOOR = Double.parseDouble(System.getProperty("perf.ingest.floor", "4000"));

    private EngineHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void sustainedIngestThroughput() {
        String source = createTopic("perf-src", PARTITIONS);
        String destination = createTopic("perf-dst", PARTITIONS);
        String dlq = createTopic("perf-dlq", 1);

        int immediate = TOTAL / 2;
        int scheduled = TOTAL - immediate;
        // Pre-load the whole backlog so the loop is never starved — the rate measures the engine, not
        // the producer. Scheduled traffic is due far enough out that it never competes for the thread.
        try (Producer<byte[], byte[]> producer = PerfSupport.newBulkProducer()) {
            PerfSupport.produce(producer, source, PARTITIONS, immediate, "imm-", VALUE_BYTES, null);
            PerfSupport.produce(
                    producer, source, PARTITIONS, scheduled, "sch-", VALUE_BYTES, PerfSupport.delayMs(600_000));
        }

        harness = EngineHarness.builder()
                .roles(Role.INGEST)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                // delay.max above the 10-minute scheduled delay so the scheduled half is accepted, not clamped.
                .delayMax(Duration.ofHours(1))
                .build();
        harness.start();

        PerfSupport.ThroughputResult result = PerfSupport.measureSustainedThroughput(
                harness.meterRegistry(), "cesium.ingest.records", TOTAL, 0.15, Duration.ofMinutes(5));

        log.info("MEASURED ingest throughput = {} | target ≥20k rec/s (CI) / ≥100k (dedicated) [§11.4]", result);
        System.out.printf(
                "%n[PERF] ingest sustained = %.0f rec/s (%d records, %.2fs window); target ≥20k CI / ≥100k dedicated%n",
                result.recordsPerSecond(), result.observed(), result.windowSeconds());

        assertEquals(
                TOTAL,
                (long) EngineMetrics.counterSum(harness.meterRegistry(), "cesium.ingest.records"),
                "every source record must be processed exactly once by ingest");
        // The immediate half relayed to the destination exactly once.
        readExactlyCommitted(destination, immediate, WAIT);
        assertTrue(
                result.recordsPerSecond() >= FLOOR,
                () -> "ingest throughput " + result + " fell below the regression floor " + FLOOR + " rec/s");
        harness.stop();
    }
}
