package events.cesium.kafka.it;

import static org.awaitility.Awaitility.await;

import events.cesium.kafka.api.headers.CesiumHeaders;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Shared producer + measurement plumbing for the design §11.4 <strong>macro</strong> performance and
 * soak integration tests (the {@code *PerfIT} family). Deliberately small and metric-driven: the
 * perf classes read sustained throughput off the engine's own §9 Micrometer counters (the same
 * meters production scrapes), so a number reported here is the number an operator would see — never
 * a synthetic in-test tally.
 *
 * <p><strong>Honesty.</strong> {@link #measureSustainedThroughput} excludes the cold-start ramp
 * (consumer group join, first {@code poll()}, replay-to-barrier) by starting its clock only once the
 * loop's counter has crossed a warm-up fraction, then measuring the steady-state slope to
 * completion. The reported rate is therefore the sustained mid-run rate, not an
 * {@code total / wall-clock} figure inflated or deflated by startup.
 */
final class PerfSupport {

    private PerfSupport() {}

    /** Bulk source loader: high-throughput async producer (large batches, short linger, acks=all). */
    static Producer<byte[], byte[]> newBulkProducer() {
        return newBulkProducer(1 << 20);
    }

    /**
     * Bulk source loader with an explicit {@code max.request.size} (the §11.4 large-payload test
     * raises it above the 1 MiB default to publish multi-megabyte source records). {@code acks=all}
     * keeps the load durable so the engine sees a real, fully-replicated backlog.
     */
    static Producer<byte[], byte[]> newBulkProducer(int maxRequestSize) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaIT.bootstrap());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(512 * 1024));
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Integer.toString(maxRequestSize));
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, Integer.toString(64 * 1024 * 1024));
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    /**
     * Fire-and-forget produce of {@code count} records round-robin across {@code partitions}, each
     * carrying the given control {@code header} (or none for immediate-relay traffic) and a value of
     * {@code valueBytes} bytes. Returns after a single {@link Producer#flush()} so the whole backlog
     * is durable before the caller starts its measurement clock.
     */
    static void produce(
            Producer<byte[], byte[]> producer,
            String topic,
            int partitions,
            int count,
            String keyPrefix,
            int valueBytes,
            HeaderFactory header) {
        byte[] value = new byte[valueBytes];
        Arrays.fill(value, (byte) 'x');
        for (int i = 0; i < count; i++) {
            List<Header> headers = header == null ? List.of() : List.of(header.forRecord(i));
            var unused = producer.send(new ProducerRecord<>(
                    topic, i % partitions, null, (keyPrefix + i).getBytes(StandardCharsets.UTF_8), value, headers));
        }
        producer.flush();
    }

    /** Per-record control header supplier (lets a deliver-at vary by index when needed). */
    @FunctionalInterface
    interface HeaderFactory {
        Header forRecord(int index);
    }

    /** A fixed {@code cesium-delay-ms} on every record (scheduled traffic that comes due after the delay). */
    static HeaderFactory delayMs(long delayMs) {
        byte[] raw = Long.toString(delayMs).getBytes(StandardCharsets.UTF_8);
        return index -> new RecordHeader(CesiumHeaders.DELAY_MS, raw);
    }

    /** A fixed {@code cesium-deliver-at} instant on every record (a common simultaneous due time). */
    static HeaderFactory deliverAt(long deliverAtMs) {
        byte[] raw = Long.toString(deliverAtMs).getBytes(StandardCharsets.UTF_8);
        return index -> new RecordHeader(CesiumHeaders.DELIVER_AT, raw);
    }

    /**
     * Measures the sustained steady-state rate at which {@code counterName} (summed over its tagged
     * children) climbs from {@code warmupFraction × total} to {@code total}. Polls the live meter,
     * snapshots {@code (count, nanos)} at the warm-up crossing and again at completion, and returns
     * the slope between them. Fails (via awaitility) if {@code total} is not reached within
     * {@code timeout} — a stall is a real perf regression, not a silently-low number.
     */
    static ThroughputResult measureSustainedThroughput(
            MeterRegistry registry, String counterName, long total, double warmupFraction, Duration timeout) {
        long warmupAt = Math.max(1, (long) (total * warmupFraction));
        long deadline = System.nanoTime() + timeout.toNanos();

        Sample warm = awaitCount(registry, counterName, warmupAt, deadline);
        Sample done = awaitCount(registry, counterName, total, deadline);

        double elapsedSeconds = (done.nanos - warm.nanos) / 1e9;
        double records = done.count - warm.count;
        double rate = elapsedSeconds > 0 ? records / elapsedSeconds : Double.NaN;
        return new ThroughputResult(rate, (long) done.count, elapsedSeconds);
    }

    private static Sample awaitCount(MeterRegistry registry, String counterName, long target, long deadlineNanos) {
        Duration remaining = Duration.ofNanos(Math.max(1, deadlineNanos - System.nanoTime()));
        await("counter " + counterName + " reaches " + target)
                .atMost(remaining)
                .pollInterval(Duration.ofMillis(50))
                .until(() -> EngineMetrics.counterSum(registry, counterName) >= target);
        return new Sample(EngineMetrics.counterSum(registry, counterName), System.nanoTime());
    }

    private record Sample(double count, long nanos) {}

    /**
     * The shortest steady-state window we treat as a precise point figure. Below this, the window is
     * comparable to the awaitility poll granularity (the warm/done snapshots are only a few polls
     * apart and the warm snapshot overshoots its target), so the rate carries a large relative error
     * and must be read as <em>order-of-magnitude headroom</em>, not a point SLA. On a fast localhost
     * broker the in-JVM engine routinely drains the CI-sized backlog inside this floor — a server-class
     * run with larger {@code -Dperf.*} counts yields a longer, representative window (design §15 risk #9).
     */
    static final double COARSE_WINDOW_SECONDS = 1.0;

    /** A measured sustained throughput: records/s, the final observed count, and the measured window. */
    record ThroughputResult(double recordsPerSecond, long observed, double windowSeconds) {
        /** True when the measurement window is too short to read the rate as a precise point figure. */
        boolean coarse() {
            return windowSeconds < COARSE_WINDOW_SECONDS;
        }

        @Override
        public String toString() {
            return String.format(
                    "%.0f rec/s (observed %d over %.2f s steady-state window)%s",
                    recordsPerSecond,
                    observed,
                    windowSeconds,
                    coarse() ? " [COARSE WINDOW <1s — read as order-of-magnitude headroom, not a point SLA]" : "");
        }
    }

    // ------------------------------------------------------------------ latency

    /**
     * Accumulates dispatch-lateness samples and computes percentiles. Lateness is
     * {@code dispatchInstant − requestedDeliverAt}: with the default {@code relay.timestamp=DISPATCH}
     * policy (D8) the destination record's timestamp <em>is</em> the dispatch instant, so
     * {@code record.timestamp() − deliverAt} reproduces the engine's {@code cesium_dispatch_lag_seconds}
     * end to end, including the relay hop.
     */
    static final class LatencyDigest {
        private final List<Long> lateMillis = new ArrayList<>();

        void add(long dispatchInstantMs, long requestedDeliverAtMs) {
            lateMillis.add(Math.max(0L, dispatchInstantMs - requestedDeliverAtMs));
        }

        int size() {
            return lateMillis.size();
        }

        long percentileMillis(double q) {
            if (lateMillis.isEmpty()) {
                return 0L;
            }
            long[] sorted =
                    lateMillis.stream().mapToLong(Long::longValue).sorted().toArray();
            int rank = (int) Math.ceil(q * sorted.length) - 1;
            return sorted[Math.min(sorted.length - 1, Math.max(0, rank))];
        }

        long maxMillis() {
            return lateMillis.stream().mapToLong(Long::longValue).max().orElse(0L);
        }

        double meanMillis() {
            return lateMillis.stream().mapToLong(Long::longValue).average().orElse(0.0);
        }
    }

    // ------------------------------------------------------------------ misc

    private static final AtomicLong PRODUCED = new AtomicLong();

    /** Monotonic counter for ad-hoc unique suffixes inside a perf class. */
    static long nextSeq() {
        return PRODUCED.incrementAndGet();
    }
}
