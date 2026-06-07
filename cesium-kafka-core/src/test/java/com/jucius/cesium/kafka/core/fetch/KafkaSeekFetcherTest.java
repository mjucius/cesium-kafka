package com.jucius.cesium.kafka.core.fetch;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

/**
 * §7 seek-fetch unit suite on {@code MockConsumer}: the found/gone/transient/carry-over decision
 * table, the three budgets, per-partition slice isolation (clock-simulated), the
 * beginningOffsets-based GONE-vs-TRANSIENT distinction, warm-assignment union behavior, and the
 * byte-accounting contract.
 *
 * <p><strong>What "decompressed" means here:</strong> the real consumer decompresses fetch batches
 * client-side and reports per-record {@code serializedKey/ValueSize} measured after that
 * decompression — exactly what the §7.2 budget protects the heap from. {@code MockConsumer} hands
 * back whatever {@code ConsumerRecord}s the test fabricates, so these tests pin the accounting
 * rule instead: explicit serialized sizes win; records fabricated without sizes fall back to the
 * key/value array lengths (identical under byte-array serdes). See the
 * {@code KafkaSeekFetcher.recordBytes} TODO for the sizing refinements deliberately out of scope.
 */
class KafkaSeekFetcherTest {

    private static final String TOPIC = "orders";
    private static final long NOW = 1_700_000_000_000L;

    private final MutableClock clock = new MutableClock(NOW);
    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("none");
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final KafkaSeekFetcher fetcher =
            new KafkaSeekFetcher(TOPIC, Duration.ofSeconds(2), consumer, registry, clock);

    private static TopicPartition tp(int partition) {
        return new TopicPartition(TOPIC, partition);
    }

    private static ConsumerRecord<byte[], byte[]> record(int partition, long offset, int valueSize) {
        return new ConsumerRecord<>(
                TOPIC,
                partition,
                offset,
                0L,
                TimestampType.CREATE_TIME,
                -1,
                valueSize,
                null,
                new byte[valueSize],
                new RecordHeaders(),
                Optional.empty());
    }

    /** MockConsumer requires assignment before addRecord; the fetcher then keeps the same set. */
    private void assignWithLogStartZero(int... partitions) {
        for (int partition : partitions) {
            consumer.updateBeginningOffsets(Map.of(tp(partition), 0L));
        }
        consumer.assign(java.util.Arrays.stream(partitions)
                .mapToObj(KafkaSeekFetcherTest::tp)
                .toList());
    }

    /** Re-arms itself so every poll advances the clock — simulates slow polls deterministically. */
    private void tickOnEveryPoll(long millis) {
        consumer.schedulePollTask(new Runnable() {
            @Override
            public void run() {
                clock.advanceMillis(millis);
                consumer.schedulePollTask(this);
            }
        });
    }

    private double counter(String name) {
        return registry.get(name).counter().count();
    }

    @Test
    void foundAcrossPartitionsViaOneSequentialScanEach() {
        assignWithLogStartZero(0, 1);
        consumer.addRecord(record(0, 5, 10));
        consumer.addRecord(record(0, 7, 10));
        consumer.addRecord(record(1, 3, 10));
        FakeDueBatch batch = FakeDueBatch.of(new int[] {0, 0, 1}, new long[] {5, 7, 3});

        FetchResult result = fetcher.fetch(batch, 1 << 20, NOW + 30_000);

        assertEquals(FetchOutcome.FOUND, result.outcome(0));
        assertEquals(FetchOutcome.FOUND, result.outcome(1));
        assertEquals(FetchOutcome.FOUND, result.outcome(2));
        assertEquals(5, result.record(0).offset());
        assertEquals(7, result.record(1).offset());
        assertEquals(3, result.record(2).offset());
        assertEquals(0, result.carryOverIndices().length);

        List<FetchResult.PartitionSummary> summaries = result.partitionSummaries();
        assertEquals(new FetchResult.PartitionSummary(0, 2, 0, 0, 0, 20), summaries.get(0));
        assertEquals(new FetchResult.PartitionSummary(1, 1, 0, 0, 0, 10), summaries.get(1));

        assertEquals(3, counter("cesium.fetch.attempts"));
        assertEquals(0, counter("cesium.fetch.misses"));
        assertEquals(0, counter("cesium.fetch.unfetchable"));
        assertEquals(30, counter("cesium.fetch.bytes"));
        assertEquals(1, registry.get("cesium.fetch.duration.seconds").timer().count());
    }

    @Test
    void goneWhenLogStartPassedWantedOffsetWithoutAnyScan() {
        assignWithLogStartZero(0);
        consumer.updateBeginningOffsets(Map.of(tp(0), 10L)); // retention expired offsets < 10
        FakeDueBatch batch = FakeDueBatch.of(new int[] {0}, new long[] {5});

        FetchResult result = fetcher.fetch(batch, 1 << 20, NOW + 30_000);

        assertEquals(FetchOutcome.GONE, result.outcome(0));
        assertThrows(IllegalStateException.class, () -> result.record(0), "record() is FOUND-only");
        assertEquals(
                new FetchResult.PartitionSummary(0, 0, 1, 0, 0, 0),
                result.partitionSummaries().get(0));
        assertEquals(1, counter("cesium.fetch.attempts"));
        assertEquals(1, counter("cesium.fetch.unfetchable"));
        assertEquals(0, counter("cesium.fetch.misses"));
    }

    @Test
    void goneWhenScanPassesWantedOffsetUnderReadCommitted() {
        assignWithLogStartZero(0);
        consumer.addRecord(record(0, 6, 10)); // offset 5 compacted away: delivery skips to 6
        consumer.addRecord(record(0, 8, 10));
        FakeDueBatch batch = FakeDueBatch.of(new int[] {0, 0}, new long[] {5, 8});

        FetchResult result = fetcher.fetch(batch, 1 << 20, NOW + 30_000);

        assertEquals(FetchOutcome.GONE, result.outcome(0), "scan passed 5 without yielding it");
        assertEquals(FetchOutcome.FOUND, result.outcome(1));
        assertEquals(1, counter("cesium.fetch.unfetchable"));
    }

    @Test
    void transientWhenSliceExhaustsAndPayloadStillInRange() {
        assignWithLogStartZero(0); // log start 0 ≤ wanted: provably NOT expired, so never GONE
        tickOnEveryPoll(1_000);
        FakeDueBatch batch = FakeDueBatch.of(new int[] {0}, new long[] {5});

        FetchResult result = fetcher.fetch(batch, 1 << 20, NOW + 4_000);

        assertEquals(FetchOutcome.TRANSIENT, result.outcome(0));
        assertEquals(
                new FetchResult.PartitionSummary(0, 0, 0, 1, 0, 0),
                result.partitionSummaries().get(0));
        assertEquals(1, counter("cesium.fetch.misses"));
        assertEquals(0, counter("cesium.fetch.unfetchable"));
        assertEquals(1, counter("cesium.fetch.attempts"));
    }

    @Test
    void passEndRecheckFlipsTransientToGoneWhenRetentionCaughtUpMidPass() {
        assignWithLogStartZero(0);
        consumer.schedulePollTask(() -> {
            clock.advanceMillis(5_000); // blow the whole deadline in one poll...
            consumer.updateBeginningOffsets(Map.of(tp(0), 9L)); // ...while retention passes the offset
        });
        FakeDueBatch batch = FakeDueBatch.of(new int[] {0}, new long[] {5});

        FetchResult result = fetcher.fetch(batch, 1 << 20, NOW + 4_000);

        assertEquals(FetchOutcome.GONE, result.outcome(0), "§7.3: re-check beginningOffsets before classifying");
        assertEquals(1, counter("cesium.fetch.unfetchable"));
        assertEquals(0, counter("cesium.fetch.misses"), "a provably-expired entry must not penalty-box");
    }

    @Test
    void transportErrorMarksOnlyThatPartitionTransient() {
        assignWithLogStartZero(0, 1);
        consumer.addRecord(record(1, 3, 10));
        consumer.setPollException(new KafkaException("fetch failed"));
        FakeDueBatch batch = FakeDueBatch.of(new int[] {0, 1}, new long[] {5, 3});

        FetchResult result = fetcher.fetch(batch, 1 << 20, NOW + 30_000);

        assertEquals(FetchOutcome.TRANSIENT, result.outcome(0), "partition 0's run hit the exception");
        assertEquals(FetchOutcome.FOUND, result.outcome(1), "partition 1 is isolated from 0's transport failure");
        assertEquals(
                new FetchResult.PartitionSummary(0, 0, 0, 1, 0, 0),
                result.partitionSummaries().get(0));
        assertEquals(
                new FetchResult.PartitionSummary(1, 1, 0, 0, 0, 10),
                result.partitionSummaries().get(1));
    }

    @Test
    void byteBudgetTripsMidPartitionKeepingFetchedAndCarryingOverTheRest() {
        assignWithLogStartZero(0, 1);
        consumer.addRecord(record(0, 5, 100));
        consumer.addRecord(record(0, 6, 100));
        consumer.addRecord(record(0, 7, 100));
        consumer.addRecord(record(1, 3, 100));
        FakeDueBatch batch = FakeDueBatch.of(new int[] {0, 0, 0, 1}, new long[] {5, 6, 7, 3});

        FetchResult result = fetcher.fetch(batch, 150, NOW + 30_000);

        // Budget trips on the record that crosses it (sizes are only known as records arrive):
        // both fetched records proceed; everything else — same partition and all later runs —
        // carries over un-popped. Never 10 GB materialized: retention is bounded by budget + one
        // record.
        assertEquals(FetchOutcome.FOUND, result.outcome(0));
        assertEquals(FetchOutcome.FOUND, result.outcome(1));
        assertEquals(FetchOutcome.CARRY_OVER, result.outcome(2));
        assertEquals(FetchOutcome.CARRY_OVER, result.outcome(3), "later run never attempted after the trip");
        assertArrayEquals(new int[] {2, 3}, result.carryOverIndices());
        assertEquals(
                new FetchResult.PartitionSummary(0, 2, 0, 0, 1, 200),
                result.partitionSummaries().get(0));
        assertEquals(
                new FetchResult.PartitionSummary(1, 0, 0, 0, 1, 0),
                result.partitionSummaries().get(1));
        assertEquals(200, counter("cesium.fetch.bytes"));
        assertEquals(2, counter("cesium.fetch.attempts"), "carried-over entries were never attempted");
        assertEquals(0, counter("cesium.fetch.misses"), "a planned truncation is not a miss");
    }

    @Test
    void perPartitionSliceStopsOneSlowPartitionFromEatingTheDeadline() {
        assignWithLogStartZero(0, 1);
        consumer.addRecord(record(1, 3, 10));
        tickOnEveryPoll(1_000); // partition 0 never yields; every poll costs a simulated second
        FakeDueBatch batch = FakeDueBatch.of(new int[] {0, 1}, new long[] {5, 3});

        // Two partitions, 10 s overall ⇒ 5 s slice each (floor 2 s). Partition 0 burns exactly
        // its slice and no more; partition 1 still fetches inside the remaining deadline.
        FetchResult result = fetcher.fetch(batch, 1 << 20, NOW + 10_000);

        assertEquals(FetchOutcome.TRANSIENT, result.outcome(0));
        assertEquals(FetchOutcome.FOUND, result.outcome(1), "the slow partition could not consume the whole deadline");
        assertTrue(clock.millis() <= NOW + 10_000, "pass ended inside the overall deadline");
    }

    @Test
    void overallDeadlineExhaustionMarksNeverAttemptedRunsTransient() {
        assignWithLogStartZero(0, 1);
        tickOnEveryPoll(4_000); // first poll blows the 3 s deadline
        FakeDueBatch batch = FakeDueBatch.of(new int[] {0, 1}, new long[] {5, 3});

        FetchResult result = fetcher.fetch(batch, 1 << 20, NOW + 3_000);

        assertEquals(FetchOutcome.TRANSIENT, result.outcome(0));
        assertEquals(FetchOutcome.TRANSIENT, result.outcome(1), "never-attempted run resolves TRANSIENT, not dropped");
        assertEquals(2, counter("cesium.fetch.misses"));
    }

    @Test
    void alreadyExpiredDeadlineSkipsScansButStillProvesGoneViaRecheck() {
        // No pre-assignment: the fetcher assigns the batch partitions itself; the pass runs no
        // poll at all (deadline ≤ now), and the §7.3 pass-end re-check still classifies honestly.
        consumer.updateBeginningOffsets(Map.of(tp(0), 9L));
        FakeDueBatch batch = FakeDueBatch.of(new int[] {0}, new long[] {5});

        FetchResult result = fetcher.fetch(batch, 1 << 20, NOW);

        assertEquals(FetchOutcome.GONE, result.outcome(0));
        assertEquals(Set.of(tp(0)), consumer.assignment(), "fetcher assigned the batch partition");
    }

    @Test
    void offsetOutOfRangeMidScanResolvesExpiredEntriesAndContinues() {
        assignWithLogStartZero(0);
        consumer.addRecord(record(0, 8, 10));
        // Retention advances past the fetch position mid-scan: the next poll raises
        // OffsetOutOfRange (reset=none), which the fetcher treats as expiry evidence.
        consumer.schedulePollTask(() -> consumer.updateBeginningOffsets(Map.of(tp(0), 8L)));
        FakeDueBatch batch = FakeDueBatch.of(new int[] {0, 0}, new long[] {5, 8});

        FetchResult result = fetcher.fetch(batch, 1 << 20, NOW + 30_000);

        assertEquals(FetchOutcome.GONE, result.outcome(0), "out-of-range + re-probe proves expiry");
        assertEquals(FetchOutcome.FOUND, result.outcome(1), "scan re-seeks and finishes the run");
        assertEquals(8, result.record(1).offset());
    }

    @Test
    void warmAssignmentKeepsRecentPartitionsAndEvictsStaleOnes() {
        // Log starts above every wanted offset: each pass resolves GONE without polling, so the
        // test exercises pure assignment bookkeeping.
        consumer.updateBeginningOffsets(Map.of(tp(0), 100L, tp(1), 100L));

        fetcher.fetch(FakeDueBatch.of(new int[] {0}, new long[] {5}), 1 << 20, NOW + 1_000);
        assertEquals(Set.of(tp(0)), consumer.assignment());

        fetcher.fetch(FakeDueBatch.of(new int[] {1}, new long[] {5}), 1 << 20, NOW + 1_000);
        assertEquals(Set.of(tp(0), tp(1)), consumer.assignment(), "§7.5: union of recent passes, not churn");

        // Passes 3..8 keep partition 0 warm (last seen pass 1 > pass − WARM_PASSES)...
        for (int pass = 3; pass <= KafkaSeekFetcher.WARM_PASSES; pass++) {
            fetcher.fetch(FakeDueBatch.of(new int[] {1}, new long[] {5}), 1 << 20, NOW + 1_000);
        }
        assertEquals(Set.of(tp(0), tp(1)), consumer.assignment());

        // ...and pass 9 evicts it.
        fetcher.fetch(FakeDueBatch.of(new int[] {1}, new long[] {5}), 1 << 20, NOW + 1_000);
        assertEquals(Set.of(tp(1)), consumer.assignment(), "stale partition evicted after WARM_PASSES passes");
    }

    @Test
    void duplicateWantedOffsetsResolveAgainstTheSameRecord() {
        assignWithLogStartZero(0);
        consumer.addRecord(record(0, 5, 10));
        FakeDueBatch batch = FakeDueBatch.of(new int[] {0, 0}, new long[] {5, 5});

        FetchResult result = fetcher.fetch(batch, 1 << 20, NOW + 30_000);

        assertEquals(FetchOutcome.FOUND, result.outcome(0));
        assertEquals(FetchOutcome.FOUND, result.outcome(1));
        assertSame(result.record(0), result.record(1));
    }

    @Test
    void byteAccountingPrefersSerializedSizesOverArrayLengths() {
        // The serialized sizes are the client's post-decompression measurement — they win even
        // when a fabricated record disagrees with its array lengths.
        ConsumerRecord<byte[], byte[]> sized = new ConsumerRecord<>(
                TOPIC,
                0,
                5,
                0L,
                TimestampType.CREATE_TIME,
                7,
                1_000,
                new byte[2],
                new byte[10],
                new RecordHeaders(),
                Optional.empty());
        assertEquals(1_007, KafkaSeekFetcher.recordBytes(sized));

        // Records fabricated without sizes (-1) fall back to array lengths — identical values
        // under byte-array serdes.
        ConsumerRecord<byte[], byte[]> unsized = new ConsumerRecord<>(TOPIC, 0, 5, new byte[3], new byte[10]);
        assertEquals(13, KafkaSeekFetcher.recordBytes(unsized));
        ConsumerRecord<byte[], byte[]> nullKey = new ConsumerRecord<>(TOPIC, 0, 5, null, new byte[10]);
        assertEquals(10, KafkaSeekFetcher.recordBytes(nullKey));
    }

    @Test
    void serializedSizesDriveTheBudgetTrip() {
        assignWithLogStartZero(0);
        consumer.addRecord(new ConsumerRecord<>(
                TOPIC,
                0,
                5,
                0L,
                TimestampType.CREATE_TIME,
                -1,
                1_000, // decompressed size; the array is deliberately smaller
                null,
                new byte[10],
                new RecordHeaders(),
                Optional.empty()));
        FakeDueBatch batch = FakeDueBatch.of(new int[] {0, 0}, new long[] {5, 6});

        FetchResult result = fetcher.fetch(batch, 500, NOW + 30_000);

        assertEquals(FetchOutcome.FOUND, result.outcome(0), "the tripping record itself proceeds");
        assertEquals(FetchOutcome.CARRY_OVER, result.outcome(1));
        assertEquals(1_000, counter("cesium.fetch.bytes"));
    }

    @Test
    void emptyBatchTouchesNothing() {
        FetchResult result = fetcher.fetch(FakeDueBatch.empty(), 1 << 20, NOW + 30_000);
        assertEquals(0, result.size());
        assertTrue(result.partitionSummaries().isEmpty());
        assertEquals(0, result.carryOverIndices().length);
        assertTrue(consumer.assignment().isEmpty(), "no consumer interaction for an empty batch");
        assertEquals(0, registry.get("cesium.fetch.duration.seconds").timer().count());
    }

    @Test
    void wakeupPropagatesAsShutdownSignal() {
        assignWithLogStartZero(0);
        consumer.wakeup();
        FakeDueBatch batch = FakeDueBatch.of(new int[] {0}, new long[] {5});
        assertThrows(WakeupException.class, () -> fetcher.fetch(batch, 1 << 20, NOW + 30_000));
    }

    @Test
    void closeIsIdempotentAndFetchAfterCloseFails() {
        fetcher.close();
        fetcher.close();
        assertTrue(consumer.closed());
        FakeDueBatch batch = FakeDueBatch.of(new int[] {0}, new long[] {5});
        assertThrows(IllegalStateException.class, () -> fetcher.fetch(batch, 1 << 20, NOW + 1));
    }

    /** Manually advanced clock: budget gating is tested without real waiting. */
    private static final class MutableClock extends Clock {
        private long nowMs;

        MutableClock(long nowMs) {
            this.nowMs = nowMs;
        }

        void advanceMillis(long ms) {
            nowMs += ms;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(nowMs);
        }
    }
}
