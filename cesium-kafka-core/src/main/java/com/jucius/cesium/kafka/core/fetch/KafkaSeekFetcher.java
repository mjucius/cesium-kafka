package com.jucius.cesium.kafka.core.fetch;

import com.jucius.cesium.kafka.api.store.DueBatch;
import com.jucius.cesium.kafka.core.headers.DelayHeaderCodec;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.errors.WakeupException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The §7 seek fetcher: owns a <strong>group-less</strong> {@code assign()}/{@code seek()} consumer
 * (locked to {@code read_committed} and {@code auto.offset.reset=none}, byte-array serdes — see
 * {@code KafkaClientFactory.seekConsumerProperties()}) and serves each due batch with one
 * {@code seek(minWantedOffset)} + forward sequential scan per source partition, so a
 * midnight-herd of 10k entries due from one partition is a single sequential pass, never 10k
 * random seeks (§7.2).
 *
 * <p><strong>The three budgets (§7.2):</strong>
 *
 * <ol>
 *   <li><em>Per-partition slice</em> — {@code max((deadline − start) / distinct-partitions,
 *       partition-time-floor)}: one slow partition (cold segments, degraded broker) cannot consume
 *       the whole deadline; later partitions keep their floor.
 *   <li><em>Overall deadline</em> — the absolute {@code deadlineMs} the dispatch loop derives from
 *       {@code dispatch.fetch.timeout}; runs that never start before it expires resolve
 *       {@code TRANSIENT} (after the beginning-offsets re-check below), never silently.
 *   <li><em>Decompressed-byte budget</em> — {@code maxBytes} accumulated as records arrive, where
 *       sizes first become known; on trip the pass truncates: fetched entries proceed, every
 *       not-yet-fetched candidate is {@link FetchOutcome#CARRY_OVER} (D8) — a 10k-entry batch of
 *       1 MB records becomes ~32 MiB passes, never 10 GB of heap. The budget can overshoot by at
 *       most the records of one poll, and only the records up to the tripping one are retained.
 * </ol>
 *
 * <p><strong>Classification evidence (§7.3):</strong> {@code GONE} requires proof — the
 * partition's log-start offset (probed once per pass) exceeds the wanted offset, or the scan
 * passed the offset without yielding it. The scan-passed rule is sound on two verified client
 * behaviors: a {@code read_committed} consumer delivers nothing above an open transaction's LSO
 * (so a delivered higher offset proves the wanted one is not pending visibility), and
 * {@code position()} advances across aborted batches and control records during {@code poll()}
 * even when zero records reach the application — a wanted offset left behind by either signal was
 * compacted or deleted. Everything else — slice/deadline exhaustion, transport errors — is
 * re-checked against {@code beginningOffsets} once at pass end (retention may have caught up
 * mid-pass) and the survivors classify {@code TRANSIENT}: still pending, never dropped, and the
 * dispatch loop penalty-boxes their source partitions (D22, D-8). Transport failures are
 * classified per partition rather than thrown, so one partition's broker trouble cannot forfeit
 * the whole pass; only process-level signals ({@link WakeupException}, {@link InterruptException},
 * authentication/authorization, {@link IllegalStateException}) propagate.
 *
 * <p><strong>Out-of-range as evidence, not error:</strong> with {@code auto.offset.reset=none}, a
 * mid-scan {@link OffsetOutOfRangeException} means the log start passed the fetch position —
 * exactly the §7.3 expiry signal. The fetcher re-probes the beginning offset, resolves the
 * provably-expired candidates {@code GONE}, re-seeks, and continues; an automatic reset would have
 * silently masked the loss class instead.
 *
 * <p><strong>Warm assignment (§7.5):</strong> the consumer keeps the union of partitions seen in
 * the last {@value #WARM_PASSES} passes assigned (all paused; each run resumes exactly its own
 * partition for its slice), so steady traffic reuses broker fetch sessions instead of churning
 * assignment. Positions are seeded for every batch partition before the first poll because a
 * consumer locked to {@code auto.offset.reset=none} fails its poll if any assigned partition —
 * paused or not — lacks a position.
 *
 * <p><strong>Byte accounting ("decompressed", §7.2):</strong> the client decompresses fetched
 * batches before handing records over, and {@link ConsumerRecord#serializedKeySize()} /
 * {@link ConsumerRecord#serializedValueSize()} report the per-record serialized sizes <em>after</em>
 * that decompression — with byte-array deserializers these are exactly the materialized payload
 * bytes the budget protects the heap from. Records fabricated without sizes (a {@code MockConsumer}
 * convenience) fall back to the key/value array lengths, which are identical under byte-array
 * serdes.
 *
 * <p><strong>Threading (§6):</strong> confined to the single dispatch thread that owns the seek
 * consumer; not thread-safe. Runs strictly <em>outside</em> the dispatch transaction (§3.2).
 */
public final class KafkaSeekFetcher implements SeekFetcher {

    private static final Logger log = LoggerFactory.getLogger(KafkaSeekFetcher.class);

    /** Passes a partition stays in the warm assignment after it last carried a candidate (§7.5). */
    static final int WARM_PASSES = 8;

    /**
     * Bound on the {@code beginningOffsets} probes (the pass-start provably-expired pre-check and
     * the §7.3 pass-end re-check). The re-check may extend a fully exhausted pass by at most this
     * long — the price of never classifying {@code TRANSIENT} on stale evidence.
     */
    static final Duration OFFSET_PROBE_TIMEOUT = Duration.ofSeconds(5);

    /** Bound on consumer close (§6: shutdown is bounded, then hard abort). */
    static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(10);

    private final String sourceTopic;
    private final long partitionTimeFloorMs;
    private final Consumer<byte[], byte[]> consumer;
    private final Clock clock;

    private final Counter attempts;
    private final Counter misses;
    private final Counter unfetchable;
    private final Counter fetchedBytes;
    private final Timer fetchDuration;

    // Warm-assignment bookkeeping: source partition → the pass that last wanted it (§7.5).
    private final Map<TopicPartition, Long> warmAssignment = new HashMap<>();
    private long passCounter;
    private boolean closed;

    /**
     * @param sourceTopic the topic the wanted payloads live on ({@code route.source})
     * @param partitionTimeFloor minimum per-partition slice ({@code dispatch.fetch
     *     .partition-time-floor}, §7.2 budget a)
     * @param consumer the group-less seek consumer ({@code KafkaClientFactory.newSeekConsumer()});
     *     the fetcher owns its lifecycle from here on
     * @param meterRegistry metrics sink for the §9 {@code cesium_fetch_*} inventory
     * @param clock injectable time source driving every budget; the fetcher never reads wall time
     *     directly
     */
    public KafkaSeekFetcher(
            String sourceTopic,
            Duration partitionTimeFloor,
            Consumer<byte[], byte[]> consumer,
            MeterRegistry meterRegistry,
            Clock clock) {
        this.sourceTopic = Objects.requireNonNull(sourceTopic, "sourceTopic");
        this.partitionTimeFloorMs =
                Objects.requireNonNull(partitionTimeFloor, "partitionTimeFloor").toMillis();
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.clock = Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(meterRegistry, "meterRegistry");
        this.attempts = meterRegistry.counter("cesium.fetch.attempts");
        this.misses = meterRegistry.counter("cesium.fetch.misses");
        this.unfetchable = meterRegistry.counter("cesium.fetch.unfetchable");
        this.fetchedBytes = meterRegistry.counter("cesium.fetch.bytes");
        this.fetchDuration = Timer.builder("cesium.fetch.duration.seconds")
                .description("one seek-fetch pass (§9); attributes cold-segment / degraded-broker cost")
                .publishPercentileHistogram()
                .register(meterRegistry);
    }

    @Override
    public FetchResult fetch(DueBatch candidates, long maxBytes, long deadlineMs) {
        Objects.requireNonNull(candidates, "candidates");
        if (closed) {
            throw new IllegalStateException("seek fetcher already closed");
        }
        FetchCandidates grouped = FetchCandidates.of(candidates);
        PassResult result = new PassResult(candidates);
        if (grouped.runs().isEmpty()) {
            result.seal(grouped);
            return result;
        }
        long startMs = clock.millis();
        refreshAssignment(grouped);
        // A pass whose deadline already expired (the loop overran its drain slice) skips the
        // probe and every scan; its candidates resolve TRANSIENT via the pass-end re-check.
        boolean deadlineAhead = startMs < deadlineMs;
        Map<TopicPartition, Long> beginnings = deadlineAhead ? probeBeginningOffsets(grouped) : null;
        if (deadlineAhead) {
            seedPositions(grouped, beginnings == null ? Map.of() : beginnings);
        }
        long sliceMs = Math.max((deadlineMs - startMs) / grouped.runs().size(), partitionTimeFloorMs);
        PassState state = new PassState(maxBytes, deadlineMs);
        for (FetchCandidates.PartitionRun run : grouped.runs()) {
            if (state.byteTripped) {
                // Truncate-and-carry-over (D8): nothing failed — the budget planned this cut.
                markCarriedOver(result, run, 0);
                continue;
            }
            int next = preclassifyGone(result, run, beginnings);
            if (next >= run.entryCount()) {
                continue; // the log-start probe resolved the whole run without a scan
            }
            if (clock.millis() >= deadlineMs) {
                markTransient(result, run, next); // deadline exhausted before this run started
                continue;
            }
            scanRun(result, state, run, next, sliceMs);
        }
        recheckTransients(result);
        result.seal(grouped);
        attempts.increment(result.foundCount + result.goneCount + result.transientCount);
        misses.increment(result.transientCount);
        unfetchable.increment(result.goneCount);
        fetchedBytes.increment((double) result.totalBytes);
        fetchDuration.record(Duration.ofMillis(Math.max(0, clock.millis() - startMs)));
        if (log.isDebugEnabled()) {
            log.debug(
                    "seek-fetch pass: {} candidate(s) over {} partition(s) → found={} gone={} transient={}"
                            + " carryOver={} bytes={}",
                    candidates.size(),
                    grouped.runs().size(),
                    result.foundCount,
                    result.goneCount,
                    result.transientCount,
                    result.carryOverCount,
                    result.totalBytes);
        }
        return result;
    }

    /** Releases the seek consumer (bounded). Idempotent. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            consumer.close(CloseOptions.timeout(CLOSE_TIMEOUT));
        } catch (RuntimeException e) {
            log.warn("seek consumer close failed", e);
        }
    }

    /**
     * Maintains the §7.5 warm assignment: the union of partitions seen in the last
     * {@value #WARM_PASSES} passes, everything paused. Each run resumes exactly its own partition
     * for its slice, so a poll only ever fetches the partition being scanned while idle-but-warm
     * partitions keep their broker fetch sessions alive.
     */
    private void refreshAssignment(FetchCandidates grouped) {
        passCounter++;
        for (FetchCandidates.PartitionRun run : grouped.runs()) {
            warmAssignment.put(topicPartition(run.sourcePartition()), passCounter);
        }
        warmAssignment.values().removeIf(lastSeen -> lastSeen <= passCounter - WARM_PASSES);
        Set<TopicPartition> desired = Set.copyOf(warmAssignment.keySet());
        if (!consumer.assignment().equals(desired)) {
            consumer.assign(desired);
        }
        consumer.pause(desired);
    }

    /**
     * Seeds a position for every batch partition before the first poll. Load-bearing under the
     * locked {@code auto.offset.reset=none}: {@code poll()} initializes positions for <em>all</em>
     * assigned partitions — paused ones included — and fails with
     * {@code NoOffsetForPartitionException} for any partition that has never been seeked. Warm
     * partitions outside this batch keep the position their last scan left. The target is bumped
     * to the known log start so a wholly-expired run never even seeks below range.
     */
    private void seedPositions(FetchCandidates grouped, Map<TopicPartition, Long> beginnings) {
        for (FetchCandidates.PartitionRun run : grouped.runs()) {
            TopicPartition partition = topicPartition(run.sourcePartition());
            long target = run.minWantedOffset();
            Long beginning = beginnings.get(partition);
            if (beginning != null && beginning > target) {
                target = beginning;
            }
            consumer.seek(partition, target);
        }
    }

    /**
     * The pass-start log-start probe backing the {@code beginningOffsets(partition) > wantedOffset
     * ⇒ GONE} rule (§7.3). Returns {@code null} when the probe fails transiently — the pass then
     * proceeds on scan evidence alone and the pass-end re-check still runs.
     */
    private @Nullable Map<TopicPartition, Long> probeBeginningOffsets(FetchCandidates grouped) {
        List<TopicPartition> partitions = new ArrayList<>(grouped.runs().size());
        for (FetchCandidates.PartitionRun run : grouped.runs()) {
            partitions.add(topicPartition(run.sourcePartition()));
        }
        try {
            return consumer.beginningOffsets(partitions, OFFSET_PROBE_TIMEOUT);
        } catch (RuntimeException e) {
            if (isFatal(e)) {
                throw e;
            }
            log.warn("beginningOffsets pre-check failed; proceeding without provably-expired pre-classification", e);
            return null;
        }
    }

    /** Resolves the run's leading candidates the log-start probe proved expired; returns the first survivor. */
    private int preclassifyGone(
            PassResult result, FetchCandidates.PartitionRun run, @Nullable Map<TopicPartition, Long> beginnings) {
        if (beginnings == null) {
            return 0;
        }
        Long beginning = beginnings.get(topicPartition(run.sourcePartition()));
        if (beginning == null) {
            return 0;
        }
        int j = 0;
        while (j < run.entryCount() && run.wantedOffset(j) < beginning) {
            result.gone(run.batchIndex(j));
            j++;
        }
        return j;
    }

    /**
     * One partition's slice: resume it (everything else stays paused), seek to the first
     * unresolved wanted offset, and poll forward collecting wanted offsets until the run resolves,
     * the slice/deadline expires ({@code TRANSIENT}), or the byte budget trips
     * ({@code CARRY_OVER}). Transport failures resolve the run's remainder {@code TRANSIENT} and
     * the pass moves on — per-partition isolation, not pass forfeiture.
     */
    private void scanRun(
            PassResult result, PassState state, FetchCandidates.PartitionRun run, int start, long sliceMs) {
        TopicPartition partition = topicPartition(run.sourcePartition());
        long sliceDeadlineMs = Math.min(DelayHeaderCodec.saturatedAddMillis(clock.millis(), sliceMs), state.deadlineMs);
        int j = start;
        consumer.resume(List.of(partition));
        try {
            consumer.seek(partition, run.wantedOffset(j));
            while (j < run.entryCount()) {
                long remainingMs = sliceDeadlineMs - clock.millis();
                if (remainingMs <= 0) {
                    markTransient(result, run, j); // re-checked against beginningOffsets at pass end
                    return;
                }
                final ConsumerRecords<byte[], byte[]> polled;
                try {
                    polled = consumer.poll(Duration.ofMillis(remainingMs));
                } catch (OffsetOutOfRangeException outOfRange) {
                    // The log start passed the fetch position mid-scan (retention caught up):
                    // under the locked reset=none this is the §7.3 expiry signal, not an error.
                    j = resolveOutOfRange(result, run, j, partition);
                    if (j < 0) {
                        return; // the re-probe failed; the remainder is already TRANSIENT
                    }
                    continue;
                }
                j = applyRecords(result, state, run, j, polled.records(partition));
                if (state.byteTripped) {
                    markCarriedOver(result, run, j);
                    return;
                }
                if (j >= run.entryCount()) {
                    return;
                }
                // poll() advances position across aborted batches and control records even when
                // zero records reach the application — a wanted offset left below the position
                // was provably compacted/deleted (scan passed it).
                long position = consumer.position(partition);
                while (j < run.entryCount() && run.wantedOffset(j) < position) {
                    result.gone(run.batchIndex(j));
                    j++;
                }
            }
        } catch (RuntimeException e) {
            if (isFatal(e)) {
                throw e;
            }
            log.warn(
                    "seek-fetch run for {} failed transiently; {} candidate(s) stay pending for the penalty box"
                            + " (§7.3)",
                    partition,
                    run.entryCount() - j,
                    e);
            markTransient(result, run, j);
        } finally {
            try {
                consumer.pause(List.of(partition));
            } catch (RuntimeException e) {
                log.warn("re-pausing {} after its slice failed", partition, e);
            }
        }
    }

    /**
     * Applies one poll's records to the run: wanted offsets the scan passed without yielding are
     * {@code GONE}; matches are {@code FOUND} and accumulate the byte budget, tripping it exactly
     * where sizes become known (§7.2). Returns the first unresolved run index.
     */
    private int applyRecords(
            PassResult result,
            PassState state,
            FetchCandidates.PartitionRun run,
            int j,
            List<ConsumerRecord<byte[], byte[]>> records) {
        for (ConsumerRecord<byte[], byte[]> record : records) {
            while (j < run.entryCount() && run.wantedOffset(j) < record.offset()) {
                result.gone(run.batchIndex(j)); // delivered past it under read_committed ⇒ provably gone
                j++;
            }
            while (j < run.entryCount() && run.wantedOffset(j) == record.offset()) {
                long bytes = recordBytes(record);
                result.found(run.batchIndex(j), record, bytes);
                state.bytesUsed += bytes;
                j++;
            }
            if (state.bytesUsed >= state.byteBudget) {
                state.byteTripped = true; // fetched entries proceed; the rest carries over (D8)
                return j;
            }
            if (j >= run.entryCount()) {
                return j;
            }
        }
        return j;
    }

    /**
     * Turns a mid-scan {@link OffsetOutOfRangeException} into evidence: re-probe the log start,
     * resolve the provably-expired candidates {@code GONE}, re-seek to the first survivor, and let
     * the scan continue. Returns the first unresolved run index, or {@code -1} when the re-probe
     * itself failed (the remainder is then {@code TRANSIENT}).
     */
    private int resolveOutOfRange(
            PassResult result, FetchCandidates.PartitionRun run, int j, TopicPartition partition) {
        final Map<TopicPartition, Long> probed;
        try {
            probed = consumer.beginningOffsets(List.of(partition), OFFSET_PROBE_TIMEOUT);
        } catch (RuntimeException e) {
            if (isFatal(e)) {
                throw e;
            }
            log.warn("beginningOffsets re-probe after out-of-range failed for {}", partition, e);
            markTransient(result, run, j);
            return -1;
        }
        Long beginning = probed.get(partition);
        if (beginning == null) {
            markTransient(result, run, j);
            return -1;
        }
        while (j < run.entryCount() && run.wantedOffset(j) < beginning) {
            result.gone(run.batchIndex(j));
            j++;
        }
        consumer.seek(partition, j < run.entryCount() ? Math.max(run.wantedOffset(j), beginning) : beginning);
        return j;
    }

    /**
     * The §7.3 "re-check {@code beginningOffsets} before classifying" step, batched once per pass:
     * retention may have caught up while the pass ran, turning a would-be {@code TRANSIENT} (which
     * would penalty-box the partition and retry forever) into a provable {@code GONE} (which the
     * unfetchable policy resolves exactly once). A failed re-check leaves {@code TRANSIENT}
     * standing — the conservative direction.
     */
    private void recheckTransients(PassResult result) {
        if (result.transientIndices.isEmpty()) {
            return;
        }
        Set<TopicPartition> partitions = new HashSet<>();
        for (int i : result.transientIndices) {
            partitions.add(topicPartition(result.batch.sourcePartition(i)));
        }
        final Map<TopicPartition, Long> beginnings;
        try {
            beginnings = consumer.beginningOffsets(partitions, OFFSET_PROBE_TIMEOUT);
        } catch (RuntimeException e) {
            if (isFatal(e)) {
                throw e;
            }
            log.debug("pass-end beginningOffsets re-check failed; TRANSIENT classifications stand", e);
            return;
        }
        for (int i : result.transientIndices) {
            Long beginning = beginnings.get(topicPartition(result.batch.sourcePartition(i)));
            if (beginning != null && result.batch.sourceOffset(i) < beginning) {
                result.flipTransientToGone(i);
            }
        }
    }

    /** Marks the run's remainder {@code TRANSIENT} (provisional until the pass-end re-check). */
    private static void markTransient(PassResult result, FetchCandidates.PartitionRun run, int j) {
        for (; j < run.entryCount(); j++) {
            result.transientMiss(run.batchIndex(j));
        }
    }

    /** Marks the run's remainder {@code CARRY_OVER} — the byte budget's planned truncation (D8). */
    private static void markCarriedOver(PassResult result, FetchCandidates.PartitionRun run, int j) {
        for (; j < run.entryCount(); j++) {
            result.carryOver(run.batchIndex(j));
        }
    }

    /**
     * Decompressed bytes one record materializes (§7.2): the client-reported serialized key+value
     * sizes (measured after fetch decompression), falling back to array lengths for records
     * fabricated without sizes — identical values under byte-array serdes.
     */
    // TODO(§7.2 sizing refinement): header bytes and per-object overhead are not counted; if a
    // real-path budget audit shows header-heavy payloads mattering, extend this to sum Header
    // key/value lengths. The payload-dominant cases the budget exists for are covered by the
    // serialized key+value sizes.
    static long recordBytes(ConsumerRecord<byte[], byte[]> record) {
        byte[] key = record.key();
        byte[] value = record.value();
        long keyBytes = record.serializedKeySize() >= 0 ? record.serializedKeySize() : (key == null ? 0 : key.length);
        long valueBytes =
                record.serializedValueSize() >= 0 ? record.serializedValueSize() : (value == null ? 0 : value.length);
        return keyBytes + valueBytes;
    }

    private TopicPartition topicPartition(int partition) {
        return new TopicPartition(sourceTopic, partition);
    }

    /**
     * Process-level signals that must propagate (shutdown, auth, engine bugs); everything else is
     * per-partition transport trouble the tri-state classifies (§7.3). Walks the cause chain like
     * the §3.8 taxonomy classifiers.
     */
    private static boolean isFatal(RuntimeException error) {
        for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof WakeupException
                    || t instanceof InterruptException
                    || t instanceof AuthenticationException
                    || t instanceof AuthorizationException
                    || t instanceof UnsupportedVersionException
                    || t instanceof IllegalStateException) {
                return true;
            }
        }
        return false;
    }

    /** The pass-wide budget state shared across partition runs (§7.2 budget c). */
    private static final class PassState {
        final long byteBudget;
        final long deadlineMs;
        long bytesUsed;
        boolean byteTripped;

        PassState(long byteBudget, long deadlineMs) {
            this.byteBudget = byteBudget;
            this.deadlineMs = deadlineMs;
        }
    }

    /**
     * The pass's {@link FetchResult}: parallel outcome/record arrays indexed like the candidate
     * batch, sealed (validated + summarized) before it leaves {@link #fetch}. Allocated fresh per
     * pass; valid until the next one per the interface contract.
     */
    private static final class PassResult implements FetchResult {

        final DueBatch batch;
        final List<Integer> transientIndices = new ArrayList<>();

        private final @Nullable FetchOutcome[] outcomes;
        private final @Nullable ConsumerRecord<byte[], byte[]>[] records;
        private final long[] foundBytes;

        private List<PartitionSummary> summaries = List.of();
        int foundCount;
        int goneCount;
        int transientCount;
        int carryOverCount;
        long totalBytes;

        PassResult(DueBatch batch) {
            this.batch = batch;
            this.outcomes = new FetchOutcome[batch.size()];
            this.records = newRecordArray(batch.size());
            this.foundBytes = new long[batch.size()];
        }

        @SuppressWarnings("unchecked")
        private static @Nullable ConsumerRecord<byte[], byte[]>[] newRecordArray(int size) {
            return (ConsumerRecord<byte[], byte[]>[]) new ConsumerRecord<?, ?>[size];
        }

        void found(int i, ConsumerRecord<byte[], byte[]> record, long bytes) {
            classify(i, FetchOutcome.FOUND);
            records[i] = record;
            foundBytes[i] = bytes;
        }

        void gone(int i) {
            classify(i, FetchOutcome.GONE);
        }

        void transientMiss(int i) {
            classify(i, FetchOutcome.TRANSIENT);
            transientIndices.add(i);
        }

        void carryOver(int i) {
            classify(i, FetchOutcome.CARRY_OVER);
        }

        private void classify(int i, FetchOutcome outcome) {
            if (outcomes[i] != null) {
                throw new IllegalStateException(
                        "candidate " + i + " classified twice: " + outcomes[i] + " then " + outcome);
            }
            outcomes[i] = outcome;
        }

        /** The §7.3 re-check upgrade: provisional {@code TRANSIENT} → provable {@code GONE}. */
        void flipTransientToGone(int i) {
            if (outcomes[i] != FetchOutcome.TRANSIENT) {
                throw new IllegalStateException("candidate " + i + " is not TRANSIENT: " + outcomes[i]);
            }
            outcomes[i] = FetchOutcome.GONE;
        }

        /** Validates every candidate classified exactly once and computes the partition summaries. */
        void seal(FetchCandidates grouped) {
            List<PartitionSummary> built = new ArrayList<>(grouped.runs().size());
            for (FetchCandidates.PartitionRun run : grouped.runs()) {
                int found = 0;
                int gone = 0;
                int transientFailures = 0;
                int carriedOver = 0;
                long bytes = 0;
                for (int j = 0; j < run.entryCount(); j++) {
                    int i = run.batchIndex(j);
                    FetchOutcome outcome = outcomes[i];
                    if (outcome == null) {
                        throw new IllegalStateException("candidate " + i + " left unclassified — engine bug");
                    }
                    switch (outcome) {
                        case FOUND -> {
                            found++;
                            bytes += foundBytes[i];
                        }
                        case GONE -> gone++;
                        case TRANSIENT -> transientFailures++;
                        case CARRY_OVER -> carriedOver++;
                    }
                }
                built.add(new PartitionSummary(
                        run.sourcePartition(), found, gone, transientFailures, carriedOver, bytes));
                foundCount += found;
                goneCount += gone;
                transientCount += transientFailures;
                carryOverCount += carriedOver;
                totalBytes += bytes;
            }
            summaries = List.copyOf(built);
        }

        @Override
        public int size() {
            return batch.size();
        }

        @Override
        public FetchOutcome outcome(int i) {
            FetchOutcome outcome = outcomes[i];
            if (outcome == null) {
                throw new IllegalStateException("candidate " + i + " not classified");
            }
            return outcome;
        }

        @Override
        public ConsumerRecord<byte[], byte[]> record(int i) {
            ConsumerRecord<byte[], byte[]> record = records[i];
            if (record == null) {
                throw new IllegalStateException("candidate " + i + " has no record: outcome is " + outcomes[i]);
            }
            return record;
        }

        @Override
        public List<PartitionSummary> partitionSummaries() {
            return summaries;
        }
    }
}
