package com.jucius.cesium.kafka.core.ingest;

import com.jucius.cesium.kafka.api.store.ScheduledRef;
import com.jucius.cesium.kafka.api.store.TrackerBackedStore;
import com.jucius.cesium.kafka.api.store.TrackerRecordData;
import com.jucius.cesium.kafka.core.admin.IdentityBlob;
import com.jucius.cesium.kafka.core.headers.DelayHeaderCodec;
import com.jucius.cesium.kafka.core.headers.DlqReasons;
import com.jucius.cesium.kafka.core.headers.HeaderParseResult;
import com.jucius.cesium.kafka.core.headers.RelayRecordFactory;
import com.jucius.cesium.kafka.core.policy.IngestDecision;
import com.jucius.cesium.kafka.core.policy.IngestPolicyEngine;
import com.jucius.cesium.kafka.core.policy.UnrelayablePolicy;
import com.jucius.cesium.kafka.core.policy.UnrelayableRejections;
import com.jucius.cesium.kafka.core.testing.CrashPoints;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.errors.WakeupException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ingest loop (design §3.1): single-threaded owner of the group-A source consumer and the
 * ingest transactional producer. Per non-empty poll batch, one transaction carries the immediate
 * relays, the tracker ADDs (same partition number as the source record), the policy DLQ records,
 * and the source-offset advancement — atomically (I1). Offsets are sent with
 * {@code consumer.groupMetadata()} fetched immediately before the call (I2) and carry the
 * {@linkplain IdentityBlob identity blob} binding them to the cluster id and source topic id
 * (§3.1, R17); whenever the loop reads committed offsets back (the in-doubt recovery's offset
 * fetch) it verifies the recorded blob against its own identity and fail-fasts on mismatch
 * (R-10's "at every offset fetch" half — startup is covered by the admin validation).
 *
 * <p><strong>I3 by construction:</strong> {@code begin..commit} completes inside one
 * {@link #runOnce() iteration}; {@code poll()} is never called while a transaction is open, so
 * rebalance callbacks (which run inside {@code poll()} on this thread) can never observe an
 * in-flight transaction. The callbacks themselves do O(1) bookkeeping only — the ingest loop is
 * stateless, so there is nothing to rebuild or flush.
 *
 * <p><strong>Error taxonomy (§3.8):</strong>
 *
 * <ul>
 *   <li><em>Definitively abortable</em> (any non-fatal failure before the commit call, or a
 *       non-ambiguous <em>first-attempt</em> commit failure): {@code abortTransaction}, seek the
 *       consumer back to the failed batch's first offsets (= the last-committed positions, since
 *       every prior batch committed or rewound before the next poll), and retry by re-polling —
 *       with consecutive failures paced by capped exponential backoff realized as {@code pause()}
 *       + heartbeat polling, so group membership stays alive while the loop waits.
 *   <li><em>Unrelayable</em> (§3.8 I-8, M2): a relay the destination <em>permanently</em> rejects
 *       (record too large / invalid — {@link UnrelayableRejections}) is not transient and must not
 *       retry forever (which would wedge the whole source partition). The batch aborts, the
 *       offending record is attributed via the send callback, and on the rewound reprocess it is
 *       routed by {@code route.relay.on-unrelayable} (DLQ record / drop) atomically with the
 *       source-offset advance — never a failure-streak event, so a poison record never parks the
 *       loop.
 *   <li><em>Fatal</em> ({@link ProducerFencedException}, {@link OutOfOrderSequenceException},
 *       authentication/authorization, fenced instance id, the I-7 tracker-partition fail-fast,
 *       …): close clients, fail the loop.
 *   <li><em>In-doubt</em> ({@link TimeoutException} from {@code commitTransaction} — and, once
 *       any commit attempt has timed out, <em>every</em> later non-fatal failure: after an
 *       ambiguous attempt the broker may already have committed, so no subsequent exception can
 *       prove "aborted" (D20); only the in-doubt procedure is sound under both outcomes): retry
 *       the commit to a definitive outcome, bounded by {@code kafka.transactions.commit-retry};
 *       if still ambiguous, run the I-4 procedure — close and recreate the producer,
 *       {@code initTransactions()} (resolves the dangling transaction broker-side without
 *       reporting the outcome), and re-poll from the <em>committed</em> offsets. This is safe
 *       precisely because ingest is stateless and offsets materialize only at commit: if the
 *       broker committed, the committed offsets are past the batch and nothing is reprocessed; if
 *       it aborted, they are before the batch and everything is reprocessed — exactly once either
 *       way, with no in-memory state to restore (I9 analog). The recovery itself is retryable
 *       (R15): a transient failure while replacing the producer or fetching committed offsets
 *       schedules another attempt under the same capped backoff instead of crash-looping the
 *       process under exactly the broker degradation that caused the ambiguity.
 *   <li><em>Retry exhaustion</em> (park-and-degrade, R15): after {@code parkThreshold}
 *       consecutive failures the {@code cesium_degraded{loop="ingest"}} gauge rises and logging
 *       escalates, but the loop keeps its membership (heartbeat polls with all partitions paused)
 *       and keeps retrying with capped backoff — broker degradation idles the loop instead of
 *       crash-looping it. Offsets are unmoved throughout, so there is no loss either way; the
 *       gauge clears on the first successful commit.
 * </ul>
 *
 * <p><strong>Metrics:</strong> the per-record counters ({@code cesium_ingest_records_total},
 * {@code cesium_dlq_records_total}, {@code cesium_header_errors_total}) reflect <em>committed</em>
 * dispositions: counts accumulate per batch and flush only after {@code commitTransaction}
 * returns, so abort/retry cycles never inflate them (§9 "ingest dispositions").
 *
 * <p><strong>Graceful shutdown:</strong> {@link #stop()} (any thread) raises the stop flag and
 * {@code wakeup()}s the consumer; the flag is honored at transaction boundaries — an in-flight
 * batch always runs to commit or abort before the loop exits and closes its clients (bounded by
 * {@link #CLOSE_TIMEOUT}, per §6's bounded-shutdown posture).
 *
 * <p>{@link CrashPoints} fire at the §3.9 ingest points I-1…I-4 (no-ops outside tests).
 */
public final class IngestLoop implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(IngestLoop.class);

    /**
     * Bound on client close calls (§6: shutdown is bounded, then hard abort). A conservative
     * constant until M7 introduces the {@code shutdown.timeout} knob; without it a producer with
     * unflushed transactional sends against a degraded broker blocks until
     * {@code delivery.timeout.ms} (2 minutes default).
     */
    static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(10);

    private final IngestLoopConfig config;
    private final Consumer<byte[], byte[]> consumer;
    private final Supplier<Producer<byte[], byte[]>> producerFactory;
    private final TrackerBackedStore store;
    private final DelayHeaderCodec headerCodec;
    private final IngestPolicyEngine policyEngine;
    private final RelayRecordFactory relayFactory;
    private final IdentityBlob identity;
    private final String identityMetadata;
    private final Clock clock;

    // Owned by the loop thread after construction (the constructing thread hands off via start).
    private Producer<byte[], byte[]> producer;
    private boolean started;
    private boolean transactionInFlight;
    private int failureStreak;
    private long backoffUntilMs;
    private int trackerPartitions;
    private @Nullable InDoubtRecovery inDoubtRecovery;
    private final BatchCounts batchCounts = new BatchCounts();

    /**
     * Source records a prior attempt found <em>permanently</em> unrelayable by the destination
     * (§3.8 I-8), keyed by source {@code (partition, offset)} → the resolved disposition. The relay
     * for such a record would only be rejected again, so on reprocess the loop routes it per the
     * {@code route.relay.on-unrelayable} policy (DLQ/DROP) instead. Loop-thread-confined; pruned on
     * commit (offsets advanced past it) and on partition revocation (the new owner re-discovers).
     */
    private final Map<SourceCoord, Unrelayable> unrelayable = new HashMap<>();

    /**
     * Permanent destination rejections attributed to a specific source record by the send callback
     * (§3.8). Written from the producer I/O thread, drained on the loop thread after the
     * transaction's outcome is known — a {@link ConcurrentLinkedQueue} for safe cross-thread
     * publication. Reset at the start of every transaction.
     */
    private final ConcurrentLinkedQueue<UnrelayableHit> unrelayableHits = new ConcurrentLinkedQueue<>();

    // Cross-thread state: stop flag, health/metrics, async send errors from the producer I/O thread.
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger degraded = new AtomicInteger();
    private final AtomicLong lastIterationMs = new AtomicLong();

    // Single-writer-per-txn volatile; null is the steady state. Reads go through a local copy.
    private volatile @Nullable Exception asyncSendError;

    private final Counter relayedImmediateRecords;
    private final Counter scheduledRecords;
    private final Counter clampedRecords;
    private final Counter dlqOutcomeRecords;
    private final Counter unrelayableDroppedRecords;
    private final Counter malformedHeaders;
    private final Counter overMaxHeaders;
    private final Counter conflictHeaders;
    private final Counter committedTransactions;
    private final Timer commitTimer;
    private final Gauge degradedGauge;
    private final Gauge heartbeatGauge;
    private final MeterRegistry registry;

    /**
     * @param config the loop settings (topics, tracker partition count, retry/backoff budgets)
     * @param consumer the group-A source consumer; the loop owns its lifecycle from here on
     * @param producerFactory creates the transactional producer — called once at construction and
     *     again whenever an in-doubt commit forces a producer replacement (§3.8); each returned
     *     producer must be freshly constructed and not yet initialized for transactions
     * @param store the tracker-backed store whose {@link TrackerBackedStore#encodeSchedule} owns
     *     the durable wire format (the loop only routes the bytes)
     * @param headerCodec the control-header codec (§2.3)
     * @param policyEngine the policy engine combining parse results with configured policies (D3)
     * @param relayFactory builds destination relays and DLQ records (§2.4)
     * @param identityBlob binds committed offsets to the cluster and source-topic identity (§3.1)
     * @param meterRegistry metrics sink (§9 inventory); the loop's gauges are removed from it when
     *     the loop closes, so a restarted loop re-registers gauges over its own state
     * @param clock injectable time source; the loop never reads wall time directly
     */
    public IngestLoop(
            IngestLoopConfig config,
            Consumer<byte[], byte[]> consumer,
            Supplier<Producer<byte[], byte[]>> producerFactory,
            TrackerBackedStore store,
            DelayHeaderCodec headerCodec,
            IngestPolicyEngine policyEngine,
            RelayRecordFactory relayFactory,
            IdentityBlob identityBlob,
            MeterRegistry meterRegistry,
            Clock clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.producerFactory = Objects.requireNonNull(producerFactory, "producerFactory");
        this.store = Objects.requireNonNull(store, "store");
        this.headerCodec = Objects.requireNonNull(headerCodec, "headerCodec");
        this.policyEngine = Objects.requireNonNull(policyEngine, "policyEngine");
        this.relayFactory = Objects.requireNonNull(relayFactory, "relayFactory");
        this.identity = Objects.requireNonNull(identityBlob, "identityBlob");
        this.identityMetadata = identityBlob.encode();
        this.registry = Objects.requireNonNull(meterRegistry, "meterRegistry");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.trackerPartitions = config.trackerPartitions();
        this.producer = Objects.requireNonNull(producerFactory.get(), "producerFactory.get()");

        this.relayedImmediateRecords = ingestRecords("relayed_immediate");
        this.scheduledRecords = ingestRecords("scheduled");
        this.clampedRecords = ingestRecords("clamped");
        this.dlqOutcomeRecords = ingestRecords("dlq");
        this.unrelayableDroppedRecords = ingestRecords("unrelayable_dropped");
        this.malformedHeaders = registry.counter("cesium.header.errors", "type", "malformed");
        this.overMaxHeaders = registry.counter("cesium.header.errors", "type", "over_max");
        this.conflictHeaders = registry.counter("cesium.header.errors", "type", "conflict");
        this.committedTransactions =
                registry.counter("cesium.transactions", "loop", "ingest", "result", "committed", "cause", "none");
        this.commitTimer = Timer.builder("cesium.txn.commit.seconds")
                .tag("loop", "ingest")
                .description("commitTransaction latency per attempt (§9)")
                .register(registry);
        this.degradedGauge = Gauge.builder("cesium.degraded", degraded, AtomicInteger::get)
                .tag("loop", "ingest")
                .description("1 while the ingest loop is parked-and-degraded (design §3.8); cause in logs")
                .register(registry);
        this.heartbeatGauge = Gauge.builder(
                        "cesium.loop.last.iteration.timestamp.seconds", lastIterationMs, ms -> ms.get() / 1000.0)
                .tag("loop", "ingest")
                .description("epoch seconds of the last loop iteration; feeds liveness")
                .register(registry);
    }

    /** Runs the loop until {@link #stop()} or a fatal failure; closes both clients on the way out. */
    @Override
    public void run() {
        try {
            start();
            while (running.get()) {
                try {
                    runOnce();
                } catch (WakeupException e) {
                    if (running.get()) {
                        throw new IngestLoopFatalException("consumer woken up without a stop request", e);
                    }
                    // Graceful shutdown: stop() interrupted the poll sleep; by I3 no transaction
                    // is in flight at this point, so this is a transaction boundary.
                }
            }
            log.info("ingest loop stopped cleanly");
        } finally {
            closeClients();
        }
    }

    /**
     * Requests a graceful stop from any thread: raises the stop flag and {@code wakeup()}s the
     * consumer. Honored at transaction boundaries — an in-flight batch completes first (I3 makes
     * every poll a boundary).
     */
    public void stop() {
        running.set(false);
        consumer.wakeup();
    }

    /** True while the loop is parked-and-degraded (§3.8); mirrors the {@code cesium_degraded} gauge. */
    public boolean isDegraded() {
        return degraded.get() == 1;
    }

    /**
     * Fences this producer against any predecessor and subscribes to the source. Separated from
     * {@link #run()} so unit tests can drive {@link #runOnce() iterations} deterministically.
     */
    void start() {
        if (started) {
            throw new IllegalStateException("ingest loop already started");
        }
        started = true;
        // initTransactions before the first poll: resolves a predecessor's dangling transaction
        // immediately (stable transactional.id, D10) instead of stalling the LSO.
        producer.initTransactions();
        consumer.subscribe(List.of(config.sourceTopic()), new IngestRebalanceListener());
        log.info(
                "ingest loop started: source={} tracker={} (group A subscribe, read_committed, reset=none)",
                config.sourceTopic(),
                config.trackerTopic());
    }

    /**
     * One iteration: heartbeat bookkeeping, pending in-doubt recovery retry, backoff gating, a
     * single poll, and — for a non-empty batch — exactly one transaction. Never opens an empty
     * transaction.
     */
    void runOnce() {
        lastIterationMs.set(clock.millis());
        InDoubtRecovery recovery = inDoubtRecovery;
        if (recovery != null) {
            if (clock.millis() < backoffUntilMs) {
                heartbeatPoll();
                return;
            }
            if (!attemptInDoubtRecovery(recovery)) {
                scheduleBackoff("in_doubt_recovery");
                return;
            }
            inDoubtRecovery = null;
            log.info("in-doubt commit recovery completed after retry; resuming intake");
        }
        maybeResumeAfterBackoff();
        final ConsumerRecords<byte[], byte[]> records;
        try {
            records = consumer.poll(pollTimeout());
        } catch (WakeupException e) {
            throw e;
        } catch (NoOffsetForPartitionException e) {
            // I-9: committed offsets expired (KIP-211) or true first run under the locked
            // auto.offset.reset=none. Neither a silent skip nor a mass duplicate is acceptable —
            // the operator chooses the reset point explicitly (design §3.6 runbook).
            throw new IngestLoopFatalException(
                    "no committed offset for assigned source partition(s) and auto.offset.reset=none is locked: "
                            + "either committed offsets expired (broker offsets.retention.minutes shorter than the "
                            + "outage) or this is a first run; an operator must seed group offsets explicitly — "
                            + "see the offset-reset runbook",
                    e);
        } catch (KafkaException e) {
            if (isFatal(e)) {
                throw new IngestLoopFatalException("source poll failed fatally", e);
            }
            log.warn("source poll failed; backing off", e);
            scheduleBackoff(causeTag(e));
            return;
        }
        if (records.isEmpty()) {
            return; // never open empty transactions (§3.1)
        }
        processBatch(records);
    }

    /** Runs one batch through the §3.8 taxonomy: commit, abort-and-rewind, in-doubt, or fatal. */
    private void processBatch(ConsumerRecords<byte[], byte[]> records) {
        try {
            transact(records);
        } catch (PolicyFailStop stop) {
            // The FAIL policy aborts the batch (nothing is consumed) and stops the loop loudly.
            abortIfInFlight();
            abortedTransactions("policy_fail").increment();
            throw new IngestLoopFatalException("delay.on-* policy FAIL fired: " + stop.getMessage(), stop);
        } catch (InDoubtCommit inDoubt) {
            recoverFromInDoubtCommit(records, inDoubt);
            return;
        } catch (IngestLoopFatalException fatal) {
            // Pre-classified fail-fasts raised mid-transaction (the I-7 tracker-partition check):
            // the producer is healthy, so the batch aborts cleanly per the failure matrix, then
            // the loop fails with the runbook message intact.
            abortIfInFlight();
            abortedTransactions("fatal").increment();
            throw fatal;
        } catch (RuntimeException e) {
            if (isFatal(e)) {
                // Fatal (§3.8): no abort attempt — a fenced/poisoned producer cannot abort; the
                // broker resolves the dangling transaction. Close + fail the loop.
                throw new IngestLoopFatalException("fatal ingest transaction failure", e);
            }
            if (!unrelayableHits.isEmpty()) {
                // §3.8 I-8: the destination PERMANENTLY rejected a relay (too large / invalid), not
                // a transient blip. Abort (nothing committed), remember the offending record(s), and
                // reprocess — routing them to the route.relay.on-unrelayable policy instead of
                // retrying forever. Distinct from the abortable path below so a poison record never
                // trips park-and-degrade and the partition advances on the very next poll.
                handleUnrelayable(records, e);
                return;
            }
            // Definitively abortable: nothing committed, nothing visible. Rewind and re-poll.
            abortIfInFlight();
            abortedTransactions(causeTag(e)).increment();
            rewindToBatchStart(records);
            log.warn("ingest transaction aborted; rewound to batch start for retry", e);
            scheduleBackoff(causeTag(e));
            return;
        }
        onBatchSuccess();
    }

    /**
     * The §3.8 I-8 unrelayable path: a relay this batch attempted was permanently rejected by the
     * destination and attributed to one or more source records. Abort the (uncommitted) batch and,
     * unless {@code route.relay.on-unrelayable=FAIL}, record each offending record's resolved
     * disposition and rewind so the reprocess routes it to the DLQ (or drops it) atomically with the
     * offset advance — never an infinite retry. {@code failureStreak} is deliberately untouched: a
     * deterministic record rejection is not broker degradation and must not park-and-degrade the
     * loop, and the rewound batch re-polls immediately (no backoff gate).
     */
    private void handleUnrelayable(ConsumerRecords<byte[], byte[]> records, RuntimeException cause) {
        abortIfInFlight();
        abortedTransactions("unrelayable").increment();
        List<UnrelayableHit> hits = new ArrayList<>();
        for (UnrelayableHit hit; (hit = unrelayableHits.poll()) != null; ) {
            hits.add(hit);
        }
        if (config.onUnrelayable() == UnrelayablePolicy.FAIL) {
            // FAIL: a permanent destination rejection is an operator problem to surface. Like the
            // other FAIL policies, the record stays at the committed offset (restart-persistent).
            UnrelayableHit first = hits.get(0);
            throw new IngestLoopFatalException(
                    "route.relay.on-unrelayable=FAIL: the destination permanently rejected the relay of source "
                            + config.sourceTopic() + "-" + first.coord().partition() + "@"
                            + first.coord().offset()
                            + ": " + first.detail(),
                    cause);
        }
        for (UnrelayableHit hit : hits) {
            promoteUnrelayable(hit);
        }
        rewindToBatchStart(records);
        log.warn(
                "ingest relay permanently rejected by the destination for {} record(s) (route.relay.on-unrelayable={});"
                        + " rewinding to route them on reprocess instead of retrying forever (§3.8 I-8)",
                hits.size(),
                config.onUnrelayable());
    }

    /**
     * Records (or escalates) an unrelayable record's disposition. A first rejection of a relay maps
     * to the policy route (DLQ, or DROP). A <em>second</em> rejection of an already-DLQ-routed record
     * means the unrelayable DLQ write was itself too large to produce (the relay exceeds
     * {@code max.request.size}, so the larger DLQ copy is rejected too) — escalate to DROP so the
     * partition still advances rather than wedging on the DLQ write, logged loudly (§3.8).
     */
    private void promoteUnrelayable(UnrelayableHit hit) {
        Unrelayable existing = unrelayable.get(hit.coord());
        if (existing == null) {
            Unrelayable.Route route =
                    config.onUnrelayable() == UnrelayablePolicy.DROP ? Unrelayable.Route.DROP : Unrelayable.Route.DLQ;
            unrelayable.put(hit.coord(), new Unrelayable(route, hit.detail()));
        } else if (existing.route() == Unrelayable.Route.DLQ) {
            log.error(
                    "the unrelayable DLQ write for source {}-{}@{} was ALSO permanently rejected ({}); escalating to"
                            + " DROP so the partition is not wedged on the DLQ write (route.relay.on-unrelayable=DLQ,"
                            + " §3.8)",
                    config.sourceTopic(),
                    hit.coord().partition(),
                    hit.coord().offset(),
                    hit.detail());
            registry.counter("cesium.unrelayable.dlq.rejected").increment();
            unrelayable.put(hit.coord(), new Unrelayable(Unrelayable.Route.DROP, hit.detail()));
        }
    }

    /**
     * Prunes resolved unrelayable entries once the committing batch advanced the source offset past
     * them (also sweeping any stale entry below the committed position). Keeps the map bounded.
     */
    private void pruneUnrelayableBelow(ConsumerRecords<byte[], byte[]> records) {
        if (unrelayable.isEmpty()) {
            return;
        }
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(partition);
            long next = partitionRecords.get(partitionRecords.size() - 1).offset() + 1;
            unrelayable.keySet().removeIf(coord -> coord.partition() == partition.partition() && coord.offset() < next);
        }
    }

    /**
     * The §3.1 transaction: begin; per record parse → decide → send; offsets with the identity
     * blob and freshly fetched group metadata; commit; flush the batch's metric counts (committed
     * dispositions only). Fires crash points I-1…I-4.
     */
    private void transact(ConsumerRecords<byte[], byte[]> records) {
        CrashPoints.maybeFire(CrashPoints.INGEST_BEFORE_BEGIN);
        asyncSendError = null;
        unrelayableHits.clear();
        batchCounts.reset();
        producer.beginTransaction();
        transactionInFlight = true;
        long nowMs = clock.millis();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            dispatch(record, nowMs);
        }
        throwIfAsyncSendFailed();
        CrashPoints.maybeFire(CrashPoints.INGEST_AFTER_SENDS);
        // I2: group metadata fetched immediately before the call — the fencing hook.
        producer.sendOffsetsToTransaction(nextOffsets(records), consumer.groupMetadata());
        CrashPoints.maybeFire(CrashPoints.INGEST_AFTER_SEND_OFFSETS);
        commitWithRetry();
        flushBatchMetrics();
        pruneUnrelayableBelow(records);
    }

    /** Parses one record's headers, applies policy, and sends the resulting record(s). */
    private void dispatch(ConsumerRecord<byte[], byte[]> record, long nowMs) {
        Unrelayable known = unrelayable.get(new SourceCoord(record.partition(), record.offset()));
        if (known != null) {
            // A prior attempt proved the destination permanently rejects this record's relay
            // (§3.8 I-8): re-relaying would only fail again and wedge the partition. Resolve it per
            // the route.relay.on-unrelayable disposition INSTEAD, atomically with the offset advance.
            applyUnrelayable(record, known, nowMs);
            return;
        }
        HeaderParseResult parsed = headerCodec.parse(record, nowMs);
        if (parsed.conflicted()) {
            batchCounts.conflict++;
            log.warn(
                    "conflicting control headers on {}-{}@{}: precedence applied (deliver-at wins; last value wins)",
                    record.topic(),
                    record.partition(),
                    record.offset());
        }
        switch (parsed) {
            case HeaderParseResult.Malformed ignored -> batchCounts.malformed++;
            case HeaderParseResult.OverMax ignored -> batchCounts.overMax++;
            default -> {}
        }
        IngestDecision decision = policyEngine.decide(parsed, nowMs);
        switch (decision) {
            case IngestDecision.RelayNow ignored -> {
                // Attributed: a permanent destination rejection on this relay is captured against
                // the source record so the §3.8 I-8 unrelayable path can route it on reprocess.
                send(relayFactory.relayImmediate(record, nowMs), new SourceCoord(record.partition(), record.offset()));
                batchCounts.relayedImmediate++;
            }
            case IngestDecision.Schedule schedule -> {
                sendTrackerAdd(record, schedule.dispatchAtMs(), false);
                batchCounts.scheduled++;
            }
            case IngestDecision.Clamp clamp -> {
                // CLAMP rides to dispatch through the store encoding (ScheduledRef.clamped):
                // the relay is stamped cesium-clamped:true when the entry is dispatched (§2.3).
                sendTrackerAdd(record, clamp.dispatchAtMs(), true);
                batchCounts.clamped++;
            }
            case IngestDecision.ToDlq toDlq -> {
                // Attributed (§3.8 I-8), exactly like the relay sends above: a malformed / over-max
                // source record whose payload is near the source max.message.bytes can produce a
                // header-error DLQ copy (original key/value + ALL headers + provenance) that exceeds
                // the producer max.request.size or a tighter DLQ max.message.bytes and is permanently
                // rejected. Attributing the send routes that rejection through the unrelayable path
                // (promoteUnrelayable escalates a rejected DLQ write to DROP) so the partition still
                // advances, instead of falling to the abortable path and wedging on an infinite retry.
                send(
                        relayFactory.headerErrorDlqRecord(record, toDlq.reason(), toDlq.detail(), nowMs),
                        new SourceCoord(record.partition(), record.offset()));
                batchCounts.dlq++;
                batchCounts.dlqReasons.merge(toDlq.reason(), 1L, Long::sum);
            }
            case IngestDecision.Fail fail -> throw new PolicyFailStop(fail.detail());
        }
    }

    /** Encodes one ADD via the store and produces it to the tracker — SAME partition number (§2.1). */
    private void sendTrackerAdd(ConsumerRecord<byte[], byte[]> record, long dispatchAtMs, boolean clamped) {
        ensureTrackerPartition(record.partition());
        TrackerRecordData data =
                store.encodeSchedule(new ScheduledRef(record.partition(), record.offset(), dispatchAtMs, clamped));
        send(new ProducerRecord<>(
                config.trackerTopic(), record.partition(), null, data.key(), data.value(), data.headers()));
    }

    /**
     * I-7 (§3.9): producing an ADD to a nonexistent tracker partition means the source partition
     * count grew past the tracker — a fail-fast with the R-7 runbook, never a generic retry loop
     * (the produce would otherwise surface as a metadata-wait {@link TimeoutException} and park
     * the loop forever without naming the problem). Live metadata is re-checked once before
     * failing so an operator who already grew the tracker is not false-positived.
     */
    private void ensureTrackerPartition(int partition) {
        if (partition < trackerPartitions) {
            return;
        }
        try {
            int live = producer.partitionsFor(config.trackerTopic()).size();
            if (live > trackerPartitions) {
                log.info(
                        "tracker topic '{}' grew from {} to {} partition(s); accepting",
                        config.trackerTopic(),
                        trackerPartitions,
                        live);
                trackerPartitions = live;
            }
        } catch (RuntimeException e) {
            log.warn("could not refresh tracker partition metadata for the I-7 check", e);
        }
        if (partition < trackerPartitions) {
            return;
        }
        throw new IngestLoopFatalException("source partition " + partition + " has no tracker partition: tracker"
                + " topic '" + config.trackerTopic() + "' has " + trackerPartitions
                + " partition(s) — the source partition count grew past the tracker (I-7, R-7). Tracker records"
                + " MUST land on the same partition number as their source record (§2.1). Runbook: grow the"
                + " tracker topic first, then the source.");
    }

    /** Fire-and-forget transactional send; async failures are captured and surfaced pre-commit. */
    private void send(ProducerRecord<byte[], byte[]> record) {
        send(record, null);
    }

    /**
     * Fire-and-forget transactional send. When {@code attribution} is non-null and the send is
     * <em>permanently</em> rejected by the destination (record too large / invalid — §3.8 I-8), the
     * failure is recorded against that source record so the offending record can be routed to the
     * {@code route.relay.on-unrelayable} policy on reprocess instead of retried forever. All async
     * failures still set {@code asyncSendError}, surfaced before the offsets are sent or at commit.
     */
    private void send(ProducerRecord<byte[], byte[]> record, @Nullable SourceCoord attribution) {
        // The future is deliberately unused: completion errors are observed via the callback (and
        // the commit itself would surface them); blocking per record would serialize the batch.
        var unused = producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                return;
            }
            if (asyncSendError == null) {
                asyncSendError = exception;
            }
            if (attribution != null && UnrelayableRejections.isPermanentRecordRejection(exception)) {
                unrelayableHits.add(new UnrelayableHit(attribution, String.valueOf(exception)));
            }
        });
    }

    /**
     * Resolves a record a prior attempt proved permanently unrelayable, per the
     * {@code route.relay.on-unrelayable} disposition (§3.8 I-8): {@code DLQ} writes an unrelayable
     * DLQ record (attributed, so a DLQ write that is <em>itself</em> rejected escalates to DROP via
     * {@link #promoteUnrelayable}), {@code DROP} produces nothing. Either way the source offset
     * advances in the same transaction — the partition makes progress past the poison record.
     */
    private void applyUnrelayable(ConsumerRecord<byte[], byte[]> record, Unrelayable known, long nowMs) {
        switch (known.route()) {
            case DLQ -> {
                send(
                        relayFactory.unrelayableDlqRecord(record, known.detail(), nowMs),
                        new SourceCoord(record.partition(), record.offset()));
                batchCounts.dlq++;
                batchCounts.dlqReasons.merge(DlqReasons.UNRELAYABLE, 1L, Long::sum);
            }
            case DROP -> batchCounts.unrelayableDropped++;
        }
    }

    /**
     * Surfaces an asynchronously failed send before offsets are sent. The real producer would
     * fail {@code commitTransaction} anyway; checking here is earlier and keeps the abort metric's
     * cause tag precise.
     */
    private void throwIfAsyncSendFailed() {
        Exception error = asyncSendError;
        if (error == null) {
            return;
        }
        if (error instanceof RuntimeException runtime) {
            throw runtime;
        }
        throw new KafkaException("transactional send failed asynchronously", error);
    }

    /**
     * The next offset to consume per partition touched by the batch — the values that materialize
     * for the group only when the transaction commits — each carrying the identity blob (§3.1).
     */
    private Map<TopicPartition, OffsetAndMetadata> nextOffsets(ConsumerRecords<byte[], byte[]> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(partition);
            long next = partitionRecords.get(partitionRecords.size() - 1).offset() + 1;
            offsets.put(partition, new OffsetAndMetadata(next, identityMetadata));
        }
        return offsets;
    }

    /**
     * Commits, retrying ambiguous timeouts to a definitive outcome bounded by
     * {@code kafka.transactions.commit-retry} (§3.8 in-doubt procedure step a). Fires I-4 after
     * the commit returns, before any local bookkeeping.
     *
     * <p><strong>Post-ambiguity classification (D20/I9):</strong> once any attempt has thrown
     * {@link TimeoutException}, the broker may already have processed the commit — a later
     * non-fatal, non-timeout failure (e.g. a {@code KafkaException} from a re-sent EndTxn) can no
     * longer prove the transaction aborted. Treating it as definitively-abortable would abort,
     * rewind, and reprocess a possibly-committed batch: a read_committed-visible duplicate and a
     * violation of the unique-committed-ADD invariant (§3.1). So every post-ambiguity non-fatal
     * failure escalates to the in-doubt procedure, which is sound under both outcomes. Fatal
     * classifications still fail the loop — a restart resolves the outcome deterministically via
     * {@code initTransactions()} and the committed offsets remain authoritative.
     */
    private void commitWithRetry() {
        int attempts = 0;
        while (true) {
            try {
                commitTimer.record(producer::commitTransaction);
                // I-4 fires after the commit returned but before ANY local bookkeeping — the
                // closest in-process approximation of dying mid-commit (outcome unknown locally).
                CrashPoints.maybeFire(CrashPoints.INGEST_DURING_COMMIT);
                transactionInFlight = false;
                committedTransactions.increment();
                return;
            } catch (TimeoutException timeout) {
                attempts++;
                if (attempts >= config.commitRetryLimit()) {
                    throw new InDoubtCommit(attempts, timeout);
                }
                log.warn(
                        "commitTransaction timed out (attempt {}/{}); retrying to a definitive outcome",
                        attempts,
                        config.commitRetryLimit(),
                        timeout);
            } catch (RuntimeException e) {
                if (attempts == 0 || isFatal(e)) {
                    // First-attempt non-timeout failures are non-ambiguous (§3.8 row 1): the
                    // commit call failed before any ambiguity existed; fatal failures always
                    // fail the loop (the restart path resolves the outcome).
                    throw e;
                }
                log.warn(
                        "commitTransaction failed non-fatally after an ambiguous timeout (attempt {}); the outcome"
                                + " cannot be proven aborted — escalating to the in-doubt procedure (D20/I9)",
                        attempts + 1,
                        e);
                throw new InDoubtCommit(attempts + 1, e);
            }
        }
    }

    /**
     * The I-4 in-doubt procedure (§3.8): the broker may have committed, so the batch must
     * <em>never</em> be re-sent from memory. Replace the producer (its state is unusable),
     * {@code initTransactions()} — which deterministically resolves the dangling transaction
     * broker-side without reporting the outcome — then seek the consumer to the now-stable
     * committed offsets and re-poll.
     *
     * <p>Why this is exactly-once: ingest is stateless and source offsets materialize only at
     * commit. If the broker committed, the committed offsets lie past the batch and nothing is
     * reprocessed; if it aborted, they lie at the batch start and the records are re-polled and
     * reprocessed into a single committed copy. Dropping the in-memory batch is therefore free —
     * the ingest analog of I9's "never restore".
     *
     * <p>The recovery is itself retryable (R15): the most likely cause of an exhausted commit
     * budget is a degraded broker — the same condition under which {@code initTransactions()} or
     * the committed-offset fetch will also fail transiently. A non-fatal recovery failure keeps
     * the replaced-producer state machine and re-attempts on a later iteration under the capped
     * backoff, instead of exiting and crash-looping.
     */
    private void recoverFromInDoubtCommit(ConsumerRecords<byte[], byte[]> records, InDoubtCommit inDoubt) {
        String cause = inDoubt.failure instanceof TimeoutException ? "commit_timeout" : causeTag(inDoubt.failure);
        registry.counter("cesium.transactions", "loop", "ingest", "result", "in_doubt", "cause", cause)
                .increment();
        log.warn(
                "commitTransaction outcome ambiguous after {} attempts; replacing producer and re-polling from"
                        + " committed offsets",
                inDoubt.attempts,
                inDoubt.failure);
        transactionInFlight = false;
        InDoubtRecovery recovery = new InDoubtRecovery(firstOffsets(records));
        if (!attemptInDoubtRecovery(recovery)) {
            inDoubtRecovery = recovery;
        }
        scheduleBackoff("in_doubt");
    }

    /**
     * One in-doubt recovery attempt: hard-close the unusable producer (the in-doubt one, or a
     * half-initialized replacement from a previous failed attempt), build and fence a fresh one,
     * and position the consumer at the committed offsets. Returns false on a transient failure —
     * the caller schedules a retry; throws on fatal classifications and rethrows
     * {@link WakeupException} so a concurrent {@link #stop()} surfaces as a clean shutdown rather
     * than a misreported recovery failure.
     */
    private boolean attemptInDoubtRecovery(InDoubtRecovery recovery) {
        try {
            if (!recovery.producerReady) {
                try {
                    producer.close(Duration.ZERO);
                } catch (RuntimeException closeFailure) {
                    log.warn("closing the unusable producer failed; continuing with replacement", closeFailure);
                }
                producer = Objects.requireNonNull(producerFactory.get(), "producerFactory.get()");
                producer.initTransactions();
                recovery.producerReady = true;
            }
            seekToCommittedOffsets(recovery.fallbackOffsets);
            return true;
        } catch (WakeupException e) {
            throw e; // stop() raced the recovery: run() resolves clean shutdown vs unexpected wakeup
        } catch (IngestLoopFatalException e) {
            throw e; // already classified (identity mismatch at the offset fetch, §3.1)
        } catch (RuntimeException e) {
            if (isFatal(e)) {
                throw new IngestLoopFatalException("in-doubt commit recovery failed fatally", e);
            }
            log.warn("in-doubt recovery attempt failed transiently; retrying with capped backoff (§3.8/R15)", e);
            return false;
        }
    }

    /**
     * A pure heartbeat iteration while an in-doubt recovery is pending: the poll keeps group
     * membership alive, but nothing may be processed — there is no usable producer. Paused
     * partitions cannot deliver, but a rebalance inside the poll can hand over unpaused ones;
     * any delivered records are rewound, never dropped and never processed.
     */
    private void heartbeatPoll() {
        final ConsumerRecords<byte[], byte[]> records;
        try {
            records = consumer.poll(pollTimeout());
        } catch (WakeupException e) {
            throw e;
        } catch (RuntimeException e) {
            log.warn("heartbeat poll during in-doubt recovery failed; recovery retries continue", e);
            return;
        }
        if (!records.isEmpty()) {
            rewindToBatchStart(records);
        }
    }

    /** The first delivered offset per partition of the in-flight batch — its pre-batch positions. */
    private static Map<TopicPartition, Long> firstOffsets(ConsumerRecords<byte[], byte[]> records) {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            offsets.put(partition, records.records(partition).get(0).offset());
        }
        return offsets;
    }

    /**
     * Positions every assigned partition at its committed offset (now stable — the dangling
     * transaction was resolved by {@code initTransactions()}), verifying each committed identity
     * blob against this loop's identity on the way (§3.1: "at every offset fetch"). A partition
     * with no committed offset at all falls back to the in-flight batch's first offset for it,
     * which was its pre-batch position; partitions untouched by the batch keep their current
     * position.
     */
    private void seekToCommittedOffsets(Map<TopicPartition, Long> fallbackOffsets) {
        Set<TopicPartition> assigned = consumer.assignment();
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(assigned);
        for (TopicPartition partition : assigned) {
            OffsetAndMetadata offset = committed.get(partition);
            if (offset != null) {
                verifyCommittedIdentity(partition, offset);
                consumer.seek(partition, offset.offset());
            } else {
                Long fallback = fallbackOffsets.get(partition);
                if (fallback != null) {
                    consumer.seek(partition, fallback);
                }
            }
        }
    }

    /**
     * §3.1/R-10 at the offset-fetch boundary: a committed offset whose identity blob names a
     * different cluster or source-topic id means the source was recreated under the same name —
     * fail-fast with the runbook; never deliver payloads from a different topic incarnation.
     * Blank/absent metadata is an integrity fail-fast, not a tolerated case (VULN-006): a genuine
     * first run has no committed offset at all (handled by the fallback branch in
     * {@link #seekToCommittedOffsets}), so a committed offset that carries no identity blob was
     * either externally reset/seeded or forged by a principal able to commit for the ingest group —
     * resuming on it would seek to an unverified, attacker-choosable position. Unreadable metadata is
     * likewise fatal (§3.6).
     */
    private void verifyCommittedIdentity(TopicPartition partition, OffsetAndMetadata offset) {
        String metadata = offset.metadata();
        if (metadata != null && metadata.equals(identityMetadata)) {
            // cesium's own commit for this incarnation — the steady-state fast path.
            return;
        }
        if (metadata == null || metadata.isBlank()) {
            throw new IngestLoopFatalException(
                    "committed offset metadata for " + partition + " is blank/absent (§3.1/R-10): refusing to resume"
                            + " on an unverifiable identity — a committed offset must carry cesium's identity blob."
                            + " A genuine first run has no committed offset; a committed-but-blank offset means the"
                            + " group's offsets were externally reset/seeded or forged. Clear the ingest group so"
                            + " cesium bootstraps from a clean state, or re-seed the offsets with a valid identity"
                            + " blob, then retry.");
        }
        IdentityBlob recorded;
        try {
            recorded = IdentityBlob.decode(metadata);
        } catch (IllegalArgumentException e) {
            throw new IngestLoopFatalException(
                    "committed offset metadata for " + partition + " is not a readable identity blob (§3.6"
                            + " integrity): " + e.getMessage() + " — refusing to resume on unverifiable identity;"
                            + " inspect and repair the group offsets",
                    e);
        }
        if (!recorded.matches(identity)) {
            throw new IngestLoopFatalException(
                    "committed offset identity mismatch for " + partition + ": " + recorded.describeMismatch(identity));
        }
    }

    /**
     * Rewinds each batch partition to the batch's first offset for it — its position as of the
     * last committed transaction, since every earlier batch either committed or was itself
     * rewound before the next poll. Partitions revoked meanwhile are skipped: the new owner
     * re-polls them from the committed offsets.
     */
    private void rewindToBatchStart(ConsumerRecords<byte[], byte[]> records) {
        Set<TopicPartition> assigned = consumer.assignment();
        for (TopicPartition partition : records.partitions()) {
            if (!assigned.contains(partition)) {
                continue;
            }
            consumer.seek(partition, records.records(partition).get(0).offset());
        }
    }

    /** Aborts the open transaction if one is in flight; a failed abort forces a producer replacement. */
    private void abortIfInFlight() {
        if (!transactionInFlight) {
            return;
        }
        transactionInFlight = false;
        try {
            producer.abortTransaction();
        } catch (RuntimeException abortFailure) {
            // Nothing committed (the commit call was never reached, or failed non-ambiguously):
            // a replacement producer whose initTransactions() aborts the dangling txn is safe.
            log.warn("abortTransaction failed; replacing producer", abortFailure);
            try {
                producer.close(Duration.ZERO);
            } catch (RuntimeException closeFailure) {
                log.warn("closing the failed producer also failed; continuing with replacement", closeFailure);
            }
            producer = Objects.requireNonNull(producerFactory.get(), "producerFactory.get()");
            producer.initTransactions();
        }
    }

    /**
     * Records a consecutive failure and arms the retry gate: all assigned partitions pause (the
     * subsequent polls become pure heartbeats), and the next processing attempt happens after a
     * capped exponential backoff. At {@code parkThreshold} consecutive failures the loop is
     * declared parked-and-degraded (§3.8/R15): gauge up, alert-level log — but the retry cadence
     * continues; the loop never gives up and never crash-loops.
     */
    private void scheduleBackoff(String cause) {
        failureStreak++;
        long backoffMs = backoffMillis(failureStreak);
        backoffUntilMs = DelayHeaderCodec.saturatedAddMillis(clock.millis(), backoffMs);
        consumer.pause(consumer.assignment());
        if (failureStreak >= config.parkThreshold()) {
            if (degraded.compareAndSet(0, 1)) {
                log.error(
                        "ingest loop parked-and-degraded after {} consecutive failures (cause={}); holding membership"
                                + " with heartbeat polls and retrying every <= {} ms",
                        failureStreak,
                        cause,
                        config.retryBackoffMax().toMillis());
            }
        } else {
            log.warn("ingest batch failed (streak={}, cause={}); retrying in {} ms", failureStreak, cause, backoffMs);
        }
    }

    /** Capped exponential backoff: {@code initial * 2^(streak-1)}, never above the cap. */
    private long backoffMillis(int streak) {
        long max = config.retryBackoffMax().toMillis();
        long backoff = config.retryBackoffInitial().toMillis();
        for (int i = 1; i < streak && backoff < max; i++) {
            backoff <<= 1;
        }
        return Math.min(backoff, max);
    }

    /** Reopens intake when the backoff deadline has passed (never while a recovery is pending). */
    private void maybeResumeAfterBackoff() {
        if (inDoubtRecovery == null && backoffUntilMs != 0 && clock.millis() >= backoffUntilMs) {
            backoffUntilMs = 0;
            consumer.resume(consumer.assignment());
        }
    }

    /** §3.8 "resume on success": clears the failure streak, the degraded flag, and any pause. */
    private void onBatchSuccess() {
        if (failureStreak == 0) {
            return;
        }
        failureStreak = 0;
        if (degraded.compareAndSet(1, 0)) {
            log.info("ingest loop recovered; degraded flag cleared");
        }
        if (backoffUntilMs != 0) {
            // A rebalance can hand the loop unpaused partitions mid-backoff; success means the
            // gate has served its purpose either way.
            backoffUntilMs = 0;
            consumer.resume(consumer.assignment());
        }
    }

    /** While backing off, sleep in poll no longer than the remaining backoff. */
    private Duration pollTimeout() {
        if (backoffUntilMs == 0) {
            return config.pollTimeout();
        }
        long remainingMs = backoffUntilMs - clock.millis();
        if (remainingMs <= 0) {
            return Duration.ZERO;
        }
        return Duration.ofMillis(Math.min(remainingMs, config.pollTimeout().toMillis()));
    }

    /** Flushes the batch's accumulated counts: only committed dispositions reach the registry (§9). */
    private void flushBatchMetrics() {
        incrementBy(relayedImmediateRecords, batchCounts.relayedImmediate);
        incrementBy(scheduledRecords, batchCounts.scheduled);
        incrementBy(clampedRecords, batchCounts.clamped);
        incrementBy(dlqOutcomeRecords, batchCounts.dlq);
        incrementBy(unrelayableDroppedRecords, batchCounts.unrelayableDropped);
        incrementBy(malformedHeaders, batchCounts.malformed);
        incrementBy(overMaxHeaders, batchCounts.overMax);
        incrementBy(conflictHeaders, batchCounts.conflict);
        for (Map.Entry<String, Long> entry : batchCounts.dlqReasons.entrySet()) {
            registry.counter("cesium.dlq.records", "reason", entry.getKey())
                    .increment(entry.getValue().doubleValue());
        }
        batchCounts.reset();
    }

    private static void incrementBy(Counter counter, long amount) {
        if (amount > 0) {
            counter.increment((double) amount);
        }
    }

    /**
     * Closes both clients with a bounded budget (§6: shutdown bounded, then hard abort) and
     * removes this loop's gauges from the registry — Micrometer returns the existing meter on
     * name+tag collision, so leaving them registered would pin a restarted loop's gauges to this
     * dead instance's state. Safe to call once whether the loop stopped cleanly or fatally.
     */
    private void closeClients() {
        try {
            producer.close(CLOSE_TIMEOUT);
        } catch (RuntimeException e) {
            log.warn("producer close failed", e);
        }
        try {
            consumer.close(CloseOptions.timeout(CLOSE_TIMEOUT));
        } catch (RuntimeException e) {
            log.warn("consumer close failed", e);
        }
        registry.remove(degradedGauge);
        registry.remove(heartbeatGauge);
    }

    /** §3.8 fatal classification, walking the cause chain (clients wrap fenced errors). */
    private static boolean isFatal(RuntimeException error) {
        for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof ProducerFencedException
                    || t instanceof OutOfOrderSequenceException
                    || t instanceof FencedInstanceIdException
                    || t instanceof AuthenticationException
                    || t instanceof AuthorizationException
                    || t instanceof UnsupportedVersionException
                    || t instanceof InterruptException
                    || t instanceof IllegalStateException) {
                return true;
            }
        }
        return false;
    }

    private Counter abortedTransactions(String cause) {
        return registry.counter("cesium.transactions", "loop", "ingest", "result", "aborted", "cause", cause);
    }

    private Counter ingestRecords(String outcome) {
        return registry.counter("cesium.ingest.records", "outcome", outcome);
    }

    private static String causeTag(Throwable error) {
        return error.getClass().getSimpleName();
    }

    /**
     * O(1) bookkeeping only (I3/§6): the ingest loop is stateless, so rebalances are logged and
     * counted, and the only action is keeping a mid-backoff pause coherent for partitions that
     * arrive while the loop is parked. Runs inside {@code poll()} on the loop thread — by I3 no
     * transaction can be in flight here.
     */
    private final class IngestRebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            registry.counter("cesium.ingest.rebalances", "event", "assigned").increment();
            log.info("source partitions assigned: {}", partitions);
            if (backoffUntilMs != 0 && !partitions.isEmpty()) {
                consumer.pause(partitions);
            }
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            registry.counter("cesium.ingest.rebalances", "event", "revoked").increment();
            log.info("source partitions revoked: {}", partitions);
            dropUnrelayable(partitions);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            registry.counter("cesium.ingest.rebalances", "event", "lost").increment();
            log.warn("source partitions lost without clean revocation: {}", partitions);
            dropUnrelayable(partitions);
        }

        /**
         * Drops unrelayable bookkeeping for partitions this member no longer owns: the new owner
         * re-discovers (re-attempts the relay, fails the same way, routes it). Runs inside
         * {@code poll()} on the loop thread (I3), so the map is touched single-threaded.
         */
        private void dropUnrelayable(Collection<TopicPartition> partitions) {
            if (unrelayable.isEmpty() || partitions.isEmpty()) {
                return;
            }
            Set<Integer> ids = new HashSet<>();
            for (TopicPartition tp : partitions) {
                ids.add(tp.partition());
            }
            unrelayable.keySet().removeIf(coord -> ids.contains(coord.partition()));
        }
    }

    /**
     * Per-batch metric accumulator: dispositions count here while the transaction is open and
     * flush to the registry only after the commit returns, so abort/retry cycles never inflate
     * the §9 counters (one committed copy ⇒ one count).
     */
    private static final class BatchCounts {
        long relayedImmediate;
        long scheduled;
        long clamped;
        long dlq;
        long unrelayableDropped;
        long malformed;
        long overMax;
        long conflict;
        final Map<String, Long> dlqReasons = new HashMap<>();

        void reset() {
            relayedImmediate = 0;
            scheduled = 0;
            clamped = 0;
            dlq = 0;
            unrelayableDropped = 0;
            malformed = 0;
            overMax = 0;
            conflict = 0;
            dlqReasons.clear();
        }
    }

    /** A source-record identity within the (single) source topic: partition + offset. */
    private record SourceCoord(int partition, long offset) {}

    /** A resolved disposition for a permanently-unrelayable record (§3.8 I-8). */
    private record Unrelayable(Route route, String detail) {
        enum Route {
            DLQ,
            DROP
        }
    }

    /** A permanent destination rejection attributed to a specific source record by a send callback. */
    private record UnrelayableHit(SourceCoord coord, String detail) {}

    /**
     * The durable inputs of a pending in-doubt recovery (§3.8 step b), kept so a transiently
     * failed recovery can resume on a later iteration: the batch's pre-batch positions (the only
     * thing remembered about the dropped batch — offsets, never records) and whether the
     * replacement producer has been fenced yet.
     */
    private static final class InDoubtRecovery {
        final Map<TopicPartition, Long> fallbackOffsets;
        boolean producerReady;

        InDoubtRecovery(Map<TopicPartition, Long> fallbackOffsets) {
            this.fallbackOffsets = fallbackOffsets;
        }
    }

    /** Control-flow signal: a {@code FAIL} delay policy fired (the violation detail is the message). */
    private static final class PolicyFailStop extends RuntimeException {
        PolicyFailStop(String detail) {
            super(detail);
        }
    }

    /** Control-flow signal: {@code commitTransaction} stayed ambiguous past the retry budget. */
    private static final class InDoubtCommit extends RuntimeException {
        final int attempts;
        final RuntimeException failure;

        InDoubtCommit(int attempts, RuntimeException failure) {
            super("commitTransaction outcome ambiguous after " + attempts + " attempts", failure);
            this.attempts = attempts;
            this.failure = failure;
        }
    }
}
