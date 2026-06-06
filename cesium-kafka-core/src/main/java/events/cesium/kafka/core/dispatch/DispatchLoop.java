package events.cesium.kafka.core.dispatch;

import events.cesium.kafka.api.store.CompletionReason;
import events.cesium.kafka.api.store.DueBatch;
import events.cesium.kafka.api.store.TrackerBackedStore;
import events.cesium.kafka.api.store.TrackerCursor;
import events.cesium.kafka.api.store.TrackerRecordData;
import events.cesium.kafka.core.fetch.FetchOutcome;
import events.cesium.kafka.core.fetch.FetchResult;
import events.cesium.kafka.core.fetch.SeekFetcher;
import events.cesium.kafka.core.headers.DelayHeaderCodec;
import events.cesium.kafka.core.headers.RelayRecordFactory;
import events.cesium.kafka.core.policy.UnfetchablePayloadPolicy;
import events.cesium.kafka.core.testing.CrashPoints;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
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
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TransactionAbortableException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.errors.WakeupException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The dispatch loop (design §3.2, §3.5, §3.6, §3.8, §6): single-threaded owner of the group-B
 * tracker consumer, the dispatch transactional producer, and the {@link SeekFetcher}, driving the
 * real {@link TrackerBackedStore} SPI. One {@code poll()} is simultaneously the sleep, the wakeup
 * channel, the intake, and the liveness heartbeat; the drain phase is time-sliced and interleaves
 * {@code poll(ZERO)} between transactions so group membership survives arbitrarily deep
 * due-storms (D-12). {@code poll()} is never called while a transaction is open (I3), so
 * rebalance callbacks — which run inside {@code poll()} on this thread and do O(1) bookkeeping
 * only — can never observe an in-flight transaction.
 *
 * <p><strong>Recovery protocol (I8, §3.6) — exact order per newly assigned partition:</strong>
 * the callback only marks the shard assigned (epoch-tagged) and pauses intake; on the loop,
 * (1) {@code consumer.committed(Set)} resolves the cursor — under the locked
 * {@code read_committed} the OffsetFetch sets {@code require_stable} and blocks through
 * {@code UNSTABLE_OFFSET_COMMIT} until every pending transactional offset commit for the
 * partition resolves; (2) integrity checks — a committed offset below the partition beginning is
 * a fail-fast ("tracker truncated/recreated", R-9), a missing offset is a fail-fast unless the
 * whole group provably never committed (the explicit first-run seek-to-beginning, D18); the
 * cursor-sidecar identity blob is validated by the store inside {@code beginRecovery} (§3.6 step
 * 2 — the sidecar encoding is store-owned and opaque here); (3) only then is the barrier
 * snapshotted — HW via {@code Admin.listOffsets(latest, READ_UNCOMMITTED)} (never the consumer's
 * {@code endOffsets}, the LSO under read_committed — D5). The barrier future is epoch-tagged and
 * discarded if the partition is revoked before it completes; a committed offset above the
 * resolved barrier is the upper-bound integrity fail-fast. Then {@code store.beginRecovery},
 * seek to the cursor, resume intake; the store flips the shard ACTIVE when the reported position
 * reaches the barrier.
 *
 * <p><strong>SPI obligations honored here:</strong> records are fed through the headers-carrying
 * {@code onTrackerRecord}; {@code onTrackerPosition} is reported for every streaming partition
 * after every poll (control-marker promotion depends on it); completion tombstones are encoded
 * per {@link CompletionReason} sub-batch view; cursors are computed for the <em>settled</em> view
 * while carry-over entries are still in flight, the transaction commits, and only then is the
 * carry-over restored via {@code onBatchAborted} (truncate-and-carry-over, §3.2/§7);
 * {@code committedCursor} proposals always ride the very next commit.
 *
 * <p><strong>Error taxonomy (§3.8, with the M5-verified kafka-clients 4.3.0 facts):</strong>
 *
 * <ul>
 *   <li><em>Definitively abortable</em> (any non-fatal failure before the commit call, a
 *       first-attempt non-timeout commit failure, or {@link TransactionAbortableException} at any
 *       point — the broker-affirmed "not committed" signal, KIP-890): abort, restore the full
 *       drained batch via {@code onBatchAborted}, stamp the batch's source partitions with a
 *       penalty not-before, and retry under capped exponential backoff.
 *   <li><em>Fatal</em> ({@link ProducerFencedException}, {@link InvalidPidMappingException},
 *       {@link OutOfOrderSequenceException}, auth, fenced instance id, tracker-integrity
 *       fail-fasts): close clients, fail the loop — the batch is never restored, the durable log
 *       is authoritative for the successor (this is why a post-ambiguity
 *       {@code ProducerFencedException} may take this path: I9 still holds).
 *   <li><em>In-doubt</em> ({@link TimeoutException} from {@code commitTransaction}, and any
 *       post-ambiguity non-fatal failure that is not provably-aborted): retry the commit to a
 *       definitive outcome bounded by {@code kafka.transactions.commit-retry} (a retried commit
 *       re-awaits the same EndTxn result); a definitive success finalizes
 *       ({@code onBatchCommitted}), a definitive {@link TransactionAbortableException} restores
 *       ({@code onBatchAborted}); still ambiguous ⇒ <strong>I9</strong>: NEITHER callback runs —
 *       the batch's shards are dropped ({@code onPartitionsLost}-shape), the producer is closed
 *       and recreated, {@code initTransactions()} resolves the dangling transaction broker-side
 *       (without reporting the outcome), cursors are re-resolved and the partitions re-enter
 *       recovery. The durable log decides.
 *   <li><em>Retry exhaustion</em> (park-and-degrade, R15): after {@code parkThreshold}
 *       consecutive failures the {@code cesium_degraded{loop="dispatch"}} gauge rises; the loop
 *       keeps polling (membership alive — intake needs no pause: applying tracker records is pure
 *       state building) and the failed entries sit in pending behind their penalty not-before, so
 *       nothing hot-spins. The gauge clears on the first successful commit.
 * </ul>
 *
 * <p><strong>Backpressure (§5.3):</strong> an ACTIVE shard pauses tracker intake above
 * {@code dispatch.max-pending-per-partition} (resumes below half of it) and while the global
 * pending cap is exceeded; a RECOVERING shard is <em>never</em> paused — replay must reach the
 * barrier. Pause state is observable as {@code cesium_shard_paused{partition}}.
 *
 * <p><strong>Idle cursor advancement (§3.5):</strong> a streaming, non-recovering partition
 * untouched by dispatch for {@code dispatch.idle-cursor-interval} gets a records-free transaction
 * ({@code begin; sendOffsets(cursor); commit}) when its cursor advanced materially — suppressed
 * while RECOVERING; all group-B offset commits go through transactions (one uniform fenced
 * path; the engine never calls {@code commitSync}).
 *
 * <p><strong>Metrics (§9):</strong> committed dispositions only ({@code cesium_dispatch_records},
 * the dispatch-lag histogram) flush after {@code commitTransaction} returns, so abort/retry
 * cycles never inflate them. {@code cesium_pending_entries}/{@code cesium_shard_state} are
 * store-owned gauges and not duplicated here; {@code cesium_lso_lag} needs a per-partition
 * read_uncommitted/read_committed offset diff from the admin plane and is deferred to the
 * observability milestone (M7) rather than half-implemented on the hot path.
 *
 * <p>{@link CrashPoints} fire at the §3.9 dispatch points D-1…D-3 (no-ops outside tests).
 */
public final class DispatchLoop implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(DispatchLoop.class);

    /** Bound on client close calls (§6: shutdown is bounded, then hard abort) — mirrors ingest. */
    static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(10);

    /**
     * Poll-timeout ceiling while a cursor resolution or barrier future is outstanding: the future
     * completes on the admin thread, and the loop must notice promptly without busy-spinning.
     */
    static final long RECOVERY_WAIT_POLL_MS = 10;

    private final DispatchLoopConfig config;
    private final Consumer<byte[], byte[]> consumer;
    private final Supplier<Producer<byte[], byte[]>> producerFactory;
    private final TrackerBackedStore store;
    private final SeekFetcher fetcher;
    private final RelayRecordFactory relayFactory;
    private final DispatchAdmin admin;
    private final Clock clock;

    // Owned by the loop thread after construction (the constructing thread hands off via start).
    private Producer<byte[], byte[]> producer;
    private boolean started;
    private boolean transactionInFlight;
    private int failureStreak;
    private long backoffUntilMs;
    private long lastPollReturnMs;
    private long epochCounter;
    private @Nullable InDoubtRecovery inDoubtRecovery;

    // Per-partition loop-side shard bookkeeping (the store owns the index; this is plumbing).
    private final Map<Integer, Long> shardEpochs = new HashMap<>();
    private final Set<Integer> needsCursor = new LinkedHashSet<>();
    private final Map<Integer, PendingBarrier> pendingBarriers = new LinkedHashMap<>();
    private final Set<Integer> streaming = new LinkedHashSet<>();
    private final Map<Integer, Long> lastCursorActivityMs = new HashMap<>();
    private final Map<Integer, Long> lastCommittedCursorOffset = new HashMap<>();
    private final Map<Integer, Integer> penaltyStreaks = new HashMap<>();
    private final Set<Integer> pausedForBackpressure = new LinkedHashSet<>();
    private final Map<Integer, AtomicInteger> pausedGauges = new HashMap<>();
    private final List<Gauge> registeredPausedGauges = new ArrayList<>();

    // Cross-thread state: stop flag, health/metrics, async send errors from the producer I/O thread.
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger degraded = new AtomicInteger();
    private final AtomicLong lastIterationMs = new AtomicLong();
    // Windowed per-iteration high-water mark of the inter-poll gap; reset at the top of runOnce.
    private final AtomicLong maxPollGapMs = new AtomicLong();

    // Single-writer-per-txn volatile; null is the steady state. Reads go through a local copy.
    private volatile @Nullable Exception asyncSendError;

    private final Counter dispatchedRecords;
    private final Counter payloadExpiredRecords;
    private final Counter droppedRecords;
    private final Counter committedTransactions;
    private final Timer commitTimer;
    private final Timer dispatchLag;
    private final Gauge degradedGauge;
    private final Gauge heartbeatGauge;
    private final Gauge pollGapGauge;
    private final Gauge penalizedPartitionsGauge;
    private final MeterRegistry registry;

    /**
     * @param config the loop settings (topics, batch/drain/cursor/fetch/penalty/backpressure
     *     budgets, retry bounds)
     * @param consumer the group-B tracker consumer; the loop owns its lifecycle from here on
     * @param producerFactory creates the transactional producer — called once at construction and
     *     again whenever an in-doubt commit forces a producer replacement (§3.8); each returned
     *     producer must be freshly constructed and not yet initialized for transactions
     * @param store the tracker-backed store whose index this loop feeds and drains
     * @param fetcher the §7 payload re-fetch pass; the loop owns its lifecycle from here on
     * @param relayFactory builds destination relays and loss notices (§2.4)
     * @param admin the §3.6 barrier snapshots and first-run probe (the cesium-admin plane)
     * @param meterRegistry metrics sink (§9 inventory); the loop's gauges are removed on close
     * @param clock injectable time source; the loop never reads wall time directly
     */
    public DispatchLoop(
            DispatchLoopConfig config,
            Consumer<byte[], byte[]> consumer,
            Supplier<Producer<byte[], byte[]>> producerFactory,
            TrackerBackedStore store,
            SeekFetcher fetcher,
            RelayRecordFactory relayFactory,
            DispatchAdmin admin,
            MeterRegistry meterRegistry,
            Clock clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.producerFactory = Objects.requireNonNull(producerFactory, "producerFactory");
        this.store = Objects.requireNonNull(store, "store");
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher");
        this.relayFactory = Objects.requireNonNull(relayFactory, "relayFactory");
        this.admin = Objects.requireNonNull(admin, "admin");
        this.registry = Objects.requireNonNull(meterRegistry, "meterRegistry");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.producer = Objects.requireNonNull(producerFactory.get(), "producerFactory.get()");

        this.dispatchedRecords = dispatchRecords("dispatched");
        this.payloadExpiredRecords = dispatchRecords("payload_expired");
        this.droppedRecords = dispatchRecords("dropped");
        this.committedTransactions =
                registry.counter("cesium.transactions", "loop", "dispatch", "result", "committed", "cause", "none");
        this.commitTimer = Timer.builder("cesium.txn.commit.seconds")
                .tag("loop", "dispatch")
                .description("commitTransaction latency per attempt (§9)")
                .register(registry);
        this.dispatchLag = Timer.builder("cesium.dispatch.lag.seconds")
                .description("actual minus scheduled dispatch time — the headline precision SLO (§9)")
                // §9 types this meter "histogram": publish the _bucket series so dispatch-lateness
                // quantiles (p50/p99) are computable server-side, not just an average (mirrors the
                // fetch-duration timer). A precision-latency SLO needs percentiles, not a mean.
                .publishPercentileHistogram()
                .register(registry);
        this.degradedGauge = Gauge.builder("cesium.degraded", degraded, AtomicInteger::get)
                .tag("loop", "dispatch")
                .description("1 while the dispatch loop is parked-and-degraded (design §3.8); cause in logs")
                .register(registry);
        this.heartbeatGauge = Gauge.builder(
                        "cesium.loop.last.iteration.timestamp.seconds", lastIterationMs, ms -> ms.get() / 1000.0)
                .tag("loop", "dispatch")
                .description("epoch seconds of the last loop iteration; feeds liveness")
                .register(registry);
        this.pollGapGauge = Gauge.builder("cesium.dispatch.poll.gap.seconds", maxPollGapMs, ms -> ms.get() / 1000.0)
                .description(
                        "max time between group-B polls this iteration; alert well below max.poll.interval.ms (§6)")
                .register(registry);
        // §7.3/§9 penalty-box occupancy: the source partitions currently held behind a fetch
        // not-before. penaltyStreaks is mutated only on the loop thread; the scraper reads size()
        // (a plain int field) — a benign stale read, never an exception, acceptable for a gauge.
        this.penalizedPartitionsGauge = Gauge.builder("cesium.fetch.penalized.partitions", penaltyStreaks, Map::size)
                .description("source partitions currently in the §7.3 fetch penalty box (penalty-box occupancy, §9)")
                .register(registry);
    }

    /** Runs the loop until {@link #stop()} or a fatal failure; closes all clients on the way out. */
    @Override
    public void run() {
        try {
            start();
            while (running.get()) {
                try {
                    runOnce();
                } catch (WakeupException e) {
                    if (running.get()) {
                        throw new DispatchLoopFatalException("consumer woken up without a stop request", e);
                    }
                    // Graceful shutdown: stop() interrupted the poll sleep; by I3 no transaction
                    // is in flight here, so this is a transaction boundary.
                }
            }
            log.info("dispatch loop stopped cleanly");
        } finally {
            closeClients();
        }
    }

    /**
     * Requests a graceful stop from any thread: raises the stop flag and {@code wakeup()}s the
     * consumer. Honored at transaction boundaries — an in-flight batch always runs to commit or
     * abort before the loop exits (I3 makes every poll a boundary), and the drain slice checks
     * the flag between transactions.
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
     * Fences this producer against any predecessor (resolving its dangling transaction
     * immediately — stable transactional.id, D10) and subscribes to the tracker. Separated from
     * {@link #run()} so unit tests can drive {@link #runOnce() iterations} deterministically.
     */
    void start() {
        if (started) {
            throw new IllegalStateException("dispatch loop already started");
        }
        started = true;
        producer.initTransactions();
        consumer.subscribe(List.of(config.trackerTopic()), new DispatchRebalanceListener());
        log.info(
                "dispatch loop started: tracker={} (group B subscribe, read_committed, reset=none)",
                config.trackerTopic());
    }

    /**
     * One iteration of the §6 event loop: heartbeat bookkeeping, pending in-doubt recovery, one
     * poll (sleep + intake + heartbeat), the I8 recovery protocol for new assignments, the
     * time-sliced drain, idle cursor advancement, backpressure, and store maintenance.
     */
    void runOnce() {
        lastIterationMs.set(clock.millis());
        // Windowed high-water: reset per iteration so the gauge reflects the worst poll gap of the
        // most recent iteration and decays back down. A monotonic-forever ratchet would latch on a
        // single legitimately large gap (a cold-segment fetch, a long recovery) and leave a
        // threshold alert firing permanently even after gaps return to healthy values (§6).
        maxPollGapMs.set(0);
        InDoubtRecovery recovery = inDoubtRecovery;
        if (recovery != null) {
            if (clock.millis() < backoffUntilMs) {
                heartbeatIntake();
                return;
            }
            if (!attemptProducerRecovery(recovery)) {
                scheduleBackoff("in_doubt_recovery");
                return;
            }
            inDoubtRecovery = null;
            log.info("in-doubt producer replacement completed; re-resolving cursors and re-entering recovery");
        }
        ConsumerRecords<byte[], byte[]> records = pollTracker(pollTimeout());
        if (records == null) {
            return; // transient poll failure: backoff armed inside pollTracker
        }
        applyRecords(records);
        reportPositions();
        resolveNewAssignments();
        if (clock.millis() >= backoffUntilMs) {
            drainDue();
        }
        if (clock.millis() >= backoffUntilMs) {
            // Re-checked: a drain failure arms the gate, and an idle commit against the same
            // degraded broker would only deepen the streak (the cursor staying behind is D-7-safe).
            maybeCommitIdleCursors();
        }
        applyBackpressure();
        store.maintenance();
    }

    // ------------------------------------------------------------------ poll & intake

    /**
     * The poll timeout per §6: sleep until the store's next deadline, capped at
     * {@code maxPollTimeout} — zero busy-polling, because an earlier deadline can only arrive via
     * {@code poll()} itself. Clamped down to {@value #RECOVERY_WAIT_POLL_MS} ms while a recovery
     * step is outstanding (a barrier future completes on the admin thread and must be noticed
     * promptly), and to the remaining retry backoff while the drain is gated (the store deadline
     * is ignored then — the penalty stamps keep {@code nextDeadlineMs} honest, and a due-now
     * entry must not turn the gated loop into a zero-timeout spin).
     */
    private Duration pollTimeout() {
        long now = clock.millis();
        long maxMs = config.maxPollTimeout().toMillis();
        long timeout;
        if (backoffUntilMs > now) {
            timeout = Math.min(maxMs, backoffUntilMs - now);
        } else {
            long deadline = store.nextDeadlineMs();
            timeout = deadline == Long.MAX_VALUE ? maxMs : Math.max(0, Math.min(deadline - now, maxMs));
        }
        if (!needsCursor.isEmpty() || !pendingBarriers.isEmpty()) {
            timeout = Math.min(timeout, RECOVERY_WAIT_POLL_MS);
        }
        return Duration.ofMillis(timeout);
    }

    /**
     * One tracker poll with the §3.8/§3.6 failure handling, returning {@code null} on a transient
     * failure (backoff armed). Also maintains the {@code cesium_dispatch_poll_gap_seconds}
     * per-iteration high-water mark — the time from the previous poll's return to this call (reset
     * at the top of {@link #runOnce()} so the gauge tracks recent gaps, not an all-time maximum).
     */
    private @Nullable ConsumerRecords<byte[], byte[]> pollTracker(Duration timeout) {
        long callAtMs = clock.millis();
        if (lastPollReturnMs != 0) {
            long gap = callAtMs - lastPollReturnMs;
            maxPollGapMs.accumulateAndGet(gap, Math::max);
        }
        try {
            return consumer.poll(timeout);
        } catch (WakeupException e) {
            throw e;
        } catch (NoOffsetForPartitionException e) {
            handleMissingCommittedOffsets(e.partitions());
            return ConsumerRecords.empty();
        } catch (InvalidOffsetException e) {
            // R11: the committed cursor points outside the live tracker log — the tracker was
            // truncated or recreated. Never auto-reset; fail fast with the DR runbook (R-9).
            throw new DispatchLoopFatalException(
                    "tracker offset out of range for " + e.partitions() + ": the tracker topic was truncated or"
                            + " recreated under the same name (§3.6 integrity, R-9). Never auto-reset — follow the"
                            + " tracker DR runbook (reset the tracker or accept loss explicitly)",
                    e);
        } catch (KafkaException e) {
            if (isFatal(e)) {
                throw new DispatchLoopFatalException("tracker poll failed fatally", e);
            }
            log.warn("tracker poll failed; backing off", e);
            scheduleBackoff(causeTag(e));
            return null;
        } finally {
            lastPollReturnMs = clock.millis();
        }
    }

    /**
     * The locked {@code auto.offset.reset=none} surfaced a partition with no committed offset
     * (design §3.6 step 2, D18): seek to the beginning explicitly <em>only</em> on a provable
     * first run — no committed offsets anywhere for the dispatch group — else fail fast with the
     * runbook (committed offsets expired per KIP-211, or were tampered with; an implicit reset
     * would silently lose all pending entries or mass-replay).
     */
    private void handleMissingCommittedOffsets(Set<TopicPartition> partitions) {
        final boolean groupHasOffsets;
        try {
            groupHasOffsets = admin.groupHasCommittedOffsets();
        } catch (RuntimeException e) {
            log.warn("first-run probe failed; retrying with backoff", e);
            scheduleBackoff(causeTag(e));
            return;
        }
        if (groupHasOffsets) {
            throw new DispatchLoopFatalException(
                    "no committed offset for assigned tracker partition(s) " + partitions + " while the dispatch"
                            + " group has committed offsets elsewhere, and auto.offset.reset=none is locked: either"
                            + " committed offsets expired (broker offsets.retention.minutes shorter than the outage)"
                            + " or the group state was modified — an operator must seed the group offsets explicitly;"
                            + " see the offset-reset runbook (§3.6, D18)");
        }
        log.warn(
                "provable first run for tracker partition(s) {} (no committed offsets anywhere in the dispatch"
                        + " group): seeking to the beginning explicitly — a full replay of the compacted tracker is"
                        + " correct but may be slow (§3.6 step 2)",
                partitions);
        consumer.seekToBeginning(partitions);
    }

    /** Feeds delivered records through the headers-carrying SPI variant (engine obligation). */
    private void applyRecords(ConsumerRecords<byte[], byte[]> records) {
        if (records.isEmpty()) {
            return;
        }
        for (ConsumerRecord<byte[], byte[]> record : records) {
            if (!streaming.contains(record.partition())) {
                continue; // recovery not begun (paused partition raced a delivery); replay covers it
            }
            store.onTrackerRecord(record.partition(), record.offset(), record.key(), record.value(), record.headers());
        }
    }

    /**
     * Reports the consumer position for every streaming partition after every poll (SPI
     * obligation): tracker offsets are not dense — the offsets below a barrier are usually
     * control markers a read_committed consumer never delivers — so the store can only promote a
     * shard ACTIVE from these reports.
     */
    private void reportPositions() {
        for (int partition : streaming) {
            TopicPartition tp = trackerPartition(partition);
            try {
                store.onTrackerPosition(partition, consumer.position(tp));
            } catch (WakeupException e) {
                throw e;
            } catch (RuntimeException e) {
                log.warn("position lookup failed for {}; replay promotion may lag one poll", tp, e);
            }
        }
    }

    /** A pure heartbeat-and-intake iteration while an in-doubt producer recovery is pending. */
    private void heartbeatIntake() {
        ConsumerRecords<byte[], byte[]> records = pollTracker(pollTimeout());
        if (records != null) {
            applyRecords(records);
            reportPositions();
        }
    }

    // ------------------------------------------------------------------ recovery protocol (I8)

    /**
     * Drives the §3.6 protocol for partitions marked assigned by the callbacks: resolve the
     * committed cursor (step 1–2), then request the barrier snapshot (step 3, strictly after),
     * then — when the epoch-tagged future completes — {@code beginRecovery}, seek, resume.
     * Gated while an in-doubt producer replacement is pending: the committed-offset fetch must
     * follow {@code initTransactions()} so the dangling transaction is already resolved instead
     * of stalling the fetch until {@code transaction.timeout.ms}.
     */
    private void resolveNewAssignments() {
        if (inDoubtRecovery != null) {
            return;
        }
        completeBarriers();
        if (!needsCursor.isEmpty()) {
            for (Integer partition : List.copyOf(needsCursor)) {
                if (!shardEpochs.containsKey(partition)) {
                    needsCursor.remove(partition); // revoked while queued
                    continue;
                }
                try {
                    resolveCursor(partition);
                    needsCursor.remove(partition);
                } catch (WakeupException | DispatchLoopFatalException e) {
                    throw e;
                } catch (RuntimeException e) {
                    if (isFatal(e)) {
                        throw new DispatchLoopFatalException(
                                "cursor resolution failed fatally for tracker partition " + partition, e);
                    }
                    // Transient (e.g. committed() timed out waiting out UNSTABLE_OFFSET_COMMIT
                    // against a degraded broker): keep the partition queued and retry.
                    log.warn("cursor resolution failed transiently for tracker partition {}; retrying", partition, e);
                    break;
                }
            }
        }
        completeBarriers();
    }

    /**
     * Steps 1–3 of §3.6 for one partition: the blocking committed-offset fetch (the
     * {@code require_stable} wait is the I8 synchronization point), the integrity checks, and the
     * epoch-tagged barrier request — submitted strictly after the fetch resolved.
     */
    private void resolveCursor(int partition) {
        TopicPartition tp = trackerPartition(partition);
        // Step 1: blocks through UNSTABLE_OFFSET_COMMIT until prior dispatch txns resolved (§3.4.2).
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(Set.of(tp));
        OffsetAndMetadata offset = committed == null ? null : committed.get(tp);
        Long beginningBoxed = consumer.beginningOffsets(Set.of(tp)).get(tp);
        long beginning = beginningBoxed == null ? 0L : beginningBoxed;
        final TrackerCursor cursor;
        if (offset == null) {
            cursor = firstRunCursor(tp, beginning);
        } else {
            // Step 2 (R11): a committed offset below the beginning means truncation/recreation.
            if (offset.offset() < beginning) {
                throw new DispatchLoopFatalException("committed cursor " + offset.offset() + " for " + tp
                        + " lies below the partition beginning " + beginning + ": the tracker was truncated or"
                        + " recreated (§3.6 integrity, R-9). Never auto-reset — follow the tracker DR runbook");
            }
            String metadata = offset.metadata();
            cursor = new TrackerCursor(offset.offset(), metadata == null ? "" : metadata);
        }
        // Step 3 (I8/D19): the barrier snapshot is requested strictly AFTER the committed fetch
        // resolved, epoch-tagged so a revoke discards it. The cursor-sidecar identity is
        // validated by the store when beginRecovery decodes it (store-owned encoding).
        Long epoch = shardEpochs.get(partition);
        if (epoch == null) {
            return; // revoked since the queue snapshot (defensive; the loop is single-threaded)
        }
        pendingBarriers.put(partition, new PendingBarrier(epoch, cursor, admin.trackerHighWatermark(partition)));
    }

    /** The §3.6 explicit first-run path: provable only when the whole group has never committed. */
    private TrackerCursor firstRunCursor(TopicPartition tp, long beginning) {
        if (admin.groupHasCommittedOffsets()) {
            throw new DispatchLoopFatalException(
                    "no committed offset for " + tp + " while the dispatch group has committed offsets elsewhere"
                            + " (auto.offset.reset=none locked): committed offsets expired or were removed — an"
                            + " operator must seed the group offsets explicitly; see the offset-reset runbook"
                            + " (§3.6, D18)");
        }
        log.warn(
                "provable first run for {}: starting recovery from the partition beginning {} — a full replay of"
                        + " the compacted tracker is correct but may be slow (§3.6 step 2)",
                tp,
                beginning);
        return new TrackerCursor(beginning, "");
    }

    /**
     * Finishes the protocol for every completed, still-current barrier future: upper-bound
     * integrity check, {@code beginRecovery}, seek to the cursor, position report (promotes
     * immediately when the barrier is at or below the cursor), resume intake.
     */
    private void completeBarriers() {
        if (pendingBarriers.isEmpty()) {
            return;
        }
        for (Map.Entry<Integer, PendingBarrier> entry : List.copyOf(pendingBarriers.entrySet())) {
            int partition = entry.getKey();
            PendingBarrier pending = entry.getValue();
            Long currentEpoch = shardEpochs.get(partition);
            if (currentEpoch == null || currentEpoch != pending.epoch()) {
                pendingBarriers.remove(partition); // stale (revoked / re-assigned): discard (I8)
                continue;
            }
            if (!pending.future().isDone()) {
                continue;
            }
            pendingBarriers.remove(partition);
            final long barrier;
            try {
                barrier = pending.future().join();
            } catch (CompletionException | CancellationException e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                if (cause instanceof RuntimeException runtime && isFatal(runtime)) {
                    throw new DispatchLoopFatalException(
                            "barrier snapshot failed fatally for tracker partition " + partition, runtime);
                }
                log.warn("barrier snapshot failed for tracker partition {}; re-resolving the cursor", partition, e);
                needsCursor.add(partition); // restart at step 1 — re-fetching keeps I8 trivially
                continue;
            }
            if (pending.cursor().offset() > barrier) {
                throw new DispatchLoopFatalException(
                        "committed cursor " + pending.cursor().offset()
                                + " for tracker partition " + partition + " exceeds the live end offset " + barrier
                                + ": the tracker was truncated or recreated (§3.6 integrity, R-9). Never auto-reset —"
                                + " follow the tracker DR runbook");
            }
            store.beginRecovery(partition, pending.cursor(), barrier);
            TopicPartition tp = trackerPartition(partition);
            consumer.seek(tp, pending.cursor().offset());
            streaming.add(partition);
            lastCursorActivityMs.put(partition, clock.millis());
            lastCommittedCursorOffset.put(partition, pending.cursor().offset());
            // Promotes immediately when barrier <= cursor (§3.6 step 3 "immediately ACTIVE").
            store.onTrackerPosition(partition, pending.cursor().offset());
            consumer.resume(Set.of(tp));
            log.info(
                    "tracker partition {} recovering: cursor={} barrier={} ({} records to replay)",
                    partition,
                    pending.cursor().offset(),
                    barrier,
                    Math.max(0, barrier - pending.cursor().offset()));
        }
    }

    // ------------------------------------------------------------------ drain & dispatch (§3.2)

    /**
     * The §6 time-sliced drain: back-to-back dispatch transactions until the slice deadline, each
     * separated by a zero-timeout poll (I3 holds — the transaction closed before the poll), so
     * the maximum poll gap is one transaction.
     */
    private void drainDue() {
        long sliceDeadlineMs = clock.millis() + config.drainSlice().toMillis();
        while (running.get() && clock.millis() < sliceDeadlineMs) {
            DueBatch batch = store.pollDue(clock.millis(), config.maxBatchEntries());
            if (batch.size() == 0) {
                return;
            }
            if (!dispatchBatch(batch)) {
                return; // failure or nothing settle-able: backoff/penalties pace the retry
            }
            ConsumerRecords<byte[], byte[]> records = pollTracker(Duration.ZERO);
            if (records == null) {
                return;
            }
            applyRecords(records);
            reportPositions();
        }
    }

    /**
     * One due batch through fetch, classification, the §3.2 transaction, and the §3.8 taxonomy.
     *
     * @return true when the batch made committed progress (keep draining)
     */
    private boolean dispatchBatch(DueBatch batch) {
        long nowMs = clock.millis();
        final FetchResult fetched;
        try {
            fetched = fetcher.fetch(
                    batch, config.maxBatchBytes(), nowMs + config.fetchTimeout().toMillis());
        } catch (RuntimeException e) {
            if (isFatal(e)) {
                throw new DispatchLoopFatalException("payload fetch failed fatally", e);
            }
            // Pre-transaction failure: nothing produced — restoring is definitive and safe.
            store.onBatchAborted(batch);
            penalizePartitions(distinctPartitions(batch), nowMs);
            log.warn("payload fetch pass failed; batch restored and penalized", e);
            scheduleBackoff(causeTag(e));
            return false;
        }
        Classified classified = classify(batch, fetched);
        if (classified.goneCount > 0 && config.onUnfetchablePayload() == UnfetchablePayloadPolicy.FAIL) {
            // Nothing produced yet: restore (definitive), then surface the environment failure.
            store.onBatchAborted(batch);
            throw new DispatchLoopFatalException("payload(s) provably gone from the source at dispatch time and"
                    + " dispatch.on-unfetchable-payload=FAIL: first lost entry source partition "
                    + batch.sourcePartition(classified.firstGoneIndex) + " offset "
                    + batch.sourceOffset(classified.firstGoneIndex) + " (§7.4; D-9 would resolve this under"
                    + " DLQ/DROP — FAIL is an explicit operator choice to stop instead)");
        }
        if (classified.settledCount == 0) {
            // Nothing fetchable this pass: restore everything; transient penalties pace re-polls.
            store.onBatchAborted(batch);
            stampTransientPenalties(classified, nowMs);
            return false;
        }
        DueBatch settledView = classified.settledCount == batch.size()
                ? batch
                : DueBatchView.select(batch, classified.settledIndices, classified.settledCount);
        final Map<Integer, TrackerCursor> cursors;
        try {
            cursors = transact(batch, settledView, classified, fetched, nowMs);
        } catch (InDoubtCommit inDoubt) {
            recoverFromInDoubtCommit(distinctPartitions(batch), inDoubt);
            return false;
        } catch (DispatchLoopFatalException fatal) {
            abortIfInFlight();
            abortedTransactions("fatal").increment();
            throw fatal;
        } catch (RuntimeException e) {
            if (isFatal(e)) {
                // Fatal (§3.8): no abort attempt — a fenced producer cannot abort; the broker
                // resolves the dangling transaction. Close + fail; never restore (I9-compatible).
                throw new DispatchLoopFatalException("fatal dispatch transaction failure", e);
            }
            // Definitively abortable (incl. TransactionAbortableException from a retried commit —
            // the broker-affirmed "not committed"): nothing committed, restore the FULL batch.
            abortIfInFlight();
            abortedTransactions(causeTag(e)).increment();
            store.onBatchAborted(batch);
            penalizePartitions(distinctPartitions(batch), clock.millis());
            log.warn("dispatch transaction aborted; batch restored for retry", e);
            scheduleBackoff(causeTag(e));
            return false;
        }
        // Success epilogue — the harness-modelled order: commit, THEN restore the carry-over.
        store.onBatchCommitted(settledView);
        if (classified.carryCount > 0) {
            store.onBatchAborted(DueBatchView.select(batch, classified.carryIndices, classified.carryCount));
        }
        stampTransientPenalties(classified, nowMs);
        clearPenaltiesOnSuccess(classified, batch);
        flushBatchMetrics(batch, classified, nowMs);
        long commitMs = clock.millis();
        for (Map.Entry<Integer, TrackerCursor> cursor : cursors.entrySet()) {
            lastCursorActivityMs.put(cursor.getKey(), commitMs);
            lastCommittedCursorOffset.put(cursor.getKey(), cursor.getValue().offset());
        }
        onBatchSuccess();
        return true;
    }

    /**
     * The §3.2 transaction: begin; FOUND ⇒ destination relay (CLAMP marker from the batch), GONE ⇒
     * unfetchable policy record; per-reason completion tombstones on the entry's own partition
     * number; cursors for every touched partition computed against the <em>settled</em> view
     * (carry-over still in flight — §3.5 sub-batch commits); offsets with freshly fetched group
     * metadata (I2); commit with the in-doubt retry discipline. Fires crash points D-1…D-3.
     *
     * @return the cursor committed per touched partition
     */
    private Map<Integer, TrackerCursor> transact(
            DueBatch batch, DueBatch settledView, Classified classified, FetchResult fetched, long nowMs) {
        CrashPoints.maybeFire(CrashPoints.DISPATCH_BEFORE_BEGIN);
        asyncSendError = null;
        producer.beginTransaction();
        transactionInFlight = true;
        boolean dlqNotices = config.onUnfetchablePayload() == UnfetchablePayloadPolicy.DLQ;
        for (int s = 0; s < classified.settledCount; s++) {
            int i = classified.settledIndices[s];
            if (fetched.outcome(i) == FetchOutcome.FOUND) {
                send(relayFactory.relayScheduled(fetched.record(i), nowMs, batch.dispatchAtMs(i), batch.clamped(i)));
            } else if (dlqNotices) {
                send(relayFactory.payloadExpiredDlqRecord(
                        config.sourceTopic(),
                        batch.sourcePartition(i),
                        batch.sourceOffset(i),
                        batch.dispatchAtMs(i),
                        nowMs));
            }
        }
        sendCompletions(batch, settledView, classified);
        throwIfAsyncSendFailed();
        Map<Integer, TrackerCursor> cursors = new LinkedHashMap<>();
        Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
        for (int partition : touchedPartitions(batch, classified)) {
            TrackerCursor cursor = store.committedCursor(partition, settledView);
            cursors.put(partition, cursor);
            offsets.put(trackerPartition(partition), new OffsetAndMetadata(cursor.offset(), cursor.metadata()));
        }
        // I2: group metadata fetched immediately before the call — the fencing hook; cursors for
        // EVERY partition the transaction's tombstones touch ride this same commit.
        producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
        CrashPoints.maybeFire(CrashPoints.DISPATCH_AFTER_SENDS);
        commitWithRetry();
        return cursors;
    }

    /**
     * Per-reason completion tombstones (the {@code encodeCompletions} grouping obligation): one
     * call per {@link CompletionReason} present in the settled set, each receiving a sub-batch
     * view covering exactly that reason's entries — or the whole settled view when homogeneous.
     */
    private void sendCompletions(DueBatch batch, DueBatch settledView, Classified classified) {
        sendCompletionGroup(
                batch,
                settledView,
                classified,
                classified.foundIndices,
                classified.foundCount,
                CompletionReason.DISPATCHED);
        CompletionReason goneReason = config.onUnfetchablePayload() == UnfetchablePayloadPolicy.DLQ
                ? CompletionReason.PAYLOAD_MISSING_DLQ
                : CompletionReason.DROPPED;
        sendCompletionGroup(batch, settledView, classified, classified.goneIndices, classified.goneCount, goneReason);
    }

    private void sendCompletionGroup(
            DueBatch batch,
            DueBatch settledView,
            Classified classified,
            int[] indices,
            int count,
            CompletionReason reason) {
        if (count == 0) {
            return;
        }
        DueBatch view = count == classified.settledCount ? settledView : DueBatchView.select(batch, indices, count);
        List<TrackerRecordData> tombstones = store.encodeCompletions(view, reason);
        if (tombstones.size() != view.size()) {
            throw new DispatchLoopFatalException("store contract violation: encodeCompletions returned "
                    + tombstones.size() + " tombstone(s) for a " + view.size() + "-entry " + reason + " view");
        }
        for (int j = 0; j < view.size(); j++) {
            TrackerRecordData data = tombstones.get(j);
            send(new ProducerRecord<>(
                    config.trackerTopic(), view.sourcePartition(j), null, data.key(), data.value(), data.headers()));
        }
    }

    /**
     * Commits, retrying ambiguous timeouts to a definitive outcome bounded by
     * {@code kafka.transactions.commit-retry} (§3.8 in-doubt step a). The verified 4.3.0 facts
     * shape the classification: a retried commit re-awaits the same EndTxn result; normal return
     * is definitively committed; {@link TransactionAbortableException} (anywhere in the chain) is
     * definitively NOT committed and is rethrown for the abortable path even after an ambiguous
     * attempt; any other post-ambiguity non-fatal failure cannot prove "aborted" (D20) and
     * escalates to in-doubt; fatal classifications (including a post-ambiguity
     * {@link ProducerFencedException} — still ambiguous as to the outcome) fail the loop with no
     * restore, which satisfies I9. Fires D-3 after the commit returns, before any bookkeeping.
     */
    private void commitWithRetry() {
        int attempts = 0;
        boolean ambiguous = false;
        while (true) {
            try {
                commitTimer.record(producer::commitTransaction);
                CrashPoints.maybeFire(CrashPoints.DISPATCH_DURING_COMMIT);
                transactionInFlight = false;
                committedTransactions.increment();
                return;
            } catch (TimeoutException timeout) {
                attempts++;
                ambiguous = true;
                if (attempts >= config.commitRetryLimit()) {
                    throw new InDoubtCommit(attempts, timeout);
                }
                log.warn(
                        "commitTransaction timed out (attempt {}/{}); retrying to a definitive outcome",
                        attempts,
                        config.commitRetryLimit(),
                        timeout);
            } catch (RuntimeException e) {
                if (isDefinitivelyNotCommitted(e)) {
                    // KIP-890 TRANSACTION_ABORTABLE (or exhausted-retry synthesis): the broker
                    // affirms the transaction did not commit — abortable even post-ambiguity.
                    throw e;
                }
                if (!ambiguous || isFatal(e)) {
                    // First-attempt non-timeout failures are non-ambiguous (§3.8 row 1); fatal
                    // failures always fail the loop and never restore.
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

    // ------------------------------------------------------------------ in-doubt commits (I9)

    /**
     * The I9 procedure (§3.8 step b): the broker may have committed, so NEITHER
     * {@code onBatchCommitted} nor {@code onBatchAborted} may run for any view of the batch —
     * restoring is the duplicate vector. Every partition the drained batch references is dropped
     * ({@code onPartitionsLost} shape — the carry-over entries ride the drop too; re-recovery is
     * cheap with the v2 cursor and unconditionally sound), the producer is replaced and fenced,
     * and the partitions re-enter the §3.6 protocol from the committed-cursor fetch — which, run
     * after {@code initTransactions()} resolved the dangling transaction, returns the truth
     * either way.
     */
    private void recoverFromInDoubtCommit(int[] partitions, InDoubtCommit inDoubt) {
        String cause = inDoubt.failure instanceof TimeoutException ? "commit_timeout" : causeTag(inDoubt.failure);
        registry.counter("cesium.transactions", "loop", "dispatch", "result", "in_doubt", "cause", cause)
                .increment();
        log.warn(
                "commitTransaction outcome ambiguous after {} attempts; dropping {} shard(s), replacing the"
                        + " producer, and re-entering recovery (I9 — the batch is never restored)",
                inDoubt.attempts,
                partitions.length,
                inDoubt.failure);
        transactionInFlight = false;
        Set<Integer> affected = new LinkedHashSet<>();
        List<TopicPartition> toPause = new ArrayList<>();
        for (int partition : partitions) {
            if (!shardEpochs.containsKey(partition)) {
                continue; // no longer owned (cannot happen mid-iteration per I3; defensive)
            }
            affected.add(partition);
            toPause.add(trackerPartition(partition));
        }
        if (!affected.isEmpty()) {
            store.onPartitionsLost(affected);
            store.onPartitionsAssigned(affected); // re-adopt: still owned in group B; state rebuilds
            for (int partition : affected) {
                shardEpochs.put(partition, ++epochCounter);
                pendingBarriers.remove(partition);
                streaming.remove(partition);
                pausedForBackpressure.remove(partition);
                lastCursorActivityMs.remove(partition);
                lastCommittedCursorOffset.remove(partition);
                needsCursor.add(partition);
            }
            consumer.pause(toPause);
        }
        InDoubtRecovery recovery = new InDoubtRecovery();
        if (!attemptProducerRecovery(recovery)) {
            inDoubtRecovery = recovery;
        }
        scheduleBackoff("in_doubt");
    }

    /**
     * One producer-replacement attempt of the in-doubt procedure: hard-close the unusable
     * producer (the in-doubt one, or a half-initialized replacement from a previous failed
     * attempt), build a fresh one, and fence it — {@code initTransactions()} resolves the
     * dangling transaction deterministically broker-side without reporting the outcome. The
     * cursor re-resolution that follows (gated on this completing) reads the resolved truth.
     * Returns false on a transient failure — the caller schedules a retry (R15).
     */
    private boolean attemptProducerRecovery(InDoubtRecovery recovery) {
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
            return true;
        } catch (WakeupException e) {
            throw e; // stop() raced the recovery: run() resolves clean shutdown vs unexpected wakeup
        } catch (RuntimeException e) {
            if (isFatal(e)) {
                throw new DispatchLoopFatalException("in-doubt commit recovery failed fatally", e);
            }
            log.warn("in-doubt recovery attempt failed transiently; retrying with capped backoff (§3.8/R15)", e);
            return false;
        }
    }

    // ------------------------------------------------------------------ idle cursor advancement

    /**
     * §3.5 "Advancement": a streaming, non-recovering partition untouched for
     * {@code dispatch.idle-cursor-interval} whose cursor advanced materially gets a records-free
     * transaction ({@code begin; sendOffsets; commit}) so replay cost tracks the live position.
     * Attempts are rate-limited by the same interval whatever the outcome — an aborted idle
     * commit just leaves the cursor behind (extra replay only, D-7). Suppressed while RECOVERING
     * (unnecessary; recovery is short) and while the failure backoff gates the loop.
     */
    private void maybeCommitIdleCursors() {
        if (inDoubtRecovery != null || streaming.isEmpty()) {
            return;
        }
        long intervalMs = config.idleCursorInterval().toMillis();
        for (Integer partition : List.copyOf(streaming)) {
            long now = clock.millis();
            if (store.isRecovering(partition)) {
                continue;
            }
            long lastTouched = lastCursorActivityMs.getOrDefault(partition, now);
            if (now - lastTouched < intervalMs) {
                continue;
            }
            lastCursorActivityMs.put(partition, now); // rate-limit attempts regardless of outcome
            TrackerCursor cursor = store.committedCursor(partition, DueBatchView.EMPTY);
            long lastCommitted = lastCommittedCursorOffset.getOrDefault(partition, Long.MIN_VALUE);
            if (cursor.offset() <= lastCommitted) {
                // Not materially advanced: skip the empty transaction. But committedCursor above
                // has already staged a proposal on the shard (hasProposal=true); leaving it set
                // would let the NEXT onBatchCommitted — a later idle partition, or an unrelated
                // dispatch commit in a future runOnce — promote this never-sent cursor into the
                // store's monotonic floor, desyncing it from Kafka's committed offset. EMPTY
                // restores nothing and clears every outstanding hasProposal flag; in the
                // single-threaded idle loop the only live proposal is the one just staged.
                store.onBatchAborted(DueBatchView.EMPTY);
                continue;
            }
            try {
                producer.beginTransaction();
                transactionInFlight = true;
                producer.sendOffsetsToTransaction(
                        Map.of(trackerPartition(partition), new OffsetAndMetadata(cursor.offset(), cursor.metadata())),
                        consumer.groupMetadata());
                commitWithRetry();
            } catch (InDoubtCommit inDoubt) {
                // No batch state exists to mis-restore, but the producer is unusable and the
                // commit may have landed: run the uniform I9 procedure for the one partition.
                recoverFromInDoubtCommit(new int[] {partition}, inDoubt);
                return;
            } catch (RuntimeException e) {
                if (isFatal(e)) {
                    throw new DispatchLoopFatalException("fatal idle-cursor transaction failure", e);
                }
                abortIfInFlight();
                abortedTransactions(causeTag(e)).increment();
                log.warn(
                        "idle cursor advancement aborted for tracker partition {} (cursor stays behind; extra"
                                + " replay only, D-7)",
                        partition,
                        e);
                continue;
            }
            store.onBatchCommitted(DueBatchView.EMPTY);
            lastCommittedCursorOffset.put(partition, cursor.offset());
        }
    }

    // ------------------------------------------------------------------ backpressure (§5.3)

    /**
     * Pause/resume tracker intake per shard: above the per-partition high-water mark (or while
     * the global cap is exceeded) an ACTIVE shard's tracker partition pauses — backlog
     * accumulates durably in the tracker topic; below the low-water mark (half the high) it
     * resumes. A RECOVERING shard is never paused: replay must reach the barrier (R7).
     */
    private void applyBackpressure() {
        if (streaming.isEmpty()) {
            return;
        }
        long high = config.maxPendingPerPartition();
        long low = Math.max(1, high / 2);
        long cap = config.maxPendingTotal();
        long total = 0;
        if (cap > 0) {
            for (int partition : streaming) {
                total += store.pendingCount(partition);
            }
        }
        boolean globalHigh = cap > 0 && total > cap;
        boolean globalRecovered = cap <= 0 || total <= cap / 2;
        for (int partition : streaming) {
            boolean paused = pausedForBackpressure.contains(partition);
            if (store.isRecovering(partition)) {
                if (paused) {
                    resumeShard(partition); // a paused shard re-entering recovery must stream
                }
                continue;
            }
            long pending = store.pendingCount(partition);
            if (!paused && (pending > high || globalHigh)) {
                consumer.pause(Set.of(trackerPartition(partition)));
                pausedForBackpressure.add(partition);
                pausedGauge(partition).set(1);
                log.info("backpressure: paused tracker partition {} (pending={}, total={})", partition, pending, total);
            } else if (paused && pending <= low && !globalHigh && globalRecovered) {
                resumeShard(partition);
            }
        }
    }

    private void resumeShard(int partition) {
        consumer.resume(Set.of(trackerPartition(partition)));
        pausedForBackpressure.remove(partition);
        pausedGauge(partition).set(0);
        log.info("backpressure: resumed tracker partition {}", partition);
    }

    private AtomicInteger pausedGauge(int partition) {
        return pausedGauges.computeIfAbsent(partition, p -> {
            AtomicInteger state = new AtomicInteger();
            registeredPausedGauges.add(Gauge.builder("cesium.shard.paused", state, AtomicInteger::get)
                    .tag("partition", String.valueOf(p))
                    .description("1 while backpressure pauses the shard's tracker intake (§5.3)")
                    .register(registry));
            return state;
        });
    }

    // ------------------------------------------------------------------ classification & penalties

    /** Single pass over the fetch result building the settled / per-reason / carry-over index sets. */
    private static Classified classify(DueBatch batch, FetchResult fetched) {
        int n = batch.size();
        if (fetched.size() != n) {
            throw new DispatchLoopFatalException("fetcher contract violation: " + fetched.size() + " outcome(s) for a "
                    + n + "-entry candidate batch");
        }
        Classified c = new Classified(n);
        for (int i = 0; i < n; i++) {
            switch (fetched.outcome(i)) {
                case FOUND -> {
                    c.settledIndices[c.settledCount++] = i;
                    c.foundIndices[c.foundCount++] = i;
                }
                case GONE -> {
                    c.settledIndices[c.settledCount++] = i;
                    c.goneIndices[c.goneCount++] = i;
                    if (c.firstGoneIndex < 0) {
                        c.firstGoneIndex = i;
                    }
                }
                case TRANSIENT -> {
                    c.carryIndices[c.carryCount++] = i;
                    c.transientPartitions.add(batch.sourcePartition(i));
                }
                case CARRY_OVER -> c.carryIndices[c.carryCount++] = i;
            }
        }
        return c;
    }

    /** The distinct partitions whose scheduler state the transaction affects (I2's touched set). */
    private static int[] touchedPartitions(DueBatch batch, Classified classified) {
        Set<Integer> touched = new TreeSet<>();
        for (int s = 0; s < classified.settledCount; s++) {
            touched.add(batch.sourcePartition(classified.settledIndices[s]));
        }
        int[] result = new int[touched.size()];
        int next = 0;
        for (int partition : touched) {
            result[next++] = partition;
        }
        return result;
    }

    private static int[] distinctPartitions(DueBatch batch) {
        Set<Integer> partitions = new TreeSet<>();
        for (int i = 0; i < batch.size(); i++) {
            partitions.add(batch.sourcePartition(i));
        }
        int[] result = new int[partitions.size()];
        int next = 0;
        for (int partition : partitions) {
            result[next++] = partition;
        }
        return result;
    }

    /**
     * §7.3 penalty box (D22): every source partition with a TRANSIENT outcome gets a not-before
     * stamp with per-partition exponential backoff ({@code dispatch.fetch.penalty.backoff} →
     * {@code backoff-max}), so a degraded source partition neither head-of-line blocks healthy
     * ones nor hot-spins the loop.
     */
    private void stampTransientPenalties(Classified classified, long nowMs) {
        for (int partition : classified.transientPartitions) {
            int streak = penaltyStreaks.merge(partition, 1, Integer::sum);
            long backoffMs = boundedExponential(
                    config.penaltyBackoff().toMillis(),
                    config.penaltyBackoffMax().toMillis(),
                    streak);
            store.penalizeSourcePartition(partition, DelayHeaderCodec.saturatedAddMillis(nowMs, backoffMs));
        }
    }

    /** Clears the penalty (stamp in the past + streak reset) for partitions that fetched cleanly. */
    private void clearPenaltiesOnSuccess(Classified classified, DueBatch batch) {
        for (int s = 0; s < classified.settledCount; s++) {
            int partition = batch.sourcePartition(classified.settledIndices[s]);
            if (!classified.transientPartitions.contains(partition) && penaltyStreaks.remove(partition) != null) {
                store.penalizeSourcePartition(partition, 0);
            }
        }
    }

    /**
     * The §3.8 park machinery for a definitively failed batch: its partitions get a not-before
     * aligned with the loop's retry gate, so {@code nextDeadlineMs} stops reporting the failed
     * entries as due-now and the poll timeout stays honest while the loop backs off.
     */
    private void penalizePartitions(int[] partitions, long nowMs) {
        long backoffMs = boundedExponential(
                config.retryBackoffInitial().toMillis(),
                config.retryBackoffMax().toMillis(),
                failureStreak + 1);
        long notBefore = DelayHeaderCodec.saturatedAddMillis(nowMs, backoffMs);
        for (int partition : partitions) {
            store.penalizeSourcePartition(partition, notBefore);
        }
    }

    // ------------------------------------------------------------------ taxonomy plumbing

    /** Fire-and-forget transactional send; async failures are captured and surfaced pre-commit. */
    private void send(ProducerRecord<byte[], byte[]> record) {
        // The future is deliberately unused: completion errors are observed via the callback (and
        // the commit itself would surface them); blocking per record would serialize the batch.
        var unused = producer.send(record, (metadata, exception) -> {
            if (exception != null && asyncSendError == null) {
                asyncSendError = exception;
            }
        });
    }

    /** Surfaces an asynchronously failed send before the offsets are sent (mirrors ingest). */
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
     * Records a consecutive failure and arms the retry gate: the drain (and idle advancement)
     * pauses until a capped exponential backoff elapses, while polls continue — membership and
     * intake stay alive (applying tracker records needs no producer). At {@code parkThreshold}
     * consecutive failures the loop is parked-and-degraded (§3.8/R15): gauge up, alert-level
     * log — the retry cadence continues; the loop never gives up and never crash-loops.
     */
    private void scheduleBackoff(String cause) {
        failureStreak++;
        long backoffMs = boundedExponential(
                config.retryBackoffInitial().toMillis(),
                config.retryBackoffMax().toMillis(),
                failureStreak);
        backoffUntilMs = DelayHeaderCodec.saturatedAddMillis(clock.millis(), backoffMs);
        if (failureStreak >= config.parkThreshold()) {
            if (degraded.compareAndSet(0, 1)) {
                log.error(
                        "dispatch loop parked-and-degraded after {} consecutive failures (cause={}); holding"
                                + " membership with heartbeat polls and retrying every <= {} ms",
                        failureStreak,
                        cause,
                        config.retryBackoffMax().toMillis());
            }
        } else {
            log.warn("dispatch batch failed (streak={}, cause={}); retrying in {} ms", failureStreak, cause, backoffMs);
        }
    }

    /** §3.8 "resume on success": clears the failure streak, the gate, and the degraded flag. */
    private void onBatchSuccess() {
        if (failureStreak == 0) {
            return;
        }
        failureStreak = 0;
        backoffUntilMs = 0;
        if (degraded.compareAndSet(1, 0)) {
            log.info("dispatch loop recovered; degraded flag cleared");
        }
    }

    /** Capped exponential backoff: {@code initial * 2^(streak-1)}, never above the cap. */
    private static long boundedExponential(long initialMs, long maxMs, int streak) {
        long backoff = initialMs;
        for (int i = 1; i < streak && backoff < maxMs; i++) {
            backoff <<= 1;
        }
        return Math.min(backoff, maxMs);
    }

    /** Flushes committed dispositions only (§9): counts and the lag histogram, post-commit. */
    private void flushBatchMetrics(DueBatch batch, Classified classified, long nowMs) {
        incrementBy(dispatchedRecords, classified.foundCount);
        if (config.onUnfetchablePayload() == UnfetchablePayloadPolicy.DLQ) {
            incrementBy(payloadExpiredRecords, classified.goneCount);
        } else {
            incrementBy(droppedRecords, classified.goneCount);
        }
        for (int f = 0; f < classified.foundCount; f++) {
            long lagMs = Math.max(0, nowMs - batch.dispatchAtMs(classified.foundIndices[f]));
            dispatchLag.record(Duration.ofMillis(lagMs));
        }
    }

    private static void incrementBy(Counter counter, long amount) {
        if (amount > 0) {
            counter.increment((double) amount);
        }
    }

    /**
     * Closes all clients with a bounded budget (§6: shutdown bounded, then hard abort) and
     * removes this loop's gauges from the registry — Micrometer returns the existing meter on
     * name+tag collision, so a restarted loop must bind fresh gauges over its own state.
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
        try {
            fetcher.close();
        } catch (RuntimeException e) {
            log.warn("seek fetcher close failed", e);
        }
        registry.remove(degradedGauge);
        registry.remove(heartbeatGauge);
        registry.remove(pollGapGauge);
        registry.remove(penalizedPartitionsGauge);
        for (Gauge gauge : registeredPausedGauges) {
            registry.remove(gauge);
        }
    }

    /**
     * §3.8 fatal classification, walking the cause chain (clients wrap fenced errors). Per the
     * verified 4.3.0 facts: coordinator-path epoch errors surface as
     * {@link ProducerFencedException} (fatal); produce-path {@code InvalidProducerEpochException}
     * is abortable (KIP-588) and deliberately NOT listed; the KIP-1050
     * {@code ApplicationRecoverableException} hierarchy is taxonomic, not behavioral, and is not
     * consulted.
     */
    private static boolean isFatal(RuntimeException error) {
        for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof ProducerFencedException
                    || t instanceof InvalidPidMappingException
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

    /** KIP-890 {@code TRANSACTION_ABORTABLE} anywhere in the chain: definitively not committed. */
    private static boolean isDefinitivelyNotCommitted(RuntimeException error) {
        for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof TransactionAbortableException) {
                return true;
            }
        }
        return false;
    }

    private Counter abortedTransactions(String cause) {
        return registry.counter("cesium.transactions", "loop", "dispatch", "result", "aborted", "cause", cause);
    }

    private Counter dispatchRecords(String outcome) {
        return registry.counter("cesium.dispatch.records", "outcome", outcome);
    }

    private TopicPartition trackerPartition(int partition) {
        return new TopicPartition(config.trackerTopic(), partition);
    }

    private static String causeTag(Throwable error) {
        return error.getClass().getSimpleName();
    }

    // ------------------------------------------------------------------ rebalance callbacks

    /**
     * O(1) bookkeeping only (I8/§6): callbacks run inside {@code poll()} on the loop thread — by
     * I3 no transaction can be in flight — and never make network calls, never snapshot barriers,
     * never replay. Assignment marks shards (epoch-tagged) and pauses intake until recovery
     * begins (also keeping the locked {@code auto.offset.reset=none} from tripping over an
     * unpositioned partition mid-drain); revocation drops shard state and discards pending
     * barrier futures (I8).
     */
    private final class DispatchRebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            registry.counter("cesium.dispatch.rebalances", "event", "assigned").increment();
            if (partitions.isEmpty()) {
                return; // rebalance-completion signal under cooperative protocols; nothing to do
            }
            log.info("tracker partitions assigned: {}", partitions);
            Set<Integer> ids = partitionIds(partitions);
            store.onPartitionsAssigned(ids);
            consumer.pause(partitions);
            for (int partition : ids) {
                shardEpochs.put(partition, ++epochCounter);
                pendingBarriers.remove(partition); // re-assignment re-snapshots (I8)
                streaming.remove(partition);
                needsCursor.add(partition);
            }
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            registry.counter("cesium.dispatch.rebalances", "event", "revoked").increment();
            if (partitions.isEmpty()) {
                return;
            }
            log.info("tracker partitions revoked: {}", partitions);
            Set<Integer> ids = partitionIds(partitions);
            store.onPartitionsRevoked(ids);
            dropLoopState(ids);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            registry.counter("cesium.dispatch.rebalances", "event", "lost").increment();
            log.warn("tracker partitions lost without clean revocation: {}", partitions);
            Set<Integer> ids = partitionIds(partitions);
            store.onPartitionsLost(ids);
            dropLoopState(ids);
        }

        private Set<Integer> partitionIds(Collection<TopicPartition> partitions) {
            Set<Integer> ids = new LinkedHashSet<>();
            for (TopicPartition tp : partitions) {
                ids.add(tp.partition());
            }
            return ids;
        }

        /** Drops all loop-side shard state in O(1) per partition; barrier futures are discarded. */
        private void dropLoopState(Set<Integer> ids) {
            for (int partition : ids) {
                shardEpochs.remove(partition);
                needsCursor.remove(partition);
                pendingBarriers.remove(partition);
                streaming.remove(partition);
                lastCursorActivityMs.remove(partition);
                lastCommittedCursorOffset.remove(partition);
                penaltyStreaks.remove(partition);
                if (pausedForBackpressure.remove(partition)) {
                    pausedGauge(partition).set(0);
                }
            }
        }
    }

    // ------------------------------------------------------------------ value plumbing

    /** An epoch-tagged in-flight barrier snapshot (I8): stale futures are discarded on revoke. */
    private record PendingBarrier(long epoch, TrackerCursor cursor, CompletableFuture<Long> future) {}

    /** One fetch pass's classification: parallel index sets over the drained batch. */
    private static final class Classified {
        final int[] settledIndices;
        final int[] foundIndices;
        final int[] goneIndices;
        final int[] carryIndices;
        final Set<Integer> transientPartitions = new TreeSet<>();
        int settledCount;
        int foundCount;
        int goneCount;
        int carryCount;
        int firstGoneIndex = -1;

        Classified(int capacity) {
            settledIndices = new int[capacity];
            foundIndices = new int[capacity];
            goneIndices = new int[capacity];
            carryIndices = new int[capacity];
        }
    }

    /**
     * The pending half of the I9 procedure: whether the replacement producer has been built and
     * fenced yet. Cursor re-resolution state lives in the ordinary {@code needsCursor} queue.
     */
    private static final class InDoubtRecovery {
        boolean producerReady;
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
