package events.cesium.kafka.store.tracker;

import events.cesium.kafka.api.store.CompletionReason;
import events.cesium.kafka.api.store.DueBatch;
import events.cesium.kafka.api.store.RouteDescriptor;
import events.cesium.kafka.api.store.ScheduledRef;
import events.cesium.kafka.api.store.StoreCapabilities;
import events.cesium.kafka.api.store.StoreContext;
import events.cesium.kafka.api.store.TrackerBackedStore;
import events.cesium.kafka.api.store.TrackerCursor;
import events.cesium.kafka.api.store.TrackerRecordData;
import events.cesium.kafka.store.tracker.index.IndexDueBatch;
import events.cesium.kafka.store.tracker.index.PartitionShard;
import events.cesium.kafka.store.tracker.index.TrackerIndex;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The flagship v1 scheduler store (design §5, D13): the fastutil-backed in-memory
 * {@link TrackerIndex} durably backed by the Kafka tracker topic, speaking the
 * {@link TrackerWireFormat} and committing the §3.5 cursor-v2 ({@link SidecarCodec} sidecar)
 * through the engine's transactions.
 *
 * <p><strong>Division of labor.</strong> The engine owns every Kafka client, the transactions and
 * fencing (I1–I9), the replay barrier protocol (§3.6) and dispatch timing; this store owns the
 * wire format, the replay rules R1/R2, the in-memory index, and the per-partition committed
 * cursor (offset + sidecar). The store never creates Kafka clients — its durable writes are bytes
 * the engine produces inside its own transactions (design §4.3).
 *
 * <p><strong>Threading</strong> (per the {@link events.cesium.kafka.api.store.SchedulerStore}
 * contract): {@link #encodeSchedule} is ingest-thread and pure; everything else runs on the
 * single dispatch thread owning the affected partitions, so no method here locks. All time
 * decisions arrive as {@code nowMs} arguments from the engine's clock; the store keeps no clock
 * state of its own.
 *
 * <p><strong>Recovery (I4).</strong> {@link #onPartitionsAssigned} creates shards gated
 * dispatch-<em>ineligible</em>; {@link #beginRecovery} seeds the sidecar's pinned entries first
 * (they are the oldest, in encoded order, with their original tracker ADD offsets all below the
 * cursor — preserving the arrival log's double sortedness) and records the barrier;
 * {@link #onTrackerRecord} is the single code path for replay and live tailing (§1.2). The shard
 * promotes ACTIVE when {@code position ≥ barrier}, where position advances on delivered records
 * <em>and</em> on the engine's {@link #onTrackerPosition} consumer-position reports — tracker
 * offsets are not dense (transaction control records and aborted batches occupy undelivered
 * offsets, I1), so the offsets directly below the barrier are typically a commit marker and only
 * the position report can carry the shard across them.
 */
public final class KafkaTrackerStore implements TrackerBackedStore {

    /** Sidecar byte budget for the committed cursor metadata (Base64 size; §3.5). */
    public static final String SIDECAR_MAX_BYTES_KEY = "cursor.sidecar-max-bytes";

    /** Default sidecar budget: 3 KiB ≈ 200–300 pinned entries (§8 defaults table). */
    public static final int DEFAULT_SIDECAR_MAX_BYTES = 3072;

    /** Per-partition pending cap used for the worst-case footprint check (§5.3). */
    public static final String MAX_PENDING_PER_PARTITION_KEY = "max-pending-per-partition";

    /** Default per-partition pending cap (§8 defaults table). */
    public static final long DEFAULT_MAX_PENDING_PER_PARTITION = 2_000_000L;

    /** Heap budget for the footprint check; {@code 0}/unset derives ~25% of {@code Xmx} (§5.3). */
    public static final String HEAP_BUDGET_BYTES_KEY = "heap-budget-bytes";

    /** Stale-heap-entry floor before a maintenance heap rebuild (§5.2 item 2). */
    public static final String STALE_REBUILD_FLOOR_KEY = "index.stale-rebuild-floor";

    /** Completed-held floor before a maintenance log sweep (§5.3). */
    public static final String COMPLETED_SWEEP_FLOOR_KEY = "index.completed-sweep-floor";

    /**
     * Worst-case planning bytes per pending entry (post-approval revision 1: 64 B typical with
     * fastutil doubling growth, 80 B worst; the JOL gate holds the structures to ≤ 56 B).
     */
    public static final long WORST_CASE_BYTES_PER_ENTRY = 64;

    private static final int MIN_SIDECAR_MAX_BYTES = 128;
    private static final int MAX_SIDECAR_MAX_BYTES = 1 << 20;
    private static final Set<String> KNOWN_CONFIG_KEYS = Set.of(
            SIDECAR_MAX_BYTES_KEY,
            MAX_PENDING_PER_PARTITION_KEY,
            HEAP_BUDGET_BYTES_KEY,
            STALE_REBUILD_FLOOR_KEY,
            COMPLETED_SWEEP_FLOOR_KEY);

    private static final StoreCapabilities CAPABILITIES = new StoreCapabilities(
            StoreCapabilities.TransactionAffinity.KAFKA_TRANSACTIONAL,
            StoreCapabilities.DispatchGuarantee.EXACTLY_ONCE,
            /* requiresTrackerTopic= */ true,
            /* supportsCancellation= */ false);

    private static final Logger log = LoggerFactory.getLogger(KafkaTrackerStore.class);

    /** Shared empty headers for the headers-blind legacy {@code onTrackerRecord} overload. */
    private static final RecordHeaders NO_HEADERS = new RecordHeaders();

    private @Nullable StoreContext context;
    private @Nullable Wiring wiring;

    @Override
    public void configure(StoreContext context) {
        if (this.context != null) {
            throw new IllegalStateException("configure() called twice");
        }
        this.context = Objects.requireNonNull(context, "context");
    }

    @Override
    public StoreCapabilities capabilities() {
        return CAPABILITIES;
    }

    /**
     * Store-declared preconditions, from configuration alone (design §4.4 item 7): unknown
     * {@code store.properties} keys, sidecar-budget sanity (bounds, and the identity header must
     * fit the Base64 budget), index tuning floors, and the worst-case index footprint —
     * {@code partitionCount × max-pending-per-partition × }{@value #WORST_CASE_BYTES_PER_ENTRY}
     * {@code B} — against the heap budget (§5.3).
     *
     * <p><strong>Validation split (deliberate).</strong> Broker-side checks are the
     * <em>engine's</em> {@code StartupValidator} job, performed with the engine's admin client —
     * this store never creates one: source/tracker partition parity (§2.1), the tombstone
     * retention floor (D14), {@code offset.metadata.max.bytes} vs the sidecar budget (§3.5 risk
     * 6), topic-id liveness/identity (R11/R17), and compaction settings. The store validates only
     * what configuration alone can prove.
     */
    @Override
    public void validate() {
        StoreContext ctx = requireContext();
        var config = ctx.config();
        Set<String> unknown = new TreeSet<>(config.keys());
        unknown.removeAll(KNOWN_CONFIG_KEYS);
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException("unknown store.properties keys for kafka-tracker: " + unknown
                    + " (supported: " + new TreeSet<>(KNOWN_CONFIG_KEYS) + ")");
        }
        int sidecarMaxBytes = config.getInt(SIDECAR_MAX_BYTES_KEY, DEFAULT_SIDECAR_MAX_BYTES);
        long maxPendingPerPartition = config.getLong(MAX_PENDING_PER_PARTITION_KEY, DEFAULT_MAX_PENDING_PER_PARTITION);
        long heapBudgetBytes = config.getLong(HEAP_BUDGET_BYTES_KEY, 0L);
        int staleRebuildFloor = config.getInt(STALE_REBUILD_FLOOR_KEY, PartitionShard.DEFAULT_STALE_REBUILD_FLOOR);
        int completedSweepFloor =
                config.getInt(COMPLETED_SWEEP_FLOOR_KEY, PartitionShard.DEFAULT_COMPLETED_SWEEP_FLOOR);

        RouteDescriptor route = ctx.route();
        Uuid trackerTopicId = route.trackerTopicId();
        if (trackerTopicId == null) {
            throw new IllegalArgumentException("kafka-tracker requires the tracker topic, but the route carries no"
                    + " tracker topic id (requiresTrackerTopic=true; the engine bootstraps the topic before"
                    + " configuring the store)");
        }
        if (sidecarMaxBytes < MIN_SIDECAR_MAX_BYTES || sidecarMaxBytes > MAX_SIDECAR_MAX_BYTES) {
            throw new IllegalArgumentException(SIDECAR_MAX_BYTES_KEY + " must be in [" + MIN_SIDECAR_MAX_BYTES + ", "
                    + MAX_SIDECAR_MAX_BYTES + "], got " + sidecarMaxBytes);
        }
        // The identity header must fit the budget; the encoder enforces exactly the encode-time rule.
        SidecarCodec.encoder(route.clusterId(), route.sourceTopicId(), trackerTopicId, sidecarMaxBytes);
        if (maxPendingPerPartition <= 0) {
            throw new IllegalArgumentException(
                    MAX_PENDING_PER_PARTITION_KEY + " must be positive, got " + maxPendingPerPartition);
        }
        if (heapBudgetBytes < 0) {
            throw new IllegalArgumentException(HEAP_BUDGET_BYTES_KEY + " must be >= 0, got " + heapBudgetBytes);
        }
        if (staleRebuildFloor <= 0 || completedSweepFloor <= 0) {
            throw new IllegalArgumentException("index maintenance floors must be positive: " + STALE_REBUILD_FLOOR_KEY
                    + "=" + staleRebuildFloor + ", " + COMPLETED_SWEEP_FLOOR_KEY + "=" + completedSweepFloor);
        }

        long effectiveHeapBudget =
                heapBudgetBytes > 0 ? heapBudgetBytes : Runtime.getRuntime().maxMemory() / 4;
        long worstCaseBytes;
        try {
            worstCaseBytes = Math.multiplyExact(
                    Math.multiplyExact((long) route.partitionCount(), maxPendingPerPartition),
                    WORST_CASE_BYTES_PER_ENTRY);
        } catch (ArithmeticException e) {
            worstCaseBytes = Long.MAX_VALUE;
        }
        if (worstCaseBytes > effectiveHeapBudget) {
            throw new IllegalArgumentException("worst-case index footprint exceeds the heap budget: "
                    + route.partitionCount() + " partitions x " + maxPendingPerPartition + " entries x "
                    + WORST_CASE_BYTES_PER_ENTRY + " B = " + worstCaseBytes + " B > " + effectiveHeapBudget
                    + " B (" + (heapBudgetBytes > 0 ? HEAP_BUDGET_BYTES_KEY : "auto: max heap / 4")
                    + "); lower " + MAX_PENDING_PER_PARTITION_KEY + " or raise the heap (design §5.3, §4.4 item 7)");
        }
        log.info(
                "kafka-tracker validated: worst-case index footprint {} partitions x {} entries x {} B = {} B"
                        + " within heap budget {} B; sidecar budget {} B",
                route.partitionCount(),
                maxPendingPerPartition,
                WORST_CASE_BYTES_PER_ENTRY,
                worstCaseBytes,
                effectiveHeapBudget,
                sidecarMaxBytes);

        this.wiring = new Wiring(
                route,
                trackerTopicId,
                ctx.meterRegistry(),
                sidecarMaxBytes,
                new TrackerIndex(staleRebuildFloor, completedSweepFloor));
    }

    @Override
    public void start() {
        wiring(); // lifecycle order: validate() must have succeeded
    }

    // ------------------------------------------------------------------------------------------
    // Ingest side (ingest thread; pure — no shared state with the dispatch-side index, §5.1)
    // ------------------------------------------------------------------------------------------

    @Override
    public TrackerRecordData encodeSchedule(ScheduledRef ref) {
        return TrackerWireFormat.encodeAdd(ref.sourceOffset(), ref.dispatchAtMs(), ref.clamped());
    }

    /**
     * {@inheritDoc}
     *
     * <p>Pure function of the batch view: handles any sub-batch the engine's per-reason grouping
     * produces (one call per {@link CompletionReason} group; the engine obligation per the
     * {@link TrackerBackedStore#encodeCompletions} contract).
     */
    @Override
    public List<TrackerRecordData> encodeCompletions(DueBatch dispatched, CompletionReason reason) {
        List<TrackerRecordData> completions = new ArrayList<>(dispatched.size());
        for (int i = 0; i < dispatched.size(); i++) {
            completions.add(TrackerWireFormat.encodeCompletion(dispatched.sourceOffset(i), reason));
        }
        return completions;
    }

    // ------------------------------------------------------------------------------------------
    // Partition lifecycle (dispatch thread)
    // ------------------------------------------------------------------------------------------

    @Override
    public void onPartitionsAssigned(Set<Integer> partitions) {
        Wiring w = wiring();
        for (int partition : partitions) {
            if (w.states.containsKey(partition)) {
                continue; // idempotent / incremental re-delivery (KIP-848)
            }
            w.index.assignPartition(partition);
            w.index.setDispatchEligible(partition, false); // I4: ASSIGNED shards are gated until promoted
            PartitionState state = new PartitionState();
            registerGauges(w, partition, state);
            w.states.put(partition, state);
        }
    }

    @Override
    public void onPartitionsRevoked(Set<Integer> partitions) {
        dropPartitions(partitions);
    }

    @Override
    public void onPartitionsLost(Set<Integer> partitions) {
        dropPartitions(partitions); // same as revoked: drop state, no flush (a new owner may be live)
    }

    private void dropPartitions(Set<Integer> partitions) {
        Wiring w = wiring();
        for (int partition : partitions) {
            PartitionState state = w.states.remove(partition);
            if (state == null) {
                continue;
            }
            // Final monotonic snapshot before the shard's plain counters drop with it.
            drainShardCounters(w, partition, state);
            w.index.revokePartition(partition); // O(1) drop; pending entries are durable in the tracker
            removeGauges(w, state);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Recovery (dispatch thread; engine drives the §3.6 protocol, the store executes)
    // ------------------------------------------------------------------------------------------

    @Override
    public void beginRecovery(int partition, TrackerCursor committed, long barrierOffset) {
        Wiring w = wiring();
        PartitionState state = requireState(w, partition);
        if (state.recoveryBegun || w.index.pendingCount(partition) > 0 || w.index.inFlightCount(partition) > 0) {
            // Idempotent re-recovery (D-6, §4.4 item 6): reset the shard so re-seeding from the
            // same cursor converges to exactly the same pending set.
            drainShardCounters(w, partition, state);
            w.index.revokePartition(partition);
            w.index.assignPartition(partition);
            w.index.setDispatchEligible(partition, false);
            // The fresh shard's plain counters restart at zero; the old totals were just pushed
            // into the monotonic counters above, so the baselines must restart too — stale
            // baselines would swallow the new shard's first N events (exactly the post-incident
            // moment the R1/R16 anomaly canary matters most).
            state.lastAnomalies = 0;
            state.lastHeapRebuilds = 0;
            state.lastLogSweeps = 0;
        }

        String metadata = committed.metadata();
        if (!metadata.isEmpty()) {
            // Corrupt or unknown-version sidecars throw — the engine fail-fasts (§3.6); a
            // corrupted committed cursor is never silently ignored.
            SidecarCodec.DecodedSidecar decoded = SidecarCodec.decode(metadata);
            validateSidecarIdentity(w, partition, decoded);
            // Seed FIRST: the pinned entries are the oldest, in encoded (tracker-ADD-offset)
            // order with their ORIGINAL offsets all below the cursor — appending them before any
            // replayed record preserves the arrival log's double sortedness (§5.2).
            for (SidecarCodec.SidecarEntry entry : decoded.entries()) {
                if (entry.trackerAddOffset() >= committed.offset()) {
                    throw new IllegalStateException("corrupt cursor sidecar for tracker partition " + partition
                            + ": seeded entry at tracker offset " + entry.trackerAddOffset()
                            + " is not below the cursor offset " + committed.offset()
                            + " (I5); refusing to recover from a corrupted cursor (§3.6)");
                }
                w.index.applyAdd(
                        partition,
                        entry.sourceOffset(),
                        entry.dispatchAtMs(),
                        entry.trackerAddOffset(),
                        entry.clamped());
            }
        }

        state.recoveryBegun = true;
        state.active = false;
        state.barrierOffset = barrierOffset;
        state.position = committed.offset();
        state.lastCommittedOffset = committed.offset();
        state.lastCommittedMetadata = metadata;
        state.hasProposal = false;
        maybePromote(w, partition, state); // barrier <= cursor: nothing to replay, immediately ACTIVE
    }

    /**
     * {@inheritDoc}
     *
     * <p>The headers-blind legacy overload: tombstone headers are unavailable, so the R12
     * forged-tombstone posture (a reason-less tombstone is counted + skipped) cannot be enforced
     * here — every structurally valid tombstone applies the completion. Engines must call the
     * {@linkplain #onTrackerRecord(int, long, byte[], byte[], Headers) headers-carrying variant},
     * where the full §2.2 posture is enforced; this overload exists so headers-blind callers
     * (older harnesses, fixtures) fail safe toward applying completions — skipping a legitimate
     * completion would be a duplicate-dispatch vector, the worse failure mode.
     */
    @Override
    public void onTrackerRecord(int partition, long trackerOffset, byte[] key, byte @Nullable [] value) {
        if (value == null) {
            Wiring w = wiring();
            PartitionState state = requireState(w, partition);
            if (key.length != TrackerWireFormat.KEY_LENGTH) {
                recordInvalid(
                        w,
                        partition,
                        trackerOffset,
                        "tombstone key length " + key.length + " (expected " + TrackerWireFormat.KEY_LENGTH + ")");
            } else {
                w.index.applyComplete(partition, TrackerWireFormat.decodeKey(key)); // R2: absent = silent no-op
            }
            advancePosition(w, partition, state, trackerOffset);
            return;
        }
        onTrackerRecord(partition, trackerOffset, key, value, NO_HEADERS);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Single code path for replay and live tailing (§1.2), enforcing the full §2.2 wire-format
     * posture via {@link TrackerWireFormat#decode}: a tombstone whose completion-reason header is
     * missing or null-valued matches the R12 forged-tombstone shape and is counted + skipped
     * (never applied — the forged tombstone is the data-loss primitive); a tombstone with a
     * <em>present but unrecognized</em> reason still applies the completion (the M4 decision —
     * skipping it would convert a newer writer's reason constant into a duplicate dispatch) and
     * is surfaced under a dedicated novelty counter.
     */
    @Override
    public void onTrackerRecord(
            int partition, long trackerOffset, byte[] key, byte @Nullable [] value, Headers headers) {
        Wiring w = wiring();
        PartitionState state = requireState(w, partition);
        switch (TrackerWireFormat.decode(key, value, headers)) {
            case TrackerDecodeResult.Add(long dispatchAtMs, boolean clamped) ->
                // R1 (duplicate-ADD anomaly handling) lives inside the index.
                w.index.applyAdd(partition, TrackerWireFormat.decodeKey(key), dispatchAtMs, trackerOffset, clamped);
            case TrackerDecodeResult.Complete ignored ->
                w.index.applyComplete(partition, TrackerWireFormat.decodeKey(key)); // R2: absent = silent no-op
            case TrackerDecodeResult.CompleteUnknownReason(String rawReason) -> {
                // The completion must still apply (M4 decision); the novelty is surfaced.
                w.unknownReasonRecords.increment();
                log.warn(
                        "tracker tombstone with unrecognized completion reason '{}' applied:"
                                + " partition {} offset {} (a newer writer's reason constant?)",
                        rawReason,
                        partition,
                        trackerOffset);
                w.index.applyComplete(partition, TrackerWireFormat.decodeKey(key));
            }
            case TrackerDecodeResult.Cancel ignored -> {
                // Reserved type 0x02 (D15): no v1 cancellation support — counted no-op, not
                // corruption (supportsCancellation=false).
                w.cancelRecords.increment();
            }
            case TrackerDecodeResult.Invalid(String detail) -> recordInvalid(w, partition, trackerOffset, detail);
        }
        advancePosition(w, partition, state, trackerOffset);
    }

    /**
     * {@inheritDoc}
     *
     * <p>The reported position is the recovery-promotion carrier across undelivered offsets
     * (transaction control records, aborted batches — I1 makes them universal at the log tail):
     * the record-derived position alone stalls strictly below the barrier behind the previous
     * transaction's commit marker. It also keeps the all-encoded committed cursor's
     * position-tracking offset exact on idle partitions.
     */
    @Override
    public void onTrackerPosition(int partition, long position) {
        Wiring w = wiring();
        PartitionState state = requireState(w, partition);
        if (position > state.position) {
            state.position = position;
            maybePromote(w, partition, state);
        }
    }

    @Override
    public boolean isRecovering(int partition) {
        Wiring w = wiring();
        PartitionState state = w.states.get(partition);
        return state != null && !state.active;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Computed "as if" <em>exactly</em> the entries of the given {@code inFlight} batch were
     * complete — sound because precisely their tombstones commit atomically with this cursor.
     * The index's in-flight state is deliberately NOT trusted as the as-if-complete set: under
     * truncate-and-carry-over (§3.2/§7-2c, D8) and TRANSIENT exclusion (D-8) the transaction
     * settles a strict subset of the drained batch, and any drained-but-unsettled entry is still
     * pending durable truth — so the visitation walks pending <em>and</em> in-flight entries in
     * tracker-ADD-offset order and skips only the given batch's entries (resolved by
     * {@code (partition, sourceOffset)}, valid for the store's own batch and engine sub-views
     * alike); every other entry is offered to the sidecar encoder or bounds the overflow cut.
     * On abort the entries are restored and {@link #onBatchAborted} discards the proposal, so
     * the monotonic floor only ever advances on a definitive commit.
     *
     * <p><strong>I5 guard.</strong> The greedy encode makes I5 hold by construction (every
     * unsettled entry is either encoded or has {@code trackerAddOffset ≥} the overflow cut; in
     * the all-encoded case every unsettled offset is below the live position). The residual
     * failure modes — a computed offset below the monotonic floor, or an out-of-order visitation
     * — are engine/store bugs and degrade safely: ERROR log + {@code cesium_cursor_guard_violations}
     * + the last definitively committed cursor is returned, never a corrupted one.
     */
    @Override
    public TrackerCursor committedCursor(int partition, DueBatch inFlight) {
        Wiring w = wiring();
        PartitionState state = requireState(w, partition);
        if (!state.recoveryBegun) {
            throw new IllegalStateException("committedCursor for tracker partition " + partition
                    + " before beginRecovery: the cursor position is undefined");
        }
        try {
            SidecarCodec.Encoder encoder = SidecarCodec.encoder(
                    w.route.clusterId(), w.route.sourceTopicId(), w.trackerTopicId, w.sidecarMaxBytes);
            PartitionShard shard = w.index.shard(partition);
            LongOpenHashSet settling = settlingOffsets(inFlight, partition);
            w.index.oldestUnsettled(partition, (slotId, sourceOffset, dispatchAtMs, trackerAddOffset) -> {
                if (settling != null && settling.contains(sourceOffset)) {
                    return true; // in the committing batch: its tombstone commits with this cursor
                }
                return encoder.offer(trackerAddOffset, sourceOffset, dispatchAtMs, shard.clamped(slotId));
            });
            SidecarCodec.EncodedCursor encoded = encoder.finish(state.position);
            if (encoded.cursorOffset() < state.lastCommittedOffset) {
                return degradeToLastSafeCursor(
                        w,
                        partition,
                        state,
                        "computed cursor offset " + encoded.cursorOffset() + " below the monotonic floor "
                                + state.lastCommittedOffset);
            }
            state.pinnedEntries = encoded.encodedCount();
            state.sidecarBytes = encoded.encodedBytes();
            state.proposedOffset = encoded.cursorOffset();
            state.proposedMetadata = encoded.metadata();
            state.hasProposal = true;
            return new TrackerCursor(encoded.cursorOffset(), encoded.metadata());
        } catch (IllegalStateException e) {
            return degradeToLastSafeCursor(w, partition, state, e.getMessage());
        }
    }

    // ------------------------------------------------------------------------------------------
    // Hot path (dispatch thread)
    // ------------------------------------------------------------------------------------------

    @Override
    public DueBatch pollDue(long nowMs, int maxBatch) {
        // Per-partition recovery gating (I4) is the index's eligibility gate — no global gate,
        // so ACTIVE shards keep dispatching while others recover (§3.6).
        return wiring().index.drainDue(nowMs, maxBatch);
    }

    @Override
    public long nextDeadlineMs() {
        return wiring().index.nextDeadlineMs();
    }

    @Override
    public long pendingCount(int partition) {
        return wiring().index.pendingCount(partition);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Deliberately overridden (never the no-op default): the penalty box is the §7/D22 fetch
     * isolation — inheriting the default would re-poll penalized entries immediately and
     * hot-spin degraded source partitions.
     */
    @Override
    public void penalizeSourcePartition(int sourcePartition, long notBeforeMs) {
        wiring().index.penalizeSourcePartition(sourcePartition, notBeforeMs);
    }

    /**
     * {@inheritDoc}
     *
     * <p>The store's own {@code pollDue} batch resolves by slot id (with a drain-generation check
     * against post-I9-drop staleness); engine-synthesized sub-batch views (truncate-and-carry-over,
     * §3.2/§7) resolve by {@code (partition, sourceOffset)} ring binary search of in-flight slots.
     *
     * <p>Also promotes the cursors proposed by {@link #committedCursor} since the last
     * resolution to the monotonic floor: the engine runs one transaction at a time (I3), so
     * every outstanding proposal belongs to the transaction that just committed — including the
     * records-free idle-advancement transaction, whose batch is empty.
     *
     * <p><strong>I9 (engine obligation, restated):</strong> this method is called only after a
     * <em>definitive</em> commit. After an in-doubt commit the engine calls neither this nor
     * {@link #onBatchAborted} — it drops the affected partitions and re-enters recovery; the
     * durable log is authoritative.
     */
    @Override
    public void onBatchCommitted(DueBatch batch) {
        Wiring w = wiring();
        if (batch instanceof IndexDueBatch indexBatch) {
            w.index.onBatchCommitted(indexBatch);
        } else if (batch.size() > 0) {
            w.index.onForeignBatchCommitted(batch);
        }
        for (Int2ObjectMap.Entry<PartitionState> entry : w.states.int2ObjectEntrySet()) {
            PartitionState state = entry.getValue();
            if (state.hasProposal) {
                state.lastCommittedOffset = state.proposedOffset;
                state.lastCommittedMetadata = state.proposedMetadata;
                state.hasProposal = false;
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Restores the batch to pending — the definitive-abort path and the truncate-and-carry-over
     * restore path alike (the carry-over may arrive as an engine-synthesized sub-batch view,
     * resolved by {@code (partition, sourceOffset)} like {@link #onBatchCommitted}) — and discards
     * every cursor proposed by {@link #committedCursor} for the aborted transaction: the proposals
     * never became durable, so promoting them would corrupt the monotonic floor.
     *
     * <p><strong>I9 (engine obligation, restated):</strong> called only after a
     * <em>definitive</em> abort. Restoring after an ambiguous commit outcome is the duplicate
     * vector — the engine drops the shards and re-recovers instead (§3.8).
     */
    @Override
    public void onBatchAborted(DueBatch batch) {
        Wiring w = wiring();
        if (batch instanceof IndexDueBatch indexBatch) {
            w.index.onBatchAborted(indexBatch);
        } else if (batch.size() > 0) {
            w.index.onForeignBatchAborted(batch);
        }
        for (Int2ObjectMap.Entry<PartitionState> entry : w.states.int2ObjectEntrySet()) {
            entry.getValue().hasProposal = false;
        }
    }

    @Override
    public void maintenance() {
        Wiring w = wiring();
        w.index.maintenance();
        // Monotonic counter wiring by per-partition snapshot/delta: the shards' plain counters
        // drop with revoked shards, so deltas are drained here (and once more at drop time).
        for (Int2ObjectMap.Entry<PartitionState> entry : w.states.int2ObjectEntrySet()) {
            drainShardCounters(w, entry.getIntKey(), entry.getValue());
        }
    }

    @Override
    public void close() {
        Wiring w = wiring;
        if (w == null) {
            return; // never validated — nothing to release
        }
        for (Int2ObjectMap.Entry<PartitionState> entry : w.states.int2ObjectEntrySet()) {
            // Final monotonic snapshot: a scrape racing close() must not lose the last deltas.
            if (w.index.isAssigned(entry.getIntKey())) {
                drainShardCounters(w, entry.getIntKey(), entry.getValue());
            }
            removeGauges(w, entry.getValue());
        }
        w.states.clear();
    }

    // ------------------------------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------------------------------

    private void advancePosition(Wiring w, int partition, PartitionState state, long trackerOffset) {
        // Position tracks the consumer's applied position; invalid records were still consumed.
        // Deliberately NOT max(): an engine misfeed below the live position must surface through
        // the cursor monotonic guard rather than be silently absorbed (the position-report path,
        // onTrackerPosition, is the high-water-mark one).
        state.position = trackerOffset + 1;
        maybePromote(w, partition, state);
    }

    private void maybePromote(Wiring w, int partition, PartitionState state) {
        if (!state.active && state.recoveryBegun && state.position >= state.barrierOffset) {
            state.active = true;
            w.index.setDispatchEligible(partition, true); // I4 gate lifts: RECOVERING -> ACTIVE
            log.info(
                    "tracker partition {} recovered: position {} reached barrier {}",
                    partition,
                    state.position,
                    state.barrierOffset);
        }
    }

    private void validateSidecarIdentity(Wiring w, int partition, SidecarCodec.DecodedSidecar decoded) {
        RouteDescriptor route = w.route;
        if (!decoded.clusterId().equals(route.clusterId())
                || !decoded.sourceTopicId().equals(route.sourceTopicId())
                || !decoded.trackerTopicId().equals(w.trackerTopicId)) {
            throw new IllegalStateException("cursor sidecar identity mismatch for tracker partition " + partition
                    + ": committed {clusterId='" + decoded.clusterId() + "', sourceTopicId=" + decoded.sourceTopicId()
                    + ", trackerTopicId=" + decoded.trackerTopicId() + "} vs live {clusterId='" + route.clusterId()
                    + "', sourceTopicId=" + route.sourceTopicId() + ", trackerTopicId=" + w.trackerTopicId
                    + "} — a topic was recreated or the cursor belongs to another cluster; failing fast"
                    + " (§3.6, R-9/R-10 runbook: never deliver payloads against a different identity)");
        }
    }

    private TrackerCursor degradeToLastSafeCursor(Wiring w, int partition, PartitionState state, @Nullable String why) {
        // An I5/monotonicity violation is a bug surfacing as metric + log — never a corrupted
        // commit (design §3.5): return the last definitively committed cursor unchanged.
        log.error(
                "cursor guard violation for tracker partition {}: {} — returning the last safe cursor" + " (offset {})",
                partition,
                why,
                state.lastCommittedOffset);
        w.cursorGuardViolations.increment();
        state.hasProposal = false;
        return new TrackerCursor(state.lastCommittedOffset, state.lastCommittedMetadata);
    }

    private void recordInvalid(Wiring w, int partition, long trackerOffset, String detail) {
        // Count-and-skip, never apply, never crash (design §2.2); also the R12 foreign-writer canary.
        w.invalidRecords.increment();
        log.warn("invalid tracker record skipped: partition {} offset {}: {}", partition, trackerOffset, detail);
    }

    private void drainShardCounters(Wiring w, int partition, PartitionState state) {
        PartitionShard shard = w.index.shard(partition);
        long anomalies = shard.anomalies();
        if (anomalies > state.lastAnomalies) {
            w.indexAnomalies.increment((double) (anomalies - state.lastAnomalies));
            state.lastAnomalies = anomalies;
        }
        long rebuilds = shard.heapRebuilds();
        if (rebuilds > state.lastHeapRebuilds) {
            w.heapRebuilds.increment((double) (rebuilds - state.lastHeapRebuilds));
            state.lastHeapRebuilds = rebuilds;
        }
        long sweeps = shard.logSweeps();
        if (sweeps > state.lastLogSweeps) {
            w.logSweeps.increment((double) (sweeps - state.lastLogSweeps));
            state.lastLogSweeps = sweeps;
        }
    }

    private void registerGauges(Wiring w, int partition, PartitionState state) {
        String tag = Integer.toString(partition);
        // M4 lesson: per-partition gauges MUST be deregistered on revoke or they bind dead state.
        state.pendingGauge = Gauge.builder("cesium.pending.entries", () -> w.index.pendingCount(partition))
                .description("live index size for the partition (design §9)")
                .tag("partition", tag)
                .register(w.registry);
        state.pinnedGauge = Gauge.builder("cesium.pinned.entries", () -> statePinned(w, partition))
                .description("sidecar occupancy of the last computed cursor; sustained at max = overflow mode (§3.5)")
                .tag("partition", tag)
                .register(w.registry);
        state.sidecarBytesGauge = Gauge.builder("cesium.cursor.sidecar.bytes", () -> stateSidecarBytes(w, partition))
                .description("encoded sidecar size of the last computed cursor vs the budget")
                .tag("partition", tag)
                .register(w.registry);
    }

    private static long statePinned(Wiring w, int partition) {
        PartitionState state = w.states.get(partition);
        return state == null ? 0 : state.pinnedEntries;
    }

    private static long stateSidecarBytes(Wiring w, int partition) {
        PartitionState state = w.states.get(partition);
        return state == null ? 0 : state.sidecarBytes;
    }

    private static void removeGauges(Wiring w, PartitionState state) {
        if (state.pendingGauge != null) {
            w.registry.remove(state.pendingGauge);
        }
        if (state.pinnedGauge != null) {
            w.registry.remove(state.pinnedGauge);
        }
        if (state.sidecarBytesGauge != null) {
            w.registry.remove(state.sidecarBytesGauge);
        }
    }

    /**
     * The source offsets of {@code inFlight} entries belonging to {@code partition} — the
     * as-if-complete set of one cursor computation. Source offsets are unique per partition
     * (§2.2), so identity-by-offset is exact for the store's own batch and engine sub-views
     * alike. {@code null} when the batch touches no entry of the partition (the idle-advancement
     * shape), so the hot path allocates nothing.
     */
    private static @Nullable LongOpenHashSet settlingOffsets(DueBatch inFlight, int partition) {
        LongOpenHashSet settling = null;
        for (int i = 0; i < inFlight.size(); i++) {
            if (inFlight.sourcePartition(i) == partition) {
                if (settling == null) {
                    settling = new LongOpenHashSet(Math.max(16, inFlight.size()));
                }
                settling.add(inFlight.sourceOffset(i));
            }
        }
        return settling;
    }

    private static PartitionState requireState(Wiring w, int partition) {
        PartitionState state = w.states.get(partition);
        if (state == null) {
            throw new IllegalStateException("tracker partition " + partition + " is not assigned");
        }
        return state;
    }

    private StoreContext requireContext() {
        StoreContext ctx = context;
        if (ctx == null) {
            throw new IllegalStateException("configure() has not been called");
        }
        return ctx;
    }

    private Wiring wiring() {
        Wiring w = wiring;
        if (w == null) {
            throw new IllegalStateException("validate() must succeed before the store is used");
        }
        return w;
    }

    /** Everything built by {@link #validate()}: settings, the index, metrics, partition states. */
    private static final class Wiring {
        final RouteDescriptor route;
        final Uuid trackerTopicId;
        final MeterRegistry registry;
        final int sidecarMaxBytes;
        final TrackerIndex index;
        final Int2ObjectOpenHashMap<PartitionState> states = new Int2ObjectOpenHashMap<>();
        final Counter invalidRecords;
        final Counter cancelRecords;
        final Counter unknownReasonRecords;
        final Counter cursorGuardViolations;
        final Counter indexAnomalies;
        final Counter heapRebuilds;
        final Counter logSweeps;

        Wiring(
                RouteDescriptor route,
                Uuid trackerTopicId,
                MeterRegistry registry,
                int sidecarMaxBytes,
                TrackerIndex index) {
            this.route = route;
            this.trackerTopicId = trackerTopicId;
            this.registry = registry;
            this.sidecarMaxBytes = sidecarMaxBytes;
            this.index = index;
            this.invalidRecords = Counter.builder("cesium.tracker.invalid.records")
                    .description("tracker wire-format violations, counted and skipped (design §2.2, R12 canary)")
                    .register(registry);
            this.cancelRecords = Counter.builder("cesium.tracker.cancel.records")
                    .description("reserved CANCEL records seen; unsupported in v1 — counted no-ops (D15)")
                    .register(registry);
            this.unknownReasonRecords = Counter.builder("cesium.tracker.unknown.reason.records")
                    .description("tombstones with an unrecognized completion reason — applied (the M4 decision:"
                            + " skipping would be a duplicate-dispatch vector) but surfaced as novelty (D15)")
                    .register(registry);
            this.cursorGuardViolations = Counter.builder("cesium.cursor.guard.violations")
                    .description("I5/monotonic cursor guard trips; the last safe cursor was returned (design §3.5)")
                    .register(registry);
            this.indexAnomalies = Counter.builder("cesium.store.index.anomalies")
                    .description("monotonic index anomalies: duplicate ADDs (R1), out-of-order drops, impossible"
                            + " completes — wired by snapshot/delta so revocations never lose counts")
                    .register(registry);
            this.heapRebuilds = Counter.builder("cesium.store.heap.rebuilds")
                    .description("monotonic dispatch-heap rebuilds across all shards")
                    .register(registry);
            this.logSweeps = Counter.builder("cesium.store.log.sweeps")
                    .description("monotonic arrival-log sweeps across all shards")
                    .register(registry);
        }
    }

    /** Per-partition recovery, cursor and metrics bookkeeping (dispatch thread only). */
    private static final class PartitionState {
        boolean recoveryBegun;
        boolean active;
        long barrierOffset;
        long position;
        long lastCommittedOffset;
        String lastCommittedMetadata = "";
        boolean hasProposal;
        long proposedOffset;
        String proposedMetadata = "";
        long pinnedEntries;
        long sidecarBytes;
        long lastAnomalies;
        long lastHeapRebuilds;
        long lastLogSweeps;

        @Nullable Gauge pendingGauge;

        @Nullable Gauge pinnedGauge;

        @Nullable Gauge sidecarBytesGauge;
    }
}
