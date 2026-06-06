package events.cesium.kafka.core.dispatch;

import events.cesium.kafka.api.store.CompletionReason;
import events.cesium.kafka.api.store.DueBatch;
import events.cesium.kafka.api.store.ScheduledRef;
import events.cesium.kafka.api.store.SchedulerStore;
import events.cesium.kafka.api.store.StoreCapabilities;
import events.cesium.kafka.api.store.StoreContext;
import events.cesium.kafka.api.store.TrackerBackedStore;
import events.cesium.kafka.api.store.TrackerCursor;
import events.cesium.kafka.api.store.TrackerRecordData;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jspecify.annotations.Nullable;

/**
 * Dispatch-side scripted stand-in for the tracker store: {@code pollDue} pops scripted batches,
 * every SPI call is recorded (into typed lists and a shared ordering-sensitive event log), and
 * recovery promotion is simulated the way a conforming store behaves — {@code beginRecovery}
 * gates the shard while {@code barrier > cursor} and {@code onTrackerPosition} reaching the
 * barrier promotes it. Ingest-side and lifecycle surface throws: the dispatch loop must never
 * touch it.
 */
final class ScriptedTrackerStore implements TrackerBackedStore {

    /** One recorded {@code beginRecovery} call. */
    record RecoveryCall(int partition, long cursorOffset, String metadata, long barrier) {}

    /** One recorded {@code committedCursor} call with a value snapshot of the in-flight view. */
    record CursorCall(int partition, List<long[]> entries) {}

    /** One recorded {@code encodeCompletions} call with a value snapshot of the view. */
    record EncodeCall(CompletionReason reason, List<long[]> entries) {}

    private final List<String> events;

    final ArrayDeque<DueBatch> dueQueue = new ArrayDeque<>();
    final List<RecoveryCall> recoveries = new ArrayList<>();
    final List<CursorCall> cursorCalls = new ArrayList<>();
    final List<EncodeCall> encodeCalls = new ArrayList<>();
    final List<List<long[]>> committedBatches = new ArrayList<>();
    final List<List<long[]>> abortedBatches = new ArrayList<>();
    final List<long[]> penalties = new ArrayList<>();
    final List<long[]> positions = new ArrayList<>();
    final List<long[]> appliedRecords = new ArrayList<>();
    final Set<Integer> assigned = new TreeSet<>();
    final Map<Integer, Long> pendingCounts = new HashMap<>();
    final Map<Integer, Long> cursorOffsets = new HashMap<>();
    final Map<Integer, Long> barriers = new HashMap<>();
    final Set<Integer> recovering = new HashSet<>();
    final Set<Integer> forceRecovering = new HashSet<>();
    long nextDeadlineMs = Long.MAX_VALUE;

    ScriptedTrackerStore(List<String> events) {
        this.events = events;
    }

    /** The cursor offset {@code committedCursor} returns for {@code partition} from here on. */
    void cursorAt(int partition, long offset) {
        cursorOffsets.put(partition, offset);
    }

    @Override
    public DueBatch pollDue(long nowMs, int maxBatch) {
        DueBatch next = dueQueue.poll();
        return next == null ? TestBatch.builder().build() : next;
    }

    @Override
    public long nextDeadlineMs() {
        return nextDeadlineMs;
    }

    @Override
    public long pendingCount(int partition) {
        return pendingCounts.getOrDefault(partition, 0L);
    }

    @Override
    public void penalizeSourcePartition(int sourcePartition, long notBeforeMs) {
        penalties.add(new long[] {sourcePartition, notBeforeMs});
        events.add("penalize:" + sourcePartition);
    }

    @Override
    public void onBatchCommitted(DueBatch batch) {
        committedBatches.add(snapshot(batch));
        events.add("batchCommitted:" + batch.size());
    }

    @Override
    public void onBatchAborted(DueBatch batch) {
        abortedBatches.add(snapshot(batch));
        events.add("batchAborted:" + batch.size());
    }

    @Override
    public void onPartitionsAssigned(Set<Integer> partitions) {
        assigned.addAll(partitions);
        events.add("assigned:" + new TreeSet<>(partitions));
    }

    @Override
    public void onPartitionsRevoked(Set<Integer> partitions) {
        dropAll(partitions);
        events.add("revoked:" + new TreeSet<>(partitions));
    }

    @Override
    public void onPartitionsLost(Set<Integer> partitions) {
        dropAll(partitions);
        events.add("lost:" + new TreeSet<>(partitions));
    }

    private void dropAll(Set<Integer> partitions) {
        assigned.removeAll(partitions);
        recovering.removeAll(partitions);
        for (int partition : partitions) {
            barriers.remove(partition);
        }
    }

    @Override
    public void beginRecovery(int partition, TrackerCursor committed, long barrierOffset) {
        recoveries.add(new RecoveryCall(partition, committed.offset(), committed.metadata(), barrierOffset));
        barriers.put(partition, barrierOffset);
        if (barrierOffset > committed.offset()) {
            recovering.add(partition);
        } else {
            recovering.remove(partition);
        }
        events.add("beginRecovery:" + partition);
    }

    @Override
    public void onTrackerRecord(int partition, long trackerOffset, byte[] key, byte @Nullable [] value) {
        appliedRecords.add(new long[] {partition, trackerOffset});
    }

    @Override
    public void onTrackerRecord(
            int partition, long trackerOffset, byte[] key, byte @Nullable [] value, Headers headers) {
        // The engine must call this headers-carrying variant; record through one path.
        onTrackerRecord(partition, trackerOffset, key, value);
        events.add("record:" + partition + ":" + trackerOffset);
    }

    @Override
    public void onTrackerPosition(int partition, long position) {
        positions.add(new long[] {partition, position});
        Long barrier = barriers.get(partition);
        if (barrier != null && position >= barrier) {
            recovering.remove(partition);
        }
    }

    @Override
    public boolean isRecovering(int partition) {
        return recovering.contains(partition) || forceRecovering.contains(partition);
    }

    @Override
    public TrackerCursor committedCursor(int partition, DueBatch inFlight) {
        cursorCalls.add(new CursorCall(partition, snapshot(inFlight)));
        events.add("cursor:" + partition + ":" + inFlight.size());
        return new TrackerCursor(cursorOffsets.getOrDefault(partition, 0L), "m" + partition);
    }

    @Override
    public List<TrackerRecordData> encodeCompletions(DueBatch dispatched, CompletionReason reason) {
        encodeCalls.add(new EncodeCall(reason, snapshot(dispatched)));
        events.add("encode:" + reason + ":" + dispatched.size());
        List<TrackerRecordData> tombstones = new ArrayList<>(dispatched.size());
        for (int i = 0; i < dispatched.size(); i++) {
            Headers headers = new RecordHeaders();
            headers.add("reason", reason.name().getBytes(StandardCharsets.US_ASCII));
            tombstones.add(new TrackerRecordData(key(dispatched.sourceOffset(i)), null, headers));
        }
        return tombstones;
    }

    static byte[] key(long sourceOffset) {
        return ByteBuffer.allocate(Long.BYTES).putLong(sourceOffset).array();
    }

    /** Value snapshot of a batch as {@code (partition, sourceOffset)} rows, in view order. */
    static List<long[]> snapshot(DueBatch batch) {
        List<long[]> rows = new ArrayList<>(batch.size());
        for (int i = 0; i < batch.size(); i++) {
            rows.add(new long[] {batch.sourcePartition(i), batch.sourceOffset(i)});
        }
        return rows;
    }

    @Override
    public void maintenance() {}

    @Override
    public void close() {}

    // --- ingest-side and lifecycle surface the dispatch loop must never call ---

    @Override
    public TrackerRecordData encodeSchedule(ScheduledRef ref) {
        throw offLimits();
    }

    @Override
    public void configure(StoreContext context) {
        throw offLimits();
    }

    @Override
    public StoreCapabilities capabilities() {
        throw offLimits();
    }

    @Override
    public void validate() {
        throw offLimits();
    }

    @Override
    public void start() {
        throw offLimits();
    }

    private static UnsupportedOperationException offLimits() {
        return new UnsupportedOperationException("the dispatch loop must not call lifecycle/ingest-side "
                + SchedulerStore.class.getSimpleName() + " methods");
    }
}
