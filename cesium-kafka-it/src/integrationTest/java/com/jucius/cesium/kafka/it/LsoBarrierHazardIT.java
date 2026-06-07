package com.jucius.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jucius.cesium.kafka.api.store.CompletionReason;
import com.jucius.cesium.kafka.api.store.TrackerRecordData;
import com.jucius.cesium.kafka.core.config.KafkaConfig;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.store.tracker.TrackerWireFormat;
import io.micrometer.core.instrument.Gauge;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Design §11.3-4 / D-10 — the LSO hazard, NON-NEGOTIABLE (deferred from M5). Proves the replay
 * barrier is the high watermark (HW via {@code Admin.listOffsets(latest, READ_UNCOMMITTED)}) and
 * <em>not</em> the consumer's {@code endOffsets} (the LSO under {@code read_committed}), so a
 * dispatch failover whose tracker partition has a <strong>committed dispatch tombstone above an
 * open foreign transaction's LSO</strong> gates replay until that transaction resolves and delivers
 * no duplicate (§3.6 "The LSO hazard").
 *
 * <p><strong>The hazard (§3.6).</strong> Previous owner O dispatched entry X in a committed
 * transaction: a destination write {@code D_X} and a tombstone {@code C_X} at tracker offset
 * {@code o_C}. Meanwhile an <em>ingest</em> transaction, open since before that commit, holds an
 * ADD on the same tracker partition at an offset <em>below</em> {@code o_C}, so
 * {@code LSO < o_C}. A replay that stops at an LSO snapshot never sees {@code C_X}, concludes X is
 * pending, and — X being past due — re-dispatches it immediately: a read_committed-visible
 * <strong>duplicate</strong>. The barrier must be the HW (above {@code o_C}); a read_committed
 * replayer cannot pass the LSO, so the shard stays {@code RECOVERING} (position &lt; barrier) and
 * X is gated (I4) until the open transaction resolves and {@code C_X} becomes readable.
 *
 * <p><strong>Mechanism (documented, deterministic — R21).</strong> The exact takeover STATE the
 * §3.6 narrative describes cannot be produced by a live previous owner: a real dispatch owner that
 * already holds the foreign open transaction's LSO is itself gated and could never have written
 * {@code C_X} above it (its own read_committed consumer is stuck at the LSO). So this test
 * constructs the precise interleaving with controlled producers — exactly the "ADD buffered below
 * the tombstone" the task sanctions — and then starts the real {@link com.jucius.cesium.kafka.core.dispatch.DispatchLoop}
 * as the <em>new owner</em> taking over the partition, exercising the real recovery path end to end
 * (first-run cursor resolution, the real {@link com.jucius.cesium.kafka.core.dispatch.KafkaDispatchAdmin}
 * HW barrier snapshot, {@code beginRecovery}, and the {@code position >= barrier} promotion gate
 * driven by a real {@code read_committed} consumer blocked at the LSO):
 *
 * <ol>
 *   <li>a committed {@code ADD_X} at tracker offset 0 (due in the past ⇒ immediately due);
 *   <li>a foreign transactional producer appends a record at tracker offset 1 and is left OPEN ⇒
 *       {@code LSO = 1} (this is the "ingest transaction open since before the commit");
 *   <li>a second transactional producer commits {@code C_X} at tracker offset 2 (above the LSO)
 *       together with the single committed {@code D_X} on the destination — the exactly-once copy.
 * </ol>
 *
 * <p>With the correct HW barrier the new owner stays gated (no second {@code D_X}); resolving the
 * open transaction lets it replay {@code C_X}, complete X, reach the barrier, and dispatch nothing.
 * With an LSO barrier it would promote at once and re-dispatch X — the bug this test forbids.
 */
class LsoBarrierHazardIT extends KafkaIT {

    /** The single source/destination key; a re-dispatch of X would collide on it (a duplicate). */
    private static final String KEY = "k-lso";

    private EngineHarness harness;
    private Producer<byte[], byte[]> foreignTxn;

    @AfterEach
    void tearDown() {
        // Resolve any still-open foreign transaction first so the harness threads are never blocked
        // behind an unresolved LSO during shutdown.
        if (foreignTxn != null) {
            try {
                foreignTxn.abortTransaction();
            } catch (RuntimeException ignored) {
                // Already resolved by the test, or the producer is being discarded — not the assertion.
            }
            foreignTxn.close(Duration.ZERO);
            foreignTxn = null;
        }
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void openForeignTransactionGatesReplayUntilItResolvesAndNeverDuplicates() {
        openForeignTransactionGatesReplayUntilItResolvesAndNeverDuplicates(KafkaConfig.GroupProtocol.CLASSIC);
    }

    /**
     * KIP-848 variant (§11.3-11, D12): the same LSO/HW-barrier hazard under
     * {@code group.protocol=consumer}. The barrier is the high watermark from
     * {@code Admin.listOffsets(READ_UNCOMMITTED)} and the {@code position >= barrier} promotion gate
     * is driven by a {@code read_committed} consumer — both protocol-agnostic — so the gate must hold
     * identically. Non-blocking nightly lane.
     */
    @Test
    @Tag("kip848")
    void openForeignTransactionGatesReplayUnderConsumerProtocol() {
        openForeignTransactionGatesReplayUntilItResolvesAndNeverDuplicates(KafkaConfig.GroupProtocol.CONSUMER);
    }

    private void openForeignTransactionGatesReplayUntilItResolvesAndNeverDuplicates(
            KafkaConfig.GroupProtocol protocol) {
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);
        harness = EngineHarness.builder()
                .roles(Role.DISPATCH)
                .groupProtocol(protocol)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .build();

        // The source payload for X — present so a buggy LSO-barrier re-dispatch would actually
        // re-fetch and relay a real duplicate (the regression this test guards must be observable).
        long sourceOffset;
        try (Producer<byte[], byte[]> plain = newProducer()) {
            sourceOffset = produce(plain, source, 0, System.currentTimeMillis(), KEY, "payload")
                    .offset();
            plain.flush();
        }

        // Create the tracker topic via the real CREATE bootstrap (idempotent: start() validates again).
        harness.validate();
        String tracker = harness.trackerTopic();

        // (1) committed ADD_X at tracker offset 0, due in the past ⇒ immediately due once recovered.
        long dueMs = System.currentTimeMillis() - 60_000;
        try (Producer<byte[], byte[]> plain = newProducer()) {
            TrackerRecordData add = TrackerWireFormat.encodeAdd(sourceOffset, dueMs, false);
            RecordMetadata addMeta =
                    sendSync(plain, new ProducerRecord<>(tracker, 0, add.key(), add.value(), add.headers()));
            assertEquals(0L, addMeta.offset(), "ADD_X must be the first tracker record (offset 0)");
        }

        // (2) foreign INGEST transaction: append a (far-future) ADD at tracker offset 1 and leave it
        //     OPEN, holding LSO = 1 below where C_X will land. A 2-minute transaction timeout keeps
        //     it open across the whole gating window rather than the 30 s default auto-aborting it.
        foreignTxn = transactionalProducer(unique("lso-foreign-ingest"), Duration.ofMinutes(2));
        foreignTxn.beginTransaction();
        TrackerRecordData foreignAdd =
                TrackerWireFormat.encodeAdd(9_999L, System.currentTimeMillis() + 3_600_000L, false);
        RecordMetadata foreignMeta = sendSync(
                foreignTxn,
                new ProducerRecord<>(tracker, 0, foreignAdd.key(), foreignAdd.value(), foreignAdd.headers()));
        assertEquals(1L, foreignMeta.offset(), "the foreign open-txn record must hold the LSO at offset 1");

        // (3) committed DISPATCH transaction: C_X (a real COMPLETE tombstone) at tracker offset 2,
        //     above the LSO, plus the single committed D_X on the destination.
        try (Producer<byte[], byte[]> dispatchTxn = transactionalProducer(unique("lso-manual-dispatch"), null)) {
            dispatchTxn.beginTransaction();
            TrackerRecordData complete = TrackerWireFormat.encodeCompletion(sourceOffset, CompletionReason.DISPATCHED);
            RecordMetadata completeMeta = sendSync(
                    dispatchTxn,
                    new ProducerRecord<>(tracker, 0, complete.key(), complete.value(), complete.headers()));
            assertEquals(2L, completeMeta.offset(), "C_X must commit above the open-txn LSO (offset 2)");
            sendSync(dispatchTxn, new ProducerRecord<>(destination, 0, bytes(KEY), bytes("payload")));
            dispatchTxn.commitTransaction();
        }

        // The hazard state is real: the LSO is held strictly below the high watermark, and a
        // committed tombstone (C_X) lives in that [LSO, HW) gap. This is what an LSO-bounded barrier
        // would miss.
        long lso = latestOffset(tracker, 0, IsolationLevel.READ_COMMITTED);
        long hw = latestOffset(tracker, 0, IsolationLevel.READ_UNCOMMITTED);
        assertEquals(1L, lso, "the open foreign transaction must hold the read_committed LSO at 1");
        assertEquals(4L, hw, "the high watermark (the barrier the new owner must use) sits above C_X");
        assertTrue(lso < hw, "the committed tombstone C_X lives in the [LSO, HW) gap");

        // The single committed destination copy exists up front (the exactly-once baseline).
        assertEquals(1, drainCount(destination, "read_committed"), "exactly one committed D_X before takeover");

        // The NEW OWNER takes the partition: first-run seek-to-beginning, HW barrier snapshot,
        // beginRecovery, and a read_committed consumer that stalls at the LSO below the barrier.
        harness.start();

        // It replayed ADD_X and holds X pending — but the shard cannot promote past the open-txn LSO,
        // so X is gated (RECOVERING, position < barrier). pending == 1 proves the entry is there to
        // be (wrongly) re-dispatched; an LSO barrier would already have promoted and fired it.
        await("new owner replayed ADD_X and holds it pending behind the LSO barrier")
                .atMost(WAIT)
                .pollInterval(Duration.ofMillis(200))
                .until(() -> pendingEntries(0) == 1.0);

        // GATED: across a generous window — while the foreign transaction still holds the LSO — no
        // second destination copy appears. (A buggy LSO barrier re-dispatches X within ~1 s.)
        List<ConsumerRecord<byte[], byte[]>> gated = readExactlyCommitted(destination, 1, WAIT, Duration.ofSeconds(8));
        assertEquals(KEY, utf8(gated.get(0).key()), "the only committed destination record is the original D_X");

        // Resolve the open transaction: the LSO advances past C_X, the new owner replays it (R2
        // removes X), reaches the barrier, promotes ACTIVE — and dispatches nothing.
        foreignTxn.abortTransaction();
        foreignTxn.close(Duration.ZERO);
        foreignTxn = null;

        await("X completed by the replayed C_X once the LSO advanced (shard reached the barrier)")
                .atMost(WAIT)
                .pollInterval(Duration.ofMillis(200))
                .until(() -> pendingEntries(0) == 0.0);

        // Still exactly one committed destination copy — no duplicate, ever. The grace window catches
        // any late asynchronous re-dispatch.
        List<ConsumerRecord<byte[], byte[]>> finalOut = readExactlyCommitted(destination, 1, WAIT, RETRY_GRACE);
        byKey(finalOut); // exactly once per key
        assertEquals(1, finalOut.size(), "exactly one committed destination record after the transaction resolved");

        harness.stop();
    }

    // ------------------------------------------------------------------ helpers

    /** The live index size for a partition, read off the new owner's store gauge ({@code -1} if absent). */
    private double pendingEntries(int partition) {
        Gauge gauge = harness.meterRegistry()
                .find("cesium.pending.entries")
                .tag("partition", Integer.toString(partition))
                .gauge();
        return gauge == null ? -1.0 : gauge.value();
    }

    /** The latest offset under an explicit isolation level: READ_COMMITTED = LSO, READ_UNCOMMITTED = HW. */
    private static long latestOffset(String topic, int partition, IsolationLevel level) {
        TopicPartition tp = new TopicPartition(topic, partition);
        return get(ADMIN.listOffsets(Map.of(tp, OffsetSpec.latest()), new ListOffsetsOptions(level))
                        .partitionResult(tp))
                .offset();
    }

    private static RecordMetadata sendSync(Producer<byte[], byte[]> producer, ProducerRecord<byte[], byte[]> record) {
        try {
            return producer.send(record).get();
        } catch (Exception e) {
            throw new AssertionError("transactional send to " + record.topic() + " failed", e);
        }
    }

    /** A byte-array transactional producer, initialized for transactions; the caller drives begin/commit. */
    private static Producer<byte[], byte[]> transactionalProducer(String transactionalId, Duration transactionTimeout) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        if (transactionTimeout != null) {
            props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, Long.toString(transactionTimeout.toMillis()));
        }
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        return producer;
    }
}
