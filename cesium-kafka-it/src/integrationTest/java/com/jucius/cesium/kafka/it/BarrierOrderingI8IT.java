package com.jucius.cesium.kafka.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.jucius.cesium.kafka.api.store.TrackerRecordData;
import com.jucius.cesium.kafka.core.config.KafkaConfig;
import com.jucius.cesium.kafka.core.config.Role;
import com.jucius.cesium.kafka.core.kafka.KafkaClientFactory;
import com.jucius.cesium.kafka.core.testing.CrashPoints;
import com.jucius.cesium.kafka.store.tracker.TrackerWireFormat;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Design §11.3-5 / D-14 — the I8 barrier-ordering hazard, NON-NEGOTIABLE (deferred from M5). The
 * subtlest correctness property in the system (R3): a dispatcher is stalled <strong>after its
 * {@code sendOffsetsToTransaction} is accepted but before {@code commitTransaction}</strong>; the
 * tracker partition fails over to a new owner; then the stall is released so the predecessor's
 * transaction commits <em>after takeover has begun</em>. The new owner's offset fetch must wait out
 * the pending commit ({@code require_stable} / {@code UNSTABLE_OFFSET_COMMIT}) and snapshot the
 * barrier strictly afterwards (I8/D19), so the predecessor's late tombstones fall below the barrier,
 * are replayed, and no duplicate is delivered.
 *
 * <p><strong>Why I8.</strong> If the barrier were snapshotted at callback time (the pre-R3 design),
 * it could predate the predecessor's {@code commitTransaction} flush; the predecessor's producer was
 * never fenced (nobody ran {@code initTransactions()} on its stable id — the new owner has a
 * different {@code transactional.id}), so its commit lands tombstones above the stale barrier and
 * the new owner, gated only to that barrier, never applies them ⇒ duplicate (§3.6). The load-bearing
 * fix is the ordering: resolve the committed cursor first (the {@code UNSTABLE_OFFSET_COMMIT} wait is
 * the synchronization point, §3.4.2), then snapshot the barrier.
 *
 * <p><strong>Mechanism (deterministic — R21).</strong> Two real dispatch instances (A = the
 * predecessor, N = the new owner) form one group-B over a single tracker partition. A {@link CrashPoints}
 * pause seam at {@link CrashPoints#DISPATCH_AFTER_SENDS} (D-2 — fired in the loop exactly between
 * {@code sendOffsetsToTransaction} and {@code commitTransaction}) blocks A's loop while it holds the
 * open transaction, genuinely exercising the post-sendOffsets/pre-commit window. A short
 * {@code max.poll.interval.ms} on A evicts its (dynamic-membership) consumer once it stops polling, so
 * the partition moves to N while A's transaction is still open; N's {@code committed()} then blocks on
 * {@code UNSTABLE_OFFSET_COMMIT}. Releasing the stall lets A commit after takeover. A long
 * {@code max.poll.interval.ms} on N keeps it in the group through the blocking offset fetch, and a
 * raised {@code transaction.timeout.ms} keeps A's dangling transaction from auto-aborting before the
 * release.
 *
 * <p>Asserted: while A is stalled and N owns the partition, N makes no progress (no committed
 * destination record — it is waiting out the pending commit); after release, the destination carries
 * the entry <strong>exactly once</strong> and the tracker its single ADD + COMPLETE — no duplicate.
 */
class BarrierOrderingI8IT extends KafkaIT {

    private static final String KEY = "k-i8";

    /** Evicts A's stalled consumer fast (dynamic member ⇒ proactive LeaveGroup at this bound). */
    private static final String A_MAX_POLL_INTERVAL_MS = "7000";

    /** Keeps N in the group through its blocking {@code committed()} wait on the pending commit. */
    private static final String N_MAX_POLL_INTERVAL_MS = "120000";

    private static final String SESSION_TIMEOUT_MS = "6000";
    private static final String HEARTBEAT_INTERVAL_MS = "2000";

    private final CountDownLatch reachedD2 = new CountDownLatch(1);
    private final CountDownLatch releaseD2 = new CountDownLatch(1);
    private final AtomicBoolean armed = new AtomicBoolean(true);

    private EngineHarness predecessor;
    private EngineHarness newOwner;

    @AfterEach
    void tearDown() {
        releaseD2.countDown(); // never leave a dispatch thread parked in the pause seam
        CrashPoints.reset();
        if (predecessor != null) {
            predecessor.close();
        }
        if (newOwner != null) {
            newOwner.close();
        }
    }

    @Test
    void predecessorCommitsAfterTakeoverAndTheNewOwnerReplaysTheLateTombstonesWithoutDuplicating() {
        predecessorCommitsAfterTakeoverAndTheNewOwnerReplaysTheLateTombstonesWithoutDuplicating(
                KafkaConfig.GroupProtocol.CLASSIC);
    }

    /**
     * KIP-848 variant (§11.3-11, D12): the I8 barrier-ordering hazard under
     * {@code group.protocol=consumer}. The synchronization point is the {@code committed()} block on
     * {@code UNSTABLE_OFFSET_COMMIT}; this variant additionally checks that a stalled consumer is
     * still evicted on {@code max.poll.interval.ms} so the partition fails over under the new protocol.
     * Non-blocking nightly lane — if the consumer protocol's eviction/{@code require_stable} timing
     * differs, this is the documented-finding case (the lane is continue-on-error).
     */
    @Test
    @Tag("kip848")
    void predecessorCommitsAfterTakeoverWithoutDuplicatingUnderConsumerProtocol() {
        predecessorCommitsAfterTakeoverAndTheNewOwnerReplaysTheLateTombstonesWithoutDuplicating(
                KafkaConfig.GroupProtocol.CONSUMER);
    }

    private void predecessorCommitsAfterTakeoverAndTheNewOwnerReplaysTheLateTombstonesWithoutDuplicating(
            KafkaConfig.GroupProtocol protocol) {
        String applicationId = unique("i8-app");
        String source = createTopic("src", 1);
        String destination = createTopic("dst", 1);
        String dlq = createTopic("dlq", 1);

        predecessor = EngineHarness.builder()
                .applicationId(applicationId)
                .instanceId("random") // dynamic membership ⇒ A leaves promptly on poll-interval expiry
                .groupProtocol(protocol)
                .roles(Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .kafkaProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, A_MAX_POLL_INTERVAL_MS)
                .kafkaProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS)
                .kafkaProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, HEARTBEAT_INTERVAL_MS)
                // Drain slice must stay <= max.poll.interval.ms / 3 (R2); shrink it in step with the
                // small poll interval A uses for fast eviction.
                .drainMaxSlice(Duration.ofSeconds(1))
                // Keep A's dangling transaction alive across the failover so it can commit on release.
                .transactionTimeout(Duration.ofMinutes(2))
                .build();
        newOwner = EngineHarness.builder()
                .applicationId(applicationId)
                .instanceId("random")
                .groupProtocol(protocol)
                .roles(Role.DISPATCH)
                .source(source)
                .destination(destination)
                .dlq(dlq)
                .kafkaProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, N_MAX_POLL_INTERVAL_MS)
                .kafkaProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS)
                .kafkaProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, HEARTBEAT_INTERVAL_MS)
                .drainMaxSlice(Duration.ofSeconds(1))
                .build();

        String groupId = KafkaClientFactory.dispatchGroupId(applicationId);

        // The source payload for X, and a committed ADD_X scheduling it (due in the past). A becomes
        // the first-ever dispatcher of this partition — so the ONLY committed-offset commit for it is
        // the one A stages (and stalls before finalizing), which is exactly what N must wait out.
        long sourceOffset;
        try (Producer<byte[], byte[]> plain = newProducer()) {
            sourceOffset = produce(plain, source, 0, System.currentTimeMillis(), KEY, "payload")
                    .offset();
            plain.flush();
        }
        predecessor.validate(); // create the tracker topic (idempotent: start() validates again)
        String tracker = predecessor.trackerTopic();
        try (Producer<byte[], byte[]> plain = newProducer()) {
            TrackerRecordData add =
                    TrackerWireFormat.encodeAdd(sourceOffset, System.currentTimeMillis() - 60_000, false);
            sendSync(plain, new ProducerRecord<>(tracker, 0, add.key(), add.value(), add.headers()));
        }

        // Arm the pause seam BEFORE A starts: block the first dispatcher exactly between
        // sendOffsetsToTransaction and commitTransaction (D-2), holding its transaction open.
        CrashPoints.install(point -> {
            if (CrashPoints.DISPATCH_AFTER_SENDS.equals(point) && armed.compareAndSet(true, false)) {
                reachedD2.countDown();
                try {
                    if (!releaseD2.await(120, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("I8 stall was never released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        // A starts, becomes the sole owner of tracker partition 0, dispatches X, and stalls at D-2.
        predecessor.start();
        await("predecessor A is the sole owner of tracker partition 0")
                .atMost(WAIT)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> soleOwnerOf(groupId, tracker, 0) != null);
        String predecessorConsumerId = soleOwnerOf(groupId, tracker, 0);
        awaitLatch(reachedD2, "predecessor reaches the post-sendOffsets/pre-commit seam (D-2)");

        // Force the failover: A is stalled (not polling) so its consumer is evicted; N joins and takes
        // tracker partition 0 while A still holds the open transaction.
        newOwner.start();
        await("new owner N (a different member) takes over tracker partition 0 after A is evicted")
                .atMost(Duration.ofSeconds(90))
                .pollInterval(Duration.ofMillis(500))
                .until(() -> ownedByOther(groupId, tracker, 0, predecessorConsumerId));

        // While A is still stalled, N is waiting out the pending offset commit (require_stable /
        // UNSTABLE_OFFSET_COMMIT) — it makes no progress: nothing is committed to the destination
        // (A's relay is uncommitted; N has not, and must not, dispatch).
        assertNoCommittedRecords(destination, Duration.ofSeconds(4));

        // Release the stall: A's commitTransaction lands its tombstone + offset commit AFTER takeover
        // began. N's offset fetch resolves, N snapshots the barrier strictly afterwards (I8) so it
        // covers the late tombstone, replays it, completes X — and dispatches nothing.
        releaseD2.countDown();
        predecessor.stop(); // A commits, then exits; cleanly removed so it cannot rejoin and churn

        // The non-negotiable property: the entry is delivered EXACTLY ONCE. The grace window catches a
        // late asynchronous re-dispatch from either instance.
        List<ConsumerRecord<byte[], byte[]>> committed = readExactlyCommitted(destination, 1, WAIT, RETRY_GRACE);
        assertEquals(1, committed.size(), "exactly one committed destination record (no duplicate)");
        assertEquals(KEY, utf8(committed.get(0).key()));

        // The durable lifecycle settled exactly once: one ADD + one COMPLETE tombstone, nothing extra.
        readExactlyCommitted(tracker, 2, WAIT, RETRY_GRACE);

        newOwner.stop();
    }

    // ------------------------------------------------------------------ helpers

    /**
     * The {@code consumerId} of the single member owning {@code (trackerTopic, partition)}, or
     * {@code null} when the group does not have exactly one member owning it.
     */
    private static String soleOwnerOf(String groupId, String trackerTopic, int partition) {
        ConsumerGroupDescription description = describe(groupId);
        if (description.members().size() != 1) {
            return null;
        }
        MemberDescription member = description.members().iterator().next();
        return member.assignment().topicPartitions().contains(new TopicPartition(trackerTopic, partition))
                ? member.consumerId()
                : null;
    }

    /** True once a member <em>other</em> than {@code notThisConsumerId} owns the partition (= N took over). */
    private static boolean ownedByOther(String groupId, String trackerTopic, int partition, String notThisConsumerId) {
        for (MemberDescription member : describe(groupId).members()) {
            if (!member.consumerId().equals(notThisConsumerId)
                    && member.assignment().topicPartitions().contains(new TopicPartition(trackerTopic, partition))) {
                return true;
            }
        }
        return false;
    }

    private static ConsumerGroupDescription describe(String groupId) {
        try {
            return ADMIN.describeConsumerGroups(List.of(groupId))
                    .all()
                    .get(20, TimeUnit.SECONDS)
                    .get(groupId);
        } catch (Exception e) {
            throw new AssertionError("describeConsumerGroups(" + groupId + ") failed", e);
        }
    }

    private static void awaitLatch(CountDownLatch latch, String what) {
        try {
            if (!latch.await(WAIT.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new AssertionError("timed out waiting for: " + what);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for: " + what, e);
        }
    }

    private static void sendSync(Producer<byte[], byte[]> producer, ProducerRecord<byte[], byte[]> record) {
        try {
            producer.send(record).get();
        } catch (Exception e) {
            throw new AssertionError("produce to " + record.topic() + " failed", e);
        }
    }
}
