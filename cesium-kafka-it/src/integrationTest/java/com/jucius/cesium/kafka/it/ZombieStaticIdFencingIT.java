package com.jucius.cesium.kafka.it;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.jucius.cesium.kafka.core.config.Role;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Design §3.4 / §3.9 (I-5, D-4) / §11.3-3 second variant: <strong>zombie fencing by duplicate
 * {@code group.instance.id}</strong> — the fully deterministic fencing primitive the design prefers
 * over wall-clock races (R21). A new static member joins the engine's consumer group with the prior
 * member's {@code group.instance.id}; the broker treats it as the static member returning and fences
 * the prior holder with {@code FencedInstanceIdException}, which the §3.8 taxonomy classifies fatal.
 * We assert the prior engine member's loop exits fatally and that each input is still delivered
 * exactly once. Covered for the ingest loop (group A) and the dispatch loop (group B).
 *
 * <p>The impostor is a plain (non-transactional) consumer, so the fence is purely a group-membership
 * event — it never spins up a second producer under the prior member's {@code transactional.id}. The
 * companion network-partition variant lives in {@link ZombieFencingIT}.
 *
 * <p>Class-{@code @Tag("nightly")} (heavy: spins the dedicated Toxiproxy broker + several engine
 * members). <strong>Classic-protocol only:</strong> there is deliberately no KIP-848 variant — under
 * {@code group.protocol=consumer} a duplicate {@code group.instance.id} join does <em>not</em> fence
 * the prior member (the broker rejects the newcomer with {@code UnreleasedInstanceIdException} and the
 * incumbent keeps its assignment), a genuine KIP-848 semantic change verified at M6 and recorded in
 * {@code docs/failure-matrix-coverage.md}. The protocol-agnostic takeover fence (broker-side
 * {@code TxnOffsetCommit} epoch rejection on eviction) is exercised under {@code consumer} by
 * {@code BarrierOrderingI8IT} instead.
 */
@Tag("nightly")
class ZombieStaticIdFencingIT {

    private static final Duration WAIT = Duration.ofSeconds(60);
    private static final Duration RETRY_GRACE = Duration.ofSeconds(6);
    private static final Duration GRACE = Duration.ofSeconds(1);
    private static final String SESSION_MS = "6000";
    private static final String HEARTBEAT_MS = "1500";

    private final List<EngineHarness> harnesses = new ArrayList<>();
    private final List<Consumer<byte[], byte[]>> impostors = new ArrayList<>();
    private ToxiproxySupport cluster;

    @AfterEach
    void tearDown() {
        for (Consumer<byte[], byte[]> impostor : impostors) {
            impostor.close();
        }
        for (EngineHarness harness : harnesses) {
            harness.close();
        }
    }

    @Test
    void duplicateIngestInstanceIdFencesPriorMemberFatallyWithoutDuplicates() {
        cluster = ToxiproxySupport.shared();
        String applicationId = ToxiproxySupport.unique("zombie-ingest");
        String source = cluster.createTopic("zf-src", 1);
        String destination = cluster.createTopic("zf-dst", 1);
        String dlq = cluster.createTopic("zf-dlq", 1);
        int n = 8;

        EngineHarness prior = build(applicationId, "slot-0", source, destination, dlq, Role.INGEST);
        prior.start();
        ScenarioSupport.produceImmediate(cluster.directBootstrap(), source, 1, n, "k");
        FencingProbe probe = new FencingProbe(cluster.directBootstrap());
        probe.readCommitted(destination, n, WAIT, GRACE); // the prior member relayed all n

        // A new member joins group A with the SAME group.instance.id: the static-id join fences the
        // prior static member (FencedInstanceIdException, fatal per §3.8).
        joinImpostor(prior.ingestGroupId(), "slot-0", source);
        assertFencingFatal(prior.awaitDeath(WAIT));

        // The fenced member never re-commits, so its committed relays remain the only copies: each
        // input exactly once, no duplicate from the fencing.
        probe.assertExactlyOnceByKey(destination, n, WAIT, RETRY_GRACE);
    }

    @Test
    void duplicateDispatchInstanceIdFencesPriorMemberFatallyWithoutDuplicates() {
        cluster = ToxiproxySupport.shared();
        String applicationId = ToxiproxySupport.unique("zombie-dispatch");
        String source = cluster.createTopic("zf-src", 1);
        String destination = cluster.createTopic("zf-dst", 1);
        String dlq = cluster.createTopic("zf-dlq", 1);
        int n = 8;
        long deliverAt = System.currentTimeMillis() + 6_000;

        // A never-fenced ingest instance fills the tracker; a single dispatcher drains it.
        EngineHarness feeder = build(applicationId, "ingest-0", source, destination, dlq, Role.INGEST);
        EngineHarness prior = build(applicationId, "dispatch-0", source, destination, dlq, Role.DISPATCH);
        feeder.start();
        prior.start();
        ScenarioSupport.produceDeliverAt(cluster.directBootstrap(), source, 1, n, "k", deliverAt);

        // Let the dispatcher drive every entry to the destination before it is fenced (so the fence
        // tests no-duplicate, not a lost in-flight batch — there is no survivor in this variant).
        FencingProbe probe = new FencingProbe(cluster.directBootstrap());
        probe.readCommitted(destination, n, WAIT, GRACE);

        // A new member joins group B with the SAME group.instance.id: the static-id join fences the
        // prior dispatcher (FencedInstanceIdException, fatal per §3.8).
        joinImpostor(prior.dispatchGroupId(), "dispatch-0", prior.trackerTopic());
        assertFencingFatal(prior.awaitDispatchDeath(WAIT));

        // Exactly once: the fenced dispatcher's tombstones are durable, nothing re-dispatches.
        probe.assertExactlyOnceByKey(destination, n, WAIT, RETRY_GRACE);
    }

    // ------------------------------------------------------------------ helpers

    /**
     * Joins {@code groupId} as a static member with {@code groupId.instanceId} (the prior engine
     * member's id), fencing it; the consumer is held open and closed in teardown so the fence stays
     * in effect through the assertions.
     */
    private void joinImpostor(String groupId, String instanceId, String topic) {
        impostors.add(ScenarioSupport.joinAsStaticImpostor(
                cluster.directBootstrap(), groupId, groupId + "." + instanceId, topic, WAIT));
    }

    private EngineHarness build(
            String applicationId, String instanceId, String src, String dst, String dlq, Role... roles) {
        EngineHarness harness = EngineHarness.builder()
                .applicationId(applicationId)
                .instanceId(instanceId)
                .bootstrapServers(cluster.directBootstrap())
                .roles(roles)
                .source(src)
                .destination(dst)
                .dlq(dlq)
                .kafkaProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_MS)
                .kafkaProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, HEARTBEAT_MS)
                .build();
        harnesses.add(harness);
        return harness;
    }

    /** Walks the cause chain for a fencing classification (the §3.8 fatal path; no restore, I9). */
    // ReferenceEquality: `t != t.getCause()` is the self-referential-cause guard — a Throwable whose
    // getCause() returns itself would loop forever. Identity is exactly the intended test, and Error
    // Prone's suggested .equals() is wrong here (Throwable does not override equals). Newly flagged
    // by Error Prone 2.50.0; 2.49.0 did not report it.
    @SuppressWarnings("ReferenceEquality")
    private static void assertFencingFatal(Throwable death) {
        assertNotNull(death, "the fenced member's loop must have died");
        for (Throwable t = death; t != null && t != t.getCause(); t = t.getCause()) {
            String name = t.getClass().getSimpleName();
            if (name.contains("Fenced") || name.contains("InvalidProducerEpoch")) {
                return;
            }
        }
        throw new AssertionError(
                "expected a fencing fatal (ProducerFenced / FencedInstanceId / InvalidProducerEpoch) but got: "
                        + death);
    }
}
