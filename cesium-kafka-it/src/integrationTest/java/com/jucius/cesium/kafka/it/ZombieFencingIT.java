package com.jucius.cesium.kafka.it;

import com.jucius.cesium.kafka.core.config.Role;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Design §3.4 / §3.9 (D-4) / §11.3-3 first variant: <strong>zombie containment by network
 * partition</strong>. A proxied dispatch member is {@link ToxiproxySupport#cut cut} from the broker
 * ~25 s before its shard's entries are due; after its session expires the survivor takes over the
 * stranded tracker shard and drives every entry to the destination exactly once, and healing the
 * link lets the returning zombie recover from the survivor-advanced cursor and replay completed work
 * as no-ops — never a duplicate.
 *
 * <p><strong>What this test verifies — and what proves the complementary claim.</strong> Because the
 * partition lands well before any entry is due (deliver-at is ~25 s out), the cut member never opens
 * a dispatch transaction for these entries: there is no in-flight zombie commit <em>here</em> to
 * fence. The load-bearing properties this test actually exercises are therefore (1) <em>survivor
 * takeover after session eviction</em> — the surviving direct member ends up owning both tracker
 * partitions and delivers each entry exactly once while its peer is partitioned away — and (2)
 * <em>heal-replay-as-no-op</em> — the returning zombie resumes from the survivor-advanced cursor,
 * replays the survivor's COMPLETE tombstones as R2 no-ops, and re-dispatches nothing. The
 * complementary guarantee — that an in-flight zombie transaction caught <em>mid-dispatch</em> aborts
 * whole rather than committing a duplicate (the I2 {@code sendOffsetsToTransaction} epoch fence
 * against live group metadata) — is proven deterministically by {@code
 * DispatchCrashPointsIT.d2CrashAfterSendsAbortsTheInFlightDispatchTransaction} (read_uncommitted
 * &gt; read_committed) and by the duplicate-{@code group.instance.id} fence in
 * {@link ZombieStaticIdFencingIT}; it is not re-derived on this single-proxied-listener substrate,
 * where a deterministic write-then-cut mid-transaction is hard — the documented R21 split.
 *
 * <p>The deterministic duplicate-{@code group.instance.id} variant — covering both group A (ingest)
 * and group B (dispatch) — lives in {@link ZombieStaticIdFencingIT}; the group-A network-partition
 * case is the symmetric mirror of this group-B one (the same stale-member {@code TxnOffsetCommit}
 * fence) and is exercised through that group-A static-id test rather than duplicated here, to keep
 * the suite's per-worker footprint light.
 *
 * <p>Class-{@code @Tag("nightly")} (heavy: dedicated Toxiproxy broker + a partitioned two-member
 * group). <strong>Classic-protocol only:</strong> the {@code TxnOffsetCommit} epoch fence it rests on
 * is protocol-agnostic, but the takeover is driven by <em>session-timeout eviction</em> of the
 * partitioned member, and under {@code group.protocol=consumer} the client {@code session.timeout.ms}
 * is broker-side (KIP-848), so eviction falls to the broker default (~45 s on the shared substrate)
 * rather than the 6 s the classic lane sets — making the wall-clock takeover too slow for this test's
 * deterministic budget. The protocol-agnostic takeover fence is exercised under {@code consumer} by
 * {@code BarrierOrderingI8IT} (fast client-side {@code max.poll.interval.ms} eviction) instead; the
 * KIP-848 eviction-timing finding is recorded in {@code docs/failure-matrix-coverage.md}.
 */
@Tag("nightly")
class ZombieFencingIT {

    private static final Duration WAIT = Duration.ofSeconds(60);

    /** Wide enough to cover a re-dispatch through several penalty/retry backoffs (50/100/200… ms). */
    private static final Duration RETRY_GRACE = Duration.ofSeconds(6);

    private static final Duration GRACE = Duration.ofSeconds(1);

    private ToxiproxySupport cluster;
    private MultiInstanceSupport group;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            ToxiproxySupport.restore(cluster.brokerProxy());
        }
        if (group != null) {
            group.close();
        }
    }

    @Test
    void toxiproxyPartitionedDispatchZombieSurvivorTakesOverExactlyOnce() {
        cluster = ToxiproxySupport.shared();
        group = MultiInstanceSupport.newGroup(cluster, 2, Role.INGEST, Role.DISPATCH);
        group.addInstance("slot-0", false); // survivor (direct, fault-immune)
        group.addInstance("slot-1", true); // zombie (proxied, partitionable)
        group.start("slot-0");
        group.start("slot-1");
        group.awaitAssignmentStable(group.ingestGroupId(), 2);
        group.awaitMemberCount(group.dispatchGroupId(), 2);

        int n = 6;
        long deliverAt = System.currentTimeMillis() + 25_000;
        ScenarioSupport.produceDeliverAt(cluster.directBootstrap(), group.sourceTopic(), 2, n, "k", deliverAt);
        FencingProbe probe = new FencingProbe(cluster.directBootstrap());
        probe.readCommitted(group.trackerTopic(), n, WAIT, GRACE); // ingest done before the cut

        // Partition the proxied dispatcher BEFORE the entries are due: its pending shard is stranded
        // and only the survivor can drive it to the destination.
        ToxiproxySupport.cut(cluster.brokerProxy());

        // After the zombie's session expires the survivor owns BOTH tracker partitions.
        group.awaitMemberPartitionCount(
                group.dispatchGroupId(), ScenarioSupport.dispatchGroupInstanceId(group, "slot-0"), 2);

        // The survivor dispatches every entry exactly once (no zombie double-delivery).
        probe.assertExactlyOnceByKey(group.destinationTopic(), n, WAIT, RETRY_GRACE);

        // Heal: the returning zombie recovers from the survivor-advanced cursor, replays the
        // tombstones as R2 no-ops, and re-dispatches nothing — still exactly once.
        ToxiproxySupport.restore(cluster.brokerProxy());
        probe.assertExactlyOnceByKey(group.destinationTopic(), n, WAIT, RETRY_GRACE);
    }
}
