package com.jucius.cesium.kafka.testkit;

import com.jucius.cesium.kafka.api.store.CompletionReason;
import java.util.ArrayList;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.Tuple;

/**
 * jqwik arbitraries for the contract's randomized properties: weighted {@link StoreOp} sequences
 * (adds with strictly increasing source offsets per partition, R1 duplicates, drains resolved by
 * commit, partial truncate-and-carry-over commit or definitive abort, idle cursor commits,
 * drop-and-re-recover round trips) and §3.7-shaped {@link TrackerEventScript}s — including
 * transactional control-marker gaps — for the pre-compacted-log convergence properties.
 *
 * <p>Every arbitrary here is built from shrinkable combinators (no {@code Arbitraries.randomValue}):
 * a failing property shrinks to a minimal op sequence or script instead of reporting a full
 * random one.
 */
public final class StoreOpArbitraries {

    private static final long MAX_DELAY_MS = 600_000;

    private StoreOpArbitraries() {}

    /**
     * Weighted op sequences over partitions {@code [0, partitionCount)}. Source offsets are
     * expressed as strictly positive gaps, so any sequence respects §3.1's unique-committed-ADD
     * shape; time only moves forward.
     */
    public static Arbitrary<List<StoreOp>> sequences(int partitionCount) {
        Arbitrary<Integer> partitions = Arbitraries.integers().between(0, partitionCount - 1);
        Arbitrary<Long> delays = Arbitraries.longs().between(-60_000, MAX_DELAY_MS);

        Arbitrary<StoreOp> add = Combinators.combine(
                        partitions, Arbitraries.integers().between(1, 50), delays, Arbitraries.of(true, false))
                .as(StoreOp.Add::new);
        Arbitrary<StoreOp> duplicate = Combinators.combine(
                        partitions, Arbitraries.integers().between(0, Integer.MAX_VALUE - 1), delays)
                .as(StoreOp.DuplicateAdd::new);
        Arbitrary<StoreOp> advance = Arbitraries.longs().between(0, 120_000).map(StoreOp.AdvanceTime::new);
        Arbitrary<StoreOp> drainCommit = Combinators.combine(
                        Arbitraries.integers().between(1, 64), Arbitraries.of(CompletionReason.class))
                .as(StoreOp.DrainCommit::new);
        Arbitrary<StoreOp> drainCommitPartial = Combinators.combine(
                        Arbitraries.integers().between(2, 64),
                        Arbitraries.integers().between(0, Integer.MAX_VALUE - 1),
                        Arbitraries.of(CompletionReason.class))
                .as(StoreOp.DrainCommitPartial::new);
        Arbitrary<StoreOp> drainAbort = Arbitraries.integers().between(1, 64).map(StoreOp.DrainAbort::new);
        Arbitrary<StoreOp> idleCommit = partitions.map(StoreOp.IdleCommit::new);
        Arbitrary<StoreOp> rerecover = partitions.map(StoreOp.Rerecover::new);

        return Arbitraries.frequencyOf(
                        Tuple.of(50, add),
                        Tuple.of(8, duplicate),
                        Tuple.of(14, advance),
                        Tuple.of(12, drainCommit),
                        Tuple.of(8, drainCommitPartial),
                        Tuple.of(6, drainAbort),
                        Tuple.of(5, idleCommit),
                        Tuple.of(5, rerecover))
                .list()
                .ofMinSize(0)
                .ofMaxSize(140);
    }

    /**
     * §3.1-shaped scripts for partition {@code partition}: adds with strictly increasing source
     * offsets, completes only for previously-added (and not yet completed) keys, and transactional
     * control-marker gaps — the raw material for the §3.7 compaction transforms. Built by a
     * deterministic fold over shrinkable steps, so failures shrink to minimal scripts.
     */
    public static Arbitrary<TrackerEventScript> compactionScripts(int partition) {
        Arbitrary<ScriptStep> steps = Combinators.combine(
                        // Step kind selector: 0-3 complete (when possible), 4-8 add, 9 marker —
                        // roughly the old generator's 40% complete bias, plus marker gaps.
                        Arbitraries.integers().between(0, 9),
                        Arbitraries.integers().between(0, Integer.MAX_VALUE - 1),
                        Arbitraries.integers().between(1, 20),
                        Arbitraries.longs().between(1_000_000, 2_000_000),
                        Arbitraries.of(true, false))
                .as(ScriptStep::new);
        return steps.list().ofMaxSize(60).map(list -> fold(partition, list));
    }

    private static TrackerEventScript fold(int partition, List<ScriptStep> steps) {
        TrackerEventScript script = TrackerEventScript.forPartition(partition);
        List<Long> completable = new ArrayList<>();
        long nextSourceOffset = 0;
        for (ScriptStep step : steps) {
            if (step.kind() <= 3 && !completable.isEmpty()) {
                script = script.complete(completable.remove(step.pickIndex() % completable.size()));
            } else if (step.kind() == 9) {
                script = script.commitMarker();
            } else {
                script = script.add(nextSourceOffset, step.dispatchAtMs(), step.clamped());
                completable.add(nextSourceOffset);
                nextSourceOffset += step.sourceOffsetGap();
            }
        }
        return script;
    }

    /** One shrinkable script-building step; folded deterministically by {@link #fold}. */
    private record ScriptStep(int kind, int pickIndex, int sourceOffsetGap, long dispatchAtMs, boolean clamped) {}
}
