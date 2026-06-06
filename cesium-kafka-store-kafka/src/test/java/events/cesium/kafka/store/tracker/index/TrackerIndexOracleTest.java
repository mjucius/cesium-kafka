package events.cesium.kafka.store.tracker.index;

import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tuple;

/**
 * Flagship M2 test: random operation sequences executed against the real {@link TrackerIndex}
 * and the naive {@link ReferenceIndex}, with full observable-equivalence checks after every op
 * (pending sets, exact within-partition drain order with cross-partition ties as
 * timestamp-grouped multisets, oldestPending visitation order, I5 trackerAddOffset preservation,
 * penalty- and I4-eligibility-adjusted nextDeadlineMs).
 *
 * <p>Maintenance floors are set very low (8) so rebuilds and sweeps fire constantly inside the
 * generated sequences. Bugs found by this property during development are pinned as
 * {@code @Example} methods below.
 */
class TrackerIndexOracleTest {

    sealed interface Op
            permits AddNew,
                    DupAdd,
                    CompleteKnown,
                    CompleteUnknown,
                    Drain,
                    Penalize,
                    Maintenance,
                    RevokeAssign,
                    SetEligible {}

    record AddNew(int partitionSel, int delayMs, boolean clamped) implements Op {}

    record DupAdd(int partitionSel, int entrySel, int delayMs, boolean clamped) implements Op {}

    record CompleteKnown(int partitionSel, int entrySel) implements Op {}

    record CompleteUnknown(int partitionSel, int offsetSel) implements Op {}

    record Drain(int advanceMs, int maxBatch, boolean commit) implements Op {}

    record Penalize(int partitionSel, int deltaMs) implements Op {}

    record Maintenance() implements Op {}

    record RevokeAssign(int partitionSel) implements Op {}

    record SetEligible(int partitionSel, boolean eligible) implements Op {}

    @Property(tries = 250)
    void indexMatchesReferenceModel(@ForAll("opSequences") List<Op> ops) {
        run(ops);
    }

    private static void run(List<Op> ops) {
        OracleHarness h = new OracleHarness(3);
        for (Op op : ops) {
            apply(h, op);
            h.checkInvariants();
        }
        h.finalDrain();
        h.checkInvariants();
    }

    private static void apply(OracleHarness h, Op op) {
        switch (op) {
            case AddNew(int p, int delay, boolean clamped) -> h.addNew(p, h.nowMs + delay, clamped);
            case DupAdd(int p, int sel, int delay, boolean clamped) -> h.dupAdd(p, sel, h.nowMs + delay, clamped);
            case CompleteKnown(int p, int sel) -> h.completeKnown(p, sel);
            case CompleteUnknown(int p, int sel) -> h.completeUnknown(p, sel);
            case Drain(int advance, int max, boolean commit) -> h.drainAndResolve(advance, max, commit);
            case Penalize(int p, int delta) -> h.penalize(p, h.nowMs + delta);
            case Maintenance() -> h.maintenance();
            case RevokeAssign(int p) -> h.revokeAssign(p);
            case SetEligible(int p, boolean eligible) -> h.setEligible(p, eligible);
        }
    }

    @Provide
    Arbitrary<List<Op>> opSequences() {
        Arbitrary<Integer> partition = Arbitraries.integers().between(0, 2);
        Arbitrary<Integer> selector = Arbitraries.integers().between(0, 1 << 16);
        // Narrow delay range on purpose: dispatchAtMs collisions exercise tie handling.
        Arbitrary<Integer> delay = Arbitraries.integers().between(-100, 400);
        Arbitrary<Boolean> clamped = Arbitraries.of(true, false);

        Arbitrary<Op> addNew = Combinators.combine(partition, delay, clamped).as(AddNew::new);
        Arbitrary<Op> dupAdd =
                Combinators.combine(partition, selector, delay, clamped).as(DupAdd::new);
        Arbitrary<Op> completeKnown = Combinators.combine(partition, selector).as(CompleteKnown::new);
        Arbitrary<Op> completeUnknown = Combinators.combine(partition, selector).as(CompleteUnknown::new);
        Arbitrary<Op> drain = Combinators.combine(
                        Arbitraries.integers().between(0, 400),
                        Arbitraries.integers().between(1, 40),
                        Arbitraries.of(true, false))
                .as(Drain::new);
        Arbitrary<Op> penalize = Combinators.combine(
                        partition, Arbitraries.integers().between(-200, 400))
                .as(Penalize::new);
        Arbitrary<Op> maintenance = Arbitraries.just(new Maintenance());
        Arbitrary<Op> revokeAssign = partition.map(RevokeAssign::new);
        Arbitrary<Op> setEligible =
                Combinators.combine(partition, Arbitraries.of(true, false)).as(SetEligible::new);

        return Arbitraries.frequencyOf(
                        Tuple.of(10, addNew),
                        Tuple.of(3, dupAdd),
                        Tuple.of(4, completeKnown),
                        Tuple.of(1, completeUnknown),
                        Tuple.of(5, drain),
                        Tuple.of(1, penalize),
                        Tuple.of(1, maintenance),
                        Tuple.of(1, revokeAssign),
                        Tuple.of(2, setEligible))
                .list()
                .ofMinSize(1)
                .ofMaxSize(160);
    }

    // ------------------------------------------------------------------------------------------
    // Pinned scenarios. These encode hazards derived analytically while designing DispatchHeap;
    // any future property-test failures get their shrunk sequences appended here as well.
    // ------------------------------------------------------------------------------------------

    /**
     * Pinned hazard: an R1 duplicate-ADD that moves a deadline LATER leaves the stale heap copy
     * positioned high, masking a still-due entry placed beneath it before the update. A naive
     * lazy heap (even with the pop-time re-push of rule b) misses src=3 here; the suspect-rebuild
     * must surface it. See DispatchHeapHazardTest for the single-shard variant.
     */
    @Example
    void duplicateAddMovedLaterMustNotMaskOtherDueEntries() {
        OracleHarness h = new OracleHarness(1);
        long base = h.nowMs;
        h.addNew(0, base + 40);
        h.addNew(0, base + 50); // the entry that will be moved later
        h.addNew(0, base + 130);
        h.addNew(0, base + 80); // masked under the moved entry in a naive heap
        h.addNew(0, base + 135);
        h.addNew(0, base + 140);
        h.addNew(0, base + 145);
        h.checkInvariants();
        h.dupAdd(0, 1, base + 500); // entrySel 1 → second-oldest by trackerAddOffset (the +50 one)
        h.checkInvariants();
        h.drainAndResolve(90, 10, true); // must drain exactly {base+40, base+80}
        h.checkInvariants();
        h.finalDrain();
    }

    /**
     * Pinned jqwik find (shrunk from seed -581934170856112650): seven equal-deadline entries,
     * then a duplicate-ADD moving entry #1 ONE millisecond EARLIER. The fresh duplicate's
     * sift-up stopped at the slot's own equal-valued stale copy stranded beneath a larger
     * parent, so {@code nextDeadlineMs()} over-reported (1_000_000 instead of 999_999) and the
     * moved entry would have dispatched late. Fixed by treating every in-place change behind
     * resident heap copies as order-suspect (rebuild before the next read), not just increases.
     */
    @Example
    void duplicateAddMovedEarlierByOneMsMustLowerNextDeadline() {
        OracleHarness h = new OracleHarness(3);
        for (int i = 0; i < 7; i++) {
            h.addNew(0, h.nowMs);
            h.checkInvariants();
        }
        h.dupAdd(0, 1, h.nowMs - 1);
        h.checkInvariants(); // nextDeadlineMs must report nowMs-1, not nowMs
        h.finalDrain();
        h.checkInvariants();
    }

    /** Abort/restore interleaved with duplicate-ADD reschedules and maintenance. */
    @Example
    void abortRestoreWithReschedulesAndMaintenance() {
        OracleHarness h = new OracleHarness(2);
        long base = h.nowMs;
        for (int i = 0; i < 12; i++) {
            h.addNew(i % 2, base + 10 + i);
            h.checkInvariants();
        }
        h.drainAndResolve(15, 4, false); // abort → restore
        h.checkInvariants();
        h.dupAdd(0, 0, base + 5); // moved earlier
        h.dupAdd(1, 1, base + 600); // moved later
        h.checkInvariants();
        h.maintenance();
        h.checkInvariants();
        h.drainAndResolve(10, 40, true);
        h.checkInvariants();
        h.completeKnown(0, 0);
        h.completeKnown(1, 0);
        h.checkInvariants();
        h.finalDrain();
        h.checkInvariants();
    }

    /** Penalty box interleaved with revoke/re-assign: penalties outlive the shard. */
    @Example
    void penaltySurvivesRevokeAssign() {
        OracleHarness h = new OracleHarness(2);
        h.addNew(0, h.nowMs + 10);
        h.addNew(1, h.nowMs + 20);
        h.penalize(0, h.nowMs + 1000);
        h.checkInvariants();
        h.drainAndResolve(100, 10, true); // only partition 1 may drain
        h.checkInvariants();
        h.revokeAssign(0);
        h.addNew(0, h.nowMs + 5);
        h.checkInvariants(); // partition 0 still penalized: nextDeadline reflects the stamp
        h.drainAndResolve(50, 10, true);
        h.checkInvariants();
        h.penalize(0, 0); // clear by past deadline
        h.checkInvariants();
        h.drainAndResolve(0, 10, true);
        h.checkInvariants();
        h.finalDrain();
    }
}
