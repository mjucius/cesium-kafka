package events.cesium.kafka.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import events.cesium.kafka.api.store.DueBatch;
import events.cesium.kafka.api.store.ExternalSchedulerStore;
import events.cesium.kafka.api.store.ScheduledRef;
import events.cesium.kafka.api.store.StoreCapabilities;
import events.cesium.kafka.api.store.StoreContext;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Executable specification for {@link ExternalSchedulerStore} implementations (design §11.2,
 * enforcing the relevant items of the §4.4 correctness checklist). Subclass with a factory and run:
 * every test here must pass for a conforming external store.
 *
 * <p>No production {@code ExternalSchedulerStore} ships in v1 — the SPI is fixed now so the contract
 * cannot drift, and this class is the executable spec future DB-backed stores are written against.
 * The testkit's own {@code InMemoryExternalSchedulerStore} (test scope) passes it, proving the SPI
 * is implementable in both the at-least-once and effectively-once modes.
 *
 * <p><strong>How to use.</strong> Implement {@link #createStore(StoreContext)} to return a
 * configured, validated, started instance; override {@link #storeProperties()} for any configuration
 * your store requires. The contract builds {@link FakeStoreContext}s with a random route identity
 * per context — pass an existing {@code route()} through the builder (which {@link #sibling} does for
 * you) to model a restart of the <em>same</em> route against the same durable external system.
 *
 * <p><strong>The ordering contracts under test.</strong> External stores cannot enlist their durable
 * writes in the engine's Kafka transactions, so correctness rests on the documented ordering:
 * idempotent {@link ExternalSchedulerStore#upsertScheduled upserts} strictly <em>before</em> the
 * ingest transaction commits, and {@link ExternalSchedulerStore#markDispatched markDispatched}
 * strictly <em>after</em> the dispatch transaction commits. The baseline guarantee is therefore
 * {@link StoreCapabilities.DispatchGuarantee#AT_LEAST_ONCE}: a crash between the dispatch commit and
 * {@code markDispatched} leaves the row pending, so it reappears on recovery and re-dispatches.
 *
 * <p><strong>The effectively-once upgrade (optional).</strong> A store that implements
 * {@link ExternalSchedulerStore#cursorToCommit cursor reconciliation} closes that window: the engine
 * commits the cursor in the offset-metadata channel atomically with the dispatch transaction, and on
 * recovery hands it back so rows at or below the cursor are reconciled as delivered rather than
 * trusted as pending. Because the v1 SPI has no dedicated method for the engine to deliver the
 * committed cursor back to the store at recovery, the contract routes it through
 * {@link #deliverReconciliationCursor} — override that (and {@link #supportsCursorReconciliation()})
 * to unlock the two reconciliation tests; the at-least-once default skips them via an assumption.
 *
 * <p>The contract models a co-located {@code FOLLOW_INGEST_GROUP} instance (the only coordination
 * mode the v1 engine implements): upserts land on a partition the same instance dispatches.
 */
public abstract class ExternalSchedulerStoreContract {

    /** Base instant for all virtual time in the contract. */
    protected static final long T0 = 1_700_000_000_000L;

    // ----------------------------------------------------------------------------------------
    // Implementer surface
    // ----------------------------------------------------------------------------------------

    /**
     * Creates a ready-to-use store: {@code configure(context) → validate() → start()} already
     * performed. Called many times per test run; instances created with the same {@code route()}
     * must share the same durable external state (the restart model), and instances created with
     * different route identities must not.
     */
    protected abstract ExternalSchedulerStore createStore(StoreContext context);

    /** {@code store.properties} merged into every context the contract builds. */
    protected Map<String, String> storeProperties() {
        return Map.of();
    }

    /** Partition count of the routes the contract builds. */
    protected int partitionCount() {
        return 4;
    }

    /**
     * Whether the store under test implements {@link ExternalSchedulerStore#cursorToCommit cursor
     * reconciliation} (the effectively-once upgrade). When {@code true}, also override
     * {@link #deliverReconciliationCursor} so the contract can hand the committed cursor back at
     * recovery; the two reconciliation tests then run instead of being skipped.
     */
    protected boolean supportsCursorReconciliation() {
        return false;
    }

    /**
     * Delivers a reconciliation cursor (read by the engine from the offset-metadata channel it was
     * committed to) back to a freshly assigned, not-yet-scanned store, so the subsequent
     * {@code scanPending} treats rows at or below the cursor as delivered. Override for a
     * reconciliation-capable store, routing the cursor into whatever recovery channel it exposes.
     *
     * @throws UnsupportedOperationException by default — overriding it is required to unlock the
     *     reconciliation tests, which are otherwise skipped via {@link #supportsCursorReconciliation()}
     */
    protected void deliverReconciliationCursor(ExternalSchedulerStore store, int partition, String committedCursor) {
        throw new UnsupportedOperationException(
                "override deliverReconciliationCursor to unlock the cursor-reconciliation tests");
    }

    // ----------------------------------------------------------------------------------------
    // Capability + coordination declaration (design §4.3, §4.5)
    // ----------------------------------------------------------------------------------------

    @Test
    public void capabilitiesDeclareExternalAffinityAndAConsistentGuarantee() {
        StoreCapabilities caps = createStore(contextBuilder().build()).capabilities();
        assertEquals(
                StoreCapabilities.TransactionAffinity.EXTERNAL,
                caps.affinity(),
                "an ExternalSchedulerStore must declare EXTERNAL transaction affinity (design §4.3)");
        if (caps.dispatchGuarantee() == StoreCapabilities.DispatchGuarantee.EXACTLY_ONCE) {
            assertTrue(
                    supportsCursorReconciliation(),
                    "an EXTERNAL store may only claim EXACTLY_ONCE when it reconciles via an"
                            + " offset-metadata cursor (design §4.3 / DispatchGuarantee.EXACTLY_ONCE)");
        }
    }

    @Test
    public void coordinationModeIsDeclared() {
        ExternalSchedulerStore.PartitionCoordination coordination =
                createStore(contextBuilder().build()).coordination();
        assertNotNull(coordination, "coordination() must declare how dispatch ownership is decided (design §4.5)");
        // The v1 engine implements only FOLLOW_INGEST_GROUP; STORE_MANAGED is a reserved, declared
        // intent that a later engine release will honor. Either is a valid declaration.
    }

    // ----------------------------------------------------------------------------------------
    // Upsert / scanPending durable semantics
    // ----------------------------------------------------------------------------------------

    @Test
    public void upsertIsIdempotentKeyedBySourceOffset() {
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 100, T0 + 10));
        // A re-poll after an aborted ingest transaction repeats the upsert with the identical
        // (deterministic) schedule — it must not create a second durable row.
        f.harness().schedule(ref(0, 100, T0 + 10));

        List<ScheduledRef> durable = durablePending(f.store(), 0);
        assertEquals(
                1,
                durable.size(),
                "a duplicate upsert must not create a second row (idempotent on (partition, offset))");
        assertEquals(new ScheduledRef(0, 100, T0 + 10, false), durable.get(0));
    }

    @Test
    public void scanPendingStreamsOnlyTheRequestedPartition() {
        Fixture f = fixture(0, 1);
        f.harness().schedule(ref(0, 1, T0));
        f.harness().schedule(ref(0, 2, T0 + 5));
        f.harness().schedule(ref(1, 9, T0));

        assertEquals(List.of(1L, 2L), partitionAndOffsets(durablePending(f.store(), 0)));
        List<ScheduledRef> partition1 = durablePending(f.store(), 1);
        assertEquals(1, partition1.size(), "scanPending(p) streams only p's pending rows");
        assertEquals(9, partition1.get(0).sourceOffset());
    }

    @Test
    public void markDispatchedExcludesEntriesFromScanPending() {
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0)); // due
        f.harness().schedule(ref(0, 2, T0 + 9_000_000)); // a pin that stays pending

        DueBatch batch = f.store().pollDue(T0, 10);
        assertEquals(1, batch.size());
        assertEquals(1, batch.sourceOffset(0));
        f.harness().commitDispatch(batch); // onBatchCommitted + markDispatched (after commit)

        List<ScheduledRef> durable = durablePending(f.store(), 0);
        assertEquals(1, durable.size(), "markDispatched must remove the settled entry from scanPending");
        assertEquals(2, durable.get(0).sourceOffset(), "the still-pending pin survives");
    }

    @Test
    public void commitWithoutMarkDispatchedLeavesTheRowPending() {
        // The at-least-once window made observable at the durable level: the Kafka dispatch
        // transaction committed, but the after-commit markDispatched did not run. The row stays
        // pending — for ANY external store; cursor reconciliation (tested below) is what closes it.
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0));
        f.harness().schedule(ref(0, 2, T0 + 9_000_000));

        DueBatch batch = f.store().pollDue(T0, 10);
        assertEquals(1, batch.size());
        f.harness().commitDispatchLosingMarkDispatched(batch);

        assertEquals(
                List.of(1L, 2L),
                partitionAndOffsets(durablePending(f.store(), 0)),
                "without markDispatched the settled entry's durable row stays pending (AT_LEAST_ONCE window)");
    }

    // ----------------------------------------------------------------------------------------
    // Dispatch index: pollDue, commit/abort, recovery rebuild, gating
    // ----------------------------------------------------------------------------------------

    @Test
    public void pollDueReturnsOnlyDueEntriesInDueOrder() {
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0 + 100));
        f.harness().schedule(ref(0, 2, T0 + 50));
        f.harness().schedule(ref(0, 3, T0 + 200));
        f.harness().schedule(ref(0, 4, T0 + 150));

        DueBatch due = f.store().pollDue(T0 + 100, 10);
        assertEquals(2, due.size(), "entries beyond nowMs are not due");
        assertEquals(2, due.sourceOffset(0), "the earliest deadline pops first");
        assertEquals(1, due.sourceOffset(1));
        assertEquals(-1, due.trackerOffset(0), "external stores have no tracker offset");
        f.harness().commitDispatch(due);

        assertEquals(T0 + 150, f.store().nextDeadlineMs(), "nextDeadline is the earliest remaining entry");
        DueBatch rest = f.store().pollDue(T0 + 1_000, 10);
        assertEquals(2, rest.size());
        assertEquals(List.of(4L, 3L), batchOffsets(rest));
        f.harness().commitDispatch(rest);
        assertEquals(Long.MAX_VALUE, f.store().nextDeadlineMs());
    }

    @Test
    public void abortedBatchRestoresEntriesToPending() {
        Fixture f = fixture(0);
        for (int i = 1; i <= 3; i++) {
            f.harness().schedule(ref(0, i, T0));
        }
        DueBatch batch = f.store().pollDue(T0, 10);
        assertEquals(3, batch.size());
        f.harness().abortDispatch(batch);

        assertEquals(3, f.store().pendingCount(0), "a definitive abort restores every entry to pending");
        DueBatch again = f.store().pollDue(T0, 10);
        assertEquals(3, again.size(), "restored entries are re-polled");
        f.harness().commitDispatch(again);
        assertEquals(0, f.store().pendingCount(0));
    }

    @Test
    public void recoveryRebuildsTheDispatchIndexFromDurableRows() {
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0));
        f.harness().schedule(ref(0, 2, T0 + 9_000_000)); // pin
        f.harness().schedule(ref(0, 3, T0 + 10));

        DueBatch first = f.store().pollDue(T0 + 100, 10);
        assertEquals(2, first.size());
        f.harness().commitDispatch(first); // 1 and 3 durably settled

        // Crash: memory dies; the external rows survive. A fresh incarnation rebuilds via scanPending.
        Fixture next = sibling(f);
        next.harness().recover(0);
        assertEquals(1, next.store().pendingCount(0), "settled entries must not resurface; the pin must");
        DueBatch pin = next.store().pollDue(Long.MAX_VALUE, 10);
        assertEquals(1, pin.size());
        assertEquals(2, pin.sourceOffset(0));
        next.harness().commitDispatch(pin);
    }

    @Test
    public void upsertWithoutMarkDispatchedReappearsOnPlainRecovery() {
        // The full at-least-once round trip: a crash between commit and markDispatched, recovered
        // WITHOUT applying any reconciliation cursor, re-dispatches the settled entry. This is the
        // baseline external guarantee — reconciliation-capable stores recover differently (below).
        Assumptions.assumeFalse(
                supportsCursorReconciliation(),
                "a reconciliation-capable store recovers through the cursor; see the reconciliation tests");
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0));
        f.harness().schedule(ref(0, 2, T0 + 9_000_000));

        DueBatch batch = f.store().pollDue(T0, 10);
        assertEquals(1, batch.size());
        f.harness().commitDispatchLosingMarkDispatched(batch);

        Fixture next = sibling(f);
        next.harness().recover(0);
        assertEquals(2, next.store().pendingCount(0), "AT_LEAST_ONCE: the unmarked entry reappears and re-dispatches");
        assertEquals(List.of(1L, 2L), partitionAndOffsets(durablePending(next.store(), 0)));
    }

    @Test
    public void anAssignedButUnscannedPartitionContributesNothing() {
        // The store learns of a granted partition before recovery has loaded its index; until it is
        // scanned, it must surface no due entries and drive no poll timeout.
        FakeStoreContext ctx = contextBuilder().build();
        ExternalSchedulerStore store = createStore(ctx);
        ExternalStoreHarness harness = new ExternalStoreHarness(store);
        harness.schedule(ref(0, 1, T0 - 5_000)); // overdue, but partition 0 is not owned yet
        harness.assign(0); // granted, not yet scanned

        assertEquals(0, store.pollDue(T0, 10).size(), "an unscanned partition contributes nothing, however overdue");
        assertEquals(Long.MAX_VALUE, store.nextDeadlineMs(), "an unscanned partition drives no poll timeout");

        harness.scan(0); // recovery loads the index
        DueBatch batch = store.pollDue(T0, 10);
        assertEquals(1, batch.size());
        assertEquals(1, batch.sourceOffset(0));
        harness.commitDispatch(batch);
    }

    @Test
    public void clampedMarkerSurvivesTheDurableRoundTrip() {
        Fixture f = fixture(0);
        f.harness().schedule(refClamped(0, 1, T0));
        f.harness().schedule(ref(0, 2, T0));

        DueBatch batch = f.store().pollDue(T0, 10);
        assertEquals(2, batch.size());
        assertTrue(batch.clamped(0), "the CLAMP marker must survive upsert → external store → index → batch");
        assertFalse(batch.clamped(1));
        f.harness().abortDispatch(batch);

        // ... and across a recovery.
        Fixture next = sibling(f);
        next.harness().recover(0);
        DueBatch recovered = next.store().pollDue(T0, 10);
        assertEquals(2, recovered.size());
        assertTrue(recovered.clamped(0), "CLAMP must survive scanPending recovery too");
        assertFalse(recovered.clamped(1));
        next.harness().commitDispatch(recovered);
    }

    // ----------------------------------------------------------------------------------------
    // Partition lifecycle: revoke, lost
    // ----------------------------------------------------------------------------------------

    @Test
    public void revokeDropsInMemoryStateButKeepsDurableRows() {
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0));
        f.harness().schedule(ref(0, 2, T0 + 9_000_000));
        assertEquals(2, f.store().pendingCount(0));

        f.store().onPartitionsRevoked(Set.of(0));
        assertEquals(
                0, f.store().pendingCount(0), "revoke drops in-memory state in O(1): the external system is the truth");
        assertEquals(2, durablePending(f.store(), 0).size(), "durable rows survive revocation");

        f.store().onPartitionsAssigned(Set.of(0));
        f.harness().scan(0);
        assertEquals(2, f.store().pendingCount(0), "re-assignment rebuilds from durable rows");
        DueBatch batch = f.store().pollDue(T0, 10);
        f.harness().commitDispatch(batch);
    }

    @Test
    public void lostPartitionsAreDroppedAndAbandonInFlightWorkToRecovery() {
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0));
        f.harness().schedule(ref(0, 2, T0));
        DueBatch inFlight = f.store().pollDue(T0, 10);
        assertEquals(2, inFlight.size());

        // Fenced mid-dispatch: drop, never markDispatched. The in-flight entries were never settled
        // durably, so a new owner re-dispatches them (at-least-once on an unclean rebalance).
        f.store().onPartitionsLost(Set.of(0));
        assertEquals(0, f.store().pendingCount(0));
        assertEquals(2, durablePending(f.store(), 0).size(), "lost is a drop, never a durable settle");

        f.store().onPartitionsAssigned(Set.of(0));
        f.harness().scan(0);
        assertEquals(2, f.store().pendingCount(0), "the abandoned in-flight batch resurfaces");
    }

    // ----------------------------------------------------------------------------------------
    // Cursor reconciliation (effectively-once upgrade) — gated
    // ----------------------------------------------------------------------------------------

    @Test
    public void cursorToCommitProducesANonEmptyReconciliationCursor() {
        Assumptions.assumeTrue(supportsCursorReconciliation(), "store does not implement cursor reconciliation");
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0));
        f.harness().schedule(ref(0, 2, T0 + 10));
        DueBatch batch = f.store().pollDue(T0 + 100, 10);
        assertEquals(2, batch.size());

        Optional<String> cursor = f.store().cursorToCommit(0, batch);
        assertTrue(
                cursor.isPresent() && !cursor.orElseThrow().isEmpty(),
                "a reconciliation-capable store must return a non-empty cursor for a settled batch");
        f.harness().commitDispatch(batch);
    }

    @Test
    public void cursorReconciliationExcludesDeliveredRowsAfterACrash() {
        Assumptions.assumeTrue(supportsCursorReconciliation(), "store does not implement cursor reconciliation");
        Fixture f = fixture(0);
        f.harness().schedule(ref(0, 1, T0)); // due
        f.harness().schedule(ref(0, 2, T0 + 10)); // due
        f.harness().schedule(ref(0, 3, T0 + 9_000_000)); // pin

        DueBatch batch = f.store().pollDue(T0 + 100, 10);
        assertEquals(2, batch.size(), "entries 1 and 2 dispatch in total order");
        // The dispatch transaction (incl. the offset-metadata cursor) commits; the process then dies
        // before markDispatched — exactly the at-least-once window.
        f.harness().commitDispatchLosingMarkDispatched(batch);
        String committed = f.harness().committedCursor(0).orElseThrow();

        // Recovery: the engine reads the committed cursor from the offset-metadata channel and hands
        // it back before scanning. Rows at or below it are reconciled as delivered.
        Fixture next = sibling(f);
        next.harness().assign(0);
        deliverReconciliationCursor(next.store(), 0, committed);
        List<ScheduledRef> durable = durablePending(next.store(), 0);
        assertEquals(
                List.of(new ScheduledRef(0, 3, T0 + 9_000_000, false)),
                durable,
                "effectively-once: rows <= cursor are reconciled as delivered, not re-dispatched");

        DueBatch rest = next.store().pollDue(Long.MAX_VALUE, 10);
        assertEquals(1, rest.size(), "only the un-reconciled pin remains dispatch-eligible");
        assertEquals(3, rest.sourceOffset(0));
        next.harness().commitDispatch(rest);
    }

    // ----------------------------------------------------------------------------------------
    // Shared plumbing
    // ----------------------------------------------------------------------------------------

    /** Context builder pre-loaded with the contract's partition count and the store's properties. */
    protected FakeStoreContext.Builder contextBuilder() {
        return FakeStoreContext.builder().partitions(partitionCount()).properties(storeProperties());
    }

    /** An unclamped {@link ScheduledRef}. */
    protected static ScheduledRef ref(int partition, long sourceOffset, long dispatchAtMs) {
        return new ScheduledRef(partition, sourceOffset, dispatchAtMs, false);
    }

    /** A clamped {@link ScheduledRef}. */
    protected static ScheduledRef refClamped(int partition, long sourceOffset, long dispatchAtMs) {
        return new ScheduledRef(partition, sourceOffset, dispatchAtMs, true);
    }

    private Fixture fixture(int... partitions) {
        FakeStoreContext ctx = contextBuilder().build();
        ExternalSchedulerStore store = createStore(ctx);
        ExternalStoreHarness harness = new ExternalStoreHarness(store);
        for (int partition : partitions) {
            harness.recover(partition);
        }
        return new Fixture(ctx, store, harness);
    }

    /** A fresh incarnation of the same route: new store, shared durable external state. */
    private Fixture sibling(Fixture previous) {
        FakeStoreContext ctx =
                contextBuilder().route(previous.context().route()).build();
        ExternalSchedulerStore store = createStore(ctx);
        return new Fixture(ctx, store, previous.harness().migrateTo(store));
    }

    /** Durable pending rows for {@code partition}, sorted by source offset (a pure read for assertions). */
    private static List<ScheduledRef> durablePending(ExternalSchedulerStore store, int partition) {
        List<ScheduledRef> collected = new ArrayList<>();
        store.scanPending(partition, collected::add);
        collected.sort(Comparator.comparingLong(ScheduledRef::sourceOffset));
        return collected;
    }

    private static List<Long> partitionAndOffsets(List<ScheduledRef> refs) {
        List<Long> offsets = new ArrayList<>(refs.size());
        for (ScheduledRef ref : refs) {
            offsets.add(ref.sourceOffset());
        }
        return offsets;
    }

    private static List<Long> batchOffsets(DueBatch batch) {
        List<Long> offsets = new ArrayList<>(batch.size());
        for (int i = 0; i < batch.size(); i++) {
            offsets.add(batch.sourceOffset(i));
        }
        return offsets;
    }

    private record Fixture(FakeStoreContext context, ExternalSchedulerStore store, ExternalStoreHarness harness) {}
}
