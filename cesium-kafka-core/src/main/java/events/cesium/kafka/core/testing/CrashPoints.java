package events.cesium.kafka.core.testing;

import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

/**
 * The crash-point test seam of design §3.9 / §11.3-2: engine loops call
 * {@link #maybeFire(String)} at the named failure-matrix points, and tests install a handler that
 * observes the point or throws {@link SimulatedCrash} to kill the loop exactly there.
 *
 * <p><strong>Production cost:</strong> one volatile read per point — nothing else. No handler is
 * ever installed outside tests, and {@link #install(Consumer)} refuses unless the
 * {@value #ENABLE_PROPERTY} system property is {@code true} — {@code core} is a published
 * artifact (§10), so without the guard any dependency on an embedder's classpath could arm crash
 * points in a production engine. The guard is checked only at install time; {@link #maybeFire}'s
 * hot path is unchanged.
 *
 * <p><strong>Crash simulation contract.</strong> A handler simulating a crash must throw
 * {@link SimulatedCrash} (an {@link Error}), never a {@code RuntimeException}: the loops'
 * error-taxonomy handlers (§3.8) deliberately catch {@code RuntimeException} to classify
 * broker/transport failures, and a runtime exception thrown <em>after</em> a successful commit
 * (point {@link #INGEST_DURING_COMMIT}) would be misread as an abortable failure of an already
 * committed transaction. {@code SimulatedCrash} passes through every taxonomy handler untouched,
 * like the SIGKILL it stands in for.
 *
 * <p><strong>Point {@code I-4} semantics.</strong> {@code commitTransaction} cannot literally be
 * interrupted in-process, so {@link #INGEST_DURING_COMMIT} fires <em>after</em>
 * {@code commitTransaction} returns but <em>before any local bookkeeping</em> — simulating the
 * §3.9 I-4 state of not knowing the outcome: the broker has resolved the transaction, the process
 * has recorded nothing.
 */
public final class CrashPoints {

    /** §3.9 I-1: before {@code beginTransaction} — offsets unmoved, nothing produced. */
    public static final String INGEST_BEFORE_BEGIN = "I-1";

    /** §3.9 I-2: after the batch's sends, before {@code sendOffsetsToTransaction}. */
    public static final String INGEST_AFTER_SENDS = "I-2";

    /** §3.9 I-3: after {@code sendOffsetsToTransaction}, before {@code commitTransaction}. */
    public static final String INGEST_AFTER_SEND_OFFSETS = "I-3";

    /**
     * §3.9 I-4: "during" {@code commitTransaction} — fired after the commit call returns but
     * before any local bookkeeping (see class javadoc for why this is the closest in-process
     * approximation).
     */
    public static final String INGEST_DURING_COMMIT = "I-4";

    /**
     * §3.9 D-1: before {@code beginTransaction} — after the payload fetch, before anything is
     * produced. A crash here loses only in-memory state; replay rebuilds it.
     */
    public static final String DISPATCH_BEFORE_BEGIN = "D-1";

    /**
     * §3.9 D-2: after the destination + COMPLETE sends and the cursor offsets, before
     * {@code commitTransaction}. A crash here aborts the transaction: the tombstones are
     * invisible, replay shows the entries pending, and they re-dispatch as the single committed
     * copy.
     */
    public static final String DISPATCH_AFTER_SENDS = "D-2";

    /**
     * §3.9 D-3: "during" {@code commitTransaction} — fired after the commit call returns but
     * before any local bookkeeping (the dispatch analog of {@link #INGEST_DURING_COMMIT}).
     */
    public static final String DISPATCH_DURING_COMMIT = "D-3";

    /**
     * The system property that must be {@code true} before {@link #install(Consumer)} will arm
     * the seam (e.g. {@code -Dcesium.testing.crashpoints=true} on the test JVM).
     */
    public static final String ENABLE_PROPERTY = "cesium.testing.crashpoints";

    // Write-once-from-tests volatile; null IS the steady state. Reads go through a local copy.
    private static volatile @Nullable Consumer<String> handler = null;

    private CrashPoints() {}

    /**
     * Installs the test handler. Tests must {@link #reset()} in an {@code @AfterEach}/finally —
     * the seam is global static state by design (the loops it instruments are wired deep behind
     * constructors that real crash tests cannot reach).
     *
     * @throws IllegalStateException unless the {@value #ENABLE_PROPERTY} system property is
     *     {@code true} — the guard keeps a published-artifact dependency from arming crash points
     *     in a production engine
     */
    public static void install(Consumer<String> testHandler) {
        if (!Boolean.getBoolean(ENABLE_PROPERTY)) {
            throw new IllegalStateException("CrashPoints.install refused: the crash-point seam is test-only; set -D"
                    + ENABLE_PROPERTY + "=true on the test JVM to arm it");
        }
        handler = testHandler;
    }

    /** Removes any installed handler; safe to call when none is installed. */
    public static void reset() {
        handler = null;
    }

    /** Fires the point: a no-op unless a test installed a handler. */
    public static void maybeFire(String id) {
        Consumer<String> h = handler;
        if (h != null) {
            h.accept(id);
        }
    }

    /**
     * The only throwable a crash-simulating handler may use: an {@link Error} so it passes
     * untouched through the loops' {@code RuntimeException} taxonomy handlers, exactly like the
     * process death it simulates.
     */
    public static final class SimulatedCrash extends Error {
        /** @param crashPointId the point being simulated, for diagnostics */
        public SimulatedCrash(String crashPointId) {
            super("simulated crash at point " + crashPointId);
        }
    }
}
