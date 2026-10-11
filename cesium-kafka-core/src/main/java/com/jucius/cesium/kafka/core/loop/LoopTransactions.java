package com.jucius.cesium.kafka.core.loop;

import static com.jucius.cesium.kafka.core.loop.LoopSupport.anyCause;

import com.jucius.cesium.kafka.core.loop.LoopSupport.InDoubtCommit;
import com.jucius.cesium.kafka.core.loop.LoopSupport.SourceCoord;
import com.jucius.cesium.kafka.core.loop.LoopSupport.UnrelayableHit;
import com.jucius.cesium.kafka.core.policy.UnrelayableRejections;
import com.jucius.cesium.kafka.core.testing.CrashPoints;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TransactionAbortableException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;

/**
 * One loop's transactional producer and the transaction mechanics the ingest and dispatch loops
 * share (design §3.8): fire-and-forget sends whose asynchronous failures surface before the offsets
 * are sent, a commit retried to a definitive outcome or declared in-doubt, abort-or-replace, and the
 * producer replacement of the in-doubt procedure. Each loop keeps its own fatal classification.
 *
 * <p>Loop-thread confined, except the send callback, which runs on the producer I/O thread and
 * writes only the volatile send error and the concurrent rejection queue.
 */
public final class LoopTransactions {

    private final Supplier<Producer<byte[], byte[]>> producerFactory;
    private final int commitRetryLimit;
    private final String commitCrashPoint;
    private final Predicate<RuntimeException> isFatal;
    private final String loop;
    private final MeterRegistry registry;
    private final Logger log;
    private final Counter committedTransactions;
    private final Timer commitTimer;

    /**
     * Permanent destination rejections attributed to a specific source record by the send callback
     * (§3.8 I-8). Written from the producer I/O thread, drained on the loop thread once the
     * transaction's outcome is known; reset at every {@link #begin()}.
     */
    private final ConcurrentLinkedQueue<UnrelayableHit> unrelayableHits = new ConcurrentLinkedQueue<>();

    private Producer<byte[], byte[]> producer;
    private boolean inFlight;

    // Single-writer-per-transaction volatile; null is the steady state.
    private volatile @Nullable Exception asyncSendError;

    /**
     * @param loop the {@code loop} metric tag ({@code ingest} or {@code dispatch})
     * @param producerFactory creates the transactional producer — called once here and again
     *     whenever a failed abort or an in-doubt commit forces a replacement; each returned producer
     *     must be freshly constructed and not yet initialized for transactions
     * @param commitRetryLimit {@code commitTransaction} attempts before the outcome is in-doubt
     * @param commitCrashPoint the crash point fired after the commit returns (I-4 / D-3)
     * @param isFatal the loop's §3.8 fatal classification
     */
    public LoopTransactions(
            String loop,
            Supplier<Producer<byte[], byte[]>> producerFactory,
            int commitRetryLimit,
            String commitCrashPoint,
            Predicate<RuntimeException> isFatal,
            MeterRegistry registry,
            Logger log) {
        this.loop = loop;
        this.producerFactory = Objects.requireNonNull(producerFactory, "producerFactory");
        this.commitRetryLimit = commitRetryLimit;
        this.commitCrashPoint = commitCrashPoint;
        this.isFatal = isFatal;
        this.registry = registry;
        this.log = log;
        this.producer = Objects.requireNonNull(producerFactory.get(), "producerFactory.get()");
        this.committedTransactions =
                registry.counter("cesium.transactions", "loop", loop, "result", "committed", "cause", "none");
        this.commitTimer = Timer.builder("cesium.txn.commit.seconds")
                .tag("loop", loop)
                .description("commitTransaction latency per attempt (§9)")
                .register(registry);
    }

    /** Fences this producer against any predecessor (stable transactional id, D10). */
    public void initTransactions() {
        producer.initTransactions();
    }

    /** Opens a transaction, clearing the previous one's send failures. */
    public void begin() {
        asyncSendError = null;
        unrelayableHits.clear();
        producer.beginTransaction();
        inFlight = true;
    }

    /** Fire-and-forget transactional send; async failures are captured and surfaced pre-commit. */
    public void send(ProducerRecord<byte[], byte[]> record) {
        send(record, null);
    }

    /**
     * Fire-and-forget transactional send. When {@code attribution} is non-null and the send is
     * <em>permanently</em> rejected by the destination (record too large / invalid — §3.8 I-8), the
     * failure is recorded against that source record so the loop can route it to the
     * {@code route.relay.on-unrelayable} policy on the retry instead of relaying it forever. Every
     * async failure also sets the send error {@link #throwIfSendFailed} surfaces.
     */
    public void send(ProducerRecord<byte[], byte[]> record, @Nullable SourceCoord attribution) {
        // The future is deliberately unused: completion errors are observed via the callback (and
        // the commit itself would surface them); blocking per record would serialize the batch.
        var unused = producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                return;
            }
            if (asyncSendError == null) {
                asyncSendError = exception;
            }
            if (attribution != null && UnrelayableRejections.isPermanentRecordRejection(exception)) {
                unrelayableHits.add(new UnrelayableHit(attribution, String.valueOf(exception)));
            }
        });
    }

    /** Surfaces an asynchronously failed send before the offsets are sent. */
    public void throwIfSendFailed() {
        Exception error = asyncSendError;
        if (error == null) {
            return;
        }
        if (error instanceof RuntimeException runtime) {
            throw runtime;
        }
        throw new KafkaException("transactional send failed asynchronously", error);
    }

    /** I2's fencing hook: the offsets ride the transaction with freshly fetched group metadata. */
    public void sendOffsets(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) {
        producer.sendOffsetsToTransaction(offsets, groupMetadata);
    }

    /**
     * Commits, retrying ambiguous timeouts to a definitive outcome bounded by
     * {@code kafka.transactions.commit-retry} (§3.8 in-doubt step a). A normal return is
     * definitively committed. {@link TransactionAbortableException} anywhere in the chain is the
     * broker affirming the transaction did NOT commit (KIP-890), so it is rethrown for the abortable
     * path even after an ambiguous attempt. Any other non-fatal failure after an ambiguous timeout
     * cannot prove "aborted" (D20) and escalates to {@link InDoubtCommit}; fatal classifications
     * fail the loop with no restore, which satisfies I9. Fires the commit crash point after the
     * commit returns, before any bookkeeping.
     *
     * @throws InDoubtCommit when the outcome stayed ambiguous
     */
    public void commit() {
        int attempts = 0;
        boolean ambiguous = false;
        while (true) {
            try {
                commitTimer.record(producer::commitTransaction);
                CrashPoints.maybeFire(commitCrashPoint);
                inFlight = false;
                committedTransactions.increment();
                return;
            } catch (TimeoutException timeout) {
                attempts++;
                ambiguous = true;
                if (attempts >= commitRetryLimit) {
                    throw new InDoubtCommit(attempts, timeout);
                }
                log.warn(
                        "commitTransaction timed out (attempt {}/{}); retrying to a definitive outcome",
                        attempts,
                        commitRetryLimit,
                        timeout);
            } catch (RuntimeException e) {
                if (anyCause(e, TransactionAbortableException.class)) {
                    throw e; // the broker affirms "not committed": abortable even post-ambiguity
                }
                if (!ambiguous || isFatal.test(e)) {
                    // First-attempt non-timeout failures are non-ambiguous (§3.8 row 1); fatal
                    // failures always fail the loop and never restore.
                    throw e;
                }
                log.warn(
                        "commitTransaction failed non-fatally after an ambiguous timeout (attempt {}); the outcome"
                                + " cannot be proven aborted — escalating to the in-doubt procedure (D20/I9)",
                        attempts + 1,
                        e);
                throw new InDoubtCommit(attempts + 1, e);
            }
        }
    }

    /** True while a transaction is open. */
    public boolean inFlight() {
        return inFlight;
    }

    /** True when the last transaction had at least one permanent destination rejection. */
    public boolean hasUnrelayableHits() {
        return !unrelayableHits.isEmpty();
    }

    /** Drains the permanent destination rejections recorded since the last {@link #begin()}. */
    public List<UnrelayableHit> drainUnrelayableHits() {
        List<UnrelayableHit> hits = new ArrayList<>();
        for (UnrelayableHit hit; (hit = unrelayableHits.poll()) != null; ) {
            hits.add(hit);
        }
        return hits;
    }

    /**
     * Aborts the open transaction if one is in flight. Nothing committed (the commit was never
     * reached, or failed non-ambiguously), so a failed abort is safe to resolve by replacing the
     * producer: the replacement's {@code initTransactions()} aborts the dangling transaction.
     */
    public void abortIfInFlight() {
        if (!inFlight) {
            return;
        }
        inFlight = false;
        try {
            producer.abortTransaction();
        } catch (RuntimeException abortFailure) {
            log.warn("abortTransaction failed; replacing producer", abortFailure);
            replaceProducer();
        }
    }

    /**
     * Counts an in-doubt commit and forgets the transaction: the I9 procedure that follows must
     * never restore it from memory.
     */
    public void markInDoubt(InDoubtCommit inDoubt) {
        inFlight = false;
        String cause =
                inDoubt.failure instanceof TimeoutException ? "commit_timeout" : LoopSupport.causeTag(inDoubt.failure);
        registry.counter("cesium.transactions", "loop", loop, "result", "in_doubt", "cause", cause)
                .increment();
    }

    /**
     * Hard-closes the current (unusable or half-initialized) producer and builds and fences a
     * fresh one — {@code initTransactions()} resolves the dangling transaction broker-side
     * without reporting its outcome.
     *
     * @throws RuntimeException when the replacement cannot be built or fenced; the caller classifies
     */
    public void replaceProducer() {
        try {
            producer.close(Duration.ZERO);
        } catch (RuntimeException closeFailure) {
            log.warn("closing the unusable producer failed; continuing with replacement", closeFailure);
        }
        producer = Objects.requireNonNull(producerFactory.get(), "producerFactory.get()");
        producer.initTransactions();
    }

    /** Counter for an aborted transaction with the given cause tag. */
    public Counter aborted(String cause) {
        return registry.counter("cesium.transactions", "loop", loop, "result", "aborted", "cause", cause);
    }

    /** Live partition metadata for {@code topic}, through the producer's metadata cache. */
    public List<PartitionInfo> partitionsFor(String topic) {
        return producer.partitionsFor(topic);
    }

    /** Closes the producer within {@code timeout}; a failure is logged, never thrown. */
    public void close(Duration timeout) {
        try {
            producer.close(timeout);
        } catch (RuntimeException e) {
            log.warn("producer close failed", e);
        }
    }
}
