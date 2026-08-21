package com.jucius.cesium.kafka.core.policy;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.jspecify.annotations.Nullable;

/**
 * Classifies a produce-send failure as a <em>permanent, record-level</em> destination rejection —
 * the {@link UnrelayablePolicy} trigger (design §3.8, M5-verified against kafka-clients 4.3.0).
 *
 * <p>The taxonomy is deliberately narrow: only failures that are intrinsic to the offending record
 * and that <em>no</em> retry can clear qualify, so the {@code on-unrelayable} policy never drops a
 * message on a transient blip.
 *
 * <ul>
 *   <li>{@link RecordTooLargeException} — the serialized relay (value + key + headers, including
 *       the appended provenance headers) exceeds the producer's {@code max.request.size}
 *       (client-side, surfaced synchronously from {@code send}) or the destination topic's
 *       {@code max.message.bytes} (broker {@code MESSAGE_TOO_LARGE}, surfaced asynchronously via
 *       the send callback / at flush/commit). Record-scoped and permanent.
 *   <li>{@link InvalidRecordException} — the broker rejected the record itself
 *       ({@code INVALID_RECORD}). Record-scoped and permanent.
 * </ul>
 *
 * <p><strong>Deliberately excluded</strong> (transient / not record-scoped — they stay on the
 * abort-retry / park-and-degrade path of §3.8 so a destination blip never drops a message):
 * {@code CorruptRecordException} (retriable {@code CORRUPT_MESSAGE}), {@code NotEnoughReplicas*},
 * {@code NotLeaderOrFollower}, {@code Timeout}, {@code NetworkException}, and every other broker-
 * availability error.
 */
public final class UnrelayableRejections {

    private UnrelayableRejections() {}

    /**
     * Walks the cause chain (kafka-clients wraps record-level errors under {@code KafkaException})
     * and returns true when a permanent, record-level destination rejection is present.
     *
     * @param error the failure surfaced by a send callback, {@code commitTransaction}, or
     *     {@code sendOffsetsToTransaction}; {@code null} is treated as "not a rejection"
     */
    // ReferenceEquality: `t.getCause() == t` is the self-referential-cause guard — a Throwable
    // whose getCause() returns itself would loop forever. Identity is the intended test, and Error
    // Prone's suggested .equals() is wrong (Throwable does not override it). Flagged from 2.50.0.
    @SuppressWarnings("ReferenceEquality")
    public static boolean isPermanentRecordRejection(@Nullable Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof RecordTooLargeException || t instanceof InvalidRecordException) {
                return true;
            }
        }
        return false;
    }
}
