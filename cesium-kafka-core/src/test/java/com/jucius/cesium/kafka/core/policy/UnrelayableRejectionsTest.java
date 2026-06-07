package com.jucius.cesium.kafka.core.policy;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * The §3.8 I-8 permanent-record-rejection classifier: exactly the record-scoped, non-retriable
 * destination rejections route to the {@code on-unrelayable} policy; every transient / broker-
 * availability error stays on the retry path so a blip never drops a message.
 */
class UnrelayableRejectionsTest {

    @Test
    void recordTooLargeIsPermanent() {
        assertTrue(UnrelayableRejections.isPermanentRecordRejection(new RecordTooLargeException("too large")));
    }

    @Test
    void invalidRecordIsPermanent() {
        assertTrue(UnrelayableRejections.isPermanentRecordRejection(new InvalidRecordException("invalid")));
    }

    @Test
    void permanentRejectionIsFoundThroughTheCauseChain() {
        // Clients wrap record-level errors; the commit surfaces a wrapping KafkaException.
        KafkaException wrapped = new KafkaException("commit failed", new RecordTooLargeException("too large"));
        assertTrue(UnrelayableRejections.isPermanentRecordRejection(wrapped));
    }

    @Test
    void transientAndAvailabilityErrorsAreNotPermanent() {
        // Misclassifying any of these as permanent would DROP/DLQ a message on a recoverable blip.
        assertFalse(UnrelayableRejections.isPermanentRecordRejection(new TimeoutException("timed out")));
        assertFalse(UnrelayableRejections.isPermanentRecordRejection(new NotEnoughReplicasException("under-min ISR")));
        assertFalse(UnrelayableRejections.isPermanentRecordRejection(new NetworkException("connection reset")));
        assertFalse(UnrelayableRejections.isPermanentRecordRejection(new CorruptRecordException("retriable")));
        assertFalse(UnrelayableRejections.isPermanentRecordRejection(new KafkaException("generic")));
        assertFalse(UnrelayableRejections.isPermanentRecordRejection(null));
    }
}
