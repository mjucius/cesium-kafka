package events.cesium.kafka.store.tracker;

import events.cesium.kafka.api.store.CompletionReason;
import events.cesium.kafka.api.store.TrackerRecordData;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jspecify.annotations.Nullable;

/**
 * The versioned binary wire format of the tracker topic (design §2.2, D15) — owned by
 * {@code KafkaTrackerStore}, opaque to the engine.
 *
 * <ul>
 *   <li><strong>Key</strong> (8 bytes): the source offset, big-endian {@code long}. Unique forever
 *       per partition (offsets never recycle) — the compaction identity. Partition identity is
 *       implicit: a tracker record always lands on the tracker partition matching its source
 *       partition.
 *   <li><strong>ADD value</strong> (12 bytes): {@code magic 0xC5 | version 0x01 | type 0x01=ADD |
 *       flags(1) | dispatchAtMs (int64 BE)}. Flag bit 0 is {@link #FLAG_CLAMPED}; unknown flag
 *       bits are <em>ignored</em> on decode (forward compatibility). Type {@code 0x02 CANCEL} is
 *       reserved and decodes to {@link TrackerDecodeResult.Cancel}.
 *   <li><strong>COMPLETE</strong>: a <em>null-value tombstone</em> (compaction requires null
 *       values); the {@link CompletionReason} travels in the {@link #COMPLETION_REASON_HEADER}
 *       record header, because headers survive where values cannot.
 * </ul>
 *
 * <p><strong>Validation posture.</strong> {@link #decode(byte[], byte[], Headers)} is total and
 * never throws: violations yield {@link TrackerDecodeResult.Invalid} so the replay loop can
 * count-and-skip (design §2.2). Unknown <em>versions</em> are rejected; unknown <em>flags</em> are
 * ignored (design §2.2 forward-compat rules); an unknown <em>completion reason</em> on a
 * structurally valid tombstone still completes
 * ({@link TrackerDecodeResult.CompleteUnknownReason}) — skipping it would convert a future
 * writer's new reason constant into a duplicate dispatch on older readers. This replaces the
 * PoC's unversioned sign-negation hack.
 *
 * <p><strong>Why CLAMPED is on the wire.</strong> The ingest-time {@code delay.on-over-max: CLAMP}
 * decision obliges the relay to be stamped {@code cesium-clamped: true} (design §2.3), but the
 * relay happens at dispatch — possibly on a different instance after a rebalance or restart, with
 * the in-memory index rebuilt purely from this format. The flags byte is the only carrier that
 * survives that round trip.
 */
public final class TrackerWireFormat {

    /** First value byte of every non-tombstone tracker record. */
    public static final byte MAGIC = (byte) 0xC5;

    /** The only wire-format version this codec reads or writes; other versions are rejected. */
    public static final byte VERSION = 0x01;

    /** Value type: schedule the keyed source offset for dispatch. */
    public static final byte TYPE_ADD = 0x01;

    /** Value type reserved for a future cancellation API (D15); never written in v1. */
    public static final byte TYPE_CANCEL = 0x02;

    /**
     * Flags bit 0: the ingest {@code delay.on-over-max: CLAMP} policy pinned the dispatch instant
     * to {@code now + delay.max}; dispatch must stamp {@code cesium-clamped: true} on the relay
     * (design §2.3).
     */
    public static final int FLAG_CLAMPED = 0x01;

    /** Exact key length: one big-endian {@code long} source offset. */
    public static final int KEY_LENGTH = 8;

    /** Exact v1 value length: magic, version, type, flags, then the 8-byte dispatch instant. */
    public static final int V1_VALUE_LENGTH = 12;

    /**
     * Header carrying the {@link CompletionReason} on completion tombstones; the value is the
     * reason's {@linkplain Enum#name() name} in US-ASCII bytes (D15).
     */
    public static final String COMPLETION_REASON_HEADER = "cesium-completion-reason";

    private static final int FLAGS_OFFSET = 3;
    private static final int DISPATCH_AT_OFFSET = 4;

    private static final CompletionReason[] REASONS = CompletionReason.values();

    private TrackerWireFormat() {}

    /** Encodes a source offset as the 8-byte big-endian record key (the compaction identity). */
    public static byte[] encodeKey(long sourceOffset) {
        byte[] key = new byte[KEY_LENGTH];
        ByteBuffer.wrap(key).putLong(sourceOffset);
        return key;
    }

    /**
     * Decodes the source offset from a record key. Callers on the replay path must validate via
     * {@link #decode(byte[], byte[], Headers)} first (which length-checks the key); a wrong-length
     * key here is a programming error, not wire input.
     *
     * @throws IllegalArgumentException if the key is not exactly {@value #KEY_LENGTH} bytes
     */
    public static long decodeKey(byte[] key) {
        if (key.length != KEY_LENGTH) {
            throw new IllegalArgumentException("tracker key must be " + KEY_LENGTH + " bytes, got " + key.length);
        }
        return ByteBuffer.wrap(key).getLong();
    }

    /**
     * Encodes the 12-byte v1 ADD value.
     *
     * @param dispatchAtMs the instant the entry is due, epoch milliseconds UTC
     * @param clamped whether the ingest {@code CLAMP} policy produced this schedule; sets
     *     {@link #FLAG_CLAMPED} so dispatch can stamp {@code cesium-clamped} on the relay
     */
    public static byte[] encodeAddValue(long dispatchAtMs, boolean clamped) {
        byte[] value = new byte[V1_VALUE_LENGTH];
        ByteBuffer.wrap(value)
                .put(MAGIC)
                .put(VERSION)
                .put(TYPE_ADD)
                .put(clamped ? (byte) FLAG_CLAMPED : 0)
                .putLong(dispatchAtMs);
        return value;
    }

    /**
     * Encodes a complete ADD record ready for the engine's transactional producer: 8-byte key,
     * 12-byte value, no headers.
     */
    public static TrackerRecordData encodeAdd(long sourceOffset, long dispatchAtMs, boolean clamped) {
        return new TrackerRecordData(
                encodeKey(sourceOffset), encodeAddValue(dispatchAtMs, clamped), new RecordHeaders());
    }

    /**
     * Encodes a completion record: a null-value <em>tombstone</em> (compaction requirement) whose
     * {@link CompletionReason} rides in the {@link #COMPLETION_REASON_HEADER} header (D15).
     */
    public static TrackerRecordData encodeCompletion(long sourceOffset, CompletionReason reason) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(COMPLETION_REASON_HEADER, reason.name().getBytes(StandardCharsets.US_ASCII));
        return new TrackerRecordData(encodeKey(sourceOffset), null, headers);
    }

    /**
     * Decodes one tracker record. Total and non-throwing: wire-format violations return
     * {@link TrackerDecodeResult.Invalid} for count-and-skip (design §2.2). A {@code null} value is
     * the tombstone path; the source offset itself is extracted separately via
     * {@link #decodeKey(byte[])} once the record is known valid.
     *
     * @param key the record key (validated to be exactly {@value #KEY_LENGTH} bytes)
     * @param value the record value, or {@code null} for a completion tombstone
     * @param headers the record headers (consulted only on the tombstone path)
     */
    public static TrackerDecodeResult decode(byte[] key, byte @Nullable [] value, Headers headers) {
        if (key.length != KEY_LENGTH) {
            return new TrackerDecodeResult.Invalid(
                    "bad key length: expected " + KEY_LENGTH + " bytes, got " + key.length);
        }
        if (value == null) {
            return decodeTombstone(headers);
        }
        if (value.length < 2) {
            return new TrackerDecodeResult.Invalid(
                    "truncated value: " + value.length + " byte(s), cannot read magic and version");
        }
        if (value[0] != MAGIC) {
            return new TrackerDecodeResult.Invalid(
                    "bad magic: expected 0xC5, got 0x" + String.format("%02X", value[0]));
        }
        if (value[1] != VERSION) {
            return new TrackerDecodeResult.Invalid(
                    "unsupported version: 0x" + String.format("%02X", value[1]) + " (this codec reads only 0x01)");
        }
        if (value.length != V1_VALUE_LENGTH) {
            return new TrackerDecodeResult.Invalid(
                    "bad v1 value length: expected " + V1_VALUE_LENGTH + " bytes, got " + value.length);
        }
        return switch (value[2]) {
            case TYPE_ADD -> {
                // Unknown flag bits are deliberately ignored (forward compatibility, design §2.2).
                boolean clamped = (value[FLAGS_OFFSET] & FLAG_CLAMPED) != 0;
                long dispatchAtMs =
                        ByteBuffer.wrap(value, DISPATCH_AT_OFFSET, Long.BYTES).getLong();
                yield new TrackerDecodeResult.Add(dispatchAtMs, clamped);
            }
            case TYPE_CANCEL -> TrackerDecodeResult.Cancel.INSTANCE;
            default -> new TrackerDecodeResult.Invalid("unknown type: 0x" + String.format("%02X", value[2]));
        };
    }

    /**
     * Tombstone decoding posture: a missing or null-valued reason header is {@code Invalid} — it
     * matches the R12 forged-tombstone shape, and skipping protects against injected data loss.
     * A <em>present but unrecognized</em> reason name decodes to
     * {@link TrackerDecodeResult.CompleteUnknownReason}: the correctness payload of a tombstone is
     * "this key completed", so a reason constant added by a future writer must still apply the
     * completion on older readers — skipping it would leave the entry pending and re-dispatch a
     * completed key (a read_committed-visible duplicate). The novelty is surfaced to the caller
     * for a dedicated metric/WARN instead.
     */
    private static TrackerDecodeResult decodeTombstone(Headers headers) {
        Header header = headers.lastHeader(COMPLETION_REASON_HEADER);
        if (header == null) {
            return new TrackerDecodeResult.Invalid("tombstone missing " + COMPLETION_REASON_HEADER + " header");
        }
        byte[] raw = header.value();
        if (raw == null) {
            return new TrackerDecodeResult.Invalid("tombstone " + COMPLETION_REASON_HEADER + " header has null value");
        }
        String name = new String(raw, StandardCharsets.US_ASCII);
        for (CompletionReason reason : REASONS) {
            if (reason.name().equals(name)) {
                return new TrackerDecodeResult.Complete(reason);
            }
        }
        return new TrackerDecodeResult.CompleteUnknownReason(name);
    }
}
