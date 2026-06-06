package events.cesium.kafka.store.tracker;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import org.apache.kafka.common.Uuid;

/**
 * The committed-cursor <em>sidecar</em> codec (design §3.5, cursor v2): the versioned,
 * size-bounded metadata blob the engine commits per tracker partition inside the group-B
 * {@code OffsetAndMetadata}, carrying the oldest pending entries plus self-describing identity
 * material. Owned by {@code KafkaTrackerStore}; opaque to the engine.
 *
 * <p><strong>Binary layout (v1)</strong>, URL-safe unpadded Base64 of:
 *
 * <pre>
 *   byte  0          version (0x01)
 *   byte  1          cluster-id UTF-8 length L (0–255)
 *   bytes 2..2+L     cluster id, UTF-8
 *   16 bytes         source topic id  (Uuid msb, lsb — big-endian)
 *   16 bytes         tracker topic id (Uuid msb, lsb — big-endian)
 *   entries, repeated to the end of the blob, oldest-first (tracker-ADD-offset order):
 *     varint         trackerAddOffset (first entry: absolute; later: delta from previous, ≥ 1)
 *     varint         sourceOffset     (first entry: absolute; later: delta from previous, ≥ 1)
 *     varint         zigzag(dispatchAtMs delta from previous entry; first: from 0)
 *     byte           flags (bit 0 = {@link #FLAG_CLAMPED}; unknown bits ignored on decode)
 * </pre>
 *
 * <p>There is no entry count: entries run to the end of the blob, so the greedy budget cut needs
 * no header re-accounting and truncation mid-entry is detectable as corruption. The strictly
 * positive delta requirement is load-bearing: it makes the decoded entry list structurally
 * double-sorted (by tracker offset <em>and</em> source offset, design §5.2), so sidecar seeding
 * preserves the arrival log's sortedness by construction. The per-entry flags byte carries the
 * CLAMP marker (design §2.3): a pinned entry's ADD record lies <em>below</em> the committed
 * cursor and may never be replayed, so the sidecar is the only carrier that keeps the bit alive
 * across recovery.
 *
 * <p><strong>Identity header.</strong> {@code {v, clusterId, sourceTopicId, trackerTopicId}} per
 * design §3.5/R17 — topic ids survive nothing (a recreated topic gets a new {@link Uuid}), so
 * recovery validates the decoded identity against the live route and fails fast on mismatch
 * (§3.6). The cluster id is length-prefixed UTF-8 rather than a Uuid because Kafka cluster ids
 * are strings (KRaft ids happen to be 22-char base64url, but the API type is {@code String}).
 *
 * <p><strong>Budget semantics.</strong> The byte budget ({@code dispatch.cursor.sidecar-max-bytes},
 * validated against broker {@code offset.metadata.max.bytes} by the engine) bounds the
 * <em>Base64-encoded metadata string</em> — that is what the broker meters — so the encoder
 * derives the raw-byte limit {@code floor(3·budget/4)} and cuts greedily: entries are accepted
 * oldest-first until the <em>next</em> entry would exceed the limit (§3.5).
 *
 * <p><strong>Error posture.</strong> {@link #decode} throws {@link IllegalArgumentException} on
 * any malformation — unknown version, bad Base64, truncation, non-increasing offsets. A committed
 * cursor that cannot be decoded is corrupted durable state; the engine must fail fast (§3.6), so
 * a corrupted sidecar is never silently ignored or partially applied.
 */
public final class SidecarCodec {

    /** The only sidecar wire version this codec reads or writes. */
    public static final byte VERSION = 0x01;

    /** Entry flags bit 0: the CLAMP marker (design §2.3); see {@code TrackerWireFormat.FLAG_CLAMPED}. */
    public static final int FLAG_CLAMPED = 0x01;

    private static final int UUID_BYTES = 16;
    private static final int MAX_CLUSTER_ID_UTF8_BYTES = 255;
    private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder BASE64_DECODER = Base64.getUrlDecoder();

    private SidecarCodec() {}

    /** One pinned entry decoded from (or offered to) a sidecar. */
    public record SidecarEntry(long trackerAddOffset, long sourceOffset, long dispatchAtMs, boolean clamped) {}

    /**
     * A fully decoded sidecar: the identity header for validation plus the seeded entries in
     * encoded (tracker-ADD-offset) order — exactly the order recovery appends them to the
     * arrival log.
     */
    public record DecodedSidecar(
            String clusterId, Uuid sourceTopicId, Uuid trackerTopicId, List<SidecarEntry> entries) {}

    /**
     * Result of one greedy cursor encode (design §3.5).
     *
     * @param metadata the Base64 sidecar blob to commit; never empty (the identity header is
     *     always present, binding the cursor's identity even with zero pinned entries)
     * @param encodedCount entries that fit the budget ({@code cesium_pinned_entries})
     * @param encodedBytes size of {@code metadata} in bytes — the value metered against the
     *     broker's {@code offset.metadata.max.bytes} ({@code cesium_cursor_sidecar_bytes})
     * @param overflowed whether at least one pending entry did not fit (the min-pending fallback
     *     mode; sustained overflow is the §3.5 honest-residual regime)
     * @param cursorOffset the offset to commit: the live position when every pending entry was
     *     encoded, else the tracker ADD offset of the first <em>non</em>-encoded pending entry
     */
    public record EncodedCursor(
            String metadata, int encodedCount, int encodedBytes, boolean overflowed, long cursorOffset) {}

    /**
     * Largest raw payload whose unpadded Base64 rendering stays within {@code base64Budget}
     * bytes: {@code floor(3·budget/4)} — exact, since {@code n} raw bytes render as
     * {@code ceil(4n/3)} unpadded Base64 bytes.
     */
    public static int maxRawBytes(int base64Budget) {
        return (int) (3L * base64Budget / 4);
    }

    /**
     * Starts a greedy sidecar encode for one cursor computation.
     *
     * @param clusterId the route's cluster id (identity header)
     * @param sourceTopicId the route's source topic id
     * @param trackerTopicId the route's tracker topic id
     * @param base64ByteBudget the validated sidecar budget bounding the final metadata string
     * @throws IllegalArgumentException if the identity header alone exceeds the budget or the
     *     cluster id exceeds {@value #MAX_CLUSTER_ID_UTF8_BYTES} UTF-8 bytes — both are
     *     configuration errors {@code KafkaTrackerStore.validate()} surfaces at startup
     */
    public static Encoder encoder(String clusterId, Uuid sourceTopicId, Uuid trackerTopicId, int base64ByteBudget) {
        return new Encoder(clusterId, sourceTopicId, trackerTopicId, base64ByteBudget);
    }

    /**
     * Decodes a sidecar blob previously produced by an {@link Encoder}.
     *
     * @throws IllegalArgumentException on malformed Base64, an unknown version, truncation,
     *     trailing bytes, or non-increasing offsets — the caller must fail fast (§3.6): a
     *     corrupted committed cursor is never silently ignored
     */
    public static DecodedSidecar decode(String metadata) {
        byte[] raw;
        try {
            raw = BASE64_DECODER.decode(metadata);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("cursor sidecar is not valid Base64: " + e.getMessage(), e);
        }
        if (raw.length < 2) {
            throw new IllegalArgumentException("cursor sidecar truncated: " + raw.length + " byte(s)");
        }
        if (raw[0] != VERSION) {
            throw new IllegalArgumentException("unknown cursor sidecar version 0x" + String.format("%02X", raw[0])
                    + " (this store reads only 0x01); the cursor may have been committed by a newer cesium"
                    + " — refusing to half-read a committed cursor (§3.6)");
        }
        int clusterLength = raw[1] & 0xFF;
        int headerLength = 2 + clusterLength + 2 * UUID_BYTES;
        if (raw.length < headerLength) {
            throw new IllegalArgumentException(
                    "cursor sidecar truncated inside the identity header: " + raw.length + " < " + headerLength);
        }
        String clusterId = new String(raw, 2, clusterLength, StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.wrap(raw, 2 + clusterLength, raw.length - 2 - clusterLength);
        Uuid sourceTopicId = getUuid(buffer);
        Uuid trackerTopicId = getUuid(buffer);

        List<SidecarEntry> entries = new ArrayList<>();
        boolean first = true;
        long trackerAddOffset = 0;
        long sourceOffset = 0;
        long dispatchAtMs = 0;
        while (buffer.hasRemaining()) {
            long trackerDelta = readVarint(buffer);
            long sourceDelta = readVarint(buffer);
            long dispatchZigzag = readVarint(buffer);
            if (!buffer.hasRemaining()) {
                throw new IllegalArgumentException("cursor sidecar truncated before an entry's flags byte");
            }
            int flags = buffer.get() & 0xFF;
            if (first) {
                trackerAddOffset = trackerDelta;
                sourceOffset = sourceDelta;
                if (trackerAddOffset < 0 || sourceOffset < 0) {
                    throw new IllegalArgumentException("cursor sidecar entry has a negative offset: trackerAddOffset="
                            + trackerAddOffset + ", sourceOffset=" + sourceOffset);
                }
            } else {
                if (trackerDelta <= 0 || sourceDelta <= 0) {
                    throw new IllegalArgumentException("cursor sidecar entries are not strictly increasing"
                            + " (trackerDelta=" + trackerDelta + ", sourceDelta=" + sourceDelta
                            + "); the encoded order is load-bearing for arrival-log sortedness");
                }
                trackerAddOffset += trackerDelta;
                sourceOffset += sourceDelta;
                if (trackerAddOffset < 0 || sourceOffset < 0) {
                    throw new IllegalArgumentException("cursor sidecar entry offset overflow");
                }
            }
            dispatchAtMs += unzigzag(dispatchZigzag);
            // Unknown flag bits are ignored (forward compatibility, matching the tracker wire
            // format's posture, design §2.2).
            entries.add(new SidecarEntry(trackerAddOffset, sourceOffset, dispatchAtMs, (flags & FLAG_CLAMPED) != 0));
            first = false;
        }
        return new DecodedSidecar(clusterId, sourceTopicId, trackerTopicId, List.copyOf(entries));
    }

    /**
     * Greedy, size-bounded sidecar encoder for one cursor computation (design §3.5). Entries are
     * {@linkplain #offer offered} oldest-first (the {@code oldestPending} visitation order);
     * acceptance stops at the first entry whose encoding would push the final Base64 string past
     * the budget. {@link #finish} then applies the §3.5 cursor rule.
     */
    public static final class Encoder {

        private final byte[] buffer;
        private final int rawLimit;
        private int position;
        private int count;
        private boolean overflowed;
        private long firstNonEncodedTrackerAddOffset;
        private boolean first = true;
        private long previousTrackerAddOffset;
        private long previousSourceOffset;
        private long previousDispatchAtMs;

        private Encoder(String clusterId, Uuid sourceTopicId, Uuid trackerTopicId, int base64ByteBudget) {
            byte[] clusterBytes = clusterId.getBytes(StandardCharsets.UTF_8);
            if (clusterBytes.length > MAX_CLUSTER_ID_UTF8_BYTES) {
                throw new IllegalArgumentException(
                        "clusterId exceeds " + MAX_CLUSTER_ID_UTF8_BYTES + " UTF-8 bytes: '" + clusterId + "'");
            }
            this.rawLimit = maxRawBytes(base64ByteBudget);
            int headerLength = 2 + clusterBytes.length + 2 * UUID_BYTES;
            if (headerLength > rawLimit) {
                throw new IllegalArgumentException("sidecar budget " + base64ByteBudget
                        + " bytes cannot fit the " + headerLength + "-byte identity header (raw limit " + rawLimit
                        + "); raise dispatch.cursor.sidecar-max-bytes");
            }
            this.buffer = new byte[rawLimit];
            buffer[0] = VERSION;
            buffer[1] = (byte) clusterBytes.length;
            System.arraycopy(clusterBytes, 0, buffer, 2, clusterBytes.length);
            this.position = 2 + clusterBytes.length;
            putUuid(sourceTopicId);
            putUuid(trackerTopicId);
        }

        /**
         * Offers the next-oldest pending entry. Designed as the body of the
         * {@code oldestPending} visitor: the return value doubles as "keep visiting".
         *
         * @return {@code true} if the entry was encoded; {@code false} if it (or an earlier
         *     entry) did not fit — the encoder records the first non-encoded entry's tracker ADD
         *     offset as the overflow cut (§3.5)
         * @throws IllegalStateException if the offered entry does not strictly increase in both
         *     tracker ADD offset and source offset over the previous one — the visitation order
         *     underpins invariant I5, so a violation must surface as a store-level safe-degrade,
         *     never as a corrupted commit
         */
        public boolean offer(long trackerAddOffset, long sourceOffset, long dispatchAtMs, boolean clamped) {
            if (overflowed) {
                return false; // defensive: a visitor that ignores the stop signal cannot corrupt the cut
            }
            long trackerDelta;
            long sourceDelta;
            if (first) {
                if (trackerAddOffset < 0 || sourceOffset < 0) {
                    throw new IllegalStateException("pending entry has a negative offset: trackerAddOffset="
                            + trackerAddOffset + ", sourceOffset=" + sourceOffset);
                }
                trackerDelta = trackerAddOffset;
                sourceDelta = sourceOffset;
            } else {
                trackerDelta = trackerAddOffset - previousTrackerAddOffset;
                sourceDelta = sourceOffset - previousSourceOffset;
                if (trackerDelta <= 0 || sourceDelta <= 0) {
                    throw new IllegalStateException("pending entries offered out of order: trackerAddOffset "
                            + previousTrackerAddOffset + " -> " + trackerAddOffset + ", sourceOffset "
                            + previousSourceOffset + " -> " + sourceOffset
                            + " (oldestPending order underpins I5)");
                }
            }
            long dispatchZigzag = zigzag(dispatchAtMs - (first ? 0 : previousDispatchAtMs));
            int entryLength = varintLength(trackerDelta) + varintLength(sourceDelta) + varintLength(dispatchZigzag) + 1;
            if (position + entryLength > rawLimit) {
                overflowed = true;
                firstNonEncodedTrackerAddOffset = trackerAddOffset;
                return false;
            }
            position = writeVarint(trackerDelta);
            position = writeVarint(sourceDelta);
            position = writeVarint(dispatchZigzag);
            buffer[position++] = clamped ? (byte) FLAG_CLAMPED : 0;
            previousTrackerAddOffset = trackerAddOffset;
            previousSourceOffset = sourceOffset;
            previousDispatchAtMs = dispatchAtMs;
            first = false;
            count++;
            return true;
        }

        /**
         * Completes the encode and applies the §3.5 cursor rule: all pending encoded ⇒
         * {@code cursorOffset = position} (the live next-to-apply tracker offset); overflow ⇒
         * {@code cursorOffset =} the first non-encoded pending entry's tracker ADD offset (the
         * min-pending fallback, degenerating to the classic watermark when nothing fits).
         *
         * @param livePosition the partition's live position — the next tracker offset the
         *     dispatch consumer will apply
         */
        public EncodedCursor finish(long livePosition) {
            String metadata = BASE64_ENCODER.encodeToString(Arrays.copyOf(buffer, position));
            long cursorOffset = overflowed ? firstNonEncodedTrackerAddOffset : livePosition;
            return new EncodedCursor(metadata, count, metadata.length(), overflowed, cursorOffset);
        }

        private void putUuid(Uuid uuid) {
            ByteBuffer.wrap(buffer, position, 2 * Long.BYTES)
                    .putLong(uuid.getMostSignificantBits())
                    .putLong(uuid.getLeastSignificantBits());
            position += 2 * Long.BYTES;
        }

        private int writeVarint(long value) {
            int pos = position;
            long v = value;
            while ((v & ~0x7FL) != 0) {
                buffer[pos++] = (byte) ((v & 0x7F) | 0x80);
                v >>>= 7;
            }
            buffer[pos++] = (byte) v;
            return pos;
        }
    }

    private static Uuid getUuid(ByteBuffer buffer) {
        long msb = buffer.getLong();
        long lsb = buffer.getLong();
        return new Uuid(msb, lsb);
    }

    private static int varintLength(long value) {
        int length = 1;
        long v = value;
        while ((v & ~0x7FL) != 0) {
            length++;
            v >>>= 7;
        }
        return length;
    }

    private static long readVarint(ByteBuffer buffer) {
        long result = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            if (!buffer.hasRemaining()) {
                throw new IllegalArgumentException("cursor sidecar truncated inside a varint");
            }
            byte b = buffer.get();
            if (shift == 63 && (b & 0x7E) != 0) {
                // The 10th byte can only contribute bit 63: any higher value bit would be
                // shifted out of range and silently dropped — a malformed blob must throw, not
                // decode to a wrong value (the fail-fast posture for corrupted cursors, §3.6).
                throw new IllegalArgumentException("cursor sidecar varint overflows 64 bits");
            }
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new IllegalArgumentException("cursor sidecar varint exceeds 10 bytes");
    }

    private static long zigzag(long value) {
        return (value << 1) ^ (value >> 63);
    }

    private static long unzigzag(long value) {
        return (value >>> 1) ^ -(value & 1);
    }
}
