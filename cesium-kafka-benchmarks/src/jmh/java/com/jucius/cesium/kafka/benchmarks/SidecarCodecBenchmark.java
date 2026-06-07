package com.jucius.cesium.kafka.benchmarks;

import com.jucius.cesium.kafka.store.tracker.SidecarCodec;
import com.jucius.cesium.kafka.store.tracker.SidecarCodec.DecodedSidecar;
import com.jucius.cesium.kafka.store.tracker.SidecarCodec.EncodedCursor;
import com.jucius.cesium.kafka.store.tracker.SidecarCodec.Encoder;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.Uuid;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Design §11.4: <em>sidecar encode/decode (300 entries) — target ≥ 100 k ops/s</em>.
 *
 * <p>The committed-cursor sidecar codec (design §3.5, cursor v2): varint/zigzag delta-encode ~300
 * pinned entries plus the identity header into a Base64 {@code OffsetAndMetadata} blob, and the
 * inverse decode. Unlike the index hot paths these are <em>not</em> allocation-free (encode builds
 * the byte buffer + Base64 string; decode builds the entry list), and that allocation is an
 * intrinsic part of the per-commit cost — so it is measured, not engineered away. Both directions
 * are non-destructive, so Throughput mode over one pre-built entry set is the natural fit.
 *
 * <p>The 300 entries carry strictly-increasing tracker and source offsets (the load-bearing
 * double-sorted order, design §5.2) and a wandering {@code dispatchAtMs} (zigzag exercises both
 * delta signs). The budget is sized generously so all 300 entries fit (the codec's greedy
 * budget-cut path is a separate concern from raw codec throughput).
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class SidecarCodecBenchmark {

    static final int ENTRIES = 300;

    /** Base64 byte budget — comfortably fits 300 entries so encode never truncates (raw limit ≈ 6 KiB). */
    static final int BUDGET_BYTES = 8192;

    private static final String CLUSTER_ID = "cesium-bench-cluster-id";
    private static final Uuid SOURCE_TOPIC_ID = new Uuid(0x1111_2222_3333_4444L, 0x5555_6666_7777_8888L);
    private static final Uuid TRACKER_TOPIC_ID = new Uuid(0x9999_AAAA_BBBB_CCCCL, 0xDDDD_EEEE_FFFF_0001L);
    private static final long LIVE_POSITION = 9_999_999L;

    private long[] trackerOffsets;
    private long[] sourceOffsets;
    private long[] dispatchAt;
    private boolean[] clamped;

    /** A representative encoded blob, decoded by {@link #decode()}. */
    private String encodedBlob;

    @Setup(Level.Trial)
    public void build() {
        SplittableRandom random = new SplittableRandom(0x51DE_CA12L);
        trackerOffsets = new long[ENTRIES];
        sourceOffsets = new long[ENTRIES];
        dispatchAt = new long[ENTRIES];
        clamped = new boolean[ENTRIES];

        long trk = random.nextInt(1_000);
        long src = random.nextInt(1_000);
        long disp = EPOCH_BASE_MS();
        for (int i = 0; i < ENTRIES; i++) {
            trk += 1 + random.nextInt(64); // strictly increasing tracker offset
            src += 1 + random.nextInt(64); // strictly increasing source offset
            disp += random.nextInt(2_000) - 1_000; // wanders both directions (zigzag deltas)
            trackerOffsets[i] = trk;
            sourceOffsets[i] = src;
            dispatchAt[i] = disp;
            clamped[i] = (i & 7) == 0; // a sprinkling of pinned/clamped entries
        }
        encodedBlob = encodeOnce().metadata();
    }

    @Benchmark
    public EncodedCursor encode() {
        return encodeOnce();
    }

    @Benchmark
    public DecodedSidecar decode() {
        return SidecarCodec.decode(encodedBlob);
    }

    @Benchmark
    public DecodedSidecar roundtrip() {
        return SidecarCodec.decode(encodeOnce().metadata());
    }

    private EncodedCursor encodeOnce() {
        Encoder encoder = SidecarCodec.encoder(CLUSTER_ID, SOURCE_TOPIC_ID, TRACKER_TOPIC_ID, BUDGET_BYTES);
        for (int i = 0; i < ENTRIES; i++) {
            encoder.offer(trackerOffsets[i], sourceOffsets[i], dispatchAt[i], clamped[i]);
        }
        return encoder.finish(LIVE_POSITION);
    }

    private static long EPOCH_BASE_MS() {
        return 1_700_000_000_000L;
    }
}
