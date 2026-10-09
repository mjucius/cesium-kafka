package com.jucius.cesium.kafka.core.dispatch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.DispatchConfig;
import com.jucius.cesium.kafka.core.config.InstanceId;
import com.jucius.cesium.kafka.core.config.RouteConfig;
import com.jucius.cesium.kafka.core.config.TopicRef;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** {@code dispatch.max-pending-total} is process-wide: each loop enforces {@code cap / workers}. */
class DispatchLoopConfigTest {

    private static final long HEAP = 4L * 1024 * 1024 * 1024;
    private static final long AUTO_CAP = DispatchConfig.heapBudgetBytes(HEAP) / DispatchConfig.INDEX_BYTES_PER_ENTRY;

    @Test
    void autoCapIsSplitAcrossWorkers() {
        assertEquals(AUTO_CAP / 4, perLoopCap(4, DispatchConfig.AUTO_MAX_PENDING_TOTAL));
    }

    @Test
    void explicitCapIsSplitAcrossWorkers() {
        assertEquals(2, perLoopCap(4, 10L));
    }

    @Test
    void perLoopCapIsAtLeastOne() {
        assertEquals(1, perLoopCap(4, 3L));
    }

    @Test
    void singleWorkerKeepsTheWholeCap() {
        assertEquals(10, perLoopCap(1, 10L));
        assertEquals(AUTO_CAP, perLoopCap(1, DispatchConfig.AUTO_MAX_PENDING_TOTAL));
    }

    private static long perLoopCap(int workers, long maxPendingTotal) {
        DispatchConfig dispatch =
                new DispatchConfig(workers, null, null, null, null, null, null, null, maxPendingTotal);
        CesiumConfig config = new CesiumConfig(
                "orders-delay",
                new InstanceId("slot-0"),
                null,
                null,
                new RouteConfig(
                        new TopicRef("orders"),
                        new TopicRef("orders-out"),
                        null,
                        Optional.of(new TopicRef("orders-dlq")),
                        null),
                null,
                null,
                null,
                dispatch,
                null,
                null);
        return DispatchLoopConfig.from(config, HEAP).maxPendingTotal();
    }
}
