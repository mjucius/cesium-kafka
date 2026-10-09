package com.jucius.cesium.kafka.core.dispatch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.DispatchConfig;
import com.jucius.cesium.kafka.core.config.InstanceId;
import com.jucius.cesium.kafka.core.config.RouteConfig;
import com.jucius.cesium.kafka.core.config.TopicRef;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** {@code dispatch.max-pending-total} resolves to the loop's cap: AUTO derives it from the heap. */
class DispatchLoopConfigTest {

    private static final long HEAP = 4L * 1024 * 1024 * 1024;
    private static final long AUTO_CAP = DispatchConfig.heapBudgetBytes(HEAP) / DispatchConfig.INDEX_BYTES_PER_ENTRY;

    @Test
    void autoCapDerivesFromTheHeap() {
        assertEquals(AUTO_CAP, loopCap(DispatchConfig.AUTO_MAX_PENDING_TOTAL));
    }

    @Test
    void explicitCapIsUsedAsIs() {
        assertEquals(10, loopCap(10L));
    }

    private static long loopCap(long maxPendingTotal) {
        DispatchConfig dispatch = new DispatchConfig(null, null, null, null, null, null, null, maxPendingTotal);
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
                dispatch,
                null,
                null);
        return DispatchLoopConfig.from(config, HEAP).maxPendingTotal();
    }
}
