package com.jucius.cesium.kafka.it;

import com.jucius.cesium.kafka.app.CesiumApp;
import com.jucius.cesium.kafka.core.testing.CrashPoints;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The engine process {@link CrashLoopSoakIT} forks: the production {@link CesiumApp#main}, with an
 * optional crash point armed first. With {@code SOAK_CRASH_POINT} set (a {@link CrashPoints} id),
 * each firing of that point halts the JVM with {@link #CRASH_EXIT} at probability
 * {@code SOAK_CRASH_PROB}. {@code Runtime.halt} skips shutdown hooks, so the crash is as abrupt as a
 * {@code SIGKILL} but lands at a chosen §3.9 point. The child needs
 * {@code -Dcesium.testing.crashpoints=true} for the arming to be allowed.
 */
public final class CrashLoopChild {

    /** The exit code of a halt at the armed crash point. */
    static final int CRASH_EXIT = 86;

    private CrashLoopChild() {}

    public static void main(String[] args) {
        String point = System.getenv("SOAK_CRASH_POINT");
        if (point != null) {
            double probability = Double.parseDouble(System.getenv("SOAK_CRASH_PROB"));
            CrashPoints.install(id -> {
                if (id.equals(point) && ThreadLocalRandom.current().nextDouble() < probability) {
                    System.err.println("crash-loop child: halting at crash point " + id);
                    Runtime.getRuntime().halt(CRASH_EXIT);
                }
            });
        }
        CesiumApp.main(args);
    }
}
