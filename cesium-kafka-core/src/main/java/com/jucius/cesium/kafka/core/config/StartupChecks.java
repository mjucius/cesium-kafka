package com.jucius.cesium.kafka.core.config;

import java.time.Duration;
import java.util.Objects;

/**
 * Startup-check strictness knobs (design §7.6, §8, R6/R13).
 *
 * @param retention source-retention validation vs {@code delay.max + margin}; default FAIL
 * @param sizeBasedRetention when {@code retention.bytes != -1} or remote/tiered storage is enabled
 *     on the source, time-based validation cannot bound payload lifetime — startup fails unless
 *     the operator sets the explicit, named acceptance {@link SizeBasedRetention#ACKNOWLEDGED}
 *     (R13); default {@link SizeBasedRetention#FAIL}
 * @param maxToleratedOutage the longest outage after which committed offsets must still exist;
 *     checked against broker {@code offsets.retention.minutes} at startup (D18, R6); default
 *     {@code P7D}
 * @param outageCheck strictness of the {@code maxToleratedOutage} vs broker offsets-retention
 *     check — the "WARN/FAIL configurable" knob of the §8 defaults table; FAIL or WARN only;
 *     default FAIL
 * @param heapBudget strictness of the worst-case index-footprint vs heap-budget check (§5.3,
 *     R7/R18) — "fails (or warns, configurable)"; FAIL or WARN only; default FAIL
 * @param trackerAcl strictness of the R12 tracker write-ACL verification: the normative requirement
 *     that tracker write access is restricted to the cesium principal (a forged ADD is a
 *     duplicate-injection primitive and a forged tombstone is a data-loss primitive). FAIL refuses
 *     to start when the restriction is not verifiably in force (missing/foreign grant, unset
 *     acl-principal, or no authorizer to verify it); WARN surfaces the same conditions without
 *     blocking; SKIP omits the check. Default WARN — set FAIL to have cesium refuse to start unless
 *     the R12 restriction is verifiably in force (recommended for production; see SECURITY.md §4)
 */
public record StartupChecks(
        CheckMode retention,
        SizeBasedRetention sizeBasedRetention,
        Duration maxToleratedOutage,
        CheckMode outageCheck,
        CheckMode heapBudget,
        CheckMode trackerAcl) {

    /** Default maximum tolerated outage (§8 defaults table). */
    public static final Duration DEFAULT_MAX_TOLERATED_OUTAGE = Duration.ofDays(7);

    /** The size-based/tiered-retention gate (R13): unset means FAIL when such eviction is detected. */
    public enum SizeBasedRetention {
        /** Fail startup when size-based or tiered eviction is detected on the source (default). */
        FAIL,
        /** The operator explicitly accepts that size/tier eviction may expire payloads early. */
        ACKNOWLEDGED
    }

    /** Materializes the §8 defaults for absent components. */
    public StartupChecks {
        retention = Objects.requireNonNullElse(retention, CheckMode.FAIL);
        sizeBasedRetention = Objects.requireNonNullElse(sizeBasedRetention, SizeBasedRetention.FAIL);
        maxToleratedOutage = Objects.requireNonNullElse(maxToleratedOutage, DEFAULT_MAX_TOLERATED_OUTAGE);
        outageCheck = Objects.requireNonNullElse(outageCheck, CheckMode.FAIL);
        heapBudget = Objects.requireNonNullElse(heapBudget, CheckMode.FAIL);
        // Default WARN (not FAIL): surfacing the unenforced R12 control on every boot without blocking
        // is backward-compatible; operators opt into FAIL to have startup refuse an unverified ACL.
        trackerAcl = Objects.requireNonNullElse(trackerAcl, CheckMode.WARN);
    }

    /** Returns a {@code StartupChecks} populated entirely from the §8 defaults table. */
    public static StartupChecks defaults() {
        return new StartupChecks(
                CheckMode.FAIL,
                SizeBasedRetention.FAIL,
                DEFAULT_MAX_TOLERATED_OUTAGE,
                CheckMode.FAIL,
                CheckMode.FAIL,
                CheckMode.WARN);
    }
}
