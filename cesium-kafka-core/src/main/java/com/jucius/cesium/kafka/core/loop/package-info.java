/**
 * Plumbing shared by the ingest and dispatch loops (design §3.8, §6): the transaction abort /
 * producer-replacement step, the error taxonomy's cause-chain walk, retry backoff, the I-8
 * unrelayable bookkeeping types, and the fatal-stop exception both loops throw.
 */
@org.jspecify.annotations.NullMarked
package com.jucius.cesium.kafka.core.loop;
