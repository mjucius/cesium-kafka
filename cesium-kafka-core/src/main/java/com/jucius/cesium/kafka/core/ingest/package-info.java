/**
 * The ingest loop (design §3.1, §3.8, §6): a single-threaded transactional pipeline from the
 * source topic to immediate relays, tracker ADDs, and DLQ records, with source offsets committed
 * inside the same transaction (invariants I1–I3) and the §3.8 error taxonomy around every failure.
 */
@org.jspecify.annotations.NullMarked
package com.jucius.cesium.kafka.core.ingest;
