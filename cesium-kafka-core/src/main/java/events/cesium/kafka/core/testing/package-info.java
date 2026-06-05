/**
 * Test seams compiled into production code by design: the {@code CrashPoints} hook the §3.9
 * crash-point integration tests (and unit sequencing tests) attach to. No-ops unless a test
 * installs a handler.
 */
@org.jspecify.annotations.NullMarked
package events.cesium.kafka.core.testing;
