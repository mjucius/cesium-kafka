/**
 * The dispatch loop (design §3.2, §3.5, §3.6, §3.8, §6): a single-threaded owner of the group-B
 * tracker consumer, the dispatch transactional producer, and the seek fetcher, driving a
 * {@link events.cesium.kafka.api.store.TrackerBackedStore} through the I8 recovery protocol, the
 * §3.2 dispatch transaction, cursor-v2 commits, the penalty box, and backpressure.
 */
@org.jspecify.annotations.NullMarked
package events.cesium.kafka.core.dispatch;
