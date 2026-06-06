/**
 * Payload re-fetch (design §7): the budgeted, partition-isolated seek-fetch pass the dispatch loop
 * runs <em>outside</em> its transaction, with the tri-state {@code FOUND / GONE / TRANSIENT}
 * classification, the decompressed-byte budget's truncate-and-carry-over, and the per-source-
 * partition penalty-box inputs. {@link events.cesium.kafka.core.fetch.SeekFetcher} is the loop's
 * boundary; {@link events.cesium.kafka.core.fetch.KafkaSeekFetcher} is the production
 * implementation owning the group-less seek consumer, and
 * {@link events.cesium.kafka.core.fetch.FetchCandidates} normalizes a drained batch into the
 * one-seek-plus-forward-scan-per-partition shape (§7.2).
 */
@org.jspecify.annotations.NullMarked
package events.cesium.kafka.core.fetch;
