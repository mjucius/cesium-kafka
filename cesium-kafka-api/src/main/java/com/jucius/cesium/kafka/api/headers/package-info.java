/**
 * The cesium header protocol (design §2.3–2.4): the names and value grammar of the control headers
 * producers use to request delayed delivery, the provenance headers cesium stamps on relayed
 * records, and the DLQ contract headers.
 *
 * <p>These constants are part of the stable public surface — producer and consumer ecosystems in
 * any language depend on the exact strings, which is why every value is canonical UTF-8 ASCII
 * decimal, producible without helper code.
 */
@org.jspecify.annotations.NullMarked
package com.jucius.cesium.kafka.api.headers;
