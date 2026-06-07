package com.jucius.cesium.kafka.core.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * L6: secret-bearing config property maps must never disclose their values through {@code toString}
 * (a stray {@code log.debug("config {}", config)} or exception interpolation), yet must stay
 * readable through the map API and behave like the immutable copies they replace.
 */
class SecretMapTest {

    private static final String SECRET = "s3cr3t-do-not-log";

    @Test
    void redactsDenylistedValuesButKeepsOthers() {
        Map<String, String> source = new LinkedHashMap<>();
        source.put("bootstrap.servers", "broker:9092");
        source.put("ssl.keystore.password", SECRET);
        source.put("ssl.key.password", SECRET);
        source.put("sasl.jaas.config", "PlainLoginModule required password=\"" + SECRET + "\";");
        source.put("provider.secret", SECRET);
        source.put("api.key", SECRET);
        source.put("vault.credentials", SECRET);
        SecretMap map = SecretMap.copyOf(source);

        String rendered = map.toString();
        assertFalse(rendered.contains(SECRET), rendered);
        assertTrue(rendered.contains("bootstrap.servers=broker:9092"), rendered);
        assertTrue(rendered.contains("ssl.keystore.password=" + SecretMap.REDACTED), rendered);
        assertTrue(rendered.contains("sasl.jaas.config=" + SecretMap.REDACTED), rendered);
        assertTrue(rendered.contains("provider.secret=" + SecretMap.REDACTED), rendered);
        assertTrue(rendered.contains("api.key=" + SecretMap.REDACTED), rendered);
        assertTrue(rendered.contains("vault.credentials=" + SecretMap.REDACTED), rendered);

        // The real value is still available through the map itself — only toString masks it.
        assertEquals(SECRET, map.get("ssl.keystore.password"));
    }

    @Test
    void behavesLikeTheImmutableCopyItReplaces() {
        SecretMap map = SecretMap.copyOf(Map.of("a", "b"));
        assertThrows(UnsupportedOperationException.class, () -> map.put("x", "y"));
        // Map contract equality holds against a plain map with the same entries.
        assertEquals(Map.of("a", "b"), map);
        assertEquals(map, Map.of("a", "b"));
    }

    @Test
    void plantedSecretNeverAppearsInKafkaConfigToString() {
        KafkaConfig kafka = new KafkaConfig(
                null,
                Map.of("ssl.truststore.password", SECRET),
                null,
                new KafkaConfig.ClientOverrides(Map.of("sasl.jaas.config", "module password=\"" + SECRET + "\";")),
                null,
                null,
                null,
                null,
                null);
        String rendered = kafka.toString();
        assertFalse(rendered.contains(SECRET), rendered);
        assertTrue(rendered.contains(SecretMap.REDACTED), rendered);
    }

    @Test
    void plantedSecretNeverAppearsInCesiumConfigToString() {
        KafkaConfig kafka = new KafkaConfig(
                null, Map.of("ssl.keystore.password", SECRET), null, null, null, null, null, null, null);
        StoreConfig store = new StoreConfig("kafka-tracker", Map.of("kafka.hmac.secret", SECRET));
        CesiumConfig config = new CesiumConfig(
                "app", new InstanceId("slot-0"), null, kafka, null, null, null, store, null, null, null, null);
        assertFalse(config.toString().contains(SECRET), config.toString());
    }
}
