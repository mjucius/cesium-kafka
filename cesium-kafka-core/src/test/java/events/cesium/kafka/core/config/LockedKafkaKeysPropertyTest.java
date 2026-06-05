package events.cesium.kafka.core.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import events.cesium.kafka.core.config.ValidationReport.Finding;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/**
 * Property: for any subset of locked keys injected into the common {@code kafka.properties} map
 * (alongside arbitrary unlocked keys), the validator rejects exactly the locked ones — no key is
 * missed and no unlocked key is rejected (design §8, §11.1).
 */
class LockedKafkaKeysPropertyTest {

    private final CesiumConfigValidator validator = new CesiumConfigValidator();

    @Property
    void exactlyTheInjectedLockedKeysAreRejected(
            @ForAll("lockedKeySubsets") Set<String> lockedKeys, @ForAll("unlockedKeys") Set<String> unlockedKeys) {
        Map<String, String> properties = new HashMap<>();
        lockedKeys.forEach(k -> properties.put(k, "v"));
        unlockedKeys.forEach(k -> properties.put(k, "v"));

        ValidationReport report =
                validator.validate(TestConfigs.withKafkaProperties(properties), TestConfigs.ROOMY_CONTEXT);

        Set<String> rejected = report.errors().stream()
                .map(Finding::path)
                .filter(p -> p.startsWith("kafka.properties."))
                .map(p -> p.substring("kafka.properties.".length()))
                .collect(Collectors.toCollection(TreeSet::new));
        assertEquals(new TreeSet<>(lockedKeys), rejected, report::render);
    }

    @Provide
    Arbitrary<Set<String>> lockedKeySubsets() {
        return Arbitraries.of(LockedKafkaKeys.all()).set().ofMinSize(1);
    }

    @Provide
    Arbitrary<Set<String>> unlockedKeys() {
        return Arbitraries.of(
                        "bootstrap.servers",
                        "client.id",
                        "max.poll.records",
                        "fetch.max.bytes",
                        "linger.ms",
                        "compression.type")
                .set();
    }
}
