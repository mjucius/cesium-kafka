package events.cesium.kafka.app.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import events.cesium.kafka.core.config.CesiumConfig;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Guard: every value leaf in the {@link CesiumConfig} graph must be reachable through the
 * {@code CESIUM_} env grammar (design §8). A record component that binds as a scalar but is
 * modeled as a subtree (the original {@code instance-id} bug) makes its key unsettable from the
 * environment — this walk fails the build if the schema and the binder ever drift again.
 */
class SchemaReachabilityTest {

    private final ConfigSchema schema = ConfigSchema.forCesiumConfig();

    @Test
    void everyLeafIsReachableByTheEnvGrammar() {
        List<List<String>> leaves = new ArrayList<>();
        collectLeafEnvPaths(CesiumConfig.class, List.of(), leaves);
        assertFalse(leaves.isEmpty());
        for (List<String> path : leaves) {
            assertNotNull(schema.resolveEnvPath(path), () -> "unreachable leaf: CESIUM_" + String.join("__", path));
        }
    }

    /** Mirrors the binder's shape: one env path per bindable value position. */
    private static void collectLeafEnvPaths(Class<?> recordType, List<String> prefix, List<List<String>> leaves) {
        for (RecordComponent component : recordType.getRecordComponents()) {
            List<String> path = new ArrayList<>(prefix);
            path.add(envSegment(component.getName()));
            collect(component.getGenericType(), path, leaves);
        }
    }

    private static void collect(Type type, List<String> path, List<List<String>> leaves) {
        if (type instanceof Class<?> cls && cls.isRecord() && !ConfigMapper.SCALAR_BOUND_RECORDS.contains(cls)) {
            collectLeafEnvPaths(cls, path, leaves);
            return;
        }
        if (type instanceof ParameterizedType parameterized) {
            Class<?> raw = (Class<?>) parameterized.getRawType();
            if (Map.class.isAssignableFrom(raw)) {
                // Open map subtrees accept arbitrary keys; probe with a representative one.
                List<String> mapPath = new ArrayList<>(path);
                mapPath.add("SOME_KEY");
                leaves.add(mapPath);
                return;
            }
            if (Optional.class.isAssignableFrom(raw)) {
                collect(parameterized.getActualTypeArguments()[0], path, leaves);
                return;
            }
        }
        // Scalars, enums, collections, and scalar-bound records are all value leaves.
        leaves.add(path);
    }

    /** camelCase component name → CESIUM_ env segment ({@code maxPendingTotal} → {@code MAX_PENDING_TOTAL}). */
    private static String envSegment(String camelName) {
        return ConfigSchema.kebab(camelName).toUpperCase(Locale.ROOT).replace('-', '_');
    }
}
