package com.jucius.cesium.kafka.app.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jucius.cesium.kafka.core.config.CesiumConfig;
import com.jucius.cesium.kafka.core.config.ValidationReport.Finding;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;

/**
 * The known-path set of the configuration schema, derived by reflecting over the
 * {@link CesiumConfig} record graph (design §8: unknown keys — YAML or env-under-prefix — are
 * startup errors).
 *
 * <p>Three node kinds: record nodes (children keyed by the kebab-cased component name, matching
 * the mapper's {@code KEBAB_CASE} strategy), open map nodes ({@code Map<String,String>} subtrees
 * such as {@code kafka.properties} accept arbitrary keys), and value leaves (scalars — including
 * the {@link ConfigMapper#SCALAR_BOUND_RECORDS scalar-bound record types} like
 * {@code instance-id} — plus collection leaves like {@code roles} whose env values split on
 * commas).
 *
 * <p>Deriving the path set reflectively rather than maintaining a hand-written list means new
 * record components are automatically known — the schema cannot drift from the records.
 */
final class ConfigSchema {

    private final RecordNode root;

    private ConfigSchema(RecordNode root) {
        this.root = root;
    }

    /** Builds the schema for the {@link CesiumConfig} graph. */
    static ConfigSchema forCesiumConfig() {
        return new ConfigSchema(recordNode(CesiumConfig.class));
    }

    // ------------------------------------------------------------------ schema model

    sealed interface Node permits RecordNode, MapNode, LeafNode {}

    /** A record subtree; children keyed by kebab-cased component name. */
    record RecordNode(Map<String, Node> children) implements Node {}

    /** An open {@code Map<String,String>} subtree — arbitrary keys are accepted below it. */
    record MapNode() implements Node {}

    /**
     * A bindable value position.
     *
     * @param collection true for collection-typed leaves ({@code roles}); env/system-property
     *     values split on commas into an array before binding
     */
    record LeafNode(boolean collection) implements Node {}

    /**
     * A resolved override path.
     *
     * @param jsonPath the field names (kebab keys, plus the literal map key for map subtrees) to
     *     set in the config tree
     * @param leaf the leaf the value binds to
     */
    record Resolved(List<String> jsonPath, LeafNode leaf) {}

    // ------------------------------------------------------------------ reflection walk

    private static RecordNode recordNode(Class<?> recordType) {
        Map<String, Node> children = new LinkedHashMap<>();
        for (RecordComponent component : recordType.getRecordComponents()) {
            children.put(kebab(component.getName()), nodeFor(component.getGenericType()));
        }
        return new RecordNode(children);
    }

    private static Node nodeFor(Type type) {
        if (type instanceof Class<?> cls) {
            // Scalar-bound records (InstanceId) bind from one YAML scalar, not a subtree: they
            // must be leaves so CESIUM_INSTANCE_ID / -Dcesium.instance-id resolve (design §8, D10).
            return cls.isRecord() && !ConfigMapper.SCALAR_BOUND_RECORDS.contains(cls)
                    ? recordNode(cls)
                    : new LeafNode(false);
        }
        if (type instanceof ParameterizedType parameterized) {
            Class<?> raw = (Class<?>) parameterized.getRawType();
            if (Map.class.isAssignableFrom(raw)) {
                return new MapNode();
            }
            if (Optional.class.isAssignableFrom(raw)) {
                // Optional wraps without changing the YAML shape: route.dlq.topic binds directly.
                return nodeFor(parameterized.getActualTypeArguments()[0]);
            }
            if (Collection.class.isAssignableFrom(raw)) {
                return new LeafNode(true);
            }
        }
        return new LeafNode(false);
    }

    /** camelCase → kebab-case, matching Jackson's {@code KEBAB_CASE} naming strategy. */
    static String kebab(String camel) {
        StringBuilder sb = new StringBuilder(camel.length() + 4);
        for (int i = 0; i < camel.length(); i++) {
            char c = camel.charAt(i);
            if (Character.isUpperCase(c)) {
                sb.append('-').append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------ unknown YAML keys

    /** Collects every unknown key in the YAML tree — aggregate, never first-error-only. */
    void collectUnknownKeys(ObjectNode tree, List<Finding> findings) {
        walkUnknown(root, tree, "", findings);
    }

    /**
     * Known legacy/relocated top-level spellings → their actual nested homes. The §8 prose placed
     * these rows differently from the authoritative {@code CesiumConfig} sketch (the sketch wins;
     * the reconciliation is recorded in the design doc's post-approval revisions), so an operator
     * transcribing the old table gets a did-you-mean instead of a bare unknown-key error.
     */
    private static final Map<String, String> KNOWN_RELOCATIONS =
            Map.of("transactions", "kafka.transactions", "relay", "route.relay");

    private static void walkUnknown(Node schema, JsonNode json, String path, List<Finding> findings) {
        if (!(schema instanceof RecordNode recordNode) || !json.isObject()) {
            // Map subtrees are open; leaves and shape mismatches are the binder's concern.
            return;
        }
        for (Entry<String, JsonNode> field : json.properties()) {
            String childPath = path.isEmpty() ? field.getKey() : path + "." + field.getKey();
            Node child = recordNode.children().get(field.getKey());
            if (child == null) {
                findings.add(Finding.error(
                        childPath,
                        "unknown configuration key" + suggestion(recordNode, path, field.getKey())
                                + " (unknown keys are startup errors, design §8)."));
            } else {
                walkUnknown(child, field.getValue(), childPath, findings);
            }
        }
    }

    private static String suggestion(RecordNode node, String path, String unknownKey) {
        String relocated = path.isEmpty() ? KNOWN_RELOCATIONS.get(unknownKey) : null;
        if (relocated != null) {
            return "; did you mean '" + relocated + "'?";
        }
        String lower = unknownKey.toLowerCase(Locale.ROOT);
        return node.children().keySet().stream()
                .filter(known -> known.replace("-", "").equals(lower.replace("-", "")))
                .findFirst()
                .map(known -> "; did you mean '" + known + "'?")
                .orElse("");
    }

    // ------------------------------------------------------------------ override-path resolution

    /**
     * Resolves an environment-variable path ({@code CESIUM_} stripped, split on {@code __}):
     * segments lowercase with {@code _} → {@code -} against record fields
     * ({@code MAX_PENDING_PER_PARTITION} → {@code max-pending-per-partition}); below a map node
     * the remaining segments become one dotted key with {@code _} → {@code .}
     * ({@code MAX_POLL_INTERVAL_MS} → {@code max.poll.interval.ms}).
     */
    @Nullable Resolved resolveEnvPath(List<String> segments) {
        return resolve(segments, s -> s.toLowerCase(Locale.ROOT).replace('_', '-'), s -> s.toLowerCase(Locale.ROOT)
                .replace('_', '.'));
    }

    /**
     * Resolves a system-property path ({@code cesium.} stripped, split on {@code .}): segments
     * are already kebab keys ({@code cesium.delay.on-over-max}); below a map node the remaining
     * segments are re-joined with dots ({@code cesium.kafka.properties.max.poll.interval.ms}).
     */
    @Nullable Resolved resolveSystemPropertyPath(List<String> segments) {
        return resolve(segments, UnaryOperator.identity(), UnaryOperator.identity());
    }

    private @Nullable Resolved resolve(
            List<String> segments, UnaryOperator<String> fieldKey, UnaryOperator<String> mapKeyPart) {
        Node current = root;
        List<String> jsonPath = new ArrayList<>(segments.size());
        for (int i = 0; i < segments.size(); i++) {
            switch (current) {
                case RecordNode recordNode -> {
                    String key = fieldKey.apply(segments.get(i));
                    Node child = recordNode.children().get(key);
                    if (child == null) {
                        return null;
                    }
                    jsonPath.add(key);
                    current = child;
                }
                case MapNode ignored -> {
                    // The rest of the path is one literal map key (kafka property names contain
                    // dots, never separators).
                    String mapKey = segments.subList(i, segments.size()).stream()
                            .map(mapKeyPart)
                            .collect(Collectors.joining("."));
                    jsonPath.add(mapKey);
                    return new Resolved(jsonPath, new LeafNode(false));
                }
                case LeafNode ignored -> {
                    return null; // path continues below a scalar
                }
            }
        }
        // Path exhausted: only a value leaf is assignable; record/map nodes need a deeper path.
        return current instanceof LeafNode leaf ? new Resolved(jsonPath, leaf) : null;
    }
}
