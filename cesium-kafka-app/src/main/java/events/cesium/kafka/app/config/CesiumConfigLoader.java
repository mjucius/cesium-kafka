package events.cesium.kafka.app.config;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import events.cesium.kafka.core.config.CesiumConfig;
import events.cesium.kafka.core.config.CesiumConfigValidator;
import events.cesium.kafka.core.config.ConfigValidationException;
import events.cesium.kafka.core.config.ValidationContext;
import events.cesium.kafka.core.config.ValidationReport;
import events.cesium.kafka.core.config.ValidationReport.Finding;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

/**
 * Loads, overlays, interpolates, binds, and validates the application configuration (design §8):
 *
 * <ol>
 *   <li>parse the YAML file ({@code --config /etc/cesium/cesium.yaml});
 *   <li>report every unknown YAML key against the {@link ConfigSchema} (aggregate);
 *   <li>overlay environment variables: {@code CESIUM_} prefix, {@code __} path separator
 *       ({@code CESIUM_ROUTE__SOURCE__TOPIC} → {@code route.source.topic}); within a segment
 *       {@code _} maps to {@code -} for record fields and to {@code .} for map keys
 *       ({@code CESIUM_KAFKA__PROPERTIES__MAX_POLL_INTERVAL_MS}); unknown env keys under the
 *       prefix are startup errors;
 *   <li>overlay system properties ({@code -Dcesium.route.source.topic=...}) — highest precedence;
 *   <li>interpolate {@code ${env:VAR}} references inside string values (secrets stay out of
 *       files); an undefined variable is an error;
 *   <li>bind to the immutable {@link CesiumConfig} records (defaults materialize in their compact
 *       constructors — precedence is therefore {@code defaults < YAML < env < system properties});
 *   <li>run the aggregate {@link CesiumConfigValidator}.
 * </ol>
 *
 * <p>Any error throws {@link ConfigValidationException} carrying the full report; the app maps it
 * to exit code {@link #EX_CONFIG}. Environment and system properties are injected so tests run
 * hermetically without touching the real process environment.
 */
public final class CesiumConfigLoader {

    /** BSD {@code sysexits.h} EX_CONFIG: the process exit code for configuration errors (§8). */
    public static final int EX_CONFIG = 78;

    private static final String ENV_PREFIX = "CESIUM_";
    private static final String ENV_SEPARATOR = "__";
    private static final String SYSPROP_PREFIX = "cesium.";
    private static final Pattern ENV_REFERENCE = Pattern.compile("\\$\\{env:([A-Za-z_][A-Za-z0-9_]*)}");

    private final Map<String, String> environment;
    private final Properties systemProperties;
    private final ValidationContext validationContext;
    private final ObjectMapper mapper = ConfigMapper.create();
    private final ConfigSchema schema = ConfigSchema.forCesiumConfig();
    private final CesiumConfigValidator validator = new CesiumConfigValidator();

    /**
     * Creates a loader with injected sources.
     *
     * @param environment the environment map (tests inject; production passes
     *     {@code System.getenv()})
     * @param systemProperties the {@code -D} overlay source (highest precedence)
     * @param validationContext heap/partition facts for the §5.3 heap-budget check
     */
    public CesiumConfigLoader(
            Map<String, String> environment, Properties systemProperties, ValidationContext validationContext) {
        this.environment = Map.copyOf(environment);
        this.systemProperties = (Properties) systemProperties.clone();
        this.validationContext = validationContext;
    }

    /**
     * A production loader over the live process environment and system properties. The partition
     * estimate is 1 at config-load time; the engine re-validates with the real assigned count
     * once topic metadata is known.
     */
    public static CesiumConfigLoader withSystemEnvironment() {
        return new CesiumConfigLoader(System.getenv(), System.getProperties(), ValidationContext.runtime(1));
    }

    /**
     * A successful load: the frozen config plus the full validation report. The report stays
     * visible to the caller because a clean load still carries operator-facing findings — the
     * always-present INFO worst-case index footprint (§5.3 prints it at startup) and any WARNING
     * findings (e.g. a heap-budget breach downgraded by {@code startup-checks.heap-budget: WARN}).
     *
     * @param config the frozen, validated configuration
     * @param report the aggregate report; never contains {@code ERROR} findings (those throw)
     */
    public record LoadedConfig(CesiumConfig config, ValidationReport report) {}

    /**
     * Loads and validates the configuration.
     *
     * @return the frozen, validated config together with the validation report (WARNING/INFO
     *     findings survive a successful load so startup can print them)
     * @throws ConfigValidationException carrying the aggregate report on any error
     */
    public LoadedConfig load(Path yamlFile) {
        ObjectNode tree = readYamlTree(yamlFile);
        List<Finding> findings = new ArrayList<>();
        schema.collectUnknownKeys(tree, findings);
        applyEnvironmentOverlay(tree, findings);
        applySystemPropertyOverlay(tree, findings);
        interpolate(tree, "", findings);
        throwIfErrors(findings);
        CesiumConfig config = bind(tree);
        ValidationReport report = validator.validate(config, validationContext);
        if (report.hasErrors()) {
            throw new ConfigValidationException(report);
        }
        return new LoadedConfig(config, report);
    }

    // ------------------------------------------------------------------ YAML

    private ObjectNode readYamlTree(Path yamlFile) {
        if (!Files.isRegularFile(yamlFile)) {
            throw singleError(yamlFile.toString(), "config file not found.");
        }
        JsonNode root;
        try (InputStream in = Files.newInputStream(yamlFile)) {
            root = mapper.readTree(in);
        } catch (IOException e) {
            throw singleError(yamlFile.toString(), "failed to parse YAML: " + e.getMessage());
        }
        if (root == null || root.isNull() || root.isMissingNode()) {
            return mapper.createObjectNode(); // empty file: pure defaults + overlays
        }
        if (!(root instanceof ObjectNode objectRoot)) {
            throw singleError(yamlFile.toString(), "the top-level YAML node must be a mapping.");
        }
        return objectRoot;
    }

    // ------------------------------------------------------------------ env overlay

    private void applyEnvironmentOverlay(ObjectNode tree, List<Finding> findings) {
        environment.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(ENV_PREFIX))
                .sorted(Map.Entry.comparingByKey()) // deterministic finding order
                .forEach(entry -> {
                    String name = entry.getKey();
                    String remainder = name.substring(ENV_PREFIX.length());
                    List<String> segments = List.of(remainder.split(ENV_SEPARATOR, -1));
                    ConfigSchema.Resolved resolved =
                            remainder.isEmpty() || segments.contains("") ? null : schema.resolveEnvPath(segments);
                    if (resolved == null) {
                        String attempted = segments.stream()
                                .map(s -> s.toLowerCase(java.util.Locale.ROOT).replace('_', '-'))
                                .collect(java.util.stream.Collectors.joining("."));
                        findings.add(Finding.error(
                                name,
                                "unknown environment variable under the " + ENV_PREFIX
                                        + " prefix: no configuration key matches path '" + attempted
                                        + "' (grammar: CESIUM_ prefix, '__' separates path segments,"
                                        + " e.g. CESIUM_ROUTE__SOURCE__TOPIC; unknown keys are startup"
                                        + " errors, design §8)."));
                        return;
                    }
                    setOverride(tree, resolved, entry.getValue());
                });
    }

    // ------------------------------------------------------------------ system-property overlay

    private void applySystemPropertyOverlay(ObjectNode tree, List<Finding> findings) {
        systemProperties.stringPropertyNames().stream()
                .filter(name -> name.startsWith(SYSPROP_PREFIX))
                .sorted()
                .forEach(name -> {
                    String remainder = name.substring(SYSPROP_PREFIX.length());
                    List<String> segments = List.of(remainder.split("\\.", -1));
                    ConfigSchema.Resolved resolved = remainder.isEmpty() || segments.contains("")
                            ? null
                            : schema.resolveSystemPropertyPath(segments);
                    if (resolved == null) {
                        findings.add(Finding.error(
                                name,
                                "unknown system property under the " + SYSPROP_PREFIX
                                        + " prefix: no configuration key matches path '" + remainder
                                        + "' (unknown keys are startup errors, design §8)."));
                        return;
                    }
                    setOverride(tree, resolved, systemProperties.getProperty(name));
                });
    }

    /**
     * Writes an override value into the tree, creating (or, per precedence, replacing) the
     * intermediate objects. Collection leaves split on commas ({@code CESIUM_ROLES=ingest,dispatch}).
     */
    private static void setOverride(ObjectNode tree, ConfigSchema.Resolved resolved, String value) {
        ObjectNode parent = tree;
        List<String> path = resolved.jsonPath();
        for (int i = 0; i < path.size() - 1; i++) {
            JsonNode child = parent.get(path.get(i));
            parent = child instanceof ObjectNode existing ? existing : parent.putObject(path.get(i));
        }
        String field = path.getLast();
        if (resolved.leaf().collection()) {
            ArrayNode array = parent.putArray(field);
            for (String item : value.split(",", -1)) {
                if (!item.isBlank()) {
                    array.add(item.trim());
                }
            }
        } else {
            parent.put(field, value);
        }
    }

    // ------------------------------------------------------------------ ${env:VAR} interpolation

    /** Replaces {@code ${env:VAR}} in every string value; undefined variables are errors. */
    private void interpolate(JsonNode node, String path, List<Finding> findings) {
        if (node instanceof ObjectNode objectNode) {
            List<String> names = new ArrayList<>();
            objectNode.fieldNames().forEachRemaining(names::add);
            for (String name : names) {
                JsonNode child = objectNode.get(name);
                String childPath = path.isEmpty() ? name : path + "." + name;
                if (child.isTextual()) {
                    String replaced = interpolateText(child.asText(), childPath, findings);
                    if (replaced != null) {
                        objectNode.put(name, replaced);
                    }
                } else {
                    interpolate(child, childPath, findings);
                }
            }
        } else if (node instanceof ArrayNode arrayNode) {
            for (int i = 0; i < arrayNode.size(); i++) {
                JsonNode child = arrayNode.get(i);
                String childPath = path + "[" + i + "]";
                if (child.isTextual()) {
                    String replaced = interpolateText(child.asText(), childPath, findings);
                    if (replaced != null) {
                        arrayNode.set(i, mapper.getNodeFactory().textNode(replaced));
                    }
                } else {
                    interpolate(child, childPath, findings);
                }
            }
        }
    }

    /** Returns the interpolated text, or null when unchanged or unresolvable. */
    private @Nullable String interpolateText(String text, String path, List<Finding> findings) {
        Matcher matcher = ENV_REFERENCE.matcher(text);
        if (!matcher.find()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        boolean failed = false;
        do {
            String variable = matcher.group(1);
            String value = environment.get(variable);
            if (value == null) {
                findings.add(Finding.error(
                        path,
                        "references undefined environment variable '" + variable + "' via ${env:" + variable
                                + "} interpolation."));
                failed = true;
            } else {
                matcher.appendReplacement(sb, Matcher.quoteReplacement(value));
            }
        } while (matcher.find());
        if (failed) {
            return null;
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    // ------------------------------------------------------------------ binding

    private CesiumConfig bind(ObjectNode tree) {
        try {
            return mapper.treeToValue(tree, CesiumConfig.class);
        } catch (JsonMappingException e) {
            throw singleError(bindingPath(e), bindingMessage(e));
        } catch (IOException e) {
            throw singleError("(binding)", e.getMessage() == null ? e.toString() : e.getMessage());
        }
    }

    /** The dotted config path of a binding failure, e.g. {@code delay.max}. */
    private static String bindingPath(JsonMappingException e) {
        StringBuilder sb = new StringBuilder();
        for (JsonMappingException.Reference reference : e.getPath()) {
            String fieldName = reference.getFieldName();
            if (fieldName != null) {
                if (sb.length() > 0) {
                    sb.append('.');
                }
                sb.append(fieldName);
            } else if (reference.getIndex() >= 0) {
                sb.append('[').append(reference.getIndex()).append(']');
            }
        }
        return sb.length() == 0 ? "(root)" : sb.toString();
    }

    /** A binding failure message naming the offending value where Jackson knows it. */
    private static String bindingMessage(JsonMappingException e) {
        String original = e.getOriginalMessage();
        if (e instanceof InvalidFormatException invalidFormat) {
            return "invalid value '" + invalidFormat.getValue() + "': " + original;
        }
        return original == null ? e.toString() : original;
    }

    // ------------------------------------------------------------------ error helpers

    private static void throwIfErrors(List<Finding> findings) {
        if (findings.stream().anyMatch(f -> f.severity() == ValidationReport.Severity.ERROR)) {
            throw new ConfigValidationException(new ValidationReport(findings));
        }
    }

    private static ConfigValidationException singleError(String path, String message) {
        return new ConfigValidationException(new ValidationReport(List.of(Finding.error(path, message))));
    }
}
