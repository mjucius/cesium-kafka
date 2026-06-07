package com.jucius.cesium.kafka.app.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.jucius.cesium.kafka.core.config.InstanceId;
import java.io.IOException;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Set;

/**
 * Factory for the configured YAML {@link ObjectMapper} (design §8):
 *
 * <ul>
 *   <li>kebab-case keys ({@code on-over-max}, {@code max-pending-per-partition}) mapped onto the
 *       camelCase record components;
 *   <li>{@code FAIL_ON_UNKNOWN_PROPERTIES} as a binding-time backstop — the loader's schema walk
 *       reports unknown keys in aggregate first;
 *   <li>{@code STRICT_DUPLICATE_DETECTION}: a key pasted twice would otherwise silently last-win,
 *       losing half the operator's settings — exactly the mistake the §8 typo-rejecting posture
 *       exists to catch;
 *   <li>case-insensitive enums ({@code classic}, {@code DLQ}, {@code by_key} all bind);
 *   <li>{@link Jdk8Module} for {@code Optional} components;
 *   <li>ISO-8601 {@link Duration} and {@link InstanceId} scalar deserializers (the core records
 *       are Jackson-free, so the bindings live here).
 * </ul>
 */
final class ConfigMapper {

    /**
     * Record types bound from a single YAML scalar via the deserializers registered in
     * {@link #create()}. {@link ConfigSchema} treats these as value leaves rather than record
     * subtrees, so the env / system-property grammar can set them directly
     * ({@code CESIUM_INSTANCE_ID=blue-1}, {@code -Dcesium.instance-id=blue-1}) — exactly the
     * per-deployment-slot keys D10/D21 expect operators to inject. Keep this set in sync with the
     * {@code addDeserializer} registrations below.
     */
    static final Set<Class<?>> SCALAR_BOUND_RECORDS = Set.of(InstanceId.class);

    private ConfigMapper() {}

    /** Creates the configured mapper. */
    static ObjectMapper create() {
        SimpleModule cesiumScalars = new SimpleModule("cesium-config-scalars");
        cesiumScalars.addDeserializer(Duration.class, new IsoDurationDeserializer());
        cesiumScalars.addDeserializer(InstanceId.class, new InstanceIdDeserializer());
        return YAMLMapper.builder()
                .propertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE)
                .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .addModule(new Jdk8Module())
                .addModule(cesiumScalars)
                .build();
    }

    /**
     * Parses durations strictly as ISO-8601 ({@code PT30S}, {@code PT5M}, {@code P1D}). The
     * failure message names the offending value and shows examples — duration typos are the most
     * common config mistake and the error must be self-explanatory.
     */
    static final class IsoDurationDeserializer extends StdScalarDeserializer<Duration> {

        IsoDurationDeserializer() {
            super(Duration.class);
        }

        @Override
        public Duration deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            String text = p.getValueAsString();
            if (text == null || text.isBlank()) {
                return ctxt.reportInputMismatch(
                        Duration.class,
                        "expected an ISO-8601 duration such as PT30S, PT5M, or P1D, got an empty value");
            }
            try {
                return Duration.parse(text.trim());
            } catch (DateTimeParseException e) {
                throw ctxt.weirdStringException(
                        text, Duration.class, "expected an ISO-8601 duration such as PT30S, PT5M, or P1D");
            }
        }
    }

    /** Binds the {@code instance-id} scalar string into the {@link InstanceId} value object. */
    static final class InstanceIdDeserializer extends StdScalarDeserializer<InstanceId> {

        InstanceIdDeserializer() {
            super(InstanceId.class);
        }

        @Override
        public InstanceId deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            String text = p.getValueAsString();
            return new InstanceId(text == null ? "" : text);
        }
    }
}
