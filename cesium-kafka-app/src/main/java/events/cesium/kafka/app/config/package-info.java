/**
 * The configuration loading pipeline (design §8, D11): YAML file → Jackson → the immutable
 * {@code events.cesium.kafka.core.config} records, with precedence
 * {@code defaults < YAML < environment < system properties}, {@code ${env:VAR}} interpolation for
 * secrets, typo-rejecting unknown-key errors (YAML <em>and</em> env-under-prefix), and aggregate
 * validation. No framework.
 *
 * <p>The core config records stay Jackson-free; everything Jackson lives here.
 */
@org.jspecify.annotations.NullMarked
package events.cesium.kafka.app.config;
