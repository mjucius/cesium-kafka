/**
 * Typed, immutable configuration records and the aggregate validator (design §8).
 *
 * <p>The records mirror the YAML schema one-to-one. Every record materializes its documented
 * defaults in a compact canonical constructor, so a record constructed with absent (reflectively
 * {@code null}) optional components is fully populated and null-free afterwards — binding layers
 * (the app module's YAML/env loader) never need default tables of their own. Required fields with
 * no sensible default (e.g. {@code applicationId}) materialize as empty strings so that
 * {@link com.jucius.cesium.kafka.core.config.CesiumConfigValidator} can report <em>every</em> missing
 * field in one aggregate pass instead of failing on the first.
 *
 * <p>This package is deliberately framework-free: no Jackson types appear here. The app module
 * owns the YAML/env binding pipeline (design D11).
 */
@org.jspecify.annotations.NullMarked
package com.jucius.cesium.kafka.core.config;
