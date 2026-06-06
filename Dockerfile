# syntax=docker/dockerfile:1
# =============================================================================
# cesium-kafka — container image (design §10)
# =============================================================================
# Multi-stage: stage 1 builds the application distribution with the project's
# Gradle wrapper; stage 2 ships only the JRE + the unpacked distribution.
#
# Base choice: eclipse-temurin:21-jre, NOT distroless. The Gradle `application`
# plugin's entrypoint (bin/cesium-kafka) is a POSIX shell script that assembles
# the classpath and JVM options and honours JAVA_OPTS / CESIUM_KAFKA_OPTS;
# gcr.io/distroless/java21 has no shell, so the start script cannot run there.
# The temurin JRE keeps the image small while running the stock dist scripts.
#
# Build (context = repo root):
#   docker build -t cesium-kafka:local .
# Run (mount a config, publish the observability port):
#   docker run --rm -p 8081:8081 \
#     -e CESIUM_KAFKA__PROPERTIES__BOOTSTRAP_SERVERS=host.docker.internal:9092 \
#     -v "$PWD/config/cesium-example.yaml:/etc/cesium/cesium.yaml:ro" \
#     cesium-kafka:local
# =============================================================================

# ---- stage 1: build the distribution ----------------------------------------
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src

# Copy the wrapper and build scripts first so dependency resolution layers cache
# independently of source churn.
COPY gradlew gradlew.bat settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle/ gradle/
COPY build-logic/ build-logic/

# Then the module sources (.dockerignore keeps build/.gradle/.git out of context).
COPY cesium-kafka-api/ cesium-kafka-api/
COPY cesium-kafka-core/ cesium-kafka-core/
COPY cesium-kafka-store-kafka/ cesium-kafka-store-kafka/
COPY cesium-kafka-store-testkit/ cesium-kafka-store-testkit/
COPY cesium-kafka-app/ cesium-kafka-app/
COPY cesium-kafka-it/ cesium-kafka-it/

# installDist produces an unpacked distribution (bin/ + lib/) under build/install.
# --no-daemon: one-shot build; the daemon would only waste a layer.
RUN ./gradlew --no-daemon :cesium-kafka-app:installDist

# ---- stage 2: runtime -------------------------------------------------------
FROM eclipse-temurin:21-jre AS runtime

# Non-root runtime user (a fixed high uid plays well with restricted PodSecurity).
RUN groupadd --system --gid 10001 cesium \
    && useradd --system --uid 10001 --gid cesium --home-dir /opt/cesium --shell /usr/sbin/nologin cesium \
    && mkdir -p /etc/cesium \
    && chown -R cesium:cesium /etc/cesium

COPY --from=build --chown=cesium:cesium /src/cesium-kafka-app/build/install/cesium-kafka /opt/cesium

# Default config path (overridable with --config or CESIUM_CONFIG). Mount your
# config to this path, or override the brokers via the env overlay
# (CESIUM_KAFKA__PROPERTIES__BOOTSTRAP_SERVERS=...).
ENV CESIUM_CONFIG=/etc/cesium/cesium.yaml

# Container-aware JVM defaults (design §5.4). MaxRAMPercentage sizes the heap from
# the container memory limit; pinning Initial=Max (= "-Xms=-Xmx") keeps the
# long-lived, allocation-free index off the resize path. The index is the
# friendliest GC profile there is:
#   * <= ~10 M pending entries / <= 4 GB heap: G1 (the JDK default) is ideal.
#   * >= 8 GB heap / 100 M-entry scale: add "-XX:+UseZGC -XX:+ZGenerational" via
#     JAVA_OPTS so dispatch-accuracy p99 is independent of heap size.
# The 60% default reserves ~40% for non-heap: metaspace, thread stacks, and the
# Kafka client direct/native memory (producer/consumer buffers + the decompressing
# fetch path, which §5.4 sizes at up to 32 MiB per dispatch txn). Size the container
# as heap / 0.6. The §5.4 "heap + ~1 GB native" rule-of-thumb assumes a >= 4 GB
# container; below that the percentage reservation (not the absolute 1 GB) governs,
# so the 60% default keeps native headroom proportional on small containers.
# Operators extend/override these with JAVA_OPTS or CESIUM_KAFKA_OPTS at runtime.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=60.0 -XX:InitialRAMPercentage=60.0 -XX:+ExitOnOutOfMemoryError"

# The observability HTTP port (/metrics, /health/live, /health/ready, /info).
EXPOSE 8081

USER cesium
WORKDIR /opt/cesium

ENTRYPOINT ["/opt/cesium/bin/cesium-kafka"]
