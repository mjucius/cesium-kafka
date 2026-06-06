plugins {
    id("cesium.java-conventions")
}

description = "Integration tests against real Kafka 4.x (Testcontainers, KRaft); cesium instances run as separate JVMs for crash tests"

testing {
    suites {
        register<JvmTestSuite>("integrationTest") {
            useJUnitJupiter(libs.versions.junit.asProvider())
            dependencies {
                implementation(project(":cesium-kafka-app"))
                implementation(project(":cesium-kafka-core"))
                implementation(project(":cesium-kafka-store-kafka"))
                // MapConfigView from the published kit backs the harness's real StoreContext.
                implementation(project(":cesium-kafka-store-testkit"))
                implementation(libs.kafka.clients)
                implementation(libs.micrometer.core)
                // The app integration test wires the production ObservabilityServer over a real
                // PrometheusMeterRegistry (the same registry the engine feeds); the app declares
                // micrometer-prometheus as `implementation`, so it is not transitive to consumers.
                implementation(libs.micrometer.prometheus)
                implementation(libs.testcontainers.kafka)
                implementation(libs.testcontainers.junit)
                implementation(libs.testcontainers.toxiproxy)
                implementation(libs.awaitility)
                implementation(libs.slf4j.api)
                runtimeOnly(libs.logback.classic)
            }
            targets.all {
                testTask.configure {
                    // Testcontainers 1.21.3 pins docker-java to Docker API v1.32 when no
                    // api.version is configured (DockerClientProviderStrategy), and Docker
                    // Engine 29+ removed API versions < 1.43 (HTTP 400 on /v1.32/*). Declaring
                    // a modern version skips the legacy pin; 1.44 = Engine 25+ (Jan 2024 floor).
                    // Overridable via -Dapi.version for older daemons.
                    systemProperty("api.version", System.getProperty("api.version") ?: "1.44")
                    // The M6 fencing/rebalance scenarios (zombie fencing, cooperative revocation,
                    // onPartitionsLost) stand up several in-JVM engine instances at once inside one
                    // test worker — each owning a transactional producer with a 64 MiB buffer.memory
                    // plus consumer fetch buffers — alongside verifier consumers; the Gradle default
                    // 512 MiB worker heap is tight for that. 1 GiB gives the multi-instance suites
                    // headroom; the dump flag makes any OOM diagnosable.
                    maxHeapSize = "1024m"
                    jvmArgs("-XX:+HeapDumpOnOutOfMemoryError")
                }
            }
        }
    }
}

// The macro/soak performance lane (design §11.4). The @Tag("nightly") perf ITs (ingest/dispatch/
// burst/large-payload/heterogeneous-replay) run on the ordinary `integrationTest` task with
// -PincludeNightly=true (nightly.yml's perf batch). The @Tag("soak") SoakPerfIT is NEVER on that
// lane — it runs only through this dedicated `soakPerf` task, which the java-conventions plugin
// configures to includeTags("soak") on the integrationTest source set. The full 100M/24h/ZGC run is
// a manual dedicated-host invocation (see SoakPerfIT javadoc for the exact command + JVM flags).
tasks.register<Test>("soakPerf") {
    description = "Runs @Tag(\"soak\") macro/soak performance ITs against real Kafka (design §11.4)."
    group = "verification"
    val integrationTest = sourceSets["integrationTest"]
    testClassesDirs = integrationTest.output.classesDirs
    classpath = integrationTest.runtimeClasspath
    // Same Docker-API and heap settings the integrationTest task uses (the suite block above only
    // configures its own task, not this standalone one).
    systemProperty("api.version", providers.systemProperty("api.version").getOrElse("1.44"))
    // Heap is set HERE (Gradle emits -Xmx/-Xms on the forked test-worker command line), and a
    // command-line -Xmx/-Xms WINS over anything in JAVA_TOOL_OPTIONS. So the dedicated-host command
    // must size the worker through these knobs, NOT through `JAVA_TOOL_OPTIONS=-Xmx12g` (which the
    // hardcoded default below would silently clobber — and a JAVA_TOOL_OPTIONS -Xms12g left larger
    // than the clobbered -Xmx would make the worker refuse to start). `JAVA_TOOL_OPTIONS` carries
    // only the non-conflicting GC / pre-touch / JFR flags (-XX:+UseZGC, -XX:+AlwaysPreTouch,
    // -XX:StartFlightRecording, ...); -Xmx via -Dsoak.maxHeap and -Xms via -Dsoak.minHeap. The CI
    // scaled soak passes neither and keeps the 1536m default (ample for the few-hundred-k run).
    maxHeapSize = providers.systemProperty("soak.maxHeap").getOrElse("1536m")
    providers.systemProperty("soak.minHeap").orNull?.let { minHeapSize = it }
    jvmArgs("-XX:+HeapDumpOnOutOfMemoryError")
    // Forward the documented soak.* sizing knobs (SoakPerfIT reads them via Integer.getInteger /
    // System.getProperty) into the forked test JVM, so the dedicated-host command can scale the run
    // up: `-Dsoak.total=100000000 -Dsoak.windowMs=86400000 ...`. Read through `providers.systemProperty`
    // so each is a proper configuration-cache input (a raw System.getProperty read would be cached and
    // ignore the override).
    for (knob in listOf("soak.total", "soak.windowMs", "soak.partitions", "soak.timeoutMin")) {
        val value = providers.systemProperty(knob).orNull
        if (value != null) {
            systemProperty(knob, value)
        }
    }
    shouldRunAfter(tasks.named("integrationTest"))
}

// Integration tests are run explicitly (CI integration job / nightly matrix), not as part of `check`.
