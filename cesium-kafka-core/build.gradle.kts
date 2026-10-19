plugins {
    id("cesium.java-conventions")
    `java-library`
}

description = "cesium-kafka engine: ingest/dispatch loops, the tracker store and its in-memory index, transaction management, policies, validation (internal, no compatibility promise)"

dependencies {
    // Kafka client types (headers, Uuid, records) and the MeterRegistry appear in the engine's
    // signatures that the app and the integration tests build against.
    api(libs.kafka.clients)
    api(libs.micrometer.core)
    implementation(libs.fastutil.core)
    implementation(libs.slf4j.api)

    constraints {
        // kafka-clients' transitive codecs, raised to fixed versions; the catalog says why.
        implementation(libs.lz4.java)
        implementation(libs.snappy.java)
    }

    testImplementation(libs.jqwik)
    testImplementation(libs.jol.core)
}

// The JOL footprint gate (ShardFootprintTest) needs self-attach + dynamic agent loading for
// accurate, warning-free object-graph sizing on JDK 21, and --add-opens so JOL can reflect over
// JDK-internal fields; the 1M/10M-entry footprint and 3M growth tests need more heap than Gradle's
// default test JVM provides. Applied to every Test task so the conventions' `soakTest` lane (the
// store contract kit's 1M-entry soak) gets the same headroom.
tasks.withType<Test>().configureEach {
    maxHeapSize = "3g"
    jvmArgs(
        "-Djdk.attach.allowAttachSelf=true",
        "-XX:+EnableDynamicAgentLoading",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
    )
}
