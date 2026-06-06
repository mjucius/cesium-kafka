plugins {
    id("cesium.java-conventions")
    `java-library`
}

description = "KafkaTrackerStore: the flagship tracker-topic-backed scheduler store (fastutil-backed in-memory index)"

dependencies {
    api(project(":cesium-kafka-api"))
    implementation(libs.fastutil.core)
    implementation(libs.kafka.clients)
    implementation(libs.slf4j.api)

    testImplementation(project(":cesium-kafka-store-testkit"))
    testImplementation(libs.jqwik)
    testImplementation(libs.jol.core)
}

// Single permitted M2 build edit: the JOL footprint gate (ShardFootprintTest) needs self-attach +
// dynamic agent loading for accurate, warning-free object-graph sizing on JDK 21, and --add-opens
// so JOL can reflect over JDK-internal fields; the 1M/10M-entry footprint and 3M growth tests
// need more heap than Gradle's default test JVM provides. Applied to every Test task so the
// conventions' `soakTest` lane (the contract kit's 1M-entry soak) gets the same headroom.
tasks.withType<Test>().configureEach {
    maxHeapSize = "3g"
    jvmArgs(
        "-Djdk.attach.allowAttachSelf=true",
        "-XX:+EnableDynamicAgentLoading",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
    )
}
