import net.ltgt.gradle.errorprone.errorprone

plugins {
    id("cesium.java-conventions")
    alias(libs.plugins.jmh)
}

description = "JMH micro-benchmarks for the KafkaTrackerStore hot paths (design §11.4). " +
    "NOT CI-gating and NOT wired into build/check — run on demand: ./gradlew :cesium-kafka-benchmarks:jmh"

dependencies {
    // store-kafka exposes the api module (and kafka-clients' Uuid) transitively via java-library
    // `api`, so the benchmarks can construct TrackerIndex/PartitionShard/SidecarCodec/DueEntrySink
    // directly. fastutil stays an `implementation` detail of store-kafka — the public hot-path
    // surface the benchmarks touch never leaks a fastutil type, so it is not needed here.
    jmh(project(":cesium-kafka-store-kafka"))
}

jmh {
    // Reasonable-but-honest settings (design §11.4): enough warmup + measurement for signal, short
    // enough to finish in a few minutes on a dev box. The PER-BENCHMARK mode comes from
    // @BenchmarkMode annotations — destructive/growing hot paths (insert/drain/replay) use
    // SingleShotTime so the heavy per-invocation @Setup that rebuilds a ~1M-entry shard is EXCLUDED
    // from the measured time (Throughput/AverageTime would fold Level.Invocation setup INTO the
    // measurement); non-destructive paths (ring-search, sidecar) use Throughput. benchmarkMode is
    // therefore deliberately left UNSET here: the plugin's benchmarkMode is a CLI override that
    // would clobber the annotations.
    jmhVersion = "1.37"
    warmupIterations = 3
    iterations = 5
    fork = 2
    warmup = "1s" // per warmup iteration (Throughput benches; SingleShot ignores it)
    timeOnIteration = "1s" // per measurement iteration (Throughput benches)
    threads = 1 // design §11.4 targets are single-thread
    failOnError = true
    resultFormat = "JSON"
    resultsFile = layout.buildDirectory.file("results/jmh/results.json")
    humanOutputFile = layout.buildDirectory.file("results/jmh/human.txt")
    // 2 GB headroom: the insert/drain setups build up to ~2M-entry shards (~64 MB of primitive
    // arrays) per invocation; an explicit ceiling keeps GC noise out of the steady-state numbers.
    jvmArgs = listOf("-Xmx2g")
}

// Pin the forked benchmark JVM to the project's JDK 21 toolchain (auto-provisioned by foojay),
// rather than whatever JVM happens to run Gradle — so the numbers are taken on the same runtime
// the rest of the project compiles and tests against (design: Java 21).
val benchmarkLauncher = javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(21) }
jmh { jvm = benchmarkLauncher.map { it.executablePath.asFile.absolutePath } }

// Benchmarks are not production code (design §11.4 / build conventions). The @State objects are
// populated by JMH in @Setup, which NullAway reads as "field never initialized", and the plugin's
// generated benchmark glue is not worth annotating either. Scope-disable NullAway for the `jmh`
// source set ONLY — this configureEach runs AFTER the java-conventions one (the convention plugin
// is applied first), so it wins for the jmh* compile tasks; every other module keeps NullAway=ERROR.
tasks.withType<JavaCompile>().matching { it.name.contains("jmh", ignoreCase = true) }.configureEach {
    options.errorprone.disable("NullAway")
}

// Catch benchmark-source bit-rot on the default lane WITHOUT running JMH. The `jmh` RUN stays off
// build/check (manual / nightly only — see the description), but a signature change to
// PartitionShard/SidecarCodec that breaks src/jmh/java must not slip past `./gradlew build`. Wiring
// the COMPILE of the jmh source set into `check` makes the benchmarks compile on every PR; the heavy
// JMH measurement (compileJmhJava is a dependency of `jmh`, not the reverse) still never runs there.
tasks.named("check") { dependsOn("compileJmhJava") }
