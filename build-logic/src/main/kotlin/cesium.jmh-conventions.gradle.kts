// The java conventions PLUS the JMH plugin, for the benchmarks module (design §11.4).
//
// This exists so that no build script outside build-logic ever adds a plugin to its own `plugins {}`
// block. Gradle caches one plugin classloader per distinct classpath: when cesium-kafka-benchmarks
// applied the JMH plugin directly, its classpath differed from every sibling module's, so it got a
// separate classloader — and Spotless's SpotlessTaskService, a build-scoped BuildService, was then
// loaded twice. Spotless 8.x rejects that ("Cannot set the value of task
// ':cesium-kafka-benchmarks:spotlessJava' property 'taskService' ... loaded with
// InstrumentingVisitableURLClassLoader(...project-cesium-kafka-benchmarks) using a provider of type
// ... loaded with ...(project-cesium-kafka-api)"); 7.x simply never performed the check.
//
// Applying JMH from inside a precompiled script plugin puts it on build-logic's runtime classpath,
// which every module already shares, so all subprojects resolve to a single plugin classloader.
// A bare `id("me.champeau.jmh")` in the benchmarks build script cannot replace this: Gradle resolves
// a versionless plugin id only from core plugins, a PARENT classloader scope, or plugins an included
// build declares it provides — never from a jar that merely happens to be on another plugin's
// runtime classpath.
plugins {
    id("cesium.java-conventions")
    id("me.champeau.jmh")
}
