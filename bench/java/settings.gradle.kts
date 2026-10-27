// The Java benchmark (bench/java): one-way throughput through the Java binding, which this build includes from
// bindings/java (a composite build). Run it with the binding's wrapper: bindings/java/gradlew -p bench/java run --args=copy
plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "fastipc-bench"
includeBuild("../../bindings/java")
