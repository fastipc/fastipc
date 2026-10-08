// The Java binding of FastIPC: the Maven artifact io.github.fastipc:fipc, the module io.github.fastipc.
// The toolchain resolver downloads the build's JDK when none is installed.
plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "fipc"
