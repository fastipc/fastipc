// The Java benchmark: `python devtool.py bench java-fastipc` (java-fastipc-zerocopy, java-rpc) runs it, on the
// binding's toolchain JDK, against the library the property fipcLibrary names (devtool: zig-out/bench's, the release
// build), else the repository's zig-out build (the binding's development fallback).
plugins {
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.github.fastipc:fipc") // the included build, bindings/java
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.release = 22
    options.encoding = "UTF-8"
}

application {
    mainClass = "FipcBench"
    applicationDefaultJvmArgs = listOf("--enable-native-access=ALL-UNNAMED")
}

tasks.named<JavaExec>("run") {
    providers.gradleProperty("fipcLibrary").orNull?.let { systemProperty("fastipc.library.path", it) }
}
