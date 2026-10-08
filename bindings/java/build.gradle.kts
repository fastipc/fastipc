// The Java binding of FastIPC: io.github.fastipc:fipc, the module io.github.fastipc.
//
// The library is compiled for Java 22 (the FFM API, java.lang.foreign) by a JDK 25 toolchain, which the foojay
// resolver downloads when none is installed. The jar carries the native libraries it is given:
//
//   ./gradlew test                          the integration tests, against the repository's zig-out build
//   ./gradlew jar -PfipcNativeDir=DIR       a jar with DIR/linux-x86_64/libfastipc.so, DIR/windows-x86_64/fastipc.dll,
//                                           DIR/macos-aarch64/libfastipc.dylib and DIR/linux-aarch64/libfastipc.so
//                                           (those of them DIR holds)
//   ./gradlew publishMavenPublicationToStagingRepository -PfipcNativeDir=DIR -PfipcStagingRepo=REPO
//                                           the jar, sources, javadoc and POM in a Maven repository folder, signed
//                                           when a key is given (signingKey, signingPassword); `devtool package java`
//                                           runs it. Nothing here publishes to a remote repository.

plugins {
    `java-library`
    `maven-publish`
    signing
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
    withSourcesJar()
    withJavadocJar()
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.14.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaCompile>().configureEach {
    options.release = 22
    options.encoding = "UTF-8"
    // -restricted: the binding's calls of restricted FFM methods (the linker, reinterpret) are its purpose
    options.compilerArgs.addAll(listOf("-Xlint:all,-restricted", "-Werror"))
}

tasks.compileJava {
    options.javaModuleVersion = provider { project.version.toString() }
}

// The native libraries the jar carries, in the binding's resource folder (NativeLoader reads them from there)
val nativeDir = providers.gradleProperty("fipcNativeDir")

tasks.jar {
    if (nativeDir.isPresent) {
        from(nativeDir) {
            include(
                "linux-x86_64/libfastipc.so", "windows-x86_64/fastipc.dll", "macos-aarch64/libfastipc.dylib",
                "linux-aarch64/libfastipc.so",
            )
            into("io/github/fastipc/native")
        }
    }
    from(rootDir.resolve("../../LICENSE")) {
        into("META-INF")
    }
    manifest {
        attributes(
            "Implementation-Title" to "fipc",
            "Implementation-Version" to project.version,
            "Implementation-Vendor" to "Hayden Donnelly",
        )
    }
}

tasks.named<Jar>("sourcesJar") {
    from(rootDir.resolve("../../LICENSE")) {
        into("META-INF")
    }
}

tasks.javadoc {
    (options as StandardJavadocDocletOptions).apply {
        encoding = "UTF-8"
        docTitle = "fipc ${project.version}"
        windowTitle = "fipc ${project.version}"
        addBooleanOption("Xdoclint:all", true)
        addBooleanOption("Werror", true)
        addBooleanOption("-no-fonts", true) // the system's fonts: no 3 MB of web fonts in the javadoc jar
    }
}

// The tests run against the repository's own build (zig-out), found by the binding's development fallback. The
// two-process tests start peers on the test classpath; the cross-language test runs the Python binding with the
// repository's venv (or fipcPython).
val repoRoot = rootDir.resolve("../..").canonicalFile
val python = providers.gradleProperty("fipcPython").orElse(provider {
    listOf("venv/Scripts/python.exe", "venv/bin/python").map(repoRoot::resolve).firstOrNull { it.exists() }?.path
        ?: if (System.getProperty("os.name").startsWith("Windows")) "python" else "python3"
})

tasks.test {
    useJUnitPlatform()
    jvmArgs("--enable-native-access=ALL-UNNAMED")
    systemProperty("fastipc.test.repo", repoRoot.path)
    systemProperty("fastipc.test.python", python.get())
    systemProperty("fastipc.test.classpath", sourceSets.test.get().runtimeClasspath.asPath)
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showStandardStreams = false
    }
    // The tests talk to processes and to the native library, which isn't an input: always run them when asked, never
    // take their results from the build cache
    outputs.upToDateWhen { false }
    outputs.cacheIf { false }
}

// The toolchain's Java home, for devtool (its smoke test and the Java benchmark run that JDK)
tasks.register("printJavaHome") {
    val launcher = javaToolchains.launcherFor(java.toolchain)
    doLast {
        println("JAVA_HOME=" + launcher.get().metadata.installationPath.asFile.absolutePath)
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "fipc"
            from(components["java"])
            pom {
                name = "fipc"
                description = "Shared-memory IPC for Java: messages and RPC between two processes on one machine, the " +
                    "Java binding of FastIPC, a small library written in Zig, over the FFM API (Java 22+). Native " +
                    "libraries for Windows x64, Linux x64 and arm64 (glibc 2.34+) and macOS arm64 (14.4+) included; " +
                    "x86-64-v3 CPUs."
                url = "https://fastipc.github.io/fastipc/"
                inceptionYear = "2025"
                licenses {
                    license {
                        name = "MIT"
                        url = "https://opensource.org/license/mit"
                        distribution = "repo"
                    }
                }
                developers {
                    developer {
                        name = "Hayden Donnelly"
                        email = "austecon0922@gmail.com"
                        organization = "fastipc"
                        organizationUrl = "https://github.com/fastipc"
                    }
                }
                scm {
                    url = "https://github.com/fastipc/fastipc"
                    connection = "scm:git:https://github.com/fastipc/fastipc.git"
                    developerConnection = "scm:git:ssh://git@github.com/fastipc/fastipc.git"
                }
                issueManagement {
                    system = "GitHub"
                    url = "https://github.com/fastipc/fastipc/issues"
                }
            }
        }
    }
    repositories {
        // A folder in the Maven repository layout: what a Maven Central upload bundle holds
        maven {
            name = "staging"
            url = uri(providers.gradleProperty("fipcStagingRepo").orElse(layout.buildDirectory.dir("staging-repo").map { it.asFile.path }))
        }
    }
}

// Signing, only when a key is given: the ASCII-armored private key and its password as the Gradle properties
// signingKey and signingPassword (or the environment variables ORG_GRADLE_PROJECT_signingKey and
// ORG_GRADLE_PROJECT_signingPassword). Maven Central requires the signatures; local builds don't.
val signingKey = providers.gradleProperty("signingKey")
signing {
    isRequired = signingKey.isPresent
    if (signingKey.isPresent) {
        useInMemoryPgpKeys(signingKey.get(), providers.gradleProperty("signingPassword").orNull)
        sign(publishing.publications["maven"])
    }
}
