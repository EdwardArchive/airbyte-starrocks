import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    application
    kotlin("jvm") version "2.0.20"
    id("com.google.devtools.ksp") version "2.0.20-1.0.25"
}

// Airbyte Bulk-Load CDK (see STARROCKS-VERSION-COMPATIBILITY.md / epic #6).
// We wire the CDK dependencies directly instead of vendoring the monorepo-only
// `airbyte-bulk-connector` Gradle plugin (which depends on monorepo paths and Develocity).
val cdkVersion = "1.0.13"
val cdkBaseVersion = "1.0.3"
val micronautVersion = "4.6.1"

dependencies {
    // Micronaut DI: KSP generates bean definitions for this connector's @Singleton/@Factory classes.
    implementation(platform("io.micronaut:micronaut-core-bom:$micronautVersion"))
    ksp(platform("io.micronaut:micronaut-core-bom:$micronautVersion"))
    ksp("io.micronaut:micronaut-inject-kotlin")

    // Bulk-Load CDK core + HTTP toolkit (for the Stream Load client, issue #9).
    implementation("io.airbyte.bulk-cdk:bulk-cdk-core-base:$cdkBaseVersion")
    implementation("io.airbyte.bulk-cdk:bulk-cdk-core-load:$cdkVersion")
    implementation("io.airbyte.bulk-cdk:bulk-cdk-toolkit-load-http:$cdkVersion")

    // 9030 MySQL protocol: DDL + `SELECT current_version()` (used from issue #8 onward).
    implementation("com.mysql:mysql-connector-j:8.4.0")

    // OkHttp for the Stream Load client (also transitive via toolkit-load-http; pinned to match).
    implementation("com.squareup.okhttp3:okhttp:4.12.0")

    // zstd codec for Stream Load request-body compression (gzip is JVM-built-in; zstd is not).
    // Bundles the native libs for the connector's runtime platforms.
    implementation("com.github.luben:zstd-jni:1.5.7-11")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.10.2")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
    testImplementation("com.squareup.okhttp3:okhttp-tls:4.12.0")
    // For the LoadFormatSpec oneOf round-trip test (deserialize the discriminated union).
    testImplementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.18.3")
}

application {
    mainClass.set("io.airbyte.integrations.destination.starrocks.StarrocksDestinationKt")
    applicationDefaultJvmArgs = listOf(
        "-XX:+ExitOnOutOfMemoryError",
        "-XX:MaxRAMPercentage=75.0",
    )
}

// Compile with the JVM running Gradle (JDK 21) rather than a downloaded toolchain.
kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_21)
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

// The CDK (Micronaut) loads metadata.yaml from the runtime classpath.
tasks.named<ProcessResources>("processResources") {
    from(projectDir) {
        include("metadata.yaml")
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    }
}
