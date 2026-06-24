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

// The CDK (Micronaut) loads metadata.yaml from the runtime classpath.
tasks.named<ProcessResources>("processResources") {
    from(projectDir) {
        include("metadata.yaml")
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    }
}
