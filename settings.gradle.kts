pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        // Airbyte Bulk-Load CDK is published here (public, no auth).
        maven {
            name = "airbyte-public-jars"
            url = uri("https://airbyte.mycloudrepo.io/public/repositories/airbyte-public-jars/")
        }
    }
}

rootProject.name = "airbyte-starrocks"

include(":destination-starrocks")
