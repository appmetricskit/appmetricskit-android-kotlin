plugins {
    alias(libs.plugins.android.library)
    alias(libs.plugins.kotlin.android)
    alias(libs.plugins.vanniktech.maven.publish)
}

group = "com.appmetricskit"
version = providers.gradleProperty("VERSION_NAME").orElse("0.1.0").get()

android {
    namespace = "com.appmetricskit"
    compileSdk = 36

    defaultConfig {
        minSdk = 23
        consumerProguardFiles("consumer-rules.pro")
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    kotlinOptions {
        jvmTarget = "17"
    }

    buildFeatures {
        buildConfig = true
    }

    testOptions {
        unitTests.isReturnDefaultValues = true
    }
}

dependencies {
    testImplementation(libs.junit)
    testImplementation(libs.json)
}

mavenPublishing {
    publishToMavenCentral()

    if (
        providers.gradleProperty("signingInMemoryKey").isPresent ||
        providers.gradleProperty("signing.secretKeyRingFile").isPresent
    ) {
        signAllPublications()
    }

    coordinates(
        groupId = "com.appmetricskit",
        artifactId = "appmetricskit-android",
        version = project.version.toString(),
    )

    pom {
        name.set("AppMetricsKit Android SDK")
        description.set("Privacy-first Android analytics SDK for AppMetricsKit.")
        inceptionYear.set("2026")
        url.set("https://github.com/appmetricskit/appmetricskit-android-kotlin")

        licenses {
            license {
                name.set("MIT License")
                url.set("https://opensource.org/license/mit/")
                distribution.set("repo")
            }
        }

        developers {
            developer {
                id.set("appmetricskit")
                name.set("AppMetricsKit")
                url.set("https://github.com/appmetricskit")
            }
        }

        scm {
            url.set("https://github.com/appmetricskit/appmetricskit-android-kotlin")
            connection.set("scm:git:https://github.com/appmetricskit/appmetricskit-android-kotlin.git")
            developerConnection.set("scm:git:ssh://git@github.com:appmetricskit/appmetricskit-android-kotlin.git")
        }
    }
}
