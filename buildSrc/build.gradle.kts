plugins {
    `kotlin-dsl`
}

repositories {
    gradlePluginPortal()
    mavenCentral()
}

dependencies {
    // Exposes the version catalog (gradle/libs.versions.toml) to precompiled convention plugins.
    implementation(files(libs.javaClass.superclass.protectionDomain.codeSource.location))
}
