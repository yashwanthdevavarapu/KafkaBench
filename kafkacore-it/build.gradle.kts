plugins {
    id("kafkacore.java-conventions")
}

description = "Integration, fault-injection and regression tests against a real 3-node KRaft cluster (Docker)."

val integrationTest: SourceSet by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output
    runtimeClasspath += sourceSets.main.get().output
}

configurations[integrationTest.implementationConfigurationName].extendsFrom(configurations.testImplementation.get())
configurations[integrationTest.runtimeOnlyConfigurationName].extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    "integrationTestImplementation"(project(":kafkacore-bench"))
    "integrationTestImplementation"(project(":kafkacore-chaos"))
    "integrationTestImplementation"(libs.awaitility)
    "integrationTestRuntimeOnly"(libs.slf4j.simple)
}

val integrationTestTask = tasks.register<Test>("integrationTest") {
    group = "verification"
    description = "Runs cluster integration + fault-injection tests. Requires Docker (starts docker/docker-compose.yml)."
    testClassesDirs = integrationTest.output.classesDirs
    classpath = integrationTest.runtimeClasspath
    useJUnitPlatform { includeTags("integration") }
    workingDir = rootProject.projectDir
    systemProperty("kafkacore.root", rootProject.projectDir.absolutePath)
    systemProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")
    // Tests share one cluster and inject faults into it, so they must not run concurrently.
    maxParallelForks = 1
    shouldRunAfter(tasks.test)
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
