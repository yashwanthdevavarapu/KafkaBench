plugins {
    id("kafkacore.java-conventions")
    application
}

description = "Command-line entry point: `kafkacore bench|chaos|cluster|report`."

dependencies {
    implementation(project(":kafkacore-bench"))
    implementation(project(":kafkacore-chaos"))
    implementation(libs.picocli)
    runtimeOnly(libs.slf4j.simple)
    testImplementation(testFixtures(project(":kafkacore-bench")))
}

application {
    mainClass.set("io.kafkacore.lab.cli.KafkaCoreCli")
    applicationName = "kafkacore"
    applicationDefaultJvmArgs = listOf("-Xms128m", "-Xmx512m", "-Dfile.encoding=UTF-8", "-Dstdout.encoding=UTF-8", "-Dorg.slf4j.simpleLogger.defaultLogLevel=error")
}

tasks.named<JavaExec>("run") {
    workingDir = rootProject.projectDir
}
