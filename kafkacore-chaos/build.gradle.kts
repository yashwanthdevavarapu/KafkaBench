plugins {
    id("kafkacore.java-conventions")
    `java-library`
}

description = "Fault injection: Docker-driven broker faults, KRaft/cluster inspection and chaos scenarios."

dependencies {
    api(project(":kafkacore-bench"))
    implementation(libs.slf4j.api)
    testRuntimeOnly(libs.slf4j.simple)
}
