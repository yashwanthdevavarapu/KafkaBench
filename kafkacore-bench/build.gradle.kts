plugins {
    id("kafkacore.java-conventions")
    `java-library`
    `java-test-fixtures`
}

description = "Benchmark engine: workloads, producer/consumer drivers, latency histograms, reports, regression analysis."

dependencies {
    api(libs.kafka.clients)
    api(libs.jackson.databind)
    implementation(libs.jackson.yaml)
    implementation(libs.jackson.jsr310)
    implementation(libs.hdrhistogram)
    implementation(libs.slf4j.api)
    testRuntimeOnly(libs.slf4j.simple)
}
