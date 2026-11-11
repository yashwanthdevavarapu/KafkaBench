// Root project: modules are configured through the `kafkacore.java-conventions` plugin in buildSrc.
plugins {
    base
}

tasks.register("unitTest") {
    group = "verification"
    description = "Runs all Docker-free unit tests across modules."
    dependsOn(subprojects.mapNotNull { it.tasks.findByName("test") })
}
