# ADR-0002: Gradle multi-module build with a version catalog

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

The lab has four concerns with different dependency profiles:

1. a **benchmark engine** (Kafka clients, HdrHistogram, Jackson). It needs no Docker.
2. **fault injection** (drives Docker, inspects KRaft through the Admin API).
3. a **CLI** (picocli) that people and CI run.
4. **integration tests** that need a running cluster and take minutes.

If all of this lived in one module, a unit-test run would need Docker, the CLI's dependencies
would leak into the library, and there would be no compile-time boundary keeping the engine free
of Docker code.

## Decision

Use a Gradle (Kotlin DSL) multi-module build:

| Module | Depends on | Purpose |
|---|---|---|
| `kafkacore-bench` | kafka-clients, HdrHistogram, Jackson | workloads, drivers, metrics, reports, regression |
| `kafkacore-chaos` | `kafkacore-bench` | Docker fault injection, cluster inspection, scenarios |
| `kafkacore-cli` | bench + chaos + picocli | `kafkacore` executable (`installDist`) |
| `kafkacore-it` | bench + chaos | `integrationTest` source set against the real cluster |

- Versions live in **one place**, `gradle/libs.versions.toml`.
- Shared compiler and test settings live in a **convention plugin**
  (`buildSrc/.../kafkacore.java-conventions.gradle.kts`): Java 17 toolchain, `-Xlint:all -Werror`,
  JUnit Platform, JaCoCo.
- The Gradle **wrapper** is committed, so builds don't depend on a locally installed Gradle.
- Shared test data uses the `java-test-fixtures` plugin, so the CLI tests reuse the bench fixtures.

## Alternatives considered

- **Maven.** Viable, but Gradle's custom source sets (`integrationTest`) and convention plugins
  take less boilerplate here.
- **Single module plus JUnit tags only.** Simpler, but nothing would stop the engine from
  importing Docker code, and every build would compile everything.

## Consequences

- `./gradlew test` runs the 75 unit tests in about a minute with no Docker.
  `./gradlew :kafkacore-it:integrationTest` is opt-in.
- With `-Werror`, a warning such as a deprecated Kafka API fails the build, which forces clean
  upgrades.
- `buildSrc` slightly slows the first build after a clean checkout.
