# ADR-0009: Separate Docker-free unit tests from cluster integration tests

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

Most of the lab's logic (payload codec, integrity reconciliation, percentile maths, rate pacing,
regression verdicts, report rendering, Docker command construction) is deterministic and needs no
cluster. Other behaviour (KRaft elections, ISR changes, recovery) can only be verified against
real brokers, and those tests take minutes and mutate shared state.

## Decision

Two tiers with a hard boundary:

| Tier | Where | Needs | Runtime | Command |
|---|---|---|---|---|
| **Unit** (75 tests) | `*/src/test/java` | JDK only | ~1 min | `./gradlew test` |
| **Integration** | `kafkacore-it/src/integrationTest/java`, `@Tag("integration")` | Docker | ~10 min | `./gradlew :kafkacore-it:integrationTest` |

- Unit tests replace external systems with Kafka's own **`MockProducer` / `MockConsumer`** (driver
  tests) and a recording **`CommandExecutor`** fake (fault-injector tests).
- `test` tasks exclude the `integration` tag. Integration tests live in a separate source set and
  run with `maxParallelForks = 1`, because they share one cluster and inject faults into it.
- `KafkaClusterExtension` starts or repairs the cluster before integration classes run, and
  **skips** them (not fails) when no Docker daemon is reachable.
- `FaultInjectionIT` turns the chaos scenarios into a resilience **regression suite**: every
  scenario must pass, with 0 acknowledged messages lost.

## Consequences

- Developers get fast, deterministic feedback on every build.
- CI runs unit tests on every push and integration tests in a separate job with Docker.
- Integration tests are inherently timing-sensitive. Recovery timeouts are generous (120 s) and
  assertions are about *outcomes* (a new leader exists, ISR restored, 0 lost), never exact
  durations.
