# ADR-0001: Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

KafkaCore Lab makes many choices that are easy to second-guess later: how latency is measured,
what "no data loss" means, why faults go through Docker. Without a written rationale, someone
looking at a benchmark number months later can't tell whether it is comparable to an older one, or
why a threshold is set where it is.

## Decision

We record every significant decision as an Architecture Decision Record (ADR) in `docs/adr/`,
using Michael Nygard's lightweight format: **Context → Decision → Consequences**, plus the
alternatives we rejected.

- Files are numbered sequentially (`NNNN-kebab-title.md`) and never renumbered.
- An ADR is immutable once accepted. To change a decision, write a new ADR that *supersedes* the
  old one and update the old one's status line.
- Code that implements a decision references it in Javadoc (e.g. `see ADR-0004`).

## Consequences

- Reviewers can check changes against recorded intent.
- Benchmark methodology (ADR-0004, ADR-0005) is explicit, so results stay interpretable.
- There is a small writing cost for each significant change, which we accept.

## Index

| ADR | Title | Status |
|---|---|---|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-gradle-multi-module-build.md) | Gradle multi-module build with a version catalog | Accepted |
| [0003](0003-kraft-combined-mode-cluster.md) | 3-node KRaft cluster in combined mode | Accepted |
| [0004](0004-hdrhistogram-and-coordinated-omission.md) | HdrHistogram latency recording with coordinated-omission correction | Accepted |
| [0005](0005-in-process-end-to-end-latency.md) | End-to-end latency from an embedded timestamp, in one JVM | Accepted |
| [0006](0006-self-verifying-payload-integrity.md) | Self-verifying payloads for message-integrity checks | Accepted |
| [0007](0007-docker-cli-fault-injection.md) | Fault injection through the Docker CLI | Accepted |
| [0008](0008-baseline-regression-gating.md) | Baseline comparison with relative thresholds for regression gating | Accepted |
| [0009](0009-test-pyramid-unit-vs-integration.md) | Separate Docker-free unit tests from cluster integration tests | Accepted |
