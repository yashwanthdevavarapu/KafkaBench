# ADR-0007: Fault injection through the Docker CLI

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

Each chaos scenario needs a reliable way to: hard-crash a broker, stop it gracefully, freeze it
(simulating a GC pause), and cut it off the network. Then it must restore the broker with the
**same identity, data directory and advertised address**, so recovery behaviour (log truncation,
ISR re-join, voter catch-up) can be observed.

## Decision

`DockerFaultInjector` maps each `FaultType` onto a Docker CLI command, run through a small
`CommandExecutor` interface:

| Fault | Inject | Heal | Models |
|---|---|---|---|
| `KILL` | `docker kill --signal SIGKILL` | `docker start` | power loss, OOM kill |
| `STOP` | `docker stop --time 30` (SIGTERM) | `docker start` | rolling restart (controlled shutdown) |
| `PAUSE` | `docker pause` (cgroup freezer) | `docker unpause` | long GC pause, hung disk |
| `NETWORK_PARTITION` | `docker network disconnect` | `docker network connect` | NIC failure, network split |

- Healing is **idempotent**: it checks container state first. Every scenario heals in a `finally`
  block, so an aborted run never leaves the cluster broken.
- `ComposeCluster.ensureHealthy` also repairs leftovers (paused, stopped or detached nodes) before
  each run.
- `CommandExecutor` is an interface, so unit tests check the exact command lines with a recording
  fake and don't need Docker.

## Alternatives considered

- **Testcontainers / docker-java.** Typed API, but tied to Docker Engine API versions. Recent
  Engine releases dropped old API versions that older docker-java clients default to. Its random
  host ports also change across a `docker start`, which breaks advertised listeners.
- **Toxiproxy.** Excellent for latency and bandwidth faults, but it only sits between clients and
  brokers. Inter-broker replication and Raft traffic would bypass it unless every listener were
  proxied, which is a big configuration burden.
- **`tc netem` inside the containers.** Needs `NET_ADMIN` and `iproute2`, which the official image
  doesn't ship. Planned as a future `NETWORK_DELAY` fault.

## Consequences

- No Java dependency on Docker client libraries. The commands are exactly what an operator would
  type, so a scenario can be reproduced by hand.
- Requires the `docker` CLI on `PATH`. The integration tests skip themselves cleanly when no
  daemon is reachable.
- `docker stats` is slow on Docker Desktop (seconds per call). `ResourceMonitor` therefore
  schedules each sampler independently and lets an in-flight sample finish when it stops.
