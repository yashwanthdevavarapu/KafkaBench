# ADR-0003: 3-node KRaft cluster in combined mode

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

Since Apache Kafka 4.0, ZooKeeper is gone and **KRaft** is the only metadata mode. A Raft quorum
of controllers owns the `__cluster_metadata` log. The lab has to study controller behaviour
(elections, epochs, voter lag) as well as data-plane replication (leaders, ISR,
`min.insync.replicas`), and it must run on a developer laptop.

KRaft offers two deployment shapes:

- **Isolated mode:** dedicated controller nodes (usually 3) plus separate broker nodes.
- **Combined mode:** every node runs `process.roles=broker,controller`.

## Decision

Run **3 nodes in combined mode** (`docker/docker-compose.yml`, image `apache/kafka:4.0.0`):

- 3 voters → the quorum tolerates **1** controller failure (majority = 2).
- 3 brokers → topics use `replication.factor=3` and `min.insync.replicas=2`, which tolerates
  **1** broker failure with `acks=all` and loses no acknowledged data.
- `unclean.leader.election.enable=false`, so an out-of-sync replica can never become leader.
- Failure-detection settings are chosen to be observable, and every value is documented in the
  compose file: `replica.lag.time.max.ms=10s` (default 30s), with Kafka defaults for
  `broker.session.timeout.ms` (9s) and the Raft election and fetch timeouts.
- Fixed, distinct host ports (`19092/29092/39092`) are advertised as `localhost:<port>`, so clients
  on the host reach each broker directly and a node keeps its address across `docker stop/start`.
- Heap is capped at 384 MB per node, so the whole lab fits on an 8 GB machine.
- The container healthcheck is a cheap TCP probe (`nc -z`). Deep readiness (quorum leader, all
  brokers unfenced, 0 under-replicated partitions) is checked through the Admin API by
  `ComposeCluster.ensureHealthy`.

## Alternatives considered

- **Isolated mode (3 controllers + 3 brokers).** Closer to large production deployments, and
  killing a controller leaves the brokers alone. But it doubles the JVM count (about 2.5 GB
  extra), which this lab's target hardware can't afford. The scenario framework is
  topology-driven (`ClusterTopology`), so an isolated-mode compose file can be added without code
  changes.
- **Single node.** Can't show replication, ISR shrink, or quorum elections.
- **Testcontainers-managed cluster.** Random host ports change when a container restarts, which
  breaks advertised listeners in kill/restart scenarios (see ADR-0007).

## Consequences

- Killing the active controller also kills a broker. `ControllerFailoverScenario` measures both
  effects together, and its report says so.
- The healthcheck lesson is recorded in the compose file. The original probe,
  `kafka-broker-api-versions.sh`, started a JVM every 5 s in each container. On a constrained host
  that drove the VM's load to about 10, starved the brokers, and caused repeated KRaft
  re-elections (epoch 27 within 30 minutes of idling). Switching to `nc -z` brought idle broker
  CPU down from about 200% to under 15%.
