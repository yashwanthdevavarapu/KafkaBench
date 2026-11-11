# Fault-injection guide

Each scenario is a small, repeatable experiment: **hypothesis → fault → observation → recovery →
verification**. It runs against the 3-node KRaft cluster while a durable *integrity workload*
produces and consumes continuously.

## The integrity workload

| Setting | Value | Why |
|---|---|---|
| topic | 6 partitions, `replication.factor=3`, `min.insync.replicas=2` | survives one broker loss without losing writes |
| producer | `acks=all`, `enable.idempotence=true`, `delivery.timeout.ms=180000` | retries through elections without duplicates |
| rate | 500–1000 msg/s × 512 B (`--rate`) | steady load that makes availability dips visible |
| consumers | 2 in one group, auto-commit | exercises group-coordinator failover |
| payload | sequence + CRC32C + nanoTime (ADR-0006) | proves exactly which acknowledged writes survived |

## Scenarios

| id | Fault | Hypothesis | Measured |
|---|---|---|---|
| `leader-kill` | SIGKILL the partition-0 leader | The controller fences the broker after `broker.session.timeout.ms`, elects new leaders from the ISR, and loses 0 acked writes. The restarted broker truncates, catches up and re-joins the ISR. | `leaderElectionMs`, `allLeadersMovedMs`, `isrRejoinMs` |
| `controller-failover` | SIGKILL the active KRaft controller | The remaining voters elect a new leader in a higher epoch. Metadata writes (CreateTopics) succeed with 2 of 3 voters. The old voter catches up to lag 0. | `controllerFailoverMs`, `epochBefore/After`, `metadataWriteDuringFailureMs`, `voterCatchUpMs` |
| `broker-pause` | `docker pause` a non-controller leader (GC-pause simulation) | The frozen broker is fenced and leadership moves. Producers retry idempotently. After thawing, the broker re-registers and re-joins the ISR. | `fencedAfterMs`, `leadershipMovedMs`, `rejoinAfterThawMs` |
| `network-partition` | disconnect a follower from `kafkacore-net` | The ISR shrinks after `replica.lag.time.max.ms`, writes continue at min.insync=2, and the ISR expands after the network heals. | `isrShrinkMs`, `fencedAfterMs`, `isrExpandMs` |
| `rolling-restart` | SIGTERM and restart each node, controller last | Controlled shutdown moves leadership away first, so the restart finishes with 0 loss and full ISR between steps. | `node{N}RestartToHealthyMs` |

Every scenario also runs these **common checks**:

- the cluster is fully recovered (quorum leader, 3 unfenced brokers, 0 under-replicated partitions);
- producers make progress after recovery;
- **no acknowledged message lost**;
- no corrupted payloads;
- no producer-side re-ordering (idempotence holds).

## Running

```bash
# all scenarios, CLI
./kafkacore-cli/build/install/kafkacore/bin/kafkacore chaos run all -o results/chaos

# one scenario at a higher rate
./kafkacore-cli/build/install/kafkacore/bin/kafkacore chaos run controller-failover --rate 2000

# as a JUnit regression suite (fails the build if any check fails)
./gradlew :kafkacore-it:integrationTest --tests '*FaultInjectionIT'
```

**Interpreting timings:** recovery timers start when the `docker` command that injected the
fault returns, and that command's own latency is recorded as `injectCommandMs`. Observations are
polled through the Admin API, so reported durations are upper bounds on the protocol timings.
Measured values and their explanations are in [kraft-internals.md](kraft-internals.md).

Outputs: `chaos-results.json` (machine-readable), `chaos-summary.md` (CI summary) and
`chaos-report.html` (charts with fault and heal markers, plus per-scenario timelines).

## Safety

- Every injected fault is registered and **healed in a `finally` block**, even when an
  observation times out or throws.
- `ComposeCluster.ensureHealthy` runs before each suite. It starts stopped nodes, unpauses paused
  ones and re-attaches detached networks left over from a crashed run.
- `restart: "no"` in the compose file means Docker never brings a killed broker back on its own,
  so the scenario controls the timing.

## Adding a scenario

```java
public final class MyScenario extends AbstractScenario {
    @Override public String id() { return "my-scenario"; }
    @Override public String title() { return "Short title"; }
    @Override public String description() { return "Hypothesis in one sentence."; }
    @Override public FaultType fault() { return FaultType.PAUSE; }

    @Override
    protected void execute(Run run) throws Exception {
        var victim = run.topology().node(run.inspector().quorum().leaderId());
        run.inject(victim, FaultType.PAUSE);                    // registered → always healed
        run.awaitAndMeasure("somethingMs", "condition holds",   // metric + check + timeline event
                run.ctx().recoveryTimeout(), () -> /* Admin API predicate */ true);
        run.heal(victim);
    }
}
```

Register it in `ScenarioRegistry`, and add a test method to `FaultInjectionIT` so the scenario
becomes part of the regression suite.
