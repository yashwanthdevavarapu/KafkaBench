# Architecture

KafkaCore Lab has three layers. A **benchmark engine** generates load and measures it. A
**fault-injection layer** breaks the cluster on purpose and watches it recover. A **CLI and test
harness** drives both from a terminal or CI. All of them target a 3-node Apache Kafka 4.0 cluster
running in KRaft mode in Docker.

## Component view

```mermaid
flowchart LR
    subgraph CLI["kafkacore-cli (picocli)"]
        BR[bench run / compare]
        CR[chaos run / list]
        CS[cluster up / status / down]
        RP[report]
    end

    subgraph BENCH["kafkacore-bench"]
        WL[WorkloadLoader<br/>YAML + sweep expansion]
        RUN[BenchmarkRunner]
        SES[WorkloadSession<br/>topic + producers + consumers]
        PW[ProducerWorker<br/>RateLimiter, MessageCodec]
        CW[ConsumerWorker<br/>decode + verify]
        LAT[LatencyRecorder<br/>HdrHistogram]
        INT[IntegrityTracker<br/>bitsets per producer]
        RES[ResourceMonitor<br/>JVM + docker stats]
        REG[RegressionAnalyzer]
        REP[Markdown / CSV / HTML+SVG reports]
    end

    subgraph CHAOS["kafkacore-chaos"]
        SC[Scenarios<br/>leader-kill · controller-failover<br/>broker-pause · network-partition<br/>rolling-restart]
        FI[DockerFaultInjector]
        INS[ClusterInspector<br/>Admin API: quorum, ISR, URP]
        CC[ComposeCluster]
    end

    subgraph KAFKA["Docker: kafkacore-net"]
        K1[(kafka-1<br/>broker+controller)]
        K2[(kafka-2<br/>broker+controller)]
        K3[(kafka-3<br/>broker+controller)]
    end

    BR --> WL --> RUN --> SES
    SES --> PW & CW
    PW --> LAT & INT
    CW --> LAT & INT
    RUN --> RES
    BR --> REG --> REP
    CR --> SC --> SES
    SC --> FI & INS
    CS --> CC
    PW -- produce acks=all --> KAFKA
    CW -- fetch --> KAFKA
    INS -- DescribeMetadataQuorum<br/>DescribeTopics --> KAFKA
    FI -- docker kill/stop/pause<br/>network disconnect --> KAFKA
    RES -- docker stats --> KAFKA
```

## Benchmark run lifecycle

```mermaid
sequenceDiagram
    participant R as BenchmarkRunner
    participant A as Admin
    participant C as ConsumerWorkers
    participant P as ProducerWorkers
    participant K as Kafka (3 nodes)
    participant M as ResourceMonitor

    R->>A: createTopic(partitions, RF, configs)
    A->>K: CreateTopics, then poll until every partition has a leader
    R->>C: start, wait until the group owns every partition
    R->>P: start (open-loop RateLimiter)
    P->>K: produce (seq, nanoTime, CRC)
    K-->>P: ack → publish latency from intended slot
    K-->>C: fetch → verify CRC, e2e latency, mark seq received
    Note over R: wait until pipeline is live (first ack + first consume)
    Note over R: warm-up, then reset histograms
    R->>M: start sampling
    loop every second for durationSec
        R->>R: tick() interval histograms → timeline point
    end
    R->>P: stop + flush (every send acked or failed)
    R->>C: drain until received ⊇ acked
    R->>R: IntegrityTracker.report() → lost / dup / out-of-order / corrupt
    R->>A: deleteTopic
```

## Chaos scenario lifecycle

Every scenario extends `AbstractScenario`, which enforces the same experiment structure:

```mermaid
stateDiagram-v2
    [*] --> PreCheck: cluster healthy?
    PreCheck --> SteadyState: start integrity workload<br/>(acks=all, idempotent, RF=3, min.insync=2)
    SteadyState --> Inject: scenario-specific fault
    Inject --> Observe: measure detection, election, ISR shrink
    Observe --> Heal: restart / unpause / reconnect
    Heal --> Recover: wait for quorum + 3 brokers + 0 URP
    Recover --> Verify: steady state, stop, drain
    Verify --> [*]: checks: 0 lost, 0 corrupt,<br/>no re-ordering, progress resumed
    Inject --> Heal: on exception (finally)
```

## Module dependency graph

```mermaid
flowchart BT
    bench[kafkacore-bench]
    chaos[kafkacore-chaos] --> bench
    cli[kafkacore-cli] --> chaos
    cli --> bench
    it[kafkacore-it<br/>integrationTest] --> chaos
    it --> bench
```

## Key types

| Type | Module | Responsibility |
|---|---|---|
| `WorkloadSpec` / `WorkloadLoader` | bench | YAML schema, defaults, validation, sweep expansion |
| `MessageCodec` | bench | 25-byte self-verifying header (ADR-0006) |
| `RateLimiter` | bench | open-loop pacing, intended-time latency (ADR-0004) |
| `ProducerWorker` / `ConsumerWorker` | bench | one thread each, latency + integrity bookkeeping |
| `WorkloadSession` | bench | topic, consumer group and producer lifecycle (shared by benchmarks and chaos) |
| `LatencyRecorder` | bench | HdrHistogram `Recorder`, interval ticks + cumulative summary |
| `IntegrityTracker` | bench | acked / failed / received bitsets → `IntegrityReport` |
| `ResourceMonitor` | bench | per-sampler schedules, avg/max aggregation |
| `RegressionAnalyzer` | bench | baseline matching, thresholds, PASS/WARN/FAIL (ADR-0008) |
| `ClusterInspector` | chaos | KRaft quorum, live brokers, partition leader/ISR, URP |
| `DockerFaultInjector` | chaos | KILL / STOP / PAUSE / NETWORK_PARTITION (ADR-0007) |
| `AbstractScenario` | chaos | experiment template, timeline, checks, guaranteed heal |
| `KafkaCoreCli` | cli | `bench`, `chaos`, `cluster`, `report` commands and exit codes |
