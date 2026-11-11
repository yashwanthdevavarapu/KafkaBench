# KRaft internals: observations from the lab

This file records what the fault-injection scenarios showed about Apache Kafka 4.0 in KRaft mode,
and explains each observation using the mechanism and configuration behind it. Every number below
comes from the JSON results of real runs against the 3-node combined-mode cluster
(`docker/docker-compose.yml`).

> **Test environment:** a Windows 11 laptop with 8 logical CPUs and 7.7 GB RAM, running Docker
> Desktop (WSL2 VM capped at 3.7 GB). The host was **heavily memory-constrained** during all runs:
> about 0.5 GB free, ~20k pages/s of paging, CPU close to 100%. Absolute latencies are therefore
> far higher than on a server. The *mechanisms* and the *relative* timings are what matter here,
> and the integrity guarantees held regardless.

## Relevant configuration

| Setting | Value | Role |
|---|---|---|
| `process.roles` | `broker,controller` | combined mode: each node is a Raft voter **and** a broker |
| `controller.quorum.voters` | `1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093` | static 3-voter quorum (tolerates 1 failure) |
| `controller.quorum.election.timeout.ms` | 1000 | how long a voter waits before standing as candidate |
| `controller.quorum.fetch.timeout.ms` | 2000 | a follower with no successful fetch from the leader for this long starts an election |
| `broker.session.timeout.ms` | 9000 | the active controller fences a broker after missing heartbeats for this long |
| `broker.heartbeat.interval.ms` | 2000 | broker → controller heartbeat period |
| `replica.lag.time.max.ms` | 10000 | a follower not caught up for this long is removed from the ISR |
| `min.insync.replicas` | 2 | with `acks=all`, a write needs 2 in-sync copies |
| `unclean.leader.election.enable` | false | never elect an out-of-sync replica |

## Measured timings

Two complete runs are shown: **A** = `kafkacore chaos run` (CLI) and **B** = `FaultInjectionIT`
(JUnit). Recovery timers start when the `docker` command that injected the fault *returns*
(`injectCommandMs` is how long that command took; see "Measurement caveats").

| Observation | Run A | Run B | Explained by |
|---|---:|---:|---|
| **leader-kill**: new leader for p0 | 7.3 s (+1.9 s docker) | 6.1 s (+2.5 s docker) | ≈ 9 s session timeout → fence → ISR election |
| leader-kill: all victim's partitions moved | +53 ms | +12 ms | one metadata record batch moves them all |
| leader-kill: restarted broker back in every ISR | 23.0 s | 32.0 s | JVM start + log recovery + catch-up fetch |
| **controller-failover**: new active controller | 20.9 s | 10.6 s | Raft election, observed through the Admin API (upper bound) |
| controller-failover: epoch | 97 → 98 | 103 → 104 | exactly one Raft term bump per failover |
| controller-failover: CreateTopics with 2/3 voters | 218 ms | 364 ms | majority (2) still commits metadata |
| controller-failover: restarted voter at lag 0 | 43.3 s | 38.1 s | restart + fetch of `__cluster_metadata` from the new leader |
| **broker-pause**: frozen broker fenced | 8.2 s (+1.0 s docker) | 9.3 s (+0.3 s docker) | **= `broker.session.timeout.ms` (9 s)** |
| broker-pause: leadership moved after fencing | 4 ms | 12 ms | fencing and leader election are one controller decision |
| broker-pause: re-joined ISR after thaw | 0.9 s | 0.9 s | data was only seconds behind; no restart needed |
| **network-partition**: follower dropped from ISR | 7.4 s (+1.7 s docker) | 8.5 s (+0.6 s docker) | fencing at ~9 s beats `replica.lag.time.max.ms` (10 s) |
| network-partition: fenced after ISR shrink | +5 ms | +2 ms | the same controller event removes it from the ISR |
| network-partition: ISR fully expanded after heal | 13.4 s | 13.5 s | re-register + catch up + leader's ISR-expand check |
| **rolling-restart**: per-node restart → healthy | 17–22 s | 24–29 s | controlled shutdown, restart, catch-up |
| **Acked messages verified / lost** | 155,500 / **0** | 155,773 / **0** | `acks=all` + min.insync=2 + no unclean election |

## What the observations show

### 1. Broker failure detection is governed by `broker.session.timeout.ms`, not by TCP

In KRaft, brokers prove they're alive by sending `BrokerHeartbeat` requests to the active
controller every `broker.heartbeat.interval.ms`. When a broker dies, freezes or is isolated, the
controller notices only once heartbeats have been missing for `broker.session.timeout.ms` (9 s).
It then **fences** the broker: it writes a `BrokerRegistrationChangeRecord` to the metadata log,
which removes the broker from every ISR and elects new leaders for its partitions in the same
batch.

The data confirms this precisely. In *broker-pause*, the time from `docker pause` returning to
fencing was 9.3 s with a 0.3 s Docker call, which is exactly the session timeout. Leadership then
moved 4–12 ms after fencing, so election is not a separate step. In *leader-kill*, the election
time plus the Docker call time adds up to the same ~9 s.

**Consequence:** for a hard broker crash, the window in which affected partitions are unavailable
for `acks=all` writes is about `broker.session.timeout.ms`. Lowering it speeds up failover, but
risks fencing brokers that are only slow (for example during long GC pauses). That trade-off is
exactly what *broker-pause* demonstrates.

### 2. ISR shrink came from fencing, not from replica lag

In *network-partition*, the isolated follower left the ISR **2–5 ms before it was reported as
fenced**. The two are the same controller decision. Fencing (9 s) happens before the leader's own
`replica.lag.time.max.ms` check (10 s) would remove the follower. With these settings, the
controller's liveness signal is what shrinks the ISR. The lag check matters for followers that
are alive but slow (heartbeating, yet not fetching fast enough), which a frozen or isolated node
is not.

### 3. Controller failover: one Raft term, metadata keeps flowing

Killing the active controller always produced **exactly one epoch increment** (97→98, 103→104;
54→55 in an earlier run). That means a single clean election with no split votes: the remaining
two voters noticed the missing leader through `controller.quorum.fetch.timeout.ms`, one stood as
candidate, and it won the majority (2 of 3).

While one voter was down, `CreateTopics` committed in 218–364 ms. That is the defining Raft
property: a majority is enough to commit. The restarted voter rejoined as a follower and caught
up on `__cluster_metadata` to lag 0 within about 40 s, most of which was JVM start-up on the
starved host.

The measured failover time varied widely (98 ms in a first run, 10.6 s and 20.9 s later). See
the caveats below. The protocol needs about the fetch timeout plus the election timeout (around
3 s) on a healthy host, and the variation comes from how fast the Admin client could re-discover
the new controller under heavy CPU and memory pressure.

### 4. Combined mode couples failures

In combined mode, killing the active controller also kills a broker, so *controller-failover*
measures two overlapping effects: Raft leader election **and** broker fencing for the partitions
that node led. In isolated mode, a controller crash would leave the data plane alone. This is the
main trade-off recorded in ADR-0003.

### 5. A frozen broker is not a dead broker

After `docker unpause`, the frozen broker found it had been fenced, re-registered, and re-joined
every ISR in **0.9 s**. A killed broker took 23–32 s. The frozen JVM kept its page cache, open
segments and in-memory state: it only needed to fetch a few seconds of data. This is why long
GC pauses cause leadership churn without a long recovery.

### 6. Durability held under every fault

Across both runs, **311,273 acknowledged messages** were produced during faults. Every one was
consumed, CRC-valid, with **0 lost, 0 corrupt, 0 re-ordered** (ADR-0006). That follows from:

- `acks=all` with `min.insync.replicas=2`: an acked write exists on at least 2 replicas;
- leaders are elected only from the ISR (`unclean.leader.election.enable=false`), so the new
  leader always holds every acked write;
- the idempotent producer (sequence numbers per partition) made retries during leader changes
  safe from duplication and re-ordering.

### 7. The whole cluster survives a "power cut"

During the session, the Docker Desktop VM itself hung under host memory pressure and was
restarted. All three containers exited with code 255 at once, which is the equivalent of a
data-centre power loss. On restart, the brokers recovered their logs, the quorum re-elected
(epoch 68 → 83, reflecting the election rounds while nodes came back one by one), and
`kafkacore cluster status` reported **HEALTHY with 0 under-replicated partitions**. No manual
intervention was needed.

## Measurement caveats

- **Timers start when the `docker` command returns.** On this host, `docker kill` took 1–5 s, and
  the process may have died before the command returned. `injectCommandMs` is recorded with every
  scenario so this can be accounted for. Wall-clock time from the actual failure is therefore
  somewhere between the reported value and that value plus `injectCommandMs`.
- **Observations are made through the Admin API**, polled every 200 ms with 5 s request
  timeouts. While a node is down, a request routed to it can take up to the timeout before
  another node is tried. So the reported times are **upper bounds** on the protocol's own
  timings. This matters most for `controllerFailoverMs`.
- The broker logs (`docker logs kafka-N | grep RaftManager`) contain the exact protocol
  timestamps for anyone who needs millisecond precision.

## Reproduce

```bash
kafkacore cluster up
kafkacore chaos run all -o results/chaos          # JSON + Markdown + HTML
docker logs kafka-2 2>&1 | grep -E "RaftManager.*(Candidate|Leader)"   # Raft state transitions
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
    --bootstrap-server localhost:9092 describe --status                  # quorum, epoch, lag
```
