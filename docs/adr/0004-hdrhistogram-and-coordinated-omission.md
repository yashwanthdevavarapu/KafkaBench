# ADR-0004: HdrHistogram latency recording with coordinated-omission correction

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

The project reports **p95 and p99** latency and gates regressions on them, so tail percentiles
must be accurate and cheap to record from many threads.

Two classic mistakes would make the tail numbers wrong:

1. **Averaging or sampling.** A mean hides the tail. A reservoir sample of 1,000 values can't
   resolve p99.9 reliably.
2. **Coordinated omission** (described by Gil Tene). A closed-loop load generator that waits for
   slow responses also *stops sending* while the system is stalled. It never records the requests
   that would have queued up during the stall, so the measured tail looks far better than what
   users would see.

## Decision

- Record every latency in an **HdrHistogram** `Recorder`: lock-free for writers, 3 significant
  digits, microsecond resolution, range up to 5 minutes. Values are never sampled or dropped.
- A single reader calls `tick()` once per second. It gets an *interval* histogram (used for the
  per-second p99 timeline charts) and folds it into a *cumulative* histogram (used for the final
  p50/p95/p99/p99.9/max).
- The warm-up window is discarded with `reset()`.
- Load generation is **open loop**. `RateLimiter` hands out send slots on a fixed schedule, and
  `acquire()` returns the slot's **intended** start time. When a rate is set, publish latency is
  measured from that intended time, not from when `send()` was actually called. If the producer
  stalls for 2 s, every queued message is charged the stall time it really experienced.
- Unthrottled runs (`targetRatePerSec: 0`) measure from the actual send time, because no schedule
  exists for them.

## Alternatives considered

- **Kafka client JMX metrics** (`request-latency-avg/max`). These cover per-request broker
  round-trips only, have no percentiles, and don't include accumulator queueing.
- **Micrometer timers.** They add a dependency and still need HdrHistogram underneath for exact
  percentiles.

## Consequences

- Under overload, reported latencies grow steadily as backlog builds. That is the intended
  behaviour: the numbers reflect what an application producing at that rate would see.
- Histograms take a few hundred KB each, which is negligible.
- Percentiles carry up to 0.1% value error (3 significant digits), well below run-to-run noise.
