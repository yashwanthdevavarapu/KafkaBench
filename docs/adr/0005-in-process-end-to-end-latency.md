# ADR-0005: End-to-end latency from an embedded timestamp, in one JVM

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

Publish (ack) latency tells only half the story. Users care about **end-to-end** latency: the time
from `send()` until a consumer holds the record. That includes replication to the ISR, the
high-watermark advance, and the consumer's fetch. Measuring it requires a timestamp from the
producing side to be compared with "now" on the consuming side.

If producer and consumer run on different hosts, wall-clock skew (often milliseconds under NTP)
corrupts the measurement. Kafka's record `CreateTime` has millisecond resolution and is set by the
client's wall clock.

## Decision

- Producers and consumers of a benchmark run **in the same JVM**.
- Each payload carries the producer's `System.nanoTime()` (monotonic, nanosecond resolution) at
  send time, inside the self-verifying header (ADR-0006).
- The consumer records `System.nanoTime() - sendNanos` into its own HdrHistogram.
- Consumers start and must own every partition **before** producers start, so consumer-group
  rebalancing never shows up as latency.

## Alternatives considered

- **Record `CreateTime` plus a wall clock.** Millisecond resolution, and skew between machines.
- **Separate producer and consumer processes plus a clock-sync protocol.** More realistic for
  multi-host tests, but much more complex. That work is deferred until the lab supports remote
  load generators.

## Consequences

- E2E numbers are precise and skew-free.
- One JVM generates the whole load, so the client can become the bottleneck at very high rates.
  The report includes client CPU (`client.cpu.pct`) so this is visible, and you can add producers
  to spread the work across more threads.
- E2E latency always includes the consumer fetch wait (`fetch.max.wait.ms`, set to 100 ms), which
  is a real component of user-visible latency.
