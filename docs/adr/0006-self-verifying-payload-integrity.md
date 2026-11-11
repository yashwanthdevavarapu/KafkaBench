# ADR-0006: Self-verifying payloads for message-integrity checks

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

Fault-injection tests are only meaningful if we can prove **what happened to every message**.
Was each acknowledged write delivered? Delivered twice? Out of order? Corrupted? Counting messages
isn't enough, because one loss and one duplicate cancel each other out.

## Decision

Every payload is produced by `MessageCodec` with a 25-byte header:

| Field | Bytes | Purpose |
|---|---|---|
| magic `0x4B` | 1 | rejects foreign data |
| producerId | 4 | identifies the producer thread |
| sequence | 8 | gap-free counter per producer, starting at 0 |
| sendNanos | 8 | end-to-end latency (ADR-0005) |
| CRC32C | 4 | covers header and padding: detects any corruption |

The remaining bytes are seeded pseudo-random padding drawn from a 64-symbol alphabet. This keeps
the data realistically compressible: neither all zeros nor incompressible random bytes.

`IntegrityTracker` keeps **three bitsets per producer**: *acked* (from the producer callback),
*failed* (callback with exception), and *received* (from consumers). At the end of a run:

| Metric | Definition | Expected with `acks=all` + idempotence |
|---|---|---|
| `lost` | acked \ received | **0**, otherwise durability is broken |
| `corrupt` | CRC or magic mismatch | **0** |
| `outOfOrder` | lower seq consumed after higher seq, same producer and partition | **0** |
| `duplicates` | the same seq consumed again | ≥ 0 (consumer re-delivery after a rebalance is legal at-least-once) |
| `phantom` | failed according to the producer, but present in the log | ≥ 0 (timeout after the leader appended; legal) |

A re-delivered record counts as a duplicate, never as out-of-order, so a consumer rebalance
doesn't look like broken producer ordering.

## Consequences

- One bit per message: 10 million messages fit in about 4 MB of tracking state.
- A sequence must fit in an `int` index (2.1 billion messages per producer per run). That is
  enforced, and far above any realistic run length.
- Chaos scenarios can assert hard guarantees ("0 acknowledged messages lost") instead of
  "the counts look about right".
