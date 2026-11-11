# ADR-0008: Baseline comparison with relative thresholds for regression gating

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

A performance regression test has to answer one question: "did this change make Kafka (or our
configuration of it) meaningfully worse?" It must do so automatically in CI, and not cry wolf. A
gate that flakes gets switched off.

## Decision

- Results are stored as JSON (`results.json`, an array of runs). A blessed copy is committed as
  `benchmarks/baselines/baseline.json`.
- `RegressionAnalyzer` matches runs **by workload name** and compares each metric against a
  per-metric **relative** threshold in `RegressionPolicy` (YAML-configurable,
  `workloads/regression-policy.yaml`):

  | Metric | Direction | FAIL beyond |
  |---|---|---|
  | produce / consume throughput | higher is better | −10% |
  | publish p95 | lower is better | +20% |
  | publish p99 | lower is better | +25% |
  | end-to-end p99 | lower is better | +30% |
  | integrity (lost + corrupt) | must be 0 | any |

- **WARN** when more than half of a threshold is used up (`warnFraction: 0.5`). **IMPROVED** when
  a metric moves the other way by the same margin.
- An **absolute noise floor** (`minAbsoluteLatencyDeltaMs: 1.0`) stops a 0.6 ms → 0.9 ms change
  (+50%) from failing the build.
- Integrity is never relative: one lost acknowledged message fails the gate whatever the
  performance.
- `kafkacore bench compare` and `bench run --baseline` exit with code **2** on FAIL. CI uses that
  exit code directly.

## Alternatives considered

- **Statistical tests (Mann-Whitney U over repeated runs).** More rigorous, but each comparison
  needs N repetitions, which multiplies CI time. A future extension: `RegressionAnalyzer` already
  isolates the decision logic.
- **Absolute SLO thresholds** (e.g. p99 < 50 ms). Useful as an extra guard, but they don't carry
  across hardware, while relative change against a baseline from the same environment does.

## Consequences

- A baseline is only valid on comparable hardware. `RunMetadata` (host, CPU count, OS, Java and
  client version, git SHA) is stored with each run so mismatches are visible. CI regenerates its
  own baseline instead of comparing against laptop numbers.
- The thresholds are a starting point, to be tuned from observed run-to-run variance.
