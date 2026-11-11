package io.kafkacore.lab.bench.regression;

import io.kafkacore.lab.bench.regression.RegressionReport.Direction;
import io.kafkacore.lab.bench.regression.RegressionReport.MetricComparison;
import io.kafkacore.lab.bench.regression.RegressionReport.Status;
import io.kafkacore.lab.bench.regression.RegressionReport.Verdict;
import io.kafkacore.lab.bench.regression.RegressionReport.WorkloadComparison;
import io.kafkacore.lab.bench.result.BenchmarkResult;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Compares benchmark results against a baseline using a {@link RegressionPolicy}. */
public final class RegressionAnalyzer {

    private final RegressionPolicy policy;

    public RegressionAnalyzer(RegressionPolicy policy) {
        this.policy = policy;
    }

    /** Matches runs by workload name; runs present on only one side are reported as unmatched. */
    public RegressionReport compare(List<BenchmarkResult> baseline, List<BenchmarkResult> current) {
        Map<String, BenchmarkResult> base = index(baseline);
        Map<String, BenchmarkResult> cur = index(current);
        List<WorkloadComparison> comparisons = new ArrayList<>();
        List<String> unmatched = new ArrayList<>();
        for (Map.Entry<String, BenchmarkResult> e : cur.entrySet()) {
            BenchmarkResult b = base.get(e.getKey());
            if (b == null) {
                unmatched.add(e.getKey() + " (no baseline)");
            } else {
                comparisons.add(compare(b, e.getValue()));
            }
        }
        base.keySet().stream().filter(k -> !cur.containsKey(k)).forEach(k -> unmatched.add(k + " (missing from current)"));
        Verdict overall = comparisons.stream().map(WorkloadComparison::verdict).reduce(Verdict.PASS, RegressionAnalyzer::worst);
        return new RegressionReport(overall, comparisons, unmatched);
    }

    public WorkloadComparison compare(BenchmarkResult baseline, BenchmarkResult current) {
        List<MetricComparison> metrics = new ArrayList<>();
        metrics.add(metric("produce throughput (msg/s)", baseline.throughput().producedMsgsPerSec(),
                current.throughput().producedMsgsPerSec(), policy.maxThroughputDropPct(), Direction.HIGHER_IS_BETTER, false));
        metrics.add(metric("produce throughput (MB/s)", baseline.throughput().producedMBPerSec(),
                current.throughput().producedMBPerSec(), policy.maxThroughputDropPct(), Direction.HIGHER_IS_BETTER, false));
        metrics.add(metric("consume throughput (msg/s)", baseline.throughput().consumedMsgsPerSec(),
                current.throughput().consumedMsgsPerSec(), policy.maxThroughputDropPct(), Direction.HIGHER_IS_BETTER, false));
        metrics.add(metric("publish latency p95 (ms)", baseline.publishLatency().p95Ms(),
                current.publishLatency().p95Ms(), policy.maxPublishP95IncreasePct(), Direction.LOWER_IS_BETTER, true));
        metrics.add(metric("publish latency p99 (ms)", baseline.publishLatency().p99Ms(),
                current.publishLatency().p99Ms(), policy.maxPublishP99IncreasePct(), Direction.LOWER_IS_BETTER, true));
        metrics.add(metric("end-to-end latency p99 (ms)", baseline.endToEndLatency().p99Ms(),
                current.endToEndLatency().p99Ms(), policy.maxEndToEndP99IncreasePct(), Direction.LOWER_IS_BETTER, true));
        if (policy.requireIntegrity()) {
            long lost = current.integrity().lost();
            long corrupt = current.integrity().corrupt();
            boolean ok = lost == 0 && corrupt == 0;
            metrics.add(new MetricComparison("integrity (lost + corrupt)", 0, lost + corrupt, 0, 0,
                    Direction.LOWER_IS_BETTER, ok ? Status.PASS : Status.FAIL,
                    ok ? "no acknowledged message lost" : lost + " lost, " + corrupt + " corrupt"));
        }
        Verdict v = Verdict.PASS;
        for (MetricComparison m : metrics) {
            if (m.status() == Status.FAIL) v = Verdict.FAIL;
            else if (m.status() == Status.WARN && v == Verdict.PASS) v = Verdict.WARN;
        }
        return new WorkloadComparison(current.workload().name(), baseline.runId(), current.runId(), v, metrics);
    }

    private MetricComparison metric(String name, double base, double cur, double thresholdPct, Direction dir,
                                    boolean latency) {
        if (base <= 0) {
            return new MetricComparison(name, base, cur, 0, thresholdPct, dir, Status.PASS, "no baseline value");
        }
        double deltaPct = round((cur - base) / base * 100.0);
        // Positive "badness" = movement in the harmful direction.
        double badness = dir == Direction.HIGHER_IS_BETTER ? -deltaPct : deltaPct;
        String note = "";
        Status status;
        if (latency && Math.abs(cur - base) < policy.minAbsoluteLatencyDeltaMs()) {
            // Sub-millisecond jitter on a fast path is not a regression even if it is +40%.
            status = Status.PASS;
            note = "within absolute noise floor (" + policy.minAbsoluteLatencyDeltaMs() + " ms)";
        } else if (badness > thresholdPct) {
            status = Status.FAIL;
        } else if (badness > thresholdPct * policy.warnFraction()) {
            status = Status.WARN;
        } else if (-badness > thresholdPct * policy.warnFraction()) {
            status = Status.IMPROVED;
        } else {
            status = Status.PASS;
        }
        return new MetricComparison(name, base, cur, deltaPct, thresholdPct, dir, status, note);
    }

    private static Map<String, BenchmarkResult> index(List<BenchmarkResult> results) {
        Map<String, BenchmarkResult> m = new LinkedHashMap<>();
        for (BenchmarkResult r : results) m.put(r.workload().name(), r);
        return m;
    }

    private static Verdict worst(Verdict a, Verdict b) {
        return a.ordinal() >= b.ordinal() ? a : b;
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
