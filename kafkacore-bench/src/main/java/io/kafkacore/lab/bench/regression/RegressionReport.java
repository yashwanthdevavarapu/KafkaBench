package io.kafkacore.lab.bench.regression;

import java.util.List;

/** Comparison of one or more current runs against their baselines. */
public record RegressionReport(Verdict verdict, List<WorkloadComparison> workloads, List<String> unmatched) {

    public enum Verdict { PASS, WARN, FAIL }

    public enum Status { IMPROVED, PASS, WARN, FAIL }

    public enum Direction { HIGHER_IS_BETTER, LOWER_IS_BETTER }

    /** All metric comparisons for one workload name present in both baseline and current. */
    public record WorkloadComparison(String workload, String baselineRunId, String currentRunId, Verdict verdict,
                                     List<MetricComparison> metrics) {
    }

    /**
     * @param deltaPct signed relative change (current - baseline) / baseline * 100
     */
    public record MetricComparison(String metric, double baseline, double current, double deltaPct,
                                   double thresholdPct, Direction direction, Status status, String note) {
    }
}
