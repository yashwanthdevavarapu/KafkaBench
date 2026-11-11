package io.kafkacore.lab.bench.result;

import io.kafkacore.lab.bench.integrity.IntegrityReport;
import io.kafkacore.lab.bench.metrics.LatencyStats;
import io.kafkacore.lab.bench.metrics.ResourceSummary;
import io.kafkacore.lab.bench.metrics.TimeSeriesPoint;
import io.kafkacore.lab.bench.workload.WorkloadSpec;

import java.util.List;

/** Everything measured by one benchmark run; serialised as JSON for baselines and reports. */
public record BenchmarkResult(
        String runId,
        WorkloadSpec workload,
        RunMetadata metadata,
        ThroughputStats throughput,
        LatencyStats publishLatency,
        LatencyStats endToEndLatency,
        IntegrityReport integrity,
        ResourceSummary resources,
        List<TimeSeriesPoint> timeline) {

    /** Throughput over the measurement window (warm-up excluded). */
    public record ThroughputStats(double producedMsgsPerSec, double producedMBPerSec, double consumedMsgsPerSec,
                                  double consumedMBPerSec, long totalSent, long totalAcked, long totalErrors,
                                  double measuredSeconds) {
    }
}
