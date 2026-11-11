package io.kafkacore.lab.bench;

import io.kafkacore.lab.bench.integrity.IntegrityReport;
import io.kafkacore.lab.bench.metrics.LatencyStats;
import io.kafkacore.lab.bench.metrics.ResourceSummary;
import io.kafkacore.lab.bench.metrics.TimeSeriesPoint;
import io.kafkacore.lab.bench.result.BenchmarkResult;
import io.kafkacore.lab.bench.result.RunMetadata;
import io.kafkacore.lab.bench.workload.WorkloadSpec;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Synthetic results for report and regression tests. */
public final class Fixtures {

    private Fixtures() {
    }

    public static BenchmarkResult result(String name, int size, double msgsPerSec, double p95, double p99, long lost) {
        WorkloadSpec spec = WorkloadSpec.builder(name).messageSizeBytes(size).targetRatePerSec(5000).build().withDefaults();
        double mbps = msgsPerSec * size / (1024.0 * 1024.0);
        return new BenchmarkResult(
                name + "-id", spec,
                new RunMetadata(Instant.parse("2026-01-01T00:00:00Z"), "host", "Linux amd64", 8, "17", "4.0.0", "localhost:19092", "abc123"),
                new BenchmarkResult.ThroughputStats(msgsPerSec, mbps, msgsPerSec, mbps, 1000, 1000, 0, 30),
                new LatencyStats(1000, 0.5, p95 / 2, p95 / 3, p95, p99, p99 * 1.5, p99 * 2),
                new LatencyStats(1000, 0.8, p95 / 2, p95 / 2, p95 * 1.1, p99 * 1.1, p99 * 1.6, p99 * 2.1),
                new IntegrityReport(1000, 0, 1000 - lost, lost, 0, 0, 0, 0),
                new ResourceSummary(Map.of("broker.kafka-1.cpu.pct", new ResourceSummary.Gauge(40, 60, 10),
                        "client.heap.mb", new ResourceSummary.Gauge(120, 150, 10))),
                List.of(new TimeSeriesPoint(1, (long) msgsPerSec, (long) msgsPerSec, p99, p99 * 1.1, 0),
                        new TimeSeriesPoint(2, (long) msgsPerSec, (long) msgsPerSec, p99, p99 * 1.1, 0)));
    }
}
