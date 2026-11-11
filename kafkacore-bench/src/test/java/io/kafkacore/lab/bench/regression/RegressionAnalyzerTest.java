package io.kafkacore.lab.bench.regression;

import io.kafkacore.lab.bench.Fixtures;
import io.kafkacore.lab.bench.regression.RegressionReport.MetricComparison;
import io.kafkacore.lab.bench.regression.RegressionReport.Status;
import io.kafkacore.lab.bench.regression.RegressionReport.Verdict;
import io.kafkacore.lab.bench.result.BenchmarkResult;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RegressionAnalyzerTest {

    private final RegressionAnalyzer analyzer = new RegressionAnalyzer(RegressionPolicy.defaults());

    @Test
    void identicalResultsPass() {
        BenchmarkResult r = Fixtures.result("w", 1024, 10_000, 10, 20, 0);
        RegressionReport report = analyzer.compare(List.of(r), List.of(r));
        assertThat(report.verdict()).isEqualTo(Verdict.PASS);
        assertThat(report.workloads()).singleElement().satisfies(w ->
                assertThat(w.metrics()).allMatch(m -> m.status() == Status.PASS));
    }

    @Test
    void throughputDropBeyondThresholdFails() {
        BenchmarkResult base = Fixtures.result("w", 1024, 10_000, 10, 20, 0);
        BenchmarkResult cur = Fixtures.result("w", 1024, 8_500, 10, 20, 0); // -15 %
        RegressionReport report = analyzer.compare(List.of(base), List.of(cur));
        assertThat(report.verdict()).isEqualTo(Verdict.FAIL);
        MetricComparison tp = metric(report, "produce throughput (msg/s)");
        assertThat(tp.deltaPct()).isEqualTo(-15.0);
        assertThat(tp.status()).isEqualTo(Status.FAIL);
    }

    @Test
    void moderateLatencyIncreaseWarns() {
        BenchmarkResult base = Fixtures.result("w", 1024, 10_000, 10, 20, 0);
        BenchmarkResult cur = Fixtures.result("w", 1024, 10_000, 10, 24, 0); // p99 +20 % (threshold 25 %)
        RegressionReport report = analyzer.compare(List.of(base), List.of(cur));
        assertThat(metric(report, "publish latency p99 (ms)").status()).isEqualTo(Status.WARN);
        assertThat(report.verdict()).isEqualTo(Verdict.WARN);
    }

    @Test
    void largeRelativeChangeBelowNoiseFloorPasses() {
        BenchmarkResult base = Fixtures.result("w", 1024, 10_000, 1.0, 1.5, 0);
        BenchmarkResult cur = Fixtures.result("w", 1024, 10_000, 1.4, 2.1, 0); // +40 % but only +0.6 ms
        MetricComparison p99 = metric(analyzer.compare(List.of(base), List.of(cur)), "publish latency p99 (ms)");
        assertThat(p99.status()).isEqualTo(Status.PASS);
        assertThat(p99.note()).contains("noise floor");
    }

    @Test
    void improvementsAreFlagged() {
        BenchmarkResult base = Fixtures.result("w", 1024, 10_000, 10, 20, 0);
        BenchmarkResult cur = Fixtures.result("w", 1024, 13_000, 6, 12, 0);
        RegressionReport report = analyzer.compare(List.of(base), List.of(cur));
        assertThat(metric(report, "produce throughput (msg/s)").status()).isEqualTo(Status.IMPROVED);
        assertThat(metric(report, "publish latency p99 (ms)").status()).isEqualTo(Status.IMPROVED);
        assertThat(report.verdict()).isEqualTo(Verdict.PASS);
    }

    @Test
    void anyLostMessageFailsRegardlessOfPerformance() {
        BenchmarkResult base = Fixtures.result("w", 1024, 10_000, 10, 20, 0);
        BenchmarkResult cur = Fixtures.result("w", 1024, 20_000, 5, 10, 1);
        RegressionReport report = analyzer.compare(List.of(base), List.of(cur));
        assertThat(report.verdict()).isEqualTo(Verdict.FAIL);
        assertThat(metric(report, "integrity (lost + corrupt)").note()).contains("1 lost");
    }

    @Test
    void unmatchedWorkloadsAreReportedNotCompared() {
        RegressionReport report = analyzer.compare(
                List.of(Fixtures.result("a", 1024, 1, 1, 1, 0), Fixtures.result("b", 1024, 1, 1, 1, 0)),
                List.of(Fixtures.result("b", 1024, 1, 1, 1, 0), Fixtures.result("c", 1024, 1, 1, 1, 0)));
        assertThat(report.workloads()).extracting(RegressionReport.WorkloadComparison::workload).containsExactly("b");
        assertThat(report.unmatched()).containsExactlyInAnyOrder("c (no baseline)", "a (missing from current)");
    }

    @Test
    void policyLoadsFromYaml(@org.junit.jupiter.api.io.TempDir Path dir) throws Exception {
        Path f = dir.resolve("p.yaml");
        Files.writeString(f, """
                maxThroughputDropPct: 5
                maxPublishP95IncreasePct: 10
                maxPublishP99IncreasePct: 10
                maxEndToEndP99IncreasePct: 10
                warnFraction: 0.5
                minAbsoluteLatencyDeltaMs: 0.5
                requireIntegrity: false
                """);
        RegressionPolicy p = RegressionPolicy.load(f);
        assertThat(p.maxThroughputDropPct()).isEqualTo(5);
        assertThat(p.requireIntegrity()).isFalse();
        RegressionReport r = new RegressionAnalyzer(p).compare(
                List.of(Fixtures.result("w", 1024, 10_000, 10, 20, 0)), List.of(Fixtures.result("w", 1024, 9_400, 10, 20, 0)));
        assertThat(r.verdict()).isEqualTo(Verdict.FAIL);
    }

    private static MetricComparison metric(RegressionReport r, String name) {
        return r.workloads().get(0).metrics().stream().filter(m -> m.metric().equals(name)).findFirst().orElseThrow();
    }
}
