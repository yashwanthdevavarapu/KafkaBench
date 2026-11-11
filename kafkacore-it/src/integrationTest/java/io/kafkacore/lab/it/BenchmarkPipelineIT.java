package io.kafkacore.lab.it;

import io.kafkacore.lab.bench.driver.BenchmarkRunner;
import io.kafkacore.lab.bench.metrics.DockerStatsSampler;
import io.kafkacore.lab.bench.metrics.JvmResourceSampler;
import io.kafkacore.lab.bench.metrics.ResourceMonitor;
import io.kafkacore.lab.bench.process.ProcessCommandExecutor;
import io.kafkacore.lab.bench.regression.RegressionAnalyzer;
import io.kafkacore.lab.bench.regression.RegressionPolicy;
import io.kafkacore.lab.bench.regression.RegressionReport;
import io.kafkacore.lab.bench.report.HtmlReport;
import io.kafkacore.lab.bench.result.BenchmarkResult;
import io.kafkacore.lab.bench.result.ResultIO;
import io.kafkacore.lab.bench.workload.WorkloadSpec;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.kafkacore.lab.it.KafkaClusterExtension.TOPOLOGY;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end: workload -> real cluster -> metrics -> integrity -> JSON -> regression gate -> HTML. */
@Tag("integration")
@ExtendWith(KafkaClusterExtension.class)
class BenchmarkPipelineIT {

    @Test
    void durableWorkloadDeliversEveryMessageExactlyOnceAndInOrder() throws Exception {
        WorkloadSpec spec = WorkloadSpec.builder("it-durable")
                .topic("it-bench")
                .partitions(3)
                .replicationFactor(3)
                .messageSizeBytes(256)
                .producers(2)
                .consumers(2)
                .targetRatePerSec(400)
                .durationSec(6)
                .warmupSec(2)
                .producerConfig("acks", "all")
                .producerConfig("enable.idempotence", "true")
                .topicConfig("min.insync.replicas", "2")
                .build();
        BenchmarkRunner runner = new BenchmarkRunner(TOPOLOGY.bootstrapServers(),
                () -> new ResourceMonitor(List.of(new JvmResourceSampler(),
                        new DockerStatsSampler(new ProcessCommandExecutor(), TOPOLOGY.containers())), 2_000),
                System.out::println);

        BenchmarkResult r = runner.run(spec);

        assertThat(r.throughput().producedMsgsPerSec()).isPositive();
        assertThat(r.throughput().totalAcked()).isEqualTo(r.integrity().acked());
        assertThat(r.integrity().durable()).as("no lost or corrupt messages: %s", r.integrity()).isTrue();
        assertThat(r.integrity().exactlyOnceOrdered()).as("idempotent producer: %s", r.integrity()).isTrue();
        assertThat(r.integrity().received()).isEqualTo(r.integrity().acked());
        assertThat(r.publishLatency().count()).isPositive();
        assertThat(r.publishLatency().p99Ms()).isGreaterThanOrEqualTo(r.publishLatency().p95Ms());
        assertThat(r.endToEndLatency().count()).isPositive();
        assertThat(r.timeline()).hasSize(6);
        assertThat(r.resources().gauges()).containsKey("client.heap.mb");
        assertThat(r.resources().gauges().keySet()).anyMatch(k -> k.startsWith("broker.kafka-"));

        // Persist, gate against itself (PASS) and against an inflated baseline (FAIL), render HTML.
        Path dir = Path.of("build", "it-results");
        ResultIO.write(dir.resolve("results.json"), List.of(r));
        List<BenchmarkResult> back = ResultIO.read(dir.resolve("results.json"));
        RegressionAnalyzer analyzer = new RegressionAnalyzer(RegressionPolicy.defaults());
        assertThat(analyzer.compare(back, List.of(r)).verdict()).isEqualTo(RegressionReport.Verdict.PASS);

        BenchmarkResult fasterBaseline = new BenchmarkResult(r.runId(), r.workload(), r.metadata(),
                new BenchmarkResult.ThroughputStats(r.throughput().producedMsgsPerSec() * 2, r.throughput().producedMBPerSec() * 2,
                        r.throughput().consumedMsgsPerSec() * 2, r.throughput().consumedMBPerSec() * 2,
                        r.throughput().totalSent(), r.throughput().totalAcked(), 0, r.throughput().measuredSeconds()),
                r.publishLatency(), r.endToEndLatency(), r.integrity(), r.resources(), r.timeline());
        RegressionReport gate = analyzer.compare(List.of(fasterBaseline), List.of(r));
        assertThat(gate.verdict()).isEqualTo(RegressionReport.Verdict.FAIL);

        Files.writeString(dir.resolve("report.html"), HtmlReport.render(List.of(r), gate));
        assertThat(Files.readString(dir.resolve("report.html"))).contains("it-durable", "<svg");
    }
}
