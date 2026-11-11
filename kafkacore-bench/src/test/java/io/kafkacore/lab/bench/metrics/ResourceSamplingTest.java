package io.kafkacore.lab.bench.metrics;

import io.kafkacore.lab.bench.process.CommandExecutor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class ResourceSamplingTest {

    @Test
    void parsesDockerStatsJsonLines() {
        List<List<String>> calls = new ArrayList<>();
        CommandExecutor fake = (cmd, timeout) -> {
            calls.add(cmd);
            return new CommandExecutor.CommandResult(0, """
                    {"Name":"kafka-1","CPUPerc":"12.50%","MemUsage":"512MiB / 3.7GiB","NetIO":"1.5MB / 2kB","BlockIO":"0B / 10.2MB"}
                    {"Name":"kafka-2","CPUPerc":"--","MemUsage":"1.5GiB / 3.7GiB","NetIO":"0B / 0B","BlockIO":"0B / 0B"}
                    not-json
                    """, "");
        };
        Map<String, Double> s = new DockerStatsSampler(fake, List.of("kafka-1", "kafka-2")).sample();

        assertThat(calls.get(0)).containsSequence("docker", "stats", "--no-stream").contains("kafka-1", "kafka-2");
        assertThat(s.get("broker.kafka-1.cpu.pct")).isEqualTo(12.5);
        assertThat(s.get("broker.kafka-1.mem.mb")).isEqualTo(512.0);
        assertThat(s.get("broker.kafka-1.net.rx.mb")).isCloseTo(1.43, within(0.01));
        assertThat(s.get("broker.kafka-1.disk.write.mb")).isCloseTo(9.73, within(0.01));
        assertThat(s.get("broker.kafka-2.cpu.pct")).isZero();
        assertThat(s.get("broker.kafka-2.mem.mb")).isEqualTo(1536.0);
    }

    @Test
    void unitConversion() {
        assertThat(DockerStatsSampler.toMiB("1KiB")).isCloseTo(0.0, within(0.01));
        assertThat(DockerStatsSampler.toMiB("1GiB")).isEqualTo(1024.0);
        assertThat(DockerStatsSampler.toMiB("garbage")).isZero();
        assertThat(DockerStatsSampler.percent("99.9%")).isEqualTo(99.9);
    }

    @Test
    void monitorAggregatesAvgAndMaxAndSurvivesFailingSamplers() {
        int[] n = {0};
        ResourceSampler counting = () -> Map.of("g", (double) (++n[0] * 10));
        ResourceSampler broken = () -> {
            throw new IllegalStateException("docker unavailable");
        };
        try (ResourceMonitor m = new ResourceMonitor(List.of(counting, broken), 1000)) {
            m.sampleOnce();
            m.sampleOnce();
            m.sampleOnce();
            ResourceSummary.Gauge g = m.summary().gauges().get("g");
            assertThat(g.avg()).isEqualTo(20.0);
            assertThat(g.max()).isEqualTo(30.0);
            assertThat(g.samples()).isEqualTo(3);
            m.reset();
            assertThat(m.summary().gauges()).isEmpty();
        }
    }

    @Test
    void jvmSamplerReportsHeapAndGc() {
        Map<String, Double> s = new JvmResourceSampler().sample();
        assertThat(s).containsKeys("client.heap.mb", "client.gc.ms");
        assertThat(s.get("client.heap.mb")).isPositive();
    }
}
