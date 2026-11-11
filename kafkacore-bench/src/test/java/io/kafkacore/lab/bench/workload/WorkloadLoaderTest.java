package io.kafkacore.lab.bench.workload;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkloadLoaderTest {

    @Test
    void appliesDefaultsToMinimalSpec() {
        WorkloadSpec s = WorkloadLoader.parse("name: tiny\n", "inline");
        assertThat(s.partitions()).isEqualTo(6);
        assertThat(s.replicationFactor()).isEqualTo(3);
        assertThat(s.messageSizeBytes()).isEqualTo(1024);
        assertThat(s.targetRatePerSec()).isZero();
        assertThat(s.keyStrategy()).isEqualTo(WorkloadSpec.KeyStrategy.NONE);
        assertThat(s.producerConfig()).isEmpty();
    }

    @Test
    void parsesClientAndTopicOverrides() {
        WorkloadSpec s = WorkloadLoader.parse("""
                name: custom
                keyStrategy: RANDOM
                producerConfig:
                  acks: "1"
                  compression.type: lz4
                topicConfig:
                  min.insync.replicas: "2"
                """, "inline");
        assertThat(s.producerConfig()).containsEntry("acks", "1").containsEntry("compression.type", "lz4");
        assertThat(s.topicConfig()).containsEntry("min.insync.replicas", "2");
        assertThat(s.keyStrategy()).isEqualTo(WorkloadSpec.KeyStrategy.RANDOM);
    }

    @Test
    void rejectsUnknownFieldsSoTyposDoNotSilentlyUseDefaults() {
        assertThatThrownBy(() -> WorkloadLoader.parse("name: x\nmesageSizeBytes: 10\n", "typo.yaml"))
                .isInstanceOf(WorkloadLoader.WorkloadValidationException.class)
                .hasMessageContaining("typo.yaml");
    }

    @Test
    void reportsEveryValidationError() {
        WorkloadLoader.WorkloadValidationException e = (WorkloadLoader.WorkloadValidationException)
                org.assertj.core.api.Assertions.catchThrowable(() -> WorkloadLoader.parse("""
                        name: bad
                        partitions: 0
                        messageSizeBytes: 10
                        targetRatePerSec: -1
                        topic: "bad topic!"
                        """, "bad.yaml"));
        assertThat(e.errors()).hasSize(4)
                .anyMatch(m -> m.startsWith("partitions"))
                .anyMatch(m -> m.startsWith("messageSizeBytes"))
                .anyMatch(m -> m.startsWith("targetRatePerSec"))
                .anyMatch(m -> m.startsWith("topic"));
    }

    @Test
    void expandsSweepAsCartesianProductWithDescriptiveNames() {
        WorkloadSpec s = WorkloadLoader.parse("""
                name: sweep
                producers: 2
                sweep:
                  messageSizeBytes: [100, 1000]
                  targetRatePerSec: [10, 20, 30]
                """, "inline");
        List<WorkloadSpec> runs = s.expand();
        assertThat(runs).hasSize(6);
        assertThat(runs).extracting(WorkloadSpec::name)
                .startsWith("sweep[size=100,rate=10]", "sweep[size=100,rate=20]")
                .endsWith("sweep[size=1000,rate=30]");
        assertThat(runs).allSatisfy(r -> {
            assertThat(r.sweep()).isNull();
            assertThat(r.producers()).isEqualTo(2);
        });
    }

    @Test
    void specWithoutSweepExpandsToItself() {
        WorkloadSpec s = WorkloadSpec.builder("solo").messageSizeBytes(256).build();
        assertThat(s.expand()).singleElement().satisfies(r -> {
            assertThat(r.name()).isEqualTo("solo");
            assertThat(r.messageSizeBytes()).isEqualTo(256);
        });
    }

    @Test
    void everyBundledWorkloadFileIsValid() throws Exception {
        Path dir = findWorkloadsDir();
        try (Stream<Path> files = Files.list(dir)) {
            List<Path> yamls = files.filter(p -> p.toString().endsWith(".yaml") && !p.getFileName().toString().contains("policy")).toList();
            assertThat(yamls).isNotEmpty();
            for (Path p : yamls) {
                WorkloadSpec spec = WorkloadLoader.load(p);
                assertThat(spec.expand()).as(p.toString()).isNotEmpty();
            }
        }
    }

    static Path findWorkloadsDir() {
        Path p = Path.of("").toAbsolutePath();
        while (p != null && !Files.isDirectory(p.resolve("workloads"))) p = p.getParent();
        assertThat(p).as("workloads/ directory").isNotNull();
        return p.resolve("workloads");
    }
}
