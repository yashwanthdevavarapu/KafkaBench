package io.kafkacore.lab.it;

import io.kafkacore.lab.bench.process.ProcessCommandExecutor;
import io.kafkacore.lab.chaos.cluster.ClusterTopology;
import io.kafkacore.lab.chaos.cluster.ComposeCluster;
import io.kafkacore.lab.chaos.docker.DockerCli;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Ensures the docker-compose KRaft cluster is up and healthy before a test class runs. Starts it if
 * needed and repairs leftovers from an aborted fault run (paused/stopped/disconnected nodes).
 * Skips (rather than fails) when no Docker daemon is reachable.
 */
public final class KafkaClusterExtension implements BeforeAllCallback {

    public static final ClusterTopology TOPOLOGY = ClusterTopology.local();
    public static final DockerCli DOCKER = new DockerCli(new ProcessCommandExecutor());

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        assumeTrue(DOCKER.available(), "Docker daemon not reachable — skipping cluster integration tests");
        Path root = Path.of(System.getProperty("kafkacore.root", ".")).toAbsolutePath();
        new ComposeCluster(DOCKER, TOPOLOGY, root).ensureHealthy(Duration.ofMinutes(5), System.out::println);
    }
}
