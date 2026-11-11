package io.kafkacore.lab.chaos.cluster;

import io.kafkacore.lab.chaos.docker.DockerCli;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

/** Starts, stops and health-checks the docker-compose KRaft cluster. */
public final class ComposeCluster {

    private final DockerCli docker;
    private final ClusterTopology topology;
    private final Path composeFile;

    public ComposeCluster(DockerCli docker, ClusterTopology topology, Path projectRoot) {
        this.docker = docker;
        this.topology = topology;
        this.composeFile = projectRoot.resolve(topology.composeFile()).toAbsolutePath().normalize();
    }

    public Path composeFile() {
        return composeFile;
    }

    /** True when every node container exists and is in the running state. */
    public boolean allRunning() {
        return topology.containers().stream().allMatch(c -> "running".equals(docker.state(c)));
    }

    /** {@code docker compose up -d --wait}: blocks until every broker healthcheck passes. */
    public void up(Consumer<String> log) {
        if (!Files.exists(composeFile)) {
            throw new IllegalStateException("Compose file not found: " + composeFile);
        }
        log.accept("Starting 3-node KRaft cluster (" + composeFile.getFileName() + ") ...");
        docker.compose(composeFile.toString(), List.of("up", "-d", "--wait", "--wait-timeout", "240"), Duration.ofMinutes(6));
    }

    public void down(boolean removeVolumes) {
        docker.compose(composeFile.toString(), removeVolumes ? List.of("down", "-v") : List.of("down"), Duration.ofMinutes(3));
    }

    /**
     * Makes sure every node is running (starting stopped ones, unpausing paused ones and
     * re-attaching detached networks left over from an aborted fault run), then waits for health.
     */
    public void ensureHealthy(Duration timeout, Consumer<String> log) throws InterruptedException {
        if (topology.containers().stream().anyMatch(c -> "missing".equals(docker.state(c)))) {
            up(log);
        }
        for (ClusterTopology.Node n : topology.nodes()) {
            String state = docker.state(n.container());
            switch (state) {
                case "paused" -> {
                    log.accept("Unpausing leftover paused node " + n.container());
                    docker.unpause(n.container());
                }
                case "exited", "created" -> {
                    log.accept("Starting stopped node " + n.container());
                    docker.start(n.container());
                }
                default -> { }
            }
            if (!docker.networks(n.container()).contains(topology.network())) {
                log.accept("Re-attaching " + n.container() + " to " + topology.network());
                docker.networkConnect(topology.network(), n.container());
            }
        }
        try (ClusterInspector inspector = new ClusterInspector(topology.bootstrapServers())) {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                if (inspector.healthy(topology.size())) return;
                Thread.sleep(1_000);
            }
            throw new IllegalStateException("Cluster not healthy after " + timeout + ": " + inspector.snapshot());
        }
    }
}
