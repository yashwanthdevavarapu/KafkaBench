package io.kafkacore.lab.chaos.fault;

import io.kafkacore.lab.bench.process.CommandExecutor;
import io.kafkacore.lab.chaos.cluster.ClusterTopology;
import io.kafkacore.lab.chaos.docker.DockerCli;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DockerFaultInjectorTest {

    /** Records commands and answers {@code docker inspect} with a configurable container state. */
    private static final class FakeDocker implements CommandExecutor {
        final List<String> commands = new ArrayList<>();
        String state = "running";
        String networks = "kafkacore-net ";
        int failExit = 0;

        @Override
        public CommandResult execute(List<String> command, java.time.Duration timeout) {
            String joined = String.join(" ", command);
            if (joined.startsWith("docker inspect") && joined.contains(".State.Status")) {
                return new CommandResult(0, state + "\n", "");
            }
            if (joined.startsWith("docker inspect") && joined.contains("Networks")) {
                return new CommandResult(0, networks, "");
            }
            commands.add(joined);
            return new CommandResult(failExit, "", failExit == 0 ? "" : "Error response from daemon: boom");
        }
    }

    private FakeDocker docker;
    private DockerFaultInjector injector;
    private final ClusterTopology.Node node = ClusterTopology.local().node(2);

    @BeforeEach
    void setUp() {
        docker = new FakeDocker();
        injector = new DockerFaultInjector(new DockerCli(docker), "kafkacore-net");
    }

    @Test
    void mapsEachFaultToTheRightDockerCommand() {
        injector.inject(node, FaultType.KILL);
        injector.inject(node, FaultType.STOP);
        injector.inject(node, FaultType.PAUSE);
        injector.inject(node, FaultType.NETWORK_PARTITION);
        assertThat(docker.commands).containsExactly(
                "docker kill --signal SIGKILL kafka-2",
                "docker stop --time 30 kafka-2",
                "docker pause kafka-2",
                "docker network disconnect --force kafkacore-net kafka-2");
    }

    @Test
    void healStartsKilledContainer() {
        docker.state = "exited";
        injector.heal(node, FaultType.KILL);
        assertThat(docker.commands).containsExactly("docker start kafka-2");
    }

    @Test
    void healIsIdempotent() {
        docker.state = "running";
        injector.heal(node, FaultType.KILL);
        injector.heal(node, FaultType.PAUSE);
        injector.heal(node, FaultType.NETWORK_PARTITION); // still attached
        assertThat(docker.commands).isEmpty();
    }

    @Test
    void healUnpausesAndReconnects() {
        docker.state = "paused";
        docker.networks = "bridge ";
        injector.heal(node, FaultType.PAUSE);
        injector.heal(node, FaultType.NETWORK_PARTITION);
        assertThat(docker.commands).containsExactly("docker unpause kafka-2", "docker network connect kafkacore-net kafka-2");
    }

    @Test
    void dockerFailureSurfacesStderr() {
        docker.failExit = 1;
        assertThatThrownBy(() -> injector.inject(node, FaultType.KILL))
                .isInstanceOf(CommandExecutor.CommandFailedException.class)
                .hasMessageContaining("docker kill").hasMessageContaining("boom");
    }

    @Test
    void stateReportsMissingContainers() {
        DockerCli cli = new DockerCli((cmd, t) -> new CommandExecutor.CommandResult(1, "", "No such object"));
        assertThat(cli.state("ghost")).isEqualTo("missing");
        assertThat(cli.networks("ghost")).isEmpty();
        assertThat(cli.available()).isFalse();
    }
}
