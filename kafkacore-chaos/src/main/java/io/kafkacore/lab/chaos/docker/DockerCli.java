package io.kafkacore.lab.chaos.docker;

import io.kafkacore.lab.bench.process.CommandExecutor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Thin, testable wrapper over the {@code docker} CLI (see ADR-0007). Shelling out keeps the lab
 * independent of Docker Engine API versions and matches exactly what an operator would type.
 */
public final class DockerCli {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);

    private final CommandExecutor executor;

    public DockerCli(CommandExecutor executor) {
        this.executor = executor;
    }

    /** Sends a signal (default SIGKILL): the process dies instantly, no controlled shutdown. */
    public void kill(String container, String signal) {
        run(List.of("docker", "kill", "--signal", signal, container));
    }

    /** Graceful SIGTERM, letting Kafka run controlled shutdown, then SIGKILL after the timeout. */
    public void stop(String container, int timeoutSec) {
        run(List.of("docker", "stop", "--time", Integer.toString(timeoutSec), container), Duration.ofSeconds(timeoutSec + 30L));
    }

    public void start(String container) {
        run(List.of("docker", "start", container));
    }

    /** Freezes every process in the container via the cgroup freezer (simulates a long GC/IO stall). */
    public void pause(String container) {
        run(List.of("docker", "pause", container));
    }

    public void unpause(String container) {
        run(List.of("docker", "unpause", container));
    }

    public void networkDisconnect(String network, String container) {
        run(List.of("docker", "network", "disconnect", "--force", network, container));
    }

    public void networkConnect(String network, String container) {
        run(List.of("docker", "network", "connect", network, container));
    }

    /** Container state as reported by {@code docker inspect} ("running", "paused", "exited", ...). */
    public String state(String container) {
        CommandExecutor.CommandResult r = executor.execute(
                List.of("docker", "inspect", "--format", "{{.State.Status}}", container), DEFAULT_TIMEOUT);
        return r.ok() ? r.stdout().strip() : "missing";
    }

    /** Networks the container is currently attached to. */
    public List<String> networks(String container) {
        CommandExecutor.CommandResult r = executor.execute(List.of("docker", "inspect", "--format",
                "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}", container), DEFAULT_TIMEOUT);
        List<String> out = new ArrayList<>();
        if (r.ok()) {
            for (String s : r.stdout().trim().split("\\s+")) if (!s.isBlank()) out.add(s);
        }
        return out;
    }

    public CommandExecutor.CommandResult compose(String composeFile, List<String> args, Duration timeout) {
        List<String> cmd = new ArrayList<>(List.of("docker", "compose", "-f", composeFile));
        cmd.addAll(args);
        return executor.execute(cmd, timeout).orThrow(cmd);
    }

    public boolean available() {
        try {
            return executor.execute(List.of("docker", "version", "--format", "{{.Server.Version}}"), Duration.ofSeconds(20)).ok();
        } catch (RuntimeException e) {
            return false;
        }
    }

    private void run(List<String> cmd) {
        run(cmd, DEFAULT_TIMEOUT);
    }

    private void run(List<String> cmd, Duration timeout) {
        executor.execute(cmd, timeout).orThrow(cmd);
    }
}
