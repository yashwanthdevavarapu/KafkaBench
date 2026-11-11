package io.kafkacore.lab.cli;

import io.kafkacore.lab.bench.process.ProcessCommandExecutor;
import io.kafkacore.lab.chaos.cluster.ClusterInspector;
import io.kafkacore.lab.chaos.cluster.ClusterTopology;
import io.kafkacore.lab.chaos.cluster.ComposeCluster;
import io.kafkacore.lab.chaos.docker.DockerCli;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Callable;

@Command(name = "cluster", description = "Manage and inspect the local 3-node KRaft cluster.",
        mixinStandardHelpOptions = true,
        subcommands = {ClusterCommand.Status.class, ClusterCommand.Up.class, ClusterCommand.Down.class})
public final class ClusterCommand implements Runnable {

    @Spec
    CommandSpec spec;

    @Override
    public void run() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }

    @Command(name = "status", mixinStandardHelpOptions = true,
            description = "Show KRaft quorum, broker liveness, container state and under-replicated partitions.")
    static final class Status implements Callable<Integer> {
        @Spec
        CommandSpec spec;

        @Override
        public Integer call() {
            PrintWriter out = spec.commandLine().getOut();
            ClusterTopology topology = ClusterTopology.local();
            DockerCli docker = new DockerCli(new ProcessCommandExecutor());
            try (ClusterInspector inspector = new ClusterInspector(topology.bootstrapServers())) {
                ClusterInspector.ClusterSnapshot s = inspector.snapshot();
                out.println("KafkaCore Lab cluster — " + topology.bootstrapServers());
                out.println();
                out.printf("%-6s %-10s %-10s %-10s %-14s %-10s%n", "NODE", "CONTAINER", "STATE", "BROKER", "KRAFT ROLE", "LAG");
                for (ClusterTopology.Node n : topology.nodes()) {
                    String role = "-";
                    String lag = "-";
                    if (s.quorum() != null) {
                        role = s.quorum().leaderId() == n.id() ? "leader" : "follower";
                        lag = s.quorum().voters().stream().filter(v -> v.id() == n.id()).findFirst()
                                .map(v -> Long.toString(v.lag())).orElse("-");
                    }
                    out.printf("%-6d %-10s %-10s %-10s %-14s %-10s%n", n.id(), n.container(), docker.state(n.container()),
                            s.liveBrokers().contains(n.id()) ? "live" : "DOWN", role, lag);
                }
                out.println();
                if (s.error() != null) {
                    out.println("Admin API error: " + s.error());
                    return 1;
                }
                out.printf("KRaft leader: node %d   epoch: %d   metadata high-watermark: %d%n",
                        s.quorum().leaderId(), s.quorum().leaderEpoch(), s.quorum().highWatermark());
                out.printf("Under-replicated partitions: %d%n", s.underReplicatedPartitions());
                boolean healthy = s.underReplicatedPartitions() == 0 && s.liveBrokers().size() == topology.size();
                out.println("Status: " + (healthy ? "HEALTHY" : "DEGRADED"));
                return healthy ? 0 : 1;
            }
        }
    }

    @Command(name = "up", mixinStandardHelpOptions = true, description = "docker compose up and wait until healthy.")
    static final class Up implements Callable<Integer> {
        @Spec
        CommandSpec spec;

        @Override
        public Integer call() throws Exception {
            PrintWriter out = spec.commandLine().getOut();
            ComposeCluster c = new ComposeCluster(new DockerCli(new ProcessCommandExecutor()), ClusterTopology.local(), Path.of("."));
            c.up(out::println);
            c.ensureHealthy(Duration.ofMinutes(4), out::println);
            out.println("Cluster is healthy: " + ClusterTopology.local().bootstrapServers());
            return 0;
        }
    }

    @Command(name = "down", mixinStandardHelpOptions = true, description = "docker compose down.")
    static final class Down implements Callable<Integer> {
        @Spec
        CommandSpec spec;

        @Option(names = {"-v", "--volumes"}, description = "Also remove data volumes")
        boolean volumes;

        @Override
        public Integer call() {
            new ComposeCluster(new DockerCli(new ProcessCommandExecutor()), ClusterTopology.local(), Path.of(".")).down(volumes);
            spec.commandLine().getOut().println("Cluster stopped.");
            return 0;
        }
    }
}
