package io.kafkacore.lab.cli;

import io.kafkacore.lab.bench.process.ProcessCommandExecutor;
import io.kafkacore.lab.chaos.cluster.ClusterInspector;
import io.kafkacore.lab.chaos.cluster.ClusterTopology;
import io.kafkacore.lab.chaos.cluster.ComposeCluster;
import io.kafkacore.lab.chaos.docker.DockerCli;
import io.kafkacore.lab.chaos.fault.DockerFaultInjector;
import io.kafkacore.lab.chaos.report.ScenarioReport;
import io.kafkacore.lab.chaos.scenario.ChaosScenario;
import io.kafkacore.lab.chaos.scenario.ScenarioContext;
import io.kafkacore.lab.chaos.scenario.ScenarioRegistry;
import io.kafkacore.lab.chaos.scenario.ScenarioResult;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "chaos", description = "Fault-injection scenarios against the local KRaft cluster.",
        mixinStandardHelpOptions = true, subcommands = {ChaosCommand.ListScenarios.class, ChaosCommand.RunScenarios.class})
public final class ChaosCommand implements Runnable {

    @Spec
    CommandSpec spec;

    @Override
    public void run() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }

    @Command(name = "list", mixinStandardHelpOptions = true, description = "List available scenarios.")
    static final class ListScenarios implements Callable<Integer> {
        @Spec
        CommandSpec spec;

        @Override
        public Integer call() {
            PrintWriter out = spec.commandLine().getOut();
            for (ChaosScenario s : new ScenarioRegistry().all()) {
                out.printf("%-20s %-30s %s%n", s.id(), s.fault().description(), s.title());
                out.printf("%-20s %s%n%n", "", s.description());
            }
            return 0;
        }
    }

    @Command(name = "run", mixinStandardHelpOptions = true, description = "Run scenarios (ids or 'all'). Exit 3 if any fails.")
    static final class RunScenarios implements Callable<Integer> {
        @Spec
        CommandSpec spec;

        @Parameters(arity = "0..*", paramLabel = "SCENARIO", description = "Scenario ids, or 'all' (default)")
        List<String> ids = new ArrayList<>();

        @Option(names = "--rate", description = "Background workload rate in msg/s (default: ${DEFAULT-VALUE})")
        int rate = 1000;

        @Option(names = "--steady-state", description = "Seconds of traffic before the fault and after recovery (default: ${DEFAULT-VALUE})")
        int steadyStateSec = 6;

        @Option(names = "--recovery-timeout", description = "Max seconds for each recovery step (default: ${DEFAULT-VALUE})")
        int recoveryTimeoutSec = 90;

        @Option(names = {"-o", "--output"}, description = "Output directory (default: ${DEFAULT-VALUE})")
        Path output = Path.of("results", "chaos");

        @Override
        public Integer call() throws Exception {
            PrintWriter out = spec.commandLine().getOut();
            ClusterTopology topology = ClusterTopology.local();
            DockerCli docker = new DockerCli(new ProcessCommandExecutor());
            ComposeCluster cluster = new ComposeCluster(docker, topology, Path.of("."));
            cluster.ensureHealthy(Duration.ofMinutes(4), line -> { out.println(line); out.flush(); });
            List<ScenarioResult> results = new ArrayList<>();
            try (ClusterInspector inspector = new ClusterInspector(topology.bootstrapServers())) {
                ScenarioContext ctx = new ScenarioContext(topology, new DockerFaultInjector(docker, topology.network()),
                        inspector, rate, Duration.ofSeconds(steadyStateSec), Duration.ofSeconds(recoveryTimeoutSec),
                        line -> { out.println(line); out.flush(); });
                for (ChaosScenario s : new ScenarioRegistry().resolve(ids)) {
                    out.println();
                    results.add(s.run(ctx));
                }
            }
            Files.createDirectories(output);
            ScenarioReport.writeJson(output.resolve("chaos-results.json"), results);
            String md = ScenarioReport.markdown(results);
            Files.writeString(output.resolve("chaos-summary.md"), md);
            Files.writeString(output.resolve("chaos-report.html"), ScenarioReport.html(results));
            out.println();
            out.println(md);
            out.println("Results written to " + output.toAbsolutePath());
            return results.stream().allMatch(ScenarioResult::passed) ? 0 : KafkaCoreCli.EXIT_CHAOS_FAILED;
        }
    }
}
