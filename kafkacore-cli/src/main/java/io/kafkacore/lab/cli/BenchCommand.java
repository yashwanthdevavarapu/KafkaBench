package io.kafkacore.lab.cli;

import io.kafkacore.lab.bench.driver.BenchmarkRunner;
import io.kafkacore.lab.bench.metrics.DockerStatsSampler;
import io.kafkacore.lab.bench.metrics.JvmResourceSampler;
import io.kafkacore.lab.bench.metrics.ResourceMonitor;
import io.kafkacore.lab.bench.metrics.ResourceSampler;
import io.kafkacore.lab.bench.process.ProcessCommandExecutor;
import io.kafkacore.lab.bench.regression.RegressionAnalyzer;
import io.kafkacore.lab.bench.regression.RegressionPolicy;
import io.kafkacore.lab.bench.regression.RegressionReport;
import io.kafkacore.lab.bench.report.CsvReport;
import io.kafkacore.lab.bench.report.HtmlReport;
import io.kafkacore.lab.bench.report.MarkdownReport;
import io.kafkacore.lab.bench.result.BenchmarkResult;
import io.kafkacore.lab.bench.result.ResultIO;
import io.kafkacore.lab.bench.workload.WorkloadLoader;
import io.kafkacore.lab.bench.workload.WorkloadSpec;
import io.kafkacore.lab.chaos.cluster.ClusterTopology;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "bench", description = "Run benchmarks and compare results against a baseline.",
        mixinStandardHelpOptions = true, subcommands = {BenchCommand.Run.class, BenchCommand.Compare.class})
public final class BenchCommand implements Runnable {

    @Spec
    CommandSpec spec;

    @Override
    public void run() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }

    @Command(name = "run", mixinStandardHelpOptions = true,
            description = "Run one or more workload files and write JSON/CSV/Markdown/HTML results.")
    static final class Run implements Callable<Integer> {

        @Spec
        CommandSpec spec;

        @Parameters(arity = "1..*", paramLabel = "WORKLOAD", description = "Workload YAML file(s), e.g. workloads/baseline.yaml")
        List<Path> workloads;

        @Option(names = {"-b", "--bootstrap"}, description = "Bootstrap servers (default: the local 3-node cluster)")
        String bootstrap = ClusterTopology.local().bootstrapServers();

        @Option(names = {"-o", "--output"}, description = "Output directory (default: ${DEFAULT-VALUE})")
        Path output = Path.of("results", "latest");

        @Option(names = "--duration", description = "Override measurement seconds for every workload")
        Integer duration;

        @Option(names = "--warmup", description = "Override warm-up seconds for every workload")
        Integer warmup;

        @Option(names = "--no-docker-stats", description = "Do not sample broker containers with docker stats")
        boolean noDockerStats;

        @Option(names = "--baseline", description = "Baseline results.json to gate against (exit 2 on FAIL)")
        Path baseline;

        @Option(names = "--policy", description = "Regression policy YAML (default: built-in thresholds)")
        Path policy;

        @Override
        public Integer call() throws Exception {
            PrintWriter out = spec.commandLine().getOut();
            List<BenchmarkResult> results = new ArrayList<>();
            List<ResourceSampler> samplers = new ArrayList<>();
            samplers.add(new JvmResourceSampler());
            if (!noDockerStats) {
                samplers.add(new DockerStatsSampler(new ProcessCommandExecutor(), ClusterTopology.local().containers()));
            }
            BenchmarkRunner runner = new BenchmarkRunner(bootstrap, () -> new ResourceMonitor(samplers, 2_000),
                    line -> { out.println(line); out.flush(); });
            for (Path file : workloads) {
                WorkloadSpec template = WorkloadLoader.load(file);
                if (duration != null || warmup != null) {
                    template = template.withDuration(warmup != null ? warmup : template.warmupSec(),
                            duration != null ? duration : template.durationSec());
                }
                out.printf("%n=== %s (%d run(s)) ===%n", file, template.expand().size());
                results.addAll(runner.runAll(template));
            }
            RegressionReport regression = null;
            if (baseline != null) {
                RegressionPolicy p = policy != null ? RegressionPolicy.load(policy) : RegressionPolicy.defaults();
                regression = new RegressionAnalyzer(p).compare(ResultIO.read(baseline), results);
            }
            writeOutputs(output, results, regression);
            out.println();
            out.println(MarkdownReport.results(results));
            if (regression != null) {
                out.println(MarkdownReport.regression(regression));
            }
            out.println("Results written to " + output.toAbsolutePath());
            return regression != null && regression.verdict() == RegressionReport.Verdict.FAIL ? KafkaCoreCli.EXIT_REGRESSION : 0;
        }
    }

    @Command(name = "compare", mixinStandardHelpOptions = true,
            description = "Compare current results against a baseline. Exit code 2 when any metric FAILs.")
    static final class Compare implements Callable<Integer> {

        @Spec
        CommandSpec spec;

        @Option(names = "--baseline", required = true, description = "Baseline results.json")
        Path baseline;

        @Option(names = "--current", required = true, description = "Current results.json")
        Path current;

        @Option(names = "--policy", description = "Regression policy YAML")
        Path policy;

        @Option(names = "--html", description = "Also write an HTML report with the comparison")
        Path html;

        @Option(names = "--markdown", description = "Also write the comparison as Markdown (e.g. $GITHUB_STEP_SUMMARY)")
        Path markdown;

        @Override
        public Integer call() throws Exception {
            RegressionPolicy p = policy != null ? RegressionPolicy.load(policy) : RegressionPolicy.defaults();
            List<BenchmarkResult> cur = ResultIO.read(current);
            RegressionReport report = new RegressionAnalyzer(p).compare(ResultIO.read(baseline), cur);
            String md = MarkdownReport.regression(report);
            spec.commandLine().getOut().println(md);
            if (markdown != null) Files.writeString(markdown, md);
            if (html != null) Files.writeString(html, HtmlReport.render(cur, report));
            return report.verdict() == RegressionReport.Verdict.FAIL ? KafkaCoreCli.EXIT_REGRESSION : 0;
        }
    }

    static void writeOutputs(Path dir, List<BenchmarkResult> results, RegressionReport regression) throws Exception {
        Files.createDirectories(dir);
        ResultIO.write(dir.resolve("results.json"), results);
        Files.writeString(dir.resolve("results.csv"), CsvReport.render(results));
        String md = MarkdownReport.results(results) + (regression == null ? "" : "\n" + MarkdownReport.regression(regression));
        Files.writeString(dir.resolve("summary.md"), md);
        Files.writeString(dir.resolve("report.html"), HtmlReport.render(results, regression));
    }
}
