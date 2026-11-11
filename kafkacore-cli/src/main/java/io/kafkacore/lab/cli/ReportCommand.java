package io.kafkacore.lab.cli;

import io.kafkacore.lab.bench.regression.RegressionAnalyzer;
import io.kafkacore.lab.bench.regression.RegressionPolicy;
import io.kafkacore.lab.bench.regression.RegressionReport;
import io.kafkacore.lab.bench.report.HtmlReport;
import io.kafkacore.lab.bench.result.BenchmarkResult;
import io.kafkacore.lab.bench.result.ResultIO;
import io.kafkacore.lab.chaos.report.ScenarioReport;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "report", mixinStandardHelpOptions = true,
        description = "Render existing results (benchmark or chaos JSON) into a self-contained HTML report.")
public final class ReportCommand implements Callable<Integer> {

    @Spec
    CommandSpec spec;

    @Parameters(arity = "1..*", paramLabel = "RESULTS", description = "results.json / chaos-results.json file(s)")
    List<Path> inputs;

    @Option(names = {"-o", "--output"}, required = true, description = "HTML file to write")
    Path output;

    @Option(names = "--baseline", description = "Baseline results.json to include a regression section")
    Path baseline;

    @Option(names = "--chaos", description = "Treat inputs as chaos scenario results")
    boolean chaos;

    @Override
    public Integer call() throws Exception {
        if (output.toAbsolutePath().getParent() != null) Files.createDirectories(output.toAbsolutePath().getParent());
        if (chaos) {
            List<io.kafkacore.lab.chaos.scenario.ScenarioResult> all = new ArrayList<>();
            for (Path p : inputs) all.addAll(ScenarioReport.readJson(p));
            Files.writeString(output, ScenarioReport.html(all));
        } else {
            List<BenchmarkResult> all = new ArrayList<>();
            for (Path p : inputs) all.addAll(ResultIO.read(p));
            RegressionReport regression = baseline == null ? null
                    : new RegressionAnalyzer(RegressionPolicy.defaults()).compare(ResultIO.read(baseline), all);
            Files.writeString(output, HtmlReport.render(all, regression));
        }
        spec.commandLine().getOut().println("Wrote " + output.toAbsolutePath());
        return 0;
    }
}
