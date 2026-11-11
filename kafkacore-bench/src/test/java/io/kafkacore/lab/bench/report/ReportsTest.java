package io.kafkacore.lab.bench.report;

import io.kafkacore.lab.bench.Fixtures;
import io.kafkacore.lab.bench.regression.RegressionAnalyzer;
import io.kafkacore.lab.bench.regression.RegressionPolicy;
import io.kafkacore.lab.bench.regression.RegressionReport;
import io.kafkacore.lab.bench.result.BenchmarkResult;
import io.kafkacore.lab.bench.result.ResultIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ReportsTest {

    private final List<BenchmarkResult> results = List.of(
            Fixtures.result("sweep[size=128]", 128, 50_000, 3, 6, 0),
            Fixtures.result("sweep[size=1024]", 1024, 30_000, 5, 9, 0),
            Fixtures.result("sweep[size=<8192>]", 8192, 8_000, 12, 25, 0));

    @Test
    void jsonRoundTripPreservesEverything(@TempDir Path dir) {
        Path f = dir.resolve("nested/results.json");
        ResultIO.write(f, results);
        List<BenchmarkResult> back = ResultIO.read(f);
        assertThat(back).usingRecursiveComparison().isEqualTo(results);
    }

    @Test
    void csvHasHeaderAndOneRowPerRun() {
        String csv = CsvReport.render(results);
        String[] lines = csv.split("\n");
        assertThat(lines).hasSize(4);
        assertThat(lines[0]).isEqualTo(CsvReport.HEADER);
        assertThat(lines[1].split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).hasSize(CsvReport.HEADER.split(",").length);
        assertThat(lines[2]).contains("\"sweep[size=1024]\"", "30000.00");
    }

    @Test
    void markdownContainsTableAndRegressionVerdict() {
        RegressionReport r = new RegressionAnalyzer(RegressionPolicy.defaults()).compare(results,
                List.of(Fixtures.result("sweep[size=128]", 128, 40_000, 3, 6, 0)));
        assertThat(MarkdownReport.results(results)).contains("| sweep[size=1024] | 1024 |", "p99");
        assertThat(MarkdownReport.regression(r)).contains("Regression check: ❌ FAIL", "-20.0%");
    }

    @Test
    void htmlIsSelfContainedEscapedAndHasSweepCharts() {
        RegressionReport r = new RegressionAnalyzer(RegressionPolicy.defaults()).compare(results, results);
        String html = HtmlReport.render(results, r);
        assertThat(html).startsWith("<!doctype html>")
                .contains("<svg", "Throughput vs message size", "Publish latency percentiles", "Regression check")
                .contains("sweep[size=&lt;8192&gt;]")
                .doesNotContain("sweep[size=<8192>]")
                .doesNotContain("<script", "http://cdn", "https://");
    }

    @Test
    void htmlHandlesEmptyResults() {
        assertThat(HtmlReport.render(List.of(), null)).contains("No results");
    }

    @Test
    void niceTicksCoverMaxWithRoundSteps() {
        assertThat(SvgCharts.niceTicks(97)).containsExactly(0, 20, 40, 60, 80, 100);
        assertThat(SvgCharts.niceTicks(0)).containsExactly(0, 1);
        double[] t = SvgCharts.niceTicks(0.37);
        assertThat(t[t.length - 1]).isGreaterThanOrEqualTo(0.37);
    }
}
