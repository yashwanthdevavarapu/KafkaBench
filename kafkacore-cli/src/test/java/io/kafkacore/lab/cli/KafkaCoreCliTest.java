package io.kafkacore.lab.cli;

import io.kafkacore.lab.bench.Fixtures;
import io.kafkacore.lab.bench.result.ResultIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaCoreCliTest {

    private final StringWriter out = new StringWriter();
    private final StringWriter err = new StringWriter();

    private int run(String... args) {
        CommandLine cl = KafkaCoreCli.commandLine();
        cl.setOut(new PrintWriter(out, true));
        cl.setErr(new PrintWriter(err, true));
        return cl.execute(args);
    }

    @Test
    void topLevelHelpListsAllCommands() {
        assertThat(run("--help")).isZero();
        assertThat(out.toString()).contains("bench", "chaos", "cluster", "report");
    }

    @Test
    void chaosListShowsEveryScenario() {
        assertThat(run("chaos", "list")).isZero();
        assertThat(out.toString()).contains("leader-kill", "controller-failover", "broker-pause", "network-partition",
                "rolling-restart");
    }

    @Test
    void compareExitsWithRegressionCodeAndWritesArtifacts(@TempDir Path dir) throws Exception {
        Path base = dir.resolve("base.json");
        Path cur = dir.resolve("cur.json");
        ResultIO.write(base, List.of(Fixtures.result("w", 1024, 10_000, 5, 10, 0)));
        ResultIO.write(cur, List.of(Fixtures.result("w", 1024, 7_000, 5, 10, 0)));
        Path md = dir.resolve("summary.md");
        Path html = dir.resolve("report.html");

        int code = run("bench", "compare", "--baseline", base.toString(), "--current", cur.toString(),
                "--markdown", md.toString(), "--html", html.toString());

        assertThat(code).isEqualTo(KafkaCoreCli.EXIT_REGRESSION);
        assertThat(out.toString()).contains("FAIL", "-30.0%");
        assertThat(Files.readString(md)).contains("Regression check");
        assertThat(Files.readString(html)).contains("<svg");
    }

    @Test
    void comparePassesForEquivalentResults(@TempDir Path dir) {
        Path base = dir.resolve("base.json");
        ResultIO.write(base, List.of(Fixtures.result("w", 1024, 10_000, 5, 10, 0)));
        assertThat(run("bench", "compare", "--baseline", base.toString(), "--current", base.toString())).isZero();
    }

    @Test
    void reportRendersHtmlFromResults(@TempDir Path dir) throws Exception {
        Path in = dir.resolve("r.json");
        ResultIO.write(in, List.of(Fixtures.result("w", 1024, 10_000, 5, 10, 0)));
        Path html = dir.resolve("out/r.html");
        assertThat(run("report", in.toString(), "-o", html.toString())).isZero();
        assertThat(Files.readString(html)).contains("KafkaCore Lab — Benchmark Report");
    }

    @Test
    void invalidWorkloadFailsWithReadableError(@TempDir Path dir) throws Exception {
        Path bad = dir.resolve("bad.yaml");
        Files.writeString(bad, "name: x\npartitions: 0\n");
        assertThat(run("bench", "run", bad.toString(), "-o", dir.resolve("o").toString())).isEqualTo(1);
        assertThat(err.toString()).contains("partitions must be >= 1");
    }

    @Test
    void unknownCommandIsUsageError() {
        assertThat(run("frobnicate")).isEqualTo(2);
    }
}
