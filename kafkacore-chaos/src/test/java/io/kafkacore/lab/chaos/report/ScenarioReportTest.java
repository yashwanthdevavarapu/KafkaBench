package io.kafkacore.lab.chaos.report;

import io.kafkacore.lab.bench.integrity.IntegrityReport;
import io.kafkacore.lab.bench.metrics.LatencyStats;
import io.kafkacore.lab.chaos.scenario.ScenarioResult;
import io.kafkacore.lab.chaos.scenario.Timeline;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ScenarioReportTest {

    static ScenarioResult sample(boolean passed) {
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("controllerFailoverMs", 1234L);
        metrics.put("epochBefore", 3L);
        metrics.put("epochAfter", 4L);
        metrics.put("victimNode", 2);
        return new ScenarioResult("controller-failover", "KRaft active controller failover", "desc <b>",
                "hard crash (SIGKILL)", Instant.parse("2026-01-01T00:00:00Z"), 42_000, passed,
                List.of(new ScenarioResult.Check("new KRaft controller elected", true, "1234 ms"),
                        new ScenarioResult.Check("no acknowledged message lost", passed, passed ? "0 lost" : "3 lost")),
                metrics, new IntegrityReport(5000, 0, 5000, passed ? 0 : 3, 12, 0, 0, 0),
                new LatencyStats(5000, 1, 2, 2, 5, 8, 20, 900), 5000, 0,
                List.of(new Timeline.Event(1000, Timeline.Kind.INFO, "start"),
                        new Timeline.Event(7000, Timeline.Kind.FAULT, "kill node 2"),
                        new Timeline.Event(8234, Timeline.Kind.OBSERVE, "new controller"),
                        new Timeline.Event(12000, Timeline.Kind.HEAL, "start node 2")),
                List.of(new ScenarioResult.Sample(1000, 1000, 1000, 3, 0),
                        new ScenarioResult.Sample(8000, 200, 150, 900, 0),
                        new ScenarioResult.Sample(13000, 1000, 1100, 4, 0)),
                null);
    }

    @Test
    void jsonRoundTrip(@TempDir Path dir) {
        Path f = dir.resolve("chaos.json");
        ScenarioReport.writeJson(f, List.of(sample(true)));
        ScenarioResult back = ScenarioReport.readJson(f).get(0);
        assertThat(back.id()).isEqualTo("controller-failover");
        assertThat(back.timeline()).hasSize(4);
        assertThat(((Number) back.metrics().get("controllerFailoverMs")).longValue()).isEqualTo(1234L);
    }

    @Test
    void markdownSummarisesChecksAndTimings() {
        String md = ScenarioReport.markdown(List.of(sample(true), sample(false)));
        assertThat(md).contains("✅ PASS", "❌ FAIL", "controllerFailover 1.2 s", "❌ no acknowledged message lost — 3 lost");
    }

    @Test
    void htmlHasFaultMarkersTilesAndEscapedText() {
        String html = ScenarioReport.html(List.of(sample(true)));
        assertThat(html).contains("KRaft controller failover", "1234 ms", "epoch 3 → 4", ">fault<", ">heal<", "Timeline")
                .contains("desc &lt;b&gt;").doesNotContain("desc <b>");
    }

    @Test
    void timingsIgnoreNonDurationMetrics() {
        assertThat(ScenarioReport.timings(Map.of("victimNode", 2, "leaderElectionMs", 6500L)))
                .isEqualTo("leaderElection 6.5 s");
    }
}
