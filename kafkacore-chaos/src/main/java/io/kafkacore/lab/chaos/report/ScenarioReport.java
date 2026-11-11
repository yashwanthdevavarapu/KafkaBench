package io.kafkacore.lab.chaos.report;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kafkacore.lab.bench.report.HtmlPage;
import io.kafkacore.lab.bench.report.SvgCharts;
import io.kafkacore.lab.bench.result.ResultIO;
import io.kafkacore.lab.chaos.scenario.ScenarioResult;
import io.kafkacore.lab.chaos.scenario.Timeline;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** JSON / Markdown / HTML output for chaos scenario results. */
public final class ScenarioReport {

    private ScenarioReport() {
    }

    public static void writeJson(Path file, List<ScenarioResult> results) {
        try {
            if (file.getParent() != null) Files.createDirectories(file.getParent());
            ResultIO.JSON.writeValue(file.toFile(), results);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static List<ScenarioResult> readJson(Path file) {
        try {
            return ResultIO.JSON.readValue(file.toFile(), new TypeReference<List<ScenarioResult>>() { });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static String markdown(List<ScenarioResult> results) {
        StringBuilder sb = new StringBuilder("## KafkaCore Lab — fault-injection results\n\n");
        sb.append("| Scenario | Fault | Result | Key timings | Acked | Lost | Duration |\n|---|---|---|---|---:|---:|---:|\n");
        for (ScenarioResult r : results) {
            sb.append(String.format(Locale.ROOT, "| %s | %s | %s | %s | %,d | %d | %d s |%n", r.id(), r.fault(),
                    r.passed() ? "✅ PASS" : "❌ FAIL", timings(r.metrics()), r.producedAcked(), r.integrity().lost(),
                    r.durationMs() / 1000));
        }
        for (ScenarioResult r : results) {
            sb.append("\n### ").append(r.title()).append(" (`").append(r.id()).append("`)\n\n").append(r.description()).append("\n\n");
            for (ScenarioResult.Check c : r.checks()) {
                sb.append("- ").append(c.passed() ? "✅ " : "❌ ").append(c.name()).append(" — ").append(c.detail()).append('\n');
            }
        }
        return sb.toString();
    }

    public static String html(List<ScenarioResult> results) {
        long passed = results.stream().filter(ScenarioResult::passed).count();
        long acked = results.stream().mapToLong(ScenarioResult::producedAcked).sum();
        long lost = results.stream().mapToLong(r -> r.integrity().lost()).sum();
        StringBuilder b = new StringBuilder("<div class='tiles'>");
        b.append(HtmlPage.tile("Scenarios passed", passed + " / " + results.size(), "fault-injection experiments",
                passed == results.size() ? "good" : "bad"));
        b.append(HtmlPage.tile("Acked messages verified", String.format(Locale.ROOT, "%,d", acked), "sequence + CRC checked", "info"));
        b.append(HtmlPage.tile("Acknowledged data lost", Long.toString(lost), "acks=all, min.insync.replicas=2", lost == 0 ? "good" : "bad"));
        results.stream().filter(r -> r.metrics().containsKey("controllerFailoverMs")).findFirst().ifPresent(r ->
                b.append(HtmlPage.tile("KRaft controller failover", r.metrics().get("controllerFailoverMs") + " ms",
                        "epoch " + r.metrics().get("epochBefore") + " → " + r.metrics().get("epochAfter"), "info")));
        b.append("</div>");

        b.append("<h2>Summary</h2><div class='card'><table><tr><th>Scenario</th><th>Fault</th><th>Result</th>"
                + "<th>Key timings</th><th>Acked</th><th>Lost</th><th>Dup (re-delivery)</th><th>Send errors</th><th>Duration</th></tr>");
        for (ScenarioResult r : results) {
            b.append(String.format(Locale.ROOT,
                    "<tr><td><b>%s</b></td><td>%s</td><td>%s</td><td style='text-align:left;white-space:normal'>%s</td><td>%,d</td><td>%d</td><td>%d</td><td>%d</td><td>%d s</td></tr>",
                    SvgCharts.esc(r.id()), SvgCharts.esc(r.fault()), HtmlPage.chip(r.passed() ? "PASS" : "FAIL"),
                    SvgCharts.esc(timings(r.metrics())), r.producedAcked(), r.integrity().lost(), r.integrity().duplicates(),
                    r.producerErrors(), r.durationMs() / 1000));
        }
        b.append("</table></div>");

        for (ScenarioResult r : results) {
            b.append("<h2>").append(SvgCharts.esc(r.title())).append(' ').append(HtmlPage.chip(r.passed() ? "PASS" : "FAIL"))
                    .append("</h2><p class='muted'>").append(SvgCharts.esc(r.description())).append("</p><div class='grid2'><div class='card'>");
            if (!r.throughput().isEmpty()) {
                double[] xs = r.throughput().stream().mapToDouble(s -> s.atMs() / 1000.0).toArray();
                List<SvgCharts.Marker> markers = new ArrayList<>();
                for (Timeline.Event e : r.timeline()) {
                    if (e.kind() == Timeline.Kind.FAULT) markers.add(new SvgCharts.Marker(e.atMs() / 1000.0, "fault", "#dc2626"));
                    if (e.kind() == Timeline.Kind.HEAL) markers.add(new SvgCharts.Marker(e.atMs() / 1000.0, "heal", "#047857"));
                }
                b.append(SvgCharts.lineChart("Client throughput during the fault", List.of(
                        new SvgCharts.Series("acked msg/s", xs, r.throughput().stream().mapToDouble(ScenarioResult.Sample::producedPerSec).toArray(), SvgCharts.PALETTE[0]),
                        new SvgCharts.Series("consumed msg/s", xs, r.throughput().stream().mapToDouble(ScenarioResult.Sample::consumedPerSec).toArray(), SvgCharts.PALETTE[1])),
                        "seconds since scenario start", "msg/s", markers));
                b.append(SvgCharts.lineChart("Publish p99 per second", List.of(
                        new SvgCharts.Series("publish p99 (ms)", xs, r.throughput().stream().mapToDouble(ScenarioResult.Sample::publishP99Ms).toArray(), SvgCharts.PALETTE[2])),
                        "seconds since scenario start", "ms", markers));
            }
            b.append("<h3>Checks</h3><table>");
            for (ScenarioResult.Check c : r.checks()) {
                b.append("<tr><td style='text-align:left;white-space:normal'>").append(HtmlPage.chip(c.passed() ? "PASS" : "FAIL")).append(' ')
                        .append(SvgCharts.esc(c.name())).append("</td><td style='text-align:left;white-space:normal' class='muted'>")
                        .append(SvgCharts.esc(c.detail())).append("</td></tr>");
            }
            b.append("</table></div><div class='card'><h3 style='margin-top:0'>Timeline</h3><ul class='tlx'>");
            for (Timeline.Event e : r.timeline()) {
                if (e.kind() == Timeline.Kind.CHECK) continue;
                String color = switch (e.kind()) {
                    case FAULT, ERROR -> "#dc2626";
                    case HEAL, RECOVERED -> "#047857";
                    case OBSERVE -> "#2563eb";
                    default -> "#64748b";
                };
                b.append(String.format(Locale.ROOT, "<li><span class='ts'>%.1f s</span><span class='ty' style='color:%s'>%s</span><span>%s</span></li>",
                        e.atMs() / 1000.0, color, e.kind(), SvgCharts.esc(e.message())));
            }
            b.append("</ul></div></div>");
        }
        return HtmlPage.wrap("KafkaCore Lab — Fault Injection Report",
                results.size() + " scenario(s) against a 3-node KRaft cluster · Apache Kafka 4.0", b.toString());
    }

    /** Human-readable summary of the *Ms metrics, e.g. "leaderElection 6.2 s · isrRejoin 3.1 s". */
    static String timings(Map<String, Object> metrics) {
        List<String> parts = new ArrayList<>();
        metrics.forEach((k, v) -> {
            if (k.endsWith("Ms") && v instanceof Number n && !k.startsWith("fullRecovery")) {
                parts.add(k.substring(0, k.length() - 2) + " " + String.format(Locale.ROOT, "%.1f s", n.longValue() / 1000.0));
            }
        });
        return String.join(" · ", parts);
    }
}
