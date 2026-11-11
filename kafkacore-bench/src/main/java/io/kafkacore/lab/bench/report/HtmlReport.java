package io.kafkacore.lab.bench.report;

import io.kafkacore.lab.bench.metrics.ResourceSummary;
import io.kafkacore.lab.bench.metrics.TimeSeriesPoint;
import io.kafkacore.lab.bench.regression.RegressionReport;
import io.kafkacore.lab.bench.result.BenchmarkResult;

import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/** Self-contained HTML benchmark report with SVG charts and an optional regression section. */
public final class HtmlReport {

    private static final int MAX_TIMELINES = 6;

    private HtmlReport() {
    }

    public static String render(List<BenchmarkResult> results, RegressionReport regression) {
        if (results.isEmpty()) {
            return HtmlPage.wrap("KafkaCore Lab — Benchmark Report", "no results", "<p>No results.</p>");
        }
        BenchmarkResult first = results.get(0);
        String subtitle = String.format(Locale.ROOT, "%d run(s) · kafka-clients %s · %s · %s · %s",
                results.size(), first.metadata().kafkaClientVersion(), first.metadata().bootstrapServers(),
                first.metadata().os(),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm 'UTC'").withZone(ZoneOffset.UTC).format(first.metadata().startedAt()));
        StringBuilder b = new StringBuilder();
        tiles(b, results, regression);
        summaryTable(b, results);
        charts(b, results);
        if (regression != null) regressionSection(b, regression);
        resources(b, results);
        timelines(b, results);
        return HtmlPage.wrap("KafkaCore Lab — Benchmark Report", subtitle, b.toString());
    }

    private static void tiles(StringBuilder b, List<BenchmarkResult> results, RegressionReport regression) {
        BenchmarkResult peak = results.stream().max((x, y) -> Double.compare(x.throughput().producedMBPerSec(),
                y.throughput().producedMBPerSec())).orElseThrow();
        BenchmarkResult bestP99 = results.stream().min((x, y) -> Double.compare(x.publishLatency().p99Ms(),
                y.publishLatency().p99Ms())).orElseThrow();
        long lost = results.stream().mapToLong(r -> r.integrity().lost()).sum();
        long acked = results.stream().mapToLong(r -> r.integrity().acked()).sum();
        b.append("<div class='tiles'>");
        b.append(HtmlPage.tile("Peak throughput", String.format(Locale.ROOT, "%.1f MB/s", peak.throughput().producedMBPerSec()),
                String.format(Locale.ROOT, "%,.0f msg/s · %s", peak.throughput().producedMsgsPerSec(), peak.workload().name()), "info"));
        b.append(HtmlPage.tile("Best publish p99", String.format(Locale.ROOT, "%.2f ms", bestP99.publishLatency().p99Ms()),
                bestP99.workload().name(), "info"));
        b.append(HtmlPage.tile("Message integrity", lost == 0 ? "0 lost" : lost + " lost",
                String.format(Locale.ROOT, "%,d acknowledged messages verified", acked), lost == 0 ? "good" : "bad"));
        if (regression != null) {
            b.append(HtmlPage.tile("Regression gate", regression.verdict().name(), "vs. baseline",
                    switch (regression.verdict()) { case PASS -> "good"; case WARN -> "warn"; case FAIL -> "bad"; }));
        }
        b.append("</div>");
    }

    private static void summaryTable(StringBuilder b, List<BenchmarkResult> results) {
        b.append("<h2>Results</h2><div class='card'><table><tr><th>Workload</th><th>Size</th><th>Target</th><th>Prod.</th>"
                + "<th>msg/s</th><th>MB/s</th><th>p50</th><th>p95</th><th>p99</th><th>p99.9</th><th>E2E p99</th>"
                + "<th>Acked</th><th>Lost</th><th>Dup</th></tr>");
        for (BenchmarkResult r : results) {
            var w = r.workload();
            var p = r.publishLatency();
            b.append(String.format(Locale.ROOT,
                    "<tr><td>%s</td><td>%s B</td><td>%s</td><td>%d</td><td>%,.0f</td><td>%.2f</td><td>%.2f</td><td>%.2f</td>"
                            + "<td><b>%.2f</b></td><td>%.2f</td><td>%.2f</td><td>%,d</td><td>%s</td><td>%d</td></tr>",
                    SvgCharts.esc(w.name()), SvgCharts.fmt(w.messageSizeBytes()),
                    w.targetRatePerSec() == 0 ? "max" : SvgCharts.fmt(w.targetRatePerSec()) + "/s", w.producers(),
                    r.throughput().producedMsgsPerSec(), r.throughput().producedMBPerSec(), p.p50Ms(), p.p95Ms(),
                    p.p99Ms(), p.p999Ms(), r.endToEndLatency().p99Ms(), r.integrity().acked(),
                    r.integrity().lost() == 0 ? HtmlPage.chip("0") : HtmlPage.chip("FAIL") + " " + r.integrity().lost(),
                    r.integrity().duplicates()));
        }
        b.append("</table><p class='muted'>Latencies in milliseconds. Publish latency = send() to broker ack, "
                + "corrected for coordinated omission when rate-limited. E2E = producer send to consumer receive.</p></div>");
    }

    private static void charts(StringBuilder b, List<BenchmarkResult> results) {
        List<String> names = results.stream().map(r -> r.workload().name()).toList();
        double[][] lat = new double[results.size()][];
        double[][] tp = new double[results.size()][];
        for (int i = 0; i < results.size(); i++) {
            var p = results.get(i).publishLatency();
            lat[i] = new double[]{p.p50Ms(), p.p95Ms(), p.p99Ms(), p.p999Ms()};
            tp[i] = new double[]{results.get(i).throughput().producedMBPerSec(), results.get(i).throughput().consumedMBPerSec()};
        }
        b.append("<h2>Latency &amp; throughput</h2><div class='grid2'><div class='card'>");
        b.append(SvgCharts.groupedBars("Publish latency percentiles", names, List.of("p50", "p95", "p99", "p99.9"), lat, "ms"));
        b.append("</div><div class='card'>");
        b.append(SvgCharts.groupedBars("Throughput", names, List.of("produced MB/s", "consumed MB/s"), tp, "MB/s"));
        b.append("</div></div>");

        // Message-size sweep: throughput and p99 vs size, when the runs differ by size.
        long distinctSizes = results.stream().map(r -> r.workload().messageSizeBytes()).distinct().count();
        if (distinctSizes > 1) {
            List<BenchmarkResult> sorted = new ArrayList<>(results);
            sorted.sort((x, y) -> Integer.compare(x.workload().messageSizeBytes(), y.workload().messageSizeBytes()));
            double[] xs = sorted.stream().mapToDouble(r -> r.workload().messageSizeBytes()).toArray();
            b.append("<div class='grid2'><div class='card'>");
            b.append(SvgCharts.lineChart("Throughput vs message size", List.of(
                    new SvgCharts.Series("MB/s", xs, sorted.stream().mapToDouble(r -> r.throughput().producedMBPerSec()).toArray(), SvgCharts.PALETTE[0])),
                    "message size (bytes)", "MB/s"));
            b.append("</div><div class='card'>");
            b.append(SvgCharts.lineChart("Publish latency vs message size", List.of(
                    new SvgCharts.Series("p95", xs, sorted.stream().mapToDouble(r -> r.publishLatency().p95Ms()).toArray(), SvgCharts.PALETTE[1]),
                    new SvgCharts.Series("p99", xs, sorted.stream().mapToDouble(r -> r.publishLatency().p99Ms()).toArray(), SvgCharts.PALETTE[2])),
                    "message size (bytes)", "ms"));
            b.append("</div></div>");
        }
    }

    private static void regressionSection(StringBuilder b, RegressionReport report) {
        b.append("<h2>Regression check ").append(HtmlPage.chip(report.verdict().name())).append("</h2>");
        for (RegressionReport.WorkloadComparison w : report.workloads()) {
            b.append("<div class='card'><h3>").append(SvgCharts.esc(w.workload())).append(' ')
                    .append(HtmlPage.chip(w.verdict().name())).append("</h3><table><tr><th>Metric</th><th>Baseline</th>"
                            + "<th>Current</th><th>Δ</th><th>Threshold</th><th>Status</th></tr>");
            for (RegressionReport.MetricComparison m : w.metrics()) {
                b.append(String.format(Locale.ROOT, "<tr><td>%s</td><td>%s</td><td>%s</td><td>%+.1f%%</td><td>%s</td><td>%s</td></tr>",
                        SvgCharts.esc(m.metric()), SvgCharts.fmt(m.baseline()), SvgCharts.fmt(m.current()), m.deltaPct(),
                        m.thresholdPct() == 0 ? "—" : "±" + SvgCharts.fmt(m.thresholdPct()) + "%", HtmlPage.chip(m.status().name())));
            }
            b.append("</table></div>");
        }
    }

    private static void resources(StringBuilder b, List<BenchmarkResult> results) {
        Map<String, double[]> rows = new TreeMap<>();
        for (BenchmarkResult r : results) {
            ResourceSummary rs = r.resources();
            if (rs == null) continue;
            rs.gauges().forEach((k, g) -> {
                if (k.endsWith("cpu.pct") || k.endsWith("mem.mb") || k.endsWith("heap.mb")) {
                    double[] a = rows.computeIfAbsent(k, x -> new double[]{0, 0, 0});
                    a[0] += g.avg();
                    a[1] = Math.max(a[1], g.max());
                    a[2]++;
                }
            });
        }
        if (rows.isEmpty()) return;
        b.append("<h2>Resource utilisation</h2><div class='card'><table><tr><th>Gauge</th><th>Avg (across runs)</th><th>Max</th></tr>");
        rows.forEach((k, a) -> b.append(String.format(Locale.ROOT, "<tr><td><code>%s</code></td><td>%.2f</td><td>%.2f</td></tr>",
                SvgCharts.esc(k), a[0] / a[2], a[1])));
        b.append("</table><p class='muted'>broker.* gauges come from <code>docker stats</code> on each KRaft node; "
                + "client.* from the benchmark JVM (CPU % of all cores).</p></div>");
    }

    private static void timelines(StringBuilder b, List<BenchmarkResult> results) {
        b.append("<h2>Per-second timelines</h2><div class='grid2'>");
        int n = 0;
        for (BenchmarkResult r : results) {
            if (n++ >= MAX_TIMELINES || r.timeline() == null || r.timeline().isEmpty()) continue;
            double[] xs = r.timeline().stream().mapToDouble(TimeSeriesPoint::second).toArray();
            b.append("<div class='card'>");
            b.append(SvgCharts.lineChart(r.workload().name() + " — msg/s", List.of(
                    new SvgCharts.Series("produced", xs, r.timeline().stream().mapToDouble(TimeSeriesPoint::producedMsgs).toArray(), SvgCharts.PALETTE[0]),
                    new SvgCharts.Series("consumed", xs, r.timeline().stream().mapToDouble(TimeSeriesPoint::consumedMsgs).toArray(), SvgCharts.PALETTE[1])),
                    "second", "msg/s"));
            b.append(SvgCharts.lineChart(r.workload().name() + " — p99 per second", List.of(
                    new SvgCharts.Series("publish p99", xs, r.timeline().stream().mapToDouble(TimeSeriesPoint::publishP99Ms).toArray(), SvgCharts.PALETTE[2]),
                    new SvgCharts.Series("e2e p99", xs, r.timeline().stream().mapToDouble(TimeSeriesPoint::endToEndP99Ms).toArray(), SvgCharts.PALETTE[3])),
                    "second", "ms"));
            b.append("</div>");
        }
        b.append("</div>");
    }
}
